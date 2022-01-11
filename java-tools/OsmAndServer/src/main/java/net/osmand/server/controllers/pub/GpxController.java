package net.osmand.server.controllers.pub;


import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import javax.xml.stream.FactoryConfigurationError;
import javax.xml.stream.XMLStreamException;

import net.osmand.server.WebSecurityConfiguration.OsmAndProUser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import org.xmlpull.v1.XmlPullParserException;

import com.google.gson.Gson;

import net.osmand.GPXUtilities;
import net.osmand.GPXUtilities.GPXFile;
import net.osmand.GPXUtilities.GPXTrackAnalysis;
import net.osmand.GPXUtilities.Track;
import net.osmand.GPXUtilities.TrkSegment;
import net.osmand.GPXUtilities.WptPt;
import net.osmand.IndexConstants;
import net.osmand.obf.OsmGpxWriteContext;
import net.osmand.obf.OsmGpxWriteContext.QueryParams;
import net.osmand.obf.preparation.IndexHeightData;
import net.osmand.server.controllers.pub.UserSessionResources.GPXSessionContext;
import net.osmand.server.controllers.pub.UserSessionResources.GPXSessionFile;
import net.osmand.util.Algorithms;


@RestController
@RequestMapping("/gpx/")
public class GpxController {
    
    public static final int MAX_SIZE_FILES = 10;
    public static final int MAX_SIZE_FILES_AUTH = 100;

	Gson gson = new Gson();
	
	@Autowired
	UserSessionResources session;
	
	@Value("${srtm.location}")
	String srtmLocation;
	
	@PostMapping(path = {"/clear"}, produces = "application/json")
	@ResponseBody
	public String clear(HttpServletRequest request, HttpSession httpSession) throws IOException {
		GPXSessionContext ctx = session.getGpxResources(httpSession);
		for (File f : ctx.tempFiles) {
			f.delete();
		}
		ctx.tempFiles.clear();
		ctx.files.clear();
		return gson.toJson(Map.of("all", ctx.files));
	}
	
	@GetMapping(path = { "/get-gpx-info" }, produces = "application/json")
	@ResponseBody
	public String getGpx(HttpServletRequest request, HttpSession httpSession) throws IOException {
		GPXSessionContext ctx = session.getGpxResources(httpSession);
		return gson.toJson(Map.of("all", ctx.files));
	}
	
	
	
	@GetMapping(path = {"/get-gpx-file"}, produces = "application/json")
	@ResponseBody
	public ResponseEntity<Resource> getGpx(@RequestParam(required = true) String name,
			HttpSession httpSession) throws IOException {
		GPXSessionContext ctx = session.getGpxResources(httpSession);
		File tmpGpx = null;
		for (int i = 0; i < ctx.files.size(); i++) {
			GPXSessionFile file = ctx.files.get(i);
			if (file.analysis != null && file.analysis.name.equals(name)) {
				tmpGpx = file.file;
			}
		}
		if (tmpGpx == null) {
			return ResponseEntity.notFound().build();
		}
		HttpHeaders headers = new HttpHeaders();
		headers.add(HttpHeaders.CONTENT_DISPOSITION, String.format("attachment; filename=\"" + name + "\""));
		headers.add(HttpHeaders.CONTENT_TYPE, "application/octet-binary");
		return ResponseEntity.ok().headers(headers).body(new FileSystemResource(tmpGpx));
	}
	
	public void cleanupFromNan(GPXTrackAnalysis analysis) {
		// process analysis
		if (Double.isNaN(analysis.minHdop)) {
			analysis.minHdop = -1;
			analysis.maxHdop = -1;
		}
		if(analysis.minSpeed > analysis.maxSpeed) {
			analysis.minSpeed = analysis.maxSpeed;
		}
		if(analysis.startTime > analysis.endTime) {
			analysis.startTime = analysis.endTime = 0;
		}
		cleanupFromNan(analysis.locationStart);
		cleanupFromNan(analysis.locationEnd);
	}

	private void cleanupFromNan(WptPt wpt) {
		if (wpt == null) {
			return;
		}
		if (Float.isNaN(wpt.heading)) {
			wpt.heading = 0;
		}
		if (Double.isNaN(wpt.ele)) {
			wpt.ele = 99999;
		}
		if (Double.isNaN(wpt.hdop)) {
			wpt.hdop = -1;
		}
	}
	
	@PostMapping(path = {"/upload-session-gpx"}, produces = "application/json")
	@ResponseBody
	public ResponseEntity<String> uploadGpx(HttpServletRequest request, HttpSession httpSession,
			@RequestParam(required = true) MultipartFile file) throws IOException {
		GPXSessionContext ctx = session.getGpxResources(httpSession);
		File tmpGpx = File.createTempFile("gpx_" + httpSession.getId(), ".gpx");
		
        double fileSizeMb = file.getSize()/(double)(1 << 20);
        double filesSize = getCommonSavedFilesSize(ctx.files);
        double maxSizeMb = getCommonMaxSizeFiles();
        
        if (fileSizeMb + filesSize > maxSizeMb) {
            return ResponseEntity.badRequest().body(String.format("You don't have enough space to save this file!"
                    + "\n"
                    + "Size of your file = %.2f Mb"
                    + "\n"
                    + "Max storage files = %.2f Mb"
                    + "\n"
                    + "Free space = %.2f Mb", fileSizeMb, maxSizeMb, maxSizeMb - filesSize));
        }
        
		InputStream is = file.getInputStream();
		FileOutputStream fous = new FileOutputStream(tmpGpx);
		Algorithms.streamCopy(is, fous);
		is.close();
		fous.close();
		
		ctx.tempFiles.add(tmpGpx);
		
		GPXFile gpxFile = GPXUtilities.loadGPXFile(tmpGpx);
		if (gpxFile.error != null) {
            return ResponseEntity.badRequest().body("Error reading gpx!");
        } else {
			GPXSessionFile sessionFile = new GPXSessionFile();
			ctx.files.add(sessionFile);
			gpxFile.path = file.getOriginalFilename();
			
			GPXTrackAnalysis analysis = gpxFile.getAnalysis(System.currentTimeMillis());
			sessionFile.file = tmpGpx;
            sessionFile.size = fileSizeMb;
			cleanupFromNan(analysis);
			sessionFile.analysis = analysis;
			GPXFile srtmGpx = calculateSrtmAltitude(gpxFile, null);
            GPXTrackAnalysis srtmAnalysis = null;
			if (srtmGpx != null) {
                srtmAnalysis = srtmGpx.getAnalysis(System.currentTimeMillis());
            }
			sessionFile.srtmAnalysis = srtmAnalysis;
			return ResponseEntity.ok(gson.toJson(Map.of("info", sessionFile)));
		}
	}
    
    private double getCommonMaxSizeFiles() {
        Object principal = SecurityContextHolder.getContext().getAuthentication().getPrincipal();
        if (principal instanceof OsmAndProUser) {
            return MAX_SIZE_FILES_AUTH;
        } else
            return MAX_SIZE_FILES;
    }
    
    private double getCommonSavedFilesSize(List<GPXSessionFile> files) {
	    double sizeFiles = 0L;
        for (GPXSessionFile file: files) {
            sizeFiles += file.size;
        }
        return sizeFiles;
    }

	@RequestMapping(path = { "/download-obf"})
	@ResponseBody
    public ResponseEntity<Resource> downloadObf(@RequestParam(defaultValue="", required=false) String gzip,
			HttpSession httpSession, HttpServletResponse resp) throws IOException, FactoryConfigurationError,
			XMLStreamException, SQLException, InterruptedException, XmlPullParserException {
		GPXSessionContext ctx = session.getGpxResources(httpSession);
		File tmpOsm = File.createTempFile("gpx_obf_" + httpSession.getId(), ".osm.gz");
		ctx.tempFiles.add(tmpOsm);
		List<File> files = new ArrayList<>();
		for (GPXSessionFile f : ctx.files) {
			files.add(f.file);
		}
		String sessionId = httpSession.getId();
		File tmpFolder = new File(tmpOsm.getParentFile(), sessionId);
		String fileName = "gpx_" + sessionId;
		QueryParams qp = new QueryParams();
		qp.osmFile = tmpOsm;
		qp.details = QueryParams.DETAILS_ELE_SPEED;
		OsmGpxWriteContext writeCtx = new OsmGpxWriteContext(qp);
		File targetObf = new File(tmpFolder.getParentFile(), fileName + IndexConstants.BINARY_MAP_INDEX_EXT);
		writeCtx.writeObf(files, tmpFolder, fileName, targetObf);
		ctx.tempFiles.add(targetObf);
		HttpHeaders headers = new HttpHeaders();
		headers.add(HttpHeaders.CONTENT_DISPOSITION, String.format("attachment; filename=\"gpx.obf\""));
		headers.add(HttpHeaders.CONTENT_TYPE, "application/octet-binary");
		return ResponseEntity.ok().headers(headers).body(new FileSystemResource(targetObf));
	}
	
	
	public GPXFile calculateSrtmAltitude(GPXFile gpxFile, File[] missingFile) {
		if (srtmLocation == null) {
			return null;
		}
		File srtmFolder = new File(srtmLocation);
		if (!srtmFolder.exists()) {
			return null;
		}
		IndexHeightData hd = new IndexHeightData();
		hd.setSrtmData(srtmFolder);
		for (Track tr : gpxFile.tracks) {
			for (TrkSegment s : tr.segments) {
				for (int i = 0; i < s.points.size(); i++) {
					WptPt wpt = s.points.get(i);
					double h = hd.getPointHeight(wpt.lat, wpt.lon, missingFile);
					if (h != IndexHeightData.INEXISTENT_HEIGHT) {
						wpt.ele = h;
					} else if (i == 0) {
						return null;
					}

				}
			}
		}
		return gpxFile;
	}

}
