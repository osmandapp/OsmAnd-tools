package net.osmand.server.controllers.pub;


import java.io.*;
import java.sql.SQLException;
import java.util.*;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import javax.validation.Valid;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import javax.xml.stream.FactoryConfigurationError;
import javax.xml.stream.XMLStreamException;

import com.google.gson.GsonBuilder;
import net.osmand.server.api.services.GpxService;
import net.osmand.server.api.services.OsmAndMapsService;
import net.osmand.server.utils.WebGpxParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.InputStreamResource;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;
import org.xmlpull.v1.XmlPullParserException;

import com.google.gson.Gson;

import net.osmand.GPXUtilities;
import net.osmand.GPXUtilities.GPXFile;
import net.osmand.GPXUtilities.GPXTrackAnalysis;
import net.osmand.IProgress;
import net.osmand.IndexConstants;
import net.osmand.obf.OsmGpxWriteContext;
import net.osmand.obf.OsmGpxWriteContext.QueryParams;
import net.osmand.server.WebSecurityConfiguration.OsmAndProUser;
import net.osmand.server.controllers.pub.UserSessionResources.GPXSessionContext;
import net.osmand.server.controllers.pub.UserSessionResources.GPXSessionFile;
import net.osmand.util.Algorithms;


@RestController
@RequestMapping("/gpx/")
public class GpxController {
    
	protected static final Log LOGGER = LogFactory.getLog(GpxController.class);
	
    public static final int MAX_SIZE_FILES = 10;
    public static final int MAX_SIZE_FILES_AUTH = 100;

	Gson gson = new Gson();
	
	Gson gsonWithNans = new GsonBuilder().serializeSpecialFloatingPointValues().create();
	
	@Autowired
	WebGpxParser webGpxParser;
	
	@Autowired
	OsmAndMapsService osmAndMapsService;
	
	@Autowired
	UserSessionResources session;
	
	@Autowired
	protected GpxService gpxService;
	
	@Value("${osmand.srtm.location}")
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
	
	@PostMapping(path = {"/process-srtm"}, produces = "application/json")
	public ResponseEntity<StreamingResponseBody> attachSrtm(@RequestPart(name = "file") @Valid @NotNull @NotEmpty MultipartFile file) throws IOException {
		GPXFile gpxFile = GPXUtilities.loadGPXFile(file.getInputStream());
		final StringBuilder err = new StringBuilder(); 
		if (srtmLocation == null) {
			err.append(String.format("Server is not configured for srtm processing. "));
		}
		GPXFile srtmGpx = gpxService.calculateSrtmAltitude(gpxFile, null);
		if (srtmGpx == null) {
			err.append(String.format(String.format("Couldn't calculate altitude for %s (%d KB)",
					file.getName(), file.getSize() / 1024l)));
		}
	    StreamingResponseBody responseBody = outputStream -> {
	    	OutputStreamWriter ouw = new OutputStreamWriter(outputStream);
			if (err.length() > 0) {
				ouw.write(err.toString());
			} else {
				GPXUtilities.writeGpx(ouw, srtmGpx, IProgress.EMPTY_PROGRESS);
			}
	    	ouw.close();
	    };
	    if (err.length() > 0) {
	    	return ResponseEntity.badRequest().body(responseBody);
	    }
		return ResponseEntity.ok()
	            .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename="+file.getName())
	            .contentType(MediaType.APPLICATION_XML)
	            .body(responseBody);
	}
	
	@PostMapping(path = {"/upload-session-gpx"}, produces = "application/json")
	public ResponseEntity<String> uploadGpx(@RequestPart(name = "file") @Valid @NotNull @NotEmpty MultipartFile file, 
			HttpServletRequest request, HttpSession httpSession) throws IOException {
		GPXSessionContext ctx = session.getGpxResources(httpSession);
		File tmpGpx = File.createTempFile("gpx_" + httpSession.getId(), ".gpx");

		double fileSizeMb = file.getSize() / (double) (1 << 20);
		double filesSize = getCommonSavedFilesSize(ctx.files);
		double maxSizeMb = getCommonMaxSizeFiles();

		if (fileSizeMb + filesSize > maxSizeMb) {
			return ResponseEntity.badRequest()
					.body(String.format(
							"You don't have enough cloud space to store this file!" 
									+ "\nUploaded file size: %.1f MB."
									+ "\nMax cloud space: %.0f MB."  
									+ "\nAvailable free space: %.1f MB.",
							fileSizeMb, maxSizeMb, maxSizeMb - filesSize));
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
			gpxService.cleanupFromNan(analysis);
			sessionFile.analysis = analysis;
			GPXFile srtmGpx = gpxService.calculateSrtmAltitude(gpxFile, null);
			GPXTrackAnalysis srtmAnalysis = null;
			if (srtmGpx != null) {
				srtmAnalysis = srtmGpx.getAnalysis(System.currentTimeMillis());
			}
			sessionFile.srtmAnalysis = srtmAnalysis;
			if (srtmAnalysis != null) {
				gpxService.cleanupFromNan(srtmAnalysis);
			}
			return ResponseEntity.ok(gson.toJson(Map.of("info", sessionFile)));
		}
	}
	
	@PostMapping(path = {"/get-gpx-analysis"}, produces = "application/json")
	@ResponseBody
	public ResponseEntity<String> getGpxInfo(@RequestPart(name = "file") @Valid @NotNull @NotEmpty MultipartFile file,
	                                         HttpServletRequest request, HttpSession httpSession) throws IOException {
		
		File tmpGpx = File.createTempFile("gpx_" + httpSession.getId(), ".gpx");
		InputStream is = file.getInputStream();
		FileOutputStream fous = new FileOutputStream(tmpGpx);
		Algorithms.streamCopy(is, fous);
		is.close();
		fous.close();
		
		GPXFile gpxFile = GPXUtilities.loadGPXFile(tmpGpx);
		if (gpxFile.error != null) {
			return ResponseEntity.badRequest().body("Error reading gpx!");
		} else {
			GPXSessionFile sessionFile = new GPXSessionFile();
			gpxFile.path = file.getOriginalFilename();
			GPXTrackAnalysis analysis = gpxFile.getAnalysis(System.currentTimeMillis());
			sessionFile.file = tmpGpx;
			sessionFile.size = file.getSize() / (double) (1 << 20);
			gpxService.cleanupFromNan(analysis);
			sessionFile.analysis = analysis;
			GPXFile srtmGpx = gpxService.calculateSrtmAltitude(gpxFile, null);
			GPXTrackAnalysis srtmAnalysis = null;
			if (srtmGpx != null) {
				srtmAnalysis = srtmGpx.getAnalysis(System.currentTimeMillis());
			}
			sessionFile.srtmAnalysis = srtmAnalysis;
			if (srtmAnalysis != null) {
				gpxService.cleanupFromNan(srtmAnalysis);
			}
			return ResponseEntity.ok(gson.toJson(Map.of("info", sessionFile)));
		}
	}
	
	@PostMapping(path = {"/process-track-data"}, produces = "application/json")
	@ResponseBody
	public ResponseEntity<String> processTrackData(@RequestPart(name = "file") @Valid @NotNull @NotEmpty MultipartFile file,
	                                               HttpSession httpSession) throws IOException {
		
		File tmpGpx = File.createTempFile("gpx_" + httpSession.getId(), ".gpx");
		
		InputStream is = file.getInputStream();
		FileOutputStream fous = new FileOutputStream(tmpGpx);
		Algorithms.streamCopy(is, fous);
		is.close();
		fous.close();
		session.getGpxResources(httpSession).tempFiles.add(tmpGpx);
		
		GPXFile gpxFile = GPXUtilities.loadGPXFile(tmpGpx);
		
		if (gpxFile.error != null) {
			return ResponseEntity.badRequest().body("Error reading gpx!");
		} else {
			WebGpxParser.TrackData gpxData = gpxService.getTrackDataByGpxFile(gpxFile, tmpGpx);
			return ResponseEntity.ok(gsonWithNans.toJson(Map.of("gpx_data", gpxData)));
		}
	}
	
	@PostMapping(path = {"/save-track-data"}, produces = "application/json")
	@ResponseBody
	public ResponseEntity<InputStreamResource> saveTrackData(@RequestBody String data,
	                                                         HttpSession httpSession) throws IOException {
		WebGpxParser.TrackData trackData = new Gson().fromJson(data, WebGpxParser.TrackData.class);
		
		GPXFile gpxFile = webGpxParser.createGpxFileFromTrackData(trackData);
		File tmpGpx = File.createTempFile("gpx_" + httpSession.getId(), ".gpx");
		InputStreamResource resource = new InputStreamResource(new FileInputStream(tmpGpx));
		Exception exception = GPXUtilities.writeGpxFile(tmpGpx, gpxFile);
		if (exception != null) {
			return ResponseEntity.badRequest().body(resource);
		}
		
		String fileName = gpxFile.metadata.name != null ? gpxFile.metadata.name : tmpGpx.getName();
		
		return ResponseEntity.ok()
				.header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=" + fileName)
				.contentType(MediaType.APPLICATION_XML)
				.body(resource);
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
}
