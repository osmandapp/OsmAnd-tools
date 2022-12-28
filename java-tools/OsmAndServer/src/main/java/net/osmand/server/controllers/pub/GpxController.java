package net.osmand.server.controllers.pub;


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import javax.validation.Valid;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import javax.xml.stream.FactoryConfigurationError;
import javax.xml.stream.XMLStreamException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.InputStreamResource;
import org.springframework.core.io.Resource;
import org.springframework.http.ContentDisposition;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;
import org.xmlpull.v1.XmlPullParserException;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import net.osmand.IProgress;
import net.osmand.IndexConstants;
import net.osmand.gpx.GPXFile;
import net.osmand.gpx.GPXTrackAnalysis;
import net.osmand.gpx.GPXUtilities;
import net.osmand.gpx.GPXUtilities.Elevation;
import net.osmand.gpx.GPXUtilities.Speed;
import net.osmand.gpx.GPXUtilities.Track;
import net.osmand.gpx.GPXUtilities.TrkSegment;
import net.osmand.gpx.GPXUtilities.WptPt;
import net.osmand.obf.OsmGpxWriteContext;
import net.osmand.obf.OsmGpxWriteContext.QueryParams;
import net.osmand.obf.preparation.IndexHeightData;
import net.osmand.server.WebSecurityConfiguration.OsmAndProUser;
import net.osmand.server.api.services.OsmAndMapsService;
import net.osmand.server.controllers.pub.UserSessionResources.GPXSessionContext;
import net.osmand.server.controllers.pub.UserSessionResources.GPXSessionFile;
import net.osmand.server.utils.WebGpxParser;
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
		Iterator<Speed> itS = analysis.speedData.iterator();
		float sumDist = 0;
		while (itS.hasNext()) {
			Speed sp = itS.next();
			if (Float.isNaN(sp.speed)) {
				sumDist += sp.distance;
				itS.remove();
			} else if (sumDist > 0) {
				sp.distance += sumDist;
				sumDist = 0;
			}
		}
		Iterator<Elevation> itE = analysis.elevationData.iterator();
		sumDist = 0;
		while (itE.hasNext()) {
			Elevation e = itE.next();
			if (Float.isNaN(e.elevation)) {
				sumDist += e.distance;
				itE.remove();
			} else if (sumDist > 0) {
				e.distance += sumDist;
				sumDist = 0;
			}
		}
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
	
	@PostMapping(path = {"/process-srtm"}, produces = "application/json")
	public ResponseEntity<StreamingResponseBody> attachSrtm(@RequestPart(name = "file") @Valid @NotNull @NotEmpty MultipartFile file) throws IOException {
		GPXFile gpxFile = GPXUtilities.loadGPXFile(file.getInputStream());
		final StringBuilder err = new StringBuilder(); 
		if (srtmLocation == null) {
			err.append(String.format("Server is not configured for srtm processing. "));
		}
		GPXFile srtmGpx = calculateSrtmAltitude(gpxFile, null);
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
			cleanupFromNan(analysis);
			sessionFile.analysis = analysis;
			GPXFile srtmGpx = calculateSrtmAltitude(gpxFile, null);
			GPXTrackAnalysis srtmAnalysis = null;
			if (srtmGpx != null) {
				srtmAnalysis = srtmGpx.getAnalysis(System.currentTimeMillis());
			}
			sessionFile.srtmAnalysis = srtmAnalysis;
			if (srtmAnalysis != null) {
				cleanupFromNan(srtmAnalysis);
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
			cleanupFromNan(analysis);
			sessionFile.analysis = analysis;
			GPXFile srtmGpx = calculateSrtmAltitude(gpxFile, null);
			GPXTrackAnalysis srtmAnalysis = null;
			if (srtmGpx != null) {
				srtmAnalysis = srtmGpx.getAnalysis(System.currentTimeMillis());
			}
			sessionFile.srtmAnalysis = srtmAnalysis;
			if (srtmAnalysis != null) {
				cleanupFromNan(srtmAnalysis);
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
			WebGpxParser.TrackData gpxData = new WebGpxParser.TrackData();
			
			gpxData.metaData = new WebGpxParser.MetaData(gpxFile.metadata);
			gpxData.wpts = webGpxParser.getWpts(gpxFile);
			gpxData.pointsGroups = webGpxParser.getPointsGroups(gpxFile);
			gpxData.tracks = webGpxParser.getTracks(gpxFile);
			gpxData.ext = gpxFile.extensions;
			
			if (!gpxFile.routes.isEmpty()) {
				webGpxParser.addRoutePoints(gpxFile, gpxData);
			}
			GPXFile gpxFileForAnalyse = GPXUtilities.loadGPXFile(tmpGpx);
			GPXTrackAnalysis analysis = getAnalysis(gpxFileForAnalyse, false);
			GPXTrackAnalysis srtmAnalysis = getAnalysis(gpxFileForAnalyse, true);
			gpxData.analysis = webGpxParser.getTrackAnalysis(analysis, srtmAnalysis);
			
			if (!gpxData.tracks.isEmpty()) {
				webGpxParser.addSrtmEle(gpxData.tracks, srtmAnalysis);
				webGpxParser.addDistance(gpxData.tracks, analysis);
			}
			
			return ResponseEntity.ok(gsonWithNans.toJson(Map.of("gpx_data", gpxData)));
		}
	}
	
	private GPXTrackAnalysis getAnalysis(GPXFile gpxFile, boolean isSrtm) {
		GPXTrackAnalysis analysis = null;
		if (!isSrtm) {
			analysis = gpxFile.getAnalysis(System.currentTimeMillis());
		} else {
			GPXFile srtmGpx = calculateSrtmAltitude(gpxFile, null);
			if (srtmGpx != null) {
				analysis = srtmGpx.getAnalysis(System.currentTimeMillis());
			}
		}
		if (analysis != null) {
			cleanupFromNan(analysis);
		}
		return analysis;
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
	
	
	public GPXFile calculateSrtmAltitude(GPXFile gpxFile, File[] missingFile) {
		if (srtmLocation == null ) {
			return null;
		}
		if (srtmLocation.startsWith("http://") || srtmLocation.startsWith("https://")) {
			String serverUrl = srtmLocation + "/gpx/process-srtm";
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			GPXUtilities.writeGpx(new OutputStreamWriter(baos), gpxFile, null);
			
			HttpHeaders headers = new HttpHeaders();
			headers.setContentType(MediaType.MULTIPART_FORM_DATA);
			
	        MultiValueMap<String, String> fileMap = new LinkedMultiValueMap<>();
	        ContentDisposition contentDisposition = ContentDisposition.builder("form-data").name("file")
					.filename("route.gpx").build();
	        fileMap.add(HttpHeaders.CONTENT_DISPOSITION, contentDisposition.toString());
	        HttpEntity<byte[]> fileEntity = new HttpEntity<>(baos.toByteArray(), fileMap);
	        
			MultiValueMap<String, Object> body = new LinkedMultiValueMap<>();
	        body.add("file", fileEntity);

			HttpEntity<MultiValueMap<String, Object>> requestEntity = new HttpEntity<>(body, headers);
			RestTemplate restTemplate = new RestTemplate();
			try {
				ResponseEntity<byte[]> response = restTemplate.postForEntity(serverUrl, requestEntity, byte[].class);
				if (response.getStatusCode().is2xxSuccessful()) {
					return GPXUtilities.loadGPXFile(new ByteArrayInputStream(response.getBody()));
				}
			} catch (RestClientException e) {
				LOGGER.error(e.getMessage(), e);
			}
			return null;
		} else {
			File srtmFolder = new File(srtmLocation);
			if (!srtmFolder.exists()) {
				return null;
			}
			IndexHeightData hd = new IndexHeightData();
			hd.setSrtmData(srtmFolder.getAbsolutePath(), srtmFolder);
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
		}
		return gpxFile;
	}

}
