package net.osmand.server.controllers.pub;



import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpSession;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

import net.osmand.shared.gpx.GpxTrackAnalysis;
import net.osmand.shared.io.KFile;
import net.osmand.shared.util.IProgress;
import net.osmand.shared.gpx.GpxFile;
import net.osmand.shared.gpx.GpxUtilities;
import okio.Buffer;
import okio.Okio;
import okio.Source;
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
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import net.osmand.server.api.services.GpxService;
import net.osmand.server.controllers.pub.UserSessionResources.GPXSessionContext;
import net.osmand.server.controllers.pub.UserSessionResources.GPXSessionFile;
import net.osmand.server.utils.WebGpxParser;

import static net.osmand.shared.IndexConstants.GPX_FILE_PREFIX;


@RestController
@RequestMapping("/gpx/")
public class GpxController {
    
	protected static final Log LOGGER = LogFactory.getLog(GpxController.class);

	Gson gson = new Gson();
	
	Gson gsonWithNans = new GsonBuilder().serializeSpecialFloatingPointValues().create();
	
	@Autowired
	WebGpxParser webGpxParser;
	
	@Autowired
	UserSessionResources session;
	
	@Autowired
	protected GpxService gpxService;
	
	@Value("${osmand.srtm.location}")
	String srtmLocation;
	
	@PostMapping(path = {"/clear"}, produces = "application/json")
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
	public String getGpx(HttpSession httpSession) {
		GPXSessionContext ctx = session.getGpxResources(httpSession);
		return gson.toJson(Map.of("all", ctx.files));
	}
	
	@GetMapping(path = {"/get-gpx-file"}, produces = "application/json")
	public ResponseEntity<Resource> getGpx(@RequestParam String name, HttpSession httpSession) {
		GPXSessionContext ctx = session.getGpxResources(httpSession);
		File tmpGpx = null;
		for (int i = 0; i < ctx.files.size(); i++) {
			GPXSessionFile file = ctx.files.get(i);
			if (file.analysis != null) {
				String fileName = file.analysis.getName();
				if (fileName != null && fileName.equals(name)) {
					tmpGpx = file.file;
				}
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
		final StringBuilder err = new StringBuilder();
		GpxFile gpxFile = null;
		InputStream in = file.getInputStream();
		if (gpxService.isGzipStream(in)) {
			in = new GZIPInputStream(in);
		}
		try (Source source = new Buffer().readFrom(in)) {
			gpxFile = GpxUtilities.INSTANCE.loadGpxFile(source);
		} catch (IOException e) {
			err.append("error reading gpx file ");
		}
		if (gpxFile == null || gpxFile.getError() != null) {
			err.append("loadGPXFile error (process-srtm) ");
		}
		if (srtmLocation == null) {
			err.append("Server is not configured for srtm processing. ");
		}
		GpxFile srtmGpx = gpxService.calculateSrtmAltitude(gpxFile, null);
		if (srtmGpx == null) {
			err.append(String.format(String.format("Couldn't calculate altitude for %s (%d KB)",
					file.getName(), file.getSize() / 1024L)));
		}
		StreamingResponseBody responseBody = outputStream -> {
			OutputStreamWriter ouw = new OutputStreamWriter(outputStream);
			if (!err.isEmpty()) {
				ouw.write(err.toString());
			} else {
				Exception exception = GpxUtilities.INSTANCE.writeGpx(null, Okio.buffer(Okio.sink(outputStream)), srtmGpx, IProgress.Companion.getEMPTY_PROGRESS());
				if (exception != null) {
					ouw.write("Error writing gpx file: " + exception.getMessage());
				}
			}
			ouw.close();
		};
	    if (!err.isEmpty()) {
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
		double fileSizeMb = file.getSize() / (double) (1 << 20);
		double filesSize = getCommonSavedFilesSize(ctx.files);
		double maxSizeMb = gpxService.getCommonMaxSizeFiles();

		if (fileSizeMb + filesSize > maxSizeMb) {
			return ResponseEntity.badRequest()
					.body(String.format(
							"You don't have enough cloud space to store this file!" 
									+ "\nUploaded file size: %.1f MB."
									+ "\nMax cloud space: %.0f MB."  
									+ "\nAvailable free space: %.1f MB.",
							fileSizeMb, maxSizeMb, maxSizeMb - filesSize));
		}

		File tmpGpx = gpxService.saveMultipartFileToTemp(file, httpSession.getId());
		ctx.tempFiles.add(tmpGpx);
		GpxFile gpxFile = GpxUtilities.INSTANCE.loadGpxFile(Okio.source(tmpGpx));
		if (gpxFile.getError() != null) {
			return ResponseEntity.badRequest().body("Error reading gpx!");
		} else {
			GPXSessionFile sessionFile = new GPXSessionFile();
			ctx.files.add(sessionFile);
			gpxFile.setPath(file.getOriginalFilename());
			GpxTrackAnalysis analysis = gpxFile.getAnalysis(System.currentTimeMillis());
			sessionFile.file = tmpGpx;
			sessionFile.size = fileSizeMb;
			gpxService.cleanupFromNan(analysis);
			sessionFile.analysis = analysis;
			GpxFile srtmGpx = gpxService.calculateSrtmAltitude(gpxFile, null);
			GpxTrackAnalysis srtmAnalysis = null;
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
	public ResponseEntity<String> getGpxInfo(@RequestPart(name = "file") @Valid @NotNull @NotEmpty MultipartFile file,
	                                         HttpServletRequest request, HttpSession httpSession) throws IOException {

		File tmpGpx = gpxService.saveMultipartFileToTemp(file, httpSession.getId());
		GpxFile gpxFile = GpxUtilities.INSTANCE.loadGpxFile(Okio.source(tmpGpx));
		if (gpxFile.getError() != null) {
			return ResponseEntity.badRequest().body("Error reading gpx!");
		} else {
			GPXSessionFile sessionFile = new GPXSessionFile();
			gpxFile.setPath(file.getOriginalFilename());
			GpxTrackAnalysis analysis = gpxFile.getAnalysis(System.currentTimeMillis());
			sessionFile.file = tmpGpx;
			sessionFile.size = file.getSize() / (double) (1 << 20);
			gpxService.cleanupFromNan(analysis);
			sessionFile.analysis = analysis;
			GpxFile srtmGpx = gpxService.calculateSrtmAltitude(gpxFile, null);
			GpxTrackAnalysis srtmAnalysis = null;
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
	public ResponseEntity<String> processTrackData(@RequestPart(name = "file") @Valid @NotNull @NotEmpty MultipartFile file,
	                                               HttpSession httpSession) throws IOException {

		String filename = file.getOriginalFilename();
		File tmpFile = gpxService.saveMultipartFileToTemp(file, httpSession.getId());
		session.getGpxResources(httpSession).tempFiles.add(tmpFile);
		GpxFile gpxFile = gpxService.importGpx(Okio.source(tmpFile), filename);
		if (gpxFile.getError() != null) {
			LOGGER.error(String.format(
					"process-track-data loadGpxFile (%s) error (%s)", filename, gpxFile.getError().getMessage()));
			return ResponseEntity.badRequest().body("Error reading gpx: " + gpxFile.getError().getMessage());
		} else {
			WebGpxParser.TrackData gpxData = gpxService.buildTrackDataFromGpxFile(gpxFile, null);
			return ResponseEntity.ok(gsonWithNans.toJson(Map.of("gpx_data", gpxData)));
		}
	}

	@PostMapping(path = "/save-track-data", produces = MediaType.APPLICATION_XML_VALUE)
	public ResponseEntity<StreamingResponseBody> saveTrackData(@RequestBody byte[] data,
															   @RequestParam Boolean simplified,
	                                                           HttpSession httpSession) throws IOException {
		String jsonData = decompressGzip(data);
		WebGpxParser.TrackData trackData = new Gson().fromJson(jsonData, WebGpxParser.TrackData.class);

		GpxFile gpxFile = webGpxParser.createGpxFileFromTrackData(trackData);
		if (simplified != null && simplified) {
			gpxFile = gpxService.createSimplifiedGpxFile(gpxFile);
		}
		File tmpGpx = File.createTempFile(GPX_FILE_PREFIX + httpSession.getId(), ".gpx");

		Exception exception = GpxUtilities.INSTANCE.writeGpxFile(new KFile(tmpGpx.getAbsolutePath()), gpxFile);
		if (exception != null) {
			Files.deleteIfExists(tmpGpx.toPath());
			return ResponseEntity.badRequest().build();
		}

		final Path path = tmpGpx.toPath();
		final long len = Files.size(path);

		if (len == 0) {
			Files.deleteIfExists(path);
			return ResponseEntity.badRequest().build();
		}

		StreamingResponseBody body = outputStream -> {
			try (InputStream in = Files.newInputStream(path)) {
				in.transferTo(outputStream);
				outputStream.flush();
			} catch (IOException ignore) {
				// client aborted
			} finally {
				Files.deleteIfExists(path);
			}
		};

		return ResponseEntity.ok()
				.contentType(MediaType.APPLICATION_XML)
				.contentLength(len)
				.body(body);
	}

	@RequestMapping(path = {"/get-srtm-data"}, produces = "application/json")
	public ResponseEntity<String> getSrtmData(@RequestBody byte[] data) throws IOException {
		String jsonData = decompressGzip(data);
		WebGpxParser.TrackData trackData = gson.fromJson(jsonData, WebGpxParser.TrackData.class);
		trackData = gpxService.addSrtmData(trackData);
		
		return ResponseEntity.ok(gsonWithNans.toJson(Map.of("data", trackData)));
	}

	@RequestMapping(path = {"/get-analysis"}, produces = "application/json")
	public ResponseEntity<String> getAnalysis(@RequestBody byte[] data) throws IOException {
		String jsonData = decompressGzip(data);
		WebGpxParser.TrackData trackData = gson.fromJson(jsonData, WebGpxParser.TrackData.class);
		trackData = gpxService.addAnalysisData(trackData);

		return ResponseEntity.ok(gsonWithNans.toJson(Map.of("data", trackData)));
	}

	private static String decompressGzip(byte[] compressed) throws IOException {
		try (GZIPInputStream gis = new GZIPInputStream(new ByteArrayInputStream(compressed));
		     ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
			byte[] buffer = new byte[4096];
			int len;
			while ((len = gis.read(buffer)) != -1) {
				baos.write(buffer, 0, len);
			}
			return baos.toString(StandardCharsets.UTF_8);
		}
	}

    private double getCommonSavedFilesSize(List<GPXSessionFile> files) {
	    double sizeFiles = 0L;
        for (GPXSessionFile file: files) {
            sizeFiles += file.size;
        }
        return sizeFiles;
    }
}
