package net.osmand.server.controllers.pub;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import javax.xml.stream.FactoryConfigurationError;
import javax.xml.stream.XMLStreamException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
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
import net.osmand.IProgress;
import net.osmand.IndexConstants;
import net.osmand.binary.MapZooms;
import net.osmand.obf.preparation.IndexCreator;
import net.osmand.obf.preparation.IndexCreatorSettings;
import net.osmand.osm.MapRenderingTypesEncoder;
import net.osmand.server.controllers.pub.UserSessionResources.GPXSessionContext;
import net.osmand.server.osmgpx.DownloadOsmGPX.QueryParams;
import net.osmand.server.osmgpx.OsmGpxWriteContext;
import net.osmand.util.Algorithms;
import rtree.RTree;


@RestController
@RequestMapping("/gpx/")
public class GpxController {
	

	Gson gson = new Gson();
	
	@Autowired
	UserSessionResources session;
	
	public static class GPXSessionInfo {
		public double totalDist;
		public int trkPoints;
		public int waypoints;
		public int files;
		
		public static GPXSessionInfo getInfo(GPXSessionContext c) {
			GPXSessionInfo info = new GPXSessionInfo();
			info.files = c.files.size();
			for(int i = 0; i < c.files.size(); i++ ) {
				GPXTrackAnalysis analysis = c.analysis.get(i);
				info.totalDist += analysis.totalDistance;
				info.trkPoints += analysis.points;
				info.waypoints += analysis.wptPoints;
			}
			return info;
		}
	}
	
	
	
	@PostMapping(path = {"/clear"}, produces = "application/json")
	@ResponseBody
	public String clear(HttpServletRequest request, HttpSession httpSession) throws IOException {
		GPXSessionContext ctx = session.getGpxResources(httpSession);
		for (File f : ctx.tempFiles) {
			f.delete();
		}
		ctx.tempFiles.clear();
		ctx.files.clear();
		ctx.analysis.clear();
		return gson.toJson(GPXSessionInfo.getInfo(ctx));
	}
	
	@GetMapping(path = {"/get-gpx-info"}, produces = "application/json")
	@ResponseBody
	public String getGpx(HttpServletRequest request, HttpSession httpSession) throws IOException {
		GPXSessionContext ctx = session.getGpxResources(httpSession);
		return gson.toJson(GPXSessionInfo.getInfo(ctx));
	}
	
	@PostMapping(path = {"/upload-session-gpx"}, produces = "application/json")
	@ResponseBody
	public String uploadGpx(HttpServletRequest request, HttpSession httpSession,
			@RequestParam(required = true) MultipartFile file) throws IOException {
		GPXSessionContext ctx = session.getGpxResources(httpSession);
		File tmpGpx = File.createTempFile("gpx_" + httpSession.getId(), ".gpx");
		ctx.tempFiles.add(tmpGpx);
		InputStream is = file.getInputStream();
		FileOutputStream fous = new FileOutputStream(tmpGpx);
		Algorithms.streamCopy(is, fous);
		is.close();
		fous.close();
		GPXFile gpxFile = GPXUtilities.loadGPXFile(tmpGpx);
		if (gpxFile != null) {
			GPXTrackAnalysis analysis = gpxFile.getAnalysis(System.currentTimeMillis());
			ctx.files.add(tmpGpx);
			ctx.analysis.add(analysis);
		}
		return gson.toJson(GPXSessionInfo.getInfo(ctx));
	}
	

	
	
	@RequestMapping(path = { "/download-obf"})
	@ResponseBody
    public ResponseEntity<Resource> downloadObf(@RequestParam(defaultValue="", required=false) String gzip,
			HttpSession httpSession, HttpServletResponse resp) throws IOException, FactoryConfigurationError,
			XMLStreamException, SQLException, InterruptedException, XmlPullParserException {
		GPXSessionContext ctx = session.getGpxResources(httpSession);
		File tmpOsm = File.createTempFile("gpx_obf_" + httpSession.getId(), ".osm.gz");
		QueryParams qp = new QueryParams();
		qp.osmFile = tmpOsm;
		qp.details = QueryParams.DETAILS_ELE_SPEED;
		ctx.tempFiles.add(tmpOsm);
		OsmGpxWriteContext writeCtx = new OsmGpxWriteContext(qp);
		writeCtx.startDocument();
		for (File gf : ctx.files) {
			GPXFile f = GPXUtilities.loadGPXFile(gf);
			GPXTrackAnalysis analysis = f.getAnalysis(gf.lastModified());
			writeCtx.writeTrack(null, null, f, analysis, "GPX");
		}
		writeCtx.endDocument();

		IndexCreatorSettings settings = new IndexCreatorSettings();
		settings.indexMap = true;
		settings.indexAddress = false;
		settings.indexPOI = true;
		settings.indexTransport = false;
		settings.indexRouting = false;
		String sessionId = httpSession.getId();
		RTree.clearCache();
		File folder = new File(tmpOsm.getParentFile(), sessionId);
		String fileName = "gpx_" + sessionId;
		File targetObf = new File(folder.getParentFile(), fileName + IndexConstants.BINARY_MAP_INDEX_EXT);
		try {
			folder.mkdirs();
			IndexCreator ic = new IndexCreator(folder, settings);
			MapRenderingTypesEncoder types = new MapRenderingTypesEncoder(null, fileName);
			ic.setMapFileName(fileName);
			// IProgress.EMPTY_PROGRESS
			IProgress prog = IProgress.EMPTY_PROGRESS;
			// prog = new ConsoleProgressImplementation();
			ic.generateIndexes(tmpOsm, prog, null, MapZooms.getDefault(), types, null);
			new File(folder, ic.getMapFileName()).renameTo(targetObf);
			ctx.tempFiles.add(targetObf);
		} finally {
			Algorithms.removeAllFiles(folder);
		}
		HttpHeaders headers = new HttpHeaders();
		headers.add(HttpHeaders.CONTENT_DISPOSITION, String.format("attachment; filename=\"gpx.obf\""));
		headers.add(HttpHeaders.CONTENT_TYPE, "application/octet-binary");
		return ResponseEntity.ok().headers(headers).body(new FileSystemResource(targetObf));
	}

	

	
	

}
