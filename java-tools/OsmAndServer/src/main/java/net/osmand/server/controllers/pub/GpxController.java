package net.osmand.server.controllers.pub;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
import org.xmlpull.v1.XmlPullParserException;

import com.google.gson.Gson;

import net.osmand.GPXUtilities;
import net.osmand.GPXUtilities.GPXFile;
import net.osmand.GPXUtilities.GPXTrackAnalysis;
import net.osmand.GPXUtilities.Track;
import net.osmand.GPXUtilities.TrkSegment;
import net.osmand.GPXUtilities.WptPt;
import net.osmand.IProgress;
import net.osmand.IndexConstants;
import net.osmand.binary.MapZooms;
import net.osmand.obf.preparation.IndexCreator;
import net.osmand.obf.preparation.IndexCreatorSettings;
import net.osmand.osm.MapRenderingTypesEncoder;
import net.osmand.osm.edit.Node;
import net.osmand.osm.edit.Way;
import net.osmand.osm.io.OsmStorageWriter;
import net.osmand.server.controllers.pub.UserSessionResources.GPXSessionContext;
import net.osmand.util.Algorithms;


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
			@RequestParam(required = true) String gpxContent) throws IOException {
		GPXFile gpxFile = GPXUtilities.loadGPXFile(new ByteArrayInputStream(gpxContent.getBytes()));
		GPXSessionContext ctx = session.getGpxResources(httpSession);
		if (gpxFile != null) {
			GPXTrackAnalysis analysis = gpxFile.getAnalysis(System.currentTimeMillis());
			ctx.files.add(gpxFile);
			ctx.analysis.add(analysis);
		}
		return gson.toJson(GPXSessionInfo.getInfo(ctx));
	}
	

	
	
	@RequestMapping(path = { "/download-obf"})
	@ResponseBody
    public ResponseEntity<Resource> indexesPhp(@RequestParam(defaultValue="", required=false) String gzip,
    		 HttpSession httpSession, HttpServletResponse resp) throws IOException, FactoryConfigurationError, XMLStreamException, SQLException, InterruptedException, XmlPullParserException {
		GPXSessionContext ctx = session.getGpxResources(httpSession);
		File tmpOsm = File.createTempFile("gpx_obf_" + httpSession.getId(), ".osm");
		ctx.tempFiles.add(tmpOsm);
		FileOutputStream fous = new FileOutputStream(tmpOsm);
		try {
			List<Node> nodes = new ArrayList<>();
			List<Way> ways = new ArrayList<>();
			long id = -1;
			for (GPXFile f : ctx.files) {
				id = createOsmFromGPX(nodes, ways, id, f);
			}
			OsmStorageWriter osmWriter = new OsmStorageWriter();
			osmWriter.writeOSM(fous, null, nodes, ways, Collections.emptyList());
		} finally {
			fous.close();
		}
		
        
        IndexCreatorSettings settings = new IndexCreatorSettings();
		settings.indexMap = true;
		settings.indexAddress = false;
		settings.indexPOI = true;
		settings.indexTransport = false;
		settings.indexRouting = false;
		String sessionId = httpSession.getId();
		File folder = new File(tmpOsm.getParentFile(), sessionId);
		String fileName = "gpx_" + sessionId;
		File targetObf = new File(folder.getParentFile(), fileName + IndexConstants.BINARY_MAP_INDEX_EXT);
		try {
			folder.mkdirs();
			IndexCreator ic = new IndexCreator(folder, settings);
			MapRenderingTypesEncoder types = new MapRenderingTypesEncoder(null, fileName);
			ic.setMapFileName(fileName);
			// IProgress.EMPTY_PROGRESS
			ic.generateIndexes(tmpOsm, IProgress.EMPTY_PROGRESS, null, MapZooms.getDefault(), types, null);
			new File(folder, ic.getMapFileName()).renameTo(targetObf);
			ctx.tempFiles.add(targetObf);
		} finally {
			Algorithms.removeAllFiles(folder);
		}
		HttpHeaders headers = new HttpHeaders();
        headers.add(HttpHeaders.CONTENT_DISPOSITION, String.format("attachment; filename=\"gpx.obf\""));
        headers.add(HttpHeaders.CONTENT_TYPE, "application/octet-binary");
		return  ResponseEntity.ok().headers(headers).body(new FileSystemResource(targetObf));
	}

	private long createOsmFromGPX(List<Node> nodes, List<Way> ways, long id, GPXFile f) {
		for (Track t : f.tracks) {
			for (TrkSegment s : t.segments) {
				Way w = new Way(id--);
				w.putTag("gpx", "segment");
				for (WptPt p : s.points) {
					Node n = createPoint(id--, p);
					w.addNode(n);
					nodes.add(n);
				}
				int color = s.getColor(0);
				if(color != 0) {
					w.putTag("color", MapRenderingTypesEncoder.formatColorToPalette(Algorithms.colorToString(color), false));
				}
				ways.add(w);
			}
		}

		for (WptPt p : f.getPoints()) {
			Node n = createPoint(id--, p);
			n.putTag("gpx", "point");
			nodes.add(n);
		}
		return id;
	}

	private Node createPoint(long id, WptPt p) {
		Node n = new Node(p.lat, p.lon, id);
		if (!Algorithms.isEmpty(p.name)) {
			n.putTag("name", p.name);
		}
		if (!Algorithms.isEmpty(p.desc)) {
			n.putTag("description", p.desc);
		}
		if (!Algorithms.isEmpty(p.category)) {
			n.putTag("category", p.category);
		}
		if (!Algorithms.isEmpty(p.comment)) {
			n.putTag("comment", p.comment);
		}
		if (!Algorithms.isEmpty(p.link)) {
			n.putTag("link", p.link);
		}
		if (!Algorithms.isEmpty(p.getIconName())) {
			n.putTag("icon", p.getIconName());
		}
		if (!Algorithms.isEmpty(p.getBackgroundType())) {
			n.putTag("bg", p.getBackgroundType());
		}
		int color = p.getColor(0);
		if(color != 0) {
			n.putTag("color", MapRenderingTypesEncoder.formatColorToPalette(Algorithms.colorToString(color), false));
		}
		if (!Double.isNaN(p.ele)) {
			n.putTag("ele", p.ele + "");
		}
		if (!Double.isNaN(p.speed)) {
			n.putTag("speed", p.speed + "");
		}
		return n;
	}
	

}
