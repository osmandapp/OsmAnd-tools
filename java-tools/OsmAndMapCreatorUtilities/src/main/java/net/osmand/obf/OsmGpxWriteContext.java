package net.osmand.obf;

import static net.osmand.IndexConstants.BINARY_MAP_INDEX_EXT;
import static net.osmand.IndexConstants.GPX_FILE_EXT;
import static net.osmand.obf.preparation.IndexRouteRelationCreator.DIST_STEP;
import static net.osmand.obf.preparation.IndexRouteRelationCreator.MAX_GRAPH_SKIP_POINTS_BITS;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.zip.GZIPOutputStream;

import org.xmlpull.v1.XmlPullParserException;
import org.xmlpull.v1.XmlSerializer;

import net.osmand.IProgress;
import net.osmand.PlatformUtil;
import net.osmand.binary.MapZooms;
import net.osmand.data.QuadRect;
import net.osmand.gpx.GPXFile;
import net.osmand.gpx.GPXTrackAnalysis;
import net.osmand.gpx.GPXUtilities;
import net.osmand.gpx.GPXUtilities.Track;
import net.osmand.gpx.GPXUtilities.TrkSegment;
import net.osmand.gpx.GPXUtilities.WptPt;
import net.osmand.obf.preparation.IndexCreator;
import net.osmand.obf.preparation.IndexCreatorSettings;
import net.osmand.obf.preparation.IndexHeightData;
import net.osmand.osm.MapRenderingTypesEncoder;
import net.osmand.osm.RouteActivityType;
import net.osmand.util.Algorithms;
import net.osmand.util.MapAlgorithms;
import net.osmand.util.MapUtils;
import rtree.RTree;

public class OsmGpxWriteContext {
	private final static NumberFormat latLonFormat = new DecimalFormat("0.00#####", new DecimalFormatSymbols());
	public final QueryParams qp;
	public int tracks = 0;
	public int segments = 0;
	long id = -10;

	XmlSerializer serializer = null;
	OutputStream outputStream = null;

	public OsmGpxWriteContext(QueryParams qp) {
		this.qp = qp;
	}

	private boolean validatedTrackSegment(TrkSegment t) {
		boolean isOnePointIn = false;
		boolean testPoints = qp.minlat != OsmGpxFile.ERROR_NUMBER || qp.minlon != OsmGpxFile.ERROR_NUMBER
				|| qp.maxlat != OsmGpxFile.ERROR_NUMBER || qp.maxlon != OsmGpxFile.ERROR_NUMBER;
		for (WptPt p : t.points) {
			if (p.lat >= 90 || p.lat <= -90 || p.lon >= 180 || p.lon <= -180) {
				return false;
			}
			if (testPoints) {
				if (p.lat >= qp.minlat && p.lat <= qp.maxlat && p.lon >= qp.minlon && p.lon <= qp.maxlon) {
					isOnePointIn = true;
				}
			}
		}
		if (testPoints && !isOnePointIn) {
			return false;
		}
		return true;
	}

	public void startDocument() throws IllegalArgumentException, IllegalStateException, IOException {
		if (qp.osmFile != null) {
			outputStream = new FileOutputStream(qp.osmFile);
			if (qp.osmFile.getName().endsWith(".gz")) {
				outputStream = new GZIPOutputStream(outputStream);
			}
			serializer = PlatformUtil.newSerializer();
			serializer.setOutput(new BufferedWriter(new OutputStreamWriter(outputStream)));
			serializer.setFeature("http://xmlpull.org/v1/doc/features.html#indent-output", true); //$NON-NLS-1$
			serializer.startDocument("UTF-8", true); //$NON-NLS-1$
			serializer.startTag(null, "osm"); //$NON-NLS-1$
			serializer.attribute(null, "version", "0.6"); //$NON-NLS-1$ //$NON-NLS-2$
		}
	}
	

	public void writeTrack(OsmGpxFile gpxInfo, Map<String, String> extraTrackTags, GPXFile gpxFile, GPXTrackAnalysis analysis, 
			String routeIdPrefix)
			throws IOException, SQLException {
		Map<String, String> gpxTrackTags = new LinkedHashMap<String, String>();
		if (qp.details < QueryParams.DETAILS_TRACKS) {
			boolean validTrack = false;
			for (Track t : gpxFile.tracks) {
				for (TrkSegment s : t.segments) {
					if (s.points.isEmpty()) {
						continue;
					}
					if (!validatedTrackSegment(s)) {
						continue;
					}
					validTrack = true;
				}
			}
			if (validTrack) {
				serializer.startTag(null, "node");
				serializer.attribute(null, "id", id-- + "");
				serializer.attribute(null, "action", "modify");
				serializer.attribute(null, "version", "1");
				serializer.attribute(null, "lat", latLonFormat.format(gpxFile.findPointToShow().lat));
				serializer.attribute(null, "lon", latLonFormat.format(gpxFile.findPointToShow().lon));
				tagValue(serializer, "route", "segment");
				tagValue(serializer, "route_type", "track");
				tagValue(serializer, "route_radius", gpxFile.getOuterRadius());
				addGenericTags(gpxTrackTags, null);
				addGpxInfoTags(gpxTrackTags, gpxInfo, routeIdPrefix);
				addAnalysisTags(gpxTrackTags, analysis);
				seraizeTags(extraTrackTags, gpxTrackTags);
				serializer.endTag(null, "node");
			}
		} else {
			for (Track t : gpxFile.tracks) {
				for (TrkSegment s : t.segments) {
					if (s.points.isEmpty()) {
						continue;
					}
					if (!validatedTrackSegment(s)) {
						continue;
					}
					segments++;
					long idStart = id;
					double dlon = s.points.get(0).lon;
					double dlat = s.points.get(0).lat;
					QuadRect qr = new QuadRect(dlon, dlat, dlon, dlat);
					for (WptPt p : s.points) {
						long nid = id--;
						GPXUtilities.updateQR(qr, p, dlat, dlon);
						writePoint(nid, p, null, null, null);
					}
					long endid = id;
					serializer.startTag(null, "way");
					serializer.attribute(null, "id", id-- + "");
					serializer.attribute(null, "action", "modify");
					serializer.attribute(null, "version", "1");

					for (long nid = idStart; nid > endid; nid--) {
						serializer.startTag(null, "nd");
						serializer.attribute(null, "ref", nid + "");
						serializer.endTag(null, "nd");
					}
					tagValue(serializer, "route", "segment");
					tagValue(serializer, "route_type", "track");
					int radius = (int) MapUtils.getDistance(qr.bottom, qr.left, qr.top, qr.right);
					tagValue(serializer, "route_radius", MapUtils.convertDistToChar(radius, GPXUtilities.TRAVEL_GPX_CONVERT_FIRST_LETTER, GPXUtilities.TRAVEL_GPX_CONVERT_FIRST_DIST,
							GPXUtilities.TRAVEL_GPX_CONVERT_MULT_1, GPXUtilities.TRAVEL_GPX_CONVERT_MULT_2));
					addGenericTags(gpxTrackTags, t);
					addGpxInfoTags(gpxTrackTags, gpxInfo, routeIdPrefix);
					addAnalysisTags(gpxTrackTags, analysis);
					addElevationTags(gpxTrackTags, s);
					seraizeTags(extraTrackTags, gpxTrackTags);
					serializer.endTag(null, "way");
				}
			}

			for (WptPt p : gpxFile.getPoints()) {
				long nid = id--;
				if (gpxInfo != null) {
					writePoint(nid, p, "point", routeIdPrefix + gpxInfo.id, gpxInfo.name);
				}
			}
		}
		tracks++;
	}

	private void addElevationTags(Map<String, String> gpxTrackTags, TrkSegment s) {
		IndexHeightData.WayGeneralStats wgs = new IndexHeightData.WayGeneralStats();
		for (WptPt p : s.points) {
			wgs.altitudes.add(p.ele);
			wgs.dists.add(p.distance);
		}
		IndexHeightData.calculateEleStats(wgs, (int) DIST_STEP);
		if (wgs.eleCount > 0) {
			int st = (int) wgs.startEle;
			gpxTrackTags.put("start_ele", String.valueOf((int) wgs.startEle));
			gpxTrackTags.put("end_ele__start", String.valueOf((int) wgs.endEle - st));
			gpxTrackTags.put("avg_ele__start", String.valueOf((int) (wgs.sumEle / wgs.eleCount) - st));
			gpxTrackTags.put("min_ele__start", String.valueOf((int) wgs.minEle - st));
			gpxTrackTags.put("max_ele__start", String.valueOf((int) wgs.maxEle - st));
			gpxTrackTags.put("diff_ele_up", String.valueOf((int) wgs.up));
			gpxTrackTags.put("diff_ele_down", String.valueOf((int) wgs.down));
			gpxTrackTags.put("ele_graph", MapAlgorithms.encodeIntHeightArrayGraph(wgs.step, wgs.altIncs, MAX_GRAPH_SKIP_POINTS_BITS));
		}
	}

	private void seraizeTags(Map<String, String> extraTrackTags, Map<String, String> gpxTrackTags) throws IOException {
		if (extraTrackTags != null) {
			gpxTrackTags.putAll(extraTrackTags);
		}
		Iterator<Entry<String, String>> it = gpxTrackTags.entrySet().iterator();
		while (it.hasNext()) {
			Entry<String, String> e = it.next();
			tagValue(serializer, e.getKey(), e.getValue());
		}
	}

	public void endDocument() throws IOException {
		if(serializer != null) {
			serializer.endDocument();
			serializer.flush();
			outputStream.close();
		}		
	}
	

	private void addGenericTags(Map<String, String> gpxTrackTags, Track p) throws IOException {
		if (p != null) {
			if (!Algorithms.isEmpty(p.name)) {
				gpxTrackTags.put("name", p.name);
			}
			if (!Algorithms.isEmpty(p.desc)) {
				gpxTrackTags.put("description", p.desc);
			}
			int color = p.getColor(0);
			if (color != 0) {
				gpxTrackTags.put("color",
						MapRenderingTypesEncoder.formatColorToPalette(Algorithms.colorToString(color), false));
				gpxTrackTags.put("color_int", Algorithms.colorToString(color));
			}
		}
	}

	private void addGpxInfoTags(Map<String, String> gpxTrackTags, OsmGpxFile gpxInfo, String routeIdPrefix) {
		if (gpxInfo != null) {
			gpxTrackTags.put("route_id", routeIdPrefix + gpxInfo.id);
			gpxTrackTags.put("ref", gpxInfo.id % 1000 + "");
			gpxTrackTags.put("name", gpxInfo.name);
			gpxTrackTags.put("route_name", gpxInfo.name);
			gpxTrackTags.put("user", gpxInfo.user);
			gpxTrackTags.put("date", gpxInfo.timestamp.toString());
			gpxTrackTags.put("description", gpxInfo.description);
			RouteActivityType activityType = RouteActivityType.getTypeFromTags(gpxInfo.tags); 
			for (String tg : gpxInfo.tags) {
				gpxTrackTags.put("tag_" + tg, tg);
			}
			if (activityType != null) {
				// red, blue, green, orange, yellow
				// gpxTrackTags.put("gpx_icon", "");
				gpxTrackTags.put("gpx_bg", activityType.getColor() + "_hexagon_3_road_shield");
				gpxTrackTags.put("color", activityType.getColor());
				gpxTrackTags.put("route_activity_type", activityType.getName().toLowerCase());
			}
		}
	}
	

	private void addAnalysisTags(Map<String, String> gpxTrackTags, GPXTrackAnalysis analysis) throws IOException {
		gpxTrackTags.put("distance", latLonFormat.format(analysis.totalDistance));
		if (analysis.isTimeSpecified()) {
			gpxTrackTags.put("time_span", analysis.timeSpan + "");
			gpxTrackTags.put("time_span_no_gaps", analysis.timeSpanWithoutGaps + "");
			gpxTrackTags.put("time_moving", analysis.timeMoving + "");
			gpxTrackTags.put("time_moving_no_gaps", analysis.timeMovingWithoutGaps + "");
		}
		if (analysis.hasElevationData) {
			gpxTrackTags.put("avg_ele", latLonFormat.format(analysis.avgElevation));
			gpxTrackTags.put("min_ele", latLonFormat.format(analysis.minElevation));
			gpxTrackTags.put("max_ele", latLonFormat.format(analysis.maxElevation));
			gpxTrackTags.put("diff_ele_up", latLonFormat.format(analysis.diffElevationUp));
			gpxTrackTags.put("diff_ele_down", latLonFormat.format(analysis.diffElevationDown));
		}
		if (analysis.hasSpeedData) {
			gpxTrackTags.put("avg_speed", latLonFormat.format(analysis.avgSpeed));
			gpxTrackTags.put("max_speed", latLonFormat.format(analysis.maxSpeed));
			gpxTrackTags.put("min_speed", latLonFormat.format(analysis.minSpeed));
		}
	}
	
	private void writePoint(long id, WptPt p, String routeType, String routeId, String routeName) throws IOException {
		serializer.startTag(null, "node");
		serializer.attribute(null, "lat", latLonFormat.format(p.lat));
		serializer.attribute(null, "lon", latLonFormat.format(p.lon));
		serializer.attribute(null, "id", id + "");
		serializer.attribute(null, "action", "modify");
		serializer.attribute(null, "version", "1");
		if (routeType != null) {
			tagValue(serializer, "route", routeType);
			tagValue(serializer, "route_type", "track_point");
			tagValue(serializer, "route_id", routeId);
			tagValue(serializer, "route_name", routeName);
		}
		if (!Algorithms.isEmpty(p.name)) {
			tagValue(serializer, "name", p.name);
		}
		if (!Algorithms.isEmpty(p.desc)) {
			tagValue(serializer, "description", p.desc);
		}
		if (!Algorithms.isEmpty(p.category)) {
			tagValue(serializer, "category", p.category);
		}
		if (!Algorithms.isEmpty(p.comment)) {
			tagValue(serializer, "note", p.comment);
		}
		if (!Algorithms.isEmpty(p.link)) {
			tagValue(serializer, "url", p.link);
		}
		if (!Algorithms.isEmpty(p.getIconName())) {
			tagValue(serializer, "gpx_icon", p.getIconName());
		}
		if (!Algorithms.isEmpty(p.getBackgroundType())) {
			tagValue(serializer, "gpx_bg", p.getBackgroundType());
		}
		int color = p.getColor(0);
		if(color != 0) {
			tagValue(serializer, "color", MapRenderingTypesEncoder.formatColorToPalette(Algorithms.colorToString(color), false));
			tagValue(serializer, "color_int", Algorithms.colorToString(color));
		}
		if (qp.details >= QueryParams.DETAILS_ELE_SPEED) {
			if (!Double.isNaN(p.ele)) {
				tagValue(serializer, "ele", latLonFormat.format(p.ele));
			}
			if (!Double.isNaN(p.speed) && p.speed > 0) {
				tagValue(serializer, "speed", latLonFormat.format(p.speed));
			}
			if (!Double.isNaN(p.hdop)) {
				tagValue(serializer, "hdop", latLonFormat.format(p.hdop));
			}
		}
		serializer.endTag(null, "node");
	}

	private void tagValue(XmlSerializer serializer, String tag, String value) throws IOException {
		if (Algorithms.isEmpty(value)) {
			return;
		}
		serializer.startTag(null, "tag");
		serializer.attribute(null, "k", tag);
		serializer.attribute(null, "v", value);
		serializer.endTag(null, "tag");
	}

	public File writeObf(List<File> files, File tmpFolder, String fileName, File targetObf) throws IOException,
			SQLException, InterruptedException, XmlPullParserException {

		startDocument();
		for (File gf : files) {
			GPXFile f = GPXUtilities.loadGPXFile(gf, null, false);
			GPXTrackAnalysis analysis = f.getAnalysis(gf.lastModified());
			OsmGpxFile file = new OsmGpxFile();
			String name = gf.getName();
			if (name.lastIndexOf('.') != -1) {
				name = name.substring(0, name.lastIndexOf('.'));
			}
			file.name = name;
			file.id = gf.lastModified() / 1000;
			file.timestamp = new Date(gf.lastModified());
			file.description = "";
			file.tags = new String[0];
			writeTrack(file, null, f, analysis, "GPX");
		}
		endDocument();

		IndexCreatorSettings settings = new IndexCreatorSettings();
		settings.indexMap = true;
		settings.indexAddress = false;
		settings.indexPOI = true;
		settings.indexTransport = false;
		settings.indexRouting = false;
		RTree.clearCache();
		try {
			tmpFolder.mkdirs();
			IndexCreator ic = new IndexCreator(tmpFolder, settings);
			MapRenderingTypesEncoder types = new MapRenderingTypesEncoder(null, fileName);
			ic.setMapFileName(fileName);
			// IProgress.EMPTY_PROGRESS
			IProgress prog = IProgress.EMPTY_PROGRESS;
			// prog = new ConsoleProgressImplementation();
			ic.generateIndexes(qp.osmFile, prog, null, MapZooms.getDefault(), types, null);
			new File(tmpFolder, ic.getMapFileName()).renameTo(targetObf);
		} finally {
			Algorithms.removeAllFiles(tmpFolder);
		}
		return targetObf;
	}
	
	public static void generateObfFromGpx(List<String> subArgs) throws IOException, SQLException,
			XmlPullParserException, InterruptedException {
		if (subArgs.size() != 0) {
			File file = new File(subArgs.get(0));
			if (file.isDirectory() || file.getName().endsWith(GPX_FILE_EXT) || file.getName().endsWith(".gpx.gz")) {
				OsmGpxWriteContext.QueryParams qp = new OsmGpxWriteContext.QueryParams();
				qp.osmFile = File.createTempFile(Algorithms.getFileNameWithoutExtension(file), ".osm");
				OsmGpxWriteContext ctx = new OsmGpxWriteContext(qp);
				File tmpFolder = new File(file.getParentFile(), String.valueOf(System.currentTimeMillis()));
				String path = file.isDirectory() ? file.getAbsolutePath() : file.getParentFile().getPath();
				File targetObf = new File(path, Algorithms.getFileNameWithoutExtension(file) + BINARY_MAP_INDEX_EXT);
				List<File> files = new ArrayList<>();
				if (file.isDirectory()) {
					files = Arrays.asList(Objects.requireNonNull(file.listFiles()));
				} else {
					files.add(file);
				}
				if (!files.isEmpty()) {
					ctx.writeObf(files, tmpFolder, Algorithms.getFileNameWithoutExtension(file), targetObf);
				}
				if (!qp.osmFile.delete()) {
					qp.osmFile.deleteOnExit();
				}
			}
		}
	}

	public static class OsmGpxFile {

		public static final double ERROR_NUMBER = -1000;
		public long id;
		public String name;
		public Date timestamp;
		public boolean pending;
		public String user;
		public String visibility;
		public double lat;
		public double lon;
		public String description;

		public double minlat = ERROR_NUMBER;
		public double minlon = ERROR_NUMBER;
		public double maxlat = ERROR_NUMBER;
		public double maxlon = ERROR_NUMBER;

		public String[] tags;
		public String gpx;
		public byte[] gpxGzip;
	}

	public static class QueryParams {
		public static final int DETAILS_POINTS = 0;
		public static final int DETAILS_TRACKS = 1;
		public static final int DETAILS_ELE_SPEED = 2;

		public int details = DETAILS_ELE_SPEED;
		public File osmFile;
		public File obfFile;
		public String tag;
		public int limit = -1;
		public String user;
		public String datestart;
		public String dateend;
		public Set<RouteActivityType> activityTypes = null;
		public double minlat = OsmGpxFile.ERROR_NUMBER;
		public double maxlat = OsmGpxFile.ERROR_NUMBER;
		public double maxlon = OsmGpxFile.ERROR_NUMBER;
		public double minlon = OsmGpxFile.ERROR_NUMBER;
	}
}