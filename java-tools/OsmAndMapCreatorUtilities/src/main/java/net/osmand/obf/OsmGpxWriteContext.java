package net.osmand.obf;

import static net.osmand.IndexConstants.BINARY_MAP_INDEX_EXT;
import static net.osmand.IndexConstants.GPX_FILE_EXT;
import static net.osmand.obf.preparation.IndexRouteRelationCreator.DIST_STEP;
import static net.osmand.obf.preparation.IndexRouteRelationCreator.MAX_GRAPH_SKIP_POINTS_BITS;
import static net.osmand.shared.gpx.GpxUtilities.PointsGroup.OBF_POINTS_GROUPS_CATEGORY;
import static net.osmand.shared.gpx.GpxUtilities.PointsGroup.OBF_POINTS_GROUPS_DELIMITER;
import static net.osmand.shared.gpx.GpxUtilities.PointsGroup.OBF_POINTS_GROUPS_NAMES;
import static net.osmand.shared.gpx.GpxUtilities.PointsGroup.OBF_POINTS_GROUPS_ICONS;
import static net.osmand.shared.gpx.GpxUtilities.PointsGroup.OBF_POINTS_GROUPS_COLORS;
import static net.osmand.shared.gpx.GpxUtilities.PointsGroup.OBF_POINTS_GROUPS_BACKGROUNDS;
import static net.osmand.shared.gpx.primitives.GpxExtensions.OBF_GPX_EXTENSION_TAG_PREFIX;

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

import net.osmand.shared.io.KFile;
import org.xmlpull.v1.XmlPullParserException;
import org.xmlpull.v1.XmlSerializer;

import net.osmand.IProgress;
import net.osmand.PlatformUtil;
import net.osmand.binary.MapZooms;
import net.osmand.shared.util.KAlgorithms;
import net.osmand.shared.data.KQuadRect;
import net.osmand.shared.gpx.GpxFile;
import net.osmand.shared.gpx.GpxTrackAnalysis;
import net.osmand.shared.gpx.GpxUtilities;
import net.osmand.shared.gpx.GpxUtilities.PointsGroup;
import net.osmand.shared.gpx.primitives.Track;
import net.osmand.shared.gpx.primitives.TrkSegment;
import net.osmand.shared.gpx.primitives.WptPt;
import net.osmand.obf.preparation.IndexCreator;
import net.osmand.obf.preparation.IndexCreatorSettings;
import net.osmand.obf.preparation.IndexHeightData;
import net.osmand.osm.MapRenderingTypesEncoder;
import net.osmand.osm.OsmRouteType;
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
		for (WptPt p : t.getPoints()) {
			if (p.getLat() >= 90 || p.getLat() <= -90 || p.getLon() >= 180 || p.getLon() <= -180) {
				return false;
			}
			if (testPoints) {
				if (p.getLat() >= qp.minlat && p.getLat() <= qp.maxlat && p.getLon() >= qp.minlon && p.getLon() <= qp.maxlon) {
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
	

	public void writeTrack(OsmGpxFile gpxInfo, Map<String, String> extraTrackTags, GpxFile gpxFile, GpxTrackAnalysis analysis,
			String routeIdPrefix)
			throws IOException, SQLException {
		Map<String, String> gpxTrackTags = new LinkedHashMap<String, String>();
		if (qp.details < QueryParams.DETAILS_TRACKS) {
			boolean validTrack = false;
			for (Track t : gpxFile.getTracks()) {
				for (TrkSegment s : t.getSegments()) {
					if (s.getPoints().isEmpty()) {
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
				serializer.attribute(null, "lat", latLonFormat.format(gpxFile.findPointToShow().getLat()));
				serializer.attribute(null, "lon", latLonFormat.format(gpxFile.findPointToShow().getLon()));
				tagValue(serializer, "route", "segment");
				tagValue(serializer, "route_type", "track");
				tagValue(serializer, "route_radius", gpxFile.getOuterRadius());
				addGenericTags(gpxTrackTags, null);
				addGpxInfoTags(gpxTrackTags, gpxInfo, routeIdPrefix);
				addExtensionsTags(gpxTrackTags, gpxFile.getExtensions());
				addPointGroupsTags(gpxTrackTags, gpxFile.getPointsGroups());
				addAnalysisTags(gpxTrackTags, analysis);
				serializeTags(extraTrackTags, gpxTrackTags);
				serializer.endTag(null, "node");
			}
		} else {
			for (Track t : gpxFile.getTracks()) {
				for (TrkSegment s : t.getSegments()) {
					if (s.getPoints().isEmpty()) {
						continue;
					}
					if (!validatedTrackSegment(s)) {
						continue;
					}
					segments++;
					long idStart = id;
					double dlon = s.getPoints().get(0).getLon();
					double dlat = s.getPoints().get(0).getLat();
					KQuadRect qr = new KQuadRect(dlon, dlat, dlon, dlat);
					for (WptPt p : s.getPoints()) {
						long nid = id--;
						GpxUtilities.INSTANCE.updateQR(qr, p, dlat, dlon);
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
					int radius = (int) MapUtils.getDistance(qr.getBottom(), qr.getLeft(), qr.getTop(), qr.getRight());
					tagValue(serializer, "route_radius", MapUtils.convertDistToChar(radius, GpxUtilities.TRAVEL_GPX_CONVERT_FIRST_LETTER, GpxUtilities.TRAVEL_GPX_CONVERT_FIRST_DIST,
							GpxUtilities.TRAVEL_GPX_CONVERT_MULT_1, GpxUtilities.TRAVEL_GPX_CONVERT_MULT_2));
					addGenericTags(gpxTrackTags, t);
					addGpxInfoTags(gpxTrackTags, gpxInfo, routeIdPrefix);
					addExtensionsTags(gpxTrackTags, gpxFile.getExtensions());
					addPointGroupsTags(gpxTrackTags, gpxFile.getPointsGroups());
					addAnalysisTags(gpxTrackTags, analysis);
					addElevationTags(gpxTrackTags, s);
					serializeTags(extraTrackTags, gpxTrackTags);
					serializer.endTag(null, "way");
				}
			}

			for (WptPt p : gpxFile.getPointsList()) {
				long nid = id--;
				if (gpxInfo != null) {
					writePoint(nid, p, "point", routeIdPrefix + gpxInfo.id, gpxInfo.name);
				}
			}
		}
		tracks++;
	}

	private void addPointGroupsTags(Map<String, String> gpxTrackTags, Map<String, PointsGroup> pointsGroups) {
		List<String> pgNames = new ArrayList<>();
		List<String> pgIcons = new ArrayList<>();
		List<String> pgColors = new ArrayList<>();
		List<String> pgBackgrounds = new ArrayList<>();
		for (String name : pointsGroups.keySet()) {
			pgNames.add(name);
			pgIcons.add(pointsGroups.get(name).getIconName());
			pgBackgrounds.add(pointsGroups.get(name).getBackgroundType());
			pgColors.add(KAlgorithms.INSTANCE.colorToString(pointsGroups.get(name).getColor()));
		}
		final String delimiter = OBF_POINTS_GROUPS_DELIMITER;
		gpxTrackTags.put(OBF_POINTS_GROUPS_NAMES, String.join(delimiter, pgNames));
		gpxTrackTags.put(OBF_POINTS_GROUPS_ICONS, String.join(delimiter, pgIcons));
		gpxTrackTags.put(OBF_POINTS_GROUPS_COLORS, String.join(delimiter, pgColors));
		gpxTrackTags.put(OBF_POINTS_GROUPS_BACKGROUNDS, String.join(delimiter, pgBackgrounds));
	}

	private void addExtensionsTags(Map<String, String> gpxTrackTags, Map<String, String> extensions) {
		if (extensions != null) {
			if (extensions.containsKey("color")) {
				gpxTrackTags.put("color", extensions.get("color")); // osmand:color
				// prioritize osmand:color over GPX color
				gpxTrackTags.remove("colour_int");
				gpxTrackTags.remove("colour");
			}
			if (extensions.containsKey("width")) {
				gpxTrackTags.put("width", extensions.get("width")); // osmand:width
			}
			for (String key : extensions.keySet()) {
				gpxTrackTags.put(OBF_GPX_EXTENSION_TAG_PREFIX + key, extensions.get(key));
			}
		}
	}

	private void addElevationTags(Map<String, String> gpxTrackTags, TrkSegment s) {
		IndexHeightData.WayGeneralStats wgs = new IndexHeightData.WayGeneralStats();
		for (WptPt p : s.getPoints()) {
			wgs.altitudes.add(p.getEle());
			wgs.dists.add(p.getDistance());
		}
		IndexHeightData.calculateEleStats(wgs, (int) DIST_STEP);
		if (wgs.eleCount > 0) {
			int st = (int) wgs.startEle;
			gpxTrackTags.put("start_ele", String.valueOf((int) wgs.startEle));
			gpxTrackTags.put("end_ele__start", String.valueOf((int) wgs.endEle - st));
			gpxTrackTags.put("avg_ele__start", String.valueOf((int) (wgs.sumEle / wgs.eleCount) - st));
			gpxTrackTags.put("min_ele__start", String.valueOf((int) wgs.minEle - st));
			gpxTrackTags.put("max_ele__start", String.valueOf((int) wgs.maxEle - st));
			gpxTrackTags.putIfAbsent("diff_ele_up", String.valueOf((int) wgs.up)); // prefer GpxTrackAnalysis
			gpxTrackTags.putIfAbsent("diff_ele_down", String.valueOf((int) wgs.down)); // prefer GpxTrackAnalysis
			gpxTrackTags.put("ele_graph", MapAlgorithms.encodeIntHeightArrayGraph(wgs.step, wgs.altIncs, MAX_GRAPH_SKIP_POINTS_BITS));
		}
	}

	private void serializeTags(Map<String, String> extraTrackTags, Map<String, String> gpxTrackTags) throws IOException {
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
			if (!Algorithms.isEmpty(p.getName())) {
				gpxTrackTags.put("name", p.getName());
			}
			if (!Algorithms.isEmpty(p.getDesc())) {
				gpxTrackTags.put("description", p.getDesc());
			}
			int color = p.getColor(0);
			if (color != 0) {
				gpxTrackTags.put("colour",
						MapRenderingTypesEncoder.formatColorToPalette(Algorithms.colorToString(color), false));
				gpxTrackTags.put("colour_int", Algorithms.colorToString(color));
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
			OsmRouteType activityType = OsmRouteType.getTypeFromTags(gpxInfo.tags);
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

	private void addAnalysisTags(Map<String, String> gpxTrackTags, GpxTrackAnalysis analysis) throws IOException {
		gpxTrackTags.put("distance", latLonFormat.format(analysis.getTotalDistance()));
		if (analysis.isTimeSpecified()) {
			gpxTrackTags.put("time_span", analysis.getTimeSpan() + "");
			gpxTrackTags.put("time_span_no_gaps", analysis.getTimeSpanWithoutGaps() + "");
			gpxTrackTags.put("time_moving", analysis.getTimeMoving() + "");
			gpxTrackTags.put("time_moving_no_gaps", analysis.getTimeMovingWithoutGaps() + "");
		}
		if (analysis.hasElevationData()) {
			gpxTrackTags.put("avg_ele", latLonFormat.format(analysis.getAvgElevation()));
			gpxTrackTags.put("min_ele", latLonFormat.format(analysis.getMinElevation()));
			gpxTrackTags.put("max_ele", latLonFormat.format(analysis.getMaxElevation()));
			gpxTrackTags.put("diff_ele_up", latLonFormat.format(analysis.getDiffElevationUp()));
			gpxTrackTags.put("diff_ele_down", latLonFormat.format(analysis.getDiffElevationDown()));
		}
		if (analysis.hasSpeedData()) {
			gpxTrackTags.put("avg_speed", latLonFormat.format(analysis.getAvgSpeed()));
			gpxTrackTags.put("max_speed", latLonFormat.format(analysis.getMaxSpeed()));
			gpxTrackTags.put("min_speed", latLonFormat.format(analysis.getMinSpeed()));
		}
	}
	
	private void writePoint(long id, WptPt p, String routeType, String routeId, String routeName) throws IOException {
		serializer.startTag(null, "node");
		serializer.attribute(null, "lat", latLonFormat.format(p.getLat()));
		serializer.attribute(null, "lon", latLonFormat.format(p.getLon()));
		serializer.attribute(null, "id", id + "");
		serializer.attribute(null, "action", "modify");
		serializer.attribute(null, "version", "1");
		if (routeType != null) {
			tagValue(serializer, "route", routeType);
			tagValue(serializer, "route_type", "track_point");
			tagValue(serializer, "route_id", routeId);
			tagValue(serializer, "route_name", routeName);
		}
		if (!Algorithms.isEmpty(p.getName())) {
			tagValue(serializer, "name", p.getName());
		}
		if (!Algorithms.isEmpty(p.getDesc())) {
			tagValue(serializer, "description", p.getDesc());
		}
		if (!Algorithms.isEmpty(p.getCategory())) {
			tagValue(serializer, "category", p.getCategory());
			tagValue(serializer, OBF_POINTS_GROUPS_CATEGORY, p.getCategory());
		}
		if (!Algorithms.isEmpty(p.getComment())) {
			tagValue(serializer, "note", p.getComment());
		}
		if (!Algorithms.isEmpty(p.getLink())) {
			tagValue(serializer, "url", p.getLink());
		}
		if (!Algorithms.isEmpty(p.getIconName())) {
			tagValue(serializer, "gpx_icon", p.getIconName());
		}
		if (!Algorithms.isEmpty(p.getBackgroundType())) {
			tagValue(serializer, "gpx_bg", p.getBackgroundType());
		}
		int color = p.getColor(0);
		if(color != 0) {
			tagValue(serializer, "colour", MapRenderingTypesEncoder.formatColorToPalette(Algorithms.colorToString(color), false));
			tagValue(serializer, "colour_int", Algorithms.colorToString(color));
		}
		if (qp.details >= QueryParams.DETAILS_ELE_SPEED) {
			if (!Double.isNaN(p.getEle())) {
				tagValue(serializer, "ele", latLonFormat.format(p.getEle()));
			}
			if (!Double.isNaN(p.getSpeed()) && p.getSpeed() > 0) {
				tagValue(serializer, "speed", latLonFormat.format(p.getSpeed()));
			}
			if (!Double.isNaN(p.getHdop())) {
				tagValue(serializer, "hdop", latLonFormat.format(p.getHdop()));
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
	
	public File writeObf(Map<String, GpxFile> gpxFiles, List<KFile> files, File tmpFolder, String fileName, File targetObf) throws IOException,
			SQLException, InterruptedException, XmlPullParserException {
		
		startDocument();
		if (gpxFiles != null) {
			for (Entry<String, GpxFile> entry : gpxFiles.entrySet()) {
				GpxFile gpxFile = entry.getValue();
				writeFile(gpxFile, entry.getKey());
			}
		} else if (files != null) {
			for (KFile gf : files) {
				GpxFile gpxFile = GpxUtilities.INSTANCE.loadGpxFile(gf, null, false);
				writeFile(gpxFile, gf.name());
			}
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
	
	private void writeFile(GpxFile gpxFile, String fileName) throws SQLException, IOException {
		GpxTrackAnalysis analysis = gpxFile.getAnalysis(gpxFile.getModifiedTime());
		OsmGpxFile file = new OsmGpxFile();
		String name = fileName;
		if (name.lastIndexOf('.') != -1) {
			name = name.substring(0, name.lastIndexOf('.'));
		}
		file.name = name;
		file.id = gpxFile.getModifiedTime() / 1000;
		file.timestamp = new Date(gpxFile.getModifiedTime());
		file.description = "";
		file.tags = new String[0];
		writeTrack(file, null, gpxFile, analysis, "GPX");
	}
	
	public static void generateObfFromGpx(List<String> subArgs) throws IOException, SQLException,
			XmlPullParserException, InterruptedException {
		if (subArgs.size() != 0) {
			KFile file = new KFile(subArgs.get(0));
			if (file.isDirectory() || file.name().endsWith(GPX_FILE_EXT) || file.name().endsWith(".gpx.gz")) {
				OsmGpxWriteContext.QueryParams qp = new OsmGpxWriteContext.QueryParams();
				qp.osmFile = File.createTempFile(file.getFileNameWithoutExtension(), ".osm");
				OsmGpxWriteContext ctx = new OsmGpxWriteContext(qp);
				File dir = new File(file.isDirectory() ? file.name() : file.parent().name());
				File tmpFolder = new File(dir, String.valueOf(System.currentTimeMillis()));
				File targetObf = new File(dir, file.getFileNameWithoutExtension() + BINARY_MAP_INDEX_EXT);
				List<KFile> files = new ArrayList<>();
				if (file.isDirectory()) {
					files = Arrays.asList(Objects.requireNonNull(file.listFiles()));
				} else {
					files.add(file);
				}
				if (!files.isEmpty()) {
					ctx.writeObf(null, files, tmpFolder, file.getFileNameWithoutExtension(), targetObf);
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
		public Set<OsmRouteType> activityTypes = null;
		public double minlat = OsmGpxFile.ERROR_NUMBER;
		public double maxlat = OsmGpxFile.ERROR_NUMBER;
		public double maxlon = OsmGpxFile.ERROR_NUMBER;
		public double minlon = OsmGpxFile.ERROR_NUMBER;
	}
}