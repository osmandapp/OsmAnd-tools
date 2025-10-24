package net.osmand.obf;

import static net.osmand.IndexConstants.BINARY_MAP_INDEX_EXT;
import static net.osmand.IndexConstants.GPX_FILE_EXT;
import static net.osmand.obf.preparation.IndexRouteRelationCreator.*;
import static net.osmand.obf.preparation.IndexRouteRelationCreatorV1.DIST_STEP;
import static net.osmand.obf.preparation.IndexRouteRelationCreatorV1.MAX_GRAPH_SKIP_POINTS_BITS;
import static net.osmand.osm.MapPoiTypes.OTHER_MAP_CATEGORY;
import static net.osmand.osm.MapPoiTypes.ROUTES;
import static net.osmand.shared.gpx.GpxFile.XML_COLON;
import static net.osmand.shared.gpx.GpxUtilities.PointsGroup.OBF_POINTS_GROUPS_BACKGROUNDS;
import static net.osmand.shared.gpx.GpxUtilities.PointsGroup.OBF_POINTS_GROUPS_CATEGORY;
import static net.osmand.shared.gpx.GpxUtilities.PointsGroup.OBF_POINTS_GROUPS_COLORS;
import static net.osmand.shared.gpx.GpxUtilities.PointsGroup.OBF_POINTS_GROUPS_DELIMITER;
import static net.osmand.shared.gpx.GpxUtilities.PointsGroup.OBF_POINTS_GROUPS_EMPTY_NAME_STUB;
import static net.osmand.shared.gpx.GpxUtilities.PointsGroup.OBF_POINTS_GROUPS_ICONS;
import static net.osmand.shared.gpx.GpxUtilities.PointsGroup.OBF_POINTS_GROUPS_NAMES;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
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
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.zip.GZIPOutputStream;

import com.google.gson.Gson;
import net.osmand.data.Amenity;
import net.osmand.data.LatLon;
import net.osmand.obf.preparation.IndexRouteRelationCreator;
import net.osmand.osm.MapPoiTypes;
import net.osmand.osm.PoiType;
import net.osmand.shared.gpx.primitives.*;
import okio.GzipSource;
import okio.Okio;

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
import net.osmand.obf.preparation.IndexCreator;
import net.osmand.obf.preparation.IndexCreatorSettings;
import net.osmand.obf.preparation.IndexHeightData;
import net.osmand.osm.MapRenderingTypesEncoder;
import net.osmand.osm.OsmRouteType;
import net.osmand.util.Algorithms;
import net.osmand.util.MapAlgorithms;
import net.osmand.util.MapUtils;
import rtree.RTree;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class OsmGpxWriteContext {
	private final static NumberFormat oneTenthFormat = new DecimalFormat("0.0", new DecimalFormatSymbols(Locale.US));
	private final static NumberFormat latLonFormat = new DecimalFormat("0.00#####", new DecimalFormatSymbols(Locale.US));

	public final QueryParams qp;
	public int tracks = 0;
	public int segments = 0;
	private long baseOsmId = -10; // could be pseudo-random (commit 479f502)

	XmlSerializer serializer = null;
	OutputStream outputStream = null;
	private final Gson gson = new Gson();

	public OsmGpxWriteContext(QueryParams qp) {
		net.osmand.shared.util.PlatformUtil.INSTANCE.initialize(new ToolsOsmAndContextImpl());
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

	private void updateGpxInfoByGpxExtensions(OsmGpxFile gpxInfo, GpxFile gpxFile) {
		Map <String, String> extensions = new LinkedHashMap<>();

		extensions.putAll(gpxFile.getMetadata().getExtensionsToRead());
		extensions.putAll(gpxFile.getExtensionsToRead());

		gpxInfo.updateRouteId(extensions.get(ROUTE_ID_TAG));
		if (Amenity.ROUTE_ID_OSM_PREFIX.equals(gpxInfo.routeIdPrefix)) {
			gpxInfo.name = null; // allow empty name for OSM-tracks
		}

		gpxInfo.updateName(gpxFile.getMetadata().getName());
		gpxInfo.updateDescription(gpxFile.getMetadata().getDescription());

		gpxInfo.updateRef(extensions.get("ref"));
		gpxInfo.updateName(extensions.get("name"));
		gpxInfo.updateDescription(extensions.get("description"));
	}

	private void flushXmlTag(String xmlTag, Map<String, String> osmTags, LatLon ll, long idStart, long idEnd) throws IOException {
		serializer.startTag(null, xmlTag);
		serializer.attribute(null, "id", "" + baseOsmId--);
		serializer.attribute(null, "action", "modify");
		serializer.attribute(null, "version", "1");
		if (ll != null) {
			serializer.attribute(null, "lat", latLonFormat.format(ll.getLatitude()));
			serializer.attribute(null, "lon", latLonFormat.format(ll.getLongitude()));
		}
		if (idStart != 0 && idEnd != 0) {
			for (long nid = idStart; nid > idEnd; nid--) {
				serializer.startTag(null, "nd");
				serializer.attribute(null, "ref", nid + "");
				serializer.endTag(null, "nd");
			}
		}
		serializeTags(osmTags);
		serializer.endTag(null, xmlTag);
	}

	// slightly outdated (never used even by Planet__OSM_GPX_Query job)
	private void writeTrackWithoutDetails(OsmGpxFile gpxInfo, GpxFile gpxFile, GpxTrackAnalysis analysis) throws IOException {
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
			WptPt pointToShow = gpxFile.findPointToShow();
			LatLon ll = new LatLon(pointToShow.getLat(), pointToShow.getLon());
			Map<String, String> poiSectionTags = new LinkedHashMap<>();
			collectGpxTrackTags(gpxInfo, gpxFile, analysis, poiSectionTags, null, null, null);
			flushXmlTag("node", poiSectionTags, ll, 0, 0);
		}
	}

	private void writeWaysAndPoints(OsmGpxFile gpxInfo, GpxFile gpxFile, GpxTrackAnalysis analysis) throws IOException {
		boolean hasSegments = false, hasPoints = false;
		for (Track t : gpxFile.getTracks()) {
			for (TrkSegment s : t.getSegments()) {
				if (s.getPoints().isEmpty()) {
					continue;
				}
				if (!validatedTrackSegment(s)) {
					continue;
				}
				hasSegments = true;
				segments++;

				// 1. Write points as <node> for the following <way> [MAP-section]
				List<LatLon> pointsForPoiSearch = new ArrayList<>();
				long idStart = baseOsmId;

				List<WptPt> points = s.getPoints();
				if (points.size() >= 2) {
					// place the very first point in the approx middle
					WptPt middle = points.get(points.size() / 2);
					pointsForPoiSearch.add(new LatLon(middle.getLatitude(), middle.getLongitude()));

					for (int i = 0; i < points.size(); i++) {
						writePoint(baseOsmId--, points.get(i), null, null, null);

						// place the very next points close to start/end
						// afterward, spread points evenly along the geometry
						int alternateIndex = i % 2 == 0 ? i : points.size() - i - 1;
						WptPt candidate = points.get(alternateIndex);
						WptPt firstPoint = points.get(0);
						WptPt lastPoint = points.get(points.size() - 1);
						double distStart = MapUtils.getDistance(candidate.getLatitude(), candidate.getLongitude(),
								firstPoint.getLatitude(), firstPoint.getLongitude());
						double distEnd = MapUtils.getDistance(candidate.getLatitude(), candidate.getLongitude(),
								lastPoint.getLatitude(), lastPoint.getLongitude());
						if (distStart > POI_SEARCH_POINTS_EDGE_DISTANCE_M && distEnd > POI_SEARCH_POINTS_EDGE_DISTANCE_M) {
							if (pointsForPoiSearch.stream().noneMatch(wpt ->
									MapUtils.getDistance(candidate.getLatitude(), candidate.getLongitude(),
											wpt.getLatitude(), wpt.getLongitude()) < POI_SEARCH_POINTS_INTERVAL_M)) {
								pointsForPoiSearch.add(new LatLon(candidate.getLatitude(), candidate.getLongitude()));
							}
						}
					}
				}

				long idEnd = baseOsmId;

				// 2. Write segment as <way> (without route_type tag) [MAP-section]
				Map<String, String> poiSectionTags = new LinkedHashMap<>();
				Map<String, String> mapSectionTags = new LinkedHashMap<>();
				collectGpxTrackTags(gpxInfo, gpxFile, analysis, poiSectionTags, mapSectionTags, t, s);
				flushXmlTag("way", mapSectionTags, null, idStart, idEnd);

				// 3. Write segment as <node> (with route_type tag) every 5 km [POI-section]
				for (LatLon ll : pointsForPoiSearch) {
					flushXmlTag("node", poiSectionTags, ll, 0, 0);
				}
			}
		}

		// 4. Write all GPX waypoints
		for (WptPt p : gpxFile.getPointsList()) {
			if (gpxInfo != null) {
				writePoint(baseOsmId--, p, "point", gpxInfo.getRouteId(), gpxInfo.name);
				hasPoints = true;
			}
		}

		// 5. Write center-point to search tracks without any segments [POI-section]
		if (hasSegments == false && hasPoints == true) {
			KQuadRect bbox = gpxFile.getRect();
			LatLon center = new LatLon(bbox.centerY(), bbox.centerX());
			Map<String, String> poiSectionTags = new LinkedHashMap<>();
			collectGpxTrackTags(gpxInfo, gpxFile, analysis, poiSectionTags, null, null, null);
			flushXmlTag("node", poiSectionTags, center, 0, 0);
		}
	}

	public void writeTrack(OsmGpxFile gpxInfo, GpxFile gpxFile, GpxTrackAnalysis analysis) throws IOException {
		updateGpxInfoByGpxExtensions(gpxInfo, gpxFile);
		if (qp.details < QueryParams.DETAILS_TRACKS) {
			writeTrackWithoutDetails(gpxInfo, gpxFile, analysis);
		} else {
			writeWaysAndPoints(gpxInfo, gpxFile, analysis);
		}
		tracks++;
	}

	private Map<String, String> collectGpxTrackTags(OsmGpxFile gpxInfo, GpxFile gpxFile, GpxTrackAnalysis analysis,
	                                                @Nullable Map<String, String> poiSectionTags,
	                                                @Nullable Map<String, String> mapSectionTags,
			@Nullable Track track, @Nullable TrkSegment segment) {
		Map<String, String> allTags = new LinkedHashMap<>();
		Map<String, String> metadataExtraTags = new LinkedHashMap<>();
		Map<String, String> extensionsExtraTags = new LinkedHashMap<>();
		try {
			addGpxInfoTags(allTags, gpxInfo);
			addAnalysisTags(allTags, analysis);
			addElevationGraphTags(allTags, segment);
			addMetadataTags(allTags, gpxFile.getMetadata());
			addPointGroupsTags(allTags, gpxFile.getPointsGroups());
			addNameDescDisplaycolor(allTags, extensionsExtraTags, track);

			addExtensionsTags(allTags, extensionsExtraTags, gpxFile.getExtensionsToRead());
			addExtensionsTags(allTags, metadataExtraTags, gpxFile.getMetadata().getExtensionsToRead());

			IndexRouteRelationCreator.finalizeRouteShieldTags(allTags);
			IndexRouteRelationCreator.finalizeActivityTypeAndColors(allTags, metadataExtraTags, extensionsExtraTags,
					gpxInfo.tags);

			allTags.put("route_bbox_radius", gpxFile.getOuterRadius());

			if (!metadataExtraTags.isEmpty()) {
				allTags.put("metadata_extra_tags", gson.toJson(metadataExtraTags));
			}
			if (!extensionsExtraTags.isEmpty()) {
				allTags.put("extensions_extra_tags", gson.toJson(extensionsExtraTags));
			}

			if (poiSectionTags != null) {
				poiSectionTags.putAll(allTags);
				poiSectionTags.remove(TRACK_COLOR); // track_color is required for Rendering only
			}
			if (mapSectionTags != null) {
				mapSectionTags.putAll(allTags);
				mapSectionTags.put("route", "segment");
				mapSectionTags.remove(ROUTE_TYPE); // avoid creation of POI-data when indexing Ways
			}

		} catch (RuntimeException e) {
			// TODO: handle exception
			System.err.printf("Error for object with all tags %s,  metadata tags %s \n", allTags, metadataExtraTags);
			throw e;
		}
		return allTags; // compatibility for writeTrackWithoutDetails
	}

	private void addMetadataTags(Map<String, String> allTags, Metadata metadata) {
		if (metadata.getLink() != null && !Algorithms.isEmpty(metadata.getLink().getHref())) {
			allTags.put("url", metadata.getLink().getHref());
			if (!Algorithms.isEmpty(metadata.getLink().getText())) {
				allTags.put("url_text", metadata.getLink().getText());
			}
		}
	}

	private void addPointGroupsTags(Map<String, String> gpxTrackTags, Map<String, PointsGroup> pointsGroups) {
		List<String> pgNames = new ArrayList<>();
		List<String> pgIcons = new ArrayList<>();
		List<String> pgColors = new ArrayList<>();
		List<String> pgBackgrounds = new ArrayList<>();
		for (String name : pointsGroups.keySet()) {
			PointsGroup pg = pointsGroups.get(name);
			if (pg.getIconName() != null && pg.getBackgroundType() != null && pg.getColor() != 0) {
				pgNames.add(name.isEmpty() ? OBF_POINTS_GROUPS_EMPTY_NAME_STUB : name);
				pgIcons.add(pg.getIconName());
				pgBackgrounds.add(pg.getBackgroundType());
				pgColors.add(KAlgorithms.INSTANCE.colorToString(pg.getColor()));
			}
		}
		if (!pgNames.isEmpty()) {
			final String delimiter = OBF_POINTS_GROUPS_DELIMITER;
			gpxTrackTags.put(OBF_POINTS_GROUPS_NAMES, String.join(delimiter, pgNames));
			gpxTrackTags.put(OBF_POINTS_GROUPS_ICONS, String.join(delimiter, pgIcons));
			gpxTrackTags.put(OBF_POINTS_GROUPS_COLORS, String.join(delimiter, pgColors));
			gpxTrackTags.put(OBF_POINTS_GROUPS_BACKGROUNDS, String.join(delimiter, pgBackgrounds));
		}
	}

	final Set<String> alwaysExtraTags = Set.of("route", "note", "fixme"); // avoid garbage in Map section

	private void addExtensionsTags(@Nonnull Map<String, String> gpxTrackTags,
	                               @Nonnull Map<String, String> extraTags,
	                               @Nonnull Map<String, String> extensions) {
		MapPoiTypes poiTypes = MapPoiTypes.getDefault();
		for (final String tag : extensions.keySet()) {
			final String val = extensions.get(tag);
			PoiType pt = poiTypes.getPoiTypeByTagValue(tag.replaceAll(XML_COLON, ":"), val);
			if (!alwaysExtraTags.contains(tag) && pt != null &&
					(ROUTES.equals(pt.getCategory().getKeyName())
							|| OTHER_MAP_CATEGORY.equals(pt.getCategory().getKeyName()))) {
				gpxTrackTags.putIfAbsent(tag, val);
			} else {
				extraTags.putIfAbsent(tag, val);
			}
		}
	}

	private void addElevationGraphTags(Map<String, String> gpxTrackTags, TrkSegment s) {
		if (s != null) {
			IndexHeightData.WayGeneralStats wgs = new IndexHeightData.WayGeneralStats();
			for (WptPt p : s.getPoints()) {
				wgs.altitudes.add(p.getEle());
				wgs.dists.add(p.getDistance());
			}
			IndexHeightData.calculateEleStats(wgs, (int) DIST_STEP);
			if (wgs.eleCount > 0 && !Double.isNaN(wgs.startEle) && !Double.isNaN(wgs.endEle)) {
//			int st = (int) wgs.startEle;
				gpxTrackTags.put("start_ele", String.valueOf((int) wgs.startEle));
//			gpxTrackTags.put("end_ele__start", String.valueOf((int) wgs.endEle - st));
//			gpxTrackTags.put("avg_ele__start", String.valueOf((int) (wgs.sumEle / wgs.eleCount) - st));
//			gpxTrackTags.put("min_ele__start", String.valueOf((int) wgs.minEle - st));
//			gpxTrackTags.put("max_ele__start", String.valueOf((int) wgs.maxEle - st));
				gpxTrackTags.putIfAbsent("diff_ele_up", String.valueOf((int) wgs.up)); // prefer GpxTrackAnalysis
				gpxTrackTags.putIfAbsent("diff_ele_down", String.valueOf((int) wgs.down)); // prefer GpxTrackAnalysis
				gpxTrackTags.put("ele_graph", MapAlgorithms.encodeIntHeightArrayGraph(wgs.step, wgs.altIncs, MAX_GRAPH_SKIP_POINTS_BITS));
			}
		}
	}

	private void serializeTags(Map<String, String> gpxTrackTags) throws IOException {
		Iterator<Entry<String, String>> it = gpxTrackTags.entrySet().iterator();
		while (it.hasNext()) {
			Entry<String, String> e = it.next();
			tagValue(serializer, e.getKey().replaceAll(XML_COLON, ":"), e.getValue());
		}
	}

	public void endDocument() throws IOException {
		if (serializer != null) {
			serializer.endDocument();
			serializer.flush();
			outputStream.close();
		}		
	}

	private void addNameDescDisplaycolor(Map<String, String> gpxTrackTags, Map<String, String> extensionsExtraTags, Track t) {
		if (t != null) {
			if (!Algorithms.isEmpty(t.getName())) {
				if (gpxTrackTags.containsKey("name") && gpxTrackTags.containsKey("filename")) {
					if (!t.getName().equals(gpxTrackTags.get("name"))) {
						// allow filename search if the original name is changed
						gpxTrackTags.put("name:file", gpxTrackTags.get("filename"));
					}
				}
				gpxTrackTags.put("name", t.getName());
			}
			if (!Algorithms.isEmpty(t.getDesc())) {
				gpxTrackTags.put("description", t.getDesc());
			}
			int color = t.getColor(0); // gpx-color of the distinct track (not supported completely)
			if (color != 0) {
				extensionsExtraTags.put(DISPLAYCOLOR,
						MapRenderingTypesEncoder.formatColorToPalette(Algorithms.colorToString(color), false));
			}
		}
	}

	private void addGpxInfoTags(Map<String, String> gpxTrackTags, OsmGpxFile gpxInfo) {
		if (gpxInfo != null) {
			if (!Algorithms.isEmpty(gpxInfo.user)) {
				gpxTrackTags.put("user", gpxInfo.user);
			}
			if (!Algorithms.isEmpty(gpxInfo.name)) {
				gpxTrackTags.put("name", gpxInfo.name);
			}
			if (!Algorithms.isEmpty(gpxInfo.filename)) {
				gpxTrackTags.put("filename", gpxInfo.filename);
			}
			if (!Algorithms.isEmpty(gpxInfo.ref)) {
				gpxTrackTags.put("ref", gpxInfo.ref);
				if (gpxInfo.ref.length() >= MIN_REF_LENGTH_TO_USE_FOR_SEARCH) {
					gpxTrackTags.put("name:ref", gpxInfo.ref);
				}
			}
			if (!Algorithms.isEmpty(gpxInfo.description)) {
				gpxTrackTags.put("description", gpxInfo.description);
			}
			gpxTrackTags.put(ROUTE_ID_TAG, gpxInfo.getRouteId());

			if (gpxInfo.timestamp.getTime() > 0) {
				gpxTrackTags.put("date", gpxInfo.timestamp.toString());
			}
		}
	}

	private void addAnalysisTags(Map<String, String> gpxTrackTags, GpxTrackAnalysis analysis) {
		gpxTrackTags.put("distance", oneTenthFormat.format(analysis.getTotalDistance() / 1000));
		if (analysis.isTimeSpecified()) {
			gpxTrackTags.put("time_span", analysis.getTimeSpan() + "");
			gpxTrackTags.put("time_span_no_gaps", analysis.getTimeSpanWithoutGaps() + "");
			gpxTrackTags.put("time_moving", analysis.getTimeMoving() + "");
			gpxTrackTags.put("time_moving_no_gaps", analysis.getTimeMovingWithoutGaps() + "");
		}
		if (analysis.hasElevationData()) {
			gpxTrackTags.put("avg_ele", oneTenthFormat.format(analysis.getAvgElevation()));
			gpxTrackTags.put("min_ele", oneTenthFormat.format(analysis.getMinElevation()));
			gpxTrackTags.put("max_ele", oneTenthFormat.format(analysis.getMaxElevation()));
			gpxTrackTags.put("diff_ele_up", oneTenthFormat.format(analysis.getDiffElevationUp()));
			gpxTrackTags.put("diff_ele_down", oneTenthFormat.format(analysis.getDiffElevationDown()));
		}
		if (analysis.hasSpeedData()) {
			gpxTrackTags.put("avg_speed", oneTenthFormat.format(analysis.getAvgSpeed()));
			gpxTrackTags.put("max_speed", oneTenthFormat.format(analysis.getMaxSpeed()));
			gpxTrackTags.put("min_speed", oneTenthFormat.format(analysis.getMinSpeed()));
		}
	}
	
	private void writePoint(long id, WptPt p, String routeType, String routeId, String routeName) throws IOException {
		serializer.startTag(null, "node");
		serializer.attribute(null, "lat", latLonFormat.format(p.getLat()));
		serializer.attribute(null, "lon", latLonFormat.format(p.getLon()));
		serializer.attribute(null, "id", id + "");
		serializer.attribute(null, "action", "modify");
		serializer.attribute(null, "version", "1");

		Map<String, String> pointPoiTags = new LinkedHashMap<>();
		Map<String, String> pointExtraTags = new LinkedHashMap<>();
		addExtensionsTags(pointPoiTags, pointExtraTags, p.getExtensionsToRead());
		if (!pointExtraTags.isEmpty()) {
			tagValue(serializer, WPT_EXTRA_TAGS, gson.toJson(pointExtraTags));
		}
		serializeTags(pointPoiTags);

		if (routeType != null) {
			tagValue(serializer, "route", routeType);
			tagValue(serializer, ROUTE_TYPE, "track_point");
			tagValue(serializer, ROUTE_ID_TAG, routeId);
			tagValue(serializer, "route_name", routeName); // required by fetchSegmentsAndPoints / searchPoiByName
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
		if (p.getLink() != null && !Algorithms.isEmpty(p.getLink().getHref())) {
			tagValue(serializer, "url", p.getLink().getHref());
			if (!Algorithms.isEmpty(p.getLink().getText())) {
				tagValue(serializer, "url_text", p.getLink().getText());
			}
		}
		if (!Algorithms.isEmpty(p.getIconName())) {
			tagValue(serializer, "icon", p.getIconName());
		}
		if (!Algorithms.isEmpty(p.getBackgroundType())) {
			tagValue(serializer, "background", p.getBackgroundType());
		}
		int color = p.getColor(0); // gpx-point color
		if (color != 0) {
			tagValue(serializer, COLOR, MapRenderingTypesEncoder.formatColorToPalette(Algorithms.colorToString(color), false));
		}
		if (qp.details >= QueryParams.DETAILS_ELE_SPEED) {
			if (!Double.isNaN(p.getEle())) {
				tagValue(serializer, "ele", oneTenthFormat.format(p.getEle()));
			}
			if (!Double.isNaN(p.getSpeed()) && p.getSpeed() > 0) {
				tagValue(serializer, "speed", oneTenthFormat.format(p.getSpeed()));
			}
			if (!Double.isNaN(p.getHdop())) {
				tagValue(serializer, "hdop", oneTenthFormat.format(p.getHdop()));
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
	
	public File writeObf(Map<String, GpxFile> gpxFiles, List<KFile> files, File tmpFolder, String fileName,
	                     File targetObf) throws IOException, SQLException, InterruptedException, XmlPullParserException {
		startDocument();
		if (gpxFiles != null) {
			for (Entry<String, GpxFile> entry : gpxFiles.entrySet()) {
				GpxFile gpxFile = entry.getValue();
				writeFile(gpxFile, entry.getKey());
			}
		} else if (files != null) {
			for (KFile gf : files) {
				if (gf.name().endsWith(".bz2")) {
					throw new RuntimeException("writeObf: unsupported input file extension: " + gf.name());
				}
				if (gf.name().endsWith(".gz")) {
					InputStream fis = new FileInputStream(gf.absolutePath());
					GpxFile gpxFile = GpxUtilities.INSTANCE.loadGpxFile(null, new GzipSource(Okio.source(fis)), null, false);
					writeFile(gpxFile, gf.name());
				} else if (gf.name().endsWith(".gpx")) {
					GpxFile gpxFile = GpxUtilities.INSTANCE.loadGpxFile(gf, null, false);
					writeFile(gpxFile, gf.name());
				}
			}
		}
		endDocument();
		
		IndexCreatorSettings settings = new IndexCreatorSettings();
		settings.indexMap = true;
		settings.indexPOI = true;
		settings.indexAddress = false;
		settings.indexRouting = false;
		settings.indexTransport = false;
//		String srtmDirectory = System.getenv("SRTM_DIRECTORY"); // HeightData 994081a880fc47479e0d17e4d7f1ee8defa96fc5
//		if (srtmDirectory != null) {
//			settings.indexRouting = true;
//			settings.srtmDataFolderUrl = srtmDirectory;
//		}
		// reduce memory footprint for single thread generation
		// Remove it if it is called in multithread
		RTree.clearCache();
		try {
			tmpFolder.mkdirs();
			IndexCreator ic = new IndexCreator(tmpFolder, settings);
			MapRenderingTypesEncoder types = new MapRenderingTypesEncoder(null, fileName);
			ic.setMapFileName(fileName);
			IProgress prog = IProgress.EMPTY_PROGRESS;
			ic.generateIndexes(qp.osmFile, prog, null, MapZooms.getDefault(), types, null);
			File obfInTmpFolder = new File(tmpFolder, ic.getMapFileName()); // might need to move between two fs
			Files.move(obfInTmpFolder.toPath(), targetObf.toPath(), StandardCopyOption.REPLACE_EXISTING);
		} finally {
			Algorithms.removeAllFiles(tmpFolder);
		}
		return targetObf;
	}

	private void writeFile(GpxFile gpxFile, String fileName) throws SQLException, IOException {
		if (gpxFile == null || gpxFile.getError() != null) {
			System.err.printf("WARN: writeFile %s gpxFile error (%s)\n",
					fileName, gpxFile != null ? gpxFile.getError().getMessage() : "unknown");
			return;
		}
		GpxTrackAnalysis analysis = gpxFile.getAnalysis(0);

		OsmGpxFile file = new OsmGpxFile("GPX");

		final String baseFileName = fileName.substring(fileName.lastIndexOf('/') + 1); // ok with/without '/'
		final String noGzFileName = baseFileName.replaceAll("(?i)\\.gz$", ""); // gpx filename
		final String noGpxFileName = noGzFileName.replaceAll("(?i)\\.gpx$", ""); // <name>

		file.filename = noGzFileName;
		file.name = noGpxFileName;
		file.tags = new String[0];

		int xor = 0;
		for (int i = 0; i < file.name.length(); i++) {
			xor += file.name.charAt(i);
		}

		long ts = gpxFile.getModifiedTime() > 0 ? gpxFile.getModifiedTime() : System.currentTimeMillis();
		file.timestamp = new Date(ts);
		file.id = ts ^ xor;

		writeTrack(file, gpxFile, analysis);
	}
	
	public static void main(String[] args) throws IOException, SQLException, XmlPullParserException, InterruptedException {
		generateObfFromGpx(Arrays.asList(args));
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
					files = file.listFiles();
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

		private String routeIdPrefix;
		public long id;

		public String ref;
		public String name;
		public Date timestamp;
		public boolean pending;
		public String user;
		public String visibility;
		public double lat;
		public double lon;
		public String description;
		public String filename;

		public static final double ERROR_NUMBER = -1000;
		public double minlat = ERROR_NUMBER;
		public double minlon = ERROR_NUMBER;
		public double maxlat = ERROR_NUMBER;
		public double maxlon = ERROR_NUMBER;

		public String[] tags;
		public String gpx;
		public byte[] gpxGzip;

		public OsmGpxFile(@Nonnull String prefix) {
			this.routeIdPrefix = prefix;
		}

		public String getRouteId() {
			return routeIdPrefix + id;
		}

		public void updateRouteId(@Nullable String routeId) {
			if (routeId != null) {
				// parse route_id tag in format of /[A-Z]+[0-9]+/ such as OSM12345
				String letters = routeId.replaceAll("[^A-Za-z]", "");
				String digits = routeId.replaceAll("[^0-9]", "");
				if (!letters.isEmpty() && !digits.isEmpty()) {
					this.id = Long.parseLong(digits);
					this.routeIdPrefix = letters;
				}
			}
		}

		public void updateName(@Nullable String name) {
			if (!Algorithms.isEmpty(name)) {
				this.name = name;
			}
		}

		public void updateRef(@Nullable String ref) {
			if (!Algorithms.isEmpty(ref)) {
				this.ref = ref;
			}
		}

		public void updateDescription(@Nullable String description) {
			if (!Algorithms.isEmpty(description)) {
				this.description = description;
			}
		}
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