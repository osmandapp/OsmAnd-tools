package net.osmand.server.osmgpx;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.NumberFormat;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.zip.GZIPOutputStream;

import org.xmlpull.v1.XmlSerializer;

import net.osmand.GPXUtilities.GPXFile;
import net.osmand.GPXUtilities.GPXTrackAnalysis;
import net.osmand.GPXUtilities.Track;
import net.osmand.GPXUtilities.TrkSegment;
import net.osmand.GPXUtilities.WptPt;
import net.osmand.PlatformUtil;
import net.osmand.osm.MapRenderingTypesEncoder;
import net.osmand.server.osmgpx.DownloadOsmGPX.OsmGpxFile;
import net.osmand.server.osmgpx.DownloadOsmGPX.QueryParams;
import net.osmand.util.Algorithms;

public class OsmGpxWriteContext {
	private final static NumberFormat latLonFormat = new DecimalFormat("0.00#####", new DecimalFormatSymbols());
	final QueryParams qp;
	int tracks = 0;
	int segments = 0;
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
			serializer.setOutput(new OutputStreamWriter(outputStream));
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
					for (WptPt p : s.points) {
						long nid = id--;
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
					addGenericTags(gpxTrackTags, t);
					addGpxInfoTags(gpxTrackTags, gpxInfo, routeIdPrefix);
					addAnalysisTags(gpxTrackTags, analysis);
					
					seraizeTags(extraTrackTags, gpxTrackTags);
					serializer.endTag(null, "way");
				}
			}
			
			for (WptPt p : gpxFile.getPoints()) {
				long nid = id--;
				writePoint(nid, p, "point", routeIdPrefix + gpxInfo.id, gpxInfo.name);
			}
		}
		tracks++;
	}

	private void seraizeTags(Map<String, String> extraTrackTags, Map<String, String> gpxTrackTags) throws IOException {
		if(extraTrackTags != null) {
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
			// red, blue, green, orange, yellow
			String color = null;
			for (String tg : gpxInfo.tags) {
				gpxTrackTags.put("tag_" + tg, tg);
				color = getColorFromTag(color, tg);
			}
			if (color != null) {
				// gpxTrackTags.put("gpx_icon", "");
				gpxTrackTags.put("gpx_bg", color + "_hexagon_3_road_shield");
				gpxTrackTags.put("color", color);
			}
		}
	}

	private String getColorFromTag(String color, String tg) {
		switch(tg) {
		case "mountainbiking":
		case "mtb":
		case "bike":
		case "cycling":
			return "blue";
		case "driving":
		case "car":
			return "green";
		case "skating":
		case "riding":
			return "yellow";
		case "running":
		case "walking":
		case "hiking":
			return "orange";
		}
		return color;
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
}