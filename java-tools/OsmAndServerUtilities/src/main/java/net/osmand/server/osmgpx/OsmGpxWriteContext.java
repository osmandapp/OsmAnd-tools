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
	

	public void writeTrack(OsmGpxFile gpxInfo, Map<String, String> trackTags, GPXFile gpxFile, GPXTrackAnalysis analysis)
			throws IOException, SQLException {
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
				tagValue(serializer, "gpx", "segment");
				addGenericInfoTags(serializer, gpxInfo, null);
				addAnalysisTags(serializer, analysis);
				addTrackSpecificTags(serializer, trackTags);
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
						writePoint(nid, p, null);
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
					tagValue(serializer, "gpx", "segment");
					addGenericInfoTags(serializer, gpxInfo, t);
					addAnalysisTags(serializer, analysis);
					addTrackSpecificTags(serializer, trackTags);
					serializer.endTag(null, "way");
				}
			}
			
			for (WptPt p : gpxFile.getPoints()) {
				long nid = id--;
				writePoint(nid, p, "point");
			}
		}
		tracks++;
	}

	public void endDocument() throws IOException {
		if(serializer != null) {
			serializer.endDocument();
			serializer.flush();
			outputStream.close();
		}		
	}
	
	private void addTrackSpecificTags(XmlSerializer serializer, Map<String, String> tags)
			throws SQLException, IOException {
		if (tags != null && !tags.isEmpty()) {
			Iterator<Entry<String, String>> it = tags.entrySet().iterator();
			while (it.hasNext()) {
				Entry<String, String> e = it.next();
				tagValue(serializer, e.getKey(), e.getValue());
			}
		}
	}

	private void addGenericInfoTags(XmlSerializer serializer, OsmGpxFile gpxInfo, Track p) throws IOException {
		if (p != null) {
			if (!Algorithms.isEmpty(p.name)) {
				tagValue(serializer, "name", p.name);
			}
			if (!Algorithms.isEmpty(p.desc)) {
				tagValue(serializer, "description", p.desc);
			}
			int color = p.getColor(0);
			if (color != 0) {
				tagValue(serializer, "color",
						MapRenderingTypesEncoder.formatColorToPalette(Algorithms.colorToString(color), false));
				tagValue(serializer, "color_int", Algorithms.colorToString(color));
			}
		}
		if (gpxInfo != null) {
			tagValue(serializer, "trackid", gpxInfo.id + "");
			tagValue(serializer, "ref", gpxInfo.id % 1000 + "");
			tagValue(serializer, "name", gpxInfo.name);
			tagValue(serializer, "user", gpxInfo.user);
			tagValue(serializer, "date", gpxInfo.timestamp.toString());
			tagValue(serializer, "description", gpxInfo.description);
		}
	}

	private void addAnalysisTags(XmlSerializer serializer, GPXTrackAnalysis analysis) throws IOException {
		tagValue(serializer, "distance", latLonFormat.format(analysis.totalDistance));
		if (analysis.isTimeSpecified()) {
			tagValue(serializer, "time_span", analysis.timeSpan + "");
			tagValue(serializer, "time_span_no_gaps", analysis.timeSpanWithoutGaps + "");
			tagValue(serializer, "time_moving", analysis.timeMoving + "");
			tagValue(serializer, "time_moving_no_gaps", analysis.timeMovingWithoutGaps + "");
		}
		if (analysis.hasElevationData) {
			tagValue(serializer, "avg_ele", latLonFormat.format(analysis.avgElevation));
			tagValue(serializer, "min_ele", latLonFormat.format(analysis.minElevation));
			tagValue(serializer, "max_ele", latLonFormat.format(analysis.maxElevation));
			tagValue(serializer, "diff_ele_up", latLonFormat.format(analysis.diffElevationUp));
			tagValue(serializer, "diff_ele_down", latLonFormat.format(analysis.diffElevationDown));
		}
		if (analysis.hasSpeedData) {
			tagValue(serializer, "avg_speed", latLonFormat.format(analysis.avgSpeed));
			tagValue(serializer, "max_speed", latLonFormat.format(analysis.maxSpeed));
			tagValue(serializer, "min_speed", latLonFormat.format(analysis.minSpeed));
			
		}
	}
	
	private void writePoint(long id, WptPt p, String gpxValue) throws IOException {
		serializer.startTag(null, "node");
		serializer.attribute(null, "lat", latLonFormat.format(p.lat));
		serializer.attribute(null, "lon", latLonFormat.format(p.lon));
		serializer.attribute(null, "id", id + "");
		serializer.attribute(null, "action", "modify");
		serializer.attribute(null, "version", "1");
		if (gpxValue != null) {
			tagValue(serializer, "gpx", gpxValue);
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
			tagValue(serializer, "comment", p.comment);
		}
		if (!Algorithms.isEmpty(p.link)) {
			tagValue(serializer, "link", p.link);
		}
		if (!Algorithms.isEmpty(p.getIconName())) {
			tagValue(serializer, "icon", p.getIconName());
		}
		if (!Algorithms.isEmpty(p.getBackgroundType())) {
			tagValue(serializer, "bg", p.getBackgroundType());
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