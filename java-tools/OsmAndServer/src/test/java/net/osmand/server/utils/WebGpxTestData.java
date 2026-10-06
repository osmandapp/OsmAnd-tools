package net.osmand.server.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Objects;

import com.google.gson.Gson;

import kotlin.Pair;
import net.osmand.shared.gpx.GpxFile;
import net.osmand.shared.gpx.GpxUtilities;
import okio.Buffer;

// the track data of src/test/resources/gpx/points-with-extensions.gpx as the web map gets it
public class WebGpxTestData {

	private static final String GPX = "points-with-extensions.gpx";

	// as GpxService.buildTrackDataFromGpxFile, without the analysis
	public static String trackDataJson(Gson gson) throws IOException {
		GpxFile gpxFile;
		try (InputStream in = WebGpxTestData.class.getResourceAsStream("/gpx/" + GPX);
		     Buffer source = new Buffer()) {
			source.readFrom(Objects.requireNonNull(in, GPX));
			gpxFile = GpxUtilities.INSTANCE.loadGpxFile(source);
		}
		WebGpxParser parser = new WebGpxParser();
		WebGpxParser.TrackData data = new WebGpxParser.TrackData();
		data.metaData = new WebGpxParser.WebMetaData(gpxFile.getMetadata());
		data.wpts = parser.getWpts(gpxFile);
		Pair<List<WebGpxParser.WebTrack>, List<GpxUtilities.RouteType>> tracks = parser.getTracks(gpxFile);
		data.tracks = tracks.getFirst();
		data.routeTypes = tracks.getSecond();
		data.ext = gpxFile.getExtensions();
		data.pointsGroups = parser.getPointsGroups(gpxFile);

		return gson.toJson(data);
	}
}
