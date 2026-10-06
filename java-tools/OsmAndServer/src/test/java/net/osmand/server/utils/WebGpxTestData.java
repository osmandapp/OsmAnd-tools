package net.osmand.server.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

import org.springframework.test.util.ReflectionTestUtils;

import com.google.gson.Gson;

import net.osmand.server.api.services.GpxService;
import net.osmand.shared.gpx.GpxFile;
import net.osmand.shared.gpx.GpxUtilities;
import okio.Buffer;

// the track data of src/test/resources/gpx/points-with-extensions.gpx as the web map gets it
public class WebGpxTestData {

	private static final String GPX = "points-with-extensions.gpx";

	public static GpxFile load() throws IOException {
		try (InputStream in = WebGpxTestData.class.getResourceAsStream("/gpx/" + GPX);
		     Buffer source = new Buffer()) {
			source.readFrom(Objects.requireNonNull(in, GPX));
			return GpxUtilities.INSTANCE.loadGpxFile(source);
		}
	}

	public static WebGpxParser.TrackData trackData() throws IOException {
		return gpxService().buildTrackDataFromGpxFile(load(), null);
	}

	public static GpxService gpxService() {
		GpxService gpxService = new GpxService();
		ReflectionTestUtils.setField(gpxService, "webGpxParser", new WebGpxParser());

		return gpxService;
	}

	public static String trackDataJson(Gson gson) throws IOException {
		return gson.toJson(trackData());
	}

	// as prepareTrackData of the web map sends the track back to save it: without the appearance and the analysis
	public static String sentBackJson(Gson gson) throws IOException {
		WebGpxParser.TrackData data = trackData();
		data.trackAppearance = null;
		data.analysis = null;

		return gson.toJson(data);
	}
}
