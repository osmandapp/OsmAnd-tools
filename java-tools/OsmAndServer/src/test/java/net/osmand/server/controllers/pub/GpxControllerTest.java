package net.osmand.server.controllers.pub;

import static net.osmand.server.utils.WebGpxTestData.sentBackJson;
import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPOutputStream;

import org.junit.Test;
import org.springframework.mock.web.MockHttpSession;
import org.springframework.test.util.ReflectionTestUtils;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import net.osmand.server.api.services.GpxService;
import net.osmand.server.utils.GpxJson;
import net.osmand.server.utils.WebGpxParser;

// /gpx/save-track-data and /gpx/get-analysis with the track data the web map sends back
public class GpxControllerTest {

	private static final String[] SAVED_TAGS = { "amenity_opening_hours", "osm_tag_phone", "my_note", "hr>120",
			"custom_pt", "custom_trk", "custom_meta" };

	// /gpx/save-track-data: the GPX keeps the extensions of the waypoints, track points, track and metadata
	@Test
	public void saveTrackDataKeepsExtensions() throws IOException {
		assertSavedTags(saveTrackData(sentBackJson(GpxJson.createWithNans())));
	}

	// /gpx/save-track-data: a track the web map got before GpxJson carries the extensions as flat arrays
	@Test
	public void saveTrackDataReadsExtensionsArray() throws IOException {
		assertSavedTags(saveTrackData(sentBackJson(new GsonBuilder().serializeSpecialFloatingPointValues().create())));
	}

	// /gpx/get-analysis: the track comes back with the extensions of its points
	@Test
	public void getAnalysisKeepsExtensions() throws IOException {
		String json = controller().getAnalysis(gzip(sentBackJson(GpxJson.createWithNans()))).getBody();
		JsonObject data = new JsonParser().parse(json).getAsJsonObject().getAsJsonObject("data");
		JsonObject routePoint = data.getAsJsonArray("tracks").get(0).getAsJsonObject().getAsJsonArray("points").get(1)
				.getAsJsonObject();
		JsonObject trackPoint = routePoint.getAsJsonArray("geometry").get(0).getAsJsonObject();
		JsonObject wpt = data.getAsJsonArray("wpts").get(0).getAsJsonObject();
		assertEquals("p0", trackPoint.getAsJsonObject("ext").getAsJsonObject("extensions").get("custom_pt").getAsString());
		assertEquals("hello", wpt.getAsJsonObject("ext").getAsJsonObject("extensions").get("my_note").getAsString());
	}

	private static void assertSavedTags(String gpx) {
		for (String tag : SAVED_TAGS) {
			assertTrue(tag, gpx.contains(tag));
		}
	}

	private static String saveTrackData(String json) throws IOException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		controller().saveTrackData(gzip(json), false, new MockHttpSession()).getBody().writeTo(out);

		return out.toString(StandardCharsets.UTF_8);
	}

	private static GpxController controller() {
		WebGpxParser parser = new WebGpxParser();
		GpxService gpxService = new GpxService();
		ReflectionTestUtils.setField(gpxService, "webGpxParser", parser);
		GpxController controller = new GpxController();
		controller.webGpxParser = parser;
		controller.gpxService = gpxService;
		controller.session = new UserSessionResources();

		return controller;
	}

	// the web map sends the track data gzipped
	private static byte[] gzip(String json) throws IOException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		try (GZIPOutputStream gzip = new GZIPOutputStream(out)) {
			gzip.write(json.getBytes(StandardCharsets.UTF_8));
		}

		return out.toByteArray();
	}
}
