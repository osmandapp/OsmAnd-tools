package net.osmand.server.controllers.pub;

import static net.osmand.server.utils.WebGpxTestData.gpxService;
import static net.osmand.server.utils.WebGpxTestData.sentBackJson;
import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPOutputStream;

import org.junit.Test;
import org.springframework.mock.web.MockHttpSession;
import org.springframework.mock.web.MockMultipartFile;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import net.osmand.server.utils.GpxJson;
import net.osmand.server.utils.WebGpxParser;
import net.osmand.server.utils.exception.OsmAndPublicApiException;

// /gpx/save-track-data and /gpx/get-analysis with the track data the web map sends back, /gpx/process-track-data with the file
public class GpxControllerTest {

	private static final String[] SAVED_TAGS = { "amenity_opening_hours", "osm_tag_brand", "United States", "hr>94",
			"displaycolor", "activity>hiking" };

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
		assertEquals("94", trackPoint.getAsJsonObject("ext").getAsJsonObject("extensions").get("gpxtpx:hr").getAsString());
		assertEquals("Starbucks", wpt.getAsJsonObject("ext").getAsJsonObject("extensions").get("osm_tag_brand").getAsString());
	}

	// /gpx/process-track-data: the web map sends the file gzipped, an old web map sends it as is
	@Test
	public void processTrackDataReadsGzippedFile() throws IOException {
		byte[] gpx;
		try (InputStream in = GpxControllerTest.class.getResourceAsStream("/gpx/points-with-extensions.gpx")) {
			gpx = in.readAllBytes();
		}
		assertEquals(processTrackData(gpx), processTrackData(gzip(gpx)));
	}

	// /gpx/process-track-data: a gzipped file is limited once unzipped, as nginx limits a plain one
	@Test
	public void processTrackDataRejectsLargeUnzippedFile() throws IOException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		try (GZIPOutputStream gzip = new GZIPOutputStream(out)) {
			byte[] mb = new byte[1 << 20];
			for (int i = 0; i <= 100; i++) {
				gzip.write(mb);
			}
		}
		assertThrows(OsmAndPublicApiException.class, () -> processTrackData(out.toByteArray()));
	}

	private static String processTrackData(byte[] file) throws IOException {
		MockMultipartFile part = new MockMultipartFile("file", "points-with-extensions.gpx", null, file);

		return controller().processTrackData(part, new MockHttpSession()).getBody();
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
		GpxController controller = new GpxController();
		controller.gpxService = gpxService();
		controller.webGpxParser = new WebGpxParser();
		controller.session = new UserSessionResources();

		return controller;
	}

	// the web map sends the track data gzipped
	private static byte[] gzip(String json) throws IOException {
		return gzip(json.getBytes(StandardCharsets.UTF_8));
	}

	private static byte[] gzip(byte[] data) throws IOException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		try (GZIPOutputStream gzip = new GZIPOutputStream(out)) {
			gzip.write(data);
		}

		return out.toByteArray();
	}
}
