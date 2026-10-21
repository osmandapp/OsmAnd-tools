package net.osmand.server.utils;

import static net.osmand.server.utils.WebGpxTestData.trackDataJson;
import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.Test;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

// GpxJson: GPX objects of OsmAnd-shared to and from the JSON of the web map
public class GpxJsonTest {

	// Gson without GpxJson: point extensions go out as the flat arrays of OsmAnd-shared
	private static final Gson PLAIN = new GsonBuilder().serializeSpecialFloatingPointValues().create();

	@Test
	public void writesExtensionsAsObject() throws IOException {
		JsonObject ext = wptExt(trackDataJson(GpxJson.createWithNans()));
		assertFalse(ext.has("extensionsArray"));
		JsonObject extensions = ext.getAsJsonObject("extensions");
		assertEquals("Mo-Fr 07:00-21:00; Sa 09:00-21:00; Su 09:00-19:30", extensions.get("amenity_opening_hours").getAsString());
		assertEquals("Starbucks", extensions.get("osm_tag_brand").getAsString());
	}

	@Test
	public void plainGsonWritesArrays() throws IOException {
		JsonObject ext = wptExt(trackDataJson(PLAIN));
		assertTrue(ext.has("extensionsArray"));
		assertFalse(ext.has("extensions"));
	}

	@Test
	public void skipsDefaultValues() throws IOException {
		JsonObject ext = new JsonParser().parse(trackDataJson(GpxJson.createWithNans())).getAsJsonObject()
				.getAsJsonArray("tracks").get(0).getAsJsonObject().getAsJsonArray("points").get(0).getAsJsonObject()
				.getAsJsonObject("ext");
		assertFalse(ext.has("hdop"));
		assertFalse(ext.has("deleted"));
		assertTrue(ext.has("lat"));
	}

	@Test
	public void readsSkippedFieldsAsDefaults() throws IOException {
		WebGpxParser.TrackData all = GpxJson.create().fromJson(trackDataJson(PLAIN), WebGpxParser.TrackData.class);
		WebGpxParser.TrackData skipped = GpxJson.create().fromJson(trackDataJson(GpxJson.createWithNans()),
				WebGpxParser.TrackData.class);
		assertEquals(PLAIN.toJson(all), PLAIN.toJson(skipped));
	}

	// the JSON of the web map written before GpxJson carries the arrays
	@Test
	public void readsExtensionsArray() throws IOException {
		WebGpxParser.TrackData data = GpxJson.create().fromJson(trackDataJson(PLAIN), WebGpxParser.TrackData.class);
		assertEquals("Mo-Fr 07:00-21:00; Sa 09:00-21:00; Su 09:00-19:30", data.wpts.get(0).ext.getExtensions().get("amenity_opening_hours"));
	}

	// JSON.stringify of the web map writes NaN as null
	@Test
	public void readsNullAsDefault() throws IOException {
		String json = trackDataJson(GpxJson.createWithNans()).replace("NaN", "null");
		WebGpxParser.TrackData data = GpxJson.create().fromJson(json, WebGpxParser.TrackData.class);
		assertTrue(Double.isNaN(data.wpts.get(0).ext.getHdop()));
		assertEquals("Starbucks", data.wpts.get(0).ext.getExtensions().get("osm_tag_brand"));
	}

	private static JsonObject wptExt(String json) {
		return new JsonParser().parse(json).getAsJsonObject().getAsJsonArray("wpts").get(0).getAsJsonObject()
				.getAsJsonObject("ext");
	}
}
