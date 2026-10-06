package net.osmand.server.utils;

import static net.osmand.server.utils.WebGpxTestData.trackDataJson;
import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.Test;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
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
		assertEquals("Mo-Fr 08:00-20:00", extensions.get("amenity_opening_hours").getAsString());
		assertEquals("+380441234567", extensions.get("osm_tag_phone").getAsString());
		assertEquals("hello", extensions.get("my_note").getAsString());
	}

	@Test
	public void plainGsonWritesArrays() throws IOException {
		JsonObject ext = wptExt(trackDataJson(PLAIN));
		assertTrue(ext.has("extensionsArray"));
		assertFalse(ext.has("extensions"));
	}

	// all but the extensions is written as Gson writes the fields
	@Test
	public void otherFieldsAsPlainGson() throws IOException {
		JsonElement plain = new JsonParser().parse(trackDataJson(PLAIN));
		arraysToObjects(plain);
		assertEquals(plain, new JsonParser().parse(trackDataJson(GpxJson.createWithNans())));
	}

	// the JSON of the web map written before GpxJson carries the arrays
	@Test
	public void readsExtensionsArray() throws IOException {
		WebGpxParser.TrackData data = GpxJson.create().fromJson(trackDataJson(PLAIN), WebGpxParser.TrackData.class);
		assertEquals("Mo-Fr 08:00-20:00", data.wpts.get(0).ext.getExtensions().get("amenity_opening_hours"));
	}

	// JSON.stringify of the web map writes NaN as null
	@Test
	public void readsNullAsDefault() throws IOException {
		String json = trackDataJson(GpxJson.createWithNans()).replace("NaN", "null");
		WebGpxParser.TrackData data = GpxJson.create().fromJson(json, WebGpxParser.TrackData.class);
		assertTrue(Double.isNaN(data.wpts.get(0).ext.getHdop()));
		assertEquals("hello", data.wpts.get(0).ext.getExtensions().get("my_note"));
	}

	private static JsonObject wptExt(String json) {
		return new JsonParser().parse(json).getAsJsonObject().getAsJsonArray("wpts").get(0).getAsJsonObject()
				.getAsJsonObject("ext");
	}

	private static void arraysToObjects(JsonElement e) {
		if (e.isJsonArray()) {
			e.getAsJsonArray().forEach(GpxJsonTest::arraysToObjects);
		} else if (e.isJsonObject()) {
			JsonObject o = e.getAsJsonObject();
			o.entrySet().forEach(entry -> arraysToObjects(entry.getValue()));
			arrayToObject(o, "extensionsArray", "extensions");
			arrayToObject(o, "deferredArray", "deferredExtensions");
		}
	}

	private static void arrayToObject(JsonObject o, String arrayName, String objectName) {
		JsonElement array = o.remove(arrayName);
		if (array == null || !array.isJsonArray()) {
			return;
		}
		JsonArray pairs = array.getAsJsonArray();
		JsonObject map = new JsonObject();
		for (int i = 0; i + 1 < pairs.size(); i += 2) {
			if (!pairs.get(i + 1).isJsonNull()) {
				map.add(pairs.get(i).getAsString(), pairs.get(i + 1));
			}
		}
		o.add(objectName, map);
	}
}
