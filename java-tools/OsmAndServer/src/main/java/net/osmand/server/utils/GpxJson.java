package net.osmand.server.utils;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.LinkedHashMap;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.TypeAdapter;
import com.google.gson.TypeAdapterFactory;
import com.google.gson.internal.bind.JsonTreeReader;
import com.google.gson.internal.bind.JsonTreeWriter;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

import net.osmand.shared.gpx.primitives.GpxExtensions;

/**
 * Gson for GPX objects sent to and from the web map.
 * <p>
 * OsmAnd-shared keeps the extensions of a point in flat arrays (extensionsArray, deferredArray) behind the
 * extensions / deferredExtensions properties; Gson serializes fields, so without this the web gets the arrays and
 * reads no tags. The JSON keeps the shape it had before: "extensions" and "deferredExtensions" as objects.
 */
public class GpxJson {

	private static final String EXTENSIONS = "extensions";
	private static final String DEFERRED = "deferredExtensions";
	private static final Type MAP_TYPE = new TypeToken<LinkedHashMap<String, String>>() {}.getType();

	public static GsonBuilder builder() {
		return new GsonBuilder().registerTypeAdapterFactory(new ExtensionsFactory());
	}

	public static Gson create() {
		return builder().create();
	}

	public static Gson createWithNans() {
		return builder().serializeSpecialFloatingPointValues().create();
	}

	private static class ExtensionsFactory implements TypeAdapterFactory {

		@Override
		public <T> TypeAdapter<T> create(Gson gson, TypeToken<T> type) {
			if (!GpxExtensions.class.isAssignableFrom(type.getRawType())) {
				return null;
			}
			TypeAdapter<T> delegate = gson.getDelegateAdapter(this, type);
			TypeAdapter<JsonElement> elements = gson.getAdapter(JsonElement.class);
			return new TypeAdapter<T>() {
				@Override
				public void write(JsonWriter out, T value) throws IOException {
					if (value == null) {
						out.nullValue();
						return;
					}
					// a lenient tree writer: points carry NaN (ele, speed...), the outer writer decides about them
					JsonTreeWriter treeWriter = new JsonTreeWriter();
					treeWriter.setLenient(true);
					delegate.write(treeWriter, value);
					JsonElement tree = treeWriter.get();
					if (tree.isJsonObject()) {
						JsonObject o = tree.getAsJsonObject();
						o.remove("extensionsArray");
						o.remove("deferredArray");
						GpxExtensions g = (GpxExtensions) value;
						put(o, EXTENSIONS, g.getExtensions());
						put(o, DEFERRED, g.getDeferredExtensions());
					}
					elements.write(out, tree);
				}

				private void put(JsonObject o, String key, Map<String, String> map) {
					if (map != null) {
						o.add(key, gson.toJsonTree(new LinkedHashMap<>(map), MAP_TYPE));
					}
				}

				@Override
				public T read(JsonReader in) throws IOException {
					JsonElement tree = elements.read(in);
					if (tree == null || tree.isJsonNull()) {
						return null;
					}
					JsonElement ext = null, deferred = null;
					if (tree.isJsonObject()) {
						ext = tree.getAsJsonObject().remove(EXTENSIONS);
						deferred = tree.getAsJsonObject().remove(DEFERRED);
					}
					JsonTreeReader treeReader = new JsonTreeReader(tree);
					treeReader.setLenient(true);
					T value = delegate.read(treeReader);
					if (value != null) {
						GpxExtensions g = (GpxExtensions) value;
						if (ext != null && ext.isJsonObject()) {
							g.setExtensions(gson.fromJson(ext, MAP_TYPE));
						}
						if (deferred != null && deferred.isJsonObject()) {
							g.setDeferredExtensions(gson.fromJson(deferred, MAP_TYPE));
						}
					}
					return value;
				}
			};
		}
	}
}
