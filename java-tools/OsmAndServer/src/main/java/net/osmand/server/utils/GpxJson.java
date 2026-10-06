package net.osmand.server.utils;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.LinkedHashMap;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParseException;
import com.google.gson.TypeAdapter;
import com.google.gson.TypeAdapterFactory;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import net.osmand.shared.gpx.primitives.GpxExtensions;

/**
 * Gson for GPX objects sent to and from the web map.
 * <p>
 * OsmAnd-shared keeps the extensions of a point in flat arrays (extensionsArray, deferredArray) behind the
 * extensions / deferredExtensions properties; Gson serializes fields, so without this the web gets the arrays and
 * reads no tags. The JSON keeps the shape it had before: "extensions" and "deferredExtensions" as objects.
 * The arrays are still read, as a JSON written before this fix carries them.
 */
public class GpxJson {

	private static final String EXTENSIONS = "extensions";
	private static final String DEFERRED_EXTENSIONS = "deferredExtensions";
	private static final String EXTENSIONS_ARRAY = "extensionsArray";
	private static final String DEFERRED_ARRAY = "deferredArray";
	private static final TypeToken<Map<String, String>> MAP_TYPE = new TypeToken<Map<String, String>>() {};

	public static Gson create() {
		return new GsonBuilder().registerTypeAdapterFactory(new ExtensionsFactory()).create();
	}

	public static Gson createWithNans() {
		return new GsonBuilder().registerTypeAdapterFactory(new ExtensionsFactory())
				.serializeSpecialFloatingPointValues().create();
	}

	private static class ExtensionsFactory implements TypeAdapterFactory {

		@Override
		public <T> TypeAdapter<T> create(Gson gson, TypeToken<T> type) {
			if (!GpxExtensions.class.isAssignableFrom(type.getRawType())) {
				return null;
			}
			return new ExtensionsAdapter<>(gson, type.getRawType());
		}
	}

	// the fields as Gson's reflective adapter binds them, with the arrays written as objects
	private static class ExtensionsAdapter<T> extends TypeAdapter<T> {

		private final Gson gson;
		private final Class<? super T> rawType;
		private final Map<String, BoundField> fields = new LinkedHashMap<>();
		private final TypeAdapter<Map<String, String>> mapAdapter;

		ExtensionsAdapter(Gson gson, Class<? super T> rawType) {
			this.gson = gson;
			this.rawType = rawType;
			this.mapAdapter = gson.getAdapter(MAP_TYPE);
			for (Class<?> c = rawType; c != Object.class; c = c.getSuperclass()) {
				for (Field field : c.getDeclaredFields()) {
					int modifiers = field.getModifiers();
					if (Modifier.isStatic(modifiers) || Modifier.isTransient(modifiers) || field.isSynthetic()) {
						continue;
					}
					field.setAccessible(true);
					fields.putIfAbsent(field.getName(), new BoundField(field, gson.getAdapter(TypeToken.get(field.getGenericType()))));
				}
			}
		}

		@Override
		public void write(JsonWriter out, T value) throws IOException {
			if (value == null) {
				out.nullValue();
				return;
			}
			out.beginObject();
			for (Map.Entry<String, BoundField> e : fields.entrySet()) {
				String name = e.getKey();
				if (name.equals(EXTENSIONS_ARRAY) || name.equals(DEFERRED_ARRAY)) {
					continue;
				}
				Object fieldValue = e.getValue().get(value);
				if (fieldValue != value) {
					out.name(name);
					e.getValue().adapter(gson, fieldValue).write(out, fieldValue);
				}
			}
			GpxExtensions g = (GpxExtensions) value;
			writeMap(out, EXTENSIONS, g.getExtensions());
			writeMap(out, DEFERRED_EXTENSIONS, g.getDeferredExtensions());
			out.endObject();
		}

		private void writeMap(JsonWriter out, String name, Map<String, String> map) throws IOException {
			if (map != null) {
				out.name(name);
				mapAdapter.write(out, map);
			}
		}

		@Override
		public T read(JsonReader in) throws IOException {
			if (in.peek() == JsonToken.NULL) {
				in.nextNull();
				return null;
			}
			T value = newInstance();
			GpxExtensions g = (GpxExtensions) value;
			in.beginObject();
			while (in.hasNext()) {
				String name = in.nextName();
				BoundField field = fields.get(name);
				if (name.equals(EXTENSIONS)) {
					g.setExtensions(mapAdapter.read(in));
				} else if (name.equals(DEFERRED_EXTENSIONS)) {
					g.setDeferredExtensions(mapAdapter.read(in));
				} else if (field != null) {
					field.read(in, value);
				} else {
					in.skipValue();
				}
			}
			in.endObject();
			return value;
		}

		@SuppressWarnings("unchecked")
		private T newInstance() {
			try {
				Constructor<? super T> constructor = rawType.getDeclaredConstructor();
				constructor.setAccessible(true);
				return (T) constructor.newInstance();
			} catch (ReflectiveOperationException e) {
				throw new JsonParseException("Can't create " + rawType.getName(), e);
			}
		}
	}

	private static class BoundField {

		private final Field field;
		private final TypeAdapter<Object> adapter;

		@SuppressWarnings("unchecked")
		BoundField(Field field, TypeAdapter<?> adapter) {
			this.field = field;
			this.adapter = (TypeAdapter<Object>) adapter;
		}

		Object get(Object owner) {
			try {
				return field.get(owner);
			} catch (IllegalAccessException e) {
				throw new JsonParseException("Can't read " + field.getName(), e);
			}
		}

		// as Gson does: a field of a plain class type is written with the adapter of its runtime class
		@SuppressWarnings("unchecked")
		TypeAdapter<Object> adapter(Gson gson, Object fieldValue) {
			if (fieldValue != null && !field.getType().isPrimitive() && field.getGenericType() instanceof Class
					&& fieldValue.getClass() != field.getType()) {
				return (TypeAdapter<Object>) gson.getAdapter(fieldValue.getClass());
			}
			return adapter;
		}

		void read(JsonReader in, Object owner) throws IOException {
			Object fieldValue = adapter.read(in);
			if (fieldValue == null && field.getType().isPrimitive()) {
				return;
			}
			try {
				field.set(owner, fieldValue);
			} catch (IllegalAccessException e) {
				throw new JsonParseException("Can't write " + field.getName(), e);
			}
		}
	}
}
