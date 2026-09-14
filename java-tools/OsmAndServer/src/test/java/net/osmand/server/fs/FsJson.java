package net.osmand.server.fs;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Objects;

import com.google.gson.Gson;

// FastSpring API responses and webhooks recorded in src/test/resources/fs
class FsJson {

	static <T> T read(String name, Class<T> type) throws IOException {
		try (InputStream in = FsJson.class.getResourceAsStream("/fs/" + name)) {
			return new Gson().fromJson(new InputStreamReader(Objects.requireNonNull(in, name)), type);
		}
	}
}
