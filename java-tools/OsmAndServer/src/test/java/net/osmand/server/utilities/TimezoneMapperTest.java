package net.osmand.server.utilities;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

import org.junit.BeforeClass;
import org.junit.Test;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

public class TimezoneMapperTest {

	private static JsonObject aliases;

	@BeforeClass
	public static void loadAliases() throws IOException {
		aliases = readResource(TimezoneMapperDataGenerator.ALIASES_FILE);
	}

	@Test
	public void ianaCoordinatesMatchTimezones() throws IOException {
		checkTimezones(TimezoneMapperDataGenerator.IANA_FILE);
	}

	@Test
	public void basemapCitiesCoordinatesMatchTimezones() throws IOException {
		checkTimezones(TimezoneMapperDataGenerator.CITIES_FILE);
	}

	private static void checkTimezones(String resource) throws IOException {
		TimezoneMapperDataGenerator.checkTimezoneMappings(resource, readResource(resource), aliases,
				TimezoneMapper::latLngToTimezoneString)
				.assertTimezonesMatch();
	}

	private static JsonObject readResource(String filename) throws IOException {
		String resource = "/test-resources/tz/" + filename;
		try (InputStream input = TimezoneMapperTest.class.getResourceAsStream(resource);
				InputStreamReader reader = new InputStreamReader(
						Objects.requireNonNull(input, resource), StandardCharsets.UTF_8)) {
			return new Gson().fromJson(reader, JsonObject.class);
		}
	}
}
