package net.osmand.server.utils;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import org.junit.BeforeClass;
import org.junit.Test;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

public class TimezoneMapperTest {

	private static JsonObject aliases;

	@BeforeClass
	public static void loadAliases() throws IOException {
		aliases = readResource("TimezoneMapperAliases.json");
	}

	@Test
	public void ianaCoordinatesMatchTimezones() throws IOException {
		checkTimezones("TimezoneMapperIANA.json");
	}

	@Test
	public void basemapCitiesCoordinatesMatchTimezones() throws IOException {
		checkTimezones("TimezoneMapperBasemapCities.json");
	}

	private static void checkTimezones(String resource) throws IOException {
		TimezoneCase[] cases = new Gson().fromJson(readResource(resource).get("cases"), TimezoneCase[].class);
		assertTrue("No timezone cases: " + resource, cases.length > 0);
		List<String> mismatches = new ArrayList<>();
		for (TimezoneCase testCase : cases) {
			String actual = TimezoneMapper.latLngToTimezoneString(testCase.lat, testCase.lon);
			String normalized = normalize(actual);
			boolean matches = normalize(testCase.timezone).equals(normalized);
			if (!matches && testCase.alternatives != null) {
				matches = Arrays.stream(testCase.alternatives)
						.anyMatch(timezone -> normalize(timezone).equals(normalized));
			}
			if (!matches) {
				mismatches.add(testCase.city + " (" + testCase.lat + ", " + testCase.lon
						+ "): expected=" + testCase.timezone
						+ (testCase.alternatives == null ? ""
								: " alternatives=" + Arrays.toString(testCase.alternatives))
						+ " actual=" + actual);
			}
		}
		String summary = resource + ": total=" + cases.length + " mismatches=" + mismatches.size();
		System.out.println(summary);
		assertTrue(summary + "\n" + String.join("\n", mismatches), mismatches.isEmpty());
	}

	private static JsonObject readResource(String filename) throws IOException {
		String resource = "/test-resources/tz/" + filename;
		try (InputStream input = TimezoneMapperTest.class.getResourceAsStream(resource);
				InputStreamReader reader = new InputStreamReader(
						Objects.requireNonNull(input, resource), StandardCharsets.UTF_8)) {
			return new Gson().fromJson(reader, JsonObject.class);
		}
	}

	private static String normalize(String timezone) {
		while (aliases.has(timezone)) {
			timezone = aliases.get(timezone).getAsString();
		}
		return timezone;
	}

	private static class TimezoneCase {
		String city;
		double lat;
		double lon;
		String timezone;
		String[] alternatives;
	}
}
