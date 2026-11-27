package net.osmand.server.utilities;

import java.io.IOException;

import org.junit.BeforeClass;
import org.junit.Test;

import com.google.gson.JsonObject;

import net.osmand.server.utilities.TimezoneMapperTestDataGenerator.TimezoneTestData;

public class TimezoneMapperTest {

	private static TimezoneTestData data;

	@BeforeClass
	public static void loadTestData() throws IOException {
		data = TimezoneMapperTestDataGenerator.readTestDataResources();
	}

	@Test
	public void ianaCoordinatesMatchTimezones() {
		checkTimezones(TimezoneMapperTestDataGenerator.IANA_FILE, data.iana());
	}

	@Test
	public void basemapCitiesCoordinatesMatchTimezones() {
		checkTimezones(TimezoneMapperTestDataGenerator.CITIES_FILE, data.cities());
	}

	private static void checkTimezones(String resource, JsonObject cases) {
		TimezoneMapperTestDataGenerator.checkTimezoneMappings(resource, cases, data.aliases(),
				TimezoneMapper::latLngToTimezoneString)
				.assertTimezonesMatch();
	}
}
