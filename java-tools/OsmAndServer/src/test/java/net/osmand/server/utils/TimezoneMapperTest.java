package net.osmand.server.utils;

import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeNoException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.junit.Test;

// TimezoneMapper against the GeoNames city list: every city of 15000+ people with its IANA time zone.
// A city fails when the mapper gives it a different local time on one of four days of the year (a zone
// with another name but the same clock, Europe/Kiev for Europe/Kyiv, is fine). The list (3.4 MB zip) is kept in
// build/geonames and downloaded again when older than a week; with no copy and no network the test is skipped.
// The full list of wrong cities is in build/timezone-mismatches.txt.
public class TimezoneMapperTest {

	private static final String CITIES_URL = "https://download.geonames.org/export/dump/cities15000.zip";
	private static final Path CITIES_ZIP = Path.of("build", "geonames", "cities15000.zip");
	// GeoNames updates the list every day, a week is fresh enough for time zone borders
	private static final Duration REFRESH = Duration.ofDays(7);
	private static final Path REPORT = Path.of("build", "timezone-mismatches.txt");
	private static final int SHOWN = 25;

	private record City(String name, String country, double lat, double lon, long population, String timeZone) {
	}

	private record Mismatch(City city, String mapped) {
		@Override
		public String toString() {
			return String.format("%s (%s) %.5f,%.5f pop %d: %s, mapper %s", city.name, city.country, city.lat,
					city.lon, city.population, city.timeZone, mapped);
		}
	}

	@Test
	public void citiesHaveTheirLocalTime() throws IOException {
		List<City> cities = loadCities();
		Instant[] days = daysOfThisYear();
		List<Mismatch> wrong = new ArrayList<>();
		for (City city : cities) {
			String mapped = TimezoneMapper.latLngToTimezoneString(city.lat, city.lon);
			if (!sameClock(city.timeZone, mapped, days)) {
				wrong.add(new Mismatch(city, mapped));
			}
		}
		wrong.sort(Comparator.comparingLong((Mismatch m) -> m.city.population).reversed());

		StringBuilder report = new StringBuilder();
		wrong.forEach(m -> report.append(m).append('\n'));
		Files.createDirectories(REPORT.getParent());
		Files.writeString(REPORT, report);

		StringBuilder message = new StringBuilder(String.format("%d of %d cities get a wrong local time, largest:%n",
				wrong.size(), cities.size()));
		wrong.stream().limit(SHOWN).forEach(m -> message.append(m).append('\n'));
		assertTrue(message.toString(), wrong.isEmpty());
	}

	// the zones may have different names, the clock must be the same in winter, summer and around the switches
	private static boolean sameClock(String expected, String mapped, Instant[] days) {
		ZoneId expectedZone;
		try {
			expectedZone = ZoneId.of(expected);
		} catch (RuntimeException e) {
			return true; // a zone unknown to this JVM can't be compared
		}
		ZoneId mappedZone;
		try {
			mappedZone = ZoneId.of(mapped);
		} catch (RuntimeException e) {
			return false; // "unknown" or a removed id
		}
		for (Instant day : days) {
			if (!expectedZone.getRules().getOffset(day).equals(mappedZone.getRules().getOffset(day))) {
				return false;
			}
		}
		return true;
	}

	private static Instant[] daysOfThisYear() {
		int year = LocalDate.now(ZoneOffset.UTC).getYear();
		return new Instant[] { day(year, 1, 15), day(year, 4, 1), day(year, 7, 15), day(year, 10, 30) };
	}

	private static Instant day(int year, int month, int dayOfMonth) {
		return LocalDate.of(year, month, dayOfMonth).atTime(12, 0).toInstant(ZoneOffset.UTC);
	}

	private static List<City> loadCities() throws IOException {
		boolean exists = Files.exists(CITIES_ZIP);
		if (!exists || Files.getLastModifiedTime(CITIES_ZIP).toInstant().isBefore(Instant.now().minus(REFRESH))) {
			try {
				Files.createDirectories(CITIES_ZIP.getParent());
				Path tmp = Files.createTempFile(CITIES_ZIP.getParent(), "cities", ".zip");
				try (InputStream in = URI.create(CITIES_URL).toURL().openStream()) {
					Files.copy(in, tmp, StandardCopyOption.REPLACE_EXISTING);
				}
				Files.move(tmp, CITIES_ZIP, StandardCopyOption.REPLACE_EXISTING);
			} catch (IOException e) {
				if (!exists) {
					assumeNoException("GeoNames is not reachable", e);
				}
				// offline: the older copy still checks the mapper
			}
		}
		List<City> cities = new ArrayList<>();
		try (ZipInputStream zip = new ZipInputStream(Files.newInputStream(CITIES_ZIP))) {
			ZipEntry entry;
			while ((entry = zip.getNextEntry()) != null) {
				if (!entry.getName().endsWith(".txt")) {
					continue;
				}
				BufferedReader reader = new BufferedReader(new InputStreamReader(zip, StandardCharsets.UTF_8));
				String line;
				while ((line = reader.readLine()) != null) {
					// geonameid, name, asciiname, alternatenames, latitude, longitude, feature class, feature code,
					// country code, cc2, admin1-4, population, elevation, dem, timezone, modification date
					String[] f = line.split("\t", -1);
					long population = f[14].isEmpty() ? 0 : Long.parseLong(f[14]);
					cities.add(new City(f[1], f[8], Double.parseDouble(f[4]), Double.parseDouble(f[5]), population,
							f[17]));
				}
				break;
			}
		}
		assertTrue("too few cities in " + CITIES_ZIP + ": " + cities.size(), cities.size() > 20000);
		return cities;
	}
}
