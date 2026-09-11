package net.osmand.server.traffic.feeds;

import net.osmand.server.traffic.TrafficFeed;
import net.osmand.server.traffic.TrafficFeeds;
import net.osmand.server.traffic.TrafficSensor;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPInputStream;

/**
 * Paris permanent road sensors (opendata.paris.fr "comptages-routiers-permanents"): hourly flow, occupancy and state
 * per street arc with its geometry. Published the next night, so the last RETENTION_DAYS complete days are fetched.
 */
public class ParisFeed extends TrafficFeed {

	private static final ZoneId PARIS = ZoneId.of("Europe/Paris");
	private static final String EXPORT = "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/comptages-routiers-permanents/exports/csv";
	private static final DateTimeFormatter ODSQL_TIME = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'+00:00'");
	private static final String FILE = "counts.csv.gz";

	@Override
	public String id() {
		return "paris";
	}

	@Override
	public String name() {
		return "Paris";
	}

	@Override
	public ZoneId zone() {
		return PARIS;
	}

	@Override
	public List<String> obfFiles() {
		return List.of("France_ile-de-france_europe_2.obf");
	}

	@Override
	public double defaultSpeedKmh() {
		return 30;
	}

	@Override
	public Duration publishInterval() {
		return Duration.ofDays(1);
	}

	@Override
	public Duration valueWindow() {
		return Duration.ofHours(1);
	}

	@Override
	public boolean keepsHistory() {
		return true;
	}

	@Override
	public void download(LocalDate today) throws Exception {
		for (int i = 1; i <= TrafficFeeds.RETENTION_DAYS; i++) {
			LocalDate day = today.minusDays(i);
			File target = new File(rawDir(day), FILE);
			if (target.exists()) {
				continue;
			}
			Instant from = day.atStartOfDay(PARIS).toInstant(), to = day.plusDays(1).atStartOfDay(PARIS).toInstant();
			// t_1h is the END of the counted hour
			String where = "t_1h > '" + ODSQL_TIME.format(from.atOffset(ZoneOffset.UTC)) + "' and t_1h <= '" + ODSQL_TIME.format(to.atOffset(ZoneOffset.UTC)) + "'";
			File part = new File(staticDir(), day + ".csv.gz");
			download(EXPORT + "?where=" + encode(where) + "&timezone=UTC&delimiter=" + encode(";"), part, true);
			int hours = hoursIn(part), expected = (int) Duration.between(from, to).toHours();
			if (hours < expected) {
				Files.delete(part.toPath());
				TrafficFeeds.log("  %s: %d of %d hours published so far, not kept", day, hours, expected);
				continue;
			}
			rawDir(day).mkdirs();
			Files.move(part.toPath(), target.toPath(), StandardCopyOption.REPLACE_EXISTING);
			TrafficFeeds.log("  %s: downloaded %d hours", day, hours);
		}
	}

	private static int hoursIn(File csvGz) throws IOException {
		Set<String> hours = new HashSet<>();
		try (BufferedReader reader = open(csvGz)) {
			List<String> cols = splitCsv(reader.readLine().replace("﻿", ""), ';');
			int iTime = cols.indexOf("t_1h");
			String line;
			while ((line = reader.readLine()) != null) {
				hours.add(splitCsv(line, ';').get(iTime));
			}
		}
		return hours.size();
	}

	@Override
	public Day read(LocalDate day) throws IOException {
		Day result = new Day();
		result.source = "Ville de Paris, Comptage routier - Données trafic issues des capteurs permanents (opendata.paris.fr), ODbL";
		Map<String, TrafficSensor> sensors = new LinkedHashMap<>();
		try (BufferedReader reader = open(new File(rawDir(day), FILE))) {
			List<String> cols = splitCsv(reader.readLine().replace("﻿", ""), ';');
			int iId = cols.indexOf("iu_ac"), iName = cols.indexOf("libelle"), iTime = cols.indexOf("t_1h");
			int iQ = cols.indexOf("q"), iK = cols.indexOf("k"), iState = cols.indexOf("etat_trafic");
			int iFrom = cols.indexOf("libelle_nd_amont"), iTo = cols.indexOf("libelle_nd_aval");
			int iRoad = cols.indexOf("etat_barre"), iGeo = cols.indexOf("geo_shape");
			String line;
			while ((line = reader.readLine()) != null) {
				List<String> f = splitCsv(line, ';');
				ZonedDateTime start = OffsetDateTime.parse(f.get(iTime)).atZoneSameInstant(PARIS).minusHours(1);
				if (!start.toLocalDate().equals(day) || f.get(iGeo).isEmpty()) {
					continue;
				}
				TrafficSensor s = sensors.get(f.get(iId));
				if (s == null) {
					JSONObject geometry = new JSONObject(f.get(iGeo));
					if (!"LineString".equals(geometry.optString("type"))) {
						continue;
					}
					s = new TrafficSensor(f.get(iId));
					s.name = f.get(iName).replace('_', ' ');
					s.from = f.get(iFrom).replace('_', ' ');
					s.to = f.get(iTo).replace('_', ' ');
					s.directional = true;
					JSONArray coords = geometry.getJSONArray("coordinates");
					s.lat = new double[coords.length()];
					s.lon = new double[coords.length()];
					for (int i = 0; i < coords.length(); i++) {
						s.lon[i] = coords.getJSONArray(i).getDouble(0);
						s.lat[i] = coords.getJSONArray(i).getDouble(1);
					}
					sensors.put(s.id, s);
				}
				int h = start.getHour();
				s.flow[h] = parse(f.get(iQ));
				s.occupancy[h] = parse(f.get(iK));
				s.state[h] = switch (f.get(iState)) {
					case "Fluide" -> TrafficSensor.STATE_FLUID;
					case "Pré-saturé" -> TrafficSensor.STATE_PRE_SATURATED;
					case "Saturé" -> TrafficSensor.STATE_SATURATED;
					case "Bloqué" -> TrafficSensor.STATE_BLOCKED;
					default -> TrafficSensor.STATE_UNKNOWN;
				};
				s.roadState[h] = switch (f.get(iRoad)) {
					case "Ouvert" -> 1;
					case "Barré" -> 2;
					case "Invalide" -> 3;
					default -> 0;
				};
			}
		}
		result.sensors.addAll(sensors.values());
		return result;
	}

	private static BufferedReader open(File csvGz) throws IOException {
		return new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(csvGz)), StandardCharsets.UTF_8));
	}
}
