package net.osmand.server.traffic.feeds;

import net.osmand.server.traffic.LoopSpeedEstimator;
import net.osmand.server.traffic.ObfRoadMatcher;
import net.osmand.server.traffic.TrafficFeed;
import net.osmand.server.traffic.TrafficSensor;
import org.json.JSONArray;
import org.json.JSONObject;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilderFactory;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.time.DayOfWeek;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAdjusters;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * Madrid traffic detectors: the real-time feed (informo.madrid.es, a snapshot every 5 minutes) is downloaded once per
 * run into raw/&lt;day&gt;/&lt;HHmm&gt;.xml.gz; a day averages the snapshots it has per hour. Speed is measured on the
 * M-30 detectors; elsewhere it is estimated with each detector's calibration from the latest monthly history
 * (datos.madrid.es, static/history-MM-YYYY.zip).
 */
public class MadridFeed extends TrafficFeed {

	private static final ZoneId MADRID = ZoneId.of("Europe/Madrid");
	private static final String LIVE = "https://informo.madrid.es/informo/tmadrid/pm.xml";
	private static final String CKAN = "https://datos.madrid.es/api/3/action/package_show?id=";
	private static final String LOCATIONS_PACKAGE = "202468-0-intensidad-trafico";
	private static final String HISTORY_PACKAGE = "208627-0-transporte-ptomedida-historico";
	private static final Pattern DIRECTION = Pattern.compile("\\b(NE|NO|SE|SO|N|S|E|O)-(NE|NO|SE|SO|N|S|E|O)\\b");
	private static final Pattern HISTORY_FILE = Pattern.compile("history-(\\d{2})-(\\d{4})\\.zip");
	private static final DateTimeFormatter SNAPSHOT_TIME = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss");

	@Override
	public String id() {
		return "madrid";
	}

	@Override
	public String name() {
		return "Madrid";
	}

	@Override
	public ZoneId zone() {
		return MADRID;
	}

	@Override
	public List<String> obfFiles() {
		return List.of("Spain_madrid_europe_2.obf");
	}

	@Override
	public Duration publishInterval() {
		return Duration.ofMinutes(5);
	}

	// treated as a 5-minute value: the feed does not say over how long intensidad and ocupacion are measured
	@Override
	public Duration valueWindow() {
		return Duration.ofMinutes(5);
	}

	@Override
	public void download(LocalDate today) throws Exception {
		File part = new File(staticDir(), "pm.xml.gz");
		download(LIVE, part, true);
		LocalDateTime time = snapshotTime(part);
		File target = new File(rawDir(time.toLocalDate()), time.format(DateTimeFormatter.ofPattern("HHmm")) + ".xml.gz");
		target.getParentFile().mkdirs();
		Files.move(part.toPath(), target.toPath(), StandardCopyOption.REPLACE_EXISTING);

		File locations = new File(staticDir(), "locations.csv");
		if (olderThanDays(locations, 30)) {
			download(latestResource(LOCATIONS_PACKAGE, Pattern.compile("pmed_ubicacion_(\\d{2})-(\\d{4})\\.csv$")), locations, false);
		}
		if (historyZip() == null) {
			String url = latestResource(HISTORY_PACKAGE, Pattern.compile("/(\\d{2})-(\\d{4})\\.zip$"));
			Matcher m = Pattern.compile("(\\d{2})-(\\d{4})\\.zip$").matcher(url);
			m.find();
			download(url, new File(staticDir(), "history-" + m.group(1) + "-" + m.group(2) + ".zip"), false);
		}
	}

	// newest resource of a datos.madrid.es package whose URL matches month (group 1) and year (group 2)
	private static String latestResource(String packageId, Pattern pattern) throws Exception {
		JSONArray resources = new JSONObject(downloadText(CKAN + packageId)).getJSONObject("result").getJSONArray("resources");
		String best = null;
		String bestKey = "";
		for (int i = 0; i < resources.length(); i++) {
			String url = resources.getJSONObject(i).optString("url");
			Matcher m = pattern.matcher(url);
			if (m.find() && (m.group(2) + m.group(1)).compareTo(bestKey) > 0) {
				bestKey = m.group(2) + m.group(1);
				best = url;
			}
		}
		if (best == null) {
			throw new IOException("No resource matching " + pattern + " in " + packageId);
		}
		return best;
	}

	private File historyZip() {
		File best = null;
		for (File f : filesOf(staticDir(), ".zip")) {
			Matcher m = HISTORY_FILE.matcher(f.getName());
			if (m.matches() && (best == null || key(f).compareTo(key(best)) > 0)) {
				best = f;
			}
		}
		return best;
	}

	private static String key(File history) {
		Matcher m = HISTORY_FILE.matcher(history.getName());
		return m.matches() ? m.group(2) + m.group(1) : "";
	}

	@Override
	public Day read(LocalDate day) throws Exception {
		Map<String, String[]> locations = readLocations(new File(staticDir(), "locations.csv"));
		Map<String, TrafficSensor> sensors = new LinkedHashMap<>();
		List<File> snapshots = snapshotFiles(day, ".xml.gz");
		String first = null, last = null;
		int lastHour = 0, used = 0;
		for (File file : snapshots) {
			Document doc = parseXml(file);
			LocalDateTime time = LocalDateTime.parse(text(doc.getDocumentElement(), "fecha_hora"), SNAPSHOT_TIME);
			if (!time.toLocalDate().equals(day)) {
				continue;
			}
			used++;
			String hhmm = time.format(DateTimeFormatter.ofPattern("HH:mm"));
			first = first == null ? hhmm : first;
			last = hhmm;
			lastHour = time.getHour();
			NodeList pms = doc.getElementsByTagName("pm");
			for (int i = 0; i < pms.getLength(); i++) {
				Element pm = (Element) pms.item(i);
				String id = text(pm, "idelem");
				String[] loc = locations.get(id);
				if (!"N".equals(text(pm, "error")) || loc == null) {
					continue;
				}
				TrafficSensor s = sensors.computeIfAbsent(id, k -> newSensor(k, loc));
				if (s.name.isEmpty()) {
					setName(s, text(pm, "descripcion"));
				}
				double level = parse(text(pm, "nivelServicio"));
				s.addSample(time.getHour(), parse(text(pm, "intensidad")), parse(text(pm, "ocupacion")), parse(text(pm, "velocidad")),
						level >= 0 ? (int) Math.min(TrafficSensor.STATE_BLOCKED, level + 1) : 0);
			}
		}
		Day result = new Day();
		for (TrafficSensor s : sensors.values()) {
			s.finishSamples(1);
			result.sensors.add(s);
		}
		result.stateSource = "city";
		result.defaultHour = lastHour;
		result.source = String.format(Locale.US, "Ayuntamiento de Madrid, real-time traffic feed (informo.madrid.es): %d snapshots, %s–%s local time",
				used, first, last);
		return result;
	}

	/**
	 * Live data has few light-traffic hours: reuse each detector's effective length from a weekday of the latest
	 * monthly history (a property of the loop and the road), and report the error where speed is also measured.
	 */
	@Override
	public String estimate(Day day, LoopSpeedEstimator estimator, ObfRoadMatcher matcher) throws Exception {
		File zip = historyZip();
		if (zip == null) {
			return super.estimate(day, estimator, matcher);
		}
		Matcher m = HISTORY_FILE.matcher(zip.getName());
		m.matches();
		LocalDate calibrationDay = LocalDate.of(Integer.parseInt(m.group(2)), Integer.parseInt(m.group(1)), 1)
				.with(TemporalAdjusters.lastInMonth(DayOfWeek.WEDNESDAY));
		List<TrafficSensor> history = match(readHistory(zip, new File(staticDir(), "locations.csv"), calibrationDay), matcher, defaultSpeedKmh());
		calibrateOnOwnHours(history, estimator);
		Map<String, Double> lengths = new HashMap<>();
		for (TrafficSensor s : history) {
			if (!s.effectiveLengthFallback) {
				lengths.put(s.id, s.effectiveLength);
			}
		}
		List<Double> errors = new ArrayList<>();
		int calibrated = 0, measured = 0;
		for (TrafficSensor s : day.sensors) {
			Double length = lengths.get(s.id);
			s.effectiveLength = length == null ? Double.NaN : length;
			s.effectiveLengthFallback = length == null;
			estimator.estimate(s);
			calibrated += length == null ? 0 : 1;
			measured += s.hasMeasuredSpeed() ? 1 : 0;
			for (int h = 0; h < TrafficSensor.HOURS; h++) {
				if (s.measuredSpeed[h] > 0 && !Double.isNaN(s.estimatedSpeed[h])) {
					errors.add(Math.abs(s.estimatedSpeed[h] - s.measuredSpeed[h]));
				}
			}
		}
		return String.format(Locale.US, "Speed measured on %d detectors; elsewhere estimated with each detector's calibration from %s "
						+ "(%d detectors). Where both exist the estimate is off by a median %.1f km/h (%d detector-hours).",
				measured, calibrationDay, calibrated, LoopSpeedEstimator.median(errors), errors.size());
	}

	// 15-minute rows: id;fecha;tipo_elem;intensidad;ocupacion;carga;vmed;error;periodo_integracion
	private static List<TrafficSensor> readHistory(File zip, File locationsCsv, LocalDate day) throws IOException {
		Map<String, String[]> locations = readLocations(locationsCsv);
		Map<String, TrafficSensor> sensors = new HashMap<>();
		String prefix = day.toString();
		try (ZipFile zf = new ZipFile(zip)) {
			ZipEntry entry = zf.entries().nextElement();
			try (BufferedReader reader = new BufferedReader(new InputStreamReader(zf.getInputStream(entry), StandardCharsets.ISO_8859_1), 1 << 16)) {
				reader.readLine();
				String line;
				while ((line = reader.readLine()) != null) {
					int q = line.indexOf('"');
					if (q < 0 || !line.startsWith(prefix, q + 1)) {
						continue;
					}
					List<String> f = splitCsv(line, ';');
					String[] loc = locations.get(f.get(0));
					if (!"N".equals(f.get(7)) || loc == null) {
						continue;
					}
					sensors.computeIfAbsent(f.get(0), k -> newSensor(k, loc))
							.addSample(Integer.parseInt(f.get(1).substring(11, 13)), parse(f.get(3)), parse(f.get(4)), parse(f.get(6)), 0);
				}
			}
		}
		List<TrafficSensor> list = new ArrayList<>();
		for (TrafficSensor s : sensors.values()) {
			s.finishSamples(2);
			list.add(s);
		}
		return list;
	}

	private static Map<String, String[]> readLocations(File csv) throws IOException {
		Map<String, String[]> locations = new HashMap<>(); // id -> type, name, lon, lat
		try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(csv), StandardCharsets.ISO_8859_1))) {
			List<String> cols = splitCsv(reader.readLine(), ';');
			int iType = cols.indexOf("tipo_elem"), iId = cols.indexOf("id"), iName = cols.indexOf("nombre");
			int iLon = cols.indexOf("longitud"), iLat = cols.indexOf("latitud");
			String line;
			while ((line = reader.readLine()) != null) {
				List<String> f = splitCsv(line, ';');
				locations.put(f.get(iId), new String[] {f.get(iType), f.get(iName), f.get(iLon), f.get(iLat)});
			}
		}
		return locations;
	}

	private static TrafficSensor newSensor(String id, String[] loc) {
		TrafficSensor s = new TrafficSensor(id);
		s.type = loc[0];
		s.lon = new double[] {parse(loc[2])};
		s.lat = new double[] {parse(loc[3])};
		setName(s, loc[1]);
		return s;
	}

	// names carry the travel direction as compass letters, e.g. "N-S" (north to south), "O-E" (west to east)
	private static void setName(TrafficSensor s, String name) {
		s.name = name;
		Matcher m = DIRECTION.matcher(name);
		if (m.find()) {
			double[] a = compass(m.group(1)), b = compass(m.group(2));
			s.heading = heading(b[0] - a[0], b[1] - a[1]);
		}
	}

	private static double[] compass(String c) {
		double x = c.contains("E") ? 1 : c.contains("O") ? -1 : 0;
		double y = c.contains("N") ? 1 : c.contains("S") ? -1 : 0;
		double l = Math.hypot(x, y);
		return new double[] {x / l, y / l};
	}

	private static LocalDateTime snapshotTime(File xmlGz) throws Exception {
		return LocalDateTime.parse(text(parseXml(xmlGz).getDocumentElement(), "fecha_hora"), SNAPSHOT_TIME);
	}

	private static Document parseXml(File xmlGz) throws Exception {
		byte[] bytes;
		try (InputStream in = new GZIPInputStream(new FileInputStream(xmlGz))) {
			bytes = in.readAllBytes();
		}
		int bom = bytes.length >= 3 && (bytes[0] & 0xff) == 0xEF && (bytes[1] & 0xff) == 0xBB && (bytes[2] & 0xff) == 0xBF ? 3 : 0;
		return DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(new ByteArrayInputStream(bytes, bom, bytes.length - bom));
	}

	private static String text(Element e, String tag) {
		NodeList list = e.getElementsByTagName(tag);
		return list.getLength() == 0 ? "" : list.item(0).getTextContent().trim();
	}
}
