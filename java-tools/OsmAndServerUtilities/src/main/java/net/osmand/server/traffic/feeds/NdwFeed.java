package net.osmand.server.traffic.feeds;

import net.osmand.server.traffic.LoopSpeedEstimator;
import net.osmand.server.traffic.ObfRoadMatcher;
import net.osmand.server.traffic.TrafficEvent;
import net.osmand.server.traffic.TrafficFeed;
import net.osmand.server.traffic.TrafficFeeds;
import net.osmand.server.traffic.TrafficSensor;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.zip.GZIPInputStream;

/**
 * Netherlands, NDW open data (opendata.ndw.nu, DATEX II 2.0): a minute snapshot of flow and speed per lane for every
 * loop site (trafficspeed.xml.gz) and the site table (measurement_current.xml.gz). Only point sites in the Randstad
 * are used; the travel-time segments (floating car data) are skipped. Speed is measured, so state is the speed band.
 */
public class NdwFeed extends TrafficFeed {

	private static final ZoneId AMSTERDAM = ZoneId.of("Europe/Amsterdam");
	private static final String SNAPSHOT = "https://opendata.ndw.nu/trafficspeed.xml.gz";
	private static final String SITES = "https://opendata.ndw.nu/measurement_current.xml.gz";
	private static final String EVENTS = "https://opendata.ndw.nu/actueel_beeld.xml.gz";
	private static final double TOP = 52.6, BOTTOM = 51.8, LEFT = 4.0, RIGHT = 5.4; // Randstad
	private static final String XSI = "http://www.w3.org/2001/XMLSchema-instance";

	private static class Site {
		String id;
		String name = "";
		String carriageway = "";
		double lat = Double.NaN;
		double lon = Double.NaN;
		double heading = Double.NaN;
		boolean point;
		final Map<Integer, String[]> values = new HashMap<>(); // index -> value type, lane (anyVehicle only)
	}

	@Override
	public String id() {
		return "ndw";
	}

	@Override
	public String name() {
		return "Netherlands · Randstad";
	}

	@Override
	public ZoneId zone() {
		return AMSTERDAM;
	}

	@Override
	public List<String> obfFiles() {
		return List.of("Netherlands_noord-holland_europe_2.obf", "Netherlands_zuid-holland_europe_2.obf", "Netherlands_utrecht_europe_2.obf",
				"Netherlands_flevoland_europe_2.obf", "Netherlands_gelderland_europe_2.obf", "Netherlands_noord-brabant_europe_2.obf");
	}

	@Override
	public double defaultSpeedKmh() {
		return 80;
	}

	@Override
	public java.time.Duration publishInterval() {
		return java.time.Duration.ofMinutes(1);
	}

	@Override
	public java.time.Duration valueWindow() {
		return java.time.Duration.ofMinutes(1);
	}

	@Override
	public void download(LocalDate today) throws Exception {
		File part = new File(staticDir(), "trafficspeed.xml.gz");
		download(SNAPSHOT, part, false);
		ZonedDateTime time = publicationTime(part).atZone(AMSTERDAM);
		File target = new File(rawDir(time.toLocalDate()), time.format(DateTimeFormatter.ofPattern("HHmm")) + ".xml.gz");
		target.getParentFile().mkdirs();
		Files.move(part.toPath(), target.toPath(), StandardCopyOption.REPLACE_EXISTING);
		TrafficFeeds.log("  snapshot %s", time.toLocalDateTime());
		// current situations (closures, reroutings, obstructions, accidents) next to the measurements, same HHmm
		// the file is gzip already and is not sent with Content-Encoding: store it as is
		download(EVENTS, new File(target.getParentFile(), target.getName().replace(".xml.gz", ".events.xml.gz")), false);
		File sites = new File(staticDir(), "measurement_current.xml.gz");
		if (olderThanDays(sites, 1)) {
			download(SITES, sites, false);
		}
	}

	@Override
	public Day read(LocalDate day) throws Exception {
		Map<String, Site> sites = readSites(new File(staticDir(), "measurement_current.xml.gz"));
		Map<String, TrafficSensor> sensors = new LinkedHashMap<>();
		List<File> snapshots = snapshotFiles(day, ".xml.gz").stream().filter(f -> !f.getName().contains(".events.")).toList();
		Day result = new Day();
		String first = null, last = null;
		int used = 0;
		for (File file : snapshots) {
			int lastHour = readSnapshot(file, day, sites, sensors);
			if (lastHour >= 0) {
				used++;
				String hhmm = file.getName().substring(0, 2) + ":" + file.getName().substring(2, 4);
				first = first == null ? hhmm : first;
				last = hhmm;
				result.defaultHour = lastHour;
			}
		}
		for (TrafficSensor s : sensors.values()) {
			s.finishSamples(1);
			result.sensors.add(s);
		}
		Map<String, TrafficEvent> events = new LinkedHashMap<>();
		for (File file : filesOf(rawDir(day), ".events.xml.gz")) {
			readEvents(file, Integer.parseInt(file.getName().substring(0, 2)), events);
		}
		result.events.addAll(events.values());
		result.stateSource = "speed";
		result.source = String.format(Locale.US, "NDW Nationaal Dataportaal Wegverkeer (opendata.ndw.nu), loop sites in the Randstad: %d snapshots, %s–%s local time",
				used, first, last);
		return result;
	}

	/** Speed is measured; the state (for the congestion colours) is the speed as a share of the OBF max speed. */
	@Override
	public String estimate(Day day, LoopSpeedEstimator estimator, ObfRoadMatcher matcher) {
		int withSpeed = statesFromMeasuredSpeed(day);
		return String.format(Locale.US, "Speed measured by loops on %d of %d sites (flow-weighted over lanes); state is the speed as a share of the OBF max speed.",
				withSpeed, day.sensors.size());
	}

	// point sites in the bounding box, with the value indexes that count all vehicles
	private static Map<String, Site> readSites(File file) throws IOException, XMLStreamException {
		Map<String, Site> sites = new HashMap<>();
		try (InputStream in = new GZIPInputStream(new FileInputStream(file), 1 << 16)) {
			XMLStreamReader r = XMLInputFactory.newInstance().createXMLStreamReader(in);
			Site site = null;
			int index = -1;
			String type = null, lane = null;
			boolean anyVehicle = false, inName = false, inDisplay = false;
			while (r.hasNext()) {
				int event = r.next();
				if (event == XMLStreamConstants.START_ELEMENT) {
					String tag = r.getLocalName();
					switch (tag) {
						case "measurementSiteRecord" -> {
							site = new Site();
							site.id = r.getAttributeValue(null, "id");
						}
						case "measurementSiteName" -> inName = true;
						case "value" -> {
							if (inName && site != null && site.name.isEmpty()) {
								site.name = r.getElementText();
							}
						}
						case "measurementSide" -> site.heading = heading(r.getElementText());
						case "measurementSpecificCharacteristics" -> {
							String idx = r.getAttributeValue(null, "index");
							if (idx != null) {
								index = Integer.parseInt(idx);
								type = lane = null;
								anyVehicle = false;
							}
						}
						case "specificMeasurementValueType" -> type = r.getElementText();
						case "specificLane" -> lane = r.getElementText();
						case "vehicleType" -> anyVehicle |= "anyVehicle".equals(r.getElementText());
						case "measurementSiteLocation" -> site.point = "Point".equals(r.getAttributeValue(XSI, "type"));
						case "locationForDisplay" -> inDisplay = true;
						case "latitude" -> {
							if (inDisplay) {
								site.lat = Double.parseDouble(r.getElementText());
							}
						}
						case "longitude" -> {
							if (inDisplay) {
								site.lon = Double.parseDouble(r.getElementText());
							}
						}
						case "carriageway" -> {
							if (site.carriageway.isEmpty()) {
								site.carriageway = r.getElementText();
							}
						}
						default -> {
						}
					}
				} else if (event == XMLStreamConstants.END_ELEMENT) {
					switch (r.getLocalName()) {
						case "measurementSiteName" -> inName = false;
						case "locationForDisplay" -> inDisplay = false;
						case "measurementSpecificCharacteristics" -> {
							if (index >= 0 && type != null) {
								if (anyVehicle) {
									site.values.put(index, new String[] {type, lane == null ? "" : lane});
								}
								index = -1;
								type = null;
							}
						}
						case "measurementSiteRecord" -> {
							if (site.point && site.lat >= BOTTOM && site.lat <= TOP && site.lon >= LEFT && site.lon <= RIGHT && !site.values.isEmpty()) {
								sites.put(site.id, site);
							}
							site = null;
						}
						default -> {
						}
					}
				}
			}
		}
		return sites;
	}

	// adds one sample per site to its hour; returns the hour of the snapshot or -1 when it is not on this day
	private static int readSnapshot(File file, LocalDate day, Map<String, Site> sites, Map<String, TrafficSensor> sensors) throws IOException, XMLStreamException {
		int snapshotHour = -1;
		try (InputStream in = new GZIPInputStream(new FileInputStream(file), 1 << 16)) {
			XMLStreamReader r = XMLInputFactory.newInstance().createXMLStreamReader(in);
			Site site = null;
			ZonedDateTime time = null;
			int index = -1;
			Map<String, double[]> lanes = new HashMap<>(); // lane -> flow, speed
			while (r.hasNext()) {
				int event = r.next();
				if (event == XMLStreamConstants.START_ELEMENT) {
					switch (r.getLocalName()) {
						case "siteMeasurements" -> {
							site = null;
							time = null;
							lanes.clear();
						}
						case "measurementSiteReference" -> site = sites.get(r.getAttributeValue(null, "id"));
						case "measurementTimeDefault" -> time = Instant.parse(r.getElementText()).atZone(AMSTERDAM);
						case "measuredValue" -> {
							String idx = r.getAttributeValue(null, "index");
							if (idx != null) {
								index = Integer.parseInt(idx);
							}
						}
						case "vehicleFlowRate", "speed" -> {
							String[] value = site == null ? null : site.values.get(index);
							double v = parse(r.getElementText());
							if (value != null) {
								double[] lane = lanes.computeIfAbsent(value[1], k -> new double[] {Double.NaN, Double.NaN});
								if (value[0].equals("trafficFlow")) {
									lane[0] = v;
								} else if (value[0].equals("trafficSpeed")) {
									lane[1] = v;
								}
							}
						}
						default -> {
						}
					}
				} else if (event == XMLStreamConstants.END_ELEMENT && r.getLocalName().equals("siteMeasurements")) {
					if (site == null || time == null || !time.toLocalDate().equals(day)) {
						continue;
					}
					double flow = 0, speedFlow = 0, flowWithSpeed = 0, speedSum = 0;
					int flows = 0, speeds = 0;
					for (double[] lane : lanes.values()) {
						if (lane[0] >= 0) {
							flow += lane[0];
							flows++;
						}
						if (lane[1] > 0) {
							speedSum += lane[1];
							speeds++;
							if (lane[0] > 0) {
								speedFlow += lane[1] * lane[0];
								flowWithSpeed += lane[0];
							}
						}
					}
					double speed = flowWithSpeed > 0 ? speedFlow / flowWithSpeed : speeds > 0 ? speedSum / speeds : -1;
					Site s = site;
					TrafficSensor sensor = sensors.computeIfAbsent(s.id, k -> {
						TrafficSensor t = new TrafficSensor(k);
						t.name = s.name;
						t.type = s.carriageway;
						t.lat = new double[] {s.lat};
						t.lon = new double[] {s.lon};
						t.heading = s.heading;
						// measurementSide is an 8-point compass label for the road, not the local direction: 1,030 of
						// 10,041 Randstad sites had no road within 60 degrees; below 90 still rules out the other carriageway
						t.headingTolerance = 89;
						return t;
					});
					sensor.addSample(time.getHour(), flows > 0 ? flow : Double.NaN, Double.NaN, speed, 0);
					snapshotHour = time.getHour();
				}
			}
		}
		return snapshotHour;
	}

	private static final Map<String, String> EVENT_KINDS = Map.of("RoadOrCarriagewayOrLaneManagement", TrafficEvent.CLOSURE,
			"ReroutingManagement", TrafficEvent.REROUTING, "VehicleObstruction", TrafficEvent.OBSTRUCTION, "GeneralObstruction", TrafficEvent.OBSTRUCTION,
			"Accident", TrafficEvent.ACCIDENT, "SpeedManagement", TrafficEvent.SPEED, "MaintenanceWorks", TrafficEvent.ROADWORKS,
			"ConstructionWorks", TrafficEvent.ROADWORKS, "PublicEvent", TrafficEvent.EVENT);
	private static final java.util.Set<String> EVENT_DETAILS = java.util.Set.of("roadOrCarriagewayOrLaneManagementType", "vehicleObstructionType",
			"obstructionType", "accidentType", "reroutingManagementType", "speedManagementType", "generalNetworkManagementType", "roadMaintenanceType",
			"publicEventType", "roadName", "roadNumber");

	// DATEX II 3 situation records with a point (latitude/longitude) or a line (posList), in the bounding box
	private static void readEvents(File file, int hour, Map<String, TrafficEvent> events) throws IOException, XMLStreamException {
		try (InputStream in = new GZIPInputStream(new FileInputStream(file), 1 << 16)) {
			XMLStreamReader r = XMLInputFactory.newInstance().createXMLStreamReader(in);
			TrafficEvent e = null;
			String type = null, posList = null, source = null;
			double lat = Double.NaN, lon = Double.NaN;
			boolean inComment = false, inSource = false;
			java.util.List<String> comments = new java.util.ArrayList<>(), details = new java.util.ArrayList<>();
			while (r.hasNext()) {
				int event = r.next();
				if (event == XMLStreamConstants.START_ELEMENT) {
					String tag = r.getLocalName();
					if (tag.equals("situationRecord")) {
						e = new TrafficEvent(r.getAttributeValue(null, "id"));
						String t = r.getAttributeValue(XSI, "type");
						type = t == null ? "" : t.substring(t.indexOf(':') + 1);
						posList = source = null;
						lat = lon = Double.NaN;
						inComment = inSource = false;
						comments.clear();
						details.clear();
					} else if (e != null) {
						switch (tag) {
							case "generalPublicComment" -> inComment = true;
							case "sourceName" -> inSource = true;
							case "value" -> {
								String v = r.getElementText();
								if (inComment) {
									comments.add(v);
								} else if (inSource && source == null) {
									source = v;
								}
							}
							case "latitude" -> lat = Double.isNaN(lat) ? parse(r.getElementText()) : lat;
							case "longitude" -> lon = Double.isNaN(lon) ? parse(r.getElementText()) : lon;
							case "posList" -> posList = posList == null ? r.getElementText() : posList;
							case "overallStartTime" -> e.start = r.getElementText();
							case "overallEndTime" -> e.end = r.getElementText();
							default -> {
								if (EVENT_DETAILS.contains(tag) && details.size() < 6) {
									details.add(tag.replaceAll("([A-Z])", " $1").toLowerCase(Locale.ROOT) + ": " + r.getElementText());
								}
							}
						}
					}
				} else if (event == XMLStreamConstants.END_ELEMENT) {
					String tag = r.getLocalName();
					if (tag.equals("generalPublicComment")) {
						inComment = false;
					} else if (tag.equals("sourceName")) {
						inSource = false;
					} else if (tag.equals("situationRecord") && e != null) {
						if (posList != null) {
							String[] v = posList.trim().split("\\s+");
							// GML posList in EPSG:4326 order: latitude first
							e.lineLat = new double[v.length / 2];
							e.lineLon = new double[v.length / 2];
							for (int i = 0; i + 1 < v.length; i += 2) {
								e.lineLat[i / 2] = parse(v[i]);
								e.lineLon[i / 2] = parse(v[i + 1]);
							}
							if (Double.isNaN(lat) && e.lineLat.length > 0) {
								lat = e.lineLat[0];
								lon = e.lineLon[0];
							}
						}
						if (lat >= BOTTOM && lat <= TOP && lon >= LEFT && lon <= RIGHT) {
							e.lat = lat;
							e.lon = lon;
							e.kind = EVENT_KINDS.getOrDefault(type, TrafficEvent.OTHER);
							e.title = type.replaceAll("([a-z])([A-Z])", "$1 $2");
							e.subtitle = source == null ? "" : source;
							java.util.List<String> text = new java.util.ArrayList<>(comments);
							text.addAll(details);
							e.description = String.join("\n", text);
							e.blocked = details.stream().anyMatch(d -> d.contains("closed"));
							e.source = "NDW · " + type;
							TrafficEvent.add(events, e, hour);
						}
						e = null;
					}
				}
			}
		}
	}

	private static Instant publicationTime(File file) throws IOException, XMLStreamException {
		try (InputStream in = new GZIPInputStream(new FileInputStream(file))) {
			XMLStreamReader r = XMLInputFactory.newInstance().createXMLStreamReader(in);
			while (r.hasNext()) {
				if (r.next() == XMLStreamConstants.START_ELEMENT && r.getLocalName().equals("publicationTime")) {
					return Instant.parse(r.getElementText());
				}
			}
		}
		throw new IOException("No publicationTime in " + file);
	}

	private static double heading(String side) {
		return switch (side) {
			case "northBound" -> 0;
			case "northEastBound" -> 45;
			case "eastBound" -> 90;
			case "southEastBound" -> 135;
			case "southBound" -> 180;
			case "southWestBound" -> 225;
			case "westBound" -> 270;
			case "northWestBound" -> 315;
			default -> Double.NaN;
		};
	}
}
