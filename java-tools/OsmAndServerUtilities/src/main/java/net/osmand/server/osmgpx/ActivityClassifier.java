package net.osmand.server.osmgpx;

import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Activity of an OSM GPX trace from stored columns and track_stats only, so the same rules run in parse_tracks and
 * classify_tracks. A label from the file or its text is accepted only when the track's own speed fits that activity;
 * otherwise the next label is tried and, when none fits, the activity comes from speed alone. A winter-sport word counts
 * only where and when snow is possible, and a fast track is a flight only when it climbs.
 */
public class ActivityClassifier {

	public static final String NOSPEED = "nospeed";
	public static final String AVIATION = "aviation";
	public static final String TRAIN = "train_riding";
	public static final String FOOT = "foot";
	public static final String CYCLING = "cycling";
	public static final String DRIVING = "driving";
	private static final String WINTER_SPORT = "winter_sport";
	private static final String SNOWMOBILING = "snowmobiling";

	// what a track has: text and file labels and the start latitude from osm_gpx_data, everything measured from track_stats
	public record Track(String name, String description, List<String> tags, String fileActivity, Double lat, JsonNode stats) {
	}

	// speedMatches: null without usable speed, otherwise whether the activity fits the track's speed
	public record Result(String activity, String source, Boolean speedMatches) {
	}

	// p85 speed range and p95 ceiling (km/h) a label must fit
	private record Envelope(double minP85, double maxP85, double maxP95) {
		boolean fits(double p85, double p95) {
			return p85 >= minP85 && p85 <= maxP85 && p95 <= maxP95;
		}
	}

	private static final Map<String, Envelope> GROUP_ENVELOPES = Map.of(
			"foot", new Envelope(0, 12, 20),
			"cycling", new Envelope(5, 45, 70),
			"winter_sport", new Envelope(3, 120, 160),
			"driving", new Envelope(10, 250, 300),
			"motorcycling", new Envelope(10, 250, 300),
			"air_sports", new Envelope(15, 1200, 1300),
			"water_sport", new Envelope(0, 90, 120));
	private static final Map<String, Envelope> ACTIVITY_ENVELOPES = Map.ofEntries(
			Map.entry("road_running", new Envelope(5, 20, 26)),
			Map.entry("trail_running", new Envelope(4, 20, 26)),
			Map.entry("e_biking", new Envelope(8, 50, 60)),
			Map.entry("cross_country_skiing", new Envelope(3, 35, 60)),
			Map.entry("ski_touring", new Envelope(0, 60, 100)),
			Map.entry("snowshoeing", new Envelope(0, 10, 16)),
			Map.entry("aviation", new Envelope(100, 1200, 1300)),
			Map.entry("paragliding", new Envelope(5, 100, 150)),
			Map.entry("hang_gliding", new Envelope(5, 100, 150)),
			Map.entry("kayak", new Envelope(0, 15, 25)),
			Map.entry("canoe", new Envelope(0, 15, 25)),
			Map.entry("sup", new Envelope(0, 15, 25)),
			Map.entry("rafting", new Envelope(0, 20, 30)),
			Map.entry("swimming_outdoor", new Envelope(0, 6, 10)),
			Map.entry("train_riding", new Envelope(20, 350, 400)),
			Map.entry("horse_riding", new Envelope(0, 25, 40)));

	// activities.json keywords that name a place, a road or something generic rather than how the track was made
	private static final Set<String> STOP_KEYWORDS = Set.of("van", "vehicle", "motorway", "terrain",
			"feldweg", "feldwege", "racing", "course", "langlauf", "winter", "skating", "designated", "water", "river",
			"lake", "canal", "waterway", "boat", "boating", "riding", "multi", "climbing", "walkway", "cycleway", "etna",
			"etnanatura", "rungis", "fitotrack",
			"piste", "pistes"); // French "piste cyclable", "piste agricole": a path, not a ski run

	// winter-sport words activities.json does not list, in the languages of the stored tracks; without them a ski day is
	// labelled by speed as cycling or foot. Like every winter-sport keyword they count only when snow is possible.
	private static final Map<String, String> WINTER_KEYWORDS = Map.ofEntries(
			Map.entry("loipe", "cross_country_skiing"), Map.entry("loipen", "cross_country_skiing"),
			Map.entry("skiloipe", "cross_country_skiing"), Map.entry("skiløype", "cross_country_skiing"),
			Map.entry("skiløyper", "cross_country_skiing"), Map.entry("skidspår", "cross_country_skiing"),
			Map.entry("ski de fond", "cross_country_skiing"),
			Map.entry("skitag", "skiing"), Map.entry("skifahren", "skiing"), Map.entry("skigebiet", "skiing"),
			Map.entry("skidor", "skiing"), Map.entry("skidåkning", "skiing"), Map.entry("skijanje", "skiing"),
			Map.entry("lyže", "skiing"), Map.entry("lyžování", "skiing"), Map.entry("lyžovanie", "skiing"),
			Map.entry("sci", "skiing"), Map.entry("sciare", "skiing"), Map.entry("hiihto", "skiing"),
			Map.entry("лыжная", "skiing"), Map.entry("лыжный", "skiing"), Map.entry("лыжные", "skiing"),
			Map.entry("лыжах", "skiing"), Map.entry("горнолыжная", "skiing"), Map.entry("горнолыжный", "skiing"),
			Map.entry("skitouren", "ski_touring"), Map.entry("skialp", "ski_touring"), Map.entry("scialpinismo", "ski_touring"));

	private static final String[] CAR_CREATORS = {"sunnypilot", "dragonpilot", "openpilot"};

	private static final int MIN_POINTS = 10;
	private static final double MIN_DISTANCE_M = 200;
	private static final double MIN_MOVING_S = 120;
	private static final double MIN_TIME_FRAC = 0.9;
	private static final double SYNTHETIC_CV = 0.03; // generated timestamps: almost constant speed
	private static final double SYNTHETIC_P95_TO_P50 = 1.08;
	// planners set walking or cycling speeds; motorway, rail and flights keep a steady speed above this
	private static final double PLANNED_MAX_P50_KMH = 50;
	// GPSies times its routes at 10 km/h whatever the activity, so that speed says nothing
	private static final double PLANNER_DEFAULT_MIN_KMH = 9.5;
	private static final double PLANNER_DEFAULT_MAX_KMH = 10.5;
	private static final double FOOT_MAX_P85_KMH = 8.5;
	private static final double FOOT_MAX_P95_KMH = 14;
	private static final double CYCLING_MAX_P85_KMH = 32;
	private static final double CYCLING_MAX_P95_KMH = 50;
	private static final double FLIGHT_MEDIAN_KMH = 350; // high-speed trains keep a median of 250-320 km/h
	private static final double TRAIN_MEDIAN_KMH = 200;
	// GPS in a cabin passes 1000 m even on short turboprop hops; rail, roads and routes drawn with made-up times stay lower
	private static final double FLIGHT_MIN_ELE_M = 1000;
	private static final double FLIGHT_MIN_KM_WITHOUT_ELE = 150; // a file without elevation needs a long line instead
	private static final double TRAIN_MIN_KM = 50; // a shorter fast low track is a drawn route with made-up times
	// snow by latitude of the start (north of the equator, south shifted by six months): October-May from 55 degrees,
	// November-April from 35, November-April above 1000 m from the tropics' edge; any month on glaciers
	private static final double SNOW_LONG_WINTER_LAT = 55;
	private static final double SNOW_WINTER_LAT = 35;
	private static final double SNOW_SUBTROPICS_LAT = 23.5;
	private static final double SNOW_SUBTROPICS_MIN_ELE_M = 1000;
	private static final double GLACIER_ELE_M = 2500;

	private final Map<String, String> groups; // activity or group id -> group id
	private final List<Map.Entry<String, String>> keywords; // normalized keyword -> activity id, longest first

	public ActivityClassifier(Map<String, List<String>> activityKeywords, Map<String, String> activityGroups) {
		groups = new HashMap<>(activityGroups);
		Map<String, String> byKeyword = new HashMap<>();
		activityKeywords.forEach((activity, tags) -> {
			groups.putIfAbsent(activity, activity); // group ids are activities of their own
			for (String tag : tags) {
				String keyword = normalize(tag);
				if (!keyword.isEmpty() && !STOP_KEYWORDS.contains(keyword)) {
					byKeyword.putIfAbsent(keyword, activity);
				}
			}
		});
		WINTER_KEYWORDS.forEach((keyword, activity) -> {
			if (groups.containsKey(activity)) {
				byKeyword.putIfAbsent(normalize(keyword), activity);
			}
		});
		keywords = new ArrayList<>(byKeyword.entrySet());
		keywords.sort(Comparator.comparingInt((Map.Entry<String, String> e) -> -e.getKey().length())
				.thenComparing(Map.Entry::getKey));
	}

	public Result classify(Track track) {
		JsonNode stats = track.stats();
		if (stats.path("clean_points").asInt() < MIN_POINTS) {
			return new Result(GarbageClassifier.SPARSE, "garbage", null);
		}
		if (stats.path("clean_distance_m").asDouble() < MIN_DISTANCE_M) {
			return new Result(GarbageClassifier.SHORT, "garbage", null);
		}
		boolean timed = stats.has("speed_p85") && stats.path("moving_s").asDouble() >= MIN_MOVING_S
				&& stats.path("time_frac").asDouble() >= MIN_TIME_FRAC;
		double p50 = stats.path("speed_p50").asDouble();
		double p85 = stats.path("speed_p85").asDouble();
		double p95 = stats.path("speed_p95").asDouble();
		boolean steady = timed && (stats.path("speed_cv").asDouble() < SYNTHETIC_CV
				|| (p50 > 0 && p95 / p50 < SYNTHETIC_P95_TO_P50));
		boolean synthetic = steady && p50 < PLANNED_MAX_P50_KMH;
		boolean checkSpeed = timed && !synthetic;

		// out of the snow season a winter-sport word names a place or a trail walked in summer ("Ski, Akershus", "Official
		// Winter Trail"); a snowmobile route keeps its label unless it was walked, since old receivers log wrong dates
		boolean snowless = Boolean.FALSE.equals(snowPossible(track.lat(), stats));
		boolean walked = checkSpeed && p85 <= FOOT_MAX_P85_KMH && p95 <= FOOT_MAX_P95_KMH;
		Predicate<String> inSeason = activity -> !snowless || !WINTER_SPORT.equals(groups.get(activity))
				|| (SNOWMOBILING.equals(activity) && !walked);

		// candidates in the order they are trusted; the first one the speed allows wins
		String[][] candidates = {
				{track.fileActivity(), "file"},
				{creatorActivity(stats.path("creator").asText("")), "creator"},
				{keyword(String.join(" | ", track.tags()), inSeason), "tag"},
				{keyword(track.name(), inSeason), "name"},
				{keyword(track.description(), inSeason), "description"}};
		for (String[] candidate : candidates) {
			String activity = candidate[0];
			if (activity == null || !groups.containsKey(activity)) {
				continue;
			}
			if (!checkSpeed) {
				return new Result(activity, candidate[1], null);
			}
			if (fits(activity, p85, p95)) {
				return new Result(activity, candidate[1], true);
			}
		}
		if (!checkSpeed) {
			if (synthetic && (p50 < PLANNER_DEFAULT_MIN_KMH || p50 > PLANNER_DEFAULT_MAX_KMH)) {
				return new Result(bySpeed(p50, p85, p95, stats), "speed", null); // the speed picked in the planner
			}
			return new Result(NOSPEED, "none", null);
		}
		String activity = bySpeed(p50, p85, p95, stats);
		return NOSPEED.equals(activity) ? new Result(NOSPEED, "none", null) : new Result(activity, "speed", true);
	}

	/**
	 * Whether snow can lie where and when the track starts: any month on glaciers (ele_max from {@value #GLACIER_ELE_M} m),
	 * otherwise by the latitude band and the month in UTC, south of the equator shifted by six months. Null without a
	 * start time or position, so nothing is decided on it.
	 */
	static Boolean snowPossible(Double lat, JsonNode stats) {
		if (lat == null || !stats.has("start_time")) {
			return null;
		}
		double eleMax = stats.path("ele_max").asDouble(0);
		if (eleMax >= GLACIER_ELE_M) {
			return true;
		}
		int month = Instant.ofEpochSecond(stats.path("start_time").asLong()).atZone(ZoneOffset.UTC).getMonthValue();
		int m = lat >= 0 ? month : (month + 5) % 12 + 1; // July in the south is January in the north
		double latitude = Math.abs(lat);
		boolean novemberToApril = m >= 11 || m <= 4;
		if (latitude >= SNOW_LONG_WINTER_LAT) {
			return m >= 10 || m <= 5;
		}
		if (latitude >= SNOW_WINTER_LAT) {
			return novemberToApril;
		}
		if (latitude >= SNOW_SUBTROPICS_LAT) {
			return novemberToApril && eleMax >= SNOW_SUBTROPICS_MIN_ELE_M;
		}
		return false;
	}

	private boolean fits(String activity, double p85, double p95) {
		Envelope envelope = ACTIVITY_ENVELOPES.get(activity);
		if (envelope == null) {
			envelope = GROUP_ENVELOPES.get(groups.get(activity));
		}
		return envelope == null || envelope.fits(p85, p95); // no envelope: speed says nothing against it
	}

	// NOSPEED for a fast track that is neither a flight nor a train: a route drawn with made-up times
	private static String bySpeed(double p50, double p85, double p95, JsonNode stats) {
		if (p50 > TRAIN_MEDIAN_KMH) {
			double km = stats.path("clean_distance_m").asDouble() / 1000;
			double eleMax = stats.path("ele_max").asDouble(0);
			boolean noElevation = eleMax == 0 && stats.path("ele_min").asDouble(0) == 0;
			if (eleMax >= FLIGHT_MIN_ELE_M || (noElevation && km >= FLIGHT_MIN_KM_WITHOUT_ELE)) {
				return AVIATION;
			}
			return p50 <= FLIGHT_MEDIAN_KMH && km >= TRAIN_MIN_KM ? TRAIN : NOSPEED;
		}
		if (p85 <= FOOT_MAX_P85_KMH && p95 <= FOOT_MAX_P95_KMH) {
			return FOOT;
		}
		if (p85 <= CYCLING_MAX_P85_KMH && p95 <= CYCLING_MAX_P95_KMH) {
			return CYCLING;
		}
		return DRIVING;
	}

	private static String creatorActivity(String creator) {
		String lower = creator.toLowerCase(Locale.ROOT);
		for (String car : CAR_CREATORS) {
			if (lower.contains(car)) {
				return "car";
			}
		}
		return null;
	}

	// first keyword, longest first, found as whole words in the text: with hyphens kept and with hyphens as spaces;
	// keywords of activities the track rules out are skipped, so a later keyword of the same text can still match
	String keyword(String text, Predicate<String> allowed) {
		if (text == null || text.isEmpty()) {
			return null;
		}
		String words = " " + normalize(text) + " ";
		String split = " " + words.replace('-', ' ').trim() + " ";
		for (Map.Entry<String, String> e : keywords) {
			String k = " " + e.getKey() + " ";
			if ((words.contains(k) || split.contains(k)) && allowed.test(e.getValue())) {
				return e.getValue();
			}
		}
		return null;
	}

	// lower case, letters, digits and hyphens only, single spaces: "MTB-Tour_2019 (Alps)" -> "mtb-tour 2019 alps"
	static String normalize(String text) {
		return text.toLowerCase(Locale.ROOT).replaceAll("[^\\p{L}\\p{N}-]+", " ").trim();
	}
}
