package net.osmand.server.osmgpx;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class ActivityClassifierTest {

	private static final ObjectMapper JSON = new ObjectMapper();

	private final ActivityClassifier classifier = new ActivityClassifier(
			Map.of("skiing", List.of("ski", "skiing", "лыжи", "nordic"), "cross_country_skiing", List.of(),
					"ski_touring", List.of("skitour"), "snowmobiling", List.of("snowmobile"),
					"walking", List.of("walking", "walk"), "road_cycling", List.of("bike")),
			Map.of("skiing", "winter_sport", "cross_country_skiing", "winter_sport", "ski_touring", "winter_sport",
					"snowmobiling", "winter_sport", "walking", "foot", "road_cycling", "cycling"));

	// a timed track: speeds in km/h, start date in UTC (null: no time), elevation maximum (null: no elevation), length
	private static ObjectNode stats(double p50, double p85, double p95, String date, Integer eleMax, double km) {
		ObjectNode s = JSON.createObjectNode();
		s.put("clean_points", 500);
		s.put("clean_distance_m", km * 1000);
		s.put("time_frac", 1.0);
		s.put("moving_s", 3600);
		s.put("speed_cv", 0.5);
		s.put("speed_p50", p50);
		s.put("speed_p85", p85);
		s.put("speed_p95", p95);
		if (date != null) {
			s.put("start_time", LocalDate.parse(date).atTime(10, 0).toEpochSecond(ZoneOffset.UTC));
		}
		if (eleMax != null) {
			s.put("ele_min", 0);
			s.put("ele_max", eleMax);
		}
		return s;
	}

	private String activity(String name, double lat, JsonNode stats) {
		return classifier.classify(new ActivityClassifier.Track(name, null, List.of(), null, lat, stats)).activity();
	}

	@Test
	public void snowSeasonByLatitudeMonthAndAltitude() {
		assertEquals(true, ActivityClassifier.snowPossible(47.3, stats(10, 20, 30, "2020-01-15", 600, 10)));
		assertEquals(false, ActivityClassifier.snowPossible(47.3, stats(10, 20, 30, "2020-07-15", 600, 10)));
		assertEquals(false, ActivityClassifier.snowPossible(47.3, stats(10, 20, 30, "2020-10-15", 600, 10)));
		assertEquals(true, ActivityClassifier.snowPossible(47.3, stats(10, 20, 30, "2020-07-15", 3100, 10))); // glacier
		assertEquals(true, ActivityClassifier.snowPossible(66.5, stats(10, 20, 30, "2020-05-10", 200, 10)));
		assertEquals(true, ActivityClassifier.snowPossible(60.0, stats(10, 20, 30, "2020-10-20", 200, 10)));
		assertEquals(false, ActivityClassifier.snowPossible(60.0, stats(10, 20, 30, "2020-08-20", 200, 10)));
		assertEquals(true, ActivityClassifier.snowPossible(-36.5, stats(10, 20, 30, "2020-07-15", 1500, 10)));
		assertEquals(false, ActivityClassifier.snowPossible(-36.5, stats(10, 20, 30, "2020-01-15", 1500, 10)));
		assertEquals(false, ActivityClassifier.snowPossible(30.0, stats(10, 20, 30, "2020-01-15", 500, 10)));
		assertEquals(true, ActivityClassifier.snowPossible(30.0, stats(10, 20, 30, "2020-01-15", 1500, 10)));
		assertEquals(false, ActivityClassifier.snowPossible(10.0, stats(10, 20, 30, "2020-01-15", 1500, 10)));
		assertNull(ActivityClassifier.snowPossible(47.3, stats(10, 20, 30, null, 600, 10)));
		assertNull(ActivityClassifier.snowPossible(null, stats(10, 20, 30, "2020-01-15", 600, 10)));
	}

	@Test
	public void winterWordCountsOnlyInSnowSeason() {
		// the town of Ski, Norway: a walk in July is not skiing, a January outing is
		assertEquals("foot", activity("Ski", 59.7, stats(4, 5, 7, "2012-07-10", 150, 8)));
		assertEquals("skiing", activity("Ski", 59.7, stats(4, 5, 7, "2012-01-10", 150, 8)));
		// out of season the longer winter word is skipped and a later keyword of the same text still matches
		assertEquals("walking", activity("Nordic walk", 47.5, stats(4, 5, 7, "2012-07-10", 400, 8)));
		assertEquals("skiing", activity("Nordic walk", 47.5, stats(4, 5, 7, "2012-01-10", 400, 8)));
	}

	@Test
	public void winterWordsMissingFromActivitiesJson() {
		assertEquals("skiing", activity("Skidåkning, Maserloppet", 60.9, stats(9, 11.5, 14, "2013-02-10", 300, 12)));
		assertEquals("cycling", activity("Skidåkning, Maserloppet", 60.9, stats(9, 11.5, 14, "2013-07-10", 300, 12)));
		assertEquals("cross_country_skiing", activity("Loipe Benneckenstein", 51.7, stats(8, 10, 12, "2022-02-05", 560, 14)));
		assertEquals("skiing", activity("Лыжная прогулка", 59.9, stats(4, 5.9, 8, "2021-12-26", 160, 16)));
		// no start time: the season is unknown and the word is taken
		JsonNode untimed = stats(8, 10, 12, null, 960, 4);
		assertEquals("cross_country_skiing", activity("loipe", 47.6, untimed));
	}

	@Test
	public void snowmobileRouteWalkedInSummer() {
		ActivityClassifier.Track walked = new ActivityClassifier.Track("trail", null, List.of("snowmobile"), null, 64.0,
				stats(4, 5, 7, "2013-08-15", 200, 6));
		assertEquals("foot", classifier.classify(walked).activity());
		// a route driven at snowmobile speed with a summer date keeps its label: old receivers log wrong dates
		ActivityClassifier.Track driven = new ActivityClassifier.Track("reitti", null, List.of("snowmobile"), null, 66.5,
				stats(40, 55, 70, "2000-07-15", 250, 118));
		assertEquals("snowmobiling", classifier.classify(driven).activity());
	}

	@Test
	public void fastTracksNeedAltitudeToBeFlights() {
		assertEquals("train_riding", activity("", 31.2, stats(280, 300, 330, "2019-08-01", 60, 120)));
		assertEquals("aviation", activity("", 55.0, stats(750, 820, 850, "2019-08-01", 10500, 1500)));
		assertEquals("aviation", activity("Flight from Reykjavík to Akureyri", 64.1, stats(280, 320, 450, "2008-12-21", 1082, 271)));
		assertEquals("aviation", activity("", 40.0, stats(600, 700, 760, "2008-12-21", null, 800)));
		// cycling routes drawn with made-up times
		ActivityClassifier.Result drawn = classifier.classify(new ActivityClassifier.Track("Leemcule", null, List.of(), null, 52.4,
				stats(460, 600, 730, "2007-01-01", 48, 41)));
		assertEquals("nospeed", drawn.activity());
		assertEquals("none", drawn.source());
		assertNull(drawn.speedMatches());
		assertEquals("nospeed", activity("", 52.4, stats(460, 520, 560, "2007-01-01", null, 40)));
		assertEquals("nospeed", activity("", 50.9, stats(260, 290, 300, "2006-01-01", 50, 20)));
	}
}
