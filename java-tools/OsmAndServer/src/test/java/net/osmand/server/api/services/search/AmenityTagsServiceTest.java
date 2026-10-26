package net.osmand.server.api.services.search;

import static net.osmand.shared.data.AmenityTagEntriesBuilder.ValueLabel.NAME;
import static net.osmand.shared.data.AmenityTagEntriesBuilder.ValueLabel.NAME_OVER_VALUE;
import static net.osmand.shared.data.AmenityTagEntriesBuilder.ValueLabel.WHOLE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Test;

import net.osmand.data.Amenity;
import net.osmand.server.utils.WebGpxParser;
import net.osmand.shared.data.AmenityTagEntriesBuilder.ValueLabel;
import net.osmand.shared.gpx.GpxUtilities;
import okio.Buffer;

// /search/visible-tags for the extensions of a GPX point: every key is shown but OsmAnd's own fields
public class AmenityTagsServiceTest {

	private final AmenityTagsService service = new AmenityTagsService(loadedPoiTypesService());

	private static PoiTypesService loadedPoiTypesService() {
		PoiTypesService poiTypesService = new PoiTypesService();
		poiTypesService.loadSharedPoiTypes();
		return poiTypesService;
	}

	// real files of src/test/resources/gpx/visible-tags-points.gpx, sent as the web sends them: wpt.ext.extensions
	@Test
	public void realPoints() throws IOException {
		Map<String, Set<String>> shown = new LinkedHashMap<>();
		for (WebGpxParser.Wpt wpt : loadWpts("visible-tags-points.gpx")) {
			shown.put(wpt.name, visibleKeys(wpt.ext.getExtensionsToRead()));
		}
		assertEquals(Set.of("test:country", "test:state", "test:telephone", "test:postcode", "test:start_date"),
				shown.get("Test"));

		assertShown(shown.get("Starbucks"), "opening_hours", "official_name", "outdoor_seating_yes",
				"collapsable_cuisine", "brand", "takeaway_yes", "collapsable_wheelchair_accessibility",
				"toilets_wheelchair_yes");
		assertHidden(shown.get("Starbucks"), "amenity_origin", "amenity_type", "amenity_subtype", "amenity_name");

		assertHidden(shown.get("Cara Lodge"), "visited_date", "amenity_origin", "amenity_type", "amenity_subtype",
				"amenity_name");

		assertShown(shown.get("Schoodic Woods Campground"), "gpxx:street_address", "gpxx:city", "gpxx:state",
				"gpxx:country", "gpxx:postal_code", "phone", "displaymode");
	}

	// date fields Android writes: FavouritePoint (pickup_date, calendar_event), ItineraryDataHelper (creation_date)
	@Test
	public void osmAndDatesHidden() {
		Map<String, String> tags = Map.of("creation_date", "2025-04-27T23:57:35Z", "pickup_date",
				"2025-05-02T19:21:32Z", "calendar_event", "true", "visited_date", "2025-05-20T16:14:43Z");
		assertEquals(Collections.emptySet(), visibleKeys(tags));
	}

	@Test
	public void onlyUnknownKeysOfThePointAreGeneric() {
		Map<String, String> tags = Map.of("amenity_type", "sustenance", "amenity_subtype", "cafe",
				"internet_access:ssid", "CafeWiFi", "authentication:phone_call:number", "+380441234567",
				"test:country", "US", "my_note", "hello");
		Set<String> generic = service.convertToVisibleTags(tags, "en").stream()
				.filter(tag -> Boolean.TRUE.equals(tag.generic())).map(AmenityTagsService.VisibleTag::key)
				.collect(Collectors.toSet());
		assertEquals(Set.of("test:country", "my_note"), generic);
		assertShown(visibleKeys(tags), "internet_access:ssid", "authentication:phone_call:number");
	}

	@Test
	public void valueLabels() {
		assertEquals(List.of(NAME_OVER_VALUE), labels(Map.of("amenity_type", "sustenance", "amenity_subtype", "cafe",
				"takeaway_yes", "yes"), "takeaway_yes"));
		assertEquals(List.of(NAME_OVER_VALUE), labels(Map.of("amenity_type", "sustenance", "amenity_subtype", "cafe",
				"collapsable_wheelchair_accessibility", "wheelchair_limited"), "collapsable_wheelchair_accessibility"));
		assertEquals(List.of(NAME, NAME), labels(Map.of("amenity_type", "service", "amenity_subtype", "boat_rental",
				"collapsable_boat_rental_type", "boat_kayak_rental_yes;boat_canoe_rental_yes"),
				"collapsable_boat_rental_type"));
		assertEquals(List.of(NAME, WHOLE), labels(Map.of("amenity_type", "transportation",
				"amenity_subtype", "toll_booth", "collapsable_payment_toll_type",
				"payment_e_zpass_yes;payment_pikepass_designated"), "collapsable_payment_toll_type"));
	}

	@Test
	public void otherRowsHaveNoLabels() {
		Map<String, String> tags = Map.of("amenity_type", "sustenance", "amenity_subtype", "cafe", "bar", "yes",
				"phone", "+380441234567", "description:en", "Hello", "description:de", "Hallo");
		for (String key : List.of("collapsable_sustenance", "phone", "description")) {
			assertNull(key, labels(tags, key));
		}
	}

	// Lukas-Realschule (node 9422579452); as in Android a rule the parser cannot read is dropped,
	// a value without any rule it can read is shown as it is
	@Test
	public void openingHoursAreRulesOfTheParser() {
		assertEquals("Mo-Fr 08:00-11:00; PH, SH off", openingHours("Mo-Fr 08:00-11:00; PH,SH off; nach Vereinbarung"));
		assertEquals("nach Vereinbarung", openingHours("nach Vereinbarung"));
	}

	private String openingHours(String value) {
		return service.convertToVisibleTags(Map.of(GpxUtilities.AMENITY_PREFIX + Amenity.OPENING_HOURS, value), "en")
				.stream().filter(tag -> tag.key().equals(Amenity.OPENING_HOURS)).findFirst().orElseThrow().value();
	}

	private List<ValueLabel> labels(Map<String, String> tags, String key) {
		return service.convertToVisibleTags(tags, "en").stream().filter(tag -> tag.key().equals(key)).findFirst()
				.orElseThrow().labels();
	}

	private Set<String> visibleKeys(Map<String, String> tags) {
		return service.convertToVisibleTags(tags, "en").stream().map(AmenityTagsService.VisibleTag::key)
				.collect(Collectors.toSet());
	}

	private static void assertShown(Set<String> shown, String... keys) {
		assertTrue(shown.toString(), shown.containsAll(List.of(keys)));
	}

	private static void assertHidden(Set<String> shown, String... keys) {
		assertTrue(shown.toString(), Collections.disjoint(shown, List.of(keys)));
	}

	private static List<WebGpxParser.Wpt> loadWpts(String name) throws IOException {
		try (InputStream in = AmenityTagsServiceTest.class.getResourceAsStream("/gpx/" + name);
		     Buffer source = new Buffer()) {
			source.readFrom(Objects.requireNonNull(in, name));
			return new WebGpxParser().getWpts(GpxUtilities.INSTANCE.loadGpxFile(source));
		}
	}
}
