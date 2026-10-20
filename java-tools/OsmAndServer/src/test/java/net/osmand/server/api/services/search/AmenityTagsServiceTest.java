package net.osmand.server.api.services.search;

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
import java.util.function.Function;
import java.util.stream.Collectors;

import org.junit.Test;

import net.osmand.server.utils.WebGpxParser;
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
				"internet_access:fee", "no", "authentication:phone_call:number", "+380441234567",
				"test:country", "US", "my_note", "hello");
		Set<String> generic = service.convertToVisibleTags(tags, "en").stream()
				.filter(tag -> Boolean.TRUE.equals(tag.generic())).map(AmenityTagsService.VisibleTag::key)
				.collect(Collectors.toSet());
		assertEquals(Set.of("test:country", "my_note"), generic);
		assertShown(visibleKeys(tags), "internet_access:fee", "authentication:phone_call:number");
	}

	// a group of one property (wheelchair=*) or of a list of options (boat:<kind>:rental=yes) with its OSM values
	@Test
	public void groupsTellOneTagAndValues() {
		Map<String, String> tags = Map.of("amenity_type", "sustenance", "amenity_subtype", "cafe",
				"collapsable_wheelchair_accessibility", "wheelchair_limited");
		AmenityTagsService.VisibleTag wheelchair = service.convertToVisibleTags(tags, "en").stream()
				.filter(tag -> tag.key().equals("collapsable_wheelchair_accessibility")).findFirst().orElseThrow();
		assertEquals(Boolean.TRUE, wheelchair.oneTag());
		assertEquals(List.of("limited"), wheelchair.osmValues());

		Map<String, String> boats = Map.of("amenity_type", "service", "amenity_subtype", "boat_rental",
				"collapsable_boat_rental_type", "boat_kayak_rental_yes;boat_canoe_rental_yes");
		AmenityTagsService.VisibleTag rented = service.convertToVisibleTags(boats, "en").stream()
				.filter(tag -> tag.key().equals("collapsable_boat_rental_type")).findFirst().orElseThrow();
		assertEquals(Boolean.FALSE, rented.oneTag());
		assertEquals(List.of("yes", "yes"), rented.osmValues());
	}

	// only a group of additional values carries oneTag / osmValues
	@Test
	public void otherRowsHaveNoGroupFields() {
		Map<String, String> tags = Map.of("amenity_type", "sustenance", "amenity_subtype", "cafe", "bar", "yes",
				"phone", "+380441234567", "description:en", "Hello", "description:de", "Hallo",
				"collapsable_smoking", "smoking_no");
		Map<String, AmenityTagsService.VisibleTag> byKey = service.convertToVisibleTags(tags, "en").stream()
				.collect(Collectors.toMap(AmenityTagsService.VisibleTag::key, Function.identity()));
		assertEquals(Set.of("collapsable_sustenance", "phone", "description", "collapsable_smoking"), byKey.keySet());
		for (String key : List.of("collapsable_sustenance", "phone", "description")) {
			assertNull(key, byKey.get(key).oneTag());
			assertNull(key, byKey.get(key).osmValues());
		}
		assertEquals(Boolean.TRUE, byKey.get("collapsable_smoking").oneTag());
		assertEquals(List.of("no"), byKey.get("collapsable_smoking").osmValues());
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
