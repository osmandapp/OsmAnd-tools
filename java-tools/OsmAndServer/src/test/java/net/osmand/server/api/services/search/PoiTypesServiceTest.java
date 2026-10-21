package net.osmand.server.api.services.search;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Map;

import org.junit.Test;

import net.osmand.osm.AbstractPoiType;
import net.osmand.osm.MapPoiTypes;
import net.osmand.osm.PoiAdditionalLabels;

public class PoiTypesServiceTest {

	private final PoiTypesService service = new PoiTypesService();
	private final MapPoiTypes poiTypes = service.getMapPoiTypes("en");

	@Test
	public void typesAreTranslatedByTheirKeyNotByTheirIcon() {
		assertEquals("Enforcement: maxheight", type("enforcement_maxheight").getTranslation());
		assertEquals("End date", type("end_date").getTranslation());
	}

	@Test
	public void categoriesAreNamedLikeTheApps() {
		assertEquals("Enforcement: maxspeed", PoiAdditionalLabels.getSearchName(type("enforcement_maxspeed")));
		assertEquals("Subway station", PoiAdditionalLabels.getSearchName(type("subway_station")));
	}

	@Test
	public void searchCategoriesGetTheNamesOfTheApps() {
		assertEquals(Map.of("subway_station", "Subway station", "enforcement_maxspeed", "Enforcement: maxspeed"),
				service.getCategoryNames(List.of("subway_station", "enforcement_maxspeed", "McDonald's"), "en"));
	}

	@Test
	public void categoriesAreNamedInTheLanguageOfTheRequest() {
		assertEquals("Станція метро", service.getCategoryName("subway_station", "uk"));
		assertEquals("Туристична пам'ятка", service.getCategoryName("attraction", "uk"));
		assertEquals("Subway station", service.getCategoryName("subway_station", null));
		assertEquals("Subway station", service.getCategoryName("subway_station", ""));
	}

	private AbstractPoiType type(String key) {
		AbstractPoiType type = poiTypes.getAnyPoiTypeByKey(key);
		return type != null ? type : poiTypes.getAnyPoiAdditionalTypeByKey(key);
	}
}
