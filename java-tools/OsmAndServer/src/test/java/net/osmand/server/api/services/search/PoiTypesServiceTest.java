package net.osmand.server.api.services.search;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import net.osmand.osm.AbstractPoiType;
import net.osmand.osm.MapPoiTypes;
import net.osmand.osm.PoiAdditionalLabels;

public class PoiTypesServiceTest {

	private final MapPoiTypes poiTypes = new PoiTypesService().getMapPoiTypes("en");

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

	private AbstractPoiType type(String key) {
		AbstractPoiType type = poiTypes.getAnyPoiTypeByKey(key);
		return type != null ? type : poiTypes.getAnyPoiAdditionalTypeByKey(key);
	}
}
