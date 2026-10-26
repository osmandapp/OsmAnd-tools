package net.osmand.server.api.services.search;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.junit.Test;
import org.springframework.test.util.ReflectionTestUtils;

import net.osmand.binary.BinaryMapDataObject;
import net.osmand.data.LatLon;
import net.osmand.map.OsmandRegions;
import net.osmand.map.WorldRegion;

public class SpatialSearchServiceTest {

	@Test
	public void languagesWithoutPhrasesDoNotGrowTheCache() throws IOException {
		LatLon telAviv = new LatLon(32.0853, 34.7818);
		LatLon leipzig = new LatLon(51.3397, 12.3731);
		OsmandRegions regions = mock(OsmandRegions.class);
		when(regions.getSmallestBinaryMapDataObjectAt(telAviv)).thenReturn(region("he,ar,en"));
		when(regions.getSmallestBinaryMapDataObjectAt(leipzig)).thenReturn(region("de"));
		MapReadersService mapReadersService = mock(MapReadersService.class);
		when(mapReadersService.getOsmandRegions()).thenReturn(regions);
		SpatialSearchService service = new SpatialSearchService();
		ReflectionTestUtils.setField(service, "poiTypesService", new PoiTypesService());
		ReflectionTestUtils.setField(service, "mapReadersService", mapReadersService);

		service.getSpatialPoiTypeSearch(null, telAviv);
		service.getSpatialPoiTypeSearch("a1", telAviv);
		service.getSpatialPoiTypeSearch("he", telAviv);
		service.getSpatialPoiTypeSearch("a1", leipzig);

		Map<?, ?> poiSearchByLangs = (Map<?, ?>) ReflectionTestUtils.getField(service, "poiSearchByLangs");
		assertEquals(Set.of("en", "en,de"), poiSearchByLangs.keySet());
	}

	private static Map.Entry<WorldRegion, BinaryMapDataObject> region(String regionLang) {
		WorldRegion region = new WorldRegion("test");
		ReflectionTestUtils.setField(region.getParams(), "regionLang", regionLang);
		return Map.entry(region, mock(BinaryMapDataObject.class));
	}
}
