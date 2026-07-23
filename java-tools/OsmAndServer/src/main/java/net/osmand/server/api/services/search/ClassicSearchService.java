package net.osmand.server.api.services.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.binary.BinaryMapIndexReaderStats;
import net.osmand.data.LatLon;
import net.osmand.data.QuadRect;
import net.osmand.search.SearchUICore;
import net.osmand.search.core.ObjectType;
import net.osmand.search.core.SearchCoreFactory;
import net.osmand.search.core.SearchExportSettings;
import net.osmand.search.core.SearchPhrase;
import net.osmand.search.core.SearchResult;
import net.osmand.search.core.SearchSettings;
import net.osmand.server.api.services.OsmAndMapsService;
import net.osmand.server.controllers.pub.GeojsonClasses.Feature;

@Service
public class ClassicSearchService {

	private static final Log LOGGER = LogFactory.getLog(ClassicSearchService.class);

	private static final ObjectType[] CLASSIC_SEARCH_TYPES = {
			// Exclude PARTIAL_LOCATION and app-only types (fav, tracks, markers, etc.)
			ObjectType.CITY, ObjectType.VILLAGE, ObjectType.BOUNDARY, ObjectType.POSTCODE,
			ObjectType.STREET, ObjectType.HOUSE, ObjectType.STREET_INTERSECTION,
			ObjectType.POI_TYPE, ObjectType.POI, ObjectType.LOCATION, ObjectType.REGION
	};

	private static final int SEARCH_RADIUS_LEVEL = 1;
	private static final int TOTAL_LIMIT_SEARCH_RESULTS = 15000;
	static final int TOTAL_LIMIT_SEARCH_RESULTS_TO_WEB = 1000;
	static final String DELIMITER = " ";

	@Autowired
	OsmAndMapsService osmAndMapsService;

	@Autowired
	private MapReadersService mapReadersService;

	@Autowired
	private PoiTypesService poiTypesService;

	@Autowired
	private SearchResultConverter searchResultConverter;

	@Autowired
	private PoiSearchService poiSearchService;

	public record SearchContext(double lat, double lon, String text, String locale, boolean baseSearch,
	                            String northWest, String southEast) {
	}

	public record SearchOption(boolean unlimited, SearchExportSettings exportedSettings, Double radiusToLoadMaps,
	                           boolean queryIsCompleted, boolean isBatch, ObjectType... searchTypes) {
		private static final double SEARCH_RADIUS_DEGREE = 1.5;

		public double getRadius() {
			return radiusToLoadMaps == null ? SEARCH_RADIUS_DEGREE : radiusToLoadMaps;
		}
	}

	public record SearchResults(List<SearchResult> results, SearchSettings settings, JSONObject unitTestJson,
	                            SearchPhrase phrase) {
		public SearchResults(List<SearchResult> results) {
			this(results, null, null, null);
		}
	}

	public List<Feature> search(SearchContext ctx, String timeZone) throws IOException {
		long tm = System.currentTimeMillis();
		SearchResults searchResults = getImmediateSearchResults(ctx,
				new SearchOption(false, null, null, true, false, CLASSIC_SEARCH_TYPES), null);
		List<SearchResult> res = searchResults.results();
		if (System.currentTimeMillis() - tm > 1000) {
			BinaryMapIndexReaderStats.SearchStat stat = searchResults.settings() != null
					? searchResults.settings().getStat()
					: null;
			LOGGER.info(String.format("Search %s results %d took %.2f sec - %s", ctx.text(),
					searchResults.results() == null ? 0 : searchResults.results().size(),
					(System.currentTimeMillis() - tm) / 1000.0, stat));
		}
		List<Feature> features = new ArrayList<>();
		if (res != null && !res.isEmpty()) {
			searchResultConverter.saveSearchResult(res, features, timeZone);
		}

		return !features.isEmpty() ? features : Collections.emptyList();
	}

	public SearchResults getImmediateSearchResults(SearchContext ctx, SearchOption option,
	                                               Consumer<List<SearchResult>> consumerInContext) throws IOException {
		if (!osmAndMapsService.validateAndInitConfig()) {
			return new SearchResults(Collections.emptyList());
		}
		SearchUICore searchUICore = new SearchUICore(poiTypesService.getMapPoiTypes(ctx.locale()), ctx.locale(), false);
		if (option.searchTypes() != null) {
			searchUICore.getPhrase().getSettings().updateSearchTypes(option.searchTypes());
		}
		if (!option.unlimited()) {
			searchUICore.setTotalLimit(TOTAL_LIMIT_SEARCH_RESULTS);
		}
		searchUICore.getSearchSettings().setRegions(mapReadersService.getOsmandRegions());

		QuadRect points = osmAndMapsService.points(null,
				new LatLon(ctx.lat() + option.getRadius(), ctx.lon() - option.getRadius()),
				new LatLon(ctx.lat() - option.getRadius(), ctx.lon() + option.getRadius()));
		List<BinaryMapIndexReader> usedMapList = new ArrayList<>();
		try {
			List<OsmAndMapsService.BinaryMapIndexReaderReference> list = mapReadersService.getMapsForSearch(points, ctx.baseSearch());
			if (list.isEmpty()) {
				return new SearchResults(Collections.emptyList());
			}
			usedMapList = osmAndMapsService.getReaders(list, null);
			if (usedMapList.isEmpty()) {
				return new SearchResults(Collections.emptyList());
			}
			SearchSettings settings = searchUICore.getPhrase().getSettings();
			BinaryMapIndexReaderStats.SearchStat stat = new BinaryMapIndexReaderStats.SearchStat();
			stat.isBatch = option.isBatch();
			settings.setStat(stat);

			settings.setOfflineIndexes(usedMapList);
			settings.setRadiusLevel(SEARCH_RADIUS_LEVEL);
			settings.setExportSettings(option.exportedSettings());
			searchUICore.updateSettings(settings);

			searchUICore.init();
			searchUICore.registerAPI(new SearchCoreFactory.SearchRegionByNameAPI());

			SearchUICore.SearchResultCollection resultCollection = searchUICore.immediateSearch(
					ctx.text() + (option.queryIsCompleted() ? DELIMITER : ""), new LatLon(ctx.lat(), ctx.lon()));
			resultCollection = poiTypesService.addPoiCategoriesToSearchResult(resultCollection, ctx.text(), ctx.locale(), searchUICore);

			List<SearchResult> res = resultCollection != null ? resultCollection.getCurrentSearchResults()
					: Collections.emptyList();
			res = poiSearchService.filterBrandsOutsideBBox(res, ctx.northWest(), ctx.southEast(), ctx.locale(), ctx.lat(), ctx.lon(),
					ctx.baseSearch());
			res = res.size() > TOTAL_LIMIT_SEARCH_RESULTS_TO_WEB ? res.subList(0, TOTAL_LIMIT_SEARCH_RESULTS_TO_WEB)
					: res;
			if (consumerInContext != null) {
				consumerInContext.accept(res);
			}

			JSONObject unitTestJson = null;
			if (option.exportedSettings() != null) {
				unitTestJson = SearchUICore.createTestJSON(resultCollection, settings.getExportedObjects(),
						settings.getExportedCities());
			}
			return new SearchResults(res, settings, unitTestJson,
					resultCollection == null ? null : resultCollection.getPhrase());
		} finally {
			osmAndMapsService.unlockReaders(usedMapList);
		}
	}
}
