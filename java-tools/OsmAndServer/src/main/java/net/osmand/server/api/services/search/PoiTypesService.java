package net.osmand.server.api.services.search;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.xmlpull.v1.XmlPullParser;
import org.xmlpull.v1.XmlPullParserException;
import org.xmlpull.v1.XmlPullParserFactory;

import net.osmand.osm.AbstractPoiType;
import net.osmand.osm.MapPoiTypes;
import net.osmand.osm.MapRenderingTypes;
import net.osmand.osm.PoiCategory;
import net.osmand.osm.PoiType;
import net.osmand.search.SearchUICore;
import net.osmand.search.core.SearchCoreFactory;
import net.osmand.search.core.SearchResult;
import net.osmand.server.utils.MapPoiTypesTranslator;

@Service
public class PoiTypesService {

	public static final String DEFAULT_SEARCH_LANG = "en";
	private static final String AND_RES = "/androidResources/";

	@Autowired
	private SearchResultConverter searchResultConverter;

	private final ConcurrentHashMap<String, Map<String, String>> translationsCache = new ConcurrentHashMap<>();
	private final ConcurrentHashMap<String, MapPoiTypes> poiTypesByLocale = new ConcurrentHashMap<>();

	public MapPoiTypes getMapPoiTypes(String locale) {
		locale = locale == null ? DEFAULT_SEARCH_LANG : locale;

		return poiTypesByLocale.computeIfAbsent(locale, loc -> {
			MapPoiTypes mapPoiTypes = new MapPoiTypes(null);
			mapPoiTypes.init();
			Map<String, String> translations = getTranslations(loc);
			Map<String, String> enTranslations = getTranslations(DEFAULT_SEARCH_LANG);
			mapPoiTypes.setPoiTranslator(new MapPoiTypesTranslator(translations, enTranslations));
			return mapPoiTypes;
		});
	}

	private Map<String, String> getTranslations(String locale) {
		return translationsCache.computeIfAbsent(locale, loc -> {
			try {
				String validLoc = validateLocale(loc);
				String localPath = validLoc.equals("en") ? "values" : "values-" + validLoc;

				InputStream phrasesStream = this.getClass().getResourceAsStream(AND_RES + localPath + "/phrases.xml");
				if (phrasesStream == null) {
					throw new IllegalArgumentException("Locale not found: " + loc);
				}

				return parseStringsXml(phrasesStream);
			} catch (XmlPullParserException | IOException e) {
				throw new RuntimeException(e);
			}
		});
	}

	public MapPoiTypesTranslator parseGlobalTranslations() {
		Map<String, String> enTranslations = getTranslations(DEFAULT_SEARCH_LANG);
		MapPoiTypesTranslator translations = new MapPoiTypesTranslator(enTranslations, enTranslations);
		for (String l : MapRenderingTypes.langs) {
			InputStream phrasesStream = this.getClass().getResourceAsStream(AND_RES + "values-" + l + "/phrases.xml");
			if (phrasesStream != null) {
				try {
					Map<String, String> stringsXml = parseStringsXml(phrasesStream);
					translations.appendTranslations(l, stringsXml);
				} catch (XmlPullParserException | IOException e) {
					throw new RuntimeException(e);
				}
			}
		}
		return translations;
	}

	private Map<String, String> parseStringsXml(InputStream inputStream) throws XmlPullParserException, IOException {
		Map<String, String> resultMap = new HashMap<>();

		XmlPullParserFactory factory = XmlPullParserFactory.newInstance();
		XmlPullParser parser = factory.newPullParser();
		parser.setInput(inputStream, "UTF-8");

		int eventType = parser.getEventType();
		String key = null;
		String value = null;

		while (eventType != XmlPullParser.END_DOCUMENT) {
			String tagName = parser.getName();
			switch (eventType) {
				case XmlPullParser.START_TAG:
					if (tagName.equals("string")) {
						key = parser.getAttributeValue(null, "name");
					}
					break;
				case XmlPullParser.TEXT:
					value = parser.getText();
					break;
				case XmlPullParser.END_TAG:
					if (tagName.equals("string") && key != null && value != null) {
						resultMap.put(key, value);
						key = null;
						value = null;
					}
					break;
			}
			eventType = parser.next();
		}
		inputStream.close();
		return resultMap;
	}

	private String validateLocale(String locale) {
		if (locale == null || locale.isEmpty()) {
			throw new IllegalArgumentException("Locale cannot be null or empty");
		}
		// Remove potentially dangerous characters such as '/'
		return locale.replaceAll("[/\\\\]", "");
	}

	public List<String> getTopFilters() {
		List<String> filters = new ArrayList<>();
		SearchUICore searchUICore = new SearchUICore(MapPoiTypes.getDefault(), DEFAULT_SEARCH_LANG, true);
		searchUICore.getPoiTypes().getTopVisibleFilters().forEach(f -> filters.add(f.getKeyName()));
		return filters;
	}

	public Map<String, Map<String, String>> searchPoiCategories(String search, String locale) {
		Map<String, Map<String, String>> results = new HashMap<>();
		List<SearchResult> categories = searchPoiCategoriesByName(search, locale);
		if (categories != null && !categories.isEmpty()) {
			results = preparePoiCategoriesResult(categories);
		}
		return results;
	}

	private List<SearchResult> searchPoiCategoriesByName(String search, String locale) {
		MapPoiTypes mapPoiTypes = getMapPoiTypes(locale);
		SearchUICore searchUICore = new SearchUICore(mapPoiTypes, locale, true);
		SearchCoreFactory.SearchAmenityTypesAPI searchAmenityTypesAPI = new SearchCoreFactory.SearchAmenityTypesAPI(
				mapPoiTypes);
		List<AbstractPoiType> topFilters = searchUICore.getPoiTypes().getTopVisibleFilters();
		List<String> filterOrder = topFilters.stream().map(AbstractPoiType::getKeyName).toList();
		searchAmenityTypesAPI.setActivePoiFiltersByOrder(filterOrder);
		searchUICore.registerAPI(searchAmenityTypesAPI);

		return searchUICore.immediateSearch(search, null).getCurrentSearchResults();
	}

	private Map<String, Map<String, String>> preparePoiCategoriesResult(List<SearchResult> results) {
		Map<String, Map<String, String>> searchRes = new HashMap<>();
		results.forEach(res -> searchRes.put(res.localeName, searchResultConverter.getPoiTypeFields(res.object)));
		return searchRes;
	}

	public Map<String, List<String>> searchPoiCategories(String locale) {
		SearchUICore searchUICore = new SearchUICore(getMapPoiTypes(locale), locale, false);
		List<PoiCategory> categoriesList = searchUICore.getPoiTypes().getCategories(false);
		Map<String, List<String>> res = new HashMap<>();
		categoriesList.forEach(poiCategory -> {
			String category = poiCategory.getKeyName();
			List<PoiType> poiTypes = poiCategory.getPoiTypes();
			List<String> typesNames = new ArrayList<>();
			poiTypes.forEach(type -> typesNames.add(type.getOsmValue()));
			res.put(category, typesNames);

		});
		return res;
	}

	public SearchUICore.SearchResultCollection addPoiCategoriesToSearchResult(
			SearchUICore.SearchResultCollection resultCollection, String text, String locale,
			SearchUICore searchUICore) {
		List<SearchResult> poiCategories = searchPoiCategoriesByName(text, locale);
		List<SearchResult> uniquePoiCategories = new ArrayList<>(poiCategories.stream()
				.collect(Collectors.toMap(sr -> sr.localeName, Function.identity(), (first, second) -> first))
				.values());
		if (!uniquePoiCategories.isEmpty()) {
			if (resultCollection != null) {
				resultCollection.addSearchResults(uniquePoiCategories, true, true);
			} else {
				resultCollection = searchUICore.getCurrentSearchResult();
				if (resultCollection != null) {
					resultCollection.addSearchResults(uniquePoiCategories, true, true);
				}
			}
		}
		return resultCollection;
	}
}
