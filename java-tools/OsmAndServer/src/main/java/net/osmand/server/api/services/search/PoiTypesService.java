package net.osmand.server.api.services.search;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import net.osmand.data.AdditionalInfoBundle;
import net.osmand.data.Amenity;
import net.osmand.data.AmenityRowData;
import net.osmand.data.AmenityRowsBuilder;
import net.osmand.util.Algorithms;
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

	public List<VisibleTag> getVisibleTags(Map<String, String> tags, String lang) {
		if (tags == null || tags.isEmpty()) {
			return Collections.emptyList();
		}
		AdditionalInfoBundle infoBundle = new AdditionalInfoBundle(getMapPoiTypes(lang), tags);
		List<AmenityRowData> tagEntries = infoBundle.getVisibleTags(false); // The "note" tag is enabled only for OSM editing
		return toVisibleTags(sortInfoRows(infoBundle, tagEntries, lang), lang);
	}

	private List<AmenityRowData> sortInfoRows(AdditionalInfoBundle infoBundle, List<AmenityRowData> rows, String lang) {
		PoiCategory category = infoBundle.getCategory();
		List<AmenityRowData> named = new ArrayList<>(rows.size());
		for (AmenityRowData row : rows) {
			named.add(withSortName(row, resolveSortName(infoBundle, category, row, lang)));
		}
		AmenityRowsBuilder.sortInfoRows(named);
		return named;
	}

	private String resolveSortName(AdditionalInfoBundle infoBundle, PoiCategory category, AmenityRowData row, String lang) {
		if (row.collapsableRowType == AmenityRowData.CollapsableRowType.POI_TYPE_GROUP) {
			return resolveGroupSortName(row);
		}
		String key = row.key;
		String value = row.value;
		if (row.collapsableRowType == AmenityRowData.CollapsableRowType.PLAIN && !Algorithms.isEmpty(row.collapsableRows)) {
			AmenityRowData mainChild = findMainChild(row, lang);
			key = mainChild.key;
			value = mainChild.value;
		}
		PoiType pType = infoBundle.resolvePoiType(category, key, value).pType;
		return pType != null ? pType.getKeyName() : key;
	}

	private String resolveGroupSortName(AmenityRowData row) {
		List<PoiType> types = row.collapsablePoiTypes;
		if (row.poiAdditional) {
			return types.get(0).getKeyName();
		}
		PoiCategory groupCategory = row.collapsableCategory;
		for (PoiType pt : types) {
			groupCategory = pt.getCategory();
		}
		return groupCategory.getKeyName();
	}

	private AmenityRowData withSortName(AmenityRowData row, String name) {
		return new AmenityRowData.Builder(row.key)
				.setValue(row.value)
				.setOrder(row.order)
				.setName(name)
				.setCollapsablePoiTypes(row.collapsablePoiTypes)
				.setCollapsableRows(row.collapsableRows)
				.setCollapsableRowType(row.collapsableRowType)
				.build();
	}

	private String joinPoiTypeKeys(AmenityRowData row) {
		return row.collapsablePoiTypes.stream()
				.map(PoiType::getKeyName)
				.collect(Collectors.joining(Amenity.SEPARATOR));
	}

	private List<VisibleTag> toVisibleTags(List<AmenityRowData> rows, String lang) {
		List<VisibleTag> result = new ArrayList<>();
		for (AmenityRowData row : rows) {
			VisibleTag tag = switch (row.collapsableRowType) {
				case POI_TYPE_GROUP -> toGroupTag(row);
				case PLAIN -> toLocalizedTag(row, lang);
				default -> toPlainTag(row);
			};
			if (tag != null) {
				result.add(tag);
			}
		}
		return result;
	}

	private VisibleTag toGroupTag(AmenityRowData row) {
		String key = Amenity.COLLAPSABLE_PREFIX + row.key;
		return new VisibleTag(key, joinPoiTypeKeys(row), null);
	}

	private VisibleTag toPlainTag(AmenityRowData row) {
		return row.value != null ? new VisibleTag(row.key, row.value, null) : null;
	}

	private VisibleTag toLocalizedTag(AmenityRowData row, String lang) {
		if (Algorithms.isEmpty(row.collapsableRows)) {
			return null;
		}
		AmenityRowData mainChild = findMainChild(row, lang);
		List<LangValue> otherLangs = row.collapsableRows.stream()
				.filter(child -> child != mainChild)
				.map(this::toLangValue)
				.collect(Collectors.toList());
		LangValue mainValue = toLangValue(mainChild);
		return new VisibleTag(row.key, mainValue.value(), mainValue.lang(),
				otherLangs.isEmpty() ? null : otherLangs);
	}

	private AmenityRowData findMainChild(AmenityRowData row, String lang) {
		String headerKey = lang != null ? row.key + ":" + lang : row.key;
		return row.collapsableRows.stream()
				.filter(child -> child.key.equals(headerKey))
				.findFirst()
				.orElse(row.collapsableRows.get(0));
	}

	private LangValue toLangValue(AmenityRowData child) {
		int idx = child.key.indexOf(':');
		return idx >= 0
				? new LangValue(child.value, child.key.substring(idx + 1))
				: new LangValue(child.value, null);
	}

	public record VisibleTag(String key, String value, String lang, List<LangValue> otherLangs) {
		public VisibleTag(String key, String value, String lang) {
			this(key, value, lang, null);
		}
	}

	public record LangValue(String value, String lang) {
	}
}
