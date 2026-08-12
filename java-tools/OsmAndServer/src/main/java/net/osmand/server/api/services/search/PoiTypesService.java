package net.osmand.server.api.services.search;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import net.osmand.data.AdditionalInfoBundle;
import net.osmand.data.Amenity;
import net.osmand.data.AmenityTagEntry;
import net.osmand.data.AmenityTagEntriesBuilder;
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
		List<String> preferredLangs = lang != null ? List.of(lang) : List.of();
		boolean allowNoteTag = false; // The "note" tag is enabled only for OSM editing.
		List<AmenityTagEntry> tagEntries = infoBundle.getVisibleTags(allowNoteTag, preferredLangs);

		List<AmenityTagEntry> infoTagEntries = new ArrayList<>();
		List<AmenityTagEntry> descriptionTagEntries = new ArrayList<>();
		for (AmenityTagEntry tagEntry : tagEntries) {
			if (tagEntry.isDescription) {
				descriptionTagEntries.add(tagEntry);
			} else {
				infoTagEntries.add(tagEntry);
			}
		}
		AmenityTagEntriesBuilder.sortDescriptionEntries(descriptionTagEntries, lang);

		List<AmenityTagEntry> sortedTagEntries = sortTagEntries(infoBundle, infoTagEntries);
		sortedTagEntries.addAll(descriptionTagEntries);
		return toVisibleTags(sortedTagEntries);
	}

	private List<AmenityTagEntry> sortTagEntries(AdditionalInfoBundle infoBundle, List<AmenityTagEntry> tagEntries) {
		PoiCategory category = infoBundle.getCategory();
		List<AmenityTagEntry> namedTagEntries = new ArrayList<>();
		for (AmenityTagEntry tagEntry : tagEntries) {
			namedTagEntries.add(buildWithName(tagEntry, resolveSortName(infoBundle, category, tagEntry)));
		}
		AmenityTagEntriesBuilder.sortInfoEntries(namedTagEntries);
		return namedTagEntries;
	}

	private String resolveSortName(AdditionalInfoBundle infoBundle, PoiCategory category, AmenityTagEntry tagEntry) {
		if (tagEntry.collapsableEntryType == AmenityTagEntry.CollapsableEntryType.POI_TYPE_GROUP) {
			return resolveGroupSortName(tagEntry);
		}
		PoiType pType = infoBundle.resolvePoiType(category, tagEntry.key, tagEntry.value).pType;
		return pType != null ? pType.getKeyName() : tagEntry.key;
	}

	private String resolveGroupSortName(AmenityTagEntry tagEntry) {
		List<PoiType> types = tagEntry.collapsablePoiTypes;
		if (tagEntry.poiAdditional) {
			return types.get(0).getKeyName();
		}
		return types.get(0).getCategory().getKeyName();
	}

	private AmenityTagEntry buildWithName(AmenityTagEntry tagEntry, String name) {
		return AmenityTagEntry.Builder.from(tagEntry).setName(name).build();
	}

	private String joinPoiTypeKeys(AmenityTagEntry tagEntry) {
		return tagEntry.collapsablePoiTypes.stream()
				.map(PoiType::getKeyName)
				.collect(Collectors.joining(Amenity.SEPARATOR));
	}

	private List<VisibleTag> toVisibleTags(List<AmenityTagEntry> tagEntries) {
		List<VisibleTag> result = new ArrayList<>();
		for (AmenityTagEntry tagEntry : tagEntries) {
			VisibleTag tag = switch (tagEntry.collapsableEntryType) {
				case POI_TYPE_GROUP -> toGroupTag(tagEntry);
				case PLAIN -> toLocalizedTag(tagEntry);
				default -> toPlainTag(tagEntry);
			};
			if (tag != null) {
				result.add(tag);
			}
		}
		return result;
	}

	private VisibleTag toGroupTag(AmenityTagEntry tagEntry) {
		String key = Amenity.COLLAPSABLE_PREFIX + tagEntry.key;
		return new VisibleTag(key, joinPoiTypeKeys(tagEntry), null);
	}

	private VisibleTag toPlainTag(AmenityTagEntry tagEntry) {
		return tagEntry.value != null ? new VisibleTag(tagEntry.key, tagEntry.value, null) : null;
	}

	private VisibleTag toLocalizedTag(AmenityTagEntry tagEntry) {
		LangValue mainValue = toLangValue(tagEntry);
		List<LangValue> otherLangs = tagEntry.collapsableEntries.stream()
				.map(this::toLangValue)
				.collect(Collectors.toList());
		int idx = tagEntry.key.indexOf(':');
		String mainKey = idx >= 0 ? tagEntry.key.substring(0, idx) : tagEntry.key;
		return new VisibleTag(mainKey, mainValue.value(), mainValue.lang(),
				otherLangs.isEmpty() ? null : otherLangs);
	}

	private LangValue toLangValue(AmenityTagEntry child) {
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
