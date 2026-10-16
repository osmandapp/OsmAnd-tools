package net.osmand.util.abbrs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.osmand.search.rules.SearchModLocales;
import net.osmand.util.abbrs.PoiExtraction.ExtractedEntity;
import net.osmand.util.abbrs.PoiExtraction.ExtractedPoi;

/**
 * "A row of top_abbrs.csv applies to a POI" (top-org-abbrs-spec.md 5.2, 8.2): one condition for the analyzer (gain,
 * coverage) and the generator. The row applies when
 * <ul>
 * <li>{@code owner} is {@code *} or the region of the map or its ancestor;</li>
 * <li>{@code kind=org}: {@code locale} matches the locale of the map; {@code kind=word}: the locale of a name
 * ({@link SearchModLocales#forName}); a language row ("de") covers every locale of the language;</li>
 * <li>{@code types} is empty or holds the subtype or an additional type of the POI;</li>
 * <li>{@code bind=operator/network}: a normalized value of the tag equals {@code bind_value};
 * {@code bind=phrase}: a name has the words of {@code bind_value} in a row and the entity has no tag
 * {@code require_absent};</li>
 * <li>the abbreviation is not a word of an indexed name yet.</li>
 * </ul>
 * Names are those of the name index of the POI ({@link ExtractedPoi#names}): the phrase and the abbreviation are
 * looked for where the generator indexes them.
 */
public class TopAbbrPredicate {

	private final SearchModLocales locales;
	private final RegionTree regions;
	private final Map<String, String> mapLocales = new HashMap<>();
	private final Map<String, List<String>> mapChains = new HashMap<>();

	public TopAbbrPredicate(SearchModLocales locales, RegionTree regions) {
		this.locales = locales;
		this.regions = regions;
	}

	public String mapLocale(String map) {
		return mapLocales.computeIfAbsent(map, m -> locales.normalize(locales.forMap(m)));
	}

	public boolean applies(TopAbbrRow row, ExtractedPoi poi) {
		ExtractedEntity e = poi.entity;
		String mapLocale = mapLocale(poi.map);
		if (!TopAbbrRow.OWNER_ANY.equals(row.owner)) {
			List<String> chain = mapChains.computeIfAbsent(poi.map,
					m -> regions == null ? new ArrayList<>() : regions.chain(m));
			if (!chain.contains(row.owner)) {
				return false;
			}
		}
		if (TopAbbrRow.KIND_ORG.equals(row.kind) && !localeMatches(row.locale, mapLocale)) {
			return false;
		}
		if (!row.types.isEmpty() && !poi.hasType(row.types)) {
			return false;
		}
		if (TopAbbrRow.BIND_PHRASE.equals(row.bind)) {
			if (!row.requireAbsent.isEmpty() && e.tag(row.requireAbsent) != null) {
				return false;
			}
			if (!hasPhrase(row, poi, mapLocale)) {
				return false;
			}
		} else if (TopAbbrRow.KIND_WORD.equals(row.kind) || !hasBindValue(e, row.bind, row.bindValue)) {
			// a word row binds only by phrase
			return false;
		}
		return !hasWord(poi, row.abbrNormalized());
	}

	private boolean hasPhrase(TopAbbrRow row, ExtractedPoi poi, String mapLocale) {
		List<String> phrase = row.bindWords();
		for (Map.Entry<String, String> t : poi.names.entrySet()) {
			if (TopAbbrRow.KIND_WORD.equals(row.kind)
					&& !localeMatches(row.locale, locales.forName(nameLanguageTag(t.getKey()), mapLocale))) {
				continue;
			}
			for (String v : AbbrText.values(t.getValue())) {
				if (AbbrText.containsPhrase(AbbrText.words(v), phrase)) {
					return true;
				}
			}
		}
		return false;
	}

	/** a language row ("de") covers its locales (de_DE, de_IT), a locale row only itself */
	public boolean localeMatches(String rowLocale, String nameLocale) {
		if (rowLocale.equals(nameLocale)) {
			return true;
		}
		return rowLocale.indexOf('_') < 0 && !rowLocale.isEmpty() && rowLocale.equals(locales.language(nameLocale));
	}

	public static boolean hasBindValue(ExtractedEntity e, String tag, String bindValue) {
		for (String v : AbbrText.values(e.tag(tag))) {
			if (AbbrText.normalize(v).equals(bindValue)) {
				return true;
			}
		}
		return false;
	}

	/** the word is a word of an indexed name of the POI */
	public static boolean hasWord(ExtractedPoi poi, String word) {
		for (String name : poi.names.values()) {
			for (String v : AbbrText.values(name)) {
				if (AbbrText.words(v).contains(word)) {
					return true;
				}
			}
		}
		return false;
	}

	/** tag for SearchModLocales.forName: null for the names in the language of the map */
	public static String nameLanguageTag(String tag) {
		return tag.indexOf(':') < 0 ? null : tag;
	}
}
