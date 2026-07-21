package net.osmand.server.utils;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import net.osmand.osm.AbstractPoiType;
import net.osmand.osm.MapPoiTypes;
import net.osmand.util.Algorithms;

public class MapPoiTypesTranslator implements MapPoiTypes.PoiTranslator {

	private final Map<String, Set<String>> enPhrases = new HashMap<>();
	private final Map<String, Set<String>> phrases = new HashMap<>();

	public MapPoiTypesTranslator(Map<String, String> main, Map<String, String> enPhrases) {
		appendMap(this.phrases, main);
		appendMap(this.enPhrases, enPhrases);
	}

	public void appendTranslations(String lang, Map<String, String> main) {
		appendMap(phrases, main);
	}

	private void appendMap(Map<String, Set<String>> phrases, Map<String, String> main) {
		Iterator<Entry<String, String>> it = main.entrySet().iterator();
		while (it.hasNext()) {
			Entry<String, String> e = it.next();
			Set<String> set = phrases.get(e.getKey());
			if (!phrases.containsKey(e.getKey())) {
				set = new LinkedHashSet<String>();
				phrases.put(e.getKey(), set);
			}
			String[] vls = e.getKey().split(";");
			for (String v : vls) {
				set.add(v);
			}
		}
	}

	@Override
	public String getTranslation(AbstractPoiType type) {
		AbstractPoiType baseLangType = type.getBaseLangType();
		if (baseLangType != null) {
			return getTranslation(baseLangType) + " (" + type.getLang().toLowerCase() + ")";
		}
		return getTranslation(type.getIconKeyName());
	}

	@Override
	public String getTranslation(String keyName) {
		Set<String> st = phrases.get("poi_" + keyName);
		if (st != null) {
			return st.iterator().next();
		}
		return null;
	}

	@Override
	public String getEnTranslation(AbstractPoiType type) {
		AbstractPoiType baseLangType = type.getBaseLangType();
		if (baseLangType != null) {
			return getEnTranslation(baseLangType) + " (" + type.getLang().toLowerCase() + ")";
		}
		return getEnTranslation(type.getIconKeyName());
	}

	@Override
	public String getEnTranslation(String keyName) {
		if (enPhrases.isEmpty()) {
			return Algorithms.capitalizeFirstLetter(keyName.replace('_', ' '));
		}
		Set<String> set = enPhrases.get("poi_" + keyName);
		if (set != null) {
			return set.iterator().next();
		}
		return null;
	}

	@Override
	public String getSynonyms(AbstractPoiType type) {
		AbstractPoiType baseLangType = type.getBaseLangType();
		if (baseLangType != null) {
			return getSynonyms(baseLangType);
		}
		return getSynonyms(type.getIconKeyName());
	}

	@Override
	public String getSynonyms(String keyName) {
		Set<String> set = phrases.get("poi_" + keyName);
		StringBuilder sb = new StringBuilder();
		if (set != null) {
			Iterator<String> it = set.iterator();
			it.next();
			while (it.hasNext()) {
				sb.append(it.next());
				if (it.hasNext()) {
					sb.append(";");
				}
			}
			return sb.toString();
		}
		return null;
	}

	@Override
	public String getAllLanguagesTranslationSuffix() {
		return "all languages";
	}
}
