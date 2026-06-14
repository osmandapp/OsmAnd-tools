package net.osmand.obf.preparation;

import static net.osmand.util.SearchAlgorithms.splitAndNormalize;

import java.text.Collator;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import net.osmand.data.MapObject;
import net.osmand.obf.preparation.IndexPoiCreator.PoiTileBox;
import net.osmand.util.Algorithms;
import net.osmand.util.SearchAlgorithms;
import net.osmand.util.SearchIndexPrepareAlgorithms;
import net.sf.junidecode.Junidecode;

public class NameIndexCreator {

	Map<String, MapObjectIndex> namesIndex = new TreeMap<String, MapObjectIndex>(Collator.getInstance());;

	Map<String, Set<PoiTileBox>> poiData = new TreeMap<String, Set<PoiTileBox>>();

	public static class MapObjectIndex {
		private final Map<MapObject, LinkedHashSet<String>> objectTokens = new LinkedHashMap<>();

		public void addToken(MapObject object, String token) {
			objectTokens.computeIfAbsent(object, ignored -> new LinkedHashSet<>()).add(token);
		}

		public Set<MapObject> getObjects() {
			return objectTokens.keySet();
		}

		public Set<String> getTokens(MapObject object) {
			LinkedHashSet<String> tokens = objectTokens.get(object);
			return tokens == null ? Collections.emptySet() : tokens;
		}
	}

	public void putPoiObjectPrefix(String name, String nameEn, PoiTileBox data, Set<String> names, Set<String> idNames,
			IndexCreatorSettings settings) {
		if (name != null) {
			parsePoiPrefix(name, data, settings.charsToBuildPoiNameIndex);
			if (Algorithms.isEmpty(nameEn)) {
				nameEn = Junidecode.unidecode(name);
			}
		}
		if (!Algorithms.objectEquals(nameEn, name) && !Algorithms.isEmpty(nameEn)) {
			parsePoiPrefix(nameEn, data, settings.charsToBuildPoiNameIndex);
		}
		if (names != null) {
			for (String nk : names) {
				if (!Algorithms.objectEquals(nk, name) && !Algorithms.isEmpty(nk)) {
					parsePoiPrefix(nk, data, settings.charsToBuildPoiNameIndex);
				}
			}
		}
		if (idNames != null) {
			for (String nk : idNames) {
				if (!Algorithms.isEmpty(nk)) {
					parsePoiPrefix(nk, data, settings.charsToBuildPoiIdNameIndex);
				}
			}
		}
	}

	private void parsePoiPrefix(String name, PoiTileBox data, int ind) {
		List<String> splitName = SearchAlgorithms.splitAndNormalize(name);
		SearchAlgorithms.removeCommonWords(splitName);
		for (String token : splitName) {
			if (Algorithms.isEmpty(token)) {
				continue;
			}
			String str = SearchIndexPrepareAlgorithms.nameIndexPreparePrefix(token, ind);
			if (Algorithms.isEmpty(str)) {
				continue;
			}
			if (!poiData.containsKey(str)) {
				poiData.put(str, new LinkedHashSet<>());
			}
			poiData.get(str).add(data);
			data.addToken(token);
		}
	}

	private void parseAddrPrefix(String name, MapObject data, IndexCreatorSettings settings) {
		name = removeBraces(name);
		List<String> splitNames = splitAndNormalize(name);
		SearchAlgorithms.removeCommonWords(splitNames);
		// add to the map
		for (String token : splitNames) {
			String val = SearchIndexPrepareAlgorithms.nameIndexPreparePrefix(token,
					settings.charsToBuildAddressNameIndex);
			if (val.isEmpty()) {
				continue;
			}
			MapObjectIndex entry = namesIndex.get(val);
			if (entry == null) {
				entry = new MapObjectIndex();
				namesIndex.put(val, entry);
			}
			entry.addToken(data, token);
		}
	}

	public void putAddrNamedMapObject(MapObject o, long fileOffset, IndexCreatorSettings settings) {
		String name = o.getName();
		parseAddrPrefix(name, o, settings);
		// getOtherNames ignores "admin_level", "place"
		for (String nm : o.getOtherNames(true, name)) {
			if (!nm.equals(name)) {
				parseAddrPrefix(nm, o, settings);
			}
		}
		if (fileOffset > Integer.MAX_VALUE) {
			throw new IllegalArgumentException("File offset > 2 GB.");
		}
		o.setFileOffset((int) fileOffset);
	}

	private static String removeBraces(String localeName) {
		int i = localeName.indexOf('(');
		String retName = localeName;
		if (i > -1) {
			retName = localeName.substring(0, i);
			int j = localeName.indexOf(')', i);
			if (j > -1) {
				// remove
				retName = retName.trim() + ' ' + localeName.substring(j + 1).trim();
			}
		}
		return retName;
	}

}
