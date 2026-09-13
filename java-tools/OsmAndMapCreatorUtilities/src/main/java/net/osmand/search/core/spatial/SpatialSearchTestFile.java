package net.osmand.search.core.spatial;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONObject;

import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.data.LatLon;
import net.osmand.util.Algorithms;

/**
 * The JSON of a spatial search unit test, read the same way by the tests and by the search-test server: phrases with
 * their own settings ("cafe {\"lat\": 50.1}"), the search location and the search settings.
 */
public final class SpatialSearchTestFile {

	public record Phrase(String query, JSONObject settings) {
	}

	private SpatialSearchTestFile() {
	}

	public static List<Phrase> parsePhrases(JSONObject test) {
		List<Phrase> phrases = new ArrayList<>();
		addPhrase(phrases, test.optString("phrase", null));
		JSONArray array = test.optJSONArray("phrases");
		for (int i = 0; array != null && i < array.length(); i++) {
			addPhrase(phrases, array.optString(i, null));
		}
		return phrases;
	}

	private static void addPhrase(List<Phrase> phrases, String phrase) {
		if (Algorithms.isEmpty(phrase)) {
			return;
		}
		int settingsStart = phrase.lastIndexOf('{');
		if (settingsStart < 0 || !phrase.trim().endsWith("}")) {
			phrases.add(new Phrase(phrase, null));
		} else {
			phrases.add(new Phrase(phrase.substring(0, settingsStart).trim(), new JSONObject(phrase.substring(settingsStart))));
		}
	}

	/** the test settings with the settings of one phrase on top */
	public static JSONObject merge(JSONObject settings, JSONObject phraseSettings) {
		JSONObject merged = new JSONObject(settings.toString());
		if (phraseSettings != null) {
			for (String key : phraseSettings.keySet()) {
				merged.put(key, phraseSettings.get(key));
			}
		}
		return merged;
	}

	public static LatLon parseLocation(JSONObject settings) {
		JSONObject location = settings.optJSONObject("location");
		if (location != null) {
			return new LatLon(location.getDouble("lat"), location.getDouble("lon"));
		}
		if (settings.has("lat") && settings.has("lon")) {
			return new LatLon(settings.getDouble("lat"), settings.getDouble("lon"));
		}
		return null;
	}

	/** search settings of the test: defaults with the ladder ranking the expected results were written with */
	public static SpatialTextSearch.SpatialTextSearchSettings parseSettings(JSONObject json) {
		SpatialTextSearch.SpatialTextSearchSettings settings = SpatialTextSearch.SpatialTextSearchSettings.defaultSettings();
		settings.SCORE_RANKING = false;
		settings.SEARCH_ADDR = json.optBoolean("SEARCH_ADDR", settings.SEARCH_ADDR);
		settings.SEARCH_POI = json.optBoolean("SEARCH_POI", settings.SEARCH_POI);
		settings.SEARCH_BUILDINGS = json.optBoolean("SEARCH_BUILDINGS", settings.SEARCH_BUILDINGS);
		settings.SEARCH_STREET_INTERSECTIONS = json.optBoolean("SEARCH_STREET_INTERSECTIONS", settings.SEARCH_STREET_INTERSECTIONS);
		settings.SEARCH_POI_INTERSECTIONS = json.optBoolean("SEARCH_POI_INTERSECTIONS", settings.SEARCH_POI_INTERSECTIONS);
		settings.SEARCH_POI_CATEGORIES = json.optBoolean("SEARCH_POI_CATEGORIES", settings.SEARCH_POI_CATEGORIES);
		settings.ALLOW_VIRTUAL_STREET_INTERSECTIONS = json.optBoolean("ALLOW_VIRTUAL_STREET_INTERSECTIONS",
				settings.ALLOW_VIRTUAL_STREET_INTERSECTIONS);
		settings.OPTIM_DELETE_EMBEDDED_BOUNDARIES = json.optBoolean("OPTIM_DELETE_EMBEDDED_BOUNDARIES",
				settings.OPTIM_DELETE_EMBEDDED_BOUNDARIES);
		settings.OPTIM_FLAG_POI_SAME_AS_CITY_STREET = json.optBoolean("OPTIM_FLAG_POI_SAME_AS_CITY_STREET",
				settings.OPTIM_FLAG_POI_SAME_AS_CITY_STREET);
		settings.DEDUPLICATE_RES = json.optBoolean("DEDUPLICATE_RES", settings.DEDUPLICATE_RES);
		settings.LIMIT_POI_CATEGORY_BY_FREQ = json.optInt("LIMIT_POI_CATEGORY_BY_FREQ", settings.LIMIT_POI_CATEGORY_BY_FREQ);
		settings.OPTIM_READ_COMMON_WORDS_LIMIT = json.optInt("OPTIM_READ_COMMON_WORDS_LIMIT", settings.OPTIM_READ_COMMON_WORDS_LIMIT);
		settings.LANG_DEDUPLICATE = json.optString("LANG_DEDUPLICATE", settings.LANG_DEDUPLICATE);
		settings.MIN_ELO_RATING = json.optInt("MIN_ELO_RATING", settings.MIN_ELO_RATING);
		settings.MIN_CHARACTERS_INCOMPLETE = json.optInt("MIN_CHARACTERS_INCOMPLETE", settings.MIN_CHARACTERS_INCOMPLETE);
		settings.LIMIT_ATOMIC_OBJECTS = json.optInt("LIMIT_ATOMIC_OBJECTS", settings.LIMIT_ATOMIC_OBJECTS);
		settings.LIMIT_STOP_GOALS_ANY_LEVEL_WHEN_REACHED_RES = json.optInt("LIMIT_ALL_GOALS_MAX_UNIQUE_OBJECTS",
				settings.LIMIT_STOP_GOALS_ANY_LEVEL_WHEN_REACHED_RES);
		settings.LIMIT_STOP_GOALS_LEVEL_1__WHEN_REACHED_RES = json.optInt("LIMIT_STOP_OTHER_GOALS_WHEN_REACHED_UNIQUE_OBJECTS",
				settings.LIMIT_STOP_GOALS_LEVEL_1__WHEN_REACHED_RES);
		settings.LIMIT_STOP_GOALS_LEVEL_1__WHEN_REACHED_RES = json.optInt("LIMIT_GOAL_LEVEL_2",
				settings.LIMIT_STOP_GOALS_LEVEL_1__WHEN_REACHED_RES);
		settings.DEV_USE_PIPELINE = json.optBoolean("DEV_USE_PIPELINE", settings.DEV_USE_PIPELINE);
		return settings;
	}

	public static BinaryMapIndexReader openReader(File obf) throws IOException {
		RandomAccessFile raf = new RandomAccessFile(obf, "r");
		try {
			return new BinaryMapIndexReader(raf, obf);
		} catch (IOException | RuntimeException e) {
			raf.close();
			throw e;
		}
	}
}
