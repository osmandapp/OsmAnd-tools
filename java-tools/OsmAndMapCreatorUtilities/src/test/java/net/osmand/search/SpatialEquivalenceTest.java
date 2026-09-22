package net.osmand.search;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runners.Parameterized;

import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.data.Amenity;
import net.osmand.data.Building;
import net.osmand.data.City;
import net.osmand.data.LatLon;
import net.osmand.data.MapObject;
import net.osmand.data.Street;
import net.osmand.search.core.SearchCoreFactory;
import net.osmand.search.core.spatial.SpatialSearchResult;
import net.osmand.search.core.spatial.test.SpatialSearchTestFile;
import net.osmand.search.core.spatial.test.SpatialTestSearchEngine;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;

/**
 * Equivalent query forms of the {@link SpatialSearchPipelineTest} unit tests: a checked phrase written another way
 * (word order, a meaningless word, an abbreviation, added context, a synonym) must not
 * rank its target lower: {@code variantRank <= canonicalRank + tolerance}.
 * <p>
 * Every unit test is checked with all kinds of {@link EquivalentForms#ALL_KINDS}. {@code "equivalenceKind"} at the top
 * of a test JSON or in its "settings" narrows them to a kind or several ({@code ["abbreviation", "noise"]} or
 * {@code "abbreviation, noise"}); {@code "equivalenceKind": ""} turns the check off. One phrase may have its own
 * ({@code "cafe {\"equivalenceKind\": \"context\"}"} or {@code "cafe {\"equivalenceKind\": \"\"}"}), which wins over
 * the one of the test. Besides it the test or the phrase takes:
 * <li>{@code "equivalenceTarget": 2} - the target is the 2nd expected row that is not a POI_TYPE (default 1);</li>
 * <li>{@code "equivalenceTolerance": 1} - how many positions the target of a form may lose (default 0).</li>
 * <p>
 * The target of a phrase is its expected row, its checked rank is the position of the row among the expected rows that
 * are not POI_TYPE. The phrase is searched again first: when the row is not at its rank the pipeline test of the phrase
 * is red and the phrase is skipped, a comparison with it proves nothing. Every phrase gets a search engine of its own,
 * the search context caches atoms by word between phrases.
 * <p>
 * All forms are run and all failures are reported; {@code EQUIVALENCE_STOP_ON_FAIL=true} (environment variable or system
 * property) stops on the first failing form and skips the tests after it.
 */
public class SpatialEquivalenceTest extends SpatialSearchPipelineTest {
	private static final String KIND_KEY = "equivalenceKind";
	private static final String TARGET_KEY = "equivalenceTarget";
	private static final String TOLERANCE_KEY = "equivalenceTolerance";
	private static final String STOP_ON_FAIL_ENV = "EQUIVALENCE_STOP_ON_FAIL";
	private static final int NOT_FOUND = Integer.MAX_VALUE;
	private static final double TARGET_RADIUS_M = 100;

	private static volatile String firstFailure;

	private final File testFile;
	private final String testName;

	public SpatialEquivalenceTest(String name, File file) {
		super(name, file);
		this.testFile = file;
		this.testName = name;
	}

	/** the unit tests of {@link SpatialSearchPipelineTest} but those whose equivalenceKind turns every phrase off */
	@Parameterized.Parameters(name = "{index}: {0}")
	public static Iterable<Object[]> data() {
		File[] files = new File(SEARCH_RESOURCES_PATH).listFiles();
		List<Object[]> arrayList = new ArrayList<>();
		if (files != null) {
			for (File file : files) {
				String fileName = file.getName();
				if (file.isFile() && fileName.endsWith(".json") && isChecked(new JSONObject(Algorithms.getFileAsString(file)))) {
					arrayList.add(new Object[] { fileName.substring(0, fileName.length() - ".json".length()), file });
				}
			}
		}
		arrayList.sort(Comparator.comparing(a -> ((String) a[0])));
		return arrayList;
	}

	/** a test is checked unless its equivalenceKind is empty and no phrase has kinds of its own */
	private static boolean isChecked(JSONObject test) {
		JSONObject settings = test.optJSONObject("settings");
		// the kinds of the settings win over the top of the test, as the phrases take them
		JSONObject owner = settings != null && settings.has(KIND_KEY) ? settings : test;
		if (kindsOf(owner).size() > 0) {
			return true;
		}
		for (SpatialSearchTestFile.Phrase phrase : SpatialSearchTestFile.parsePhrases(test)) {
			if (phrase.settings() != null && phrase.settings().has(KIND_KEY) && kindsOf(phrase.settings()).size() > 0) {
				return true;
			}
		}
		return false;
	}

	/** the kinds of equivalenceKind: all of them without the key, none for an empty one */
	private static Set<String> kindsOf(JSONObject settings) {
		return settings.has(KIND_KEY) ? EquivalentForms.parseKinds(settings.opt(KIND_KEY))
				: new LinkedHashSet<>(EquivalentForms.ALL_KINDS);
	}

	private static boolean isStopOnFail() {
		String value = getOption(STOP_ON_FAIL_ENV);
		return "true".equalsIgnoreCase(value) || "1".equals(value);
	}

	/** a main result of the engine with its row as the pipeline tests format it */
	private record FoundRow(String row, SpatialSearchResult result) {

		boolean isPoiType() {
			return result.isPoiCategory();
		}

		LatLon location() {
			return result.getLatLon();
		}
	}

	/**
	 * the object the canonical phrase found, the forms have to find the same one: the same map object id, or for an
	 * object without one (a house) the same kind and name within {@link #TARGET_RADIUS_M}
	 */
	private record Target(MapObject object, LatLon location) {

		boolean matches(FoundRow row) {
			MapObject o = row.result().getMainObject();
			if (o == null || o.getClass() != object.getClass()) {
				return false;
			}
			Long id = object.getId();
			if (id != null && id != 0 && o.getId() != null && o.getId() != 0) {
				return id.equals(o.getId());
			}
			LatLon l = row.location();
			return Algorithms.emptyIfNull(o.getName()).equalsIgnoreCase(Algorithms.emptyIfNull(object.getName()))
					&& (location == null || l == null || MapUtils.getDistance(location, l) <= TARGET_RADIUS_M);
		}
	}

	/** main results of the phrase, searched by an engine of its own with the settings of the phrase */
	private List<FoundRow> search(JSONObject phraseSettings, List<BinaryMapIndexReader> readers, String text)
			throws IOException {
		SpatialTestSearchEngine engine = createSearchEngine(SpatialSearchTestFile.parseSettings(phraseSettings),
				SpatialSearchTestFile.parseLocation(phraseSettings), readers, phraseSettings.optBoolean("translation"));
		List<FoundRow> rows = new ArrayList<>();
		for (SpatialSearchResult res : engine.searchResults(text, false)) {
			rows.add(new FoundRow(engine.formatResult(res), res));
		}
		return rows;
	}

	private static int rankOf(List<FoundRow> rows, Target target) {
		int rank = 0;
		for (FoundRow row : rows) {
			if (row.isPoiType()) {
				continue;
			}
			rank++;
			if (target.matches(row)) {
				return rank;
			}
		}
		return NOT_FOUND;
	}

	/**
	 * Parents of a result for the context forms, from its objects only, the way the detector and the row formatter
	 * take them: the city of its street (a street, the street of a house, the streets of an intersection), the city
	 * tags of a POI, the closest city of a district; the district is the one the name of its street has in brackets
	 * ("East 57th Street (Manhattan)"). The query has a place of the target when the result has a city of its own.
	 */
	private static EquivalentForms.Context contextOf(SpatialSearchResult res) {
		MapObject main = res.getMainObject();
		if (main == null) {
			return EquivalentForms.Context.NONE;
		}
		List<Street> streets = new ArrayList<>();
		if (main instanceof Street street) {
			streets.add(street);
		}
		boolean hasPlace = false;
		for (MapObject o : res.getObjects()) {
			if (o instanceof Street street && !streets.contains(street)) {
				streets.add(street);
			}
			hasPlace |= o instanceof City && o != main;
		}
		String city = null;
		String boundary = null;
		for (Street street : streets) {
			if (city == null && street.getCity() != null) {
				city = street.getCity().getName();
			}
			String name = street.getName();
			int open = name == null ? -1 : name.lastIndexOf(" (");
			if (boundary == null && open > 0 && name.endsWith(")")) {
				boundary = name.substring(open + 2, name.length() - 1);
			}
		}
		if (city == null && main instanceof Amenity amenity) {
			city = amenity.getCityFromTagGroups("en");
		} else if (city == null && main instanceof City place && place.getClosestCity() != null
				&& place.getClosestCity() != place) {
			city = place.getClosestCity().getName();
		}
		return new EquivalentForms.Context(Algorithms.isEmpty(city) ? null : city, boundary, hasPlace);
	}

	private static String rankStr(int rank) {
		return rank == NOT_FOUND ? "not found" : "#" + rank;
	}

	@Override
	@Test
	public void testSearch() throws IOException, JSONException, SQLException {
		boolean stopOnFail = isStopOnFail();
		Assume.assumeTrue("Stopped on the first failure: " + firstFailure, !stopOnFail || firstFailure == null);

		JSONObject sourceJson = new JSONObject(Algorithms.getFileAsString(testFile));
		JSONObject settingsJson = sourceJson.getJSONObject("settings");
		String skip = sourceJson.optBoolean("ignore") || settingsJson.optBoolean("ignore") ? "ignored"
				: settingsJson.optBoolean("disabled") ? "disabled"
				: !settingsJson.optBoolean("useData", true) ? "no data"
				: settingsJson.optBoolean("world") ? "world maps"
				: settingsJson.optBoolean("translation") ? "test translations" : null;
		if (skip != null) {
			return;
		}
		phrasesLang = settingsJson.optString("phrasesLang", null);
		// the equivalence keys of the test are defaults of its phrases
		JSONObject testSettings = new JSONObject(settingsJson.toString());
		for (String key : new String[] { KIND_KEY, TARGET_KEY, TOLERANCE_KEY }) {
			if (sourceJson.has(key) && !testSettings.has(key)) {
				testSettings.put(key, sourceJson.get(key));
			}
		}
		List<SpatialSearchTestFile.Phrase> phrases = SpatialSearchTestFile.parsePhrases(sourceJson);
		List<List<String>> results = parseExpectedResults(sourceJson, "results", phrases.size());
		EquivalentForms forms = new EquivalentForms(defaultPoiTranslator);

		List<String> failures = new ArrayList<>();
		List<BinaryMapIndexReader> readers = new ArrayList<>();
		boolean prevDisplayDefaultPoiTypes = SearchCoreFactory.DISPLAY_DEFAULT_POI_TYPES;
		try {
			loadReaders(sourceJson, SpatialSearchTestFile.parseLocation(settingsJson), readers);
			Assert.assertFalse("No OBF indexes were loaded for " + testFile.getName(), readers.isEmpty());
			// the noise word must find nothing on these maps, or it is not noise
			boolean noiseAllowed = search(testSettings, readers, EquivalentForms.NOISE_WORD).isEmpty();
			phrases:
			for (int k = 0; k < phrases.size(); k++) {
				SpatialSearchTestFile.Phrase phrase = phrases.get(k);
				JSONObject phraseSettings = SpatialSearchTestFile.merge(testSettings, phrase.settings());
				Set<String> kinds = kindsOf(phraseSettings);
				if (kinds.isEmpty() || phraseSettings.optBoolean("ignore") || results.get(k).isEmpty()) {
					continue;
				}
				int tolerance = phraseSettings.optInt(TOLERANCE_KEY, 0);
				String query = phrase.query();

				int targetNumber = phraseSettings.optInt(TARGET_KEY, 1);
				SpatialResultRow expected = null;
				int canonicalRank = 0;
				for (String row : results.get(k)) {
					SpatialResultRow parsed = SpatialResultRow.parse(row);
					if (parsed != null && !parsed.isPoiType() && ++canonicalRank == targetNumber) {
						expected = parsed;
						break;
					}
				}
				if (expected == null) {
					continue;
				}
				FoundRow live = null;
				int liveRank = 0;
				for (FoundRow row : search(phraseSettings, readers, query)) {
					if (!row.isPoiType() && ++liveRank == canonicalRank) {
						live = row;
						break;
					}
				}
				SpatialResultRow liveRow = live == null ? null : SpatialResultRow.parse(live.row());
				if (liveRow == null || !liveRow.getName().equals(expected.getName())
						|| live.result().getMainObject() == null) {
					// the pipeline test of the phrase is red: a comparison with it proves nothing
					continue;
				}
				Target target = new Target(live.result().getMainObject(), live.location());
				EquivalentForms.Context context = kinds.contains(EquivalentForms.CONTEXT) ? contextOf(live.result()) : EquivalentForms.Context.NONE;
				for (EquivalentForms.Variant v : forms.generate(query, liveRow, kinds, context)) {
					if (v.kind().equals(EquivalentForms.NOISE) && !noiseAllowed) {
						continue;
					}
					List<FoundRow> rows = search(phraseSettings, readers, v.phrase());
					int rank = rankOf(rows, target);
					boolean passed = rank != NOT_FOUND && rank <= canonicalRank + tolerance;
					// #k is the number of the phrase in the test
					String line = String.format(Locale.US, "#%d for '%s' -> '%s' (%s): #%d -> %s", k + 1,
							query, v.phrase(), v.rule(), canonicalRank, rankStr(rank));
					if (!passed) {
						System.out.printf("FAIL %s%n", line);
						failures.add(line);
						if (stopOnFail) {
							break phrases;
						}
					}
				}
			}
		} finally {
			SearchCoreFactory.DISPLAY_DEFAULT_POI_TYPES = prevDisplayDefaultPoiTypes;
			for (BinaryMapIndexReader reader : readers) {
				reader.close();
			}
		}
		if (!failures.isEmpty()) {
			if (stopOnFail) {
				firstFailure = testName + ": " + failures.get(0);
			}
			// the forms are the FAIL lines of the output
			Assert.fail(testFile.getName() + ", " + failures.size() + " forms rank the target lower");
		}
	}

	/**
	 * A result row of the spatial search unit tests as {@code SpatialResultFormatter} writes it:
	 * <pre>
	 * 4, 8th Avenue, Passaic [[4, HOUSE, t4+0-w2-oth0-tp3, 17.20 km, 40.8612, -74.1400 4 ['8 4 ave' [Building] '2 8th Avenue'
	 *     -26240855042 20071 (40.8612 -74.1400), 'paterson' [CITY_TOWN_TYPE] 'Paterson' 158855679 53893 (40.9168 -74.1718)]]]
	 * </pre>
	 * The name, the type and the distance identify the object; the groups tell which query words each object of the
	 * combination took (the role of the words), which is what the equivalent query forms are built from. Stored rows
	 * may be cut ({@code "... [[4, HOUSE,, 3.46 km]]"}): they have no groups.
	 */
	public static final class SpatialResultRow {

		/** words of the query one object of the combination took: {@code 'lee's summit' [CITY_TOWN_TYPE] 'Lee's Summit'} */
		public record Group(List<String> words, String type, String objectName, LatLon location) {

			public boolean isPoiCategory() {
				return type.startsWith("POI_TYPE");
			}

			public boolean isPoi() {
				return type.startsWith("POI ") || type.equals("POI");
			}

			public boolean isStreet() {
				return type.equals("STREET_TYPE");
			}

			public boolean isBuilding() {
				return type.equals("Building");
			}

			/** a city, a town, a village, a boundary or a postcode: the context of the address */
			public boolean isPlace() {
				return type.equals("CITY_TOWN_TYPE") || type.equals("VILLAGES_TYPE") || type.equals("BOUNDARY_TYPE")
						|| type.equals("POSTCODES_TYPE");
			}
		}

		// a group starts the list or follows the previous one; words and names may have apostrophes ('lee's summit'),
		// the object name ends before its id ("'Dunkin'' 5854108697") or the category id ("'fast_food' id=1026")
		private static final Pattern GROUP = Pattern.compile(
	            "(?:\\[|, )'(.*?)' \\[([^]]*)] '(.*?)'(?= -?\\d| id=)([^,\\]]*)");
		private static final Pattern POINT = Pattern.compile("\\((-?\\d+\\.\\d+) (-?\\d+\\.\\d+)\\)");

		private final String row;
		private final String name;
		private final int tokenCount;
		private final String type;
		private final double distanceKm;
		private final List<Group> groups;

		private SpatialResultRow(String row, String name, int tokenCount, String type, double distanceKm, List<Group> groups) {
			this.row = row;
			this.name = name;
			this.tokenCount = tokenCount;
			this.type = type;
			this.distanceKm = distanceKm;
			this.groups = groups;
		}

		/** @return the parsed row, or null for a row without {@code " [["} (reverse geocoding lines, notes) */
		public static SpatialResultRow parse(String row) {
			if (row == null) {
				return null;
			}
			String text = row.startsWith("@") ? row.substring(1) : row;
			int start = text.indexOf(" [[");
			if (start == -1) {
				return null;
			}
			String name = text.substring(0, start);
			String inner = text.substring(start + 3);
			String[] head = inner.split(", ", 5);
			int tokenCount = parseInt(head.length > 0 ? head[0] : "");
			String type = head.length > 1 ? head[1].replace(",", "").trim() : "";
			double distance = -1;
			for (int i = 2; i < head.length; i++) {
				int km = head[i].indexOf(" km");
				if (km > 0) {
					distance = parseDouble(head[i].substring(head[i].lastIndexOf(',', km) + 1, km));
					break;
				}
			}
			return new SpatialResultRow(text, name, tokenCount, type, distance, parseGroups(inner));
		}

		private static List<Group> parseGroups(String inner) {
			List<Group> groups = new ArrayList<>();
			int list = inner.indexOf("['");
			if (list == -1) {
				return groups;
			}
			Matcher m = GROUP.matcher(inner);
			m.region(list, inner.length());
			while (m.find()) {
				List<String> words = new ArrayList<>();
				for (String w : m.group(1).trim().split("\\s+")) {
					if (!w.isEmpty()) {
						words.add(w);
					}
				}
				Matcher p = POINT.matcher(m.group(4));
				LatLon location = p.find() ? new LatLon(parseDouble(p.group(1)), parseDouble(p.group(2))) : null;
				groups.add(new Group(Collections.unmodifiableList(words), m.group(2).trim(), m.group(3), location));
			}
			return groups;
		}

		private static int parseInt(String s) {
			try {
				return Integer.parseInt(s.trim());
			} catch (NumberFormatException e) {
				return -1;
			}
		}

		private static double parseDouble(String s) {
			try {
				return Double.parseDouble(s.trim());
			} catch (NumberFormatException e) {
				return -1;
			}
		}

		public String getRow() {
			return row;
		}

		/** display name: "4, 8th Avenue, Passaic" */
		public String getName() {
			return name;
		}

		/** the first part of the display name, the object itself: "Southwest Summit Valley Lane" of "..., Lee's Summit" */
		public String getObjectName() {
			int ind = name.indexOf(", ");
			if (ind == -1 || type.equals("HOUSE")) {
				// a house is "number, street, city"
				int second = ind == -1 ? -1 : name.indexOf(", ", ind + 2);
				return second == -1 ? name : name.substring(0, second);
			}
			return name.substring(0, ind);
		}

		/** the last part of the display name when it has one: the city of a street or a house, the matched city of a POI */
		public String getCityName() {
			int ind = name.lastIndexOf(", ");
			String object = getObjectName();
			return ind == -1 || ind < object.length() ? null : name.substring(ind + 2);
		}

		public int getTokenCount() {
			return tokenCount;
		}

		/** HOUSE, STREET, STREET_INTERSECTION, CITY, POI_TYPE, "POI fast_food" */
		public String getType() {
			return type;
		}

		public boolean isPoiType() {
			return type.startsWith("POI_TYPE");
		}

		/** km from the search point, -1 when the row has none */
		public double getDistanceKm() {
			return distanceKm;
		}

		public List<Group> getGroups() {
			return groups;
		}

		@Override
		public String toString() {
			return String.format(Locale.US, "%s [%s] %.2f km %s", name, type, distanceKm, groups);
		}
	}
}
