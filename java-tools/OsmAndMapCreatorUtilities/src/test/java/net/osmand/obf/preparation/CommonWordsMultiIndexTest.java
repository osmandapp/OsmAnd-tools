package net.osmand.obf.preparation;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import net.osmand.binary.NameIndexReader;
import net.osmand.search.rules.SearchModLocales;
import net.osmand.search.rules.SearchModRules;
import net.osmand.util.SearchAlgorithms;

public class CommonWordsMultiIndexTest {

	// names per million of a French-like group: rue, de, la are service words, chemin, école, gare frequent ones
	private static final String DATA = String.join("\n",
			"# test data",
			"word\tfr\t1\t130000\true",
			"word\tfr\t1\t219000\tde",
			"word\tfr\t1\t154000\tla",
			"word\tfr\t2\t3900\tchemin",
			"word\tfr\t2\t3000\técole",
			"word\tfr\t2\t1500\tgare",
			"word\tfr\t0\t300\tjules",
			"word\tnl\t1\t54000\tde",
			"");

	// one map of every language group
	private static final String[] GROUP_MAPS = { "France_europe_2.obf", "Us_texas_northamerica_2.obf",
			"Germany_bayern_europe_2.obf", "Netherlands_europe_2.obf", "Spain_europe_2.obf", "Brazil_southamerica_2.obf",
			"Italy_europe_2.obf", "Ukraine_europe_2.obf", "Poland_europe_2.obf", "Serbia_europe_2.obf",
			"Romania_europe_2.obf", "Hungary_europe_2.obf", "Greece_europe_2.obf", "Lithuania_europe_2.obf",
			"Sweden_europe_2.obf", "Finland_europe_2.obf", "Turkey_europe_2.obf", "Egypt_africa_2.obf",
			"Morocco_africa_2.obf", "Iran_asia_2.obf", "Israel_asia_2.obf", "Japan_asia_2.obf", "South-korea_asia_2.obf",
			"Vietnam_asia_2.obf", "Indonesia_asia_2.obf", "Thailand_asia_2.obf", "Laos_asia_2.obf",
			"Pakistan_asia_2.obf", "Georgia_asia_2.obf", "Ethiopia_africa_2.obf" };

	private CommonWordsMultiIndex index;

	@Before
	public void setUp() throws IOException {
		index = CommonWordsMultiIndex.load(new ByteArrayInputStream(DATA.getBytes(StandardCharsets.UTF_8)));
	}

	private List<String> keys(String group, String... words) {
		return index.getWordsToIndex(group, null, Arrays.asList(words), false);
	}

	@Test
	public void serviceWordsGoWhenAnotherWordStays() {
		Assert.assertEquals(List.of("paix"), keys("fr", "rue", "de", "la", "paix"));
	}

	@Test
	public void onlyServiceWordsStay() {
		Assert.assertEquals(List.of("rue", "de", "la"), keys("fr", "rue", "de", "la"));
	}

	@Test
	public void frequentWordGoesWhenTenTimesRarerWordExists() {
		// école 3000 >= jules 300 x 10
		Assert.assertEquals(List.of("jules", "ferry"), keys("fr", "école", "jules", "ferry"));
		// chemin 3900 < gare 1500 x 10: both frequent words stay, the service words go
		Assert.assertEquals(List.of("chemin", "gare"), keys("fr", "chemin", "de", "la", "gare"));
	}

	@Test
	public void numbersStayAndDoNotCount() {
		Assert.assertEquals(List.of("10", "paix"), keys("fr", "rue", "10", "de", "la", "paix"));
		Assert.assertEquals(List.of("rue", "10"), keys("fr", "rue", "10"));
	}

	@Test
	public void notableObjectKeepsEveryWord() {
		List<String> words = List.of("rue", "de", "la", "paix");
		Assert.assertEquals(words, index.getWordsToIndex("fr", null, words, true));
		Assert.assertEquals(List.of("paix"), index.getWordsToIndex("fr", null, words, false));
	}

	@Test
	public void wordsAreComparedAligned() throws IOException {
		// the data names the stream "ru" and "rû": one aligned word with both frequencies, frequent against "bas"
		String data = "word\tfr\t2\t2574\tbas\nword\tfr\t2\t1056\tru\nword\tfr\t0\t121\trû\nword\tfr\t1\t100000\tdu\n";
		CommonWordsMultiIndex aligned = CommonWordsMultiIndex.load(new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8)));
		Assert.assertEquals(List.of("bas", "rû"), aligned.getWordsToIndex("fr", null, List.of("bas", "du", "rû"), false));
	}

	@Test
	public void cityAsStreetMarkerIsNotAWord() {
		String marker = NameIndexReader.CITY_AS_STREET_COMMON;
		Assert.assertEquals(List.of("rue", "de", "la", marker), keys("fr", "rue", "de", "la", marker));
		Assert.assertEquals(List.of("paix", marker), keys("fr", "rue", "paix", marker));
	}

	@Test
	public void groupByLongestMapPrefix() {
		SearchModLocales locales = new SearchModRules().locales();
		Assert.assertEquals("fr", locales.groupForMap("Belgium_wallonia_europe_2.obf"));
		Assert.assertEquals("nl", locales.groupForMap("/maps/Belgium_flanders_europe_2.obf"));
		Assert.assertEquals("fr", locales.groupForMap("Luxembourg_europe"));
		Assert.assertNull(locales.groupForMap("Belgium_europe_2.obf"));
	}

	@Test
	public void mapWithoutGroupKeepsEveryWord() {
		List<String> words = List.of("rue", "de", "la", "paix");
		Assert.assertSame(words, index.getWordsToIndex(null, null, words, false));
	}

	@Test
	public void groupWithoutWordsKeepsEveryWord() {
		Assert.assertEquals(List.of("rue", "de", "la", "paix"), keys("it", "rue", "de", "la", "paix"));
	}

	@Test
	public void wordsOfOneFrequencyDoNotDropEachOther() throws IOException {
		// two frequent words of zero frequency: each would be ten times rarer than the other
		String data = "word\tfr\t2\t0\taaa\nword\tfr\t2\t0\tbbb\n";
		CommonWordsMultiIndex zero = CommonWordsMultiIndex.load(new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8)));
		Assert.assertEquals(List.of("aaa", "bbb"), zero.getWordsToIndex("fr", null, List.of("aaa", "bbb"), false));
	}

	@Test
	public void randomNamesOfRealWords() throws IOException {
		CommonWordsMultiIndex real = CommonWordsMultiIndex.getInstance();
		SearchModLocales locales = new SearchModRules().locales();
		Map<String, Map<String, int[]>> groups = readRealWords();
		Random random = new Random(5);
		for (String map : GROUP_MAPS) {
			String group = locales.groupForMap(map);
			Assert.assertNotNull(map, group);
			Map<String, int[]> words = groups.get(group);
			Assert.assertNotNull(group, words);
			List<List<String>> byClass = List.of(new ArrayList<>(), new ArrayList<>(), new ArrayList<>());
			for (Map.Entry<String, int[]> e : words.entrySet()) {
				byClass.get(e.getValue()[0]).add(e.getKey());
			}
			String[] extra = { "12", "3a", NameIndexReader.CITY_AS_STREET_COMMON, "qqzxw" };
			for (int n = 0; n < 2000; n++) {
				List<String> name = new ArrayList<>();
				int size = 1 + random.nextInt(6);
				for (int i = 0; i < size; i++) {
					int pick = random.nextInt(5);
					List<String> pool = pick < 3 ? byClass.get(pick) : null;
					name.add(pool == null || pool.isEmpty() ? extra[random.nextInt(extra.length)] : pool.get(random.nextInt(pool.size())));
				}
				List<String> keys = real.getWordsToIndex(group, null, name, false);
				Assert.assertEquals(map + " " + name, expectedKeys(name, words), keys);
				Assert.assertEquals(map + " " + name, name, real.getWordsToIndex(group, null, name, true));
			}
		}
	}

	/** the rules written out: numbers and the city marker stay, class 0 and unknown words stay, a class 2 word goes when
	 *  another word is at least ten times and strictly rarer, class 1 words go when any other word stays */
	private static List<String> expectedKeys(List<String> name, Map<String, int[]> words) {
		int size = name.size();
		boolean[] number = new boolean[size];
		int[] cls = new int[size];
		int[] freq = new int[size];
		for (int i = 0; i < size; i++) {
			String w = name.get(i);
			number[i] = SearchAlgorithms.isNumber2Letters(w) || NameIndexReader.CITY_AS_STREET_COMMON.equalsIgnoreCase(w);
			int[] v = number[i] ? null : words.get(SearchAlgorithms.alignChars(w));
			cls[i] = v == null ? 0 : v[0];
			freq[i] = v == null ? 0 : v[1];
		}
		if (size < 2) {
			return name;
		}
		boolean[] stays = new boolean[size];
		boolean otherStays = false;
		for (int i = 0; i < size; i++) {
			stays[i] = number[i] || cls[i] == 0;
			if (cls[i] == 2 && !number[i]) {
				stays[i] = true;
				for (int j = 0; j < size; j++) {
					if (j != i && !number[j] && freq[j] < freq[i] && freq[j] * 10L <= freq[i]) {
						stays[i] = false;
					}
				}
			}
			otherStays |= stays[i] && !number[i] && cls[i] != 1;
		}
		List<String> keys = new ArrayList<>();
		for (int i = 0; i < size; i++) {
			if (stays[i] || (cls[i] == 1 && !number[i] && !otherStays)) {
				keys.add(name.get(i));
			}
		}
		return keys;
	}

	// group -> aligned word -> class, names per million, merged like the index merges spellings
	private static Map<String, Map<String, int[]>> readRealWords() throws IOException {
		Map<String, Map<String, int[]>> groups = new HashMap<>();
		try (InputStream is = CommonWordsMultiIndex.class.getResourceAsStream(CommonWordsMultiIndex.RESOURCE)) {
			Assert.assertNotNull(CommonWordsMultiIndex.RESOURCE, is);
			BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
			String line;
			while ((line = reader.readLine()) != null) {
				String[] p = line.split("\t");
				if (p.length >= 5 && p[0].equals("word")) {
					Map<String, int[]> words = groups.computeIfAbsent(p[1], g -> new HashMap<>());
					String w = SearchAlgorithms.alignChars(p[4]);
					int[] v = { Integer.parseInt(p[2]), Integer.parseInt(p[3]) };
					int[] old = words.get(w);
					words.put(w, old == null ? v : new int[] { old[1] >= v[1] ? old[0] : v[0], old[1] + v[1] });
				}
			}
		}
		return groups;
	}
}
