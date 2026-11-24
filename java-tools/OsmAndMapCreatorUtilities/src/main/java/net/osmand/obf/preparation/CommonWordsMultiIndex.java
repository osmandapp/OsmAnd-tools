package net.osmand.obf.preparation;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.osmand.binary.NameIndexReader;
import net.osmand.search.rules.SearchModLocaleRules;
import net.osmand.util.SearchAlgorithms;

/**
 * Which words of a name become keys of the name index, decided by the statistics group of the map.
 * <p>
 * The group of a map comes from {@code <locales>} of the search rules ({@code SearchModLocales.groupForMap}); a group
 * holds the name statistics of its words:
 * <ul>
 * <li>class 1, service words ("rue", "de", "road", "вулиця"): dropped when the name keeps a word outside class 1;</li>
 * <li>class 2, frequent words ("chemin", "school"): dropped when the name has a word at least {@link #RARER_FACTOR}
 * times rarer;</li>
 * <li>every other word stays a key.</li>
 * </ul>
 * A name that has words always keeps one: the rarest word outside class 1 has nothing ten times rarer, and when no
 * word outside class 1 is left, the class 1 words stay. Numbers are returned as they are and take no part in the rules.
 * <p>
 * Words: {@code common_words_groups.tsv} next to this class, lines
 * {@code word <group> <class 0|1|2> <names per million> <word>}. A word of class 0 is only a frequency to compare with;
 * a word the file does not have counts as rare. {@code <class0>}, {@code <class1>}, {@code <class2>} of the rules
 * locale replace the class of the file.
 */
public class CommonWordsMultiIndex {

	public static final int RARER_FACTOR = 10;
	public static final String RESOURCE = "common_words_groups.tsv";

	private static final int KEEP = SearchModLocaleRules.CLASS_ALWAYS;
	private static final int SERVICE = SearchModLocaleRules.CLASS_SERVICE;
	private static final int FREQUENT = SearchModLocaleRules.CLASS_FREQUENT;

	private static CommonWordsMultiIndex instance;

	// group -> aligned word -> class, names per million
	private final Map<String, Map<String, int[]>> groups = new HashMap<>();

	public static CommonWordsMultiIndex getInstance() {
		if (instance == null) {
			try (InputStream is = CommonWordsMultiIndex.class.getResourceAsStream(RESOURCE)) {
				if (is == null) {
					throw new IllegalStateException(RESOURCE + " is not found next to " + CommonWordsMultiIndex.class.getName());
				}
				instance = load(is);
			} catch (IOException e) {
				throw new IllegalStateException(e);
			}
		}
		return instance;
	}

	public static CommonWordsMultiIndex load(InputStream is) throws IOException {
		CommonWordsMultiIndex index = new CommonWordsMultiIndex();
		BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
		String line;
		while ((line = reader.readLine()) != null) {
			String[] p = line.split("\t");
			if (p[0].equals("word") && p.length >= 5) {
				Map<String, int[]> words = index.groups.computeIfAbsent(p[1], k -> new HashMap<>());
				// the index keeps words aligned ("rû" is "ru"): spellings of one aligned word share its frequency,
				// and the class of the more frequent spelling
				String word = SearchAlgorithms.alignChars(p[4]);
				int[] v = new int[] { Integer.parseInt(p[2]), Integer.parseInt(p[3]) };
				int[] old = words.get(word);
				if (old != null) {
					v = new int[] { old[1] >= v[1] ? old[0] : v[0], old[1] + v[1] };
				}
				words.put(word, v);
			}
		}
		return index;
	}

	/**
	 * @param group statistics group of the map, null keeps every word
	 * @param rules rules of the locale of the map: their word classes replace the classes of the file, can be null
	 * @param words normalized words of one name (SearchAlgorithms.splitAndNormalize)
	 * @param notable an object people know by any word of its name (a travel rating, a wikipedia article): "national"
	 * has to find Tongass National Forest, so every word stays a key
	 * @return the words to index, in their order; the same list when the map has no group
	 */
	public List<String> getWordsToIndex(String group, SearchModLocaleRules rules, List<String> words, boolean notable) {
		if (group == null || notable || words.size() < 2) {
			return words;
		}
		Map<String, int[]> stats = groups.getOrDefault(group, Map.of());
		int size = words.size();
		int[] cls = new int[size];
		int[] freq = new int[size];
		boolean[] number = new boolean[size];
		for (int i = 0; i < size; i++) {
			String w = words.get(i);
			// "cityasstreetcommon" marks a street that is a place: a word of the index itself, never a word of the name
			number[i] = SearchAlgorithms.isNumber2Letters(w) || NameIndexReader.isIndexMarker(w);
			int[] v = number[i] ? null : stats.get(SearchAlgorithms.alignChars(w));
			Integer ruleClass = number[i] || rules == null ? null : rules.wordClass(w);
			cls[i] = ruleClass != null ? ruleClass : v == null ? KEEP : v[0];
			freq[i] = v == null ? 0 : v[1];
		}
		boolean[] drop = new boolean[size];
		boolean otherThanService = false;
		for (int i = 0; i < size; i++) {
			if (number[i] || cls[i] == SERVICE) {
				continue;
			}
			if (cls[i] == FREQUENT) {
				for (int j = 0; j < size; j++) {
					// strictly rarer: words of one frequency, zero included, never drop each other, so the rarest word stays
					if (j != i && !number[j] && freq[j] < freq[i] && (long) freq[j] * RARER_FACTOR <= freq[i]) {
						drop[i] = true;
						break;
					}
				}
			}
			otherThanService |= !drop[i];
		}
		List<String> result = new ArrayList<>(size);
		for (int i = 0; i < size; i++) {
			boolean dropService = cls[i] == SERVICE && !number[i] && otherThanService;
			if (!drop[i] && !dropService) {
				result.add(words.get(i));
			}
		}
		return result;
	}
}
