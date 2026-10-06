package net.osmand.obf.preparation;

import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import net.osmand.binary.CommonWordsMultiIndex;
import net.osmand.binary.NameIndexReader;
import net.osmand.binary.SearchLocales;
import net.osmand.binary.SearchVariantRules;
import net.osmand.binary.SearchVariantRules.Rule;
import net.osmand.data.City;
import net.osmand.data.Street;
import net.osmand.obf.preparation.NameIndexCreator.NameKeys;
import net.osmand.obf.preparation.NameIndexCreator.PoiNameObject;
import net.osmand.util.Algorithms;
import net.osmand.util.SearchAlgorithms;

/**
 * Alternative spellings of a name that go to the name index next to the name itself, so the search finds an object
 * by a word its name does not have as a separate word: "L'abordage" is found by "abordage", "Strada Statale 42" by
 * "SS42".
 * <p>
 * The alternative names come from {@code <index>} of the rules of the locale of the name ({@code rules.xml} and its
 * locale overlays in OsmAnd-java resources): {@code <unglue>} splits glued words, a {@code <rule>} replaces a phrase or
 * a glued form. A word of an alternative name is a key by its own class, whatever happened to the words it replaces
 * (rules-spec.md, 3.3): a word the name has keeps the decision of the name ({@link KeyDecision#ALT_SHADOWED} when the
 * name does not index it); a new word is a key ({@link KeyDecision#ALT}) when the statistics keep it among the words
 * of the alternative name, else it is attached to the keys of the name that the alternative name shares
 * ({@link KeyDecision#ALT_ATTACHED}); {@code keys="always"} and notable objects make every new word a key.
 * The index size has to be measured per rule: every alternative word is one more key or name of the object.
 */
public class AlternativeNameIndexGenerator<T> {

	/** Where an alternative key went in the name index, from the most to the least expensive. */
	public enum KeyOutcome {
		// a new prefix block
		BLOCK,
		// a new atom (object) in an existing prefix block
		ATOM,
		// one more name of the atom the object already has in the block: only its suffixes are stored
		JOIN,
		// the object already has this key with the same words: nothing is stored
		DUP
	}

	/** Alternative names, their keys by outcome and the decisions of words, per rule or per name index. */
	public static class KeyStats {
		// alternative names produced
		int alternatives;
		final int[] outcomes = new int[KeyOutcome.values().length];
		final int[] decisions = new int[KeyDecision.values().length];

		void add(KeyStats other) {
			alternatives += other.alternatives;
			for (int i = 0; i < outcomes.length; i++) {
				outcomes[i] += other.outcomes[i];
			}
			for (int i = 0; i < decisions.length; i++) {
				decisions[i] += other.decisions[i];
			}
		}

		// keys stored in the name index (every outcome but DUP)
		int keys() {
			return outcomes[KeyOutcome.BLOCK.ordinal()] + outcomes[KeyOutcome.ATOM.ordinal()]
					+ outcomes[KeyOutcome.JOIN.ordinal()];
		}

		public int decisions(KeyDecision decision) {
			return decisions[decision.ordinal()];
		}

		// "kept=.. notable=.. dropped_class1=.." for the decisions that happened
		String decisionsString() {
			StringBuilder s = new StringBuilder();
			for (KeyDecision d : KeyDecision.values()) {
				if (decisions[d.ordinal()] > 0) {
					s.append(s.length() == 0 ? "" : " ").append(d.name().toLowerCase()).append('=')
							.append(decisions[d.ordinal()]);
				}
			}
			return s.toString();
		}

		@Override
		public String toString() {
			StringBuilder s = new StringBuilder("alternatives=").append(alternatives).append(" keys=").append(keys());
			for (KeyOutcome o : KeyOutcome.values()) {
				s.append(' ').append(o.name().toLowerCase()).append('=').append(outcomes[o.ordinal()]);
			}
			String d = decisionsString();
			return d.isEmpty() ? s.toString() : s.append(' ').append(d).toString();
		}
	}

	/** One word of keys_report.tsv: its decisions and an example name. */
	static class WordReport {
		final String wordClass;
		final int[] decisions = new int[KeyDecision.values().length];
		final String example;

		WordReport(String wordClass, String example) {
			this.wordClass = wordClass;
			this.example = example;
		}
	}

	/** Keys of one name index: decisions of the words of names and the size of the alternative names. */
	public static class Stats extends KeyStats {
		// names that got at least one alternative name
		int names;
		// alternative names and keys by rule ("unglue", "street SS$1")
		final Map<String, KeyStats> byRule = new TreeMap<>();
		// keys_report.tsv: words with a class of the statistics or the rules, or with a decision other than a key by
		// the statistics; null when the report is off
		Map<String, WordReport> words;

		public void add(Stats other) {
			names += other.names;
			super.add(other);
			other.byRule.forEach((rule, s) -> byRule.computeIfAbsent(rule, r -> new KeyStats()).add(s));
			if (other.words != null) {
				if (words == null) {
					words = new TreeMap<>();
				}
				other.words.forEach((word, r) -> {
					WordReport report = words.computeIfAbsent(word, w -> new WordReport(r.wordClass, r.example));
					for (int i = 0; i < report.decisions.length; i++) {
						report.decisions[i] += r.decisions[i];
					}
				});
			}
		}

		/** @param wordClass class and source of the word ("1 tsv", "0 rules"), null when it has none */
		void decide(String word, KeyDecision decision, String name, String wordClass) {
			decisions[decision.ordinal()]++;
			if (words != null && (wordClass != null || (decision != KeyDecision.KEPT && decision != KeyDecision.NOTABLE
					&& decision != KeyDecision.NUMBER))) {
				words.computeIfAbsent(word, w -> new WordReport(wordClass, name)).decisions[decision.ordinal()]++;
			}
		}

		// "unglue: alternatives=.. keys=.. block=.. atom=.. join=.. dup=..; street $1str: ..."
		public String byRuleString() {
			StringBuilder s = new StringBuilder();
			byRule.forEach((rule, stats) -> s.append(s.length() == 0 ? "" : "; ").append(rule).append(": ").append(stats));
			return s.toString();
		}

		/** Rows {@code index word class source decision count example} of keys_report.tsv. */
		public void writeKeysReport(String index, Writer out) throws IOException {
			if (words == null) {
				return;
			}
			for (Map.Entry<String, WordReport> e : words.entrySet()) {
				WordReport r = e.getValue();
				String[] cls = r.wordClass == null ? new String[] { "", "" } : r.wordClass.split(" ");
				for (KeyDecision d : KeyDecision.values()) {
					int count = r.decisions[d.ordinal()];
					if (count > 0) {
						out.write(index + "\t" + e.getKey() + "\t" + cls[0] + "\t" + cls[1] + "\t" + d.name() + "\t"
								+ count + "\t" + r.example.replace('\t', ' ') + "\n");
					}
				}
			}
		}

		@Override
		public String toString() {
			return "names=" + names + " " + super.toString();
		}
	}

	private final NameIndexCreator<T> nameIndex;
	private final Stats stats = new Stats();
	// statistics group of the map (<locales> of rules.xml), null when no group covers it
	private String languageGroup;
	private String mapName;
	// rules locale of the map (SearchLocales.forMap: "en_US", "it_IT"), "" when no locale covers it
	private String mapLocale = "";

	public AlternativeNameIndexGenerator(NameIndexCreator<T> nameIndex) {
		this.nameIndex = nameIndex;
	}

	public String getLanguageGroup() {
		return languageGroup;
	}

	public String getMapName() {
		return mapName;
	}

	void setLanguageGroup(String languageGroup, String mapName) {
		this.languageGroup = languageGroup;
		this.mapName = mapName;
		this.mapLocale = SearchLocales.forMap(mapName);
	}

	public String getMapLocale() {
		return mapLocale;
	}

	public Stats getStats() {
		return stats;
	}

	/** Collects the words of keys_report.tsv from now on. */
	void enableKeysReport() {
		if (stats.words == null) {
			stats.words = new TreeMap<>();
		}
	}

	// class and source of a word for keys_report.tsv, null when the report is off or the word has no class
	String wordClass(String word) {
		String keysMap = nameIndex.getKeysMapName();
		return stats.words == null || keysMap == null ? null
				: CommonWordsMultiIndex.getInstance().wordClass(keysMap, word);
	}

	// name can carry the marker of an alternative name (NameIndexReader.altNameMarker): the marker is a word of the name,
	// so the alternative words refer to it and stay with that variant
	public void addAlternativeNames(String name, String lang, T obj, int maxPrefixLength) {
		if (Algorithms.isEmpty(name)) {
			return;
		}
		// name and alt_name are in the language of the map, name:de in German in the country of the map
		SearchVariantRules rules = SearchVariantRules.forLocale(SearchLocales.forName(lang, mapLocale));
		NameKeys main = nameIndex.getNameKeys(obj, name);
		int alternatives = stats.alternatives;
		String unglued = rules.unglue(name);
		if (unglued != null) {
			addAlternativeName(main, name, unglued, obj, maxPrefixLength, "unglue", false);
		}
		String owner = ownerType(obj);
		if (owner != null) {
			for (Rule rule : rules.index()) {
				if (!rule.appliesTo(owner)) {
					continue;
				}
				String alternative = rule.apply(name);
				if (alternative != null) {
					addAlternativeName(main, name, alternative, obj, maxPrefixLength, owner + " " + rule.to().trim(),
							rule.alwaysKeys());
				}
			}
		}
		if (stats.alternatives > alternatives) {
			stats.names++;
		}
	}

	private String ownerType(T obj) {
		if (obj instanceof Street) {
			return "street";
		}
		if (obj instanceof City city) {
			return switch (city.getType()) {
				case BOUNDARY -> "boundary";
				case POSTCODE -> "postcode";
				default -> "locality";
			};
		}
		return obj instanceof PoiNameObject ? "poi" : null;
	}

	private void addAlternativeName(NameKeys main, String name, String alternative, T obj, int maxPrefixLength,
			String rule, boolean alwaysKeys) {
		List<String> alternativeWords = SearchAlgorithms.splitAndNormalize(alternative, false);
		// a name the writer did not see (a test): its words are keys
		List<String> nameWords = main != null ? main.words() : SearchAlgorithms.splitAndNormalize(name, false);
		boolean notable = main != null && main.notable();
		KeyStats ruleStats = stats.byRule.computeIfAbsent(rule, r -> new KeyStats());
		stats.alternatives++;
		ruleStats.alternatives++;
		List<String> unique = new ArrayList<>(new LinkedHashSet<>(alternativeWords));
		String keysMap = nameIndex.getKeysMapName();
		// the class of a new word is decided among the words of the alternative name, as for a name
		CommonWordsMultiIndex.KeyOutcome[] outcomes = alwaysKeys || notable || keysMap == null ? null
				: CommonWordsMultiIndex.getInstance().selectKeys(keysMap, unique, false);
		List<String> attached = new ArrayList<>();
		for (int i = 0; i < unique.size(); i++) {
			String word = unique.get(i);
			String prefix = NameIndexCreator.nameIndexPreparePrefix(word, maxPrefixLength);
			if (Algorithms.isEmpty(prefix) || NameIndexReader.isIndexMarker(word)) {
				continue;
			}
			if (nameWords.contains(word)) {
				if (main != null && !main.keys().contains(word)) {
					decide(ruleStats, word, KeyDecision.ALT_SHADOWED, alternative);
				}
				continue;
			}
			KeyDecision decision;
			if (alwaysKeys) {
				decision = KeyDecision.ALWAYS;
			} else if (notable) {
				decision = KeyDecision.NOTABLE;
			} else if (outcomes == null || outcomes[i].key) {
				decision = KeyDecision.ALT;
			} else {
				attached.add(word);
				continue;
			}
			decide(ruleStats, word, decision, alternative);
			addKey(ruleStats, prefix, obj, word, alternativeWords);
		}
		if (!attached.isEmpty()) {
			// the dropped new words stay words of the alternative name under the keys of the name it shares
			List<String> shared = new ArrayList<>();
			if (main != null) {
				for (String key : unique) {
					if (main.keys().contains(key)) {
						shared.add(key);
					}
				}
			}
			for (String word : attached) {
				// no shared key to attach to: the word is a key, else the alternative name could not be found
				decide(ruleStats, word, shared.isEmpty() ? KeyDecision.ALT : KeyDecision.ALT_ATTACHED, alternative);
				if (shared.isEmpty()) {
					addKey(ruleStats, NameIndexCreator.nameIndexPreparePrefix(word, maxPrefixLength), obj, word,
							alternativeWords);
				}
			}
			for (String key : shared) {
				addKey(ruleStats, NameIndexCreator.nameIndexPreparePrefix(key, maxPrefixLength), obj, key,
						alternativeWords);
			}
		}
	}

	// a decision on a word of a name of the writer (NameIndexCreator.addToNameIndex)
	void decide(String word, KeyDecision decision, String name) {
		stats.decide(word, decision, name, wordClass(word));
	}

	private void decide(KeyStats ruleStats, String word, KeyDecision decision, String alternative) {
		ruleStats.decisions[decision.ordinal()]++;
		stats.decide(word, decision, alternative, wordClass(word));
	}

	private void addKey(KeyStats ruleStats, String prefix, T obj, String word, List<String> alternativeWords) {
		int outcome = nameIndex.addAlternativeToken(prefix, obj, word, alternativeWords).ordinal();
		stats.outcomes[outcome]++;
		ruleStats.outcomes[outcome]++;
	}
}
