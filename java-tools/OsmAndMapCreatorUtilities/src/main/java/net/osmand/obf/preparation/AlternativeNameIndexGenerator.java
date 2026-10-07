package net.osmand.obf.preparation;

import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import net.osmand.binary.CommonWordsMultiIndex;
import net.osmand.binary.SearchLocales;
import net.osmand.binary.SearchVariantRules;
import net.osmand.binary.SearchVariantRules.Rule;
import net.osmand.binary.SearchVariantRules.RuleId;
import net.osmand.util.Algorithms;

/**
 * Alternative spellings of a name that go to the name index next to the name itself, so the search finds an object
 * by a word its name does not have as a separate word: "L'abordage" is found by "abordage", "Strada Statale 42" by
 * "SS42".
 * <p>
 * The alternative names come from {@code <index>} of the rules of the locale of the name ({@code rules.xml} and its
 * locale overlays in OsmAnd-java resources): {@code <unglue>} splits glued words, a {@code <rule>} replaces a phrase or
 * a glued form. Which of their words become keys is decided with the words of the name ({@link NameIndexPlan}).
 * The index size has to be measured per rule (file, object, from): every alternative word is one more key or name of
 * the object.
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
		// alternative names and keys by rule (RuleId: "rules_it.xml street (?iu)\bStrada\s+Statale\s+(\d+)\b")
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

		// "rules.xml unglue .: alternatives=.. keys=.. block=.. atom=.. join=.. dup=..; rules_de.xml street ...: ..."
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

	/**
	 * The alternative names of a name from {@code <index>} of the rules of the locale of the name. The name can carry
	 * the marker of an alternative name (NameIndexReader.altNameMarker): the marker is a word of the name, so the
	 * alternative words refer to it and stay with that variant.
	 *
	 * @param owner owner of the name for the rules (street, locality, boundary, postcode, poi), null for none
	 */
	List<NameIndexPlan.Alternative> alternatives(String name, String lang, String owner) {
		if (Algorithms.isEmpty(name)) {
			return List.of();
		}
		// name and alt_name are in the language of the map, name:de in German in the country of the map
		SearchVariantRules rules = SearchVariantRules.forLocale(SearchLocales.forName(lang, mapLocale));
		List<NameIndexPlan.Alternative> alternatives = new ArrayList<>();
		for (SearchVariantRules.Unglued unglued : rules.unglue(name)) {
			alternatives.add(new NameIndexPlan.Alternative(unglued.id(), unglued.name(), false));
		}
		if (owner != null) {
			for (Rule rule : rules.index()) {
				if (!rule.appliesTo(owner)) {
					continue;
				}
				String alternative = rule.apply(name);
				if (alternative != null) {
					alternatives.add(new NameIndexPlan.Alternative(rule.id(), alternative, rule.alwaysKeys()));
				}
			}
		}
		return alternatives;
	}

	/** Counts the alternative names of one name: the statistics of their rules. */
	void countAlternatives(List<NameIndexPlan.Variant> alternatives) {
		if (!alternatives.isEmpty()) {
			stats.names++;
		}
		stats.alternatives += alternatives.size();
		for (NameIndexPlan.Variant v : alternatives) {
			ruleStats(v.rule()).alternatives++;
		}
	}

	KeyStats ruleStats(RuleId rule) {
		return stats.byRule.computeIfAbsent(rule.toString(), r -> new KeyStats());
	}

	// a decision on a word of a name
	void decide(String word, KeyDecision decision, String name) {
		stats.decide(word, decision, name, wordClass(word));
	}

	// a decision on a word of an alternative name of a rule
	void decide(KeyStats ruleStats, String word, KeyDecision decision, String alternative) {
		ruleStats.decisions[decision.ordinal()]++;
		stats.decide(word, decision, alternative, wordClass(word));
	}

	// where a key of an alternative name of a rule went
	void outcome(KeyStats ruleStats, KeyOutcome outcome) {
		stats.outcomes[outcome.ordinal()]++;
		ruleStats.outcomes[outcome.ordinal()]++;
	}
}
