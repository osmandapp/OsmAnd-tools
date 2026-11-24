package net.osmand.obf.preparation;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import net.osmand.binary.NameIndexReader;
import net.osmand.search.rules.SearchModLocaleRules;
import net.osmand.search.rules.SearchModLocaleRules.Rule;
import net.osmand.search.rules.SearchModLocaleRules.Unglued;
import net.osmand.search.rules.SearchModRules;
import net.osmand.search.rules.SearchModRules.SearchModRuleOwner;
import net.osmand.util.Algorithms;
import net.osmand.util.SearchAlgorithms;

/**
 * Alternative spellings of a name that go to the name index next to the name itself, so the search finds an object
 * by a word its name does not have as a separate word: "L'abordage" is found by "abordage", "Hauptstraße" by
 * "Hauptstr", "Strada Statale 42" by "SS42".
 * <p>
 * The alternative names come from {@code <index>} of the search rules: {@code <unglue>} of the locale of the map, and
 * every {@code <rule>} of the locale of the name for the owner of the name. The words of the alternative name that the
 * name does not already have become keys of the object and refer to the other words of the alternative name, the same
 * way {@link NameIndexCreator#addToNameIndex} indexes a name.
 */
public class AlternativeNameIndexGenerator<T> {

	private static final Log log = LogFactory.getLog(AlternativeNameIndexGenerator.class);

	private final NameIndexCreator<T> nameIndex;
	// null gives no alternative names
	private SearchModRules searchRules;
	private String mapLocale = "";
	// rule -> alternative names, new keys, words attached to the keys of the name
	private final Map<String, int[]> ruleStats = new TreeMap<>();

	public AlternativeNameIndexGenerator(NameIndexCreator<T> nameIndex) {
		this.nameIndex = nameIndex;
	}

	void setRules(SearchModRules searchRules, String mapLocale) {
		this.searchRules = searchRules;
		this.mapLocale = mapLocale;
	}

	/** @return the unglued name of a name, null when no word is glued */
	public String alternativeName(String name) {
		Unglued unglued = searchRules == null || Algorithms.isEmpty(name) ? null
				: searchRules.rules(mapLocale).unglue(name);
		return unglued == null ? null : unglued.name();
	}

	/**
	 * @return the alternative names of {@code <rule>} of {@code <index>} for a name in a language ("de" of name:de,
	 * null for the language of the map) and its owner, rule id -> alternative name
	 */
	public Map<String, String> ruleAlternatives(String name, String lang, SearchModRuleOwner owner) {
		Map<String, String> alternatives = new TreeMap<>();
		if (searchRules == null || Algorithms.isEmpty(name)) {
			return alternatives;
		}
		SearchModLocaleRules rules = searchRules.rules(searchRules.locales().forName(lang, mapLocale));
		for (Rule rule : rules.index()) {
			if (rule.appliesTo(owner)) {
				String alternative = rule.apply(name);
				if (alternative != null) {
					alternatives.put(rule.id().toString(), alternative);
				}
			}
		}
		return alternatives;
	}

	// name can carry the marker of an alternative name (NameIndexReader.altNameMarker): the marker is a word of the name,
	// so the alternative words refer to it and stay with that variant
	public void addAlternativeNames(String name, String lang, T obj, SearchModRuleOwner owner, int maxPrefixLength) {
		String unglued = alternativeName(name);
		Map<String, String> alternatives = ruleAlternatives(name, lang, owner);
		if (unglued == null && alternatives.isEmpty()) {
			return;
		}
		List<String> nameWords = SearchAlgorithms.splitAndNormalize(name, false);
		Set<String> nameKeys = nameIndex.nameKeys(name, obj);
		if (unglued != null) {
			addUnglued(nameWords, unglued, obj, maxPrefixLength);
		}
		for (Map.Entry<String, String> e : alternatives.entrySet()) {
			addRuleAlternative(nameWords, nameKeys, e.getValue(), obj, maxPrefixLength,
					ruleStats.computeIfAbsent(e.getKey(), k -> new int[3]));
		}
	}

	// words of an alternative name and the marker that tells the search it is a spelling made by a rule
	private List<String> spellingWords(String alternative) {
		List<String> words = SearchAlgorithms.splitAndNormalize(alternative, false);
		words.add(NameIndexReader.RULE_SPELLING_COMMON);
		nameIndex.addMarkerWord(NameIndexReader.RULE_SPELLING_COMMON);
		return words;
	}

	// every new word of an unglued name is a key; an unglued name without new words ("Пасхи" of "о. Пасхи") is not
	// stored: app versions that do not know the marker would take it for the name and lose the dropped word
	private void addUnglued(List<String> nameWords, String alternative, T obj, int maxPrefixLength) {
		List<String> alternativeWords = null;
		for (String word : new TreeSet<>(SearchAlgorithms.splitAndNormalize(alternative, false))) {
			String prefix = NameIndexCreator.nameIndexPreparePrefix(word, maxPrefixLength);
			if (nameWords.contains(word) || Algorithms.isEmpty(prefix)) {
				continue;
			}
			if (alternativeWords == null) {
				alternativeWords = spellingWords(alternative);
			}
			nameIndex.addAlternativeToken(prefix, obj, word, alternativeWords);
		}
	}

	// one more name of the object under the keys of the name that the alternative name shares; false when it shares none
	private boolean addUnderNameKeys(List<String> nameWords, Set<String> nameKeys, List<String> alternativeWords, T obj,
			int maxPrefixLength) {
		boolean shared = false;
		for (String key : new TreeSet<>(alternativeWords)) {
			String prefix = NameIndexCreator.nameIndexPreparePrefix(key, maxPrefixLength);
			if (nameWords.contains(key) && (nameKeys == null || nameKeys.contains(key)) && !Algorithms.isEmpty(prefix)
					&& !NameIndexReader.isIndexMarker(key)) {
				nameIndex.addAlternativeToken(prefix, obj, key, alternativeWords);
				shared = true;
			}
		}
		return shared;
	}

	/**
	 * A new word of the alternative name is a key, except a word that replaced a word the statistics did not keep as a
	 * key ("ave" of "Forest Avenue" where "avenue" is a service word): it would make a key of every such name. That word
	 * is attached: the alternative name is one more name of the object under the keys of the name it shares, so the
	 * search still reads it ("Forest Ave"); without a shared key the attached words are keys.
	 *
	 * @param nameKeys keys of the name, null when every word is a key
	 * @param stats    alternative names, new keys, attached words of the rule
	 */
	private void addRuleAlternative(List<String> nameWords, Set<String> nameKeys, String alternative, T obj,
			int maxPrefixLength, int[] stats) {
		List<String> alternativeWords = spellingWords(alternative);
		Map<String, String> replaced = replacedWords(nameWords, SearchAlgorithms.splitAndNormalize(alternative, false));
		List<String> attached = new ArrayList<>();
		stats[0]++;
		for (String word : new TreeSet<>(alternativeWords)) {
			String prefix = NameIndexCreator.nameIndexPreparePrefix(word, maxPrefixLength);
			if (nameWords.contains(word) || Algorithms.isEmpty(prefix) || NameIndexReader.isIndexMarker(word)) {
				continue;
			}
			String nameWord = replaced.get(word);
			if (nameKeys != null && nameWord != null && !nameKeys.contains(nameWord)) {
				attached.add(word);
			} else {
				nameIndex.addAlternativeToken(prefix, obj, word, alternativeWords);
				stats[1]++;
			}
		}
		if (attached.isEmpty()) {
			return;
		}
		boolean shared = addUnderNameKeys(nameWords, nameKeys, alternativeWords, obj, maxPrefixLength);
		for (String word : attached) {
			if (shared) {
				nameIndex.addTableWord(word);
				stats[2]++;
			} else {
				nameIndex.addAlternativeToken(NameIndexCreator.nameIndexPreparePrefix(word, maxPrefixLength), obj, word,
						alternativeWords);
				stats[1]++;
			}
		}
	}

	/**
	 * The words a rule put in place of words of the name, one for one: the name and the alternative name have as many
	 * words and differ at some of them ("Forest Avenue" -> "Forest Ave"). A phrase or a glued form ("Strada Statale 42"
	 * -> "SS42") has no such correspondence.
	 *
	 * @return a new word of the alternative name -> the word of the name at its place
	 */
	static Map<String, String> replacedWords(List<String> nameWords, List<String> alternativeWords) {
		Map<String, String> replaced = new TreeMap<>();
		if (nameWords.size() != alternativeWords.size()) {
			return replaced;
		}
		Set<String> ambiguous = new HashSet<>();
		for (int i = 0; i < nameWords.size(); i++) {
			String from = nameWords.get(i);
			String to = alternativeWords.get(i);
			// a pure number is a value of its own, not a spelling of the word it replaced
			if (from.equals(to) || nameWords.contains(to) || Algorithms.isInt(to)) {
				continue;
			}
			String previous = replaced.putIfAbsent(to, from);
			if (previous != null && !previous.equals(from)) {
				ambiguous.add(to);
			}
		}
		replaced.keySet().removeAll(ambiguous);
		return replaced;
	}

	// one line per <index> rule that applied: what it costs in the name index
	void logStats() {
		ruleStats.forEach((rule, s) -> log.info("ALTERNATIVE_NAMES_RULE " + rule + ": names=" + s[0] + " keys=" + s[1]
				+ " attached=" + s[2]));
	}
}
