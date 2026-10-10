package net.osmand.obf.preparation;

import java.util.List;
import java.util.TreeSet;

import net.osmand.search.rules.SearchModLocaleRules;
import net.osmand.search.rules.SearchModLocaleRules.Unglued;
import net.osmand.util.Algorithms;
import net.osmand.util.SearchAlgorithms;

/**
 * Alternative spellings of a name that go to the name index next to the name itself, so the search finds an object
 * by a word its name does not have as a separate word: "L'abordage" is found by "abordage", "Garry's" by "garry".
 * <p>
 * The alternative name comes from {@code <unglue>} of the search rules of the locale of the map. The words of the
 * alternative name that the name does not already have become keys of the object and refer to the other words of the
 * alternative name, the same way {@link NameIndexCreator#addToNameIndex} indexes a name.
 */
public class AlternativeNameIndexGenerator<T> {

	private final NameIndexCreator<T> nameIndex;
	// rules of the locale of the map, null gives no alternative names
	private SearchModLocaleRules rules;

	public AlternativeNameIndexGenerator(NameIndexCreator<T> nameIndex) {
		this.nameIndex = nameIndex;
	}

	void setRules(SearchModLocaleRules rules) {
		this.rules = rules;
	}

	/** @return the alternative name of a name, null when no rule applies */
	public String alternativeName(String name) {
		Unglued unglued = rules == null || Algorithms.isEmpty(name) ? null : rules.unglue(name);
		return unglued == null ? null : unglued.name();
	}

	// name can carry the marker of an alternative name (NameIndexReader.altNameMarker): the marker is a word of the name,
	// so the alternative words refer to it and stay with that variant
	public void addAlternativeNames(String name, String lang, T obj, int maxPrefixLength) {
		String alternative = alternativeName(name);
		if (alternative != null) {
			addAlternativeName(SearchAlgorithms.splitAndNormalize(name, false), alternative, obj, maxPrefixLength);
		}
	}

	private void addAlternativeName(List<String> nameWords, String alternative, T obj, int maxPrefixLength) {
		List<String> alternativeWords = SearchAlgorithms.splitAndNormalize(alternative, false);
		for (String word : new TreeSet<>(alternativeWords)) {
			String prefix = NameIndexCreator.nameIndexPreparePrefix(word, maxPrefixLength);
			if (nameWords.contains(word) || Algorithms.isEmpty(prefix)) {
				continue;
			}
			nameIndex.addAlternativeToken(prefix, obj, word, alternativeWords);
		}
	}
}
