package net.osmand.obf.preparation;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import net.osmand.binary.CommonWords;
import net.osmand.binary.CommonWordsMultiIndex;
import net.osmand.binary.NameIndexReader;
import net.osmand.binary.SearchVariantRules.RuleId;
import net.osmand.search.core.SearchPhrase;
import net.osmand.util.Algorithms;
import net.osmand.util.SearchAlgorithms;

/**
 * Every decision on the words of one name of an object and of its alternative names (rules-spec.md, 4.1), made before
 * anything is written: {@link NameIndexCreator} only stores the keys, the words of the common words table and the
 * statistics. One function decides every word, whatever variant it comes from: numbers ({@code indexNumbers}),
 * notable objects, classes, the legacy key and the words of alternative names attached to the keys of the name.
 */
public final class NameIndexPlan {

	/** What the writer does with a word of a variant. */
	enum Action {
		// a key of the name index
		KEY,
		// a key that the other words of the name still refer to in the common words table (LEGACY)
		KEY_AND_TABLE,
		// not a key: a word of the common words table, a non-indexed occurrence (DROPPED_*)
		TABLE_NON_INDEXED,
		// not a key: a word of the common words table under the keys of the name (ALT_ATTACHED), not counted as a
		// non-indexed occurrence, which counts only the words the statistics dropped (rules-spec.md, 5.2)
		TABLE,
		// a marker of the index ("cityasstreetcommon", a marker of an alternative name)
		MARKER,
		// nothing is stored: a pure number of a name without numbers, a word of an alternative name the name decides
		NONE
	}

	/** The decision on one word of a variant. */
	record Word(String word, String prefix, KeyDecision decision, Action action) {
	}

	/**
	 * One variant: the name itself or an alternative name of a rule.
	 *
	 * @param rule       the rule of an alternative name, null for the name itself
	 * @param text       the name or the alternative name, the example of the statistics
	 * @param words      every word of the variant: the names of its atoms
	 * @param decisions  the words that the writer stores or counts, in the order it does
	 * @param sharedKeys keys of the name that the alternative name shares: its attached words are stored under them
	 */
	record Variant(RuleId rule, String text, List<String> words, List<Word> decisions, List<String> sharedKeys) {
		boolean alternative() {
			return rule != null;
		}
	}

	/** An alternative name of a name: the rule and the text it gives. */
	record Alternative(RuleId rule, String text, boolean alwaysKeys) {
	}

	/**
	 * Where the name is indexed.
	 *
	 * @param keysMap         the map whose statistics group chooses the keys, null when every word is a key
	 * @param maxPrefixLength length of the prefix of a key
	 * @param indexNumbers    a pure number is a key (postcodes, ids)
	 * @param notable         an object people know by any word of its name: every word is a key
	 */
	record Context(String keysMap, int maxPrefixLength, boolean indexNumbers, boolean notable) {
	}

	final Variant name;
	final List<Variant> alternatives;

	private NameIndexPlan(Variant name, List<Variant> alternatives) {
		this.name = name;
		this.alternatives = alternatives;
	}

	/**
	 * @param indexed      the name with the markers of the index ("cityasstreetcommon", a marker of an alternative name)
	 * @param alternatives alternative names of the name, applied to the name without the markers the writer adds
	 */
	static NameIndexPlan plan(String indexed, List<Alternative> alternatives, Context ctx) {
		Variant name = planName(indexed, ctx);
		Set<String> nameKeys = new HashSet<>();
		for (Word w : name.decisions) {
			if (w.action == Action.KEY || w.action == Action.KEY_AND_TABLE) {
				nameKeys.add(w.word);
			}
		}
		List<Variant> variants = new ArrayList<>(alternatives.size());
		for (Alternative alternative : alternatives) {
			variants.add(planAlternative(alternative, name.words, nameKeys, ctx));
		}
		return new NameIndexPlan(name, variants);
	}

	private static Variant planName(String indexed, Context ctx) {
		List<String> uniqueNames = SearchAlgorithms.splitAndNormalize(indexed, true);
		List<String> allNames = SearchAlgorithms.splitAndNormalize(indexed, false);
		// one decision for every word (rules-spec.md, 4.2): numbers and markers, notable, <class0>, classes 2 and 1
		CommonWordsMultiIndex.KeyOutcome[] outcomes = ctx.keysMap == null ? null
				: CommonWordsMultiIndex.getInstance().selectKeys(ctx.keysMap, uniqueNames, ctx.notable);
		String legacyKey = outcomes == null ? null : legacyKey(uniqueNames, outcomes);
		List<Word> decisions = new ArrayList<>();
		for (int i = 0; i < uniqueNames.size(); i++) {
			String token = uniqueNames.get(i);
			if (Algorithms.isEmpty(token)) {
				continue;
			}
			if (NameIndexReader.isIndexMarker(token)) {
				decisions.add(new Word(token, null, KeyDecision.NUMBER, Action.MARKER));
				continue;
			}
			String prefix = NameIndexCreator.nameIndexPreparePrefix(token, ctx.maxPrefixLength);
			if (Algorithms.isEmpty(prefix)) {
				continue;
			}
			if (isSkippedNumber(token, ctx)) {
				decisions.add(new Word(token, prefix, KeyDecision.NUMBER, Action.NONE));
			} else if (outcomes != null && !outcomes[i].key && !token.equals(legacyKey)) {
				// not a key of this name: kept as a reference in the common words table
				decisions.add(new Word(token, prefix, outcomes[i] == CommonWordsMultiIndex.KeyOutcome.DROPPED_CLASS1
						? KeyDecision.DROPPED_CLASS1 : KeyDecision.DROPPED_CLASS2, Action.TABLE_NON_INDEXED));
			} else if (token.equals(legacyKey)) {
				// the other words of the name still refer to it, as they did before it became a key
				decisions.add(new Word(token, prefix, KeyDecision.LEGACY, Action.KEY_AND_TABLE));
			} else {
				decisions.add(new Word(token, prefix, outcomes == null ? KeyDecision.KEPT : KeyDecision.of(outcomes[i]),
						Action.KEY));
			}
		}
		return new Variant(null, indexed, allNames, decisions, List.of());
	}

	// TODO remove when app versions with the legacy search (SearchCoreFactory) no longer download maps: it looks a name
	// up by the one query word SearchPhrase picks, a word CommonWords does not know first, so the name keeps such a word
	// ("amsterdam" of Amsterdam City Farm, frequent in the Netherlands); known words like "de" stay out
	private static String legacyKey(List<String> uniqueNames, CommonWordsMultiIndex.KeyOutcome[] outcomes) {
		List<String> words = new ArrayList<>(uniqueNames);
		words.removeIf(NameIndexReader::isIndexMarker);
		String legacyWord = SearchPhrase.selectMainUnknownWordToSearch(words);
		if (legacyWord.isEmpty() || CommonWords.getInstance().getCommonSearch(legacyWord) != -1) {
			return null;
		}
		int i = uniqueNames.indexOf(legacyWord);
		// a key of the statistics already
		return i >= 0 && outcomes[i].key ? null : legacyWord;
	}

	/**
	 * A word of an alternative name is a key by its own class (rules-spec.md, 3.3): a word the name has keeps the
	 * decision of the name; a new word is a key when the statistics keep it among the words of the alternative name,
	 * else it is attached to the keys of the name that the alternative name shares.
	 */
	private static Variant planAlternative(Alternative alternative, List<String> nameWords, Set<String> nameKeys,
			Context ctx) {
		List<String> alternativeWords = SearchAlgorithms.splitAndNormalize(alternative.text, false);
		List<String> unique = new ArrayList<>(new LinkedHashSet<>(alternativeWords));
		// the class of a new word is decided among the words of the alternative name, as for a name
		CommonWordsMultiIndex.KeyOutcome[] outcomes = alternative.alwaysKeys || ctx.notable || ctx.keysMap == null
				? null : CommonWordsMultiIndex.getInstance().selectKeys(ctx.keysMap, unique, false);
		List<Word> decisions = new ArrayList<>();
		List<Word> attached = new ArrayList<>();
		for (int i = 0; i < unique.size(); i++) {
			String word = unique.get(i);
			String prefix = NameIndexCreator.nameIndexPreparePrefix(word, ctx.maxPrefixLength);
			if (Algorithms.isEmpty(prefix) || NameIndexReader.isIndexMarker(word)) {
				continue;
			}
			if (nameWords.contains(word)) {
				if (!nameKeys.contains(word)) {
					decisions.add(new Word(word, prefix, KeyDecision.ALT_SHADOWED, Action.NONE));
				}
			} else if (isSkippedNumber(word, ctx)) {
				// numbers come first (rules-spec.md, 4.2): an alternative name follows the number policy of the name
				decisions.add(new Word(word, prefix, KeyDecision.NUMBER, Action.NONE));
			} else if (alternative.alwaysKeys) {
				decisions.add(new Word(word, prefix, KeyDecision.ALWAYS, Action.KEY));
			} else if (ctx.notable) {
				decisions.add(new Word(word, prefix, KeyDecision.NOTABLE, Action.KEY));
			} else if (outcomes == null || outcomes[i].key) {
				decisions.add(new Word(word, prefix, KeyDecision.ALT, Action.KEY));
			} else {
				attached.add(new Word(word, prefix, KeyDecision.ALT_ATTACHED, Action.TABLE));
			}
		}
		List<String> shared = new ArrayList<>();
		if (!attached.isEmpty()) {
			for (String key : unique) {
				if (nameKeys.contains(key)) {
					shared.add(key);
				}
			}
			for (Word w : attached) {
				// no shared key to attach to: the word is a key, else the alternative name could not be found
				decisions.add(shared.isEmpty() ? new Word(w.word, w.prefix, KeyDecision.ALT, Action.KEY) : w);
			}
		}
		return new Variant(alternative.rule, alternative.text, alternativeWords, decisions, shared);
	}

	// a pure number is kept with the other words of the name ("6178/2.Sokak"), a number with letters is not: "33-я" of
	// "вулиця 33-я Лінія" is the only word telling apart its Лінія streets
	private static boolean isSkippedNumber(String token, Context ctx) {
		return !ctx.indexNumbers && SearchAlgorithms.isNumber2Letters(token)
				&& NameIndexCreator.parsePureIntegerSuffix(token) != null;
	}
}
