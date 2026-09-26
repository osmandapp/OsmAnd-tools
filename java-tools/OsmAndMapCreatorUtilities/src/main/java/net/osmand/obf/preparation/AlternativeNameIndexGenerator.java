package net.osmand.obf.preparation;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

import net.osmand.binary.CommonWordsMultiIndex;
import net.osmand.binary.SearchLocales;
import net.osmand.binary.SearchVariantRules;
import net.osmand.binary.SearchVariantRules.Variant;
import net.osmand.data.City;
import net.osmand.data.Street;
import net.osmand.obf.preparation.NameIndexCreator.PoiNameObject;
import net.osmand.util.Algorithms;
import net.osmand.util.SearchAlgorithms;

/**
 * Alternative spellings of a name that go to the name index next to the name itself, so the search finds an object
 * by a word its name does not have as a separate word: "L'abordage" is found by "abordage", "Garry's" by "garry".
 * <p>
 * Every rule turns a name into one alternative name (or none). The words of the alternative name that the name does
 * not already have become keys of the object and refer to the other words of the alternative name, the same way
 * {@link NameIndexCreator#addToNameIndex} indexes a name.
 * <p>
 * Rules by location and language, as {@link CommonWordsMultiIndex} chooses service words by the language group
 * of the map ({@link #getLanguageGroup()}, a group of {@link CommonWordsMultiIndex#DEFAULT_GROUPS}):
 * <ul>
 * <li>abbreviations: "St." / "Saint" / "Sankt", "Dr." / "Doctor", "вул." / "вулиця", "пр." / "проспект";</li>
 * <li>spelling variants: "ß" / "ss" (de), "ё" / "е" (ru), "ij" / "y" (nl);</li>
 * <li>numerals: "3rd" / "third", "1-й" / "перший".</li>
 * </ul>
 * Localized expressions come from {@code rules.xml} and its locale overlays in OsmAnd-java resources.
 * The index size has to be measured per rule: every alternative word is one more key of the object.
 */
public class AlternativeNameIndexGenerator<T> {
	public interface AlternativeNameRule {
		// alternative name of the name, null when the rule does not apply; lang is the language of the name ("fr" of
		// name:fr, null for the main name and alt_name), group is the language group of the map, both can be null
		String alternativeName(String name, String lang, String group);
	}

	/** Size of the alternative names of one name index: every key is one more prefix posting of an object. */
	public static class Stats {
		// names that got at least one alternative name
		int names;
		// alternative names produced by the rules
		int alternatives;
		// alternative keys added to the name index
		int keys;
		// alternative names by rule ("unglue", "street SS$1")
		final Map<String, Integer> byRule = new TreeMap<>();

		public void add(Stats other) {
			names += other.names;
			alternatives += other.alternatives;
			keys += other.keys;
			other.byRule.forEach((rule, count) -> byRule.merge(rule, count, Integer::sum));
		}

		@Override
		public String toString() {
			return "names=" + names + " alternatives=" + alternatives + " keys=" + keys;
		}
	}

	private final NameIndexCreator<T> nameIndex;
	// every rule applied to a name, in this order
	private final AlternativeNameRule[] rules = { new UnglueRule() };
	private final Stats stats = new Stats();
	// language group of the map (CommonWordsMultiIndex.DEFAULT_GROUPS), null when no group covers it
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

	// name can carry the marker of an alternative name (NameIndexReader.altNameMarker): the marker is a word of the name,
	// so the alternative words refer to it and stay with that variant
	public void addAlternativeNames(String name, String lang, T obj, int maxPrefixLength) {
		if (Algorithms.isEmpty(name)) {
			return;
		}
		List<String> nameWords = null;
		int alternatives = stats.alternatives;
		for (AlternativeNameRule rule : rules) {
			String alternative = rule.alternativeName(name, lang, languageGroup);
			if (alternative == null) {
				continue;
			}
			if (nameWords == null) {
				nameWords = SearchAlgorithms.splitAndNormalize(name, false);
			}
			addAlternativeName(nameWords, alternative, obj, maxPrefixLength, "unglue");
		}
		String owner = ownerType(obj);
		if (owner != null) {
			// name and alt_name are in the language of the map, name:de in German in the country of the map
			String locale = SearchLocales.forName(lang, mapLocale);
			for (Variant rule : SearchVariantRules.forLocale(locale).index()) {
				if (!rule.appliesTo(owner)) {
					continue;
				}
				String alternative = rule.apply(name);
				if (alternative != null) {
					if (nameWords == null) {
						nameWords = SearchAlgorithms.splitAndNormalize(name, false);
					}
					addAlternativeName(nameWords, alternative, obj, maxPrefixLength, owner + " " + rule.target().trim());
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

	private void addAlternativeName(List<String> nameWords, String alternative, T obj, int maxPrefixLength, String rule) {
		List<String> alternativeWords = SearchAlgorithms.splitAndNormalize(alternative, false);
		stats.alternatives++;
		stats.byRule.merge(rule, 1, Integer::sum);
		for (String word : new TreeSet<>(alternativeWords)) {
			String prefix = NameIndexCreator.nameIndexPreparePrefix(word, maxPrefixLength);
			if (nameWords.contains(word) || Algorithms.isEmpty(prefix)) {
				continue;
			}
			nameIndex.addAlternativeToken(prefix, obj, word, alternativeWords);
			stats.keys++;
		}
	}

	// words glued by a dot or, in latin names, an apostrophe ("L'Atelier d'Anaïs" -> "Atelier Anaïs")
    static class UnglueRule implements AlternativeNameRule {

		private static final int MIN_WORD_LENGTH = 2;

		@Override
		public String alternativeName(String name, String lang, String group) {
			List<String> words = new ArrayList<>();
			boolean glued = false;
			for (String word : SearchAlgorithms.canonicalizePunctuation(name).split(" ")) {
				if (word.isEmpty()) {
					continue;
				}
				List<String> parts = unglueWord(word);
				if (parts == null) {
					words.add(word);
				} else {
					words.addAll(parts);
					glued = true;
				}
			}
			String unglued = String.join(" ", words).trim();
			return glued && !unglued.isEmpty() ? unglued : null;
		}

		private List<String> unglueWord(String word) {
			boolean apostropheGlues = isLatin(word);
			if (word.chars().anyMatch(Character::isDigit) || word.chars().noneMatch(c -> isGlue((char) c, apostropheGlues))) {
				return null;
			}
			List<String> parts = new ArrayList<>();
			boolean letterDropped = false;
			int start = 0;
			for (int i = 0; i <= word.length(); i++) {
				if (i == word.length() || isGlue(word.charAt(i), apostropheGlues)) {
					String part = word.substring(start, i);
					if (part.length() >= MIN_WORD_LENGTH) {
						parts.add(part);
					} else if (!part.isEmpty()) {
						letterDropped = true;
					}
					start = i + 1;
				}
			}
			return parts.size() > 1 || letterDropped ? parts : null;
		}

		private boolean isGlue(char c, boolean apostropheGlues) {
			return c == '.' || (c == '\'' && apostropheGlues);
		}

		private boolean isLatin(String word) {
			return word.codePoints().filter(Character::isLetter)
					.allMatch(c -> Character.UnicodeScript.of(c) == Character.UnicodeScript.LATIN);
		}
	}
}
