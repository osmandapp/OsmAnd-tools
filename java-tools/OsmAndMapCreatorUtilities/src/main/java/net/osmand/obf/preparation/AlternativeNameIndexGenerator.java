package net.osmand.obf.preparation;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import java.util.regex.Pattern;

import net.osmand.binary.CommonWordsMultiIndex;
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
	private static final Pattern NAME_LANGUAGE = Pattern.compile("[A-Za-z]{2,3}([_-][A-Za-z0-9]{2,8})*");

	public interface AlternativeNameRule {
		// alternative name of the name, null when the rule does not apply; lang is the language of the name ("fr" of
		// name:fr, null for the main name and alt_name), group is the language group of the map, both can be null
		String alternativeName(String name, String lang, String group);
	}

	private final NameIndexCreator<T> nameIndex;
	// every rule applied to a name, in this order
	private final AlternativeNameRule[] rules = { new UnglueRule() };
	// language group of the map (CommonWordsMultiIndex.DEFAULT_GROUPS), null when no group covers it
	private String languageGroup;
	private String mapName;

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
	}

	// name can carry the marker of an alternative name (NameIndexReader.altNameMarker): the marker is a word of the name,
	// so the alternative words refer to it and stay with that variant
	public void addAlternativeNames(String name, String lang, T obj, int maxPrefixLength) {
		if (Algorithms.isEmpty(name)) {
			return;
		}
		List<String> nameWords = null;
		for (AlternativeNameRule rule : rules) {
			String alternative = rule.alternativeName(name, lang, languageGroup);
			if (alternative == null) {
				continue;
			}
			if (nameWords == null) {
				nameWords = SearchAlgorithms.splitAndNormalize(name, false);
			}
			addAlternativeName(nameWords, alternative, obj, maxPrefixLength);
		}
		String owner = ownerType(obj);
		if (owner != null) {
			String locale = lang == null ? languageGroup : nameLanguage(lang);
			for (Variant rule : SearchVariantRules.forLocale(locale).index()) {
				if (!rule.appliesTo(owner)) {
					continue;
				}
				String alternative = rule.apply(name);
				if (alternative != null) {
					if (nameWords == null) {
						nameWords = SearchAlgorithms.splitAndNormalize(name, false);
					}
					addAlternativeName(nameWords, alternative, obj, maxPrefixLength);
				}
			}
		}
	}

	private String nameLanguage(String nameTag) {
		String suffix = nameTag.substring(nameTag.lastIndexOf(':') + 1);
		return (nameTag.indexOf(':') >= 0 || suffix.length() <= 3)
				&& NAME_LANGUAGE.matcher(suffix).matches() ? suffix : null;
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
