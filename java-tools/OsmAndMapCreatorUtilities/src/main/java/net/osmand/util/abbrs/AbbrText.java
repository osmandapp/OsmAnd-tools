package net.osmand.util.abbrs;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import net.osmand.util.SearchAlgorithms;

/**
 * The one normalization of the analyzer and the generator (top-org-abbrs-spec.md 5.2.1): the words of the search
 * ({@link SearchAlgorithms#splitAndNormalize}) compared after {@link SearchAlgorithms#alignChars}. {@code abbr},
 * {@code full}, {@code bind_value} are such words joined by one space.
 */
public final class AbbrText {

	private AbbrText() {
	}

	/** a word of a name with its original spelling */
	public static final class Word {
		public final String word;
		public final String original;

		Word(String word, String original) {
			this.word = word;
			this.original = original;
		}

		@Override
		public String toString() {
			return original;
		}
	}

	/** values of a tag: "A;B" is two values */
	public static List<String> values(String value) {
		if (value == null || value.isEmpty()) {
			return Collections.emptyList();
		}
		List<String> l = new ArrayList<>();
		for (String v : value.split(";")) {
			if (!v.trim().isEmpty()) {
				l.add(v.trim());
			}
		}
		return l;
	}

	/** normalized aligned words of one value ("Hannover Hbf." -> [hannover, hbf.]) */
	public static List<String> words(String value) {
		List<String> l = new ArrayList<>();
		for (Word w : spelledWords(value)) {
			l.add(w.word);
		}
		return l;
	}

	/** normalized aligned words of one value with their original spelling */
	public static List<Word> spelledWords(String value) {
		if (value == null || value.isEmpty()) {
			return Collections.emptyList();
		}
		List<String> tokens = new ArrayList<>();
		List<String> normalized = SearchAlgorithms.splitAndNormalize(value, tokens, false);
		// the tokens of the search are lower case: the original spelling is the same range of the value
		String canonical = SearchAlgorithms.canonicalizePunctuation(value);
		String lower = canonical.toLowerCase();
		boolean sameLength = lower.length() == canonical.length();
		List<Word> l = new ArrayList<>(normalized.size());
		int from = 0;
		for (int i = 0; i < normalized.size(); i++) {
			String w = align(normalized.get(i));
			String token = i < tokens.size() ? tokens.get(i) : normalized.get(i);
			String original = token;
			int at = sameLength ? lower.indexOf(token, from) : -1;
			if (at >= 0) {
				original = canonical.substring(at, at + token.length());
				from = at + token.length();
			}
			if (!w.isEmpty()) {
				l.add(new Word(w, original));
			}
		}
		return l;
	}

	/** one normalized word: aligned, without the trailing dot of an abbreviation ("str." -> "str") */
	public static String align(String word) {
		String w = SearchAlgorithms.alignChars(word);
		while (w.endsWith(".")) {
			w = w.substring(0, w.length() - 1);
		}
		return w;
	}

	/** normalized value: its words by one space */
	public static String normalize(String value) {
		return String.join(" ", words(value));
	}

	/** {@code phrase} (words) is a run of consecutive words of {@code words} */
	public static boolean containsPhrase(List<String> words, List<String> phrase) {
		if (phrase.isEmpty() || phrase.size() > words.size()) {
			return false;
		}
		for (int i = 0; i + phrase.size() <= words.size(); i++) {
			if (words.subList(i, i + phrase.size()).equals(phrase)) {
				return true;
			}
		}
		return false;
	}

	public static boolean hasLetters(String w) {
		for (int i = 0; i < w.length(); i++) {
			if (Character.isLetter(w.charAt(i))) {
				return true;
			}
		}
		return false;
	}

	/** the script of the word has case: Latin, Cyrillic, Greek; not CJK, Arabic, Thai, Georgian */
	public static boolean hasCase(String w) {
		for (int i = 0; i < w.length(); i++) {
			char c = w.charAt(i);
			if (Character.isLetter(c) && Character.isUpperCase(c) != Character.isLowerCase(c)) {
				return true;
			}
		}
		return false;
	}

	public static boolean isAllUpper(String w) {
		boolean letter = false;
		for (int i = 0; i < w.length(); i++) {
			char c = w.charAt(i);
			if (Character.isLetter(c)) {
				letter = true;
				if (!Character.isUpperCase(c)) {
					return false;
				}
			}
		}
		return letter;
	}

	public static boolean isCapitalized(String w) {
		for (int i = 0; i < w.length(); i++) {
			char c = w.charAt(i);
			if (Character.isLetter(c)) {
				return Character.isUpperCase(c);
			}
		}
		return false;
	}

	/** a name of two and more words with letters, every such word in capitals ("NYS DMV OFFICE") */
	public static boolean isAllCapsName(List<Word> words) {
		int n = 0;
		for (Word w : words) {
			if (hasLetters(w.original)) {
				if (!isAllUpper(w.original)) {
					return false;
				}
				n++;
			}
		}
		return n >= 2;
	}
}
