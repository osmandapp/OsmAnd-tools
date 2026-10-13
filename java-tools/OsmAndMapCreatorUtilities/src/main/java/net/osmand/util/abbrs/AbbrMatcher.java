package net.osmand.util.abbrs;

import java.util.List;
import java.util.Set;

/**
 * "The short form is an abbreviation of the long one" for words normalized by {@link AbbrText}
 * (top-org-abbrs-spec.md 5.2.2-5.2.4, prototype alias-proto/mine.py).
 */
public final class AbbrMatcher {

	public static final String ACRONYM = "acronym";
	public static final String GLUED = "glued";

	private AbbrMatcher() {
	}

	/** a pair of the difference of two names: the short word and the words of the long form */
	public static final class Pair {
		public final String abbr;
		public final List<String> full;
		/** words of the long side left out of a span ({@link #spanPair}), 0 for a whole difference */
		public final int leftover;
		/** the short form is the word of the first name */
		public final boolean shortInFirst;

		Pair(String abbr, List<String> full, int leftover, boolean shortInFirst) {
			this.abbr = abbr;
			this.full = full;
			this.leftover = leftover;
			this.shortInFirst = shortInFirst;
		}

		public String fullText() {
			return String.join(" ", full);
		}
	}

	static boolean isSubsequence(String s, String w) {
		int j = 0;
		for (int i = 0; i < s.length(); i++) {
			char c = s.charAt(i);
			while (j < w.length() && w.charAt(j) != c) {
				j++;
			}
			if (j == w.length()) {
				return false;
			}
			j++;
		}
		return true;
	}

	/** hbf of hauptbahnhof, str of strasse: same first letter, letters in order, clearly shorter */
	public static boolean abbrOfWord(String s, String w) {
		return s.length() >= 2 && s.length() < w.length() && s.length() <= Math.max(4, (int) (w.length() * 0.6))
				&& s.charAt(0) == w.charAt(0) && isSubsequence(s.substring(1), w.substring(1));
	}

	/**
	 * zob of zentraler omnibusbahnhof, dmv of department (of) motor vehicles: {@code s} splits into chunks of at most
	 * {@code maxChunk} letters, chunk i starts with the first letter of word i and the rest is a subsequence of the
	 * word; stop words may be skipped
	 */
	public static boolean abbrOfWords(String s, List<String> ws, int maxChunk, Set<String> stop) {
		return ws.size() >= 2 && abbrOfWords(s, 0, ws, 0, maxChunk, stop);
	}

	private static boolean abbrOfWords(String s, int si, List<String> ws, int wi, int maxChunk, Set<String> stop) {
		if (wi == ws.size()) {
			return si == s.length();
		}
		String w = ws.get(wi);
		if (stop.contains(w) && abbrOfWords(s, si, ws, wi + 1, maxChunk, stop)) {
			return true;
		}
		if (si >= s.length() || w.isEmpty() || s.charAt(si) != w.charAt(0)) {
			return false;
		}
		for (int end = si + 1; end <= Math.min(s.length(), si + maxChunk); end++) {
			if (isSubsequence(s.substring(si + 1, end), w.substring(1))
					&& abbrOfWords(s, end, ws, wi + 1, maxChunk, stop)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * H3: {@link #ACRONYM} - the token is the initials (chunks of {@code maxChunk} letters) of a run of at least two
	 * words of the value; {@link #GLUED} - the token is the glued words of a run (cashzone); "" - neither.
	 *
	 * @param headEnd when &ge; 0 the run has to end at this word (the last significant word of the value)
	 * @return kind and the run as {@code [kind, start, end)}
	 */
	public static Object[] spanAcronym(String token, List<String> value, int maxLen, int maxChunk, Set<String> stop,
			int headEnd) {
		int n = value.size();
		Object[] best = null;
		for (int i = 0; i < n; i++) {
			for (int j = i + 2; j <= n; j++) {
				if (headEnd >= 0 && j != headEnd) {
					continue;
				}
				List<String> span = value.subList(i, j);
				if (stop.contains(span.get(0)) || stop.contains(span.get(span.size() - 1))) {
					continue;
				}
				StringBuilder glued = new StringBuilder();
				StringBuilder gluedNoStop = new StringBuilder();
				for (String w : span) {
					glued.append(w);
					if (!stop.contains(w)) {
						gluedNoStop.append(w);
					}
				}
				if (token.equals(glued.toString()) || token.equals(gluedNoStop.toString())) {
					if (best == null) {
						best = new Object[] { GLUED, i, j };
					}
					continue;
				}
				// the longest run wins: "new york state department of motor vehicles" -> department ... vehicles
				if (token.length() <= maxLen && abbrOfWords(token, span, maxChunk, stop)
						&& (best == null || best[0] != ACRONYM || j - i > (int) best[2] - (int) best[1])) {
					best = new Object[] { ACRONYM, i, j };
				}
			}
		}
		return best;
	}

	/** common prefix and suffix of two word lists removed: {@code [start, endA, endB]} */
	static int[] trimCommon(List<String> a, List<String> b) {
		int i = 0;
		while (i < a.size() && i < b.size() && a.get(i).equals(b.get(i))) {
			i++;
		}
		int j = 0;
		while (j < a.size() - i && j < b.size() - i && a.get(a.size() - 1 - j).equals(b.get(b.size() - 1 - j))) {
			j++;
		}
		return new int[] { i, a.size() - j, b.size() - j };
	}

	/**
	 * The aligned difference of two names (common prefix and suffix removed): one side is one word, the short form,
	 * of the other side - one word or {@code maxLongWords} words.
	 */
	public static Pair diffPair(List<String> a, List<String> b, int maxLongWords, int maxChunk, Set<String> stop) {
		int[] t = trimCommon(a, b);
		List<String> da = a.subList(t[0], t[1]);
		List<String> db = b.subList(t[0], t[2]);
		if (da.isEmpty() || db.isEmpty()) {
			return null;
		}
		for (int k = 0; k < 2; k++) {
			List<String> sh = k == 0 ? da : db;
			List<String> lg = k == 0 ? db : da;
			if (sh.size() != 1 || lg.size() > maxLongWords) {
				continue;
			}
			String s = sh.get(0);
			if (lg.size() == 1 && abbrOfWord(s, lg.get(0))) {
				return new Pair(s, List.copyOf(lg), 0, k == 0);
			}
			if (lg.size() > 1 && s.length() <= 6 && abbrOfWords(s, lg, maxChunk, stop)) {
				return new Pair(s, List.copyOf(lg), 0, k == 0);
			}
		}
		return null;
	}

	/**
	 * Fallback of {@link #diffPair}: one side of the difference is one word, the other has a run of 2 to
	 * {@code spanMaxWords} words the word is the initials of: "new york state department of motor vehicles" vs "dmv"
	 * gives dmv of "department of motor vehicles", three words left over; the longest run wins.
	 */
	public static Pair spanPair(List<String> a, List<String> b, int spanMaxWords, int minLen, int maxLen, int maxChunk,
			Set<String> stop) {
		int[] t = trimCommon(a, b);
		List<String> da = a.subList(t[0], t[1]);
		List<String> db = b.subList(t[0], t[2]);
		if (da.isEmpty() || db.isEmpty()) {
			return null;
		}
		for (int k = 0; k < 2; k++) {
			List<String> sh = k == 0 ? da : db;
			List<String> lg = k == 0 ? db : da;
			if (sh.size() != 1 || lg.size() < 2 || sh.get(0).length() < minLen || sh.get(0).length() > maxLen) {
				continue;
			}
			String s = sh.get(0);
			int bi = -1, bj = -1;
			for (int i = 0; i < lg.size(); i++) {
				for (int j = i + 2; j <= Math.min(lg.size(), i + spanMaxWords); j++) {
					if (stop.contains(lg.get(i)) || stop.contains(lg.get(j - 1))) {
						continue;
					}
					if (abbrOfWords(s, lg.subList(i, j), maxChunk, stop) && (bi < 0 || j - i > bj - bi)) {
						bi = i;
						bj = j;
					}
				}
			}
			if (bi >= 0) {
				return new Pair(s, List.copyOf(lg.subList(bi, bj)), lg.size() - (bj - bi), k == 0);
			}
		}
		return null;
	}

	/** roman numerals: vii viii */
	public static boolean isRoman(String w) {
		if (w.isEmpty()) {
			return false;
		}
		for (int i = 0; i < w.length(); i++) {
			if ("ivxlcdm".indexOf(w.charAt(i)) < 0) {
				return false;
			}
		}
		return true;
	}
}
