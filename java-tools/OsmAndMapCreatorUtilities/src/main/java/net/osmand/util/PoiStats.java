package net.osmand.util;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.osmand.util.TagValuesCollector.Counts;

/**
 * "collect-tag-values --poi-stats": how POIs are named and who operates them, per region. Input for search
 * synonyms, abbreviations (DMV, ЦНАП) and operator rules. Only objects with one of {@link #TYPE_KEYS} count;
 * the type is that key=value (plus government= / religion= where they split it).
 *
 * Rows (kind, a, b, count, extra, sample):
 * <ul>
 * <li>type: a = type, count = objects, extra = objects with operator</li>
 * <li>operator / network / brand: a = value, b = type, extra = objects whose name contains the value</li>
 * <li>word: a = lower-case word of the name, b = type or * for all types</li>
 * <li>acronym: a = upper-case token of a mixed-case name, b = type, extra = objects with operator, sample = a name</li>
 * </ul>
 */
public class PoiStats {

	static final String[] TYPE_KEYS = { "amenity", "shop", "office", "healthcare", "tourism", "leisure", "craft",
			"emergency", "historic", "military", "man_made", "public_transport", "aeroway", "club" };
	static final String[] VALUE_KEYS = { "operator", "network", "brand" };
	static final String[] TAGS;
	static final String HEADER = "kind\ta\tb\tcount\textra\tsample";

	private static final int GOVERNMENT, RELIGION, NAME, OPERATOR, VALUES;
	private static final Pattern WORD_SPLIT = Pattern.compile("[^\\p{L}]+");
	private static final Pattern ACRONYM = Pattern.compile("(?<![\\p{L}\\d])\\p{Lu}{2,7}(?![\\p{L}\\d])");

	static {
		int n = TYPE_KEYS.length;
		TAGS = Arrays.copyOf(TYPE_KEYS, n + 3 + VALUE_KEYS.length);
		GOVERNMENT = n;
		RELIGION = n + 1;
		NAME = n + 2;
		VALUES = n + 3;
		OPERATOR = VALUES;
		TAGS[GOVERNMENT] = "government";
		TAGS[RELIGION] = "religion";
		TAGS[NAME] = "name";
		System.arraycopy(VALUE_KEYS, 0, TAGS, VALUES, VALUE_KEYS.length);
	}

	private final int minCount;

	public PoiStats(int minCount) {
		this.minCount = minCount;
	}

	/** type rows are always written, the long tail of the rest only from minCount objects */
	boolean keep(String key, int count) {
		return count >= minCount || key.startsWith("type\t");
	}

	static String line(Entry<String, int[]> e, Counts c) {
		String sample = c.samples.get(e.getKey());
		return e.getKey() + "\t" + e.getValue()[0] + "\t" + e.getValue()[1] + "\t" + (sample == null ? "" : sample);
	}

	/** o = raw values indexed as {@link #TAGS}; cleared on return */
	void object(Counts c, String[] o) {
		try {
			String type = null;
			for (int i = 0; i < TYPE_KEYS.length && type == null; i++) {
				if (!isEmpty(o[i])) {
					type = TYPE_KEYS[i] + "=" + TagValuesCollector.normalize(o[i]);
				}
			}
			if (type == null) {
				return;
			}
			if (type.equals("office=government") && !isEmpty(o[GOVERNMENT])) {
				type += ";government=" + TagValuesCollector.normalize(o[GOVERNMENT]);
			} else if (type.equals("amenity=place_of_worship") && !isEmpty(o[RELIGION])) {
				type += ";religion=" + TagValuesCollector.normalize(o[RELIGION]);
			}
			String name = isEmpty(o[NAME]) ? null : o[NAME];
			String lowName = name == null ? null : TagValuesCollector.normalize(name);
			int withOperator = isEmpty(o[OPERATOR]) ? 0 : 1;
			c.add("type\t" + type + "\t", 1, withOperator);
			for (int i = 0; i < VALUE_KEYS.length; i++) {
				String v = o[VALUES + i];
				if (!isEmpty(v)) {
					v = TagValuesCollector.normalize(v);
					c.add(VALUE_KEYS[i] + "\t" + v + "\t" + type, 1, lowName != null && lowName.contains(v) ? 1 : 0);
				}
			}
			if (name == null) {
				return;
			}
			Set<String> seen = new HashSet<>();
			for (String w : WORD_SPLIT.split(name.toLowerCase(Locale.ROOT))) {
				if (w.length() >= 2 && seen.add(w)) {
					c.add("word\t" + w + "\t" + type, 1, 0);
					c.add("word\t" + w + "\t*", 1, 0);
				}
			}
			if (!hasLowerCase(name) && seen.size() > 2) {
				// "ESCUELA PRIMARIA RURAL": every word of an upper-case name looks like an abbreviation,
				// but a short one ("PNR", "BANDEC Guamá" typed as "BANDEC GUAMA") usually is one
				return;
			}
			seen.clear();
			Matcher m = ACRONYM.matcher(name);
			while (m.find()) {
				if (seen.add(m.group())) {
					String key = "acronym\t" + m.group() + "\t" + type;
					c.add(key, 1, withOperator);
					c.samples.putIfAbsent(key, name.replaceAll("[\t\n\r]+", " "));
				}
			}
		} finally {
			Arrays.fill(o, null);
		}
	}

	private static boolean hasLowerCase(String s) {
		for (int i = 0; i < s.length(); i++) {
			if (Character.isLowerCase(s.charAt(i))) {
				return true;
			}
		}
		return false;
	}

	private static boolean isEmpty(String s) {
		return s == null || s.isEmpty();
	}
}
