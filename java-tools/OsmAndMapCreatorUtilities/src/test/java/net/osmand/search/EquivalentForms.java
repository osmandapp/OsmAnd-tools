package net.osmand.search;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import net.osmand.binary.Abbreviations;
import net.osmand.osm.MapPoiTypes;
import net.osmand.search.SpatialSearchEquivalenceTest.SpatialResultRow;
import net.osmand.util.Algorithms;

/**
 * Equivalent query forms of a checked phrase: the same intent written another way, so the target of the phrase must
 * not be ranked lower for them. The forms are made by fixed rules from the query and the groups of the row of its
 * target ({@link SpatialResultRow}): no randomness, the same input gives the same forms in the same order.
 */
public final class EquivalentForms {

	public static final String NOISE_WORD = "Zzxq";

	public static final String ABBREVIATION = "abbreviation", 
			PERMUTATION = "permutation", NOISE = "noise", CONTEXT = "context";
	public static final List<String> ALL_KINDS = List.of(ABBREVIATION, PERMUTATION, NOISE, CONTEXT);

	private static final int MAX_PER_KIND = 3;
	private static final Set<String> DIRECTIONS = Set.of("n", "s", "e", "w", "ne", "nw", "se", "sw");
	private static final String PUNCTUATION = ",.;:!?()\"";

	/**
	 * @param control an artificial form (a meaningless word): its failure is a
	 *                signal to look at, not a bug of a class
	 * @param rule    how the form was made, e.g. "ave -> Avenue (engine)"
	 */
	public record Variant(String kind, String phrase, String rule, boolean control) {
	}

	/** street and place words the engine does not know; they are what A1 is about */
	private static final Map<String, String> EXTRA_ABBREVIATIONS = new TreeMap<>();
	static {
		EXTRA_ABBREVIATIONS.put("pl", "Place");
		EXTRA_ABBREVIATIONS.put("pkwy", "Parkway");
		EXTRA_ABBREVIATIONS.put("ct", "Court");
		EXTRA_ABBREVIATIONS.put("ter", "Terrace");
		EXTRA_ABBREVIATIONS.put("cir", "Circle");
		EXTRA_ABBREVIATIONS.put("sq", "Square");
		EXTRA_ABBREVIATIONS.put("expy", "Expressway");
		EXTRA_ABBREVIATIONS.put("tpke", "Turnpike");
		EXTRA_ABBREVIATIONS.put("mt", "Mount");
		EXTRA_ABBREVIATIONS.put("ft", "Fort");
		EXTRA_ABBREVIATIONS.put("hts", "Heights");
		EXTRA_ABBREVIATIONS.put("twp", "Township");
	}

	/**
	 * Parents of the target, taken from the objects of its search result only: a context form is made only from a
	 * parent the result has. Null parts are not added.
	 *
	 * @param city     the city of a street or of the street of a house, the city tags of a POI, the closest city of a
	 *                 district
	 * @param boundary the district a street is split by, "Manhattan" of "East 57th Street (Manhattan)"
	 * @param hasPlace the query already took a place of the target: its city is written, maybe otherwise
	 */
	public record Context(String city, String boundary, boolean hasPlace) {
		public static final Context NONE = new Context(null, null, false);
	}

	private final Map<String, String> expand = new TreeMap<>(); // "ln" -> "Lane"
	private final Map<String, String> contract = new TreeMap<>(); // "lane" -> "ln"
	private final Map<String, String> source = new TreeMap<>(); // "ln" -> "engine"

    /** @param translator POI category names and synonyms of the test ({@code null}: no category synonyms) */
	public EquivalentForms(MapPoiTypes.PoiTranslator translator) {
        Map<String, String> engineAbbr = new TreeMap<>(Abbreviations.getAbbreviations());
		engineAbbr.putAll(Abbreviations.getSearchabbreviations());
		for (Map.Entry<String, String> e : engineAbbr.entrySet()) {
			addAbbreviation(e.getKey(), e.getValue(), "engine");
		}
		for (Map.Entry<String, String> e : EXTRA_ABBREVIATIONS.entrySet()) {
			if (!expand.containsKey(e.getKey())) {
				addAbbreviation(e.getKey(), e.getValue(), "extra");
			}
		}
	}

	private void addAbbreviation(String abbr, String full, String from) {
		String[] fulls = full.split(" ");
		// only single-word pairs of latin letters: "1st" -> "First" and "о" -> "Остров" are synonyms, not abbreviations
		if (!abbr.matches("[a-z]+") || !fulls[0].matches("[A-Za-z]+") || abbr.equalsIgnoreCase(fulls[0])) {
			return;
		}
		expand.put(abbr, fulls[0]);
		source.put(abbr, from);
		String lower = fulls[0].toLowerCase(Locale.ROOT);
		// the shortest abbreviation of a word ("av" and "ave" of Avenue -> "av"), then the first one
		String current = contract.get(lower);
		if (current == null || abbr.length() < current.length()) {
			contract.put(lower, abbr);
		}
	}

	/**
	 * kinds of {@code "equivalenceKind"}: "noise", ["noise", "context"] or "noise, context"; none or empty is an empty
	 * set, an unknown kind is an error
	 */
	public static Set<String> parseKinds(Object value) {
		Set<String> kinds = new LinkedHashSet<>();
		if (value instanceof Collection<?> list) {
			for (Object o : list) {
				kinds.addAll(parseKinds(o));
			}
		} else if (value instanceof org.json.JSONArray array) {
			for (int i = 0; i < array.length(); i++) {
				kinds.addAll(parseKinds(array.get(i)));
			}
		} else if (value != null && !Algorithms.isEmpty(value.toString().trim())) {
			for (String k : value.toString().split("[,;\\s]+")) {
				if (!k.isEmpty()) {
					String kind = k.toLowerCase(Locale.ROOT);
					if (!ALL_KINDS.contains(kind)) {
						throw new IllegalArgumentException("Unknown kind '" + k + "', expected " + ALL_KINDS);
					}
					kinds.add(kind);
				}
			}
		}
		return kinds;
	}

	/**
	 * @param query     the checked phrase
	 * @param target    the row of its target, with groups (a row the engine wrote, not a cut stored one)
	 * @param kinds     kinds to make
	 * @param context   parents of the target for the context forms and the language of its map
	 */
	public List<Variant> generate(String query, SpatialResultRow target, Set<String> kinds, Context context) {
		Query q = new Query(query, target);
		List<Variant> result = new ArrayList<>();
		Set<String> seen = new LinkedHashSet<>();
		seen.add(normalize(query));
		for (String kind : ALL_KINDS) {
			if (!kinds.contains(kind)) {
				continue;
			}
			List<Variant> forms = switch (kind) {
				case ABBREVIATION -> abbreviations(q);
				case PERMUTATION -> permutations(q);
				case NOISE -> noise(q);
				case CONTEXT -> context(q, context);
				default -> List.of();
			};
			int added = 0;
			for (Variant v : forms) {
				if (added < MAX_PER_KIND && seen.add(normalize(v.phrase()))) {
					result.add(v);
					added++;
				}
			}
		}
		return result;
	}

	private List<Variant> abbreviations(Query q) {
		List<Variant> forms = new ArrayList<>();
		List<String> words = new ArrayList<>(q.words);
		List<String> rules = new ArrayList<>();
		for (int i = 0; i < q.words.size(); i++) {
			Group g = q.groupOf(i);
			if (g != null && g.group.isPoiCategory()) {
				continue;
			}
			String core = core(q.words.get(i));
			String lower = core.toLowerCase(Locale.ROOT);
			String replacement = null;
			String rule = null;
			if (expand.containsKey(lower)) {
				replacement = expand.get(lower);
				if (lower.equals("st") && g != null && !g.group.isStreet() && !g.group.isBuilding()) {
					replacement = "Saint";
				}
				rule = lower + " -> " + replacement + " (" + source.get(lower) + ")";
			} else if (contract.containsKey(lower)) {
				String abbr = contract.get(lower);
				replacement = abbr;
				rule = lower + " -> " + abbr + " (" + source.get(abbr) + ")";
			}
			if (replacement != null) {
				// directions are written in capitals: Southwest -> SW, not Sw
				boolean direction = DIRECTIONS.contains(replacement) && Character.isUpperCase(core.charAt(0));
				String word = q.words.get(i).replace(core,
						direction ? replacement.toUpperCase(Locale.ROOT) : sameCase(core, replacement));
				forms.add(new Variant(ABBREVIATION, q.with(i, word), rule, false));
				words.set(i, word);
				rules.add(rule);
			}
		}
		if (rules.size() > 1) {
			forms.add(0, new Variant(ABBREVIATION, String.join(" ", words), String.join("; ", rules), false));
		}
		return forms;
	}

	/** groups of the target keep their words together: the last one first, the first one last */
	private List<Variant> permutations(Query q) {
		List<List<String>> segments = q.segments();
		List<Variant> forms = new ArrayList<>();
		if (segments == null || segments.size() < 2) {
			return forms;
		}
		List<List<String>> lastFirst = new ArrayList<>(segments);
		lastFirst.add(0, lastFirst.remove(lastFirst.size() - 1));
		forms.add(new Variant(PERMUTATION, join(lastFirst), "last group first", false));
		List<List<String>> firstLast = new ArrayList<>(segments);
		firstLast.add(firstLast.remove(0));
		forms.add(new Variant(PERMUTATION, join(firstLast), "first group last", false));
		return forms;
	}

	private List<Variant> noise(Query q) {
		List<Variant> forms = new ArrayList<>();
		forms.add(new Variant(NOISE, stripEnd(q.text) + " " + NOISE_WORD, "noise word last", true));
		List<List<String>> segments = q.segments();
		// not between a number and its street: "2. Zzxq Sokak" is no one's typing
		if (segments != null && segments.size() >= 2 && hasLetters(segments.get(0))) {
			List<List<String>> inserted = new ArrayList<>(segments);
			inserted.add(1, List.of(NOISE_WORD));
			forms.add(new Variant(NOISE, join(inserted), "noise word after the first group", true));
		}
		return forms;
	}

	/**
	 * The query with a parent of its target the query does not name yet: its city (when the query has no place of the
	 * target, which may be the city written otherwise) or its district. A parent in another script than the query
	 * ("Kyiv" and "Київ") is not added.
	 */
	private List<Variant> context(Query q, Context ctx) {
		List<Variant> forms = new ArrayList<>();
		String city = ctx.city();
		if (city != null && !ctx.hasPlace() && !contains(q.text, city) && latin(city) == latin(q.text)) {
			forms.add(new Variant(CONTEXT, stripEnd(q.text) + ", " + city, "+ city", false));
		}
		String boundary = ctx.boundary();
		if (boundary != null && !contains(q.text, boundary) && !boundary.equalsIgnoreCase(city)
				&& latin(boundary) == latin(q.text)) {
			forms.add(new Variant(CONTEXT, stripEnd(q.text) + ", " + boundary, "+ parent boundary", false));
		}
		return forms;
	}

	private static String sameCase(String original, String replacement) {
		if (original.length() > 1 && original.equals(original.toUpperCase(Locale.ROOT))
				&& replacement.length() <= original.length()) {
			return replacement.toUpperCase(Locale.ROOT);
		}
		if (!original.isEmpty() && Character.isUpperCase(original.charAt(0))) {
			return Algorithms.capitalizeFirstLetter(replacement.toLowerCase(Locale.ROOT));
		}
		return replacement.toLowerCase(Locale.ROOT);
	}

	/** the word without the punctuation around it: "Ln," -> "Ln" */
	static String core(String word) {
		int s = 0;
		int e = word.length();
		while (s < e && PUNCTUATION.indexOf(word.charAt(s)) >= 0) {
			s++;
		}
		while (e > s && PUNCTUATION.indexOf(word.charAt(e - 1)) >= 0) {
			e--;
		}
		return word.substring(s, e);
	}

	private static String normalize(String s) {
		StringBuilder b = new StringBuilder();
		for (String w : s.trim().split("\\s+")) {
			String c = core(w).toLowerCase(Locale.ROOT);
			if (!c.isEmpty()) {
				b.append(b.isEmpty() ? "" : " ").append(c);
			}
		}
		return b.toString();
	}

	private static boolean hasLetters(List<String> words) {
		for (String w : words) {
			if (core(w).codePoints().filter(Character::isLetter).count() >= 2) {
				return true;
			}
		}
		return false;
	}

	/** all letters of the text are latin ones: "Kyiv 1" is not continued with "Київ" */
	static boolean latin(String text) {
		return text.codePoints().filter(Character::isLetter)
				.allMatch(c -> Character.UnicodeScript.of(c) == Character.UnicodeScript.LATIN);
	}

	/** "Wilkes Barre" has "Wilkes-Barre" */
	private static boolean contains(String text, String part) {
		return (" " + normalize(text.replace('-', ' ')) + " ").contains(" " + normalize(part.replace('-', ' ')) + " ");
	}

	/** the text without a separator at its end; a dot stays: "fen." is a prefix of a word to the engine */
	private static String stripEnd(String text) {
		String t = text.trim();
		while (!t.isEmpty() && ",;".indexOf(t.charAt(t.length() - 1)) >= 0) {
			t = t.substring(0, t.length() - 1).trim();
		}
		return t;
	}

	private static String join(List<?> parts) {
		List<String> words = new ArrayList<>();
		for (Object p : parts) {
			if (p instanceof List<?> list) {
				for (Object w : list) {
					words.add(w.toString());
				}
			} else {
				words.add(p.toString());
			}
		}
		// commas stay between the groups where the user put them, never at the end
		for (int i = 0; i < words.size(); i++) {
			words.set(i, i == words.size() - 1 ? stripEnd(words.get(i)) : words.get(i));
		}
		return String.join(" ", words).replaceAll("\\s+", " ").trim();
	}

	private static final class Group {
		final SpatialResultRow.Group group;
		final List<Integer> indexes = new ArrayList<>();

		Group(SpatialResultRow.Group group) {
			this.group = group;
		}

		List<String> words(Query q) {
			List<String> w = new ArrayList<>();
			for (int i : indexes) {
				w.add(core(q.words.get(i)));
			}
			return w;
		}

		boolean contiguous() {
			for (int i = 1; i < indexes.size(); i++) {
				if (indexes.get(i) != indexes.get(i - 1) + 1) {
					return false;
				}
			}
			return !indexes.isEmpty();
		}
	}

	/** the query as words, each word assigned to the group of the target row that took it */
	private static final class Query {
		final String text;
		final List<String> words;
		final List<Group> groups = new ArrayList<>();
		final Group[] owner;

		Query(String text, SpatialResultRow target) {
			this.text = text;
			this.words = new ArrayList<>(Arrays.asList(text.trim().split("\\s+")));
			this.owner = new Group[words.size()];
			for (SpatialResultRow.Group rowGroup : target.getGroups()) {
				Group g = new Group(rowGroup);
				int from = 0;
				for (String w : rowGroup.words()) {
					for (int i = from; i < words.size(); i++) {
						if (owner[i] == null && core(words.get(i)).equalsIgnoreCase(w)) {
							owner[i] = g;
							g.indexes.add(i);
							from = i + 1;
							break;
						}
					}
				}
				g.indexes.sort(Integer::compare);
				groups.add(g);
			}
			groups.sort((a, b) -> Integer.compare(a.indexes.isEmpty() ? Integer.MAX_VALUE : a.indexes.get(0),
					b.indexes.isEmpty() ? Integer.MAX_VALUE : b.indexes.get(0)));
		}

		Group groupOf(int i) {
			return owner[i];
		}

		boolean hasWord(String word) {
			for (String w : words) {
				if (core(w).equalsIgnoreCase(word)) {
					return true;
				}
			}
			return false;
		}

		/**
		 * words of the POI category: a POI_TYPE group, or the words of a POI that are its type
		 * ({@code 'fast food' [POI Fast food] 'Dunkin''})
		 */
		Group category() {
			for (Group g : groups) {
				if (!g.indexes.isEmpty() && (g.group.isPoiCategory() || g.group.isPoi()
						&& normalize(String.join(" ", g.words(this))).equals(normalize(g.group.type().substring(3))))) {
					return g;
				}
			}
			return null;
		}

		/**
		 * the query cut into the groups of the target, words of no group (commas, "and") staying with the group
		 * before them; null when a group is not contiguous or no group took a word
		 */
		List<List<String>> segments() {
			List<List<String>> segments = new ArrayList<>();
			Group current = null;
			List<String> segment = null;
			for (int i = 0; i < words.size(); i++) {
				Group g = owner[i];
				if (g != null && g != current) {
					if (!g.contiguous()) {
						return null;
					}
					current = g;
					segment = new ArrayList<>();
					segments.add(segment);
				}
				if (segment == null) {
					segment = new ArrayList<>();
					segments.add(segment);
				}
				segment.add(words.get(i));
			}
			int assigned = 0;
			for (Group g : groups) {
				assigned += g.indexes.isEmpty() ? 0 : 1;
			}
			return assigned == 0 ? null : segments;
		}

		String with(int i, String word) {
			List<String> w = new ArrayList<>(words);
			w.set(i, word);
			return String.join(" ", w);
		}

		String replace(Group g, String phrase) {
			List<String> w = new ArrayList<>();
			for (int i = 0; i < words.size(); i++) {
				if (owner[i] != g) {
					w.add(words.get(i));
				} else if (i == g.indexes.get(0)) {
					w.add(phrase);
				}
			}
			return String.join(" ", w);
		}
	}
}
