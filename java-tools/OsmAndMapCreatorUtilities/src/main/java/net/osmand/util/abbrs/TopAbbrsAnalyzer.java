package net.osmand.util.abbrs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import net.osmand.obf.preparation.CommonWordsMultiIndex;
import net.osmand.obf.preparation.PoiNamesExtractor;
import net.osmand.osm.MapPoiTypes;
import net.osmand.search.rules.SearchModLocales;
import net.osmand.search.rules.SearchModRules;
import net.osmand.util.MapUtils;
import net.osmand.util.abbrs.AbbrText.Word;
import net.osmand.util.abbrs.PoiExtraction.ExtractedEntity;
import net.osmand.util.abbrs.PoiExtraction.ExtractedPoi;

/**
 * Builds {@code top_abbrs.csv} from the POI extractions of the generation (top-org-abbrs-spec.md 5): abbreviations of
 * words of names (H1: two spellings of one name) and of organizations (H2 + H3: a word shared by the names of a group
 * {@code operator} / {@code network}, the initials of the value), thresholds in unique entities, filters of
 * {@code top-abbrs-rules.xml}. Writes {@code top_abbrs.csv}, {@code top_abbrs.html}, {@code candidates.tsv},
 * {@code regions.csv} and {@code top_abbrs.manifest.json}.
 * <p>
 * Not in this version: H5 ({@code <propagate>}), the difference with the last month, the sample for labeling,
 * automatic publication, the limit per map.
 * <p>
 * Usage: {@code top-abbrs-analyzer <extraction dir or file>... --out=<dir> [--rules=top-abbrs-rules.xml]
 * [--brands=brands.lst]}
 */
public class TopAbbrsAnalyzer {

	public static final String RULES_RESOURCE = "top-abbrs-rules.xml";
	static final Set<String> PLACE_SUBTYPES = Set.of("state", "province", "city", "town", "village", "hamlet",
			"borough", "suburb", "county");
	private static final int MAX_EXAMPLES = 6;
	private static final int MAX_NEAR_BUCKET = 400;
	private static final double WORD_TYPES_COVERAGE = 0.9;
	private static final int WORD_MAX_TYPES = 5;
	private static final int COMPOUND_MIN_FREQ = 20;

	/** a pair of H1: (locale, abbr, full) */
	public static class H1Candidate {
		public final String locale;
		public final String abbr;
		public final List<String> full;
		public final Set<String> entities = new LinkedHashSet<>();
		final Set<Long> tiles = new HashSet<>();
		public int same, near, span, pairs;
		final Map<String, Integer> spellings = new HashMap<>();
		int caseCounted, upper, capital;
		final Map<String, Integer> types = new HashMap<>();
		public final List<String> examples = new ArrayList<>();
		/** supporting entities by map */
		public final Map<String, Integer> perMap = new TreeMap<>();
		/** the noise filter that drops the pair, kept with {@link TopAbbrsAnalyzer#keepNoise} */
		public String noise;
		public boolean agreed;
		public String status = "";
		public String reason = "";

		H1Candidate(String locale, String abbr, List<String> full) {
			this.locale = locale;
			this.abbr = abbr;
			this.full = full;
		}

		public String fullText() {
			return String.join(" ", full);
		}

		public boolean isWord() {
			return full.size() == 1;
		}

		public String key() {
			return locale + "|" + abbr + "|" + fullText();
		}

		Set<String> sources() {
			Set<String> s = new LinkedHashSet<>();
			if (same > 0) {
				s.add(span > 0 ? "h1span" : "h1same");
			}
			if (near > 0) {
				s.add("h1near");
			}
			return s;
		}
	}

	/** a word of the names of a group operator / network (H2) with its check of initials (H3) */
	public static class GroupCandidate {
		public final String tag;
		public final String value;
		public final String locale;
		public final List<ExtractedEntity> members;
		public final String abbr;
		public int withToken;
		public double share;
		public String h3 = "";
		public List<String> full = List.of();
		/** H3 without the head word condition */
		public String h3Free = "";
		public List<String> fullFree = List.of();
		final Map<String, Integer> spellings = new HashMap<>();
		int caseCounted, upper;
		public final List<String> examples = new ArrayList<>();
		/** the map of most members */
		public String map;
		/** the noise filter of the word, kept with {@link TopAbbrsAnalyzer#keepNoise} */
		public String noise;
		public boolean agreed;
		public String status = "";
		public String reason = "";

		GroupCandidate(String tag, String value, String locale, List<ExtractedEntity> members, String abbr) {
			this.tag = tag;
			this.value = value;
			this.locale = locale;
			this.members = members;
			this.abbr = abbr;
		}

		public String key() {
			return locale + "|" + abbr + "|" + String.join(" ", full);
		}
	}

	final PoiExtraction extraction;
	final TopAbbrsRules rules;
	final SearchModLocales locales;
	final CommonWordsMultiIndex classes;
	final TopAbbrPredicate predicate;
	final MapPoiTypes poiTypes;
	final Set<String> brands = new HashSet<>();

	final Map<String, H1Candidate> h1 = new LinkedHashMap<>();
	final List<GroupCandidate> groups = new ArrayList<>();
	final List<TopAbbrRow> rows = new ArrayList<>();
	final List<String[]> droppedRows = new ArrayList<>();
	final Map<String, Integer> noise = new TreeMap<>();
	final Map<TopAbbrRow, List<String>> gainExamples = new HashMap<>();
	/** keep the pairs of the noise filters as candidates with {@code noise} instead of dropping them */
	public boolean keepNoise;
	private final Map<String, Integer> vocab = new HashMap<>();
	private final Map<String, Set<List<String>>> placeNames = new HashMap<>();

	public TopAbbrsAnalyzer(PoiExtraction extraction, TopAbbrsRules rules, SearchModLocales locales,
			CommonWordsMultiIndex classes, RegionTree regions, MapPoiTypes poiTypes, Collection<String> brandValues) {
		this.extraction = extraction;
		this.rules = rules;
		this.locales = locales;
		this.classes = classes;
		this.poiTypes = poiTypes;
		this.predicate = new TopAbbrPredicate(locales, regions);
		for (String b : brandValues) {
			brands.add(AbbrText.normalize(b));
		}
	}

	String locale(ExtractedEntity e) {
		return predicate.mapLocale(e.map);
	}

	String statsGroup(ExtractedEntity e) {
		return locales.groupForMap(e.map);
	}

	Set<String> stop(String locale) {
		return rules.words(locale, "stop");
	}

	public List<TopAbbrRow> run() {
		prepare();
		mineH1();
		mineGroups();
		agree();
		acceptH1();
		acceptGroups();
		buildRows();
		return rows;
	}

	// ---------- vocabulary, places, brands ----------

	private void prepare() {
		for (ExtractedEntity e : extraction.entities()) {
			Set<String> ws = new HashSet<>();
			for (String v : AbbrText.values(e.tag("name"))) {
				ws.addAll(AbbrText.words(v));
			}
			for (String w : ws) {
				vocab.merge(w, 1, Integer::sum);
			}
			if (rules.boolAttr(locale(e), "groups/exclude", "brandValues")) {
				for (String b : AbbrText.values(e.tag("brand"))) {
					brands.add(AbbrText.normalize(b));
				}
			}
			for (ExtractedPoi p : e.pois) {
				if (PLACE_SUBTYPES.contains(p.subtype)) {
					for (String v : AbbrText.values(e.tag("name"))) {
						List<String> w = AbbrText.words(v);
						if (!w.isEmpty()) {
							placeNames.computeIfAbsent(locale(e), k -> new HashSet<>()).add(w);
						}
					}
					break;
				}
			}
		}
	}

	// ---------- H1 ----------

	private void mineH1() {
		for (ExtractedEntity e : extraction.entities()) {
			String loc = locale(e);
			if (!rules.boolAttr(loc, "pairs/source.same", "enabled")) {
				continue;
			}
			String lang = locales.language(loc);
			List<String> tags = new ArrayList<>();
			for (String t : rules.listAttr(loc, "pairs/source.same", "tags")) {
				if (t.contains("{lang}")) {
					if (!lang.isEmpty()) {
						tags.add(t.replace("{lang}", lang));
					}
				} else {
					tags.add(t);
				}
			}
			for (String name : AbbrText.values(e.tag("name"))) {
				List<Word> nw = AbbrText.spelledWords(name);
				if (nw.isEmpty()) {
					continue;
				}
				for (String t : tags) {
					for (String v : AbbrText.values(e.tag(t))) {
						List<Word> cw = AbbrText.spelledWords(v);
						AbbrMatcher.Pair d = pair(nw, cw, loc, true);
						if (d != null) {
							addPair(loc, d, "same", e, d.shortInFirst ? nw : cw, name + " | " + v);
						}
					}
				}
			}
		}
		mineNear();
	}

	private AbbrMatcher.Pair pair(List<Word> a, List<Word> b, String loc, boolean same) {
		List<String> wa = words(a), wb = words(b);
		Set<String> stop = stop(loc);
		int maxChunk = rules.intAttr(loc, "pairs/abbr", "maxChunk");
		AbbrMatcher.Pair d = AbbrMatcher.diffPair(wa, wb, rules.intAttr(loc, "pairs/long", "maxWords"), maxChunk, stop);
		if (d == null && same && rules.boolAttr(loc, "pairs/source.same", "span")) {
			d = AbbrMatcher.spanPair(wa, wb, rules.intAttr(loc, "pairs/source.same", "spanMaxWords"),
					rules.intAttr(loc, "pairs/abbr", "minLen"), rules.intAttr(loc, "pairs/abbr", "maxLen"), maxChunk,
					stop);
		}
		return d;
	}

	private static List<String> words(List<Word> ws) {
		List<String> l = new ArrayList<>(ws.size());
		for (Word w : ws) {
			l.add(w.word);
		}
		return l;
	}

	private void mineNear() {
		Map<String, List<Object[]>> buckets = new HashMap<>();
		Map<String, Double> cells = new HashMap<>();
		for (ExtractedEntity e : extraction.entities()) {
			String loc = locale(e);
			if (!rules.boolAttr(loc, "pairs/source.near", "enabled")) {
				continue;
			}
			String name = e.tag("name");
			if (name == null || name.indexOf(';') >= 0) {
				continue;
			}
			List<Word> nw = AbbrText.spelledWords(name);
			if (nw.size() < 2) {
				continue;
			}
			double cell = cells.computeIfAbsent(loc,
					l -> Math.max(0.001, rules.intAttr(l, "pairs/source.near", "radiusM") / 111_000.0));
			ExtractedPoi p = e.mainPoi();
			String first = rules.boolAttr(loc, "pairs/source.near", "sameFirstWord") ? nw.get(0).word : "";
			String key = loc + "|" + first + "|" + (int) Math.floor(p.lat / cell) + "|" + (int) Math.floor(p.lon / cell);
			buckets.computeIfAbsent(key, k -> new ArrayList<>()).add(new Object[] { e, nw, name });
		}
		Set<String> seen = new HashSet<>();
		for (Map.Entry<String, List<Object[]>> b : buckets.entrySet()) {
			String[] k = b.getKey().split("\\|", -1);
			String loc = k[0];
			int cy = Integer.parseInt(k[2]), cx = Integer.parseInt(k[3]);
			List<Object[]> others = new ArrayList<>();
			for (int dy = -1; dy <= 1; dy++) {
				for (int dx = -1; dx <= 1; dx++) {
					List<Object[]> o = buckets.get(loc + "|" + k[1] + "|" + (cy + dy) + "|" + (cx + dx));
					if (o != null) {
						others.addAll(o);
					}
				}
			}
			if (others.size() > MAX_NEAR_BUCKET) {
				noise.merge("near: crowded cell skipped", 1, Integer::sum);
				continue;
			}
			int radius = rules.intAttr(loc, "pairs/source.near", "radiusM");
			int maxShort = rules.intAttr(loc, "pairs/source.near", "maxShortLen");
			for (Object[] o1 : b.getValue()) {
				ExtractedEntity e1 = (ExtractedEntity) o1[0];
				@SuppressWarnings("unchecked")
				List<Word> n1 = (List<Word>) o1[1];
				for (Object[] o2 : others) {
					ExtractedEntity e2 = (ExtractedEntity) o2[0];
					@SuppressWarnings("unchecked")
					List<Word> n2 = (List<Word>) o2[1];
					if (e1 == e2 || Math.abs(n1.size() - n2.size()) > 2 || words(n1).equals(words(n2))) {
						continue;
					}
					String pk = e1.key().compareTo(e2.key()) < 0 ? e1.key() + "|" + e2.key() : e2.key() + "|" + e1.key();
					if (seen.contains(pk)) {
						continue;
					}
					AbbrMatcher.Pair d = pair(n1, n2, loc, false);
					if (d == null || d.abbr.length() > maxShort) {
						continue;
					}
					ExtractedPoi p1 = e1.mainPoi(), p2 = e2.mainPoi();
					if (MapUtils.getDistance(p1.lat, p1.lon, p2.lat, p2.lon) > radius) {
						continue;
					}
					seen.add(pk);
					// the object with the long form gains the short form
					ExtractedEntity gain = d.shortInFirst ? e2 : e1;
					addPair(loc, d, "near", gain, d.shortInFirst ? n1 : n2, o1[2] + " | " + o2[2]);
				}
			}
		}
	}

	private void addPair(String loc, AbbrMatcher.Pair d, String source, ExtractedEntity gain, List<Word> shortName,
			String example) {
		String reason = noiseReason(loc, d.abbr, d.full, statsGroup(gain));
		if (reason != null) {
			noise.merge(reason, 1, Integer::sum);
			if (!keepNoise) {
				return;
			}
		}
		H1Candidate c = h1.computeIfAbsent(loc + "|" + d.abbr + "|" + d.fullText(),
				k -> new H1Candidate(loc, d.abbr, d.full));
		if (reason != null && c.noise == null) {
			c.noise = reason;
		}
		c.pairs++;
		if (source.equals("same")) {
			c.same++;
			if (d.leftover > 0) {
				c.span++;
			}
		} else {
			c.near++;
		}
		if (c.entities.add(gain.key())) {
			c.perMap.merge(gain.map, 1, Integer::sum);
			c.tiles.add(gain.tile(rules.intAttr(loc, "pairs/accept", "tileZoom")));
			for (ExtractedPoi p : gain.pois) {
				c.types.merge(p.subtype, 1, Integer::sum);
			}
			if (c.examples.size() < MAX_EXAMPLES) {
				c.examples.add(example + " (" + gain.key() + ")");
			}
		}
		String spelling = null;
		for (Word w : shortName) {
			if (w.word.equals(d.abbr)) {
				spelling = w.original;
				break;
			}
		}
		if (spelling != null) {
			c.spellings.merge(spelling, 1, Integer::sum);
			if (!(rules.boolAttr(loc, "case", "skipAllCapsNames") && AbbrText.isAllCapsName(shortName))) {
				c.caseCounted++;
				if (AbbrText.isAllUpper(spelling)) {
					c.upper++;
				}
				if (AbbrText.isCapitalized(spelling)) {
					c.capital++;
				}
			}
		}
	}

	/** unglued form, service word, roman numerals, a word of a compound, length and digits */
	private String noiseReason(String loc, String abbr, List<String> full, String group) {
		if (abbr.length() < rules.intAttr(loc, "pairs/abbr", "minLen")
				|| abbr.length() > rules.intAttr(loc, "pairs/abbr", "maxLen")) {
			return "length of abbr";
		}
		for (int i = 0; i < abbr.length(); i++) {
			if (Character.isDigit(abbr.charAt(i))) {
				return "digits";
			}
		}
		if (rules.boolAttr(loc, "pairs/noise", "unglue") && String.join("", full).equals(abbr)) {
			return "unglued";
		}
		if (rules.boolAttr(loc, "pairs/noise", "serviceWords")
				&& (stop(loc).contains(abbr) || classes.wordClass(group, abbr) == 1)) {
			return "service word";
		}
		if (rules.boolAttr(loc, "pairs/noise", "roman") && AbbrMatcher.isRoman(abbr)) {
			boolean all = true;
			for (String w : full) {
				all &= AbbrMatcher.isRoman(w);
			}
			if (all) {
				return "roman numerals";
			}
		}
		if (rules.boolAttr(loc, "pairs/noise", "compound") && full.size() == 1) {
			String lg = full.get(0);
			String rest = lg.startsWith(abbr) ? lg.substring(abbr.length())
					: lg.endsWith(abbr) ? lg.substring(0, lg.length() - abbr.length()) : null;
			if (rest != null && vocab.getOrDefault(abbr, 0) >= COMPOUND_MIN_FREQ) {
				List<String> rests = new ArrayList<>();
				rests.add(rest);
				for (String link : rules.listAttr(loc, "pairs/noise", "compoundLinks")) {
					if (rest.startsWith(link)) {
						rests.add(rest.substring(link.length()));
					}
				}
				for (String r : rests) {
					if (r.length() >= 3 && vocab.getOrDefault(r, 0) >= COMPOUND_MIN_FREQ) {
						return "part of a compound";
					}
				}
			}
		}
		return null;
	}

	// ---------- H2 + H3 ----------

	private void mineGroups() {
		Map<String, List<ExtractedEntity>> byValue = new TreeMap<>();
		for (ExtractedEntity e : extraction.entities()) {
			String loc = locale(e);
			for (String tag : List.of(TopAbbrRow.BIND_OPERATOR, TopAbbrRow.BIND_NETWORK)) {
				if (!rules.boolAttr(loc, "groups/group." + tag, "enabled")) {
					continue;
				}
				Set<String> values = new LinkedHashSet<>();
				for (String v : AbbrText.values(e.tag(tag))) {
					String nv = AbbrText.normalize(v);
					if (!nv.isEmpty()) {
						values.add(nv);
					}
				}
				for (String nv : values) {
					byValue.computeIfAbsent(tag + "|" + nv, k -> new ArrayList<>()).add(e);
				}
			}
		}
		for (Map.Entry<String, List<ExtractedEntity>> g : byValue.entrySet()) {
			List<ExtractedEntity> members = g.getValue();
			int i = g.getKey().indexOf('|');
			String tag = g.getKey().substring(0, i);
			String value = g.getKey().substring(i + 1);
			Map<String, Integer> locs = new TreeMap<>();
			Map<String, Integer> statGroups = new TreeMap<>();
			for (ExtractedEntity e : members) {
				locs.merge(locale(e), 1, Integer::sum);
				String sg = statsGroup(e);
				if (sg != null) {
					statGroups.merge(sg, 1, Integer::sum);
				}
			}
			String loc = top(locs);
			if (members.size() < rules.intAttr(loc, "groups/accept", "minGroup")) {
				continue;
			}
			Map<String, Integer> maps = new TreeMap<>();
			for (ExtractedEntity e : members) {
				maps.merge(e.map, 1, Integer::sum);
			}
			String groupMap = top(maps);
			String statGroup = statGroups.isEmpty() ? null : top(statGroups);
			List<String> valueWords = Arrays.asList(value.split(" "));
			Map<String, GroupCandidate> tokens = new LinkedHashMap<>();
			for (ExtractedEntity e : members) {
				Map<String, Word> ws = new LinkedHashMap<>();
				List<Word> nameWords = new ArrayList<>();
				for (String t : List.of("name", "short_name", "alt_name", "official_name")) {
					for (String v : AbbrText.values(e.tag(t))) {
						List<Word> sw = AbbrText.spelledWords(v);
						if (t.equals("name")) {
							nameWords = sw;
						}
						for (Word w : sw) {
							ws.putIfAbsent(w.word, w);
						}
					}
				}
				for (Word w : ws.values()) {
					String why = tokenNoise(w.word, valueWords, loc, statGroup);
					if (why != null && (why.equals(SKIP) || !keepNoise)) {
						continue;
					}
					GroupCandidate c = tokens.computeIfAbsent(w.word,
							k -> new GroupCandidate(tag, value, loc, members, k));
					c.noise = why;
					c.withToken++;
					c.spellings.merge(w.original, 1, Integer::sum);
					if (!(rules.boolAttr(loc, "case", "skipAllCapsNames") && AbbrText.isAllCapsName(nameWords))) {
						c.caseCounted++;
						if (AbbrText.isAllUpper(w.original)) {
							c.upper++;
						}
					}
					if (c.examples.size() < MAX_EXAMPLES) {
						c.examples.add(e.tag("name") == null ? e.key() : e.tag("name"));
					}
				}
			}
			double minShare = rules.doubleAttr(loc, "groups/accept", "minShare")
					* rules.doubleAttr(loc, "agree", "shareFactor");
			List<String> stripped = stripTail(valueWords, loc);
			boolean head = rules.boolAttr(loc, "groups/accept", "headWord");
			for (GroupCandidate c : tokens.values()) {
				c.share = c.withToken / (double) members.size();
				if (c.share < minShare) {
					continue;
				}
				// the whole value first: "city university of new york" is CUNY, the place is a part of the name
				for (List<String> v : stripped.size() < valueWords.size() ? List.of(valueWords, stripped)
						: List.of(valueWords)) {
					Object[] h3 = AbbrMatcher.spanAcronym(c.abbr, v, rules.intAttr(loc, "pairs/abbr", "maxLen"),
							rules.intAttr(loc, "pairs/abbr", "maxChunk"), stop(loc), head ? v.size() : -1);
					if (h3 != null && (c.h3.isEmpty() || h3[0] == AbbrMatcher.ACRONYM)) {
						c.h3 = (String) h3[0];
						c.full = List.copyOf(v.subList((int) h3[1], (int) h3[2]));
					}
					if (AbbrMatcher.ACRONYM.equals(c.h3)) {
						break;
					}
				}
				// for the report: the initials of any run, not only the one up to the head word
				Object[] free = AbbrMatcher.spanAcronym(c.abbr, valueWords, rules.intAttr(loc, "pairs/abbr", "maxLen"),
						rules.intAttr(loc, "pairs/abbr", "maxChunk"), stop(loc), -1);
				if (free != null) {
					c.h3Free = (String) free[0];
					c.fullFree = List.copyOf(valueWords.subList((int) free[1], (int) free[2]));
				}
				c.map = groupMap;
				groups.add(c);
			}
		}
	}

	private static final String SKIP = "skip";

	/** null for a word that can be an abbreviation; {@link #SKIP} never; else the noise filter that drops it */
	private String tokenNoise(String w, List<String> valueWords, String loc, String statGroup) {
		if (valueWords.contains(w) || w.length() < rules.intAttr(loc, "pairs/abbr", "minLen")
				|| w.length() > rules.intAttr(loc, "pairs/abbr", "maxLen") || !AbbrText.hasLetters(w)) {
			return SKIP;
		}
		for (int i = 0; i < w.length(); i++) {
			if (Character.isDigit(w.charAt(i))) {
				return SKIP;
			}
		}
		if (stop(loc).contains(w)) {
			return "service word";
		}
		int cls = classes.wordClass(statGroup, w);
		return cls == 1 ? "service word" : cls == 2 ? "frequent word" : null;
	}

	/** the value without legal forms and places at its end ("bahn ag" -> "bahn") */
	List<String> stripTail(List<String> value, String loc) {
		Set<String> tails = rules.listAttr(loc, "groups/accept", "stripTail");
		Set<String> legal = tails.contains("legal") ? rules.words(loc, "legal") : Set.of();
		Set<String> placeWords = tails.contains("place") ? rules.words(loc, "places") : Set.of();
		Set<List<String>> places = tails.contains("place") ? placeNames.getOrDefault(loc, Set.of()) : Set.of();
		List<String> v = new ArrayList<>(value);
		boolean changed = true;
		while (changed && v.size() > 2) {
			changed = false;
			String last = v.get(v.size() - 1);
			if (legal.contains(last) || placeWords.contains(last) || stop(loc).contains(last)) {
				v.remove(v.size() - 1);
				changed = true;
				continue;
			}
			for (List<String> p : places) {
				if (p.size() < v.size() - 1 && v.subList(v.size() - p.size(), v.size()).equals(p)) {
					v = new ArrayList<>(v.subList(0, v.size() - p.size()));
					changed = true;
					break;
				}
			}
		}
		return v;
	}

	// ---------- agreement and thresholds ----------

	private void agree() {
		Set<String> groupKeys = new HashSet<>();
		for (GroupCandidate g : groups) {
			if (AbbrMatcher.ACRONYM.equals(g.h3)) {
				groupKeys.add(g.key());
			}
		}
		for (H1Candidate c : h1.values()) {
			c.agreed = groupKeys.contains(c.key());
		}
		for (GroupCandidate g : groups) {
			g.agreed = AbbrMatcher.ACRONYM.equals(g.h3) && h1.containsKey(g.key());
		}
	}

	private void acceptH1() {
		for (H1Candidate c : h1.values()) {
			String loc = c.locale;
			double f = c.agreed ? rules.doubleAttr(loc, "agree", "supportFactor") : 1;
			int minSupport = (int) Math.ceil(rules.intAttr(loc, "pairs/accept", "minSupport") * f);
			int minTiles = c.agreed ? 1 : rules.intAttr(loc, "pairs/accept", "minTiles");
			if (c.noise != null) {
				c.status = "noise";
				c.reason = c.noise;
			} else if (c.entities.size() < minSupport) {
				reject(c, "support " + c.entities.size() + " < " + minSupport);
			} else if (c.tiles.size() < minTiles) {
				reject(c, "tiles " + c.tiles.size() + " < " + minTiles);
			} else if (brands.contains(c.abbr) || brands.contains(c.fullText())) {
				reject(c, "brand");
			} else if (!caseAccepted(loc, c.isWord(), firstSpelling(c.spellings), c.caseCounted,
					c.isWord() ? c.capital : c.upper)) {
				reject(c, "case " + (c.isWord() ? c.capital : c.upper) + "/" + c.caseCounted);
			} else if (c.isWord()) {
				c.status = "accepted";
			} else if (c.agreed) {
				c.status = "agreed";
			} else {
				c.status = "phrase";
				c.reason = "phrase without H2+H3: <include> only";
			}
		}
	}

	private static void reject(H1Candidate c, String reason) {
		c.status = "rejected";
		c.reason = reason;
	}

	private boolean caseAccepted(String loc, boolean word, String spelling, int counted, int good) {
		String mode = rules.attr(loc, "case", word ? "word" : "org");
		if (mode.equals("none") || spelling == null || !AbbrText.hasCase(spelling)) {
			return true;
		}
		// every spelling sat in an all-caps name: nothing tells the case
		return counted == 0 || good >= rules.doubleAttr(loc, "case", "share") * counted;
	}

	private void acceptGroups() {
		for (GroupCandidate g : groups) {
			String loc = g.locale;
			double f = g.agreed ? rules.doubleAttr(loc, "agree", "shareFactor") : 1;
			double minShare = rules.doubleAttr(loc, "groups/accept", "minShare") * f;
			String acronym = rules.attr(loc, "groups/accept", "acronym");
			if (g.noise != null) {
				g.status = "noise";
				g.reason = g.noise;
			} else if (g.share < minShare) {
				g.status = "rejected";
				g.reason = String.format(Locale.US, "share %.2f < %.2f", g.share, minShare);
			} else if (g.abbr.length() == 2 && g.share < rules.doubleAttr(loc, "groups/accept", "min2LetterShare")) {
				g.status = "rejected";
				g.reason = String.format(Locale.US, "2 letters, share %.2f", g.share);
			} else if (AbbrMatcher.GLUED.equals(g.h3)) {
				g.status = "glued";
				g.reason = "glued words of the value";
			} else if (!AbbrMatcher.ACRONYM.equals(g.h3) && acronym.equals("required")) {
				g.status = "rejected";
				g.reason = "not an acronym of the value (H3)";
			} else if (brands.contains(g.abbr) || brands.contains(String.join(" ", g.full))) {
				g.status = "brand";
				g.reason = "brand";
			} else if (!caseAccepted(loc, false, firstSpelling(g.spellings), g.caseCounted, g.upper)) {
				g.status = "rejected";
				g.reason = "case " + g.upper + "/" + g.caseCounted;
			} else {
				g.status = "accepted";
			}
		}
	}

	static String firstSpelling(Map<String, Integer> spellings) {
		String best = null;
		int n = -1;
		for (Map.Entry<String, Integer> s : new TreeMap<>(spellings).entrySet()) {
			if (s.getValue() > n) {
				best = s.getKey();
				n = s.getValue();
			}
		}
		return best;
	}

	private static String top(Map<String, Integer> counts) {
		String best = null;
		int n = -1;
		for (Map.Entry<String, Integer> s : counts.entrySet()) {
			if (s.getValue() > n) {
				best = s.getKey();
				n = s.getValue();
			}
		}
		return best;
	}

	// ---------- rows ----------

	private void buildRows() {
		List<TopAbbrRow> found = new ArrayList<>();
		for (GroupCandidate g : groups) {
			if (g.status.equals("accepted")) {
				found.add(orgRow(g));
			}
		}
		found.addAll(wordRows());
		buildRows(found);
	}

	TopAbbrRow orgRow(GroupCandidate g) {
			TopAbbrRow r = new TopAbbrRow();
			r.kind = TopAbbrRow.KIND_ORG;
			r.locale = g.locale;
			r.abbr = canonical(g.spellings, g.abbr, true);
			r.full = String.join(" ", g.full);
			r.bind = g.tag;
			r.bindValue = g.value;
			for (ExtractedEntity e : g.members) {
				for (ExtractedPoi p : e.pois) {
					r.types.add(p.subtype);
				}
			}
			r.groupSize = g.members.size();
			r.supportObjects = g.withToken;
			r.sources.add("h2");
			r.sources.add("h3");
			H1Candidate c = h1.get(g.key());
			if (g.agreed && c != null) {
				r.supportPairs = c.pairs;
				r.sources.addAll(c.sources());
			}
			return r;
	}

	private void buildRows(List<TopAbbrRow> found) {
		// manual edits: exclude vetoes found rows, include adds
		for (TopAbbrRow r : found) {
			TopAbbrsRules.ManualRow ex = excludedBy(r);
			if (ex != null) {
				droppedRows.add(new String[] { r.toString(), "exclude " + ex });
			} else {
				rows.add(r);
			}
		}
		for (TopAbbrsRules.ManualRow in : rules.includes) {
			rows.add(in.toRow());
		}
		computeGain();
		dropConflicts();
		rows.sort(Comparator.comparing((TopAbbrRow r) -> r.kind).thenComparing(r -> r.locale)
				.thenComparing(TopAbbrRow::abbrNormalized).thenComparing(r -> r.full).thenComparing(r -> r.bind)
				.thenComparing(r -> r.bindValue));
		int planet = rules.intAttr("", "limit", "planet");
		if (rows.size() > planet) {
			List<TopAbbrRow> byGain = new ArrayList<>(rows);
			byGain.sort(Comparator.comparingInt((TopAbbrRow r) -> -nz(r.gainObjects)).thenComparing(TopAbbrRow::key));
			for (TopAbbrRow r : byGain.subList(planet, byGain.size())) {
				droppedRows.add(new String[] { r.toString(), "limit planet " + planet });
			}
			rows.retainAll(new HashSet<>(byGain.subList(0, planet)));
		}
	}

	private TopAbbrsRules.ManualRow excludedBy(TopAbbrRow r) {
		for (TopAbbrsRules.ManualRow ex : rules.excludes) {
			if (ex.matches(r)) {
				return ex;
			}
		}
		return null;
	}

	/** kind=word: a language row when the pair is confirmed in wordLocales locales of the language, else per locale */
	private List<TopAbbrRow> wordRows() {
		Map<String, List<H1Candidate>> byLanguage = new TreeMap<>();
		for (H1Candidate c : h1.values()) {
			if (c.status.equals("accepted")) {
				byLanguage.computeIfAbsent(locales.language(c.locale) + "|" + c.abbr + "|" + c.fullText(),
						k -> new ArrayList<>()).add(c);
			}
		}
		List<TopAbbrRow> l = new ArrayList<>();
		for (List<H1Candidate> cs : byLanguage.values()) {
			H1Candidate first = cs.get(0);
			String lang = locales.language(first.locale);
			if (cs.size() >= rules.intAttr(lang, "ownership", "wordLocales") && !lang.isEmpty()) {
				l.add(wordRow(lang, cs));
			} else {
				for (H1Candidate c : cs) {
					l.add(wordRow(c.locale, List.of(c)));
				}
			}
		}
		return l;
	}

	private TopAbbrRow wordRow(String locale, List<H1Candidate> cs) {
		TopAbbrRow r = new TopAbbrRow();
		r.kind = TopAbbrRow.KIND_WORD;
		r.locale = locale;
		Map<String, Integer> spellings = new HashMap<>();
		Map<String, Integer> types = new HashMap<>();
		Set<String> entities = new HashSet<>();
		int pairs = 0;
		for (H1Candidate c : cs) {
			c.spellings.forEach((k, v) -> spellings.merge(k, v, Integer::sum));
			c.types.forEach((k, v) -> types.merge(k, v, Integer::sum));
			entities.addAll(c.entities);
			pairs += c.pairs;
			r.sources.addAll(c.sources());
		}
		r.abbr = canonical(spellings, cs.get(0).abbr, false);
		r.full = cs.get(0).fullText();
		r.bind = TopAbbrRow.BIND_PHRASE;
		r.bindValue = r.full;
		r.supportObjects = entities.size();
		r.supportPairs = pairs;
		// the types of the supporting objects when a few of them cover them
		List<Map.Entry<String, Integer>> ts = new ArrayList<>(types.entrySet());
		ts.sort(Map.Entry.<String, Integer>comparingByValue().reversed().thenComparing(Map.Entry.comparingByKey()));
		int total = 0, covered = 0;
		for (Map.Entry<String, Integer> t : ts) {
			total += t.getValue();
		}
		Set<String> chosen = new TreeSet<>();
		for (Map.Entry<String, Integer> t : ts) {
			if (covered >= WORD_TYPES_COVERAGE * total || chosen.size() >= WORD_MAX_TYPES) {
				break;
			}
			chosen.add(t.getKey());
			covered += t.getValue();
		}
		if (covered >= WORD_TYPES_COVERAGE * total) {
			r.types = chosen;
		}
		return r;
	}

	/** the most frequent spelling; an organization in capitals when no spelling is known */
	private static String canonical(Map<String, Integer> spellings, String abbr, boolean org) {
		String s = firstSpelling(spellings);
		if (s == null) {
			return org ? abbr.toUpperCase(Locale.ROOT) : abbr;
		}
		return s.endsWith(".") ? s.substring(0, s.length() - 1) : s;
	}

	private Map<String, List<ExtractedEntity>> byWord;
	private Map<String, List<ExtractedEntity>> byValue;

	private void indexEntities() {
		if (byWord != null) {
			return;
		}
		byWord = new HashMap<>();
		byValue = new HashMap<>();
		for (ExtractedEntity e : extraction.entities()) {
			Set<String> ws = new HashSet<>();
			for (Map.Entry<String, String> t : e.tags.entrySet()) {
				if (TopAbbrPredicate.isNameTag(t.getKey())) {
					for (String v : AbbrText.values(t.getValue())) {
						ws.addAll(AbbrText.words(v));
					}
				}
			}
			for (String w : ws) {
				byWord.computeIfAbsent(w, k -> new ArrayList<>()).add(e);
			}
			for (String tag : List.of(TopAbbrRow.BIND_OPERATOR, TopAbbrRow.BIND_NETWORK)) {
				for (String v : AbbrText.values(e.tag(tag))) {
					byValue.computeIfAbsent(tag + "|" + AbbrText.normalize(v), k -> new ArrayList<>()).add(e);
				}
			}
		}
	}

	/** POI identities the row applies to; fills {@code examples} with up to MAX_EXAMPLES entities */
	Set<String> applied(TopAbbrRow r, List<String> examples) {
		indexEntities();
		List<ExtractedEntity> cands = TopAbbrRow.BIND_PHRASE.equals(r.bind)
				? byWord.getOrDefault(r.bindWords().isEmpty() ? "" : r.bindWords().get(0), List.of())
				: byValue.getOrDefault(r.bind + "|" + r.bindValue, List.of());
		Set<String> applied = new LinkedHashSet<>();
		for (ExtractedEntity e : cands) {
			for (ExtractedPoi p : e.pois) {
				if (predicate.applies(r, p)) {
					applied.add(p.identity());
					if (examples != null && examples.size() < MAX_EXAMPLES && !examples.contains(String.valueOf(e))) {
						examples.add(String.valueOf(e));
					}
				}
			}
		}
		return applied;
	}

	private void computeGain() {
		for (TopAbbrRow r : rows) {
			List<String> examples = new ArrayList<>();
			Set<String> applied = applied(r, examples);
			r.gainObjects = applied.size();
			gainExamples.put(r, examples);
			appliedPois.put(r, applied);
		}
	}

	private final Map<TopAbbrRow, Set<String>> appliedPois = new HashMap<>();

	/** one abbreviation with different full forms applied to one POI: every such row is dropped */
	private void dropConflicts() {
		Map<String, List<TopAbbrRow>> byAbbr = new HashMap<>();
		for (TopAbbrRow r : rows) {
			byAbbr.computeIfAbsent(r.abbrNormalized(), k -> new ArrayList<>()).add(r);
		}
		Set<TopAbbrRow> conflicts = new HashSet<>();
		for (List<TopAbbrRow> l : byAbbr.values()) {
			for (int i = 0; i < l.size(); i++) {
				for (int j = i + 1; j < l.size(); j++) {
					TopAbbrRow a = l.get(i), b = l.get(j);
					if (a.full.equals(b.full)) {
						continue;
					}
					Set<String> pa = appliedPois.getOrDefault(a, Set.of());
					for (String p : appliedPois.getOrDefault(b, Set.of())) {
						if (pa.contains(p)) {
							conflicts.add(a);
							conflicts.add(b);
							break;
						}
					}
				}
			}
		}
		for (TopAbbrRow r : conflicts) {
			droppedRows.add(new String[] { r.toString(), "conflict: one abbr, different full on one POI" });
		}
		rows.removeAll(conflicts);
	}

	private static int nz(Integer i) {
		return i == null ? 0 : i;
	}

	// ---------- outputs ----------

	public void write(File out, File regionsCsv, List<File> inputs) throws IOException {
		File csv = new File(out, "top_abbrs.csv");
		try (Writer w = new OutputStreamWriter(new FileOutputStream(csv), StandardCharsets.UTF_8)) {
			TopAbbrRow.writeAll(w, rows);
		}
		writeCandidates(new File(out, "candidates.tsv"));
		new TopAbbrsReport(this).write(new File(out, "top_abbrs.html"));
		writeManifest(new File(out, "top_abbrs.manifest.json"), csv, regionsCsv, inputs);
	}

	private void writeCandidates(File f) throws IOException {
		try (Writer w = new OutputStreamWriter(new FileOutputStream(f), StandardCharsets.UTF_8)) {
			w.write("source\tlocale\tabbr\tfull\tstatus\treason\tagreed\tsupport\tpairs\tsame\tnear\tspan\ttiles\t"
					+ "group_tag\tgroup_value\tgroup_size\tshare\th3\tcase\tspellings\texamples\n");
			List<H1Candidate> hs = new ArrayList<>(h1.values());
			hs.sort(Comparator.comparingInt((H1Candidate c) -> -c.entities.size()).thenComparing(H1Candidate::key));
			for (H1Candidate c : hs) {
				w.write(String.join("\t", "h1", c.locale, c.abbr, c.fullText(), c.status, c.reason,
						String.valueOf(c.agreed), String.valueOf(c.entities.size()), String.valueOf(c.pairs),
						String.valueOf(c.same), String.valueOf(c.near), String.valueOf(c.span),
						String.valueOf(c.tiles.size()), "", "", "", "", "",
						(c.isWord() ? c.capital : c.upper) + "/" + c.caseCounted, c.spellings.toString(),
						String.join(" || ", c.examples)).replace('\n', ' ') + "\n");
			}
			List<GroupCandidate> gs = new ArrayList<>(groups);
			gs.sort(Comparator.comparingInt((GroupCandidate g) -> -g.withToken).thenComparing(g -> g.tag + g.value + g.abbr));
			for (GroupCandidate g : gs) {
				w.write(String.join("\t", "h2", g.locale, g.abbr, String.join(" ", g.full), g.status, g.reason,
						String.valueOf(g.agreed), String.valueOf(g.withToken), "", "", "", "", "", g.tag, g.value,
						String.valueOf(g.members.size()), String.format(Locale.US, "%.2f", g.share), g.h3,
						g.upper + "/" + g.caseCounted, g.spellings.toString(), String.join(" || ", g.examples))
						.replace('\n', ' ') + "\n");
			}
		}
	}

	/**
	 * Every candidate with its metrics for the report that applies the thresholds itself: support by map, tiles,
	 * case, brand, agreement, noise and the gain the candidate would have as a row.
	 */
	public void writeExplorer(File f) throws IOException {
		Map<String, Object> root = new LinkedHashMap<>();
		List<Map<String, Object>> maps = new ArrayList<>();
		for (PoiExtraction.ExtractedMap m : extraction.maps()) {
			Map<String, Object> o = new LinkedHashMap<>();
			o.put("name", m.name);
			o.put("locale", predicate.mapLocale(m.name));
			o.put("entities", m.entities);
			o.put("pois", m.pois);
			maps.add(o);
		}
		root.put("maps", maps);
		List<Map<String, Object>> hs = new ArrayList<>();
		for (H1Candidate c : h1.values()) {
			Map<String, Object> o = new LinkedHashMap<>();
			o.put("loc", c.locale);
			o.put("a", c.abbr);
			o.put("f", c.fullText());
			o.put("w", c.isWord());
			o.put("n", c.entities.size());
			o.put("pm", c.perMap);
			o.put("t", c.tiles.size());
			o.put("s", c.same);
			o.put("nr", c.near);
			o.put("sp", c.span);
			o.put("p", c.pairs);
			o.put("cc", c.caseCounted);
			o.put("up", c.upper);
			o.put("cap", c.capital);
			String spelling = firstSpelling(c.spellings);
			o.put("sp1", spelling == null ? c.abbr : spelling);
			o.put("cs", spelling != null && AbbrText.hasCase(spelling));
			o.put("br", brands.contains(c.abbr) || brands.contains(c.fullText()));
			o.put("ag", c.agreed);
			o.put("nz", c.noise);
			if (c.isWord()) {
				TopAbbrRow r = wordRow(c.locale, List.of(c));
				o.put("g", applied(r, null).size());
				o.put("ty", String.join(" ", r.types));
			}
			o.put("ex", c.examples.subList(0, Math.min(3, c.examples.size())));
			hs.add(o);
		}
		root.put("h1", hs);
		List<Map<String, Object>> gs = new ArrayList<>();
		for (GroupCandidate g : groups) {
			Map<String, Object> o = new LinkedHashMap<>();
			o.put("loc", g.locale);
			o.put("map", g.map);
			o.put("tag", g.tag);
			o.put("v", g.value);
			o.put("a", g.abbr);
			o.put("gs", g.members.size());
			o.put("k", g.withToken);
			o.put("h3", g.h3);
			o.put("f", String.join(" ", g.full));
			o.put("h3f", g.h3Free);
			o.put("ff", String.join(" ", g.fullFree));
			o.put("cc", g.caseCounted);
			o.put("up", g.upper);
			String spelling = firstSpelling(g.spellings);
			o.put("sp1", spelling == null ? g.abbr : spelling);
			o.put("cs", spelling != null && AbbrText.hasCase(spelling));
			o.put("br", brands.contains(g.abbr) || brands.contains(String.join(" ", g.full)));
			o.put("ag", g.agreed);
			o.put("nz", g.noise);
			o.put("g", applied(orgRow(g), null).size());
			o.put("ex", g.examples.subList(0, Math.min(3, g.examples.size())));
			gs.add(o);
		}
		root.put("h2", gs);
		Files.writeString(f.toPath(), new com.google.gson.GsonBuilder().disableHtmlEscaping().create().toJson(root),
				StandardCharsets.UTF_8);
	}

	private void writeManifest(File f, File csv, File regionsCsv, List<File> inputs) throws IOException {
		StringBuilder b = new StringBuilder("{\n");
		b.append("  \"top_abbrs_csv\": \"").append(hash(csv)).append("\",\n");
		b.append("  \"top_abbrs_rules\": \"").append(rules.hash()).append("\",\n");
		if (regionsCsv != null) {
			b.append("  \"regions_csv\": \"").append(hash(regionsCsv)).append("\",\n");
		}
		b.append("  \"maps\": [\n");
		Map<String, File> bases = new TreeMap<>();
		for (File in : inputs) {
			File[] fs = in.isDirectory() ? in.listFiles((d, n) -> n.endsWith(PoiNamesExtractor.RECORDS_EXT))
					: new File[] { in };
			for (File r : fs == null ? new File[0] : fs) {
				String n = r.getName();
				bases.put(n.substring(0, n.indexOf(".poi-")), new File(r.getParentFile(), n.substring(0, n.indexOf(".poi-"))));
			}
		}
		int i = 0;
		for (PoiExtraction.ExtractedMap m : extraction.maps()) {
			File base = bases.get(m.name);
			b.append("    {\"map\": \"").append(m.name).append("\", \"generated\": ").append(m.generated)
					.append(", \"used\": ").append(m.used);
			if (base != null) {
				b.append(", \"entities\": \"").append(hash(new File(base.getPath() + PoiNamesExtractor.ENTITIES_EXT)))
						.append("\", \"records\": \"").append(hash(new File(base.getPath() + PoiNamesExtractor.RECORDS_EXT)))
						.append("\"");
			}
			b.append("}").append(++i < extraction.maps().size() ? ",\n" : "\n");
		}
		b.append("  ]\n}\n");
		Files.writeString(f.toPath(), b.toString(), StandardCharsets.UTF_8);
	}

	private static String hash(File f) throws IOException {
		return TopAbbrsRules.sha256(Files.readAllBytes(f.toPath()));
	}

	// ---------- command ----------

	public static void main(String[] args) throws IOException {
		List<File> inputs = new ArrayList<>();
		File out = null, rulesFile = null, brandsFile = null, explorer = null;
		for (String a : args) {
			if (a.startsWith("--out=")) {
				out = new File(a.substring("--out=".length()));
			} else if (a.startsWith("--rules=")) {
				rulesFile = new File(a.substring("--rules=".length()));
			} else if (a.startsWith("--brands=")) {
				brandsFile = new File(a.substring("--brands=".length()));
			} else if (a.startsWith("--explorer=")) {
				explorer = new File(a.substring("--explorer=".length()));
			} else if (a.startsWith("--")) {
				throw new IllegalArgumentException("Unknown option " + a);
			} else {
				inputs.add(new File(a));
			}
		}
		if (inputs.isEmpty() || out == null) {
			System.out.println("Usage: top-abbrs-analyzer <extraction dir or file>... --out=<dir> "
					+ "[--rules=top-abbrs-rules.xml] [--brands=brands.lst]");
			return;
		}
		if (!out.isDirectory() && !out.mkdirs()) {
			throw new IOException("Cannot create " + out);
		}
		long time = System.currentTimeMillis();
		MapPoiTypes poiTypes = MapPoiTypes.getDefault();
		TopAbbrsRules rules;
		try (InputStream is = rulesFile != null ? new FileInputStream(rulesFile)
				: TopAbbrsAnalyzer.class.getResourceAsStream(RULES_RESOURCE)) {
			rules = TopAbbrsRules.load(is, poiTypes);
		}
		RegionTree regions = RegionTree.fromOsmandRegions();
		File regionsCsv = new File(out, "regions.csv");
		regions.writeCsv(regionsCsv);
		PoiExtraction extraction = new PoiExtractionReader(regions,
				rules.intAttr("", "input", "maxExtractionAgeMonths")).read(inputs);
		System.out.printf("Read %d maps: %d entities, %d POI (%d duplicates of maps removed) in %.1f s%n",
				extraction.maps().size(), extraction.entities().size(), extraction.poiCount(),
				extraction.duplicatePois(), (System.currentTimeMillis() - time) / 1e3);
		List<String> brandValues = new ArrayList<>();
		if (brandsFile != null) {
			try (BufferedReader r = new BufferedReader(new InputStreamReader(new FileInputStream(brandsFile),
					StandardCharsets.UTF_8))) {
				String l;
				while ((l = r.readLine()) != null) {
					int c = l.indexOf(',');
					if (c > 0 && l.substring(0, c).equals("brand")) {
						brandValues.add(l.substring(c + 1));
					}
				}
			}
		}
		TopAbbrsAnalyzer analyzer = new TopAbbrsAnalyzer(extraction, rules, new SearchModRules().locales(),
				CommonWordsMultiIndex.getInstance(), regions, poiTypes, brandValues);
		analyzer.keepNoise = explorer != null;
		List<TopAbbrRow> rows = analyzer.run();
		analyzer.write(out, regionsCsv, inputs);
		if (explorer != null) {
			analyzer.writeExplorer(explorer);
		}
		long org = rows.stream().filter(r -> r.kind.equals(TopAbbrRow.KIND_ORG)).count();
		System.out.printf("%d rows (%d org, %d word), %d H1 pairs, %d group words in %.1f s -> %s%n", rows.size(), org,
				rows.size() - org, analyzer.h1.size(), analyzer.groups.size(),
				(System.currentTimeMillis() - time) / 1e3, out);
	}
}
