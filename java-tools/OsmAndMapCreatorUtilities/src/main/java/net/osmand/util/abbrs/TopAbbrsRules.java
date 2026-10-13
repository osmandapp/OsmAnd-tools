package net.osmand.util.abbrs;

import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import net.osmand.osm.MapPoiTypes;

/**
 * Schema {@code top-abbrs-rules.xml} (top-org-abbrs-spec.md 6): parameters of the analyzer in layers
 * {@code <defaults>} → {@code <locale id="de">} → {@code <locale id="de_CH">}, a lower layer replaces an attribute,
 * word lists ({@code <stop>}, {@code <legal>}, {@code <places>}) are replaced whole; manual {@code <include>} and
 * {@code <exclude>}. Unknown elements and attributes are errors, every attribute has a default value.
 */
public class TopAbbrsRules {

	// path of an element (pairs/source.same) -> attribute -> default value; the allowed attributes
	private static final Map<String, Map<String, String>> DEFAULTS = new LinkedHashMap<>();
	private static final Set<String> WORD_LISTS = Set.of("stop", "legal", "places");

	static {
		def("ownership", "share", "0.7", "wordLocales", "2");
		def("case", "org", "upper", "word", "capital", "share", "0.6", "skipAllCapsNames", "true");
		def("limit", "perMap", "500", "planet", "20000");
		def("publish", "firstRelease", "manual", "maxMapChange", "0.02", "maxRowGain", "2000");
		def("input", "maxExtractionAgeMonths", "2");
		def("review", "perStratum", "20", "highGain", "1000");
		def("pairs/source.same", "tags", "short_name alt_name official_name name:{lang}", "span", "true",
				"spanMaxWords", "6", "enabled", "true");
		def("pairs/source.near", "radiusM", "500", "sameFirstWord", "true", "maxShortLen", "4", "enabled", "true");
		def("pairs/abbr", "minLen", "2", "maxLen", "8", "maxChunk", "3");
		def("pairs/long", "maxWords", "3");
		def("pairs/noise", "unglue", "true", "roman", "true", "compound", "true", "compoundLinks", "s es",
				"serviceWords", "true");
		def("pairs/accept", "minSupport", "3", "minTiles", "2", "tileZoom", "10");
		def("groups/group.operator", "enabled", "true");
		def("groups/group.network", "enabled", "true");
		def("groups/accept", "minGroup", "3", "minShare", "0.25", "acronym", "required", "glued", "separate",
				"headWord", "true", "stripTail", "legal place", "min2LetterShare", "0.6");
		def("groups/exclude", "brandValues", "true");
		def("groups/propagate", "enabled", "false", "sameLocale", "true", "sameTypes", "true", "requireH1", "true",
				"minPhraseWords", "2", "maxSpread", "5", "excludeBrands", "true");
		def("agree", "supportFactor", "0.5", "shareFactor", "0.5");
	}

	private static void def(String path, String... kv) {
		Map<String, String> m = new LinkedHashMap<>();
		for (int i = 0; i < kv.length; i += 2) {
			m.put(kv[i], kv[i + 1]);
		}
		DEFAULTS.put(path, m);
	}

	/** a manual row: include sets every column, exclude any subset of the key columns */
	public static class ManualRow {
		public final Map<String, String> attrs;

		ManualRow(Map<String, String> attrs) {
			this.attrs = attrs;
		}

		/** the exclude matches the row: every given key column is equal */
		public boolean matches(TopAbbrRow r) {
			for (Map.Entry<String, String> a : attrs.entrySet()) {
				String v = switch (a.getKey()) {
					case "kind" -> r.kind;
					case "locale" -> r.locale;
					case "owner" -> r.owner;
					case "abbr" -> r.abbrNormalized();
					case "full" -> r.full;
					case "bind" -> r.bind;
					case "bind_value" -> r.bindValue;
					default -> null;
				};
				String expected = a.getKey().equals("abbr") ? AbbrText.normalize(a.getValue())
						: a.getKey().equals("full") || a.getKey().equals("bind_value") ? AbbrText.normalize(a.getValue())
						: a.getValue();
				if (!expected.equals(v)) {
					return false;
				}
			}
			return true;
		}

		public TopAbbrRow toRow() {
			TopAbbrRow r = new TopAbbrRow();
			r.kind = attrs.get("kind");
			r.locale = attrs.getOrDefault("locale", "");
			r.owner = attrs.getOrDefault("owner", TopAbbrRow.OWNER_ANY);
			r.abbr = attrs.get("abbr");
			r.full = AbbrText.normalize(attrs.get("full"));
			r.bind = attrs.get("bind");
			r.bindValue = AbbrText.normalize(attrs.getOrDefault("bind_value", attrs.get("full")));
			r.requireAbsent = attrs.getOrDefault("require_absent", "");
			String types = attrs.getOrDefault("types", "");
			r.types = types.isEmpty() ? new TreeSet<>() : new TreeSet<>(Arrays.asList(types.split(" ")));
			r.sources.add("include");
			r.status = TopAbbrRow.STATUS_INCLUDE;
			return r;
		}

		@Override
		public String toString() {
			return attrs.toString();
		}
	}

	private static final Set<String> INCLUDE_ATTRS = Set.of("kind", "locale", "owner", "abbr", "full", "bind",
			"bind_value", "require_absent", "types");
	private static final Set<String> EXCLUDE_ATTRS = Set.of("kind", "locale", "owner", "abbr", "full", "bind",
			"bind_value");

	// layer id ("" defaults, "de", "de_CH") -> path -> attribute -> value
	private final Map<String, Map<String, Map<String, String>>> layers = new HashMap<>();
	private final Map<String, Map<String, Set<String>>> words = new HashMap<>();
	public final List<ManualRow> includes = new ArrayList<>();
	public final List<ManualRow> excludes = new ArrayList<>();
	private String hash = "";

	public static TopAbbrsRules load(InputStream is, MapPoiTypes poiTypes) throws IOException {
		byte[] bytes = is.readAllBytes();
		TopAbbrsRules rules = new TopAbbrsRules();
		rules.hash = sha256(bytes);
		Document doc;
		try {
			DocumentBuilderFactory f = DocumentBuilderFactory.newInstance();
			f.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
			doc = f.newDocumentBuilder().parse(new java.io.ByteArrayInputStream(bytes));
		} catch (Exception e) {
			throw new IOException("Cannot parse top-abbrs rules: " + e.getMessage(), e);
		}
		Element root = doc.getDocumentElement();
		if (!root.getTagName().equals("top-abbrs")) {
			throw new IOException("Root element must be <top-abbrs>: " + root.getTagName());
		}
		checkAttrs(root, Set.of("version"));
		for (Element e : children(root)) {
			switch (e.getTagName()) {
				case "defaults" -> {
					checkAttrs(e, Set.of());
					rules.readLayer("", e, "");
				}
				case "pairs", "groups", "agree", "stop", "legal", "places" -> rules.readElement("", e, "");
				case "locale" -> {
					checkAttrs(e, Set.of("id"));
					String id = e.getAttribute("id");
					if (id.isEmpty()) {
						throw new IOException("<locale> without id");
					}
					rules.readLayer(id, e, "");
				}
				case "include" -> rules.includes.add(manual(e, INCLUDE_ATTRS));
				case "exclude" -> rules.excludes.add(manual(e, EXCLUDE_ATTRS));
				default -> throw new IOException("Unknown element <" + e.getTagName() + ">");
			}
		}
		rules.validate(poiTypes);
		return rules;
	}

	private void readLayer(String layer, Element parent, String prefix) throws IOException {
		for (Element e : children(parent)) {
			readElement(layer, e, prefix);
		}
	}

	private void readElement(String layer, Element e, String prefix) throws IOException {
		String tag = e.getTagName();
		if (prefix.isEmpty() && WORD_LISTS.contains(tag)) {
			checkAttrs(e, Set.of("words"));
			Set<String> ws = new LinkedHashSet<>();
			for (String w : e.getAttribute("words").trim().split("\\s+")) {
				if (!w.isEmpty()) {
					ws.add(AbbrText.normalize(w));
				}
			}
			words.computeIfAbsent(layer, k -> new HashMap<>()).put(tag, ws);
			return;
		}
		String path = prefix + tag;
		if ((tag.equals("source") || tag.equals("group")) && prefix.endsWith("/")) {
			String id = tag.equals("source") ? e.getAttribute("id") : e.getAttribute("tag");
			path = path + "." + id;
		}
		if (path.equals("pairs") || path.equals("groups")) {
			checkAttrs(e, Set.of());
			readLayer(layer, e, path + "/");
			return;
		}
		Map<String, String> allowed = DEFAULTS.get(path);
		if (allowed == null) {
			throw new IOException("Unknown element <" + tag + "> (" + path + ")");
		}
		Set<String> names = new HashSet<>(allowed.keySet());
		if (tag.equals("source")) {
			names.add("id");
		} else if (tag.equals("group")) {
			names.add("tag");
		}
		checkAttrs(e, names);
		if (!children(e).isEmpty()) {
			throw new IOException("<" + tag + "> has no child elements");
		}
		Map<String, String> values = layers.computeIfAbsent(layer, k -> new HashMap<>())
				.computeIfAbsent(path, k -> new HashMap<>());
		NamedNodeMap a = e.getAttributes();
		for (int i = 0; i < a.getLength(); i++) {
			String n = a.item(i).getNodeName();
			if (!n.equals("id") && !(tag.equals("group") && n.equals("tag"))) {
				values.put(n, a.item(i).getNodeValue());
			}
		}
	}

	private static ManualRow manual(Element e, Set<String> allowed) throws IOException {
		checkAttrs(e, allowed);
		Map<String, String> attrs = new LinkedHashMap<>();
		NamedNodeMap a = e.getAttributes();
		for (int i = 0; i < a.getLength(); i++) {
			attrs.put(a.item(i).getNodeName(), a.item(i).getNodeValue());
		}
		if (attrs.isEmpty()) {
			throw new IOException("<" + e.getTagName() + "> without attributes");
		}
		return new ManualRow(attrs);
	}

	private void validate(MapPoiTypes poiTypes) throws IOException {
		for (Map.Entry<String, Map<String, Map<String, String>>> l : layers.entrySet()) {
			if (l.getValue().containsKey("pairs/source.near") && l.getValue().get("pairs/source.near").containsKey("span")) {
				throw new IOException("span is only for the source same");
			}
		}
		for (String layer : layers.keySet()) {
			if (intAttr(layer, "pairs/abbr", "minLen") > intAttr(layer, "pairs/abbr", "maxLen")) {
				throw new IOException("minLen > maxLen in the layer '" + layer + "'");
			}
		}
		for (ManualRow in : includes) {
			for (String req : List.of("kind", "locale", "abbr", "full", "bind")) {
				if (!in.attrs.containsKey(req)) {
					throw new IOException("<include> without " + req + ": " + in);
				}
			}
			if (TopAbbrRow.BIND_PHRASE.equals(in.attrs.get("bind")) && !in.attrs.containsKey("require_absent")) {
				throw new IOException("<include bind=\"phrase\"> needs require_absent: " + in);
			}
			TopAbbrRow r = in.toRow();
			if (poiTypes != null) {
				String unknown = TopAbbrRow.unknownType(r.types, poiTypes);
				if (unknown != null) {
					throw new IOException("Unknown key of types '" + unknown + "': " + in);
				}
			}
			for (ManualRow ex : excludes) {
				if (ex.matches(r)) {
					throw new IOException("<include> and <exclude> with one key: " + in + " / " + ex);
				}
			}
		}
	}

	/** the attribute for a locale: locale_CC, then language, then defaults */
	public String attr(String locale, String path, String attr) {
		for (String layer : layerChain(locale)) {
			Map<String, Map<String, String>> l = layers.get(layer);
			if (l != null && l.containsKey(path) && l.get(path).containsKey(attr)) {
				return l.get(path).get(attr);
			}
		}
		Map<String, String> d = DEFAULTS.get(path);
		if (d == null || !d.containsKey(attr)) {
			throw new IllegalArgumentException("Unknown attribute " + path + "@" + attr);
		}
		return d.get(attr);
	}

	public int intAttr(String locale, String path, String attr) {
		return Integer.parseInt(attr(locale, path, attr));
	}

	public double doubleAttr(String locale, String path, String attr) {
		return Double.parseDouble(attr(locale, path, attr));
	}

	public boolean boolAttr(String locale, String path, String attr) {
		return Boolean.parseBoolean(attr(locale, path, attr));
	}

	public Set<String> listAttr(String locale, String path, String attr) {
		String v = attr(locale, path, attr).trim();
		return v.isEmpty() ? Set.of() : new LinkedHashSet<>(Arrays.asList(v.split("\\s+")));
	}

	/** {@code <stop>}, {@code <legal>}, {@code <places>} of the nearest layer that has the list */
	public Set<String> words(String locale, String list) {
		for (String layer : layerChain(locale)) {
			Map<String, Set<String>> l = words.get(layer);
			if (l != null && l.containsKey(list)) {
				return l.get(list);
			}
		}
		return Set.of();
	}

	public String hash() {
		return hash;
	}

	private static List<String> layerChain(String locale) {
		List<String> chain = new ArrayList<>(3);
		if (locale != null && !locale.isEmpty()) {
			chain.add(locale);
			int i = locale.indexOf('_');
			if (i > 0) {
				chain.add(locale.substring(0, i));
			}
		}
		chain.add("");
		return chain;
	}

	private static List<Element> children(Element e) {
		List<Element> l = new ArrayList<>();
		NodeList nl = e.getChildNodes();
		for (int i = 0; i < nl.getLength(); i++) {
			if (nl.item(i).getNodeType() == Node.ELEMENT_NODE) {
				l.add((Element) nl.item(i));
			}
		}
		return l;
	}

	private static void checkAttrs(Element e, Set<String> allowed) throws IOException {
		NamedNodeMap a = e.getAttributes();
		for (int i = 0; i < a.getLength(); i++) {
			if (!allowed.contains(a.item(i).getNodeName())) {
				throw new IOException("Unknown attribute " + a.item(i).getNodeName() + " of <" + e.getTagName() + ">");
			}
		}
	}

	static String sha256(byte[] bytes) {
		try {
			StringBuilder b = new StringBuilder();
			for (byte x : MessageDigest.getInstance("SHA-256").digest(bytes)) {
				b.append(String.format("%02x", x));
			}
			return b.toString();
		} catch (NoSuchAlgorithmException e) {
			throw new IllegalStateException(e);
		}
	}
}
