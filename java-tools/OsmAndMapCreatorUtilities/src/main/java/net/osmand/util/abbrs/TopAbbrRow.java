package net.osmand.util.abbrs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import net.osmand.osm.MapPoiTypes;

/** A row of {@code top_abbrs.csv} (top-org-abbrs-spec.md 7): one rule of application of an abbreviation */
public class TopAbbrRow {

	public static final String KIND_ORG = "org";
	public static final String KIND_WORD = "word";
	public static final String BIND_OPERATOR = "operator";
	public static final String BIND_NETWORK = "network";
	public static final String BIND_PHRASE = "phrase";
	public static final String OWNER_ANY = "*";
	public static final String STATUS_AUTO = "auto";
	public static final String STATUS_INCLUDE = "manual-include";

	public static final String HEADER = "kind,locale,owner,abbr,full,bind,bind_value,require_absent,types,group_size,"
			+ "support_objects,support_pairs,gain_objects,sources,status";
	private static final int COLUMNS = HEADER.split(",").length;

	public String kind;
	public String locale = "";
	public String owner = OWNER_ANY;
	/** canonical spelling, for the report */
	public String abbr;
	public String full;
	public String bind;
	public String bindValue;
	public String requireAbsent = "";
	public Set<String> types = new TreeSet<>();
	public Integer groupSize;
	public Integer supportObjects;
	public Integer supportPairs;
	public Integer gainObjects;
	public Set<String> sources = new LinkedHashSet<>();
	public String status = STATUS_AUTO;

	/** normalized abbreviation */
	public String abbrNormalized() {
		return AbbrText.normalize(abbr);
	}

	/** key of a row between months and of include / exclude */
	public String key() {
		return String.join("|", kind, locale, owner, abbrNormalized(), full, bind, bindValue);
	}

	public void write(Writer w) throws IOException {
		w.write(String.join(",", kind, locale, owner, csv(abbr), csv(full), bind, csv(bindValue), requireAbsent,
				String.join(" ", types), num(groupSize), num(supportObjects), num(supportPairs), num(gainObjects),
				String.join(" ", sources), status));
		w.write("\n");
	}

	public static void writeAll(Writer w, List<TopAbbrRow> rows) throws IOException {
		w.write(HEADER + "\n");
		for (TopAbbrRow r : rows) {
			r.write(w);
		}
	}

	/**
	 * @param poiTypes checks the keys of {@code types}, null to skip the check
	 */
	public static List<TopAbbrRow> readAll(Reader reader, MapPoiTypes poiTypes) throws IOException {
		BufferedReader r = new BufferedReader(reader);
		String header = r.readLine();
		if (!HEADER.equals(header)) {
			throw new IOException("Unexpected header of top_abbrs.csv: " + header);
		}
		List<TopAbbrRow> rows = new ArrayList<>();
		String l;
		while ((l = r.readLine()) != null) {
			if (l.isEmpty()) {
				continue;
			}
			List<String> c = parseCsv(l);
			if (c.size() != COLUMNS) {
				throw new IOException("Unexpected row of top_abbrs.csv: " + l);
			}
			TopAbbrRow row = new TopAbbrRow();
			row.kind = c.get(0);
			row.locale = c.get(1);
			row.owner = c.get(2);
			row.abbr = c.get(3);
			row.full = c.get(4);
			row.bind = c.get(5);
			row.bindValue = c.get(6);
			row.requireAbsent = c.get(7);
			row.types = split(c.get(8));
			row.groupSize = parseNum(c.get(9));
			row.supportObjects = parseNum(c.get(10));
			row.supportPairs = parseNum(c.get(11));
			row.gainObjects = parseNum(c.get(12));
			row.sources = new LinkedHashSet<>(split(c.get(13)));
			row.status = c.get(14);
			if (poiTypes != null) {
				String unknown = unknownType(row.types, poiTypes);
				if (unknown != null) {
					throw new IOException("Unknown key of types '" + unknown + "': " + l);
				}
			}
			rows.add(row);
		}
		return rows;
	}

	/** @return the first key of {@code types} that is neither a subtype nor an additional type of MapPoiTypes */
	public static String unknownType(Set<String> types, MapPoiTypes poiTypes) {
		for (String t : types) {
			if (poiTypes.getAnyPoiTypeByKey(t) == null && poiTypes.getAnyPoiAdditionalTypeByKey(t) == null) {
				return t;
			}
		}
		return null;
	}

	private static Set<String> split(String s) {
		return s.isEmpty() ? new TreeSet<>() : new TreeSet<>(Arrays.asList(s.split(" ")));
	}

	private static String num(Integer i) {
		return i == null ? "" : i.toString();
	}

	private static Integer parseNum(String s) {
		return s.isEmpty() ? null : Integer.valueOf(s);
	}

	private static String csv(String s) {
		if (s == null) {
			return "";
		}
		if (s.indexOf(',') >= 0 || s.indexOf('"') >= 0) {
			return '"' + s.replace("\"", "\"\"") + '"';
		}
		return s;
	}

	private static List<String> parseCsv(String line) {
		List<String> l = new ArrayList<>();
		StringBuilder b = new StringBuilder();
		boolean quoted = false;
		for (int i = 0; i < line.length(); i++) {
			char c = line.charAt(i);
			if (quoted) {
				if (c == '"' && i + 1 < line.length() && line.charAt(i + 1) == '"') {
					b.append('"');
					i++;
				} else if (c == '"') {
					quoted = false;
				} else {
					b.append(c);
				}
			} else if (c == '"') {
				quoted = true;
			} else if (c == ',') {
				l.add(b.toString());
				b.setLength(0);
			} else {
				b.append(c);
			}
		}
		l.add(b.toString());
		return l;
	}

	public List<String> fullWords() {
		return full == null || full.isEmpty() ? Collections.emptyList() : Arrays.asList(full.split(" "));
	}

	public List<String> bindWords() {
		return bindValue == null || bindValue.isEmpty() ? Collections.emptyList() : Arrays.asList(bindValue.split(" "));
	}

	@Override
	public String toString() {
		return kind + " " + locale + " " + abbr + " <- " + full + " (" + bind + "=" + bindValue + ")";
	}
}
