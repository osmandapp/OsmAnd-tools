package net.osmand.util.abbrs;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Predicate;

import net.osmand.util.abbrs.PoiExtraction.ExtractedMap;
import net.osmand.util.abbrs.TopAbbrsAnalyzer.GroupCandidate;
import net.osmand.util.abbrs.TopAbbrsAnalyzer.H1Candidate;

/**
 * {@code top_abbrs.html}: the rows with their sources, support and examples, and the candidates that did not become
 * rows with the reason - for review (top-org-abbrs-spec.md 5.2.9, 12.8, 12.10).
 */
class TopAbbrsReport {

	private static final int MAX_LIST = 300;

	private final TopAbbrsAnalyzer a;
	private Writer w;

	TopAbbrsReport(TopAbbrsAnalyzer analyzer) {
		this.a = analyzer;
	}

	void write(File f) throws IOException {
		try (Writer writer = new OutputStreamWriter(new FileOutputStream(f), StandardCharsets.UTF_8)) {
			w = writer;
			w.write("<!doctype html><html><head><meta charset=\"utf-8\"><title>Top abbreviations</title><style>"
					+ "body{font:13px sans-serif;margin:16px}table{border-collapse:collapse;margin-bottom:24px}"
					+ "td,th{border:1px solid #ccc;padding:2px 6px;vertical-align:top;text-align:left}"
					+ "th{background:#eee}td.ex{color:#555;max-width:600px}</style></head><body>\n");
			summary();
			rows();
			groups("Organizations rejected by a filter", g -> g.status.equals("rejected"));
			groups("Brand aliases (excluded, spec 12.10)", g -> g.status.equals("brand"));
			groups("Glued words of the value (separate)", g -> g.status.equals("glued"));
			pairs("H1 phrases without H2+H3: only by &lt;include&gt;", c -> c.status.equals("phrase"));
			pairs("H1 pairs cut by case (spec 12.8)", c -> c.reason.startsWith("case"));
			pairs("H1 pairs: brands", c -> c.reason.equals("brand"));
			pairs("H1 pairs below thresholds", c -> c.reason.startsWith("support") || c.reason.startsWith("tiles"));
			dropped();
			w.write("</body></html>\n");
		}
	}

	private void summary() throws IOException {
		w.write("<h1>Top abbreviations</h1>\n<h2>Input</h2><table><tr><th>map</th><th>generated</th><th>entities</th>"
				+ "<th>POI</th><th>used</th></tr>\n");
		for (ExtractedMap m : a.extraction.maps()) {
			w.write(tr(m.name, String.format(Locale.US, "%tF", m.generated), m.entities, m.pois,
					m.used ? "yes" : "no: " + m.skipReason));
		}
		w.write("</table>\n");
		w.write("<p>Entities " + a.extraction.entities().size() + ", POI " + a.extraction.poiCount()
				+ ", copies of maps removed " + a.extraction.duplicatePois() + ".</p>\n");
		Map<String, Integer> h1 = new TreeMap<>();
		for (H1Candidate c : a.h1.values()) {
			h1.merge(c.status, 1, Integer::sum);
		}
		Map<String, Integer> g = new TreeMap<>();
		for (GroupCandidate c : a.groups) {
			g.merge(c.status, 1, Integer::sum);
		}
		w.write("<p>H1 pairs by status " + esc(h1.toString()) + "; H1 dropped as noise " + esc(a.noise.toString())
				+ ".<br>Group words (H2) by status " + esc(g.toString()) + ".</p>\n");
	}

	private void rows() throws IOException {
		w.write("<h2>Rows of top_abbrs.csv (" + a.rows.size() + ")</h2><table><tr><th>kind</th><th>locale</th>"
				+ "<th>abbr</th><th>full</th><th>bind</th><th>bind_value</th><th>types</th><th>group</th>"
				+ "<th>support</th><th>pairs</th><th>gain</th><th>sources</th><th>gain examples</th></tr>\n");
		for (TopAbbrRow r : a.rows) {
			w.write(tr(r.kind, r.locale, r.abbr, r.full, r.bind, r.bindValue, String.join(" ", r.types),
					n(r.groupSize), n(r.supportObjects), n(r.supportPairs), n(r.gainObjects),
					String.join(",", r.sources), ex(a.gainExamples.getOrDefault(r, List.of()))));
		}
		w.write("</table>\n");
	}

	private void groups(String title, Predicate<GroupCandidate> filter) throws IOException {
		List<GroupCandidate> l = new ArrayList<>();
		for (GroupCandidate g : a.groups) {
			if (filter.test(g)) {
				l.add(g);
			}
		}
		l.sort(Comparator.comparingInt((GroupCandidate g) -> -g.withToken).thenComparing(g -> g.value + g.abbr));
		w.write("<h2>" + title + " (" + l.size() + ")</h2><table><tr><th>locale</th><th>abbr</th><th>tag</th>"
				+ "<th>value</th><th>group</th><th>with word</th><th>share</th><th>H3</th><th>full</th><th>case</th>"
				+ "<th>reason</th><th>names</th></tr>\n");
		for (GroupCandidate g : l.subList(0, Math.min(MAX_LIST, l.size()))) {
			w.write(tr(g.locale, g.abbr, g.tag, g.value, g.members.size(), g.withToken,
					String.format(Locale.US, "%.2f", g.share), g.h3, String.join(" ", g.full),
					g.upper + "/" + g.caseCounted, g.reason, ex(g.examples)));
		}
		w.write("</table>\n");
	}

	private void pairs(String title, Predicate<H1Candidate> filter) throws IOException {
		List<H1Candidate> l = new ArrayList<>();
		for (H1Candidate c : a.h1.values()) {
			if (filter.test(c)) {
				l.add(c);
			}
		}
		l.sort(Comparator.comparingInt((H1Candidate c) -> -c.entities.size()).thenComparing(H1Candidate::key));
		w.write("<h2>" + title + " (" + l.size() + ")</h2><table><tr><th>locale</th><th>abbr</th><th>full</th>"
				+ "<th>support</th><th>same</th><th>near</th><th>tiles</th><th>agreed</th><th>reason</th>"
				+ "<th>examples</th></tr>\n");
		for (H1Candidate c : l.subList(0, Math.min(MAX_LIST, l.size()))) {
			w.write(tr(c.locale, c.abbr, c.fullText(), c.entities.size(), c.same, c.near, c.tiles.size(), c.agreed,
					c.reason, ex(c.examples)));
		}
		w.write("</table>\n");
	}

	private void dropped() throws IOException {
		w.write("<h2>Rows dropped (" + a.droppedRows.size() + ")</h2><table><tr><th>row</th><th>reason</th></tr>\n");
		for (String[] d : a.droppedRows) {
			w.write(tr(d[0], d[1]));
		}
		w.write("</table>\n");
	}

	private static String ex(List<String> examples) {
		return "\u0001" + esc(String.join(" · ", examples));
	}

	private static String n(Integer i) {
		return i == null ? "" : i.toString();
	}

	private static String tr(Object... cells) {
		StringBuilder b = new StringBuilder("<tr>");
		for (Object c : cells) {
			String s = String.valueOf(c);
			if (s.startsWith("\u0001")) {
				b.append("<td class=\"ex\">").append(s.substring(1)).append("</td>");
			} else {
				b.append("<td>").append(esc(s)).append("</td>");
			}
		}
		return b.append("</tr>\n").toString();
	}

	private static String esc(String s) {
		return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;");
	}
}
