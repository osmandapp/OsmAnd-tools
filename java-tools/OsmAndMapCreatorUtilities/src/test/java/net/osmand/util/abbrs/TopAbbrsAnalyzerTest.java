package net.osmand.util.abbrs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.zip.GZIPOutputStream;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.google.gson.Gson;

import net.osmand.obf.preparation.CommonWordsMultiIndex;
import net.osmand.obf.preparation.PoiNamesExtractor;
import net.osmand.osm.MapPoiTypes;
import net.osmand.search.rules.SearchModLocales;
import net.osmand.search.rules.SearchModRules;
import net.osmand.util.abbrs.PoiExtraction.ExtractedEntity;
import net.osmand.util.abbrs.PoiExtraction.ExtractedPoi;

/** {@link TopAbbrsAnalyzer} and the common code on a synthetic extraction of two maps */
public class TopAbbrsAnalyzerTest {

	private static final String NY = "Us_new-york_northamerica";
	private static final String DE = "Germany_niedersachsen_europe";
	private static final String IT = "Italy_trentino-alto-adige_europe";

	@ClassRule
	public static TemporaryFolder tmp = new TemporaryFolder();

	private static SearchModLocales locales;
	private static RegionTree regions;
	private static PoiExtraction extraction;
	private static List<TopAbbrRow> rows;
	private static TopAbbrsAnalyzer analyzer;

	private static class MapWriter {
		final StringBuilder entities = new StringBuilder(PoiNamesExtractor.ENTITIES_HEADER + "\n");
		final StringBuilder records = new StringBuilder(PoiNamesExtractor.RECORDS_HEADER + "\n");
		long id;

		MapWriter(long firstId) {
			this.id = firstId;
		}

		void add(double lat, double lon, String subtype, String... tags) {
			Map<String, String> t = new TreeMap<>();
			for (int i = 0; i < tags.length; i += 2) {
				t.put(tags[i], tags[i + 1]);
			}
			long osmId = id++;
			entities.append("node\t").append(osmId).append('\t').append(new Gson().toJson(t)).append('\n');
			records.append("node\t").append(osmId).append("\t0\t").append(osmId * 2).append("\tother\t")
					.append(subtype).append("\t\t").append(lat).append('\t').append(lon).append('\n');
		}

		void write(File dir, String map) throws IOException {
			gzip(new File(dir, map + PoiNamesExtractor.ENTITIES_EXT), entities.toString());
			gzip(new File(dir, map + PoiNamesExtractor.RECORDS_EXT), records.toString());
		}

		private static void gzip(File f, String s) throws IOException {
			try (Writer w = new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(f)), StandardCharsets.UTF_8)) {
				w.write(s);
			}
		}
	}

	@BeforeClass
	public static void analyze() throws Exception {
		File dir = tmp.newFolder("extraction");
		MapWriter ny = new MapWriter(1);
		String dmv = "New York State Department of Motor Vehicles";
		ny.add(40.81, -73.94, "government", "name", "Harlem DMV", "operator", dmv);
		ny.add(40.75, -73.84, "government", "name", "DMV Queens", "operator", dmv);
		ny.add(40.84, -73.86, "government", "name", "Bronx DMV Office", "operator", dmv);
		ny.add(40.78, -73.83, "government", "name", "Queens North Traffic Violations Bureau", "operator", dmv);
		ny.add(40.58, -74.15, "government", "operator", dmv);
		// a brand: bp of blanco petroli is dropped
		for (int i = 0; i < 4; i++) {
			ny.add(40.6 + i * 0.01, -73.9, "fuel", "name", "BP " + i, "operator", "Blanco Petroli", "brand", "BP");
		}
		// H5 object: the phrase without the operator
		ny.add(40.65, -73.95, "government", "name", "Kings County Department of Motor Vehicles");
		ny.write(dir, NY);

		MapWriter de = new MapWriter(1000);
		String[] cities = { "Hannover", "Braunschweig", "Oldenburg", "Osnabrueck", "Goettingen", "Wolfsburg" };
		double[][] at = { { 52.37, 9.74 }, { 52.25, 10.54 }, { 53.14, 8.22 }, { 52.27, 8.06 }, { 51.54, 9.93 },
				{ 52.43, 10.79 } };
		for (int i = 0; i < 5; i++) {
			de.add(at[i][0], at[i][1], "railway_station", "name", cities[i] + " Hauptbahnhof", "short_name",
					cities[i] + " Hbf");
		}
		// a station without the short name gains Hbf
		de.add(at[5][0], at[5][1], "railway_station", "name", cities[5] + " Hauptbahnhof");
		de.write(dir, DE);

		locales = new SearchModRules().locales();
		regions = RegionTree.fromOsmandRegions();
		extraction = new PoiExtractionReader(regions, 2).read(List.of(dir));
		analyzer = new TopAbbrsAnalyzer(extraction, rules(null), locales, CommonWordsMultiIndex.getInstance(), regions,
				MapPoiTypes.getDefault(), List.of());
		rows = analyzer.run();
	}

	private static TopAbbrsRules rules(String extra) throws IOException {
		try (InputStream is = TopAbbrsAnalyzer.class.getResourceAsStream(TopAbbrsAnalyzer.RULES_RESOURCE)) {
			String xml = new String(is.readAllBytes(), StandardCharsets.UTF_8);
			if (extra != null) {
				xml = xml.replace("</top-abbrs>", extra + "</top-abbrs>");
			}
			return TopAbbrsRules.load(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)),
					MapPoiTypes.getDefault());
		}
	}

	private static TopAbbrRow row(String abbr) {
		for (TopAbbrRow r : rows) {
			if (r.abbrNormalized().equals(abbr)) {
				return r;
			}
		}
		return null;
	}

	@Test
	public void dmvByOperator() {
		TopAbbrRow r = row("dmv");
		assertNotNull(String.valueOf(rows), r);
		assertEquals(TopAbbrRow.KIND_ORG, r.kind);
		assertEquals("en_US", r.locale);
		assertEquals("DMV", r.abbr);
		assertEquals("department of motor vehicles", r.full);
		assertEquals(TopAbbrRow.BIND_OPERATOR, r.bind);
		assertEquals("new york state department of motor vehicles", r.bindValue);
		assertEquals(Integer.valueOf(5), r.groupSize);
		assertEquals(Integer.valueOf(3), r.supportObjects);
		// the bureau and the office without a name
		assertEquals(Integer.valueOf(2), r.gainObjects);
		assertTrue(r.sources.contains("h2") && r.sources.contains("h3"));
	}

	@Test
	public void hbfIsWord() {
		TopAbbrRow r = row("hbf");
		assertNotNull(String.valueOf(rows), r);
		assertEquals(TopAbbrRow.KIND_WORD, r.kind);
		assertEquals("de_DE", r.locale);
		assertEquals("Hbf", r.abbr);
		assertEquals("hauptbahnhof", r.full);
		assertEquals(TopAbbrRow.BIND_PHRASE, r.bind);
		assertEquals(Set.of("railway_station"), r.types);
		assertEquals(Integer.valueOf(5), r.supportObjects);
		assertEquals(Integer.valueOf(1), r.gainObjects);
	}

	@Test
	public void brandIsDropped() {
		assertNull(row("bp"));
		boolean brand = false;
		for (TopAbbrsAnalyzer.GroupCandidate g : analyzer.groups) {
			if (g.abbr.equals("bp")) {
				assertEquals("brand", g.status);
				brand = true;
			}
		}
		assertTrue(brand);
	}

	@Test
	public void deterministic() throws IOException {
		TopAbbrsAnalyzer again = new TopAbbrsAnalyzer(extraction, rules(null), locales,
				CommonWordsMultiIndex.getInstance(), regions, MapPoiTypes.getDefault(), List.of());
		assertEquals(csv(rows), csv(again.run()));
	}

	@Test
	public void csvRoundTrip() throws IOException {
		String csv = csv(rows);
		assertEquals(csv, csv(TopAbbrRow.readAll(new StringReader(csv), MapPoiTypes.getDefault())));
	}

	@Test
	public void includeAndExcludeWithOneKey() throws IOException {
		try {
			rules("<include kind=\"org\" locale=\"de\" abbr=\"ZOB\" full=\"zentraler omnibusbahnhof\" bind=\"phrase\" "
					+ "require_absent=\"\" types=\"bus_station\"/><exclude abbr=\"zob\"/>");
			fail("include and exclude with one key");
		} catch (IOException e) {
			assertTrue(e.getMessage(), e.getMessage().contains("one key"));
		}
		try {
			rules("<pairs><source id=\"near\" span=\"true\"/></pairs>");
			fail("span of near");
		} catch (IOException e) {
			assertTrue(e.getMessage(), e.getMessage().contains("span"));
		}
		try {
			rules("<pairs><accept minSupport=\"3\" unknown=\"1\"/></pairs>");
			fail("unknown attribute");
		} catch (IOException e) {
			assertTrue(e.getMessage(), e.getMessage().contains("unknown"));
		}
	}

	@Test
	public void excludeVetoes() throws IOException {
		TopAbbrsAnalyzer a = new TopAbbrsAnalyzer(extraction, rules("<exclude abbr=\"dmv\"/>"), locales,
				CommonWordsMultiIndex.getInstance(), regions, MapPoiTypes.getDefault(), List.of());
		for (TopAbbrRow r : a.run()) {
			assertFalse(r.abbrNormalized().equals("dmv"));
		}
	}

	@Test
	public void predicateOfPhraseAndAbsentTag() {
		TopAbbrPredicate p = new TopAbbrPredicate(locales, regions);
		TopAbbrRow h5 = new TopAbbrRow();
		h5.kind = TopAbbrRow.KIND_ORG;
		h5.locale = "en_US";
		h5.abbr = "DMV";
		h5.full = "department of motor vehicles";
		h5.bind = TopAbbrRow.BIND_PHRASE;
		h5.bindValue = "department of motor vehicles";
		h5.requireAbsent = "operator";
		List<String> applied = new ArrayList<>();
		for (ExtractedEntity e : extraction.entities()) {
			for (ExtractedPoi poi : e.pois) {
				if (p.applies(h5, poi)) {
					applied.add(e.tag("name"));
				}
			}
		}
		// the objects with the operator are out, Harlem DMV has the abbreviation
		assertEquals(List.of("Kings County Department of Motor Vehicles"), applied);
	}

	@Test
	public void nameOfAnotherLanguage() {
		TopAbbrPredicate p = new TopAbbrPredicate(locales, regions);
		assertEquals("it_IT", p.mapLocale(IT));
		// name:de of an Italian map is de_IT: a "de" word row covers it, an "it" row does not
		assertTrue(p.localeMatches("de", locales.forName("name:de", "it_IT")));
		assertFalse(p.localeMatches("it", locales.forName("name:de", "it_IT")));
	}

	@Test
	public void matcher() {
		Set<String> stop = Set.of("of");
		assertTrue(AbbrMatcher.abbrOfWord("hbf", "hauptbahnhof"));
		assertFalse(AbbrMatcher.abbrOfWord("bhf", "hauptbahnhof"));
		assertTrue(AbbrMatcher.abbrOfWords("dmv", List.of("department", "of", "motor", "vehicles"), 3, stop));
		Object[] h3 = AbbrMatcher.spanAcronym("dmv",
				List.of("new", "york", "state", "department", "of", "motor", "vehicles"), 8, 3, stop, 7);
		assertEquals(AbbrMatcher.ACRONYM, h3[0]);
		assertEquals(3, h3[1]);
		AbbrMatcher.Pair d = AbbrMatcher.diffPair(List.of("hannover", "hauptbahnhof"), List.of("hannover", "hbf"), 3, 3,
				stop);
		assertEquals("hbf", d.abbr);
		assertEquals(List.of("hauptbahnhof"), d.full);
		assertFalse(d.shortInFirst);
		assertEquals(List.of("hannover", "hbf"), AbbrText.words("Hannover Hbf."));
		assertEquals("Hbf.", AbbrText.spelledWords("Hannover Hbf.").get(1).original);
	}

	private static String csv(List<TopAbbrRow> rows) throws IOException {
		StringWriter w = new StringWriter();
		TopAbbrRow.writeAll(w, rows);
		return w.toString();
	}
}
