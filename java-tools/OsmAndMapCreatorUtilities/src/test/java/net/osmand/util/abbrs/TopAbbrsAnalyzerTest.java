package net.osmand.util.abbrs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
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
import java.time.LocalDate;
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.zip.GZIPOutputStream;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.google.gson.Gson;

import net.osmand.binary.ObfConstants;
import net.osmand.obf.preparation.CommonWordsMultiIndex;
import net.osmand.obf.preparation.PoiNamesExtractor;
import net.osmand.obf.preparation.PoiNamesExtractor.Meta;
import net.osmand.osm.MapPoiTypes;
import net.osmand.search.rules.SearchModLocales;
import net.osmand.search.rules.SearchModRules;
import net.osmand.util.MapUtils;
import net.osmand.util.abbrs.PoiExtraction.ExtractedEntity;
import net.osmand.util.abbrs.PoiExtraction.ExtractedMap;
import net.osmand.util.abbrs.PoiExtraction.ExtractedPoi;

/** {@link TopAbbrsAnalyzer} and the common code on synthetic extractions */
public class TopAbbrsAnalyzerTest {

	private static final String NY = "Us_new-york_northamerica";
	private static final String DE = "Germany_niedersachsen_europe";
	private static final String IT = "Italy_trentino-alto-adige_europe";
	private static final YearMonth PERIOD = YearMonth.of(2026, 10);
	private static final long DATA = date(2026, 10, 1);

	@ClassRule
	public static TemporaryFolder tmp = new TemporaryFolder();

	private static SearchModLocales locales;
	private static RegionTree regions;
	private static PoiExtraction extraction;
	private static List<TopAbbrRow> rows;
	private static TopAbbrsAnalyzer analyzer;

	/** an extraction of a map as {@link PoiNamesExtractor} writes it; the indexed names are the name tags */
	private static class MapWriter {
		final StringBuilder entities = new StringBuilder(PoiNamesExtractor.ENTITIES_HEADER + "\n");
		final StringBuilder records = new StringBuilder(PoiNamesExtractor.RECORDS_HEADER + "\n");
		long id;

		MapWriter(long firstId) {
			this.id = firstId;
		}

		/** a node with one POI */
		long add(double lat, double lon, String subtype, String... tags) {
			long osmId = id++;
			entity("node", osmId, tags);
			poi("node", osmId, 0, subtype, lat, lon, tags);
			return osmId;
		}

		void entity(String osmType, long osmId, String... tags) {
			entities.append(osmType).append('\t').append(osmId).append('\t').append(new Gson().toJson(tags(tags)))
					.append('\n');
		}

		void poi(String osmType, long osmId, int center, String subtype, double lat, double lon, String... tags) {
			Map<String, String> names = new TreeMap<>();
			tags(tags).forEach((k, v) -> {
				if (ObfConstants.isTagIndexedForSearchAsName(k)) {
					names.put(k, v);
				}
			});
			records.append(osmType).append('\t').append(osmId).append('\t').append(center).append('\t')
					.append(osmId * 2 + center * 2L).append("\tother\t").append(subtype).append("\t\t").append(lat)
					.append('\t').append(lon).append('\t').append(new Gson().toJson(names)).append('\n');
		}

		private static Map<String, String> tags(String... tags) {
			Map<String, String> t = new TreeMap<>();
			for (int i = 0; i < tags.length; i += 2) {
				t.put(tags[i], tags[i + 1]);
			}
			return t;
		}

		void write(File dir, String map, long dataDate) throws IOException {
			write(dir, map, new Meta(dataDate, dataDate), new Meta(dataDate, dataDate));
		}

		void write(File dir, String map, Meta entitiesMeta, Meta recordsMeta) throws IOException {
			gzip(new File(dir, map + PoiNamesExtractor.ENTITIES_EXT), entitiesMeta.line() + "\n" + entities);
			gzip(new File(dir, map + PoiNamesExtractor.RECORDS_EXT), recordsMeta.line() + "\n" + records);
		}

		private static void gzip(File f, String s) throws IOException {
			try (Writer w = new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(f)), StandardCharsets.UTF_8)) {
				w.write(s);
			}
		}
	}

	private static long date(int y, int m, int d) {
		return LocalDate.of(y, m, d).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
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
		// operator:short: USPS is the brand of the post offices too, the tag confirms the abbreviation
		String usps = "United States Postal Service";
		for (int i = 0; i < 4; i++) {
			ny.add(40.70 + i * 0.01, -73.80, "post_office", "name", "Station " + (char) ('A' + i) + " Post Office",
					"operator", usps, "operator:short", "USPS", "brand", i < 2 ? "USPS" : "");
		}
		// a value of the operator is the acronym of another value
		for (int i = 0; i < 6; i++) {
			ny.add(40.72 + i * 0.01, -73.99, "bus_stop", "name", "Broadway / " + (i + 10) + " Street", "operator", "MTA");
		}
		for (int i = 0; i < 4; i++) {
			ny.add(40.62 + i * 0.01, -73.97, "bus_stop", "name", "Ocean Avenue / Avenue " + (char) ('J' + i),
					"operator", "Metropolitan Transportation Authority");
		}
		// a short value of two unrelated long values; a loose acronym (psa of "post agency") is none
		for (int i = 0; i < 5; i++) {
			ny.add(40.55 + i * 0.01, -73.95, "parking", "name", "Lot " + (i + 1), "operator", "PSC");
			ny.add(40.55 + i * 0.01, -73.94, "parking", "name", "Garage " + (i + 1), "operator", "PSA");
		}
		for (int i = 0; i < 3; i++) {
			ny.add(40.50 + i * 0.01, -73.93, "parking", "name", "Deck " + (i + 1), "operator", "Parking Supply Company");
			ny.add(40.50 + i * 0.01, -73.92, "parking", "name", "Yard " + (i + 1), "operator", "Public Safety Center");
			ny.add(40.50 + i * 0.01, -73.91, "parking", "name", "Ramp " + (i + 1), "operator", "Post Agency");
		}
		// two objects of six with the abbreviation are below minSupport
		String csi = "College of Staten Island";
		ny.add(40.601, -74.149, "bus_stop", "name", "CSI Loop Bus Stop", "operator", csi);
		ny.add(40.602, -74.148, "bus_stop", "name", "CSI Campus Loop Bus Stop", "operator", csi);
		for (int i = 0; i < 4; i++) {
			ny.add(40.603 + i * 0.001, -74.147, "university", "name", "Building " + (i + 1) + "N", "operator", csi);
		}
		ny.write(dir, NY, DATA);

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
		de.write(dir, DE, DATA);

		locales = new SearchModRules().locales();
		regions = RegionTree.fromOsmandRegions();
		extraction = new PoiExtractionReader(regions, 2, PERIOD).read(List.of(dir));
		analyzer = analyzer(extraction, rules(null));
		rows = analyzer.run();
	}

	private static TopAbbrsAnalyzer analyzer(PoiExtraction e, TopAbbrsRules r) {
		return new TopAbbrsAnalyzer(e, r, locales, CommonWordsMultiIndex.getInstance(), regions,
				MapPoiTypes.getDefault(), List.of());
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

	private static TopAbbrRow hbfRow(String locale) {
		TopAbbrRow r = new TopAbbrRow();
		r.kind = TopAbbrRow.KIND_WORD;
		r.locale = locale;
		r.abbr = "Hbf";
		r.full = "hauptbahnhof";
		r.bind = TopAbbrRow.BIND_PHRASE;
		r.bindValue = "hauptbahnhof";
		return r;
	}

	private static ExtractedPoi poi(PoiExtraction e, String osmType, long osmId) {
		ExtractedEntity entity = e.entity(osmType, osmId);
		assertNotNull(osmType + "/" + osmId, entity);
		return entity.pois.get(0);
	}

	private static ExtractedMap map(PoiExtraction e, String name) {
		for (ExtractedMap m : e.maps()) {
			if (m.name.equals(name)) {
				return m;
			}
		}
		throw new AssertionError(name);
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
		assertEquals(csv(rows), csv(analyzer(extraction, rules(null)).run()));
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
		for (TopAbbrRow r : analyzer(extraction, rules("<exclude abbr=\"dmv\"/>")).run()) {
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
	public void orgRowOfLanguage() {
		// <include kind="org" locale="de"> of the spec applies on a map of de_DE
		TopAbbrPredicate p = new TopAbbrPredicate(locales, regions);
		ExtractedPoi station = poi(extraction, "node", 1005);
		assertEquals("de_DE", p.mapLocale(station.map));
		TopAbbrRow r = hbfRow("de");
		r.kind = TopAbbrRow.KIND_ORG;
		assertTrue(p.applies(r, station));
		r.locale = "de_DE";
		assertTrue(p.applies(r, station));
		r.locale = "de_AT";
		assertFalse(p.applies(r, station));
		r.locale = "en";
		assertFalse(p.applies(r, station));
	}

	@Test
	public void compositeSubtype() throws IOException {
		File dir = tmp.newFolder("composite");
		MapWriter ny = new MapWriter(1);
		String op = "Metro Food Services";
		for (int i = 0; i < 3; i++) {
			// amenity=cafe;restaurant: one POI of the subtype "cafe;restaurant"
			ny.add(40.7 + i * 0.01, -73.9, "cafe;restaurant", "name", "MFS " + i, "operator", op);
		}
		long diner = ny.add(40.75, -73.9, "cafe;restaurant", "name", "Downtown Diner", "operator", op);
		ny.write(dir, NY, DATA);
		PoiExtraction e = new PoiExtractionReader(regions, 2, PERIOD).read(List.of(dir));
		ExtractedPoi p = poi(e, "node", 1);
		// the identity keeps the subtype of the OBF, the types are its keys
		assertTrue(p.identity(), p.identity().endsWith("/cafe;restaurant"));
		assertTrue(p.hasType(Set.of("cafe")));
		assertTrue(p.hasType(Set.of("restaurant")));
		assertFalse(p.hasType(Set.of("cafe;restaurant")));
		assertEquals(Set.of("cafe", "restaurant"), p.entity.types());

		TopAbbrsAnalyzer a = analyzer(e, rules(null));
		List<TopAbbrRow> found = a.run();
		TopAbbrRow mfs = null;
		for (TopAbbrRow r : found) {
			if (r.abbrNormalized().equals("mfs")) {
				mfs = r;
			}
		}
		assertNotNull(String.valueOf(found), mfs);
		assertEquals(Set.of("cafe", "restaurant"), mfs.types);
		assertNull(TopAbbrRow.unknownType(mfs.types, MapPoiTypes.getDefault()));
		assertEquals(Integer.valueOf(1), mfs.gainObjects);
		// a row of one key applies
		mfs.types = new TreeSet<>(Set.of("cafe"));
		assertTrue(new TopAbbrPredicate(locales, regions).applies(mfs, poi(e, "node", diner)));
	}

	@Test
	public void entityOfTheNewestData() throws IOException {
		File dir = tmp.newFolder("versions");
		// new data: a restaurant; old data: the cafe it was, the old map is generated later
		MapWriter fresh = new MapWriter(5);
		fresh.add(52.0, 9.0, "restaurant", "name", "New Restaurant");
		fresh.entity("relation", 7, "name", "Twin Park");
		fresh.poi("relation", 7, 0, "park", 52.1, 9.1, "name", "Twin Park");
		fresh.write(dir, "Germany_europe", new Meta(DATA, date(2026, 10, 5)), new Meta(DATA, date(2026, 10, 5)));
		MapWriter old = new MapWriter(5);
		old.add(52.0, 9.0, "cafe", "name", "Old Cafe");
		old.write(dir, "Germany_niedersachsen_europe", new Meta(date(2026, 9, 1), date(2026, 10, 9)),
				new Meta(date(2026, 9, 1), date(2026, 10, 9)));
		// the same data: another center of the park is in another map
		MapWriter part = new MapWriter(100);
		part.entity("relation", 7, "name", "Twin Park");
		part.poi("relation", 7, 1, "park", 52.2, 9.2, "name", "Twin Park");
		part.write(dir, "Germany_bremen_europe", DATA);

		PoiExtraction e = new PoiExtractionReader(null, 2, PERIOD).read(List.of(dir));
		ExtractedEntity restaurant = e.entity("node", 5);
		assertEquals("New Restaurant", restaurant.tag("name"));
		assertEquals(Set.of("restaurant"), restaurant.types());
		assertEquals("Germany_europe", restaurant.map);
		assertEquals(1, e.stalePois());
		assertEquals(1, map(e, "Germany_niedersachsen_europe").stalePois);
		ExtractedEntity park = e.entity("relation", 7);
		assertEquals(2, park.pois.size());
		assertEquals("Germany_europe", park.map);
	}

	@Test
	public void ageOfTheAnalyzedPeriod() throws IOException {
		File dir = tmp.newFolder("age");
		MapWriter m = new MapWriter(1);
		m.add(52.0, 9.0, "cafe", "name", "Cafe");
		// 100 days before the period: the only extraction, it is still too old
		m.write(dir, DE, date(2026, 7, 5));
		PoiExtraction e = new PoiExtractionReader(regions, 2, PERIOD).read(List.of(dir));
		assertFalse(map(e, DE).used);
		assertTrue(map(e, DE).skipReason, map(e, DE).skipReason.contains("older than 2 months"));
		assertTrue(e.entities().isEmpty());
		// two months before the period
		m.write(dir, DE, date(2026, 8, 1));
		assertTrue(map(new PoiExtractionReader(regions, 2, PERIOD).read(List.of(dir)), DE).used);
		// after the period
		m.write(dir, DE, date(2026, 11, 1));
		assertFalse(map(new PoiExtractionReader(regions, 2, PERIOD).read(List.of(dir)), DE).used);
	}

	@Test
	public void filesOfDifferentExtractions() throws IOException {
		File dir = tmp.newFolder("pair");
		MapWriter m = new MapWriter(1);
		m.add(52.0, 9.0, "cafe", "name", "Cafe");
		m.write(dir, DE, new Meta(DATA, 1), new Meta(DATA, 2));
		PoiExtraction e = new PoiExtractionReader(regions, 2, PERIOD).read(List.of(dir));
		assertFalse(map(e, DE).used);
		assertEquals("entities of another extraction", map(e, DE).skipReason);
	}

	@Test
	public void gainByIndexedNames() throws IOException {
		File dir = tmp.newFolder("names");
		MapWriter de = new MapWriter(1);
		// the names of the name index decide, not the raw tags: official_name is not indexed here
		long notIndexed = de.id++;
		de.entity("node", notIndexed, "name", "Celle Hauptbahnhof", "official_name", "Celle Hbf");
		de.poi("node", notIndexed, 0, "railway_station", 52.0, 9.0, "name", "Celle Hauptbahnhof");
		// brand is indexed: the station has Hbf
		long brand = de.add(52.1, 9.1, "railway_station", "name", "Uelzen Hauptbahnhof", "brand", "Hbf");
		de.write(dir, DE, DATA);
		PoiExtraction e = new PoiExtractionReader(regions, 2, PERIOD).read(List.of(dir));
		TopAbbrPredicate p = new TopAbbrPredicate(locales, regions);
		assertTrue(p.applies(hbfRow("de"), poi(e, "node", notIndexed)));
		assertTrue(TopAbbrPredicate.hasWord(poi(e, "node", brand), "hbf"));
		assertFalse(p.applies(hbfRow("de"), poi(e, "node", brand)));
	}

	@Test
	public void nearByLongitudeAndCenters() throws IOException {
		File dir = tmp.newFolder("near");
		MapWriter de = new MapWriter(1);
		double cellLat = 500 / 111_000.0;
		// at 60 degrees: 445 m of longitude are 0.008 degrees, two cells of the size of the latitude
		double lon = 2220 * cellLat + 0.0044;
		de.add(60.0, lon, "railway_station", "name", "Oslo Hauptbahnhof");
		de.add(60.0, lon + 0.008, "railway_station", "name", "Oslo Hbf");
		// control: 222 m
		de.add(60.0, lon, "railway_station", "name", "Kiel Hauptbahnhof");
		de.add(60.0, lon + 0.004, "railway_station", "name", "Kiel Hbf");
		// a relation with its first center 1.37 km away and the second one next to the neighbor
		de.entity("relation", 9, "name", "Bremen Hauptbahnhof");
		de.poi("relation", 9, 0, "railway_station", 53.0, 8.0, "name", "Bremen Hauptbahnhof");
		de.poi("relation", 9, 1, "railway_station", 53.0, 8.0205, "name", "Bremen Hauptbahnhof");
		de.add(53.0, 8.021, "railway_station", "name", "Bremen Hbf");
		de.write(dir, DE, DATA);
		assertTrue(MapUtils.getDistance(60, lon, 60, lon + 0.008) < 500);
		assertTrue(MapUtils.getDistance(53, 8.0, 53, 8.021) > 1000);

		PoiExtraction e = new PoiExtractionReader(regions, 2, PERIOD).read(List.of(dir));
		TopAbbrsAnalyzer a = analyzer(e, rules(null));
		a.keepNoise = true;
		a.run();
		TopAbbrsAnalyzer.H1Candidate c = a.h1.get("de_DE|hbf|hauptbahnhof");
		assertNotNull(String.valueOf(a.h1.keySet()), c);
		assertEquals(3, c.near);
		assertEquals(Set.of("node/1", "node/3", "relation/9"), c.entities);
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
	@Test
	public void shortTagOfTheOperator() {
		TopAbbrRow r = row("usps");
		assertNotNull(String.valueOf(rows), r);
		assertEquals("USPS", r.abbr);
		assertEquals("united states postal service", r.full);
		assertEquals(TopAbbrRow.BIND_OPERATOR, r.bind);
		assertTrue(r.sources.toString(), r.sources.contains("short"));
		// brand=USPS is a name of the name index: only the offices without it gain
		assertEquals(Integer.valueOf(2), r.gainObjects);
	}

	@Test
	public void valueOfTheOperatorIsAcronymOfAnother() {
		List<TopAbbrRow> mta = new ArrayList<>();
		for (TopAbbrRow r : rows) {
			if (r.abbrNormalized().equals("mta")) {
				mta.add(r);
			}
		}
		assertEquals(String.valueOf(mta), 2, mta.size());
		Set<String> bound = new TreeSet<>();
		for (TopAbbrRow r : mta) {
			assertEquals("metropolitan transportation authority", r.full);
			assertTrue(r.sources.contains("value"));
			bound.add(r.bindValue + " " + r.gainObjects);
		}
		// both groups get the alias: the full value and operator=MTA, not indexed as a name
		assertEquals(Set.of("metropolitan transportation authority 4", "mta 6"), bound);
	}

	@Test
	public void groupWordNeedsSupport() {
		assertNull(row("csi"));
		boolean found = false;
		for (TopAbbrsAnalyzer.GroupCandidate g : analyzer.groups) {
			if (g.abbr.equals("csi")) {
				assertEquals("rejected", g.status);
				assertTrue(g.reason, g.reason.startsWith("support 2"));
				found = true;
			}
		}
		assertTrue(found);
	}

	@Test
	public void shortValueOfUnrelatedValuesIsAmbiguous() {
		assertNull(row("psc"));
		assertNull(row("psa"));
		int psc = 0;
		for (TopAbbrsAnalyzer.GroupCandidate g : analyzer.groups) {
			if (TopAbbrsAnalyzer.SOURCE_VALUE.equals(g.source)) {
				assertNotEquals("post agency", g.value);
			}
			if (g.abbr.equals("psc")) {
				assertEquals("rejected", g.status);
				assertTrue(g.reason, g.reason.startsWith("ambiguous short value"));
				psc++;
			}
		}
		assertEquals(2, psc);
	}
}
