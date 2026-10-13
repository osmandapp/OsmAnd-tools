package net.osmand.obf.preparation;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.zip.GZIPInputStream;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import net.osmand.IProgress;
import net.osmand.ResultMatcher;
import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.binary.BinaryMapIndexReader.SearchRequest;
import net.osmand.binary.BinaryMapPoiReaderAdapter.PoiRegion;
import net.osmand.data.Amenity;
import net.osmand.util.PoiNamesExtractionSummary;

import static org.junit.Assert.*;

/** the POI extraction of the generation ({@link PoiNamesExtractor}) on {@code poi-extraction/luxembourg_europe.osm} */
public class PoiNamesExtractorTest {

	private static final String MAP = "Luxembourg_europe";
	private static final long LAST_MODIFIED = 1_700_000_000_000L;

	@ClassRule
	public static TemporaryFolder tmp = new TemporaryFolder();

	private static File obfWithExtraction;
	private static File obfWithoutExtraction;
	private static File extractionDir;
	private static List<String[]> entities;
	private static List<String[]> records;

	@BeforeClass
	public static void generate() throws Exception {
		extractionDir = tmp.newFolder("extraction");
		obfWithExtraction = generate(tmp.newFolder("with"), extractionDir.getAbsolutePath());
		obfWithoutExtraction = generate(tmp.newFolder("without"), null);
		entities = read(new File(extractionDir, MAP + PoiNamesExtractor.ENTITIES_EXT), PoiNamesExtractor.ENTITIES_HEADER);
		records = read(new File(extractionDir, MAP + PoiNamesExtractor.RECORDS_EXT), PoiNamesExtractor.RECORDS_HEADER);
	}

	private static File generate(File dir, String poiExtractionDir) throws Exception {
		IndexCreatorSettings settings = new IndexCreatorSettings();
		settings.indexPOI = true;
		settings.poiExtractionDir = poiExtractionDir;
		IndexCreator creator = new IndexCreator(dir, settings);
		creator.setMapFileName(MAP + ".obf");
		creator.setLastModifiedDate(LAST_MODIFIED);
		return creator.generateIndexes(source(), IProgress.EMPTY_PROGRESS, null, null, null, null);
	}

	private static File source() throws URISyntaxException {
		return new File(PoiNamesExtractorTest.class.getResource("/poi-extraction/luxembourg_europe.osm").toURI());
	}

	@Test
	public void extractionDoesNotChangeObf() throws IOException {
		// the generation is not byte for byte stable (order of the strings of a POI), so POI are compared
		assertEquals(obfWithoutExtraction.length(), obfWithExtraction.length());
		assertEquals(describe(readObf(obfWithoutExtraction)), describe(readObf(obfWithExtraction)));
		assertEquals(Set.of(MAP + PoiNamesExtractor.ENTITIES_EXT, MAP + PoiNamesExtractor.RECORDS_EXT),
				Set.of(Objects.requireNonNull(extractionDir.list())));
	}

	@Test
	public void recordsAreThePoiOfObf() throws IOException {
		Set<String> obf = new TreeSet<>();
		for (Amenity a : readObf(obfWithExtraction)) {
			obf.add(a.getId() + " " + a.getType().getKeyName() + " " + a.getSubType());
		}
		// node 1003 has no name, operator or network: its OBF name comes from brand
		assertTrue(obf.remove((1003 << 1) + " sustenance cafe"));
		Set<String> extracted = new TreeSet<>();
		for (String[] r : records) {
			extracted.add(r[3] + " " + r[4] + " " + r[5]);
		}
		assertEquals(obf, extracted);
	}

	private static Set<String> describe(List<Amenity> amenities) {
		Set<String> s = new TreeSet<>();
		for (Amenity a : amenities) {
			StringBuilder b = new StringBuilder();
			b.append(a.getId()).append(' ').append(a.getType().getKeyName()).append(' ').append(a.getSubType())
					.append(' ').append(a.getLocation()).append(' ').append(a.getName())
					.append(' ').append(new TreeMap<>(a.getNamesMap(true)));
			for (String k : new TreeSet<>(a.getAdditionalInfoKeys())) {
				b.append(' ').append(k).append('=').append(a.getAdditionalInfo(k));
			}
			s.add(b.toString());
		}
		return s;
	}

	@Test
	public void relationCenters() {
		List<String[]> park = recordsOf("relation", "4001");
		assertEquals(2, park.size());
		assertEquals("0", park.get(0)[2]);
		assertEquals("1", park.get(1)[2]);
		assertEquals(Long.parseLong(park.get(0)[3]) + 2, Long.parseLong(park.get(1)[3]));
		assertEquals("park", park.get(0)[5]);
		assertTrue(entity("relation", "4001")[2].contains("\"name\":\"Twin Park\""));
	}

	@Test
	public void entityWithTwoTypes() {
		List<String[]> poi = recordsOf("node", "1001");
		assertEquals(2, poi.size());
		Set<String> subtypes = new TreeSet<>();
		for (String[] r : poi) {
			assertEquals("0", r[2]);
			subtypes.add(r[5]);
		}
		assertEquals(Set.of("cafe", "bakery"), subtypes);
		String tags = entity("node", "1001")[2];
		assertEquals("{\"name\":\"Two Types\",\"name:de\":\"Zwei Typen\",\"operator\":\"Bakery Co\"}", tags);
	}

	@Test
	public void operatorWithoutName() {
		assertEquals(1, recordsOf("node", "1002").size());
		assertTrue(entity("node", "1002")[2].contains("\"operator:short\":\"NYSDMV\""));
	}

	@Test
	public void notExtracted() {
		// no name, operator or network
		assertTrue(recordsOf("node", "1003").isEmpty());
        assertNull(entity("node", "1003"));
		// out of the map boundary
		assertTrue(recordsOf("node", "1004").isEmpty());
        assertNull(entity("node", "1004"));
		// untagged members of the multipolygon
		assertTrue(recordsOf("way", "3001").isEmpty());
		assertFalse(entities.isEmpty());
	}

	@Test
	public void summary() throws IOException {
		PrintStream out = System.out;
		ByteArrayOutputStream bytes = new ByteArrayOutputStream();
		System.setOut(new PrintStream(bytes, true, StandardCharsets.UTF_8));
		try {
			PoiNamesExtractionSummary.main(new String[] { extractionDir.getAbsolutePath() });
		} finally {
			System.setOut(out);
		}
		String s = bytes.toString(StandardCharsets.UTF_8);
		assertTrue(s, s.contains("entities: 3 "));
		assertTrue(s, s.contains("with operator: 2 "));
		assertTrue(s, s.contains("POI: 5 "));
		assertTrue(s, s.contains("relation centers > 0: 1, entities with several POI: 2"));
	}

	private static List<String[]> recordsOf(String osmType, String osmId) {
		List<String[]> l = new ArrayList<>();
		for (String[] r : records) {
			if (r[0].equals(osmType) && r[1].equals(osmId)) {
				l.add(r);
			}
		}
		l.sort((a, b) -> Integer.compare(Integer.parseInt(a[2]), Integer.parseInt(b[2])));
		return l;
	}

	private static String[] entity(String osmType, String osmId) {
		String[] found = null;
		for (String[] e : entities) {
			if (e[0].equals(osmType) && e[1].equals(osmId)) {
                assertNull("entity is written once", found);
				found = e;
			}
		}
		return found;
	}

	private static List<String[]> read(File f, String header) throws IOException {
		List<String[]> rows = new ArrayList<>();
		try (BufferedReader r = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(f)),
				StandardCharsets.UTF_8))) {
			assertEquals(header, r.readLine());
			int columns = header.split("\t").length;
			String l;
			while ((l = r.readLine()) != null) {
				String[] c = l.split("\t", -1);
				assertEquals(l, columns, c.length);
				rows.add(c);
			}
		}
		return rows;
	}

	private static List<Amenity> readObf(File obf) throws IOException {
		List<Amenity> result = new ArrayList<>();
		try (RandomAccessFile raf = new RandomAccessFile(obf, "r")) {
			BinaryMapIndexReader reader = new BinaryMapIndexReader(raf, obf);
			for (PoiRegion p : reader.getPoiIndexes()) {
				SearchRequest<Amenity> req = BinaryMapIndexReader.buildSearchPoiRequest(0, Integer.MAX_VALUE, 0,
						Integer.MAX_VALUE, -1, BinaryMapIndexReader.ACCEPT_ALL_POI_TYPE_FILTER,
						new ResultMatcher<Amenity>() {
							@Override
							public boolean publish(Amenity a) {
								result.add(a);
								return false;
							}

							@Override
							public boolean isCancelled() {
								return false;
							}
						});
				reader.searchPoi(req, p);
			}
		}
		return result;
	}
}
