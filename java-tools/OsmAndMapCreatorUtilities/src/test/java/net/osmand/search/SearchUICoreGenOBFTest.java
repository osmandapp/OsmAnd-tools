package net.osmand.search;

import net.osmand.ResultMatcher;
import net.osmand.binary.BinaryMapAddressReaderAdapter;
import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.binary.BinaryMapPoiReaderAdapter;
import net.osmand.binary.BinaryMapRouteReaderAdapter;
import net.osmand.binary.RouteDataObject;
import net.osmand.data.Amenity;
import net.osmand.data.City;
import net.osmand.data.LatLon;
import net.osmand.data.Street;
import net.osmand.data.Building;
import net.osmand.obf.OBFDataCreator;
import net.osmand.obf.preparation.IndexAddressCreator;
import net.osmand.obf.preparation.IndexCreator;
import net.osmand.obf.preparation.IndexPoiCreator;
import net.osmand.osm.AbstractPoiType;
import net.osmand.osm.MapPoiTypes;
import net.osmand.search.core.*;
import net.osmand.util.Algorithms;
import org.apache.commons.codec.digest.DigestUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.xmlpull.v1.XmlPullParserException;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Unit-test class is responsible for:
 * <li> Forward tests validate search phrases and expected result ordering;</li>
 * <li> Reverse geocoding tests are enabled for expected results prefixed with '@' and verify that a found
 * building resolves back to its street, city, and house number through generated routing/address data.</li>
 * <li> Search engine is selected by {@code SPATIAL_SEARCH}: {@code false} uses legacy {@link SearchUICore},
 * {@code true} uses spatial search.</li>
 * <p>
 * Unit-test is converting original OBF files into universal JSON, generating source OBFs from that JSON,
 * and running unit tests against the generated OBFs. Config JSON files from {@link #SEARCH_RESOURCES_PATH}
 * provide mandatory settings, phrases, and expected results; same-basename OBFs in that directory are original
 * transformation inputs unless files are listed explicitly.
 * <p>
 * Generated source JSON and OBF artifacts are cached as {@code *.json.gz} and {@code *.obf.gz} in {@link #GEN_DIR},
 * while plain {@code *.orig.obf}, {@code *.json}, and {@code *.obf} files are temporary.
 */
@RunWith(Parameterized.class)
public class SearchUICoreGenOBFTest {
	private static final String ANDROID_PATH_ENV = "ANDROID_PATH",
			RESOURCES_PATH_ENV = "RESOURCES_PATH",
			SEARCH_RESOURCES_PATH_ENV = "SEARCH_RESOURCES_PATH";
	private static final String RESOURCES_PATH = getResourcesPath();
	private static final String SEARCH_RESOURCES_PATH = getSearchResourcesPath();
	private static final File GEN_DIR = new File(SEARCH_RESOURCES_PATH, "gen-source");
	private static final Set<String> GENERATED_OBFS = Collections.synchronizedSet(new HashSet<>());
	private static final boolean REGENERATE_OBF = true;
	private static final boolean TEST_EXTRA_RESULTS = true;
	private static final List<Class<?>> OBF_GENERATE_CLASSES = List.of(IndexCreator.class, IndexPoiCreator.class,
			IndexAddressCreator.class);
	private static final String OBF_HASH_FILE_NAME = ".obf.hash";
	private static final boolean RUN_IGNORED_TESTS = false;
	private static final boolean TEST_NUMBER_MATCHED = true;
	
	private static final boolean FILTER_DATA_JSON = false;
	private static final double FILTER_REMOVE_PROBABILITY = 0.8; // means 80% probability of removal
	private static boolean HASH_IS_ACTUAL_FOR_RUN;

	private final File testFile;
    private Set<String> searchKeywords;

    public interface SearchTestEngine {
        List<String> apply(String text, List<String> expectedResults) throws IOException;
        
        void close();
    }
    
	public SearchUICoreGenOBFTest(String name, File file) {
		this.testFile = file;
	}

	private static String getResourcesPath() {
		String path = System.getenv(RESOURCES_PATH_ENV);
		if (Algorithms.isEmpty(path)) {
			path = "../../../resources/";
		}
		return path.endsWith("/") || path.endsWith("\\") ? path	: path + File.separator;
	}

	private static String getAndroidPath() {
		String path = System.getenv(ANDROID_PATH_ENV);
		if (Algorithms.isEmpty(path)) {
			path = "../../../android/";
		}
		return path.endsWith("/") || path.endsWith("\\") ? path	: path + File.separator;
	}

	private static String getSearchResourcesPath() {
		String searchResourcesPath = System.getenv(SEARCH_RESOURCES_PATH_ENV);
		if (Algorithms.isEmpty(searchResourcesPath)) {
			searchResourcesPath = RESOURCES_PATH + "test-resources/spatial_search";
		} else {
			searchResourcesPath = RESOURCES_PATH + searchResourcesPath;
		}
		return searchResourcesPath.endsWith("/") || searchResourcesPath.endsWith("\\")
				? searchResourcesPath
				: searchResourcesPath + File.separator;
	}

	@Parameterized.Parameters(name = "{index}: {0}")
	public static Iterable<Object[]> data() throws IOException {
		File[] files = new File(SEARCH_RESOURCES_PATH).listFiles();
		ArrayList<Object[]> arrayList = new ArrayList<>();
		if (files != null) {
			for (File file : files) {
				String fileName = file.getName();
				if (fileName.endsWith(".json")) {
					String sourceJsonText = Algorithms.getFileAsString(file);
					JSONObject sourceJson = new JSONObject(sourceJsonText);
					boolean ignore = sourceJson.optBoolean("ignore");
					if (!ignore || RUN_IGNORED_TESTS) {
						String name = fileName.substring(0, fileName.length() - ".json".length());
						arrayList.add(new Object[] { name, file });
					}
				}
			}
		}
		return arrayList;
	}

	@BeforeClass
	public static void setUp() {
		GENERATED_OBFS.clear();
		HASH_IS_ACTUAL_FOR_RUN = isHashActual();
		if (!HASH_IS_ACTUAL_FOR_RUN) {
			deleteGeneratedFiles(GEN_DIR, ".obf", ".obf.gz");
		}
		defaultSetup();
	}

	@AfterClass
	public static void tearDown() {
		deleteGeneratedFiles(GEN_DIR, ".obf", ".json");
		GENERATED_OBFS.clear();
	}

	static void defaultSetup() {
		MapPoiTypes.setDefault(new MapPoiTypes(RESOURCES_PATH + "poi/poi_types.xml"));
		MapPoiTypes poiTypes = MapPoiTypes.getDefault();
		Map<String, String> enPhrases = new HashMap<>();
		Map<String, String> phrases = new HashMap<>();
		try {
			enPhrases = Algorithms.parseStringsXml(new File(getAndroidPath() + "OsmAnd/res/values/phrases.xml"));
			//phrases = Algorithms.parseStringsXml(new File("src/test/resources/phrases/ru/phrases.xml"));
			phrases = enPhrases;
		} catch (IOException | XmlPullParserException e) {
			e.printStackTrace();
		}

		poiTypes.setPoiTranslator(new TestSearchTranslator(phrases, enPhrases));
	}

	/**
	 * Resolves a same-basename test data chain and returns a readable generated OBF:
	 * <li>Cached {@code *.obf.gz} in {@link #GEN_DIR} is reused when newer than the resolved source JSON; </li>
	 * <li>otherwise cached {@code *.json.gz} is used as source when newer than the original OBF or when no original exists. </li>
	 * <li>If no source cache is valid, the original OBF from {@link #SEARCH_RESOURCES_PATH} is exported to source JSON.
	 * <li>New plain {@code *.json} and {@code *.obf} files are compressed back to {@code *.json.gz} and {@code *.obf.gz} for later runs.
	 * <li>When {@link #REGENERATE_OBF} is {@code false}, the transformation/cache chain is skipped and only the original OBF is used.
	 */
	private File createOBFIfNeeded(String fileName) throws IOException, SQLException {
		String baseName = getBaseName(fileName);
		File originalObf = getNewestExistingFile(
				new File(SEARCH_RESOURCES_PATH, baseName + ".obf"),
				new File(SEARCH_RESOURCES_PATH, baseName + ".obf.gz"));
		if (!REGENERATE_OBF) {
			if (originalObf == null) {
				throw new FileNotFoundException("Original OBF does not exist for " + fileName);
			}
			return prepareOriginalObfFile(originalObf, new File(GEN_DIR, baseName + ".orig.obf"));
		}
		File sourceJson = getNewestExistingFile(
				new File(GEN_DIR, baseName + ".json"),
				new File(GEN_DIR, baseName + ".json.gz"));
		File preparedObf = getNewestExistingFile(
				new File(GEN_DIR, baseName + ".obf"),
				new File(GEN_DIR, baseName + ".obf.gz"));
		if (originalObf == null && sourceJson == null && preparedObf == null) {
			throw new FileNotFoundException("No OBF or source JSON found for " + fileName);
		}

		String generatedObfName = baseName + ".obf";
		File generatedObfFile = new File(GEN_DIR, generatedObfName);
		String obfPath = generatedObfFile.getAbsolutePath();
		synchronized (GENERATED_OBFS) {
			File parent = generatedObfFile.getParentFile();
			if (parent != null && !parent.isDirectory() && !parent.mkdirs()) {
				throw new IOException("Cannot create generated OBF directory " + parent);
			}

			boolean alreadyGenerated = GENERATED_OBFS.contains(obfPath);
			boolean noSourceAvailable = sourceJson == null && originalObf == null;
			boolean canUsePreparedObf = preparedObf != null
					&& (isPreparedObfActual(preparedObf, sourceJson, originalObf) || noSourceAvailable && alreadyGenerated);
			if (canUsePreparedObf) {
				File obfFile = prepareObfFile(preparedObf, generatedObfFile);
				cacheObfIfNeeded(preparedObf, obfFile, baseName);
				GENERATED_OBFS.add(obfPath);
				return obfFile;
			}
			File sourceFile = getSourceFile(baseName, originalObf, sourceJson);
			if (!alreadyGenerated || !generatedObfFile.isFile()
					|| generatedObfFile.lastModified() < sourceFile.lastModified()) {
				OBFDataCreator creator = new OBFDataCreator();
				creator.create(generatedObfFile.getAbsolutePath(), new String[] { sourceFile.getAbsolutePath() });
				writeHash();
			}
			cacheGzipIfNeeded(generatedObfFile, new File(GEN_DIR, baseName + ".obf.gz"));
			GENERATED_OBFS.add(obfPath);
		}
		return generatedObfFile;
	}

	private void cacheObfIfNeeded(File preparedObf, File obfFile, String baseName) throws IOException {
		if (preparedObf != null && preparedObf.getName().endsWith(".obf.gz")) {
			return;
		}
		cacheGzipIfNeeded(obfFile, new File(GEN_DIR, baseName + ".obf.gz"));
	}

	private File getSourceFile(String baseName, File originalObf, File sourceJson) throws IOException {
		if (sourceJson != null && (originalObf == null || sourceJson.lastModified() > originalObf.lastModified())) {
			File sourceFile = prepareJsonFile(sourceJson, new File(GEN_DIR, baseName + ".json"));
			cacheGzipIfNeeded(sourceFile, new File(GEN_DIR, baseName + ".json.gz"));
			return sourceFile;
		}
		if (originalObf == null) {
			throw new FileNotFoundException("No original OBF or prepared source JSON found for " + baseName);
		}
		File sourceObfFile = prepareOriginalObfFile(originalObf, new File(GEN_DIR, baseName + ".orig.obf"));
		sourceObfFile.deleteOnExit();
		return exportSourceJson(sourceObfFile, baseName + ".obf");
	}

	private File prepareOriginalObfFile(File originalObf, File targetFile) throws IOException {
		if (originalObf.getName().endsWith(".gz")) {
			unzipIfNeeded(originalObf, targetFile);
			return targetFile;
		}
		return originalObf;
	}

	private File prepareJsonFile(File jsonFile, File targetFile) throws IOException {
		if (jsonFile.getName().endsWith(".gz")) {
			unzipIfNeeded(jsonFile, targetFile);
			return targetFile;
		}
		return jsonFile;
	}

	private File prepareObfFile(File obfFile, File targetFile) throws IOException {
		if (obfFile.getName().endsWith(".gz")) {
			unzipIfNeeded(obfFile, targetFile);
			return targetFile;
		}
		return obfFile;
	}

	private boolean isPreparedObfActual(File preparedObf, File sourceJson, File originalObf) {
		if (preparedObf == null || !HASH_IS_ACTUAL_FOR_RUN) {
			return false;
		}
		File source = sourceJson != null && (originalObf == null || sourceJson.lastModified() > originalObf.lastModified())
				? sourceJson
				: originalObf;
		return source == null || preparedObf.lastModified() > source.lastModified();
	}

	private static boolean isHashActual() {
		return Algorithms.stringsEqual(getHash(), getObfGenerateHash());
	}

	private static File getObfHashFile() {
		return new File(GEN_DIR, OBF_HASH_FILE_NAME);
	}

	private static String getHash() {
		File hashFile = getObfHashFile();
		if (!hashFile.isFile()) {
			return null;
		}
		String hash = Algorithms.getFileAsString(hashFile);
		return hash == null ? null : hash.trim();
	}

	private void writeHash() throws IOException {
		File hashFile = getObfHashFile();
		File parent = hashFile.getParentFile();
		if (parent != null && !parent.isDirectory() && !parent.mkdirs()) {
			throw new IOException("Cannot create generated OBF directory " + parent);
		}
		try (Writer writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(hashFile), StandardCharsets.UTF_8))) {
			writer.write(getObfGenerateHash());
			writer.write(System.lineSeparator());
		}
	}

	private File getNewestExistingFile(File... files) {
		File newest = null;
		for (File file : files) {
			if (file.isFile() && (newest == null || file.lastModified() > newest.lastModified())) {
				newest = file;
			}
		}
		return newest;
	}

	private String getBaseName(String fileName) {
		if (fileName.endsWith(".obf.gz")) {
			return fileName.substring(0, fileName.length() - ".obf.gz".length());
		}
		if (fileName.endsWith(".json.gz")) {
			return fileName.substring(0, fileName.length() - ".json.gz".length());
		}
		if (fileName.endsWith(".obf")) {
			return fileName.substring(0, fileName.length() - ".obf".length());
		}
		if (fileName.endsWith(".json")) {
			return fileName.substring(0, fileName.length() - ".json".length());
		}
		return fileName;
	}

	private void unzipIfNeeded(File gzFile, File file) throws IOException {
		if (file.isFile() && file.lastModified() >= gzFile.lastModified()) {
			return;
		}
		try (GZIPInputStream inputStream = new GZIPInputStream(new FileInputStream(gzFile));
		     FileOutputStream outputStream = new FileOutputStream(file)) {
			Algorithms.streamCopy(inputStream, outputStream);
		}
		file.setLastModified(gzFile.lastModified());
	}

	private void cacheGzipIfNeeded(File sourceFile, File gzFile) throws IOException {
		if (sourceFile == null || !sourceFile.isFile() || sourceFile.equals(gzFile)) {
			return;
		}
		if (gzFile.isFile() && gzFile.lastModified() >= sourceFile.lastModified()) {
			return;
		}
		try (FileInputStream inputStream = new FileInputStream(sourceFile);
		     GZIPOutputStream outputStream = new GZIPOutputStream(new FileOutputStream(gzFile))) {
			Algorithms.streamCopy(inputStream, outputStream);
		}
		gzFile.setLastModified(sourceFile.lastModified());
	}

	private BinaryMapIndexReader openReader(File obfFile) throws IOException {
		RandomAccessFile raf = new RandomAccessFile(obfFile.getPath(), "r");
		try {
			return new BinaryMapIndexReader(raf, obfFile);
		} catch (IOException | RuntimeException e) {
			try {
				raf.close();
			} catch (IOException ignored) {
			}
			throw e;
		}
	}

	private void loadReaders(JSONObject sourceJson, List<BinaryMapIndexReader> readers) throws IOException, SQLException {
		if (!GEN_DIR.isDirectory() && !GEN_DIR.mkdirs()) {
			throw new IOException("Cannot create generated OBF directory " + GEN_DIR);
		}
		JSONArray filesJson = sourceJson.optJSONArray("files");
		if (filesJson != null) {
			for (int i = 0; i < filesJson.length(); i++) {
				String file = filesJson.optString(i, null);
				if (!Algorithms.isEmpty(file) && isDataFileName(file)) {
					File obfFile = createOBFIfNeeded(file);
					readers.add(openReader(obfFile));
				}
			}
		} else {
			File obfFile = createOBFIfNeeded(testFile.getName());
			readers.add(openReader(obfFile));
		}
	}

	private List<String> parsePhrases(JSONObject sourceJson) {
		JSONArray phrasesJson = sourceJson.optJSONArray("phrases");
		String singlePhrase = sourceJson.optString("phrase", null);
		List<String> phrases = new ArrayList<>();
		if (singlePhrase != null) {
			phrases.add(singlePhrase);
		}
		if (phrasesJson != null) {
			for (int i = 0; i < phrasesJson.length(); i++) {
				String phrase = phrasesJson.optString(i, null);
				if (!Algorithms.isEmpty(phrase)) {
					phrases.add(phrase);
				}
			}
		}
		return phrases;
	}

	private List<List<String>> parseExpectedResults(JSONObject sourceJson, int phrasesSize) {
		List<List<String>> results = new ArrayList<>();
		for (int i = 0; i < phrasesSize; i++) {
			results.add(new ArrayList<String>());
		}
		if (sourceJson.has("results")) {
			parseResults(sourceJson, "results", results);
		}
		if (TEST_EXTRA_RESULTS && sourceJson.has("extra-results")) {
			parseResults(sourceJson, "extra-results", results);
		}
		return results;
	}

	private File exportSourceJson(File originalObfFile, String generatedObfName) throws IOException {
		List<Amenity> amenities = getAmenities(originalObfFile.getAbsolutePath());
		List<City> cities = getCities(originalObfFile.getAbsolutePath());
        if (FILTER_DATA_JSON) {
            filterCities(cities);
        }
		List<RouteDataObject> routes = getRoutes(originalObfFile.getAbsolutePath());
		String jsonName = generatedObfName.endsWith(".obf")
				? generatedObfName.substring(0, generatedObfName.length() - ".obf".length()) + ".json"
				: generatedObfName + ".json";
		File jsonFile = new File(GEN_DIR, jsonName);
		createJsonFile(jsonFile, amenities, cities, routes);
		cacheGzipIfNeeded(jsonFile, new File(GEN_DIR, jsonName + ".gz"));
		return jsonFile;
	}

	@Test
	public void testSearch() throws IOException, JSONException, SQLException {
		String sourceJsonText = Algorithms.getFileAsString(testFile);
		Assert.assertNotNull(sourceJsonText);
        Assert.assertFalse(sourceJsonText.isEmpty());

		JSONObject sourceJson = new JSONObject(sourceJsonText);
//		boolean ignore = sourceJson.optBoolean("ignore");
//		if (RUN_IGNORED_TESTS) {
//			return;
//		}
        searchKeywords = getKeywords(sourceJson);
		JSONObject settingsJson = sourceJson.getJSONObject("settings");
		List<String> phrases = parsePhrases(sourceJson);
		boolean useData = settingsJson.optBoolean("useData", true);
		List<BinaryMapIndexReader> readers = new ArrayList<>();
		boolean prevDisplayDefaultPoiTypes = SearchCoreFactory.DISPLAY_DEFAULT_POI_TYPES;
		SearchTestEngine engine = null;
		try {
			if (useData) {
				loadReaders(sourceJson, readers);
				if (readers.isEmpty()) {
					throw new IllegalStateException("useData=true but no OBF indexes were loaded for " + testFile.getName());
				}
			}
		boolean disabled = settingsJson.optBoolean("disabled", false);
		if (disabled) {
			return;
		}
		List<List<String>> results = parseExpectedResults(sourceJson, phrases.size());

		Assert.assertEquals(phrases.size(), results.size());
		if (phrases.size() != results.size()) {
			return;
		}

		engine = createSearchEngine(settingsJson, readers);
		for (int k = 0; k < phrases.size(); k++) {
			String text = phrases.get(k);
			List<String> expectedResults = results.get(k);

			List<String> actualResults = engine.apply(text, expectedResults);
			for (int i = 0; i < expectedResults.size(); i++) {
				String expected = expectedResults.get(i);
				String actual = i >= actualResults.size() ? null : actualResults.get(i);
				int shift = TEST_NUMBER_MATCHED ? 4 : 0;
				if (expected.indexOf('[') != -1) {
					expected = expected.substring(0, expected.indexOf('[') + shift).trim();
				}
				if (actual != null && actual.indexOf('[') != -1) {
					actual = actual.substring(0, actual.indexOf('[') + shift).trim();
				}
				// String present = result.toString();
				expected = expected.replaceFirst("^@", "");
				String present = actual == null ? ("#MISSING " + (i + 1)) : actual;
				if (!Algorithms.stringsEqual(expected, present)) {
					System.out.printf("Phrase: %s%n", text);
					System.out.printf("Mismatch #%s for '%s' != '%s'. %n", i + 1, expected, present);
					System.out.println("CURRENT RESULTS: ");
					for (String r : actualResults) {
						System.out.printf("\t\t\"%s\",%n", r);
					}
					System.out.println("EXPECTED : ");
					for (String r : expectedResults) {
						System.out.printf("\t\t\"%s\",%n", r);
					}
				}
				Assert.assertEquals(expected, present);
			}
		}
		} finally {
			SearchCoreFactory.DISPLAY_DEFAULT_POI_TYPES = prevDisplayDefaultPoiTypes;
			if (engine != null) {
				engine.close();
			}
			for (BinaryMapIndexReader reader : readers) {
				reader.close();
			}
        }
	}

	private SearchTestEngine createSearchEngine(JSONObject settingsJson, List<BinaryMapIndexReader> readers) {
		return new SpatialTestSearchEngine(settingsJson, readers);
	}

	private static void deleteRecursively(File file) {
		if (file == null || !file.exists()) {
			return;
		}
		if (file.isDirectory()) {
			File[] files = file.listFiles();
			if (files != null) {
				for (File child : files) {
					deleteRecursively(child);
				}
			}
		}
		if (!file.delete()) {
			for (int i = 0; i < 5 && file.exists(); i++) {
				if (file.delete()) {
					return;
				}
			}
			if (file.exists()) {
				file.deleteOnExit();
			}
		}
	}

	private boolean isDataFileName(String fileName) {
		return fileName.endsWith(".obf") || fileName.endsWith(".obf.gz")
				|| fileName.endsWith(".json") || fileName.endsWith(".json.gz");
	}

	private static void deleteGeneratedFiles(File dir, String... extensions) {
		if (dir == null || !dir.isDirectory()) {
			return;
		}
		File[] files = dir.listFiles();
		if (files == null) {
			return;
		}
		for (File file : files) {
			if (file.isDirectory()) {
				deleteGeneratedFiles(file, extensions);
			} else if (hasAnyExtension(file, extensions)) {
				deleteRecursively(file);
			}
		}
	}

	private static boolean hasAnyExtension(File file, String... extensions) {
		for (String extension : extensions) {
			if (file.getName().endsWith(extension)) {
				return true;
			}
		}
		return false;
	}

	private List<Amenity> getAmenities(String obf) throws IOException {
		List<Amenity> results = new ArrayList<>();
		File file = new File(obf);
		try (RandomAccessFile r = new RandomAccessFile(file.getAbsolutePath(), "r")) {
			BinaryMapIndexReader index = new BinaryMapIndexReader(r, file);
			try {
				for (BinaryMapPoiReaderAdapter.PoiRegion poiIndex : index.getPoiIndexes()) {
					BinaryMapIndexReader.SearchRequest<Amenity> request = BinaryMapIndexReader.buildSearchPoiRequest(
							0, Integer.MAX_VALUE, 0, Integer.MAX_VALUE,
							-1, BinaryMapIndexReader.ACCEPT_ALL_POI_TYPE_FILTER, null);
					results.addAll(index.searchPoi(request, poiIndex));
				}
			} finally {
				index.close();
			}
		}
		results.sort(Comparator.comparing(a -> {
			String name = a == null ? null : a.getName("en");
			return name == null ? "" : name;
		}, String.CASE_INSENSITIVE_ORDER));
		return results;
	}

	private List<City> getCities(String obf) throws IOException {
		Map<String, City> mergedCities = new LinkedHashMap<>();
		File file = new File(obf);
		try (RandomAccessFile r = new RandomAccessFile(file.getAbsolutePath(), "r")) {
			BinaryMapIndexReader index = new BinaryMapIndexReader(r, file);
			try {
				for (BinaryMapAddressReaderAdapter.AddressRegion region : index.getAddressIndexes()) {
					for (BinaryMapAddressReaderAdapter.CityBlocks type : BinaryMapAddressReaderAdapter.CityBlocks.values()) {
						if (type == BinaryMapAddressReaderAdapter.CityBlocks.UNKNOWN_TYPE) {
							continue;
						}
						for (City city : index.getCities(null, type, region, null)) {
							index.preloadStreets(city, null, true, null);
							for (Street street : new ArrayList<>(city.getStreets())) {
								index.preloadBuildings(street, null, null);
							}
							mergeCity(mergedCities, city, "en");
						}
					}
				}
			} finally {
				index.close();
			}
		}
		List<City> results = new ArrayList<>(mergedCities.values());
		results.sort(Comparator.comparing(c -> {
			String name = c == null ? null : c.getName("en");
			return name == null ? "" : name;
		}, String.CASE_INSENSITIVE_ORDER));
		return results;
	}

	private List<RouteDataObject> getRoutes(String obf) throws IOException {
		List<RouteDataObject> results = new ArrayList<>();
		File file = new File(obf);
		try (RandomAccessFile r = new RandomAccessFile(file.getAbsolutePath(), "r")) {
			BinaryMapIndexReader index = new BinaryMapIndexReader(r, file);
			try {
				for (BinaryMapRouteReaderAdapter.RouteRegion region : index.getRoutingIndexes()) {
					BinaryMapIndexReader.SearchRequest<RouteDataObject> request = BinaryMapIndexReader.buildSearchRouteRequest(
							0, Integer.MAX_VALUE, 0, Integer.MAX_VALUE, null);
					List<BinaryMapRouteReaderAdapter.RouteSubregion> subregions =
							index.searchRouteIndexTree(request, region.getSubregions());
					index.loadRouteIndexData(subregions, new ResultMatcher<>() {
						@Override
						public boolean publish(RouteDataObject object) {
							results.add(object);
							return true;
						}

						@Override
						public boolean isCancelled() {
							return false;
						}
					});
				}
			} finally {
				index.close();
			}
		}
		results.sort(Comparator.comparingLong(rdo -> rdo == null ? Long.MAX_VALUE : rdo.id));
		return results;
	}

	private void createJsonFile(File sourceJsonFile, List<Amenity> amenities, List<City> cities, List<RouteDataObject> routes) throws IOException {
		File parent = sourceJsonFile.getParentFile();
		if (parent != null && !parent.isDirectory() && !parent.mkdirs()) {
			throw new IOException("Cannot create directory " + parent);
		}
		try (Writer writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(sourceJsonFile), StandardCharsets.UTF_8))) {
			writer.write("{\n");
			boolean hasPreviousSection = false;
			if (amenities != null && !amenities.isEmpty()) {
				hasPreviousSection = writeJsonSection(writer, "amenities", amenities, hasPreviousSection,
						amenity -> amenity == null ? null : amenity.toJSON());
			}
			if (cities != null && !cities.isEmpty()) {
				hasPreviousSection = writeJsonSection(writer, "cities", cities, hasPreviousSection,
						city -> city == null ? null : city.toJSON(true));
			}
			if (routes != null && !routes.isEmpty()) {
				long[] routeId = {1};
				writeJsonSection(writer, "routing", routes, hasPreviousSection,
						route -> route == null ? null : routeDataObjectToJson(route, routeId[0]++));
			}
			writer.write("\n}");
		}
	}

	private <T> boolean writeJsonSection(Writer writer, String name, List<T> values, boolean hasPreviousSection,
			JsonObjectWriter<T> objectWriter) throws IOException {
		boolean hasAnyObject = false;
		boolean firstObject = true;
		for (T value : values) {
			JSONObject object = objectWriter.toJson(value);
			if (object == null) {
				continue;
			}
			if (!hasAnyObject) {
				writer.write(hasPreviousSection ? ",\n" : "");
				writer.write("  \"");
				writer.write(name);
				writer.write("\": [");
				hasAnyObject = true;
			}
			if (!firstObject) {
				writer.write(",");
			}
			writer.write("\n    ");
			writer.write(object.toString());
			firstObject = false;
		}
		if (hasAnyObject) {
			writer.write("\n  ]");
			return true;
		}
		return hasPreviousSection;
	}

	private interface JsonObjectWriter<T> {
		JSONObject toJson(T value);
	}

	private void mergeCity(Map<String, City> mergedCities, City city, String lang) {
		if (city == null) {
			return;
		}
		String key = cityMergeKey(city, lang);
		City existing = mergedCities.get(key);
		if (existing == null) {
			mergedCities.put(key, city);
		} else {
			existing.mergeWith(city);
		}
	}

	private String cityMergeKey(City city, String lang) {
		Long id = city.getId();
		if (id != null) {
			return id.toString();
		}
		String name = city.getName(lang);
		LatLon location = city.getLocation();
		return city.getType() + "|" + (name == null ? "" : name.toLowerCase(Locale.ROOT)) + "|"
				+ (location == null ? "" : String.format(Locale.US, "%.6f,%.6f",
				location.getLatitude(), location.getLongitude()));
	}

	private JSONObject routeDataObjectToJson(RouteDataObject road, long routeId) {
		JSONObject routeJson = new JSONObject();
		routeJson.put("id", routeId);
		routeJson.put("pointsX", toJsonArray(road.pointsX));
		routeJson.put("pointsY", toJsonArray(road.pointsY));
		JSONArray types = new JSONArray();
		if (road.types != null) {
			for (int type : road.types) {
				BinaryMapRouteReaderAdapter.RouteTypeRule rule = road.region.quickGetEncodingRule(type);
				if (rule != null) {
					JSONObject typeJson = new JSONObject();
					typeJson.put("tag", rule.getTag());
					typeJson.put("value", rule.getValue());
					types.put(typeJson);
				}
			}
		}
		routeJson.put("types", types);
		JSONArray names = new JSONArray();
		if (road.nameIds != null && road.names != null) {
			for (int nameId : road.nameIds) {
				BinaryMapRouteReaderAdapter.RouteTypeRule rule = road.region.quickGetEncodingRule(nameId);
				if (rule == null) {
					continue;
				}
				JSONObject nameJson = new JSONObject();
				nameJson.put("tag", rule.getTag());
				String nameValue = road.names.get(nameId);
				nameJson.put("value", nameValue == null ? "" : nameValue);
				names.put(nameJson);
			}
		}
		routeJson.put("names", names);
		return routeJson;
	}

	private JSONArray toJsonArray(int[] values) {
		JSONArray arr = new JSONArray();
		if (values == null) {
			return arr;
		}
		for (int value : values) {
			arr.put(value);
		}
		return arr;
	}

	private void parseResults(JSONObject sourceJson, String tag, List<List<String>> results) {
		if (results.isEmpty()) {
			return;
		}
		List<String> result = results.get(0);
		JSONArray resultsArr = sourceJson.getJSONArray(tag);
		boolean hasInnerArray = resultsArr.length() > 0 && resultsArr.optJSONArray(0) != null;
		for (int i = 0; i < resultsArr.length(); i++) {
			if (hasInnerArray) {
				JSONArray innerArray = resultsArr.optJSONArray(i);
				if (innerArray != null && results.size() > i) {
					result = results.get(i);
					for (int k = 0; k < innerArray.length(); k++) {
						result.add(innerArray.getString(k));
					}
				}
			} else {
				result.add(resultsArr.getString(i));
			}
		}
	}

	static class TestSearchTranslator implements MapPoiTypes.PoiTranslator {

		private final Map<String, String> enPhrases;
		private final Map<String, String> phrases;
		public TestSearchTranslator(Map<String, String> phrases, Map<String, String> enPhrases) {
			this.phrases = phrases;
			this.enPhrases = enPhrases;
		}

		@Override
		public String getTranslation(AbstractPoiType type) {
			AbstractPoiType baseLangType = type.getBaseLangType();
			if (baseLangType != null) {
				return getTranslation(baseLangType) + " (" + type.getLang().toLowerCase() + ")";
			}
			return getTranslation(type.getIconKeyName());
		}

		@Override
		public String getTranslation(String keyName) {
			String val = phrases.get("poi_" + keyName);
			if (val != null) {
				int ind = val.indexOf(';');
				if (ind > 0) {
					return val.substring(0, ind);
				}
			}
			return val;
		}

		@Override
		public String getSynonyms(AbstractPoiType type) {
			AbstractPoiType baseLangType = type.getBaseLangType();
			if (baseLangType != null) {
				return getSynonyms(baseLangType);
			}
			return getSynonyms(type.getIconKeyName());
		}


		@Override
		public String getSynonyms(String keyName) {
			String val = phrases.get("poi_" + keyName);
			if (val != null) {
				int ind = val.indexOf(';');
				if (ind > 0) {
					return val.substring(ind + 1);
				}
				return "";
			}
			return null;
		}

		@Override
		public String getAllLanguagesTranslationSuffix() {
			return "all languages";
		}

		@Override
		public String getEnTranslation(AbstractPoiType type) {
			AbstractPoiType baseLangType = type.getBaseLangType();
			if (baseLangType != null) {
				return getEnTranslation(baseLangType) + " (" + type.getLang().toLowerCase() + ")";
			}
			return getEnTranslation(type.getIconKeyName());
		}

		@Override
		public String getEnTranslation(String keyName) {
			if (enPhrases.isEmpty()) {
				return Algorithms.capitalizeFirstLetter(keyName.replace('_', ' '));
			}
			String val = enPhrases.get("poi_" + keyName);
			if (val != null) {
				int ind = val.indexOf(';');
				if (ind > 0) {
					return val.substring(0, ind);
				}
			}
			return val;
		}
	}

	private static String getObfGenerateHash() {
		List<String> individualHashes = new ArrayList<>();

		for (Class<?> clazz : OBF_GENERATE_CLASSES) {
			String hash = getClassHash(clazz);
			if (!hash.startsWith("Error")) {
				individualHashes.add(hash);
			}
		}

		String allHashesCombined = String.join("\n", individualHashes);
		return DigestUtils.sha256Hex(allHashesCombined);
	}

	private static String getClassHash(Class<?> clazz) {
		String classResourcePath = "/" + clazz.getName().replace('.', '/') + ".class";
		try (InputStream is = clazz.getResourceAsStream(classResourcePath)) {
			if (is == null) {
				return "Error. Class not found";
			}
			return DigestUtils.sha256Hex(is);
		} catch (IOException e) {
			e.printStackTrace();
			return "Error: " + e.getMessage();
		}
	}

    public Set<String> getKeywords(JSONObject sourceJson) {
        Set<String> keywords = new HashSet<>();
        List<String> phrases = parsePhrases(sourceJson);
        for (String phrase : phrases) {
            extractAndAddWords(phrase, keywords);
        }

        List<List<String>> parsedResults = new ArrayList<>();
        for (int i = 0; i < phrases.size(); i++) {
            parsedResults.add(new ArrayList<String>());
        }
        String tag = sourceJson.has("results") ? "results" : "result";
        parseResults(sourceJson, tag, parsedResults);

        for (List<String> group : parsedResults) {
            if (group != null) {
                for (String resultStr : group) {
                    if (resultStr != null) {
                        int bracketIndex = resultStr.indexOf("[[");
                        if (bracketIndex != -1) {
                            resultStr = resultStr.substring(0, bracketIndex);
                        }
                        extractAndAddWords(resultStr, keywords);
                    }
                }
            }
        }

        return keywords;
    }

    private void extractAndAddWords(String text, Set<String> keywords) {
        if (text == null || text.trim().isEmpty()) {
            return;
        }
        String[] words = text.split("[\\s\\-,'.<>_\\(\\)\\[\\]]+");

        for (String word : words) {
            if (!word.isEmpty()) {
                keywords.add(word.toLowerCase());
            }
        }
    }

    private void filterCities(List<City> cities) {
        if (Algorithms.isEmpty(searchKeywords)) {
            return;
        }

        Iterator<City> cityIterator = cities.iterator();
        while (cityIterator.hasNext()) {
            City c = cityIterator.next();
            boolean match = false;
            if (match(c.getName()) || match(c.getNamesMap(true).values())) {
                match = true;
            }
            Iterator<Street> streetIterator = c.getStreets().iterator();
            while (streetIterator.hasNext()) {
                Street s = streetIterator.next();
                if (match(s.getName()) || match(s.getNamesMap(true).values())) {
                    match = true;
                } else {
                    for (Building b : s.getBuildings()) {
                        if (match(b.getName()) || match(b.getNamesMap(true).values())) {
                            match = true;
                            break;
                        }
                    }
                }
            }
            if (!match) {
                if (ThreadLocalRandom.current().nextDouble() < FILTER_REMOVE_PROBABILITY) {
                    cityIterator.remove();
                }
            }
        }
    }

    private boolean match(String name) {
        return name != null && searchKeywords.contains(name);
    }

    private boolean match(Collection<String> names) {
        for (String name : names) {
            if (match(name))
                return true;
        }
        return false;
    }
}
