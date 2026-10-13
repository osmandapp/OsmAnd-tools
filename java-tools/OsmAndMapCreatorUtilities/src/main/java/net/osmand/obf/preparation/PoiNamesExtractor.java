package net.osmand.obf.preparation;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.gson.Gson;

import gnu.trove.map.hash.TLongLongHashMap;
import net.osmand.IndexConstants;
import net.osmand.binary.ObfConstants;
import net.osmand.data.Amenity;
import net.osmand.obf.preparation.IndexPoiCreator.PoiAdditionalType;
import net.osmand.osm.MapPoiTypes;
import net.osmand.osm.edit.Entity;
import net.osmand.osm.edit.Entity.EntityType;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;
import net.sf.junidecode.Junidecode;

/**
 * Extraction of the named POI of a map as the OBF generator accepts them: the input of the top abbreviations analyzer
 * (top-org-abbrs-spec.md 5.3). Two gzipped TSV files next to each other:
 * <ul>
 * <li>{@code <map>.poi-entities.tsv.gz}: {@code osm_type, osm_id, tags} - the raw name, operator, network, brand and
 * wikidata tags of an OSM entity (JSON object), once per entity;</li>
 * <li>{@code <map>.poi-records.tsv.gz}: {@code osm_type, osm_id, center, obf_id, type, subtype, additional, lat, lon,
 * names} - every POI of these entities written to the OBF with the names the generator put into its name index.</li>
 * </ul>
 * The first line of both files is {@link Meta}: the date of the OSM data and of the generation; the reader takes the
 * files as one extraction only if both lines are equal. An entity is written with its first POI that passed the map
 * boundary filter, a POI - when it is written to the OBF (the table {@code poi} drops rows on a duplicate key and later
 * in {@code processPOIIntoTree}). Only real OSM entities with a name, an operator or a network are extracted. The files
 * are published by {@link #publish()} after the whole OBF is written.
 */
public class PoiNamesExtractor {

	private static final Log log = LogFactory.getLog(PoiNamesExtractor.class);

	public static final String ENV_POI_EXTRACTION_DIR = "POI_EXTRACTION_DIR";
	public static final String ENTITIES_EXT = ".poi-entities.tsv.gz";
	public static final String RECORDS_EXT = ".poi-records.tsv.gz";
	public static final String ENTITIES_HEADER = "osm_type\tosm_id\ttags";
	public static final String RECORDS_HEADER = "osm_type\tosm_id\tcenter\tobf_id\ttype\tsubtype\tadditional\tlat\tlon"
			+ "\tnames";
	private static final String TMP_EXT = ".tmp";
	private static final String META_PREFIX = "# poi-extraction ";
	private static final String NAME_EN = "name:en";

	// tags of an entity: exact key or the key followed by ':' (name:de, operator:short, brand:wikidata)
	private static final String[] NAME_TAGS = { "name", "short_name", "alt_name", "official_name" };
	private static final String[] BIND_TAGS = { "operator", "network" };
	private static final String[] OTHER_TAGS = { "brand", "wikidata" };

	// obf id -> (osm id, center, osm type) of the extracted entity
	private static final int TYPE_BITS = 2;
	private static final int CENTER_BITS = 10;
	private static final int MAX_CENTER = (1 << CENTER_BITS) - 1;

	private static final Gson GSON = new Gson();

	/** the first line of both files: one extraction of a map */
	public static class Meta {
		/** date of the OSM data of the map (its source file): one for the maps of one data snapshot */
		public final long dataDate;
		public final long generated;

		public Meta(long dataDate, long generated) {
			this.dataDate = dataDate;
			this.generated = generated;
		}

		public String line() {
			return META_PREFIX + "data_date=" + dataDate + " generated=" + generated;
		}

		/** null if the line is not a meta line */
		public static Meta parse(String line) {
			if (line == null || !line.startsWith(META_PREFIX)) {
				return null;
			}
			long data = -1, generated = -1;
			for (String kv : line.substring(META_PREFIX.length()).trim().split(" ")) {
				try {
					if (kv.startsWith("data_date=")) {
						data = Long.parseLong(kv.substring("data_date=".length()));
					} else if (kv.startsWith("generated=")) {
						generated = Long.parseLong(kv.substring("generated=".length()));
					}
				} catch (NumberFormatException e) {
					return null;
				}
			}
			return data < 0 || generated < 0 ? null : new Meta(data, generated);
		}

		@Override
		public boolean equals(Object o) {
			return o instanceof Meta && ((Meta) o).dataDate == dataDate && ((Meta) o).generated == generated;
		}

		@Override
		public int hashCode() {
			return Long.hashCode(dataDate) * 31 + Long.hashCode(generated);
		}

		@Override
		public String toString() {
			return line();
		}
	}

	private final Meta meta;
	private final File entitiesFile;
	private final File recordsFile;
	private final TLongLongHashMap entityByObfId = new TLongLongHashMap();
	private Writer entities;
	private Writer records;
	private long lastEntityKey = -1;
	private boolean lastEntityExtracted;
	private int entitiesCount;
	private int recordsCount;
	private int idConflicts;

	private PoiNamesExtractor(File dir, String mapName, long dataDate) throws IOException {
		if (!dir.isDirectory() && !dir.mkdirs()) {
			throw new IOException("Cannot create POI extraction directory " + dir);
		}
		meta = new Meta(dataDate, System.currentTimeMillis());
		entitiesFile = new File(dir, mapName + ENTITIES_EXT);
		recordsFile = new File(dir, mapName + RECORDS_EXT);
		entities = open(tmp(entitiesFile));
		entities.write(meta.line() + "\n" + ENTITIES_HEADER + "\n");
	}

	/**
	 * @param dataDate date of the OSM data of the map ({@link IndexCreator#setLastModifiedDate}), null - now
	 * @return the extraction of the map (file {@code mapFile}) when {@link IndexCreatorSettings#poiExtractionDir} or the
	 *         environment variable {@value #ENV_POI_EXTRACTION_DIR} is set, otherwise null
	 */
	public static PoiNamesExtractor create(IndexCreatorSettings settings, File mapFile, Long dataDate)
			throws IOException {
		String dir = settings.poiExtractionDir != null ? settings.poiExtractionDir : System.getenv(ENV_POI_EXTRACTION_DIR);
		if (Algorithms.isEmpty(dir)) {
			return null;
		}
		String mapName = mapFile.getName();
		if (mapName.endsWith(IndexConstants.BINARY_MAP_INDEX_EXT)) {
			mapName = mapName.substring(0, mapName.length() - IndexConstants.BINARY_MAP_INDEX_EXT.length());
		}
		log.info("POI extraction of " + mapName + " to " + dir);
		return new PoiNamesExtractor(new File(dir), mapName,
				dataDate == null ? System.currentTimeMillis() : dataDate.longValue());
	}

	public static boolean isExtractedTag(String tag) {
		return matches(tag, NAME_TAGS) || matches(tag, BIND_TAGS) || matches(tag, OTHER_TAGS);
	}

	private static boolean isNameOrBindTag(String tag) {
		return matches(tag, NAME_TAGS) || matches(tag, BIND_TAGS);
	}

	private static boolean matches(String tag, String[] keys) {
		for (String k : keys) {
			if (tag.startsWith(k) && (tag.length() == k.length() || tag.charAt(k.length()) == ':')) {
				return true;
			}
		}
		return false;
	}

	/**
	 * A POI of the entity {@code e} that the generator inserts into the table {@code poi} with the id {@code obfId}:
	 * writes the entity once and remembers the id to write the POI when it reaches the OBF.
	 */
	public void addPoi(Entity e, long obfId, int center) throws IOException {
		EntityType type = EntityType.valueOf(e);
		// generated and propagated objects are not OSM entities
		if (e.getId() <= 0 || e.getId() > ObfConstants.PROPAGATE_NODE_BIT || type == null || center > MAX_CENTER) {
			return;
		}
		// ids of nodes and ways are shifted (ObfConstants.createMapObjectIdFromOsmAndEntity)
		long osmId = type == EntityType.RELATION ? e.getId() : e.getId() >> ObfConstants.SHIFT_ID;
		// the POI of an entity come one after another
		long entityKey = (osmId << TYPE_BITS) | type.ordinal();
		if (entityKey != lastEntityKey) {
			lastEntityKey = entityKey;
			lastEntityExtracted = writeEntity(e, type, osmId);
		}
		if (!lastEntityExtracted) {
			return;
		}
		long value = (osmId << (TYPE_BITS + CENTER_BITS)) | ((long) center << TYPE_BITS) | type.ordinal();
		long prev = entityByObfId.putIfAbsent(obfId, value);
		if (prev != entityByObfId.getNoEntryValue() && prev != value) {
			idConflicts++;
		}
	}

	private boolean writeEntity(Entity e, EntityType type, long osmId) throws IOException {
		Map<String, String> tags = new TreeMap<>();
		boolean named = false;
		for (Map.Entry<String, String> t : e.getTags().entrySet()) {
			if (isExtractedTag(t.getKey()) && !Algorithms.isEmpty(t.getValue())) {
				tags.put(t.getKey(), t.getValue());
				named |= isNameOrBindTag(t.getKey());
			}
		}
		if (named) {
			entities.write(osmType(type) + "\t" + osmId + "\t" + GSON.toJson(tags) + "\n");
			entitiesCount++;
		}
		return named;
	}

	/**
	 * The row of the table {@code poi} written to the OBF. Its {@code names} are the names of the name index as
	 * {@code IndexPoiCreator.putPoiObjectPrefix} gets them: the text tags of
	 * {@link ObfConstants#isTagIndexedForSearchAsName} ({@code name}, {@code name:*}, {@code brand}...) and
	 * {@code name:en} - the transliteration of {@code name} when the POI has no {@code name:en}.
	 */
	public void writeRecord(long obfId, int x31, int y31, String type, String subtype,
			Map<PoiAdditionalType, String> additionalTags) throws IOException {
		if (!entityByObfId.containsKey(obfId)) {
			return;
		}
		openRecords();
		long value = entityByObfId.get(obfId);
		long osmId = value >>> (TYPE_BITS + CENTER_BITS);
		int center = (int) ((value >>> TYPE_BITS) & MAX_CENTER);
		EntityType osmType = EntityType.values()[(int) (value & ((1 << TYPE_BITS) - 1))];
		Set<String> additional = new TreeSet<>();
		Map<String, String> names = new TreeMap<>();
		for (Map.Entry<PoiAdditionalType, String> a : additionalTags.entrySet()) {
			String tag = a.getKey().getTag();
			if (!a.getKey().isText()) {
				if (!tag.startsWith(MapPoiTypes.TOP_INDEX_ADDITIONAL_PREFIX)) {
					additional.add(tag);
				}
			} else if (ObfConstants.isTagIndexedForSearchAsName(tag) && !Algorithms.isEmpty(a.getValue())) {
				names.put(tag, a.getValue());
			}
		}
		String name = names.get(Amenity.NAME);
		if (name != null && !names.containsKey(NAME_EN)) {
			names.put(NAME_EN, Junidecode.unidecode(name));
		}
		records.write(osmType(osmType) + "\t" + osmId + "\t" + center + "\t" + obfId + "\t" + clean(type) + "\t"
				+ clean(subtype) + "\t" + clean(String.join(";", additional)) + "\t"
				+ String.format(Locale.US, "%.6f\t%.6f", MapUtils.get31LatitudeY(y31), MapUtils.get31LongitudeX(x31))
				+ "\t" + GSON.toJson(names) + "\n");
		recordsCount++;
	}

	private void openRecords() throws IOException {
		if (records == null) {
			records = open(tmp(recordsFile));
			records.write(meta.line() + "\n" + RECORDS_HEADER + "\n");
		}
	}

	/** every POI is written to the OBF section: closes the files, they are published after the whole OBF */
	public void close() throws IOException {
		if (entities != null) {
			openRecords();
		}
		closeWriters();
	}

	/**
	 * The whole OBF is written: publishes the files. The reader lists maps by the records file, so it is removed first
	 * and renamed last; a pair of files of different generations is rejected by its {@link Meta}.
	 */
	public void publish() throws IOException {
		close();
		delete(recordsFile);
		delete(entitiesFile);
		rename(tmp(entitiesFile), entitiesFile);
		rename(tmp(recordsFile), recordsFile);
		log.info(String.format("POI extraction: %d entities, %d POI (%d id conflicts) - %s, %s", entitiesCount,
				recordsCount, idConflicts, entitiesFile.getName(), recordsFile.getName()));
	}

	/** generation failed: removes the unpublished files */
	public void abort() {
		try {
			closeWriters();
		} catch (IOException e) {
			log.warn("Cannot close POI extraction", e);
		}
		tmp(entitiesFile).delete();
		tmp(recordsFile).delete();
	}

	private void closeWriters() throws IOException {
		try {
			if (entities != null) {
				entities.close();
			}
		} finally {
			entities = null;
			if (records != null) {
				Writer r = records;
				records = null;
				r.close();
			}
		}
	}

	/**
	 * Reads a file of the extraction: the meta line, the header {@code header} and the rows.
	 *
	 * @return the meta line of the file, null for a file without it (its rows are not read)
	 */
	public static Meta readTsv(File f, String header, Consumer<String[]> row) throws IOException {
		int columns = header.split("\t").length;
		try (BufferedReader r = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(f)),
				StandardCharsets.UTF_8), 1 << 16)) {
			Meta meta = Meta.parse(r.readLine());
			if (meta == null) {
				return null;
			}
			String l = r.readLine();
			if (!header.equals(l)) {
				throw new IOException("Unexpected header of " + f + ": " + l);
			}
			while ((l = r.readLine()) != null) {
				String[] c = l.split("\t", -1);
				if (c.length != columns) {
					throw new IOException("Unexpected row of " + f + ": " + l);
				}
				row.accept(c);
			}
			return meta;
		}
	}

	/** the meta line of a file of the extraction, null if the file has none */
	public static Meta readMeta(File f) throws IOException {
		try (BufferedReader r = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(f)),
				StandardCharsets.UTF_8))) {
			return Meta.parse(r.readLine());
		}
	}

	private static void delete(File f) throws IOException {
		if (f.exists() && !f.delete()) {
			throw new IOException("Cannot replace " + f);
		}
	}

	private static void rename(File from, File to) throws IOException {
		delete(to);
		if (!from.renameTo(to)) {
			throw new IOException("Cannot rename " + from + " to " + to);
		}
	}

	private static File tmp(File f) {
		return new File(f.getParentFile(), f.getName() + TMP_EXT);
	}

	private static Writer open(File f) throws IOException {
		return new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(f), 1 << 16),
				StandardCharsets.UTF_8), 1 << 16);
	}

	private static String osmType(EntityType type) {
		return type.name().toLowerCase(Locale.ROOT);
	}

	private static String clean(String s) {
		return s == null ? "" : s.replace('\t', ' ').replace('\n', ' ').replace('\r', ' ');
	}
}
