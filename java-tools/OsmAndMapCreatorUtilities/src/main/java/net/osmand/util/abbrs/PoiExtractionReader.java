package net.osmand.util.abbrs;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;
import java.time.Instant;
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import net.osmand.obf.preparation.PoiNamesExtractor;
import net.osmand.obf.preparation.PoiNamesExtractor.Meta;
import net.osmand.util.abbrs.PoiExtraction.ExtractedEntity;
import net.osmand.util.abbrs.PoiExtraction.ExtractedMap;
import net.osmand.util.abbrs.PoiExtraction.ExtractedPoi;

/**
 * Reads the extractions of many maps into one {@link PoiExtraction} (top-org-abbrs-spec.md 5.3).
 * <p>
 * The analyzed period is a month: an extraction whose OSM data is older than {@code maxAgeMonths} months before it,
 * or newer than it, is skipped and listed, as is a map out of the region tree or a pair of files of different
 * extractions.
 * <p>
 * Maps overlap: a country map and the maps of its parts hold the same objects, maybe of different OSM data. An entity
 * is taken in one state: of the map with the newest OSM data that has it; its POI of maps with older data are stale
 * and dropped, different POI of maps with the same data stay. A copy of a POI identity
 * {@code (osm_type, osm_id, center, type, subtype)} is taken from the most recent generation, then from the deepest
 * map, then from the first map by name. The tags of an entity come from the map of its first kept POI.
 */
public class PoiExtractionReader {

	private static final Type MAP_TYPE = new TypeToken<TreeMap<String, String>>() {}.getType();
	private static final Gson GSON = new Gson();

	private final RegionTree regions;
	private final int maxAgeMonths;
	private final YearMonth period;

	/** @param period the analyzed month: the month of the OSM data of the last generation */
	public PoiExtractionReader(RegionTree regions, int maxAgeMonths, YearMonth period) {
		this.regions = regions;
		this.maxAgeMonths = maxAgeMonths;
		this.period = period;
	}

	public static YearMonth month(long time) {
		return YearMonth.from(Instant.ofEpochMilli(time).atZone(ZoneOffset.UTC));
	}

	/** @param inputs folders with extractions or extraction files of maps */
	public PoiExtraction read(List<File> inputs) throws IOException {
		Map<String, File> mapFiles = mapFiles(inputs);
		PoiExtraction extraction = new PoiExtraction();
		YearMonth oldest = period.minusMonths(maxAgeMonths);
		for (Map.Entry<String, File> e : mapFiles.entrySet()) {
			ExtractedMap m = new ExtractedMap(e.getKey(), regions == null ? 0 : regions.depth(e.getKey()));
			extraction.maps.add(m);
			File entities = new File(e.getValue().getPath() + PoiNamesExtractor.ENTITIES_EXT);
			Meta meta = PoiNamesExtractor.readMeta(new File(e.getValue().getPath() + PoiNamesExtractor.RECORDS_EXT));
			if (meta != null) {
				m.dataDate = meta.dataDate;
				m.generated = meta.generated;
			}
			if (meta == null) {
				skip(m, "no meta line");
			} else if (!entities.exists() || !meta.equals(PoiNamesExtractor.readMeta(entities))) {
				skip(m, "entities of another extraction");
			} else if (regions != null && !regions.contains(m.name)) {
				skip(m, "not in the region tree");
			} else if (month(m.dataDate).isBefore(oldest)) {
				skip(m, "data of " + month(m.dataDate) + " older than " + maxAgeMonths + " months before " + period);
			} else if (month(m.dataDate).isAfter(period)) {
				skip(m, "data of " + month(m.dataDate) + " after " + period);
			}
		}
		// priority: newest data, newest generation, deepest, by name
		List<ExtractedMap> order = new ArrayList<>(extraction.maps);
		order.sort(Comparator.comparingLong((ExtractedMap m) -> -m.dataDate).thenComparingLong(m -> -m.generated)
				.thenComparingInt(m -> -m.depth).thenComparing(m -> m.name));
		Set<String> identities = new HashSet<>();
		for (ExtractedMap m : order) {
			if (m.used) {
				readMap(extraction, m, mapFiles.get(m.name), identities);
			}
		}
		for (ExtractedEntity e : extraction.entities.values()) {
			e.pois.sort(Comparator.comparingInt((ExtractedPoi p) -> p.center).thenComparing(p -> p.type)
					.thenComparing(p -> p.subtype));
			e.map = e.mainPoi().map;
		}
		return extraction;
	}

	static Map<String, File> mapFiles(List<File> inputs) throws IOException {
		Map<String, File> mapFiles = new TreeMap<>();
		for (File in : inputs) {
			File[] files = in.isDirectory() ? in.listFiles((d, n) -> n.endsWith(PoiNamesExtractor.RECORDS_EXT))
					: new File[] { in };
			if (files == null) {
				throw new IOException("Cannot list " + in);
			}
			for (File f : files) {
				String n = f.getName();
				int i = n.indexOf(".poi-");
				if (i > 0) {
					mapFiles.put(n.substring(0, i), new File(f.getParentFile(), n.substring(0, i)));
				}
			}
		}
		return mapFiles;
	}

	private static void skip(ExtractedMap m, String reason) {
		m.used = false;
		m.skipReason = reason;
	}

	private void readMap(PoiExtraction extraction, ExtractedMap m, File base, Set<String> identities)
			throws IOException {
		Map<String, Map<String, String>> tags = new HashMap<>();
		PoiNamesExtractor.readTsv(new File(base.getPath() + PoiNamesExtractor.ENTITIES_EXT),
				PoiNamesExtractor.ENTITIES_HEADER, c -> {
					tags.put(c[0] + "/" + c[1], Collections.unmodifiableMap(GSON.fromJson(c[2], MAP_TYPE)));
				});
		PoiNamesExtractor.readTsv(new File(base.getPath() + PoiNamesExtractor.RECORDS_EXT),
				PoiNamesExtractor.RECORDS_HEADER, c -> {
					String key = c[0] + "/" + c[1];
					Map<String, String> t = tags.get(key);
					if (t == null) {
						return;
					}
					m.pois++;
					ExtractedEntity e = extraction.entities.get(key);
					if (e != null && e.dataDate != m.dataDate) {
						// a map with newer data has the entity
						m.stalePois++;
						extraction.stalePois++;
						return;
					}
					if (!identities.add(key + "/" + c[2] + "/" + c[4] + "/" + c[5])) {
						extraction.duplicatePois++;
						return;
					}
					if (e == null) {
						e = new ExtractedEntity(c[0], Long.parseLong(c[1]), t);
						e.dataDate = m.dataDate;
						extraction.entities.put(key, e);
						m.entities++;
					}
					Set<String> additional = c[6].isEmpty() ? Collections.emptySet()
							: Collections.unmodifiableSet(new LinkedHashSet<>(Arrays.asList(c[6].split(";"))));
					e.pois.add(new ExtractedPoi(e, Integer.parseInt(c[2]), Long.parseLong(c[3]), c[4].intern(),
							c[5].intern(), additional, Double.parseDouble(c[7]), Double.parseDouble(c[8]), m.name,
							names(e, c[9])));
				});
	}

	/** the names of a POI, shared with another POI of the entity with the same names */
	private static Map<String, String> names(ExtractedEntity e, String json) {
		Map<String, String> names = GSON.fromJson(json, MAP_TYPE);
		for (ExtractedPoi p : e.pois) {
			if (p.names.equals(names)) {
				return p.names;
			}
		}
		return Collections.unmodifiableMap(names);
	}
}
