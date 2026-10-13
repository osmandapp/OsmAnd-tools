package net.osmand.util.abbrs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
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
import java.util.zip.GZIPInputStream;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import net.osmand.obf.preparation.PoiNamesExtractor;
import net.osmand.util.abbrs.PoiExtraction.ExtractedEntity;
import net.osmand.util.abbrs.PoiExtraction.ExtractedMap;
import net.osmand.util.abbrs.PoiExtraction.ExtractedPoi;

/**
 * Reads the extractions of many maps into one {@link PoiExtraction} (top-org-abbrs-spec.md 5.3).
 * <p>
 * Maps overlap: a country map and the maps of its parts hold the same objects. A copy of a POI identity
 * {@code (osm_type, osm_id, center, type, subtype)} is taken from the most recent extraction, then from the deepest map,
 * then from the first map by name; different POI of one entity stay. The tags of an entity come from the map of its
 * first kept POI. An extraction older than {@code maxAgeMonths} months before the newest one, or of a map out of the
 * region tree, is skipped and listed.
 */
public class PoiExtractionReader {

	private static final long MONTH_MS = 31L * 24 * 3600 * 1000;
	private static final Type TAGS_TYPE = new TypeToken<TreeMap<String, String>>() {}.getType();
	private static final Gson GSON = new Gson();

	private final RegionTree regions;
	private final int maxAgeMonths;

	public PoiExtractionReader(RegionTree regions, int maxAgeMonths) {
		this.regions = regions;
		this.maxAgeMonths = maxAgeMonths;
	}

	/** @param inputs folders with extractions or extraction files of maps */
	public PoiExtraction read(List<File> inputs) throws IOException {
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
		PoiExtraction extraction = new PoiExtraction();
		long newest = 0;
		for (Map.Entry<String, File> e : mapFiles.entrySet()) {
			File records = new File(e.getValue().getPath() + PoiNamesExtractor.RECORDS_EXT);
			ExtractedMap m = new ExtractedMap(e.getKey(), records.lastModified(),
					regions == null ? 0 : regions.depth(e.getKey()));
			extraction.maps.add(m);
			newest = Math.max(newest, m.generated);
		}
		for (ExtractedMap m : extraction.maps) {
			if (regions != null && !regions.contains(m.name)) {
				m.used = false;
				m.skipReason = "not in the region tree";
			} else if (newest - m.generated > maxAgeMonths * MONTH_MS) {
				m.used = false;
				m.skipReason = "older than " + maxAgeMonths + " months";
			}
		}
		// priority of copies: newest, deepest, by name
		List<ExtractedMap> order = new ArrayList<>(extraction.maps);
		order.sort(Comparator.comparingLong((ExtractedMap m) -> -m.generated).thenComparingInt(m -> -m.depth)
				.thenComparing(m -> m.name));
		Set<String> identities = new HashSet<>();
		for (ExtractedMap m : order) {
			if (m.used) {
				readMap(extraction, m, mapFiles.get(m.name), identities);
			}
		}
		// entities without kept POI: every copy of their POI came from other maps
		extraction.entities.values().removeIf(e -> e.pois.isEmpty());
		for (ExtractedEntity e : extraction.entities.values()) {
			e.pois.sort(Comparator.comparingInt((ExtractedPoi p) -> p.center).thenComparing(p -> p.type)
					.thenComparing(p -> p.subtype));
			e.map = e.mainPoi().map;
		}
		return extraction;
	}

	private void readMap(PoiExtraction extraction, ExtractedMap m, File base, Set<String> identities)
			throws IOException {
		Map<String, Map<String, String>> tags = new HashMap<>();
		readTsv(new File(base.getPath() + PoiNamesExtractor.ENTITIES_EXT), PoiNamesExtractor.ENTITIES_HEADER, c -> {
			tags.put(c[0] + "/" + c[1], Collections.unmodifiableMap(GSON.fromJson(c[2], TAGS_TYPE)));
		});
		readTsv(new File(base.getPath() + PoiNamesExtractor.RECORDS_EXT), PoiNamesExtractor.RECORDS_HEADER, c -> {
			String key = c[0] + "/" + c[1];
			Map<String, String> t = tags.get(key);
			if (t == null) {
				return;
			}
			m.pois++;
			String identity = key + "/" + c[2] + "/" + c[4] + "/" + c[5];
			if (!identities.add(identity)) {
				extraction.duplicatePois++;
				return;
			}
			ExtractedEntity e = extraction.entities.get(key);
			if (e == null) {
				e = new ExtractedEntity(c[0], Long.parseLong(c[1]), t);
				extraction.entities.put(key, e);
				m.entities++;
			}
			Set<String> additional = c[6].isEmpty() ? Collections.emptySet()
					: Collections.unmodifiableSet(new LinkedHashSet<>(Arrays.asList(c[6].split(";"))));
			e.pois.add(new ExtractedPoi(e, Integer.parseInt(c[2]), Long.parseLong(c[3]), c[4].intern(),
					c[5].intern(), additional, Double.parseDouble(c[7]), Double.parseDouble(c[8]), m.name));
		});
	}

	private interface Row {
		void accept(String[] columns);
	}

	private static void readTsv(File f, String header, Row row) throws IOException {
		int columns = header.split("\t").length;
		try (BufferedReader r = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(f)),
				StandardCharsets.UTF_8), 1 << 16)) {
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
		}
	}
}
