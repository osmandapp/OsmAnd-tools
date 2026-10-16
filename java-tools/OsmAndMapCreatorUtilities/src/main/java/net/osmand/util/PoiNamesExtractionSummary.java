package net.osmand.util;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import net.osmand.obf.preparation.PoiNamesExtractor;

/**
 * Summary of the POI extractions written by the generation ({@link PoiNamesExtractor}): entities, POI, file sizes, the share of
 * entities with operator / network and the most frequent POI types.
 * <p>Usage: {@code poi-extraction-summary <dir or extraction file>... [--top=20]}
 */
public class PoiNamesExtractionSummary {

	private static final String OPERATOR = "operator";
	private static final String NETWORK = "network";
	private static final Gson GSON = new Gson();

	public static void main(String[] args) throws IOException {
		int top = 20;
		List<File> maps = new ArrayList<>();
		for (String a : args) {
			if (a.startsWith("--top=")) {
				top = Integer.parseInt(a.substring("--top=".length()));
				continue;
			}
			File f = new File(a);
			File[] files = f.isDirectory() ? f.listFiles((d, n) -> n.endsWith(PoiNamesExtractor.RECORDS_EXT))
					: new File[] { f };
			if (files != null) {
				for (File r : files) {
					String name = r.getName();
					if (name.endsWith(PoiNamesExtractor.RECORDS_EXT) || name.endsWith(PoiNamesExtractor.ENTITIES_EXT)) {
						String base = name.substring(0, name.indexOf(".poi-"));
						File m = new File(r.getParentFile(), base);
						if (!maps.contains(m)) {
							maps.add(m);
						}
					}
				}
			}
		}
		if (maps.isEmpty()) {
			System.out.println("Usage: poi-extraction-summary <dir or <map>" + PoiNamesExtractor.RECORDS_EXT + ">... [--top=20]");
			return;
		}
		maps.sort(null);
		Summary total = new Summary("TOTAL");
		for (File m : maps) {
			Summary s = new Summary(m.getName());
			s.read(new File(m.getPath() + PoiNamesExtractor.ENTITIES_EXT), new File(m.getPath() + PoiNamesExtractor.RECORDS_EXT));
			s.print(top);
			total.add(s);
		}
		if (maps.size() > 1) {
			total.print(top);
		}
	}

	private static class Summary {
		final String name;
		long entitiesSize, recordsSize;
		int entities, withOperator, withNetwork, records, centers, entitiesWithRecords, multiPoiEntities;
		final Map<String, Integer> types = new HashMap<>();
		final Map<String, Integer> additional = new HashMap<>();

		Summary(String name) {
			this.name = name;
		}

		void read(File entitiesFile, File recordsFile) throws IOException {
			entitiesSize = entitiesFile.length();
			recordsSize = recordsFile.length();
			Map<String, Integer> poiPerEntity = new HashMap<>();
			readTsv(recordsFile, PoiNamesExtractor.RECORDS_HEADER, c -> {
				records++;
				poiPerEntity.merge(c[0] + "/" + c[1], 1, Integer::sum);
				if (!"0".equals(c[2])) {
					centers++;
				}
				types.merge(c[4] + "/" + c[5], 1, Integer::sum);
				if (!c[6].isEmpty()) {
					for (String a : c[6].split(";")) {
						additional.merge(a, 1, Integer::sum);
					}
				}
			});
			Set<String> extracted = new HashSet<>();
			readTsv(entitiesFile, PoiNamesExtractor.ENTITIES_HEADER, c -> {
				if (!extracted.add(c[0] + "/" + c[1])) {
					return;
				}
				entities++;
				JsonObject tags = GSON.fromJson(c[2], JsonObject.class);
				if (tags.has(OPERATOR)) {
					withOperator++;
				}
				if (tags.has(NETWORK)) {
					withNetwork++;
				}
			});
			entitiesWithRecords = poiPerEntity.size();
			for (int n : poiPerEntity.values()) {
				if (n > 1) {
					multiPoiEntities++;
				}
			}
		}

		void add(Summary s) {
			entitiesSize += s.entitiesSize;
			recordsSize += s.recordsSize;
			entities += s.entities;
			withOperator += s.withOperator;
			withNetwork += s.withNetwork;
			records += s.records;
			centers += s.centers;
			entitiesWithRecords += s.entitiesWithRecords;
			multiPoiEntities += s.multiPoiEntities;
			s.types.forEach((k, v) -> types.merge(k, v, Integer::sum));
			s.additional.forEach((k, v) -> additional.merge(k, v, Integer::sum));
		}

		void print(int top) {
			System.out.println("== " + name);
			System.out.printf("entities: %d (%.1f MB), with POI: %d, with operator: %d (%.1f%%), with network: %d (%.1f%%)%n",
					entities, entitiesSize / 1e6, entitiesWithRecords, withOperator, pct(withOperator, entities),
					withNetwork, pct(withNetwork, entities));
			System.out.printf("POI: %d (%.1f MB), relation centers > 0: %d, entities with several POI: %d%n", records,
					recordsSize / 1e6, centers, multiPoiEntities);
			printTop("type/subtype", types, top);
			printTop("additional", additional, top);
		}

		private void printTop(String title, Map<String, Integer> counts, int top) {
			List<Map.Entry<String, Integer>> l = new ArrayList<>(counts.entrySet());
			l.sort(Map.Entry.<String, Integer>comparingByValue().reversed().thenComparing(Map.Entry.comparingByKey()));
			System.out.println("top " + title + ":");
			for (int i = 0; i < Math.min(top, l.size()); i++) {
				System.out.printf("  %8d  %s%n", l.get(i).getValue(), l.get(i).getKey());
			}
		}
	}

	private static double pct(int part, int all) {
		return all == 0 ? 0 : part * 100.0 / all;
	}

	private static void readTsv(File f, String header, Consumer<String[]> row) throws IOException {
		if (!f.exists()) {
			System.out.println("WARN: no file " + f);
			return;
		}
		if (PoiNamesExtractor.readTsv(f, header, row) == null) {
			System.out.println("WARN: no meta line in " + f);
		}
	}
}
