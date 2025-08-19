package net.osmand.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.zip.GZIPOutputStream;

public class TopTagValuesAnalyzer {

	private static final String PLANET_NAME = "planet";
	private static final int MIN_OCCURENCIES_PRINT = 10;
	private static final int MIN_OCCURENCIES = 5;
	private static final int TOP_PER_MAP = 100;
	private static final int WORLD_TOP = 1000;
	public static double BRAND_OWNERSHIP = 0.7;
	private static int VERBOSE = 1;
	private static char KEY_SEP = '/';

	private static int BRAND_ID;
	
	public static class TagValueInfo {
		String tag;
		String value;
		String key;
		TagValueRegion ownerRegion;
		float ownerPercent;
		Integer ownerCount;
		Integer globalCount;
		boolean include;
		String reasonInclude;
		int id = BRAND_ID++;
		TagValueInfo anotherOwnRegion = null; // linked list
		
		public boolean regionOwnsThisKey(TagValueRegion r, boolean includeChildren) {
			if (ownerRegion == r) {
				return true;
			}
			if (includeChildren && r.checkIfThisIsParent(ownerRegion)) {
				return true;
			}
			if (anotherOwnRegion == null) {
				return false;
			}
			return anotherOwnRegion.regionOwnsThisKey(r, includeChildren);
		}
	}

	public static class TagValueRegion {
		final String name;
		final Map<String, Integer> tagValues = new TreeMap<String, Integer>();
		Map<String, Integer> tagValuesSorted = new TreeMap<>(new Comparator<String>() {
			@Override
			public int compare(String o1, String o2) {
				Integer i1 = tagValues.get(o1);
				Integer i2 = tagValues.get(o2);
				if (Integer.compare(i1, i2) != 0) {
					return -Integer.compare(i1, i2);
				}
				return o1.compareTo(o2);
			}

		});
		
		int depth;
		boolean leaf = true;
		String parent;
		boolean map;
		TagValueRegion parentRegion;
		int countIncluded;

		public TagValueRegion(String regName) {
			this.name = regName;
		}
		
		public boolean checkIfThisIsParent(TagValueRegion child) {
			if (child == this) {
				return true;
			}
			TagValueRegion p = child.parentRegion;
			while (p != null) {
				if (p == this) {
					return true;
				}
				p = p.parentRegion;
			}
			return false;
		}

	}

	public static void main(String[] args) throws IOException {
		File fl;
		boolean consolidate = false;
		if (args.length == 0) {
			fl = new File("../../../all_brands.csv");
			consolidate = true;
		} else {
			fl = new File(args[0]);
		}
		for (String ar : args) {
			if ("--consolidate".equals(ar)) {
				consolidate = true;
			}
		}
		System.out.printf("Run (%s) %s consolidate=%s", Arrays.toString(args), fl.getName(), consolidate);
		new TopTagValuesAnalyzer().analyzeTopTagValues(fl, consolidate);
	}

	private void analyzeTopTagValues(File fl, boolean consolidate) throws IOException {
		String outFile = "brands";
		Map<String, TagValueRegion> regions = parseTagValueFile(fl);
		
		Map<String, TagValueInfo> tagValues = calculateOwnership(regions);
		
		boolean worldBrandsOnlyOwned = false;
		enableTagValuePerRegion(tagValues, regions, worldBrandsOnlyOwned, MIN_OCCURENCIES, WORLD_TOP, true, PLANET_NAME);
		boolean onlyOwned = true;
		enableTagValuePerRegion(tagValues, regions, onlyOwned, MIN_OCCURENCIES, TOP_PER_MAP, false, PLANET_NAME);
		if (!onlyOwned && consolidate) {
			// not needed for only owned generates only garbage
			consolidateEnabledTagValues(tagValues, regions, 1);
		}
		printRegionsSorted(regions, tagValues, null, 400, 0);
		
//		System.out.println("-----------");
//		printRegionsSorted(regions, brands, "ukraine_khar", 100, 1000 );
//		System.out.println("-----------");
//		printRegionsSorted(regions, brands, "slovakia", 100, 1000 );
//		System.out.println("-----------");
//		printRegionsSorted(regions, brands, "netherlands", 100, 1000 );
		generateHtmlPage(regions, tagValues, outFile + ".html", 0);
		
		StringBuilder tagValuesStr = new StringBuilder();
		for (TagValueInfo tagValue : tagValues.values()) {
			if (tagValue.include) {
				tagValuesStr.append(tagValue.tag).append(',').append(tagValue.value).append('\n');
			}
		}
		FileOutputStream fous = new FileOutputStream(outFile + ".lst");
		fous.write(tagValuesStr.toString().getBytes());
		fous.close();

	}

	protected void consolidateEnabledTagValues(Map<String, TagValueInfo> tagValues, Map<String, TagValueRegion> regions, int mindepth) {
		// validate main thing that for each region ownership of enabled brand includes all brands above it
		boolean changed = true;
		int iteration = 1;
		while (changed) {
			changed = false;
			for (TagValueInfo b : tagValues.values()) {
				if (!b.include) {
					continue;
				}
				String keyName = b.key;
				while (b != null) {
					TagValueRegion ownerRegion = b.ownerRegion;
					Iterator<String> it = ownerRegion.tagValuesSorted.keySet().iterator();
					while (it.hasNext()) {
						String topRegionTagValue = it.next();
						TagValueInfo topBrandToBeEnabled = tagValues.get(topRegionTagValue);
						if (!topBrandToBeEnabled.include && ownerRegion.depth >= mindepth
								&& topBrandToBeEnabled.regionOwnsThisKey(ownerRegion, false)) {
							if (VERBOSE >= 3) {
								System.out.println("Enable " + topRegionTagValue + " for " + ownerRegion.name
										+ " because of " + keyName);
							}
							topBrandToBeEnabled.include = true;
							topBrandToBeEnabled.reasonInclude = String.format("Higher than %s in %s", 
									keyName.substring(0, Math.min(keyName.length(), 7)), ownerRegion.name);
							changed = true;
						}
						if (topRegionTagValue.equals(keyName)) {
							break;
						}
					}
					b = b.anotherOwnRegion;
				}

			}
			System.out.printf("Consolidation # %d - enabled %d brands \n", iteration, countAllIncluded(tagValues));
			iteration++;
		}
	}

	private void enableTagValuePerRegion(Map<String, TagValueInfo> tagValue, Map<String, TagValueRegion> regions, boolean onlyOwned, 
			int minOccurencies, int topPerMap, boolean include, String filter) {
		for (TagValueRegion r : regions.values()) {
			if (include && !r.name.contains(filter)) {
				continue;
			} else if (!include && r.name.contains(filter)) {
				continue;
			}
			Iterator<Entry<String, Integer>> it = r.tagValuesSorted.entrySet().iterator();
			int cnt = 0;
			l: while (it.hasNext()) {
				Entry<String, Integer> e = it.next();
				String keyName = e.getKey();
				TagValueInfo tagValueInfo = tagValue.get(keyName);
				if (onlyOwned && !tagValueInfo.regionOwnsThisKey(r, false)) {
					continue;
				}
				if (cnt++ >= topPerMap) {
					break l;
				}
				
				if (e.getValue() < minOccurencies) {
					break l;
				}
				
				tagValueInfo.include = true;
				tagValueInfo.reasonInclude = String.format("Top %d (>%d) for %s", topPerMap, minOccurencies, 
						r.name);
				if (VERBOSE >= 2) {
					System.out.println("Enable " + e.getKey() + " " + r.name + " " + e.getValue());
				}
			}
		}
		System.out.printf("For %d min occurencies and top %d per map - enabled %d brands \n",
				minOccurencies, topPerMap, countAllIncluded(tagValue));
	}

	private int countAllIncluded(Map<String, TagValueInfo> brands) {
		int enabled = 0;
		for (TagValueInfo i : brands.values()) {
			if (i.include) {
				enabled++;
			}
		}
		return enabled;
	}

	private void countIncluded(Map<String, TagValueRegion> regions, Map<String, TagValueInfo> brands) {
		for (TagValueRegion r : regions.values()) {
			int count = 0;
			for (String brandName : r.tagValuesSorted.keySet()) {
				TagValueInfo brand = brands.get(brandName);
				if (brand.include) {
					count++;
				}
			}
			r.countIncluded = count;
		}
	}

	private void printRegionsSorted(Map<String, TagValueRegion> regions, Map<String, TagValueInfo> brands, 
			String filter, int limit, int topBrands) {
		countIncluded(regions, brands);
		Map<String, TagValueRegion> regSorted = new TreeMap<>(new Comparator<String>() {
			@Override
			public int compare(String o1, String o2) {
				Integer i1 = regions.get(o1).countIncluded;
				Integer i2 = regions.get(o2).countIncluded;
				if (Integer.compare(i1, i2) != 0) {
					return -Integer.compare(i1, i2);
				}
				return o1.compareTo(o2);
			}

		});
		regSorted.putAll(regions);
		int i = 0;
		for (TagValueRegion r : regSorted.values()) {
			if (filter != null && !r.name.contains(filter)) {
				continue;
			}
			if (i++ > limit) {
				break;
			}
			int cntFilter = 0;
			int owned = 0;
			int sub_owns = 0;
			for (Entry<String, Integer> brand : r.tagValues.entrySet()) {
				if (brand.getValue() > MIN_OCCURENCIES_PRINT) {
					cntFilter++;
					TagValueInfo bi = brands.get(brand.getKey());
					if (bi.regionOwnsThisKey(r, false)) {
						owned++;
					} else if (bi.regionOwnsThisKey(r, true)) {
						sub_owns++;
					}
				}
			}
			
			System.out.printf("+%d %s --- %d all --- >%d: %d, own %d + subown %d\n", 
					r.countIncluded, r.name, 
					r.tagValues.size(), MIN_OCCURENCIES_PRINT, cntFilter, owned, sub_owns);
			int l = topBrands;
			Iterator<Entry<String, Integer>> it = r.tagValuesSorted.entrySet().iterator();
			while (it.hasNext() && l-- > 0) {
				Entry<String, Integer> n = it.next();
				TagValueInfo brandInfo = brands.get(n.getKey());
				String owners = brandInfo.ownerRegion.name;
				TagValueInfo next = brandInfo.anotherOwnRegion;
				while (next != null) {
					owners += ", " + next.ownerRegion.name;
					next = next.anotherOwnRegion;
				}
				System.out.printf("    %s - %d: owner %s %d/%d %d%%\n", (brandInfo.include?"+":"-")+ n.getKey(), n.getValue(),
						owners, brandInfo.ownerCount, brandInfo.globalCount,
						(int)(brandInfo.ownerPercent  * 100));
			}
		}
	}

	private Map<String, TagValueInfo> calculateOwnership(Map<String, TagValueRegion> regions) {
		int maxDepth = 0;
		for (TagValueRegion b : regions.values()) {
			maxDepth = Math.max(maxDepth, b.depth);
		}
		Map<String, TagValueInfo> brandOnwership = new TreeMap<>();
		TagValueRegion planet = regions.get(PLANET_NAME);
		for (int depth = maxDepth; depth >= 0; depth--) {
			for (TagValueRegion reg : regions.values()) {
				if (reg.depth != depth) {
					continue;
				}
				if (VERBOSE >= 2) {
					System.out.println("---- " + reg.name);
				}
				countTagValueOnwershipPerRegion(brandOnwership, planet, reg);
			}
		}
		return brandOnwership;
	}

	private void countTagValueOnwershipPerRegion(Map<String, TagValueInfo> onwershipInfo, TagValueRegion planet,
			TagValueRegion reg) {
		
		Iterator<Entry<String, Integer>> it = reg.tagValuesSorted.entrySet().iterator();
		while (it.hasNext()) {
			Entry<String, Integer> e = it.next();
			Integer globalCount = planet.tagValuesSorted.get(e.getKey());
			float percent = ((float) e.getValue()) / globalCount;
			if (percent > BRAND_OWNERSHIP) {
				TagValueInfo info = new TagValueInfo();
				info.key = e.getKey();
				int ind = info.key.indexOf(KEY_SEP);
				info.tag = info.key.substring(0, ind);
				info.value = info.key.substring(ind + 1);
				info.ownerPercent = percent;
				info.globalCount = globalCount;
				info.ownerCount = e.getValue();
				info.ownerRegion = reg;
				TagValueInfo existing = onwershipInfo.get(info.key);
				TagValueInfo childPossible = existing;
				boolean checkIfIncludedInChild = false;
				while (childPossible != null) {
					if (reg.checkIfThisIsParent(childPossible.ownerRegion)) {
						checkIfIncludedInChild = true;
						break;
					}
					childPossible = childPossible.anotherOwnRegion;
				}
				if (!checkIfIncludedInChild) {
					if (VERBOSE >= 2 && info.ownerCount > MIN_OCCURENCIES) {
						System.out.printf("%s --- %d/%d (%.2f)\n", info.key,
								info.ownerCount, info.globalCount, percent);
					}
					if (existing != null && existing.ownerRegion.depth > info.ownerRegion.depth) {
						// skip ownership for higher region
//						existing.anotherOwnRegion = info;
					} else {
						if (existing == null || existing.ownerRegion.depth == info.ownerRegion.depth) {
							info.anotherOwnRegion = existing;
						}
						onwershipInfo.put(info.key, info);
					}
				}
			}
		}
	}

	private Map<String, TagValueRegion> parseTagValueFile(File fl) throws FileNotFoundException, IOException {
		BufferedReader r = new BufferedReader(new FileReader(fl));
		Map<String, TagValueRegion> regions = new TreeMap<>();
		String line = r.readLine(); // skip first line
		while ((line = r.readLine()) != null) {
			int ind = line.indexOf(',');
			String regName = line.substring(0, ind);
			line = line.substring(ind + 1);
			if (!regions.containsKey(regName)) {
				regions.put(regName, new TagValueRegion(regName));
			}
			TagValueRegion region = regions.get(regName);

			ind = line.indexOf(',');
			int count = Algorithms.parseIntSilently(line.substring(0, ind), 0);
			line = line.substring(ind + 1);

			ind = line.indexOf(',');
			region.parent = line.substring(0, ind);
			line = line.substring(ind + 1);

			ind = line.indexOf(',');
			region.depth = Algorithms.parseIntSilently(line.substring(0, ind), 0);
			line = line.substring(ind + 1);

			ind = line.indexOf(',');
			region.map = "true".equalsIgnoreCase(line.substring(0, ind));
			line = line.substring(ind + 1);
			
			ind = line.indexOf(',');
			String tag = line.substring(0, ind).toLowerCase();
			line = line.substring(ind + 1);

			String allValues = line;
			if (count > 0) {
				String[] splitValues = allValues.split(";");
				for(String tagValueKey : splitValues) {
					tagValueKey = normalizeTagValue(tagValueKey);
					int existing = region.tagValues.getOrDefault(tag + KEY_SEP + tagValueKey, 0);
					region.tagValues.put(tag + KEY_SEP + tagValueKey, count + existing);
				}
			}
		}
		r.close();
		finishReading(regions);
		return regions;
	}

	public static String normalizeTagValue(String tagValueKey) {
		return tagValueKey.toLowerCase().trim().replaceAll("[`’‘ʼ]", "'");
	}

	private void finishReading(Map<String, TagValueRegion> regions) {
		TagValueRegion planet = regions.get(PLANET_NAME);
		planet.map = true;
		for (TagValueRegion b : regions.values()) {
			b.tagValuesSorted.putAll(b.tagValues);
			// calculate parents
			if (b != planet) {
				b.parentRegion = regions.get(b.parent);
				if (b.parentRegion == null) {
					throw new NullPointerException(b.name + " missing parent");
				}
				b.parentRegion.leaf = false;
			}
		}
		for (TagValueRegion b : regions.values()) {
			if (b != planet) {
				while (!b.parentRegion.map) {
					b.parentRegion = b.parentRegion.parentRegion;
					b.parent = b.parentRegion.name;
				}

			}
		}
		// remove all intermediate regions (east-asia, north-europe)... and non-map
		for (TagValueRegion b : new ArrayList<>(regions.values())) {
			if (!b.map) {
				regions.remove(b.name);
				continue;
			}
			int calcdepth = 0;
			TagValueRegion p = b.parentRegion;
			while (p != null) {
				calcdepth++;
				p = p.parentRegion;
			}
			if (calcdepth != b.depth) {
				b.depth = calcdepth;
			}
		}
	}

	
	public void generateHtmlPage(Map<String, TagValueRegion> regions, Map<String, TagValueInfo> brands, String outputFile,
			int excludeGlobal) throws IOException {
		// Copy the HTML template as is
		FileOutputStream out = new FileOutputStream(outputFile);
		InputStream in = TopTagValuesAnalyzer.class.getResourceAsStream("/brands.html");
		Algorithms.streamCopy(in, out);
		out.close();
		in.close();
		System.out.println("Generated HTML page: " + outputFile);

		// Prepare data for the brands-data.js file
		StringBuilder brandData = new StringBuilder();
		brandData.append("const brandsData = {\n");
		for (TagValueInfo tagInfo : brands.values()) {
			String shortKeyName = tagInfo.value;
			if (tagInfo.tag.startsWith("brand:")) {
				shortKeyName = tagInfo.tag.substring("brand:".length()) + ":" + shortKeyName;
			} else if (!tagInfo.tag.equals("brand")) {
				shortKeyName = tagInfo.tag + ":" + shortKeyName;
			}
			brandData.append(String.format(
					"\t\t\t\t'%s': { name: '%s', owner: '%s', ownerCount: %d, globalCount: %d, ownerPercent: %d," +
					" includeReason: '%s', include: %s },\n",
					tagInfo.id, formatString(shortKeyName), formatString(tagInfo.ownerRegion.name),
					tagInfo.ownerCount, tagInfo.globalCount, (int) (tagInfo.ownerPercent * 100),
					formatString(tagInfo.reasonInclude), tagInfo.include));
		}
		brandData.append("};\n");
		brandData.append("\n");
		brandData.append("const brandsRegionData = {\n");

		Map<String, TagValueRegion> sortedRegions = new TreeMap<>(regions);
		for (TagValueRegion region : sortedRegions.values()) {
			brandData.append(String.format("	'%s': {\n", region.name));
			brandData.append(String.format("		'parent': '%s',", region.parent));
			brandData.append(String.format("		'included': '%s',", region.countIncluded));
			brandData.append(String.format("		'depth': %s,", region.depth));
			brandData.append(String.format("		'leaf': %s,", region.leaf));
			brandData.append(String.format("		'brands': [\n"));
			Iterator<Entry<String, Integer>> it = region.tagValuesSorted.entrySet().iterator();
			while (it.hasNext()) {
				Entry<String, Integer> entry = it.next();
				TagValueInfo brandInfo = brands.get(entry.getKey());
				if (brandInfo != null) {
					if (!brandInfo.include && brandInfo.globalCount <= excludeGlobal) {
						continue;
					}
					brandData.append(String.format("			{ id: %d, count: %d },\n", brandInfo.id, entry.getValue()));
				}
			}
			brandData.append("		] },\n");
		}
		brandData.append("};\n");

		File outputHtmlFile = new File(outputFile);
		String jsFileName = "brands-data.js";
		File jsFile = new File(outputHtmlFile.getParentFile(), jsFileName);
		File gzippedJsFile = new File(outputHtmlFile.getParentFile(), jsFileName + ".gz");

		// Write the final JS to a file
		try (PrintWriter writer = new PrintWriter(jsFile, "UTF-8")) {
			writer.print(brandData.toString());
		}
		System.out.println("Generated JS data file: " + jsFile.getAbsolutePath());
		FileOutputStream fos = new FileOutputStream(gzippedJsFile);
		GZIPOutputStream gzipos = new GZIPOutputStream(fos);
		gzipos.write(brandData.toString().getBytes("UTF-8"));
		gzipos.close();
		
		System.out.println("Generated gzipped JS data file: " + gzippedJsFile.getAbsolutePath());
	}

	private String formatString(String s) {
		if (s == null) {
			return "";
		}
		return s.replace("\\", "\\\\").replace("'", "\\'");
	}


}
