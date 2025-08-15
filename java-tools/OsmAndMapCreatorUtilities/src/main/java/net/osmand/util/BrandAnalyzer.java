package net.osmand.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

public class BrandAnalyzer {

	private static final String PLANET_NAME = "planet";
	private static final int MIN_OCCURENCIES_PRINT = 7;
	private static final int MIN_OCCURENCIES = 5;
	private static final int TOP_PER_MAP = 30;
	public static double BRAND_OWNERSHIP = 0.7;
	private static int VERBOSE = 1;

	public static class BrandInfo {
		String brandName;
		BrandRegion ownerRegion;
		float ownerPercent;
		Integer ownerCount;
		Integer globalCount;
		boolean include;
		
		BrandInfo anotherOwnRegion = null; // linked list
		
		public boolean regionOwnsThisBrand(BrandRegion r, boolean includeChildren) {
			if (ownerRegion == r) {
				return true;
			}
			if (includeChildren && r.checkIfThisIsParent(ownerRegion)) {
				return true;
			}
			if (anotherOwnRegion == null) {
				return false;
			}
			return anotherOwnRegion.regionOwnsThisBrand(r, includeChildren);
		}
	}

	public static class BrandRegion {
		final String name;
		final Map<String, Integer> brands = new TreeMap<String, Integer>();
		Map<String, Integer> brandsSorted = new TreeMap<>(new Comparator<String>() {
			@Override
			public int compare(String o1, String o2) {
				Integer i1 = brands.get(o1);
				Integer i2 = brands.get(o2);
				if (Integer.compare(i1, i2) != 0) {
					return -Integer.compare(i1, i2);
				}
				return o1.compareTo(o2);
			}

		});
		
		int depth;
		String parent;
		boolean map;
		BrandRegion parentRegion;
		int countIncluded;

		public BrandRegion(String regName) {
			this.name = regName;
		}
		
		public boolean checkIfThisIsParent(BrandRegion child) {
			if (child == this) {
				return true;
			}
			BrandRegion p = child.parentRegion;
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
		File fl = new File("../../../all_brands.csv");
		new BrandAnalyzer().analyzeBrands(fl);
	}

	private void analyzeBrands(File fl) throws IOException {
		Map<String, BrandRegion> regions = parseBrandsFile(fl);
		
		Map<String, BrandInfo> brands = calculateBrandOwnership(regions);
		
		enableBrandsPerRegion(brands, regions, MIN_OCCURENCIES, TOP_PER_MAP);
		consolidateEnabledBrands(brands, regions);
		// include all parent and
		countIncluded(regions, brands);
		
		printRegionsSorted(regions, brands, null, 400, 0);
		System.out.println("-----------");
//		printRegionsSorted(regions, brands, "ukraine", 100, 1000 );
//		System.out.println("-----------");
		printRegionsSorted(regions, brands, "slovakia", 100, 1000 );
		System.out.println("-----------");
		printRegionsSorted(regions, brands, "netherlands", 100, 1000 );

	}

	private void consolidateEnabledBrands(Map<String, BrandInfo> brands, Map<String, BrandRegion> regions) {
		// validate main thing that for each region ownership of enabled brand includes all brands above it
		boolean changed = true;
		int iteration = 1;
		while (changed) {
			changed = false;
			for (BrandInfo b : brands.values()) {
				if (!b.include) {
					continue;
				}
				String brandName = b.brandName;
				while (b != null) {
					BrandRegion ownerRegion = b.ownerRegion;
					Iterator<String> it = ownerRegion.brandsSorted.keySet().iterator();
					while (it.hasNext()) {
						String topRegionBrand = it.next();
						BrandInfo topBrandToBeEnabled = brands.get(topRegionBrand);
						if (!topBrandToBeEnabled.include
								&& topBrandToBeEnabled.regionOwnsThisBrand(ownerRegion, false)) {
							if (VERBOSE >= 3) {
								System.out.println("Enable " + topRegionBrand + " for " + ownerRegion.name
										+ " because of " + brandName);
							}
							topBrandToBeEnabled.include = true;
							changed = true;
						}
						if (topRegionBrand.equals(brandName)) {
							break;
						}
					}
					b = b.anotherOwnRegion;
				}

			}
			System.out.printf("Consolidation # %d - enabled %d brands \n", iteration, countAllIncluded(brands));
			iteration++;
		}
	}

	private void enableBrandsPerRegion(Map<String, BrandInfo> brands, Map<String, BrandRegion> regions,
			int minOccurencies, int topPerMap) {
		for (BrandRegion r : regions.values()) {
			Iterator<Entry<String, Integer>> it = r.brandsSorted.entrySet().iterator();
			int cnt = 0;
			while (it.hasNext() && cnt++ < topPerMap) {
				Entry<String, Integer> e = it.next();
				String brandName = e.getKey();
				if (e.getValue() < minOccurencies) {
					break;
				}
				BrandInfo brandInfo = brands.get(brandName);
				brandInfo.include = true;
				if (VERBOSE >= 2) {
					System.out.println("Enable " + e.getKey() + " " + r.name + " " + e.getValue());
				}
			}
		}
		System.out.printf("For %d min occurencies and top %d per map - enabled %d brands \n",
				minOccurencies, topPerMap, countAllIncluded(brands));
	}

	private int countAllIncluded(Map<String, BrandInfo> brands) {
		int enabled = 0;
		for (BrandInfo i : brands.values()) {
			if (i.include) {
				enabled++;
			}
		}
		return enabled;
	}

	private void countIncluded(Map<String, BrandRegion> regions, Map<String, BrandInfo> brands) {
		for (BrandRegion r : regions.values()) {
			int count = 0;
			for (String brandName : r.brandsSorted.keySet()) {
				BrandInfo brand = brands.get(brandName);
				if (brand.include) {
					count++;
				}
			}
			r.countIncluded = count;
		}
	}

	private void printRegionsSorted(Map<String, BrandRegion> regions, Map<String, BrandInfo> brands, 
			String filter, int limit, int topBrands) {
		countIncluded(regions, brands);
		Map<String, BrandRegion> regSorted = new TreeMap<>(new Comparator<String>() {
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
		for (BrandRegion r : regSorted.values()) {
			if (filter != null && !r.name.contains(filter)) {
				continue;
			}
			if (i++ > limit) {
				break;
			}
			int cntFilter = 0;
			int owned = 0;
			int sub_owns = 0;
			for (Entry<String, Integer> brand : r.brands.entrySet()) {
				if(brand.getValue() > MIN_OCCURENCIES_PRINT) {
					cntFilter++;
					BrandInfo bi = brands.get(brand.getKey());
					if(bi.regionOwnsThisBrand(r, false)) {
						owned++;
					} else if(bi.regionOwnsThisBrand(r, true)) {
						sub_owns++;
					}
				}
			}
			
			System.out.printf("+%d %s --- %d all --- >%d: %d, own %d + subown %d\n", 
					r.countIncluded, r.name, 
					r.brands.size(), MIN_OCCURENCIES_PRINT, cntFilter, owned, sub_owns);
			int l = topBrands;
			Iterator<Entry<String, Integer>> it = r.brandsSorted.entrySet().iterator();
			while (it.hasNext() && l-- > 0) {
				Entry<String, Integer> n = it.next();
				BrandInfo brandInfo = brands.get(n.getKey());
				System.out.printf("    %s - %d: owner %s %d/%d %d%%\n", (brandInfo.include?"+":"-")+ n.getKey(), n.getValue(),
						brandInfo.ownerRegion.name, brandInfo.ownerCount, brandInfo.globalCount,
						(int)(brandInfo.ownerPercent  * 100));
			}
		}
	}

	private Map<String, BrandInfo> calculateBrandOwnership(Map<String, BrandRegion> regions) {
		int maxDepth = 0;
		for (BrandRegion b : regions.values()) {
			maxDepth = Math.max(maxDepth, b.depth);
		}
		Map<String, BrandInfo> brandOnwership = new TreeMap<>();
		BrandRegion planet = regions.get(PLANET_NAME);
		for (int depth = maxDepth; depth >= 0; depth--) {
			for (BrandRegion reg : regions.values()) {
				if (reg.depth != depth) {
					continue;
				}
				if (VERBOSE >= 2) {
					System.out.println("---- " + reg.name);
				}
				countBrandOnwershipPerRegion(brandOnwership, planet, reg);
			}
		}
		return brandOnwership;
	}

	private void countBrandOnwershipPerRegion(Map<String, BrandInfo> brandOnwership, BrandRegion planet,
			BrandRegion reg) {
		
		Iterator<Entry<String, Integer>> it = reg.brandsSorted.entrySet().iterator();
		while (it.hasNext()) {
			Entry<String, Integer> e = it.next();
			Integer globalCount = planet.brandsSorted.get(e.getKey());
			float percent = ((float) e.getValue()) / globalCount;
			if (percent > BRAND_OWNERSHIP) {
				BrandInfo info = new BrandInfo();
				info.brandName = e.getKey();
				info.ownerPercent = percent;
				info.globalCount = globalCount;
				info.ownerCount = e.getValue();
				info.ownerRegion = reg;
				BrandInfo childPossible = brandOnwership.get(info.brandName);
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
						System.out.printf("%s --- %d/%d (%.2f)\n", info.brandName,
								info.ownerCount, info.globalCount, percent);
					}
					info.anotherOwnRegion = childPossible;
					brandOnwership.put(info.brandName, info);
				}
			}
		}
	}

	private Map<String, BrandRegion> parseBrandsFile(File fl) throws FileNotFoundException, IOException {
		BufferedReader r = new BufferedReader(new FileReader(fl));
		Map<String, BrandRegion> regions = new TreeMap<>();
		String line = r.readLine(); // skip first line
		while ((line = r.readLine()) != null) {
			int ind = line.indexOf(',');
			String regName = line.substring(0, ind);
			line = line.substring(ind + 1);
			if (!regions.containsKey(regName)) {
				regions.put(regName, new BrandRegion(regName));
			}
			BrandRegion region = regions.get(regName);

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

			String brand = line;
			if (count > 0) {
				region.brands.put(brand, count);
			}
		}
		r.close();
		finishReading(regions);
		return regions;
	}

	private void finishReading(Map<String, BrandRegion> regions) {
		for (BrandRegion b : regions.values()) {
			b.brandsSorted.putAll(b.brands);
			// calculate parents
			if (!b.name.equals(PLANET_NAME)) {
				b.parentRegion = regions.get(b.parent);
				if (b.parentRegion == null) {
					throw new NullPointerException(b.name + " missing parent");
				}
			}
		}
		checkDepth(regions);
	}

	private void checkDepth(Map<String, BrandRegion> regions) {
		for (BrandRegion b : regions.values()) {
			int calcdepth = 0;
			BrandRegion p = b.parentRegion;
			while (p != null) {
				calcdepth++;
				p = p.parentRegion;
			}
			if (calcdepth != b.depth) {
				throw new IllegalStateException();
			}
		}
	}

}
