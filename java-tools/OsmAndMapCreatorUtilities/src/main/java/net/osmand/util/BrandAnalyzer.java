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
	public static double BRAND_OWNERSHIP = 0.7;
	private static int VERBOSE = 1;

	public static class BrandInfo {
		String brandName;
		BrandRegion ownerRegion;
		float ownerPercent;
		Integer ownerCount;
		Integer globalCount;
		
		BrandInfo anotherOwnRegion = null; // linked list
	}

	public static class BrandRegion {
		final String name;
		final Map<String, Integer> brands = new TreeMap<String, Integer>();
		
		int depth;
		String parent;
		boolean map;
		BrandRegion parentRegion;
		int countOwnAndChildren;

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
		calculateParents(regions);
		checkDepth(regions);

		Map<String, BrandInfo> brandOwnership = calculateBrandOwnership(regions);
		
		int minOccurrencies = 15;
		// include all parent and
		for (BrandRegion r : regions.values()) {
			int countOwnAndChildren = 0;
			for (String brand : r.brands.keySet()) {
				BrandInfo owner = brandOwnership.get(brand);
				boolean includeBrand = false;
				while (owner != null) {
					if (r.checkIfThisIsParent(owner.ownerRegion)) {
						includeBrand = owner.globalCount > minOccurrencies;
						break;
					}
					owner = owner.anotherOwnRegion;
				}
				if (includeBrand) {
					countOwnAndChildren++;
				}
			}
			r.countOwnAndChildren = countOwnAndChildren;
		}
		printRegionsSorted(regions);

	}

	private void printRegionsSorted(Map<String, BrandRegion> regions) {
		Map<String, BrandRegion> regSorted = new TreeMap<>(new Comparator<String>() {
			@Override
			public int compare(String o1, String o2) {
				Integer i1 = regions.get(o1).countOwnAndChildren;
				Integer i2 = regions.get(o2).countOwnAndChildren;
				if (Integer.compare(i1, i2) != 0) {
					return -Integer.compare(i1, i2);
				}
				return o1.compareTo(o2);
			}

		});
		regSorted.putAll(regions);
		int i = 0;
		for (BrandRegion r : regSorted.values()) {
			if (i++ > 50) {
				break;
			}
			System.out.println(r.name + " --- " + r.countOwnAndChildren);
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
		Map<String, Integer> brands = new TreeMap<String, Integer>(new Comparator<String>() {
			@Override
			public int compare(String o1, String o2) {
				Integer i1 = reg.brands.get(o1);
				Integer i2 = reg.brands.get(o2);
				if (Integer.compare(i1, i2) != 0) {
					return -Integer.compare(i1, i2);
				}
				return o1.compareTo(o2);
			}
			
		});
		brands.putAll(reg.brands);
		
		
		Iterator<Entry<String, Integer>> it = brands.entrySet().iterator();
		while (it.hasNext()) {
			Entry<String, Integer> e = it.next();
			Integer globalCount = planet.brands.get(e.getKey());
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
					if (VERBOSE >= 2 && info.ownerCount > 10) {
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
		return regions;
	}

	private void calculateParents(Map<String, BrandRegion> regions) {
		for (BrandRegion b : regions.values()) {
			if (!b.name.equals(PLANET_NAME)) {
				b.parentRegion = regions.get(b.parent);
				if (b.parentRegion == null) {
					throw new NullPointerException(b.name + " missing parent");
				}
			}
		}
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
