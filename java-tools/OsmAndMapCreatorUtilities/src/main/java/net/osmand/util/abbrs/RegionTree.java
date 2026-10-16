package net.osmand.util.abbrs;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

import net.osmand.map.OsmandRegions;
import net.osmand.map.WorldRegion;

/**
 * Regions of the maps: parent, depth and whether the region has a map. Built from {@code regions.ocbf} of the
 * generator, the same tree the extracts of {@code osm-extract} are cut by ({@code .parent}, {@code .depth},
 * {@code .map}); the snapshot {@code regions.csv} goes to the manifest.
 */
public class RegionTree {

	private static class Region {
		final String name;
		final String parent;
		final int depth;
		final boolean map;

		Region(String name, String parent, int depth, boolean map) {
			this.name = name;
			this.parent = parent;
			this.depth = depth;
			this.map = map;
		}
	}

	private final Map<String, Region> regions = new TreeMap<>();

	public static RegionTree fromOsmandRegions() throws IOException {
		File tmp = File.createTempFile("regions", ".ocbf");
		try {
			try (InputStream is = OsmandRegions.class.getResourceAsStream("regions.ocbf")) {
				if (is == null) {
					throw new IOException("regions.ocbf is not found next to " + OsmandRegions.class.getName());
				}
				Files.copy(is, tmp.toPath(), StandardCopyOption.REPLACE_EXISTING);
			}
			OsmandRegions or = new OsmandRegions(tmp.getAbsolutePath());
			RegionTree tree = new RegionTree();
			for (WorldRegion r : or.getFlattenedWorldRegions()) {
				String name = r.getRegionDownloadName();
				if (name == null || name.isEmpty()) {
					continue;
				}
				String parent = null;
				int depth = 0;
				for (WorldRegion p = r.getSuperregion(); p != null; p = p.getSuperregion()) {
					String pn = p.getRegionDownloadName();
					if (pn == null || pn.isEmpty()) {
						continue;
					}
					if (parent == null) {
						parent = pn.toLowerCase(Locale.ROOT);
					}
					depth++;
				}
				tree.add(name, parent, depth, r.isRegionMapDownload());
			}
			or.getReader().close();
			return tree;
		} finally {
			tmp.delete();
		}
	}

	public void add(String name, String parent, int depth, boolean map) {
		String n = name.toLowerCase(Locale.ROOT);
		regions.put(n, new Region(n, parent, depth, map));
	}

	/** the region of a map ("Germany_bremen-city_europe_2" -> "germany_bremen-city_europe"), null if unknown */
	public String region(String mapName) {
		String n = mapName.toLowerCase(Locale.ROOT);
		if (n.endsWith("_2")) {
			n = n.substring(0, n.length() - 2);
		}
		return regions.containsKey(n) ? n : null;
	}

	public boolean contains(String mapName) {
		return region(mapName) != null;
	}

	/** depth of the region of a map, -1 for a map out of the tree */
	public int depth(String mapName) {
		String r = region(mapName);
		return r == null ? -1 : regions.get(r).depth;
	}

	/** the region of the map and its ancestors, nearest first */
	public List<String> chain(String mapName) {
		List<String> chain = new ArrayList<>();
		for (String r = region(mapName); r != null; r = regions.containsKey(r) ? regions.get(r).parent : null) {
			chain.add(r);
		}
		return chain;
	}

	public void writeCsv(File f) throws IOException {
		try (Writer w = new OutputStreamWriter(new FileOutputStream(f), StandardCharsets.UTF_8)) {
			w.write("region,parent,depth,map\n");
			for (Region r : regions.values()) {
				w.write(r.name + "," + (r.parent == null ? "" : r.parent) + "," + r.depth + "," + r.map + "\n");
			}
		}
	}
}
