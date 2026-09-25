package net.osmand.obf.preparation;

import gnu.trove.map.hash.TLongLongHashMap;
import net.osmand.binary.BinaryMapDataObject;
import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.binary.BinaryMapIndexReader.MapIndex;
import net.osmand.binary.BinaryMapIndexReader.MapRoot;
import net.osmand.util.MapUtils;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;

import java.io.*;
import java.util.*;

/**
 * natural=coastline of every zoom level stored in detailed maps, one coastline_<minzoom>-<maxzoom>.osm.bz2 per level
 * (the same shape as coastline.osm.bz2 of the basemap). Points with equal 31-bit coordinates become one node, so the
 * pieces of a coastline split between OBF blocks and maps join up again. Each way carries the map it comes from.
 *
 * Used to compare the basemap coastline (coastline.osm.bz2, oceantiles_12) with the detailed maps.
 *
 *   export-coastlines <dir of obf> <out dir>
 * Reads regular *_2.obf only: no World_*, wiki, srtm, travel.
 */
public class CoastlineExporter {

	static class Level {
		final String name;
		final File nodes, ways;
		final Writer nw, ww;
		final TLongLongHashMap ids = new TLongLongHashMap();
		long nextNode = 1, nextWay = 1, points;

		Level(File dir, String name) throws IOException {
			this.name = name;
			nodes = new File(dir, "coastline_" + name + ".nodes.tmp");
			ways = new File(dir, "coastline_" + name + ".ways.tmp");
			nw = new BufferedWriter(new FileWriter(nodes), 1 << 20);
			ww = new BufferedWriter(new FileWriter(ways), 1 << 20);
		}

		long node(int x, int y) throws IOException {
			long k = ((long) x << 32) | (y & 0xffffffffL);
			long id = ids.get(k);
			if (id == 0) {
				id = nextNode++;
				ids.put(k, id);
				nw.write(String.format(Locale.US, "  <node id='-%d' version='1' lat='%.7f' lon='%.7f'/>%n", id,
						MapUtils.get31LatitudeY(y), MapUtils.get31LongitudeX(x)));
			}
			return id;
		}

		void way(BinaryMapDataObject o, String map) throws IOException {
			StringBuilder sb = new StringBuilder();
			sb.append("  <way id='-").append(nextWay++).append("' version='1'>\n");
			for (int i = 0; i < o.getPointsLength(); i++) {
				sb.append("    <nd ref='-").append(node(o.getPoint31XTile(i), o.getPoint31YTile(i))).append("'/>\n");
				points++;
			}
			sb.append("    <tag k='natural' v='coastline'/>\n");
			sb.append("    <tag k='osmand_map' v='").append(map).append("'/>\n");
			sb.append("  </way>\n");
			ww.write(sb.toString());
		}
	}

	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.out.println("Usage: export-coastlines <dir of obf> <out dir>");
			return;
		}
		File dir = new File(args[0]), out = new File(args[1]);
		out.mkdirs();
		File[] files = dir.listFiles((d, n) -> n.endsWith("_2.obf") && !n.startsWith("World_") && !n.contains(".wiki")
				&& !n.contains(".srtm") && !n.contains(".travel") && !n.startsWith("Routing_test"));
		Arrays.sort(files);
		Map<String, Level> levels = new TreeMap<>();
		long st = System.currentTimeMillis();
		for (File f : files) {
			String map = f.getName().replace("_2.obf", "");
			StringBuilder log = new StringBuilder(map);
			try (RandomAccessFile raf = new RandomAccessFile(f, "r")) {
				BinaryMapIndexReader r = new BinaryMapIndexReader(raf, f);
				for (MapIndex mi : r.getMapIndexes()) {
					for (MapRoot root : mi.getRoots()) {
						String name = root.getMinZoom() + "-" + root.getMaxZoom();
						Level l = levels.get(name);
						if (l == null) {
							levels.put(name, l = new Level(out, name));
						}
						BinaryMapIndexReader.SearchRequest<BinaryMapDataObject> req = BinaryMapIndexReader.buildSearchRequest(
								0, Integer.MAX_VALUE, 0, Integer.MAX_VALUE, root.getMinZoom(), (types, index) -> {
									for (int i = 0; i < types.size(); i++) {
										if (types.get(i) == index.coastlineEncodingType) {
											return true;
										}
									}
									return false;
								});
						int n = 0;
						for (BinaryMapDataObject o : r.searchMapIndex(req, mi)) {
							// the request zoom selects this level only; skip objects of other types that share the filter
							if (o.containsType(mi.coastlineEncodingType) && o.getPointsLength() > 1) {
								l.way(o, map);
								n++;
							}
						}
						log.append(' ').append(name).append(':').append(n);
					}
				}
			}
			System.out.println(log);
		}
		System.out.printf("read %d maps in %d s%n", files.length, (System.currentTimeMillis() - st) / 1000);
		List<Thread> threads = new ArrayList<>();
		for (Level l : levels.values()) {
			l.nw.close();
			l.ww.close();
			Thread t = new Thread(() -> {
				try {
					File res = new File(out, "coastline_" + l.name + ".osm.bz2");
					try (OutputStream os = new BZip2CompressorOutputStream(new BufferedOutputStream(new FileOutputStream(res), 1 << 20), 9)) {
						os.write("<?xml version='1.0' encoding='UTF-8'?>\n<osm version='0.6' upload='false' generator='CoastlineExporter'>\n".getBytes("UTF-8"));
						copy(l.nodes, os);
						copy(l.ways, os);
						os.write("</osm>\n".getBytes("UTF-8"));
					}
					l.nodes.delete();
					l.ways.delete();
					System.out.printf("%s: %d ways, %d points, %d nodes, %d MB%n", res.getName(), l.nextWay - 1, l.points,
							l.nextNode - 1, res.length() >> 20);
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			});
			t.start();
			threads.add(t);
		}
		for (Thread t : threads) {
			t.join();
		}
		System.out.printf("done in %d s%n", (System.currentTimeMillis() - st) / 1000);
	}

	static void copy(File f, OutputStream os) throws IOException {
		try (InputStream in = new BufferedInputStream(new FileInputStream(f), 1 << 20)) {
			byte[] buf = new byte[1 << 20];
			int n;
			while ((n = in.read(buf)) > 0) {
				os.write(buf, 0, n);
			}
		}
	}
}
