package net.osmand.util;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.BufferedOutputStream;
import java.io.PrintWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.regex.Pattern;
import java.util.zip.DataFormatException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.Inflater;

import crosby.binary.Fileformat;

/**
 * Counts values of selected tags (brand, brand:*) in OSM extracts and writes all_brands.csv
 * for {@link TopTagValuesAnalyzer}. Replaces "osmconvert --csv | awk | sort | uniq -c":
 * reads .pbf and .o5m directly, keeps only the counts, caches one result per region.
 *
 * Every object (node, way, relation) that has a tag counts once for (tag, value).
 * Value is lower-cased, whitespace runs become one space, leading/trailing spaces are dropped.
 */
public class TagValuesCollector {

	public static final String[] DEFAULT_TAGS = { "brand", "brand:en", "brand:be", "brand:ja", "brand:zh", "brand:ru",
			"brand:uk", "brand:ar", "brand:fr", "brand:ur", "brand:ko", "brand:pa", "brand:es", "brand:pnb",
			"brand:hi", "brand:ka", "brand:zh-Hans", "brand:short", "brand:zh-Hant", "brand:ks", "brand:de",
			"brand:th", "brand:ta", "brand:ml", "brand:te", "brand:as" };
	private static final String PLANET_FILE = "planet-latest.o5m";
	private static final String PLANET = "planet";
	private static final String CACHE_HEADER = "#source ";
	private static final Pattern SPACES = Pattern.compile("[ \t\n]+");

	// brand tags, then PoiStats.TAGS (from poiBase) when poi stats are collected in the same pass
	private final String[] tags;
	private final int poiBase;
	// poi stats tags that are brand tags too ("brand"): tagIndex() finds the brand one, copy it over
	private final int[] poiDup;
	private final byte[][] tagBytes;
	private final int threads;
	// with poi stats every object is read as a whole and handed to object()
	private final PoiStats poi;

	public TagValuesCollector(String[] tags, int threads) {
		this(tags, threads, null);
	}

	public TagValuesCollector(String[] brandTags, int threads, PoiStats poi) {
		this.poiBase = brandTags.length;
		if (poi == null) {
			this.tags = brandTags;
		} else {
			this.tags = Arrays.copyOf(brandTags, brandTags.length + PoiStats.TAGS.length);
			System.arraycopy(PoiStats.TAGS, 0, this.tags, brandTags.length, PoiStats.TAGS.length);
		}
		this.threads = threads;
		this.poi = poi;
		this.poiDup = new int[tags.length];
		Arrays.fill(poiDup, -1);
		for (int i = poiBase; i < tags.length; i++) {
			for (int j = 0; j < poiBase; j++) {
				if (tags[i].equals(tags[j])) {
					poiDup[i] = j;
				}
			}
		}
		tagBytes = new byte[tags.length][];
		for (int i = 0; i < tags.length; i++) {
			tagBytes[i] = tags[i].getBytes(StandardCharsets.UTF_8);
		}
	}

	public static class Region {
		final String name;
		final File file;
		final String parent;
		final int depth;
		final boolean map;

		Region(String name, File file, String parent, int depth, boolean map) {
			this.name = name;
			this.file = file;
			this.parent = parent;
			this.depth = depth;
			this.map = map;
		}
	}

	/** key = tag + '\t' + normalized value; in --poi-stats mode key = kind + '\t' + a + '\t' + b, value = {count, extra} */
	public static class Counts {
		final Map<String, int[]> values = new HashMap<>();
		final Map<String, String> samples = new HashMap<>();
		long objects;
		// poi stats collected in the same pass as the brand counts
		Counts poi;

		Counts poi() {
			if (poi == null) {
				poi = new Counts();
			}
			return poi;
		}

		void add(String tag, String value, int cnt) {
			int[] c = values.get(tag + '\t' + value);
			if (c == null) {
				values.put(tag + '\t' + value, new int[] { cnt });
			} else {
				c[0] += cnt;
			}
		}

		void add(String key, int cnt, int extra) {
			int[] c = values.get(key);
			if (c == null) {
				values.put(key, new int[] { cnt, extra });
			} else {
				c[0] += cnt;
				c[1] += extra;
			}
		}

		void addAll(Counts o) {
			objects += o.objects;
			for (Entry<String, int[]> e : o.values.entrySet()) {
				int[] c = values.get(e.getKey());
				if (c == null) {
					values.put(e.getKey(), e.getValue());
				} else {
					for (int i = 0; i < c.length; i++) {
						c[i] += e.getValue()[i];
					}
				}
			}
			for (Entry<String, String> e : o.samples.entrySet()) {
				samples.putIfAbsent(e.getKey(), e.getValue());
			}
			if (o.poi != null) {
				poi().addAll(o.poi);
			}
		}

		List<Entry<String, int[]>> sorted() {
			List<Entry<String, int[]>> l = new ArrayList<>(values.entrySet());
			Collections.sort(l, (a, b) -> {
				int r = Integer.compare(b.getValue()[0], a.getValue()[0]);
				return r != 0 ? r : a.getKey().compareTo(b.getKey());
			});
			return l;
		}
	}

	static String normalize(String v) {
		return SPACES.matcher(v.toLowerCase(Locale.ROOT)).replaceAll(" ").trim();
	}

	public static void main(String[] args) throws Exception {
		String extracts = null, planet = null, out = "all_brands.csv", cache = "brands", file = null;
		String poiOut = null, poiCache = "poi-stats";
		String[] tags = DEFAULT_TAGS;
		int threads = Runtime.getRuntime().availableProcessors();
		boolean force = false, poiOnly = false, allRegions = false;
		int minCount = 3;
		for (String a : args) {
			if (a.equals("--poi-stats")) {
				// poi stats only, no brand counts
				poiOnly = true;
			} else if (a.startsWith("--poi-out=")) {
				poiOut = a.substring("--poi-out=".length());
			} else if (a.startsWith("--poi-cache=")) {
				poiCache = a.substring("--poi-cache=".length());
			} else if (a.equals("--all-regions")) {
				allRegions = true;
			} else if (a.startsWith("--min-count=")) {
				minCount = Integer.parseInt(a.substring("--min-count=".length()));
			} else if (a.startsWith("--extracts=")) {
				extracts = a.substring("--extracts=".length());
			} else if (a.startsWith("--planet=")) {
				planet = a.substring("--planet=".length());
			} else if (a.startsWith("--out=")) {
				out = a.substring("--out=".length());
			} else if (a.startsWith("--cache=")) {
				cache = a.substring("--cache=".length());
			} else if (a.startsWith("--file=")) {
				file = a.substring("--file=".length());
			} else if (a.startsWith("--tags=")) {
				tags = a.substring("--tags=".length()).split(",");
			} else if (a.startsWith("--threads=")) {
				threads = Integer.parseInt(a.substring("--threads=".length()));
			} else if (a.equals("--force")) {
				force = true;
			} else {
				throw new IllegalArgumentException("Unknown argument " + a);
			}
		}
		if (poiOnly && poiOut == null) {
			poiOut = "poi_name_stats.tsv";
		}
		PoiStats poi = poiOut != null ? new PoiStats(minCount) : null;
		TagValuesCollector c = new TagValuesCollector(poiOnly ? new String[0] : tags, threads, poi);
		if (file != null) {
			long t = System.currentTimeMillis();
			Counts cnt = c.collect(new File(file), poi != null);
			long ms = System.currentTimeMillis() - t;
			PrintWriter w = new PrintWriter(new OutputStreamWriter(System.out, StandardCharsets.UTF_8));
			for (Entry<String, int[]> e : cnt.sorted()) {
				w.println(e.getValue()[0] + "\t" + e.getKey());
			}
			if (poi != null) {
				for (Entry<String, int[]> e : cnt.poi().sorted()) {
					if (poi.keep(e.getKey(), e.getValue()[0])) {
						w.println(PoiStats.line(e, cnt.poi()));
					}
				}
			}
			w.flush();
			System.err.printf("%s: %d objects, %d values, %d ms, %.1f MB/s%n", file, cnt.objects, cnt.values.size(),
					ms, new File(file).length() / 1e6 / Math.max(ms, 1) * 1000);
			return;
		}
		if (extracts == null) {
			System.err.println("Usage: collect-tag-values --extracts=<osm-extract dir> [--planet=<planet.o5m|pbf>] "
					+ "[--out=all_brands.csv] [--cache=brands] [--threads=N] [--tags=brand,brand:en,...] [--force]\n"
					+ "       collect-tag-values --file=<file.pbf|file.o5m> [--tags=...] [--poi-out=x]\n"
					+ "  in the same pass, poi stats of the leaf map regions: [--poi-out=poi_name_stats.tsv] [--poi-cache=poi-stats] "
					+ "[--min-count=3] [--all-regions]; --poi-stats = poi stats only, no brand counts");
			System.exit(1);
		}
		List<Region> regions = findRegions(new File(extracts), planet == null ? null : new File(planet));
		Set<Region> poiRegions = poi == null ? Collections.emptySet() : poiRegions(regions, allRegions);
		if (poiOnly) {
			regions = new ArrayList<>(poiRegions);
			Collections.sort(regions, (a, b) -> a.name.compareTo(b.name));
		}
		c.run(regions, poiRegions, poiOnly ? null : new File(cache), poiOnly ? null : new File(out),
				new File(poiCache), poiOut == null ? null : new File(poiOut), force);
	}

	/**
	 * Map regions without a map region below them: their sum is the world once. A parent with .map
	 * (europe, germany_europe) repeats its children, the planet repeats everything.
	 */
	static Set<Region> poiRegions(List<Region> regions, boolean allRegions) {
		Map<String, Region> byName = new HashMap<>();
		for (Region r : regions) {
			byName.put(r.name, r);
		}
		Set<String> hasMapBelow = new HashSet<>();
		for (Region r : regions) {
			if (!r.map || r.name.equals(PLANET)) {
				continue;
			}
			String p = r.parent;
			for (int guard = 0; p != null && !p.isEmpty() && guard < 32 && hasMapBelow.add(p); guard++) {
				Region pr = byName.get(p);
				p = pr == null ? null : pr.parent;
			}
		}
		Set<Region> sel = new HashSet<>();
		for (Region r : regions) {
			if (!r.name.equals(PLANET) && (allRegions || (r.map && !hasMapBelow.contains(r.name)))) {
				sel.add(r);
			}
		}
		System.out.printf("Poi stats regions %d of %d%n", sel.size(), regions.size());
		return sel;
	}

	public static List<Region> findRegions(File extracts, File planet) throws IOException {
		List<Region> regions = new ArrayList<>();
		File[] dirs = extracts.listFiles();
		if (dirs == null) {
			throw new IOException("Not a directory " + extracts);
		}
		Arrays.sort(dirs);
		for (File dir : dirs) {
			String name = dir.getName();
			File pbf = new File(dir, name + ".pbf");
			if (!pbf.isFile()) {
				pbf = new File(dir, name + ".o5m");
			}
			File parent = new File(dir, ".parent");
			File depth = new File(dir, ".depth");
			if (pbf.isFile() && parent.isFile()) {
				if (!depth.isFile()) {
					System.err.println("WARN skip " + name + ": no .depth");
					continue;
				}
				regions.add(new Region(name, pbf, readText(parent), Integer.parseInt(readText(depth)),
						new File(dir, ".map").exists()));
			} else if (planet == null && name.equals(PLANET_FILE)) {
				planet = dir;
			}
		}
		if (planet != null) {
			regions.add(new Region(PLANET, planet, "", 0, true));
		} else {
			System.err.println("WARN no " + PLANET_FILE + " in " + extracts);
		}
		return regions;
	}

	private static String readText(File f) throws IOException {
		return new String(Files.readAllBytes(f.toPath()), StandardCharsets.UTF_8).trim();
	}

	public void run(List<Region> regions, File cacheDir, File out, boolean force) throws Exception {
		run(regions, Collections.emptySet(), cacheDir, out, null, null, force);
	}

	/**
	 * Brand counts of every region in {@code regions} to {@code out} (skipped when null) and, in the same pass,
	 * poi stats of {@code poiRegions} to {@code poiOut} (skipped when null). One cache file per region and kind.
	 */
	public void run(List<Region> regions, Set<Region> poiRegions, File cacheDir, File out, File poiCacheDir,
			File poiOut, boolean force) throws Exception {
		boolean brands = out != null;
		if (brands) {
			cacheDir.mkdirs();
		}
		if (poiOut != null) {
			poiCacheDir.mkdirs();
		}
		long start = System.currentTimeMillis();
		List<Region> bySize = new ArrayList<>(regions);
		// biggest first, so the planet does not start last
		Collections.sort(bySize, (a, b) -> Long.compare(b.file.length(), a.file.length()));
		ExecutorService files = Executors.newFixedThreadPool(threads);
		Map<Region, Future<Counts>> results = new HashMap<>();
		int cached = 0;
		for (Region r : bySize) {
			boolean withPoi = poiOut != null && poiRegions.contains(r);
			File cf = brands ? cacheFile(cacheDir, r) : null;
			File pf = withPoi ? cacheFile(poiCacheDir, r) : null;
			if (!force) {
				// poi stats of the whole world do not fit in memory: keep none, read each region back when writing
				Counts c = brands ? readCache(cf, r.file, false) : new Counts();
				if (c != null && (!withPoi || cacheValid(pf, r.file))) {
					cached++;
					results.put(r, CompletableFuture.completedFuture(c));
					continue;
				}
			}
			results.put(r, files.submit(() -> {
				long t = System.currentTimeMillis();
				Counts c = collect(r.file, withPoi);
				if (brands) {
					writeCache(cf, r.file, c, false);
				}
				if (withPoi) {
					writeCache(pf, r.file, c.poi(), true);
				}
				System.out.printf("%s: %d objects, %d values%s, %.1f s%n", r.name, c.objects, c.values.size(),
						withPoi ? ", " + c.poi().values.size() + " poi stats" : "", (System.currentTimeMillis() - t) / 1000.0);
				c.poi = null;
				return c;
			}));
		}
		System.out.printf("Regions %d, poi stats %d, cached %d, threads %d%n", regions.size(), poiRegions.size(),
				cached, threads);
		List<String> failed = new ArrayList<>();
		try {
			if (brands) {
				File tmp = new File(out.getAbsolutePath() + ".tmp");
				try (Writer w = writer(tmp, out)) {
					w.write("region,count,parent,depth,map,tag,value\n");
					for (Region r : regions) {
						Counts c = result(results, r, failed);
						if (c == null) {
							continue;
						}
						for (Entry<String, int[]> e : c.sorted()) {
							String k = e.getKey();
							int t = k.indexOf('\t');
							w.write(r.name + "," + e.getValue()[0] + "," + r.parent + "," + r.depth + "," + r.map + ","
									+ k.substring(0, t) + "," + k.substring(t + 1) + "\n");
						}
					}
				}
				if (failed.isEmpty()) {
					Files.move(tmp.toPath(), out.toPath(), StandardCopyOption.REPLACE_EXISTING);
					System.out.printf("Written %s in %.1f s%n", out, (System.currentTimeMillis() - start) / 1000.0);
				}
			}
			if (poiOut != null) {
				File tmp = new File(poiOut.getAbsolutePath() + ".tmp");
				try (Writer w = writer(tmp, poiOut)) {
					w.write("region\tparent\tdepth\tmap\t" + PoiStats.HEADER + "\n");
					for (Region r : regions) {
						if (!poiRegions.contains(r) || result(results, r, failed) == null) {
							continue;
						}
						Counts c = readCache(cacheFile(poiCacheDir, r), r.file, true);
						for (Entry<String, int[]> e : c.sorted()) {
							w.write(r.name + "\t" + r.parent + "\t" + r.depth + "\t" + r.map + "\t" + PoiStats.line(e, c) + "\n");
						}
					}
				}
				if (failed.isEmpty()) {
					Files.move(tmp.toPath(), poiOut.toPath(), StandardCopyOption.REPLACE_EXISTING);
					System.out.printf("Written %s in %.1f s%n", poiOut, (System.currentTimeMillis() - start) / 1000.0);
				}
			}
		} finally {
			files.shutdown();
		}
		if (!failed.isEmpty()) {
			throw new IOException("Failed regions " + failed.size() + ": " + failed);
		}
	}

	private static Counts result(Map<Region, Future<Counts>> results, Region r, List<String> failed) {
		try {
			return results.get(r).get();
		} catch (Exception e) {
			if (!failed.contains(r.name)) {
				System.err.println("ERROR " + r.name + " " + r.file + ": " + e.getCause());
				failed.add(r.name);
			}
			return null;
		}
	}

	private static File cacheFile(File dir, Region r) {
		return new File(dir, r.name + ".tsv.gz");
	}

	/** gzip when the final name ends with .gz */
	private static Writer writer(File tmp, File target) throws IOException {
		OutputStream os = new BufferedOutputStream(new FileOutputStream(tmp), 1 << 16);
		if (target.getName().endsWith(".gz")) {
			os = new GZIPOutputStream(os, 1 << 16);
		}
		return new OutputStreamWriter(os, StandardCharsets.UTF_8);
	}

	static BufferedReader reader(File f) throws IOException {
		InputStream is = new FileInputStream(f);
		if (f.getName().endsWith(".gz")) {
			is = new GZIPInputStream(is, 1 << 16);
		}
		return new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8), 1 << 16);
	}

	private static String sourceId(File src) {
		return src.getAbsolutePath() + " " + src.length() + " " + src.lastModified();
	}

	private static boolean cacheValid(File cf, File src) throws IOException {
		if (!cf.isFile()) {
			return false;
		}
		try (BufferedReader r = reader(cf)) {
			return (CACHE_HEADER + sourceId(src)).equals(r.readLine());
		}
	}

	private static Counts readCache(File cf, File src, boolean poiFormat) throws IOException {
		if (!cf.isFile()) {
			return null;
		}
		try (BufferedReader r = reader(cf)) {
			String h = r.readLine();
			if (h == null || !h.equals(CACHE_HEADER + sourceId(src))) {
				return null;
			}
			Counts c = new Counts();
			String l;
			while ((l = r.readLine()) != null) {
				if (poiFormat) {
					// count, extra, kind, a, b, sample
					String[] f = l.split("\t", -1);
					String key = f[2] + '\t' + f[3] + '\t' + f[4];
					c.add(key, Integer.parseInt(f[0]), Integer.parseInt(f[1]));
					if (!f[5].isEmpty()) {
						c.samples.put(key, f[5]);
					}
					continue;
				}
				int t1 = l.indexOf('\t');
				int t2 = l.indexOf('\t', t1 + 1);
				c.add(l.substring(t1 + 1, t2), l.substring(t2 + 1), Integer.parseInt(l.substring(0, t1)));
			}
			return c;
		}
	}

	private void writeCache(File cf, File src, Counts c, boolean poiFormat) throws IOException {
		File tmp = new File(cf.getAbsolutePath() + ".tmp");
		try (Writer w = writer(tmp, cf)) {
			w.write(CACHE_HEADER + sourceId(src) + "\n");
			for (Entry<String, int[]> e : c.sorted()) {
				if (poiFormat) {
					if (poi.keep(e.getKey(), e.getValue()[0])) {
						w.write(e.getValue()[0] + "\t" + e.getValue()[1] + "\t" + e.getKey() + "\t"
								+ c.samples.getOrDefault(e.getKey(), "") + "\n");
					}
					continue;
				}
				w.write(e.getValue()[0] + "\t" + e.getKey() + "\n");
			}
		}
		Files.move(tmp.toPath(), cf.toPath(), StandardCopyOption.REPLACE_EXISTING);
	}

	public Counts collect(File f) throws Exception {
		return collect(f, poi != null);
	}

	/** withPoi: also hand every object to PoiStats (counts in Counts.poi) */
	public Counts collect(File f, boolean withPoi) throws Exception {
		try (InputStream in = new FileInputStream(f)) {
			byte[] magic = new byte[2];
			try (InputStream peek = new FileInputStream(f)) {
				if (peek.read(magic) != 2) {
					throw new EOFException("Empty file " + f);
				}
			}
			if ((magic[0] & 0xff) == 0xff && (magic[1] & 0xff) == 0xe0) {
				return new O5mReader(in, withPoi).read();
			}
			return readPbf(in, withPoi);
		}
	}

	// ---------------------------------------------------------------- PBF

	private Counts readPbf(InputStream is, boolean withPoi) throws Exception {
		DataInputStream in = new DataInputStream(new BufferedInputStream(is, 1 << 20));
		Counts total = new Counts();
		while (true) {
			int hl;
			try {
				hl = in.readInt();
			} catch (EOFException e) {
				break;
			}
			byte[] hb = new byte[hl];
			in.readFully(hb);
			Fileformat.BlockHeader header = Fileformat.BlockHeader.parseFrom(hb);
			byte[] bb = new byte[header.getDatasize()];
			in.readFully(bb);
			if (header.getType().equals("OSMData")) {
				total.addAll(parseBlock(bb, withPoi));
			}
		}
		return total;
	}

	/**
	 * Reads only what is needed straight from the protobuf wire format: the string table and
	 * keys/vals of nodes, dense nodes, ways and relations. Ids, coordinates, refs, members and info are skipped.
	 */
	private Counts parseBlock(byte[] blob, boolean withPoi) throws IOException, DataFormatException {
		byte[] data = null;
		int rawSize = -1;
		Pbf p = new Pbf(blob, 0, blob.length);
		while (p.more()) {
			int key = (int) p.varint();
			int field = key >>> 3;
			if (field == 1 && (key & 7) == 2) { // raw
				int l = (int) p.varint();
				data = Arrays.copyOfRange(blob, p.pos, p.pos + l);
				p.pos += l;
			} else if (field == 2 && (key & 7) == 0) { // raw_size
				rawSize = (int) p.varint();
			} else if (field == 3 && (key & 7) == 2) { // zlib_data
				int l = (int) p.varint();
				if (rawSize < 0) {
					throw new IOException("raw_size must precede zlib_data");
				}
				data = new byte[rawSize];
				Inflater inf = new Inflater();
				inf.setInput(blob, p.pos, l);
				int n = inf.inflate(data);
				inf.end();
				if (n != rawSize) {
					throw new IOException("Bad zlib blob " + n + " != " + rawSize);
				}
				p.pos += l;
			} else if ((key & 7) == 2) {
				throw new IOException("Unsupported blob compression, field " + field);
			} else {
				p.skip(key);
			}
		}
		if (data == null) {
			throw new IOException("Empty blob");
		}
		return parsePrimitiveBlock(data, withPoi);
	}

	private Counts parsePrimitiveBlock(byte[] data, boolean withPoi) throws IOException {
		Counts c = new Counts();
		int[] sOff = new int[0], sLen = new int[0];
		int sCount = 0;
		List<int[]> groups = new ArrayList<>();
		Pbf p = new Pbf(data, 0, data.length);
		while (p.more()) {
			int key = (int) p.varint();
			int field = key >>> 3;
			if ((field == 1 || field == 2) && (key & 7) == 2) {
				int l = (int) p.varint();
				if (field == 1) {
					Pbf st = new Pbf(data, p.pos, p.pos + l);
					sOff = new int[1024];
					sLen = new int[1024];
					while (st.more()) {
						int k = (int) st.varint();
						if (k == (1 << 3 | 2)) {
							int sl = (int) st.varint();
							if (sCount == sOff.length) {
								sOff = Arrays.copyOf(sOff, sCount * 2);
								sLen = Arrays.copyOf(sLen, sCount * 2);
							}
							sOff[sCount] = st.pos;
							sLen[sCount++] = sl;
							st.pos += sl;
						} else {
							st.skip(k);
						}
					}
				} else {
					groups.add(new int[] { p.pos, p.pos + l });
				}
				p.pos += l;
			} else {
				p.skip(key);
			}
		}
		int[] tagOf = new int[sCount];
		for (int i = 0; i < sCount; i++) {
			tagOf[i] = tagIndex(data, sOff[i], sLen[i]);
		}
		BlockStrings bs = new BlockStrings(data, sOff, sLen, tagOf, withPoi);
		IntList keys = new IntList(), vals = new IntList();
		for (int[] g : groups) {
			Pbf gp = new Pbf(data, g[0], g[1]);
			while (gp.more()) {
				int key = (int) gp.varint();
				int field = key >>> 3;
				if (field >= 1 && field <= 4 && (key & 7) == 2) {
					int l = (int) gp.varint();
					Pbf e = new Pbf(data, gp.pos, gp.pos + l);
					gp.pos += l;
					if (field == 2) {
						parseDense(c, bs, e);
					} else {
						// node (1), way (3), relation (4): keys = 2, vals = 3
						c.objects++;
						keys.size = 0;
						vals.size = 0;
						while (e.more()) {
							int k = (int) e.varint();
							int f = k >>> 3;
							if (f == 2) {
								e.readInts(k, keys);
							} else if (f == 3) {
								e.readInts(k, vals);
							} else {
								e.skip(k);
							}
						}
						if (keys.size != vals.size) {
							throw new IOException("keys/vals size mismatch");
						}
						for (int i = 0; i < keys.size; i++) {
							bs.add(c, keys.a[i], vals.a[i]);
						}
						bs.endObject(c);
					}
				} else {
					gp.skip(key);
				}
			}
		}
		return c;
	}

	private void parseDense(Counts c, BlockStrings bs, Pbf e) throws IOException {
		while (e.more()) {
			int k = (int) e.varint();
			int f = k >>> 3;
			if (f == 1 && (k & 7) == 2) { // packed ids: count them
				int len = (int) e.varint();
				int end = e.pos + len;
				while (e.pos < end) {
					if (e.buf[e.pos++] >= 0) {
						c.objects++;
					}
				}
			} else if (f == 10 && (k & 7) == 2) { // keys_vals: k v k v 0 k v 0 ...
				int len = (int) e.varint();
				int end = e.pos + len;
				while (e.pos < end) {
					int key = (int) e.varint();
					if (key != 0) {
						bs.add(c, key, (int) e.varint());
					} else {
						bs.endObject(c);
					}
				}
			} else {
				e.skip(k);
			}
		}
	}

	/** One whole object: brand values are counted here, PoiStats gets the rest. Clears {@code o}. */
	private void object(Counts c, String[] o) {
		for (int t = poiBase; t < o.length; t++) {
			if (poiDup[t] >= 0 && o[t] == null) {
				o[t] = o[poiDup[t]];
			}
		}
		for (int t = 0; t < poiBase; t++) {
			if (o[t] != null) {
				String v = normalize(o[t]);
				if (!v.isEmpty()) {
					c.add(tags[t], v, 1);
				}
			}
		}
		poi.object(c.poi(), o, poiBase);
	}

	private class BlockStrings {
		final byte[] data;
		final int[] sOff, sLen, tagOf;
		final String[] values;
		final String[] raw;
		final String[] object = new String[tags.length];
		// whole objects for PoiStats; otherwise every brand value is counted as it is read
		final boolean perObject;

		BlockStrings(byte[] data, int[] sOff, int[] sLen, int[] tagOf, boolean withPoi) {
			this.data = data;
			this.sOff = sOff;
			this.sLen = sLen;
			this.tagOf = tagOf;
			this.values = new String[tagOf.length];
			this.perObject = poi != null && withPoi;
			this.raw = perObject ? new String[tagOf.length] : null;
		}

		void endObject(Counts c) {
			if (perObject) {
				object(c, object);
			}
		}

		void add(Counts c, int k, int v) {
			int t = tagOf[k];
			if (t < 0) {
				return;
			}
			if (perObject) {
				if (raw[v] == null) {
					raw[v] = new String(data, sOff[v], sLen[v], StandardCharsets.UTF_8);
				}
				object[t] = raw[v];
				return;
			}
			if (t >= poiBase) {
				// a poi stats tag outside a poi stats region
				return;
			}
			if (values[v] == null) {
				values[v] = normalize(new String(data, sOff[v], sLen[v], StandardCharsets.UTF_8));
			}
			if (!values[v].isEmpty()) {
				c.add(tags[t], values[v], 1);
			}
		}
	}

	private static class IntList {
		int[] a = new int[16];
		int size;

		void add(int v) {
			if (size == a.length) {
				a = Arrays.copyOf(a, size * 2);
			}
			a[size++] = v;
		}
	}

	/** Minimal protobuf wire-format cursor */
	private static class Pbf {
		final byte[] buf;
		int pos;
		final int end;

		Pbf(byte[] buf, int pos, int end) {
			this.buf = buf;
			this.pos = pos;
			this.end = end;
		}

		boolean more() {
			return pos < end;
		}

		long varint() {
			long r = 0;
			int shift = 0;
			while (true) {
				byte b = buf[pos++];
				r |= (long) (b & 0x7f) << shift;
				if (b >= 0) {
					return r;
				}
				shift += 7;
			}
		}

		/** repeated uint32, packed or not */
		void readInts(int key, IntList l) {
			if ((key & 7) == 2) {
				int len = (int) varint();
				int e = pos + len;
				while (pos < e) {
					l.add((int) varint());
				}
			} else {
				l.add((int) varint());
			}
		}

		void skip(int key) throws IOException {
			switch (key & 7) {
			case 0:
				varint();
				break;
			case 1:
				pos += 8;
				break;
			case 2:
				int l = (int) varint();
				pos += l;
				break;
			case 5:
				pos += 4;
				break;
			default:
				throw new IOException("Unsupported wire type " + (key & 7));
			}
		}
	}

	private int tagIndex(byte[] buf, int off, int len) {
		for (int t = 0; t < tagBytes.length; t++) {
			byte[] b = tagBytes[t];
			if (len == b.length && Arrays.equals(buf, off, off + len, b, 0, len)) {
				return t;
			}
		}
		return -1;
	}

	// ---------------------------------------------------------------- O5M

	/**
	 * Streaming o5m reader (https://wiki.openstreetmap.org/wiki/O5m), string table handling as in osmconvert.
	 * Only tags are decoded; ids, coordinates and refs are skipped.
	 */
	private class O5mReader {
		private static final int TABLE_SIZE = 15000;
		private static final int MAX_STR = 250;
		private static final int SLOT = MAX_STR + 2;

		private final InputStream in;
		private byte[] buf = new byte[1 << 23];
		private int pos, lim;
		private boolean eof;

		private final byte[] table = new byte[TABLE_SIZE * SLOT];
		private final int[] len1 = new int[TABLE_SIZE];
		private final int[] len2 = new int[TABLE_SIZE];
		private int tableInd;
		private long timestamp;

		// result of the last readString(): bytes in sBuf at s1/s2
		private byte[] sBuf;
		private int s1, l1, s2, l2;

		private final Counts counts = new Counts();
		private final String[] object = new String[tags.length];
		private final boolean perObject;

		O5mReader(InputStream in, boolean withPoi) {
			this.in = in;
			this.perObject = poi != null && withPoi;
		}

		/** make sure n bytes are in buf from pos */
		private boolean ensure(int n) throws IOException {
			if (lim - pos >= n) {
				return true;
			}
			if (n > buf.length) {
				buf = Arrays.copyOf(buf, Math.max(n, buf.length * 2));
			}
			System.arraycopy(buf, pos, buf, 0, lim - pos);
			lim -= pos;
			pos = 0;
			while (lim < n && !eof) {
				int r = in.read(buf, lim, buf.length - lim);
				if (r < 0) {
					eof = true;
				} else {
					lim += r;
				}
			}
			return lim - pos >= n;
		}

		Counts read() throws IOException {
			while (ensure(1)) {
				int type = buf[pos++] & 0xff;
				if (type >= 0xf0) {
					if (type == 0xff) {
						reset();
					} else if (type == 0xfe) {
						return counts;
					}
					continue;
				}
				ensure(10);
				long len = uvarint();
				if (len > Integer.MAX_VALUE || !ensure((int) len)) {
					throw new EOFException("Truncated o5m dataset type " + type);
				}
				int end = pos + (int) len;
				if (type == 0x10) {
					parseObject(end, 0);
				} else if (type == 0x11) {
					parseObject(end, 1);
				} else if (type == 0x12) {
					parseObject(end, 2);
				}
				pos = end;
			}
			throw new EOFException("o5m has no end marker (truncated file?)");
		}

		private void reset() {
			tableInd = 0;
			timestamp = 0;
			Arrays.fill(len1, 0);
			Arrays.fill(len2, 0);
		}

		private void parseObject(int end, int kind) throws IOException {
			counts.objects++;
			svarint(); // id
			if (pos >= end) {
				return;
			}
			long version = uvarint();
			if (version != 0) {
				timestamp += svarint();
				if (timestamp != 0) {
					svarint(); // changeset
					readString(true); // uid, user
				}
			}
			if (pos >= end) {
				return;
			}
			if (kind == 0) {
				svarint(); // lon
				svarint(); // lat
			} else {
				int refsLen = (int) uvarint();
				int refsEnd = pos + refsLen;
				if (kind == 1) {
					pos = refsEnd;
				} else {
					while (pos < refsEnd) {
						svarint(); // member id
						readString(false); // member type + role
					}
				}
			}
			while (pos < end) {
				readString(true);
				int t = tagIndex(sBuf, s1, l1);
				if (t >= 0 && perObject) {
					object[t] = new String(sBuf, s2, l2, StandardCharsets.UTF_8);
				} else if (t >= 0 && t < poiBase) {
					String v = normalize(new String(sBuf, s2, l2, StandardCharsets.UTF_8));
					if (!v.isEmpty()) {
						counts.add(tags[t], v, 1);
					}
				}
			}
			if (perObject) {
				object(counts, object);
			}
		}

		private void readString(boolean pair) {
			if (buf[pos] == 0) {
				pos++;
				int p = pos;
				while (buf[p] != 0) {
					p++;
				}
				sBuf = buf;
				s1 = pos;
				l1 = p - pos;
				pos = p + 1;
				s2 = pos;
				l2 = 0;
				if (pair) {
					p = pos;
					while (buf[p] != 0) {
						p++;
					}
					s2 = pos;
					l2 = p - pos;
					pos = p + 1;
				}
				if (l1 + l2 <= MAX_STR) {
					int slot = tableInd * SLOT;
					System.arraycopy(buf, s1, table, slot, l1);
					System.arraycopy(buf, s2, table, slot + l1, l2);
					len1[tableInd] = l1;
					len2[tableInd] = l2;
					tableInd = (tableInd + 1) % TABLE_SIZE;
				}
			} else {
				int ref = (int) uvarint();
				int ind = ((tableInd - ref) % TABLE_SIZE + TABLE_SIZE) % TABLE_SIZE;
				sBuf = table;
				s1 = ind * SLOT;
				l1 = len1[ind];
				s2 = s1 + l1;
				l2 = len2[ind];
			}
		}

		private long uvarint() {
			long r = 0;
			int shift = 0;
			while (true) {
				byte b = buf[pos++];
				r |= (long) (b & 0x7f) << shift;
				if (b >= 0) {
					return r;
				}
				shift += 7;
			}
		}

		private long svarint() {
			long u = uvarint();
			return (u >>> 1) ^ -(u & 1);
		}
	}
}
