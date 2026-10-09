package net.osmand.server.api.services;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import jakarta.annotation.PreDestroy;
import net.osmand.map.OsmandRegions;
import net.osmand.tester.GenerateTurnLanesTest;
import net.osmand.util.Algorithms;

/**
 * Turn-lane datasets for /admin/turn-lanes: drives through junctions of the selected maps, routed by the code
 * this server is built from, with every instruction given on them. A dataset is a folder - meta.json saying
 * how it was made and how far it got, cases.csv with one row per instruction - so it can be read, copied and
 * diffed without the server.
 */
@Service
public class TurnLanesService {

	private static final Log LOG = LogFactory.getLog(TurnLanesService.class);

	public static final String META = "meta.json";
	public static final String CASES = "cases.csv";
	static final Pattern NAME = Pattern.compile("[A-Za-z0-9._-]{1,100}");

	public enum Status {
		QUEUED, RUNNING, DONE, CANCELLED, FAILED
	}

	/** a class, not a record: it is stored in meta.json, and this Gson cannot read records back */
	public static class GenerateRequest {
		public String name;
		public String labels;
		public String obfDir;
		public String prefixes;
		public Integer junctions;
		public Integer perJunction;
		public String profile;
		public Boolean linksOnly;
		public Boolean anyTurn;
		public Boolean noneLanes;
		public String regionsPath;
		/** the smallest class of road a junction is picked on: motorway, trunk, primary, secondary, tertiary */
		public String highway;
	}

	public record ObfFile(String name, long size) {
	}

	public static class ObfProgress {
		public String name;
		public long size;
		public Status status = Status.QUEUED;
		// drives, named junctions for the admin page
		public int junctionsDone;
		public int junctionsTotal;
		public int routes;
		public int rows;
		public long elapsedMs;
		public String error;
	}

	public static class Dataset {
		public String name;
		public String labels;
		public String obfDir;
		public List<String> prefixes;
		public GenerateRequest params;
		/** the code the instructions come from: this server's build */
		public String build;
		public Status status;
		public String error;
		public long created;
		public long started;
		public long finished;
		public int routes;
		public int rows;
		public List<ObfProgress> obfs = new ArrayList<>();
		volatile transient boolean cancelRequested;
	}

	@Value("${osmand.turn-lanes.location:${osmand.files.location}/turn-lanes}")
	private String location;

	@Value("${tile-server.obf.location:}")
	private String defaultObfDir;

	@Value("${git.commit.format:}")
	private String build;

	/** what is queued or running; the rest is read from disk */
	private final Map<String, Dataset> active = new ConcurrentHashMap<>();
	// one at a time: a whole obf is read into memory as a road graph, two big ones at once could take the heap
	private final ExecutorService executor = Executors.newSingleThreadExecutor(r -> {
		Thread t = new Thread(r, "turn-lanes");
		t.setDaemon(true);
		return t;
	});

	@PreDestroy
	public void shutdown() {
		for (Dataset d : active.values()) {
			d.cancelRequested = true;
		}
		executor.shutdownNow();
	}

	public String getDefaultObfDir() {
		return defaultObfDir;
	}

	@Value("${git.android.format:}")
	private String androidBuild;

	/** what the routing comes from: the android checkout's branch and commit first, then the server's own */
	public String getBuild() {
		String server = Algorithms.isEmpty(build) ? "server" : build;
		return Algorithms.isEmpty(androidBuild) ? server : "android " + androidBuild + " · " + server;
	}

	public File getRoot() {
		return new File(location);
	}

	/** the .obf files of the folder whose name starts with one of the prefixes, all of them when none are given */
	public List<ObfFile> listObfs(String obfDir, String prefixes) {
		if (Algorithms.isEmpty(obfDir)) {
			throw new IllegalArgumentException("OBF folder is not set");
		}
		File dir = new File(obfDir);
		File[] files = dir.listFiles();
		if (files == null) {
			throw new IllegalArgumentException("Not a folder: " + obfDir);
		}
		List<String> ps = parsePrefixes(prefixes);
		List<ObfFile> out = new ArrayList<>();
		for (File f : files) {
			String n = f.getName();
			if (!f.isFile() || !n.endsWith(".obf")) {
				continue;
			}
			String lower = n.toLowerCase(Locale.ROOT);
			if (ps.isEmpty() || ps.stream().anyMatch(lower::startsWith)) {
				out.add(new ObfFile(n, f.length()));
			}
		}
		out.sort(Comparator.comparing(ObfFile::name));
		return out;
	}

	private static List<String> parsePrefixes(String prefixes) {
		List<String> out = new ArrayList<>();
		if (prefixes != null) {
			for (String p : prefixes.split(",")) {
				if (!p.trim().isEmpty()) {
					out.add(p.trim().toLowerCase(Locale.ROOT));
				}
			}
		}
		return out;
	}

	public synchronized Dataset generate(GenerateRequest req) throws IOException {
		String name = req.name == null ? "" : req.name.trim();
		if (!NAME.matcher(name).matches()) {
			throw new IllegalArgumentException("Name: letters, digits, '.', '_' and '-' only");
		}
		File dir = new File(getRoot(), name);
		if (dir.exists() || active.containsKey(name)) {
			throw new IllegalArgumentException("Dataset '" + name + "' already exists");
		}
		if (!Algorithms.isEmpty(req.highway) && !GenerateTurnLanesTest.HIGHWAYS.contains(req.highway.trim())) {
			throw new IllegalArgumentException("Highway level: one of " + GenerateTurnLanesTest.HIGHWAYS);
		}
		List<ObfFile> obfs = listObfs(req.obfDir, req.prefixes);
		if (obfs.isEmpty()) {
			throw new IllegalArgumentException("No .obf in " + req.obfDir + " matches the prefixes");
		}
		Dataset d = new Dataset();
		d.name = name;
		d.obfDir = new File(req.obfDir).getAbsolutePath();
		d.labels = req.labels == null ? "" : req.labels.trim();
		d.prefixes = parsePrefixes(req.prefixes);
		d.params = req;
		d.build = getBuild();
		d.status = Status.QUEUED;
		d.created = System.currentTimeMillis();
		for (ObfFile f : obfs) {
			ObfProgress p = new ObfProgress();
			p.name = f.name();
			p.size = f.size();
			d.obfs.add(p);
		}
		Files.createDirectories(dir.toPath());
		saveMeta(d);
		active.put(name, d);
		executor.submit(() -> run(d));
		return d;
	}

	/** generating and comparing share the one thread: both hold whole maps in memory */
	void submit(Runnable job) {
		executor.submit(job);
	}

	public GenerateTurnLanesTest.Options options(GenerateRequest req) {
		GenerateTurnLanesTest.Options o = new GenerateTurnLanesTest.Options();
		if (req.junctions != null && req.junctions > 0) {
			o.junctions = req.junctions;
		}
		if (req.perJunction != null && req.perJunction > 0) {
			o.perJunction = req.perJunction;
		}
		if (!Algorithms.isEmpty(req.profile)) {
			o.profile = req.profile.trim();
		}
		o.linksOnly = Boolean.TRUE.equals(req.linksOnly);
		o.anyTurn = Boolean.TRUE.equals(req.anyTurn);
		o.noneLanes = Boolean.TRUE.equals(req.noneLanes);
		o.regionsPath = Algorithms.isEmpty(req.regionsPath) ? null : req.regionsPath.trim();
		if (!Algorithms.isEmpty(req.highway)) {
			o.highway = req.highway.trim();
		}
		return o;
	}

	/** regions.ocbf given, or beside the maps, or the one bundled with OsmAnd-java */
	private OsmandRegions regions(Dataset d, GenerateTurnLanesTest.Options o) throws IOException {
		File file = o.regionsPath != null ? new File(o.regionsPath) : new File(d.obfDir, OsmandRegions.REGIONS_OCBF);
		if (file.exists()) {
			return new OsmandRegions(file.getAbsolutePath());
		}
		if (o.regionsPath != null) {
			throw new IOException("No " + file);
		}
		return new OsmandRegions((String) null);
	}

	private void run(Dataset d) {
		d.status = Status.RUNNING;
		d.started = System.currentTimeMillis();
		File dir = new File(getRoot(), d.name);
		File partial = new File(dir, CASES + ".part");
		try {
			saveMeta(d);
			GenerateTurnLanesTest.Options o = options(d.params);
			OsmandRegions regions = regions(d, o);
			int num = 1;
			try (Writer w = Files.newBufferedWriter(partial.toPath(), StandardCharsets.UTF_8)) {
				GenerateTurnLanesTest.writeCsvRow(w, GenerateTurnLanesTest.CSV_HEADER);
				for (ObfProgress p : d.obfs) {
					if (d.cancelRequested) {
						break;
					}
					num = runObf(d, p, o, regions, w, num);
					w.flush();
					saveMeta(d);
				}
			}
			Files.move(partial.toPath(), new File(dir, CASES).toPath(), StandardCopyOption.REPLACE_EXISTING);
			d.status = d.cancelRequested ? Status.CANCELLED : Status.DONE;
		} catch (Throwable e) {
			LOG.error("Turn-lanes dataset " + d.name + " failed", e);
			d.status = Status.FAILED;
			d.error = String.valueOf(e.getMessage());
		} finally {
			d.finished = System.currentTimeMillis();
			try {
				saveMeta(d);
			} catch (IOException e) {
				LOG.error("Cannot save " + META + " of " + d.name, e);
			}
			active.remove(d.name);
		}
	}

	private int runObf(Dataset d, ObfProgress p, GenerateTurnLanesTest.Options o, OsmandRegions regions,
	                   Writer w, int num) {
		long start = System.currentTimeMillis();
		p.status = Status.RUNNING;
		GenerateTurnLanesTest generator = new GenerateTurnLanesTest(o, new GenerateTurnLanesTest.Progress() {
			@Override
			public void drives(int done, int total, int points) {
				p.junctionsDone = done;
				p.junctionsTotal = total;
				p.elapsedMs = System.currentTimeMillis() - start;
			}

			@Override
			public boolean isCancelled() {
				return d.cancelRequested;
			}
		});
		try {
			List<GenerateTurnLanesTest.Case> cases = generator.generate(new File(d.obfDir, p.name), regions);
			int next = GenerateTurnLanesTest.writeCsv(cases, w, num);
			p.routes = cases.size();
			p.rows = cases.stream().mapToInt(c -> c.results.size()).sum();
			d.routes += p.routes;
			d.rows += p.rows;
			p.status = d.cancelRequested ? Status.CANCELLED : Status.DONE;
			return next;
		} catch (Throwable e) {
			// one broken map should not cost the others
			LOG.error("Turn-lanes dataset " + d.name + ": " + p.name + " failed", e);
			p.status = Status.FAILED;
			p.error = String.valueOf(e.getMessage());
			return num;
		} finally {
			p.elapsedMs = System.currentTimeMillis() - start;
		}
	}

	/** only the labels: the name is the folder, and the rest describes how the cases were made */
	public synchronized Dataset updateLabels(String name, String labels) throws IOException {
		Dataset d = getDataset(name);
		if (d == null) {
			return null;
		}
		d.labels = labels == null ? "" : labels.trim();
		saveMeta(d);
		return d;
	}

	public boolean cancel(String name) {
		Dataset d = active.get(name);
		if (d == null) {
			return false;
		}
		d.cancelRequested = true;
		return true;
	}

	public List<Dataset> listDatasets() {
		List<Dataset> out = new ArrayList<>();
		File[] dirs = getRoot().listFiles(File::isDirectory);
		if (dirs != null) {
			for (File dir : dirs) {
				Dataset d = getDataset(dir.getName());
				if (d != null) {
					out.add(d);
				}
			}
		}
		out.sort(Comparator.comparingLong((Dataset d) -> d.created).reversed());
		return out;
	}

	public Dataset getDataset(String name) {
		if (name == null || !NAME.matcher(name).matches()) {
			return null;
		}
		Dataset d = active.get(name);
		if (d != null) {
			return d;
		}
		File meta = new File(new File(getRoot(), name), META);
		if (!meta.exists()) {
			return null;
		}
		try (Reader r = Files.newBufferedReader(meta.toPath(), StandardCharsets.UTF_8)) {
			d = TurnLanesFiles.GSON.fromJson(r, Dataset.class);
			// the server stopped while it was running: nothing is going to finish it now
			if (d.status == Status.QUEUED || d.status == Status.RUNNING) {
				d.status = Status.FAILED;
				d.error = "Interrupted by a server restart";
			}
			return d;
		} catch (IOException | RuntimeException e) {
			LOG.warn("Cannot read " + meta + ": " + e.getMessage());
			return null;
		}
	}

	public File getCasesFile(String name) {
		Dataset d = getDataset(name);
		if (d == null) {
			return null;
		}
		File f = new File(new File(getRoot(), name), CASES);
		return f.exists() ? f : null;
	}

	/** the first {@code limit} rows of cases.csv, as header name -> value */
	public List<Map<String, String>> readRows(String name, int limit) throws IOException {
		return TurnLanesFiles.readCsv(getCasesFile(name), rec -> true, limit);
	}

	public boolean delete(String name) throws IOException {
		if (name == null || !NAME.matcher(name).matches() || active.containsKey(name)) {
			return false;
		}
		File dir = new File(getRoot(), name);
		if (!dir.isDirectory()) {
			return false;
		}
		// a dataset folder holds its compares too
		TurnLanesFiles.deleteTree(dir.toPath());
		return true;
	}

	/**
	 * A drive added by hand, at the end of cases.csv under the next free num: {@code instructions} are its
	 * segment -> expected. Counted in meta.json as one more route.
	 *
	 * @return the drive's num
	 */
	public synchronized int addCase(String dataset, String name, String start, String end, boolean leftSide, String obf,
	                                Map<String, String> instructions) throws IOException {
		Dataset d = getDataset(dataset);
		File cases = getCasesFile(dataset);
		if (d == null || cases == null || active.containsKey(dataset)) {
			throw new IllegalArgumentException("No finished dataset '" + dataset + "' with cases");
		}
		int num = 0;
		for (Map<String, String> row : TurnLanesFiles.readCsv(cases)) {
			num = Math.max(num, Integer.parseInt(row.get("num")));
		}
		num++;
		List<String[]> rows = new ArrayList<>();
		for (Map.Entry<String, String> e : instructions.entrySet()) {
			rows.add(new String[] {String.valueOf(num), name, start, end, e.getKey(), e.getValue(),
					String.valueOf(leftSide), obf, ""});
		}
		TurnLanesFiles.appendCsv(cases, rows);
		d.routes++;
		d.rows += rows.size();
		for (ObfProgress o : d.obfs) {
			if (o.name.equals(obf)) {
				o.routes++;
				o.rows += rows.size();
			}
		}
		saveMeta(d);
		return num;
	}

	private synchronized void saveMeta(Dataset d) throws IOException {
		TurnLanesFiles.write(new File(new File(getRoot(), d.name), META).toPath(), TurnLanesFiles.GSON.toJson(d));
	}
}
