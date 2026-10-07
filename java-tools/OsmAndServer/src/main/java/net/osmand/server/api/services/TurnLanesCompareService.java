package net.osmand.server.api.services;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

import org.apache.commons.csv.CSVRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import jakarta.annotation.PreDestroy;
import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.server.api.services.TurnLanesService.Dataset;
import net.osmand.server.api.services.TurnLanesService.Status;
import net.osmand.tester.GenerateTurnLanesTest;
import net.osmand.util.Algorithms;

/**
 * Replays a turn-lanes dataset: every drive of it routed again, and every instruction it recorded set against
 * what is said there now. A compare is a folder under the dataset's - compares/&lt;id&gt;/{meta.json,result.csv},
 * result.csv being cases.csv with what the checker said and how that matches.
 */
@Service
public class TurnLanesCompareService {

	private static final Log LOG = LogFactory.getLog(TurnLanesCompareService.class);

	public static final String COMPARES = "compares";
	public static final String RESULT = "result.csv";
	public static final String RUNNER_LOG = "runner.log";
	public static final String REFERENCE_EXPECTED = "expected";
	public static final String[] RESULT_HEADER = {"num", "name", "start", "end", "segment", "expected", "actual", "status",
			"note", "left_side", "obf", "point"};
	private static final int SAVE_EVERY = 50;

	/**
	 * How an instruction fares: SAME, DIFF - another instruction there, none any more (actual empty), or one the
	 * dataset did not have (expected empty) - or ERROR, the drive not routed or not taking that road at all.
	 * SIMILAR is to come, for differences that do not matter.
	 */
	public enum Verdict {
		SAME, DIFF, ERROR
	}

	public static class CompareRequest {
		public String dataset;
		public String labels;
		public String build;
	}

	public static class Compare {
		public String id;
		public String dataset;
		public String labels;
		/** which checker: {@link TurnLanesBuilds#CURRENT}, main, test or a night-builds/ date */
		public String build;
		/** what exactly it was: the server's git commit, or the night build's Last-Modified */
		public String buildVersion;
		/** what the checker is held to: {@link #REFERENCE_EXPECTED}, the generator's instructions */
		public String reference;
		public Status status;
		public String error;
		public long created;
		public long started;
		public long finished;
		public int routesTotal;
		public int routesDone;
		public int rows;
		public int same;
		public int diff;
		public int errors;
		/** what is going on before the drives: fetching the build, starting it */
		public String phase;
		/** manual check: verdict -> how many rows have it */
		public Map<String, Integer> reviewed;
		volatile transient boolean cancelRequested;
		volatile transient Process process;
	}

	/** one drive of the dataset, with its rows in the order they were written */
	private static class Drive {
		final String num;
		final List<CSVRecord> rows = new ArrayList<>();

		Drive(String num) {
			this.num = num;
		}

		CSVRecord first() {
			return rows.get(0);
		}
	}

	@Autowired
	private TurnLanesService lanes;

	@Autowired
	private TurnLanesBuilds builds;

	private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();
	/** dataset/id -> what is queued or running */
	private final Map<String, Compare> active = new ConcurrentHashMap<>();

	@PreDestroy
	public void shutdown() {
		for (Compare c : active.values()) {
			c.cancelRequested = true;
			if (c.process != null) {
				c.process.destroyForcibly();
			}
		}
	}

	private static String key(String dataset, String id) {
		return dataset + "/" + id;
	}

	private File dir(String dataset, String id) {
		return new File(new File(new File(lanes.getRoot(), dataset), COMPARES), id);
	}

	public boolean isActive(String dataset) {
		return active.values().stream().anyMatch(c -> c.dataset.equals(dataset));
	}

	public synchronized Compare start(CompareRequest req) throws IOException {
		Dataset d = lanes.getDataset(req.dataset);
		if (d == null || lanes.getCasesFile(req.dataset) == null) {
			throw new IllegalArgumentException("No dataset '" + req.dataset + "' with cases");
		}
		if (d.status == Status.QUEUED || d.status == Status.RUNNING) {
			throw new IllegalArgumentException("Dataset '" + d.name + "' is still being generated");
		}
		String build = builds.normalize(req.build);
		Compare c = new Compare();
		c.created = System.currentTimeMillis();
		c.id = build + "_" + new SimpleDateFormat("yyyyMMdd-HHmmss").format(new Date(c.created));
		c.dataset = d.name;
		c.labels = req.labels == null ? "" : req.labels.trim();
		c.build = build;
		// a night build's is known once it is fetched
		c.buildVersion = TurnLanesBuilds.CURRENT.equals(build) ? lanes.getBuild() : build;
		c.reference = REFERENCE_EXPECTED;
		c.status = Status.QUEUED;
		File dir = dir(c.dataset, c.id);
		if (dir.exists() || active.containsKey(key(c.dataset, c.id))) {
			throw new IllegalArgumentException("Compare " + c.id + " already exists, try again in a second");
		}
		Files.createDirectories(dir.toPath());
		saveMeta(c);
		active.put(key(c.dataset, c.id), c);
		lanes.submit(() -> run(c, d));
		return c;
	}

	/** a drive the checker could not route: its rows are ERROR, the rest of the compare goes on */
	private static class DriveError extends Exception {
		DriveError(String message) {
			super(message);
		}
	}

	/** what a build says on a drive: the instructions, and the roads it takes by osm id */
	private record Replayed(Map<String, String> instructions, Set<String> roads) {
	}

	/** what a build says on a drive: this server's code, or a night build's in a JVM of its own */
	private interface Checker extends AutoCloseable {
		Replayed route(String num, String obf, String start, String end, String leftSide)
				throws IOException, DriveError;

		default Replayed route(Drive drive) throws IOException, DriveError {
			CSVRecord first = drive.first();
			return route(drive.num, first.get("obf"), first.get("start"), first.get("end"), first.get("left_side"));
		}

		@Override
		void close() throws IOException;
	}

	private void run(Compare c, Dataset d) {
		c.status = Status.RUNNING;
		c.started = System.currentTimeMillis();
		File dir = dir(c.dataset, c.id);
		File partial = new File(dir, RESULT + ".part");
		try {
			List<Drive> drives = readDrives(lanes.getCasesFile(c.dataset));
			c.routesTotal = drives.size();
			saveMeta(c);
			try (Checker checker = checker(c, d, dir);
			     Writer w = Files.newBufferedWriter(partial.toPath(), StandardCharsets.UTF_8)) {
				c.phase = null;
				saveMeta(c);
				GenerateTurnLanesTest.writeCsvRow(w, RESULT_HEADER);
				for (Drive drive : drives) {
					if (c.cancelRequested) {
						break;
					}
					Replayed actual = null;
					String error = null;
					try {
						actual = checker.route(drive);
					} catch (DriveError e) {
						error = e.getMessage();
					}
					if (c.cancelRequested) {
						break; // the runner killed under the drive: not the drive's fault
					}
					compare(c, drive, actual, error, w);
					c.routesDone++;
					if (c.routesDone % SAVE_EVERY == 0) {
						w.flush();
						saveMeta(c);
					}
				}
			}
			Files.move(partial.toPath(), new File(dir, RESULT).toPath(), StandardCopyOption.REPLACE_EXISTING);
			c.status = c.cancelRequested ? Status.CANCELLED : Status.DONE;
		} catch (Throwable e) {
			LOG.error("Turn-lanes compare " + key(c.dataset, c.id) + " failed", e);
			c.status = c.cancelRequested ? Status.CANCELLED : Status.FAILED;
			c.error = String.valueOf(e.getMessage());
		} finally {
			c.phase = null;
			c.finished = System.currentTimeMillis();
			try {
				saveMeta(c);
			} catch (IOException e) {
				LOG.error("Cannot save meta of compare " + key(c.dataset, c.id), e);
			}
			active.remove(key(c.dataset, c.id));
		}
	}

	/**
	 * One drive routed again by the compare's build, between points a person may have moved: what a test case of
	 * it would expect.
	 */
	public Map<String, String> routeOnce(String dataset, String id, String obf, String start, String end,
	                                     String leftSide) throws IOException {
		Compare c = get(dataset, id);
		Dataset d = lanes.getDataset(dataset);
		if (c == null || d == null) {
			throw new IllegalArgumentException("No compare " + dataset + "/" + id);
		}
		try (Checker checker = openChecker(c.build, d, new File(dir(dataset, id), RUNNER_LOG), phase -> {
		}, () -> false)) {
			return checker.route("1", obf, start, end, leftSide).instructions();
		} catch (DriveError e) {
			throw new IllegalArgumentException(e.getMessage());
		}
	}

	/** the compare's checker, its build fetched first when it is a night build, the page told how that goes */
	private Checker checker(Compare c, Dataset d, File dir) throws IOException {
		Checker checker = openChecker(c.build, d, new File(dir, RUNNER_LOG), phase -> {
			c.phase = phase;
		}, () -> c.cancelRequested);
		if (checker instanceof RunnerChecker runner) {
			c.buildVersion = runner.version;
			c.process = runner.process;
		}
		return checker;
	}

	private Checker openChecker(String build, Dataset d, File log, Consumer<String> phase, BooleanSupplier cancelled)
			throws IOException {
		GenerateTurnLanesTest.Options options = lanes.options(d.params);
		if (TurnLanesBuilds.CURRENT.equals(build)) {
			return new ServerChecker(new GenerateTurnLanesTest(options, null), d.obfDir);
		}
		TurnLanesBuilds.Cached cached = builds.prepare(build, phase, cancelled);
		phase.accept("Starting " + build);
		return new RunnerChecker(builds.startRunner(build, options.profile, log), TurnLanesBuilds.version(cached),
				d.obfDir, log);
	}

	/** this server's routing, in this JVM */
	private static class ServerChecker implements Checker {
		private final GenerateTurnLanesTest router;
		private final String obfDir;
		private BinaryMapIndexReader reader;
		private String readerObf;

		ServerChecker(GenerateTurnLanesTest router, String obfDir) {
			this.router = router;
			this.obfDir = obfDir;
		}

		@Override
		public Replayed route(String num, String obf, String start, String end, String leftSide)
				throws IOException, DriveError {
			// the cases are written map by map, so one reader at a time is enough
			if (!obf.equals(readerObf)) {
				close();
				readerObf = obf;
				File file = new File(obfDir, obf);
				if (file.exists()) {
					reader = new BinaryMapIndexReader(new RandomAccessFile(file, "r"), file);
				}
			}
			if (reader == null) {
				throw new DriveError("No map " + new File(obfDir, obf));
			}
			try {
				GenerateTurnLanesTest.Replay r = router.replay(new BinaryMapIndexReader[] {reader}, TurnLanesFiles.latLon(start),
						TurnLanesFiles.latLon(end), Boolean.parseBoolean(leftSide));
				if (r == null) {
					throw new DriveError("No route");
				}
				Set<String> roads = new HashSet<>();
				for (long id : r.roads) {
					roads.add(String.valueOf(id));
				}
				return new Replayed(r.instructions, roads);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new IOException("Interrupted", e);
			} catch (RuntimeException e) {
				throw new DriveError(e.getClass().getSimpleName() + ": " + e.getMessage());
			}
		}

		@Override
		public void close() throws IOException {
			if (reader != null) {
				reader.close();
				reader = null;
			}
		}
	}

	/** a night build: {@link TurnLanesRunner} in its own JVM, a drive per line both ways */
	private static class RunnerChecker implements Checker {
		private final Process process;
		/** which build it is, for the compare to say */
		private final String version;
		private final String obfDir;
		private final File log;
		private final Writer in;
		private final BufferedReader out;

		RunnerChecker(Process process, String version, String obfDir, File log) {
			this.process = process;
			this.version = version;
			this.obfDir = obfDir;
			this.log = log;
			this.in = new OutputStreamWriter(process.getOutputStream(), StandardCharsets.UTF_8);
			this.out = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8));
		}

		@Override
		public Replayed route(String num, String obfName, String start, String end, String leftSide)
				throws IOException, DriveError {
			File obf = new File(obfDir, obfName);
			if (!obf.exists()) {
				throw new DriveError("No map " + obf);
			}
			in.write(num + "\t" + obf.getAbsolutePath() + "\t" + start + "\t" + end + "\t" + leftSide + "\n");
			in.flush();
			String line = out.readLine();
			if (line == null) {
				throw new IOException("The runner stopped: " + tail(log));
			}
			RunnerAnswer a = GSON.fromJson(line, RunnerAnswer.class);
			if (!num.equals(a.num)) {
				throw new IOException("The runner answered " + a.num + " to " + num);
			}
			if (a.error != null) {
				throw new DriveError(a.error);
			}
			return new Replayed(a.results, a.roads == null ? Set.of() : new HashSet<>(a.roads));
		}

		@Override
		public void close() {
			try {
				in.close();
				if (!process.waitFor(10, TimeUnit.SECONDS)) {
					process.destroyForcibly();
				}
			} catch (IOException | InterruptedException e) {
				process.destroyForcibly();
			}
		}
	}

	private static class RunnerAnswer {
		String num;
		LinkedHashMap<String, String> results;
		List<String> roads;
		String error;
	}

	/** the end of the runner's log: why it stopped */
	private static String tail(File log) {
		try {
			List<String> lines = Files.readAllLines(log.toPath(), StandardCharsets.UTF_8);
			return String.join("\n", lines.subList(Math.max(0, lines.size() - 5), lines.size()));
		} catch (IOException e) {
			return "see " + log;
		}
	}

	private static List<Drive> readDrives(File cases) throws IOException {
		List<Drive> out = new ArrayList<>();
		Drive cur = null;
		for (CSVRecord rec : TurnLanesFiles.readRecords(cases)) {
			String num = rec.get("num");
			if (cur == null || !cur.num.equals(num)) {
				cur = new Drive(num);
				out.add(cur);
			}
			cur.rows.add(rec);
		}
		return out;
	}

	/**
	 * A row per instruction the drive recorded, against what the checker said ({@code actual}, or {@code error}),
	 * then one per instruction the checker gave that the drive had not recorded.
	 */
	private void compare(Compare c, Drive drive, Replayed actual, String error, Writer w) throws IOException {
		Set<String> matched = new HashSet<>();
		for (CSVRecord rec : drive.rows) {
			String segment = rec.get("segment");
			String expected = rec.get("expected");
			String note = "";
			String got = null;
			Verdict v;
			if (actual == null) {
				v = Verdict.ERROR;
				note = error;
			} else {
				String found = findSegment(actual.instructions(), segment);
				if (found != null) {
					matched.add(found);
					got = actual.instructions().get(found);
					v = expected.equals(got) ? Verdict.SAME : Verdict.DIFF;
					if (!found.equals(segment)) {
						note = "as " + found;
					}
				} else if (actual.roads().contains(idOf(segment))) {
					// the drive still takes the road, and nothing is said on it now
					got = "";
					v = Verdict.DIFF;
					note = "no instruction";
				} else {
					v = Verdict.ERROR;
					note = "the route does not take way " + idOf(segment);
				}
			}
			count(c, v);
			// where the instruction is, for the manual check; datasets made before it was recorded have none
			String point = rec.isMapped("point") ? rec.get("point") : "";
			GenerateTurnLanesTest.writeCsvRow(w, rec.get("num"), rec.get("name"), rec.get("start"), rec.get("end"),
					segment, expected, got, v.name(), note, rec.get("left_side"), rec.get("obf"), point);
		}
		if (actual == null) {
			return;
		}
		CSVRecord first = drive.first();
		for (Map.Entry<String, String> e : actual.instructions().entrySet()) {
			if (matched.contains(e.getKey())) {
				continue;
			}
			count(c, Verdict.DIFF);
			GenerateTurnLanesTest.writeCsvRow(w, first.get("num"), first.get("name"), first.get("start"),
					first.get("end"), e.getKey(), "", e.getValue(), Verdict.DIFF.name(), "new instruction",
					first.get("left_side"), first.get("obf"), "");
		}
	}

	/**
	 * The key the checker gave the instruction the dataset has under {@code segment}. The same as the dataset's, or -
	 * a road is keyed "id:point" only once it carries two instructions - the one instruction left on that road
	 * whichever way either side keyed it.
	 */
	static String findSegment(Map<String, String> actual, String segment) {
		if (actual.containsKey(segment)) {
			return segment;
		}
		String id = idOf(segment);
		String found = null;
		for (String k : actual.keySet()) {
			if (idOf(k).equals(id)) {
				if (found != null) {
					return null; // two on that road and neither is the one asked for
				}
				found = k;
			}
		}
		// "id:point" in the dataset and the road now carrying it at another point is a different instruction
		return found != null && (segment.indexOf(':') < 0 || found.indexOf(':') < 0) ? found : null;
	}

	private static String idOf(String segment) {
		int i = segment.indexOf(':');
		return i < 0 ? segment : segment.substring(0, i);
	}

	private static void count(Compare c, Verdict v) {
		c.rows++;
		switch (v) {
			case SAME -> c.same++;
			case DIFF -> c.diff++;
			case ERROR -> c.errors++;
		}
	}

	public boolean cancel(String dataset, String id) {
		Compare c = active.get(key(dataset, id));
		if (c == null) {
			return false;
		}
		c.cancelRequested = true;
		Process p = c.process;
		if (p != null) {
			p.destroy(); // a drive can take a while, and the answer is not wanted any more
		}
		return true;
	}

	/** of one dataset, or of all of them when none is given; the newest first */
	public List<Compare> list(String dataset) {
		List<Compare> out = new ArrayList<>();
		List<String> names = new ArrayList<>();
		if (!Algorithms.isEmpty(dataset)) {
			names.add(dataset);
		} else {
			File[] dirs = lanes.getRoot().listFiles(File::isDirectory);
			for (File f : dirs == null ? new File[0] : dirs) {
				names.add(f.getName());
			}
		}
		for (String name : names) {
			if (!TurnLanesService.NAME.matcher(name).matches()) {
				continue;
			}
			File[] dirs = new File(new File(lanes.getRoot(), name), COMPARES).listFiles(File::isDirectory);
			for (File f : dirs == null ? new File[0] : dirs) {
				Compare c = get(name, f.getName());
				if (c != null) {
					out.add(c);
				}
			}
		}
		out.sort(Comparator.comparingLong((Compare c) -> c.created).reversed());
		return out;
	}

	public Compare get(String dataset, String id) {
		if (dataset == null || id == null || !TurnLanesService.NAME.matcher(dataset).matches()
				|| !TurnLanesService.NAME.matcher(id).matches()) {
			return null;
		}
		Compare c = active.get(key(dataset, id));
		if (c != null) {
			return c;
		}
		File meta = new File(dir(dataset, id), TurnLanesService.META);
		if (!meta.exists()) {
			return null;
		}
		try (Reader r = Files.newBufferedReader(meta.toPath(), StandardCharsets.UTF_8)) {
			c = GSON.fromJson(r, Compare.class);
			if (c.status == Status.QUEUED || c.status == Status.RUNNING) {
				c.status = Status.FAILED;
				c.error = "Interrupted by a server restart";
			}
			return c;
		} catch (IOException | RuntimeException e) {
			LOG.warn("Cannot read " + meta + ": " + e.getMessage());
			return null;
		}
	}

	public synchronized Compare updateLabels(String dataset, String id, String labels) throws IOException {
		Compare c = get(dataset, id);
		if (c == null) {
			return null;
		}
		c.labels = labels == null ? "" : labels.trim();
		saveMeta(c);
		return c;
	}

	/** the counts of the manual check, kept in meta.json so the list does not read every review */
	public synchronized void setReviewed(String dataset, String id, Map<String, Integer> counts) throws IOException {
		Compare c = get(dataset, id);
		if (c != null && !active.containsKey(key(dataset, id))) {
			c.reviewed = counts;
			saveMeta(c);
		}
	}

	public File getResultFile(String dataset, String id) {
		if (get(dataset, id) == null) {
			return null;
		}
		File f = new File(dir(dataset, id), RESULT);
		return f.exists() ? f : null;
	}

	/** rows of result.csv with one of the verdicts asked for (all when none), at most {@code limit} */
	public List<Map<String, String>> readRows(String dataset, String id, Set<String> verdicts, int limit)
			throws IOException {
		return TurnLanesFiles.readCsv(getResultFile(dataset, id),
				rec -> verdicts.isEmpty() || verdicts.contains(rec.get("status")), limit);
	}

	public boolean delete(String dataset, String id) throws IOException {
		if (get(dataset, id) == null || active.containsKey(key(dataset, id))) {
			return false;
		}
		TurnLanesFiles.deleteTree(dir(dataset, id).toPath());
		return true;
	}

	private synchronized void saveMeta(Compare c) throws IOException {
		TurnLanesFiles.write(new File(dir(c.dataset, c.id), TurnLanesService.META).toPath(), GSON.toJson(c));
	}
}
