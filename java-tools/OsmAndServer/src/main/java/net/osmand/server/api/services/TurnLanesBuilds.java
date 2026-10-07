package net.osmand.server.api.services;

import java.io.File;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * OsmAndMapCreator night builds to replay turn-lanes datasets on: the latest of main and test, and one a day kept
 * in night-builds/. A build is downloaded once and only its lib/ is kept, under {@code <turn-lanes>/.builds/<id>};
 * main and test are fetched again when the server has a newer one. {@link TurnLanesRunner} is run against it.
 */
@Service
public class TurnLanesBuilds {

	private static final Log LOG = LogFactory.getLog(TurnLanesBuilds.class);

	public static final String CURRENT = "current";
	public static final String LATEST_URL = "https://download.osmand.net/latest-night-build/OsmAndMapCreator-%s.zip";
	public static final String DAILY_INDEX = "https://download.osmand.net/night-builds/";
	public static final String DAILY_URL = DAILY_INDEX + "OsmAndMapCreator-%s.zip";
	private static final List<String> LATEST = List.of("main", "test");
	private static final Pattern DATE = Pattern.compile("\\d{4}-\\d{2}-\\d{2}");
	private static final Pattern DAILY_ZIP = Pattern.compile("OsmAndMapCreator-(\\d{4}-\\d{2}-\\d{2})\\.zip");
	private static final String CACHE = ".builds";
	private static final String INFO = "build.json";
	private static final long INDEX_TTL = 60 * 60 * 1000L;

	/** a build in the cache: where it came from and which one of the server's it is */
	public static class Cached {
		public String id;
		public String url;
		public String lastModified;
		public long size;
		public long fetched;
		public long used;
	}

	@Autowired
	private TurnLanesService lanes;

	/** how many downloaded builds to keep; the least recently used go first */
	@Value("${osmand.turn-lanes.builds-kept:5}")
	private int kept;

	@Value("${osmand.turn-lanes.runner-xmx:4g}")
	private String runnerXmx;

	private final Gson gson = new GsonBuilder().setPrettyPrinting().create();
	/**
	 * Fetching a build takes minutes: it holds this, not the service, so the list of builds the page asks for in
	 * the meantime is not kept waiting behind it.
	 */
	private final Object fetching = new Object();
	private List<String> dailyCache;
	private long dailyFetched;

	/** {@code current}, {@code main}, {@code test} or a date of night-builds/; anything else is refused */
	public String normalize(String build) {
		String b = build == null || build.isBlank() ? CURRENT : build.trim();
		if (b.equals(CURRENT) || LATEST.contains(b) || DATE.matcher(b).matches()) {
			return b;
		}
		throw new IllegalArgumentException("Unknown build '" + b + "': current, main, test or yyyy-MM-dd");
	}

	public String url(String build) {
		return LATEST.contains(build) ? String.format(LATEST_URL, build) : String.format(DAILY_URL, build);
	}

	/** the dates night-builds/ has, newest first; read again once an hour */
	public synchronized List<String> dailyBuilds() throws IOException {
		if (dailyCache == null || System.currentTimeMillis() - dailyFetched > INDEX_TTL) {
			HttpURLConnection c = open(DAILY_INDEX, "GET");
			String html;
			try (InputStream in = c.getInputStream()) {
				html = new String(in.readAllBytes(), StandardCharsets.UTF_8);
			}
			TreeSet<String> dates = new TreeSet<>(Collections.reverseOrder());
			Matcher m = DAILY_ZIP.matcher(html);
			while (m.find()) {
				dates.add(m.group(1));
			}
			dailyCache = new ArrayList<>(dates);
			dailyFetched = System.currentTimeMillis();
		}
		return dailyCache;
	}

	public List<Cached> cached() {
		List<Cached> out = new ArrayList<>();
		File[] dirs = cacheDir().listFiles(File::isDirectory);
		for (File d : dirs == null ? new File[0] : dirs) {
			Cached c = readInfo(d);
			if (c != null) {
				out.add(c);
			}
		}
		out.sort(Comparator.comparingLong((Cached c) -> c.used).reversed());
		return out;
	}

	private File cacheDir() {
		return new File(lanes.getRoot(), CACHE);
	}

	private Cached readInfo(File dir) {
		File f = new File(dir, INFO);
		if (!f.exists() || !new File(dir, "lib").isDirectory()) {
			return null;
		}
		try (Reader r = Files.newBufferedReader(f.toPath(), StandardCharsets.UTF_8)) {
			return gson.fromJson(r, Cached.class);
		} catch (IOException | RuntimeException e) {
			return null;
		}
	}

	private static HttpURLConnection open(String url, String method) throws IOException {
		HttpURLConnection c = (HttpURLConnection) URI.create(url).toURL().openConnection();
		c.setRequestMethod(method);
		c.setConnectTimeout(30_000);
		c.setReadTimeout(120_000);
		int code = c.getResponseCode();
		if (code != 200) {
			c.disconnect();
			throw new IOException(url + ": HTTP " + code);
		}
		return c;
	}

	/**
	 * The build's lib/, downloaded when it is not in the cache yet - or, for main and test, when the server has
	 * a newer one than the cache.
	 *
	 * @param phase told what is going on, for the page to show
	 */
	public Cached prepare(String build, Consumer<String> phase, BooleanSupplier cancelled) throws IOException {
		synchronized (fetching) {
			return fetch(build, phase, cancelled);
		}
	}

	private Cached fetch(String build, Consumer<String> phase, BooleanSupplier cancelled) throws IOException {
		File dir = new File(cacheDir(), build);
		Cached have = readInfo(dir);
		String url = url(build);
		boolean latest = LATEST.contains(build);
		if (have != null && !latest) {
			return touch(dir, have);
		}
		phase.accept("Checking " + url);
		HttpURLConnection head = open(url, "HEAD");
		String lastModified = head.getHeaderField("Last-Modified");
		long size = head.getContentLengthLong();
		head.disconnect();
		if (have != null && lastModified != null && lastModified.equals(have.lastModified)) {
			return touch(dir, have);
		}
		Files.createDirectories(cacheDir().toPath());
		File tmp = new File(cacheDir(), build + ".part");
		TurnLanesFiles.deleteTree(tmp.toPath());
		Files.createDirectories(new File(tmp, "lib").toPath());
		HttpURLConnection get = open(url, "GET");
		long[] read = {0};
		int jars = 0;
		// straight from the stream: lib/ is all that is wanted, the zip itself is not kept
		try (ZipInputStream zip = new ZipInputStream(new CountingStream(get.getInputStream(), n -> read[0] += n))) {
			ZipEntry e;
			byte[] buf = new byte[1 << 16];
			while ((e = zip.getNextEntry()) != null) {
				if (cancelled.getAsBoolean()) {
					throw new IOException("Cancelled while downloading " + build);
				}
				String name = e.getName();
				if (name.startsWith("./")) {
					name = name.substring(2);
				}
				if (e.isDirectory() || !name.startsWith("lib/") || !name.endsWith(".jar") || name.indexOf('/', 4) >= 0) {
					continue;
				}
				Path to = new File(tmp, name).toPath();
				try (OutputStream out = Files.newOutputStream(to)) {
					for (int n; (n = zip.read(buf)) > 0; ) {
						out.write(buf, 0, n);
					}
				}
				jars++;
				phase.accept("Downloading " + url + (size > 0 ? " " + Math.min(99, read[0] * 100 / size) + "%" : "")
						+ " (" + jars + " jars)");
			}
		} finally {
			get.disconnect();
		}
		if (jars == 0) {
			throw new IOException("No lib/*.jar in " + url);
		}
		Cached c = new Cached();
		c.id = build;
		c.url = url;
		c.lastModified = lastModified;
		c.size = size;
		c.fetched = System.currentTimeMillis();
		c.used = c.fetched;
		Files.writeString(new File(tmp, INFO).toPath(), gson.toJson(c), StandardCharsets.UTF_8);
		TurnLanesFiles.deleteTree(dir.toPath());
		Files.move(tmp.toPath(), dir.toPath(), StandardCopyOption.ATOMIC_MOVE);
		LOG.info("Turn-lanes build " + build + " (" + lastModified + "): " + jars + " jars");
		evict(build);
		return c;
	}

	private Cached touch(File dir, Cached c) throws IOException {
		c.used = System.currentTimeMillis();
		TurnLanesFiles.write(new File(dir, INFO).toPath(), gson.toJson(c));
		return c;
	}

	/** the least recently used builds over {@link #kept}, never the one just fetched */
	private void evict(String keep) {
		List<Cached> all = cached();
		for (int i = Math.max(1, kept); i < all.size(); i++) {
			if (!all.get(i).id.equals(keep)) {
				try {
					TurnLanesFiles.deleteTree(new File(cacheDir(), all.get(i).id).toPath());
				} catch (IOException e) {
					LOG.warn("Cannot remove build " + all.get(i).id + ": " + e.getMessage());
				}
			}
		}
	}

	/** what to show as the build's version: the date it was made */
	public static String version(Cached c) {
		return c.id + (c.lastModified == null || DATE.matcher(c.id).matches() ? "" : " · " + c.lastModified);
	}

	/**
	 * A JVM with {@link TurnLanesRunner} on the build's lib/. Its log goes to {@code log}, so that a full stderr
	 * never blocks it.
	 */
	public Process startRunner(String build, String profile, File log) throws IOException {
		File runnerDir = new File(cacheDir(), "runner");
		String resource = TurnLanesRunner.class.getName().replace('.', '/') + ".class";
		File classFile = new File(runnerDir, resource);
		Files.createDirectories(classFile.getParentFile().toPath());
		// written every time: it is the server's, and the server may have been rebuilt since
		try (InputStream in = TurnLanesRunner.class.getClassLoader().getResourceAsStream(resource)) {
			if (in == null) {
				throw new IOException("No " + resource + " on the server's classpath");
			}
			Files.copy(in, classFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
		}
		String java = ProcessHandle.current().info().command()
				.orElse(System.getProperty("java.home") + File.separator + "bin" + File.separator + "java");
		String cp = runnerDir.getAbsolutePath() + File.pathSeparator
				+ new File(new File(cacheDir(), build), "lib").getAbsolutePath() + File.separator + "*";
		ProcessBuilder pb = new ProcessBuilder(java, "-Xmx" + runnerXmx, "-Djava.awt.headless=true", "-cp", cp,
				TurnLanesRunner.class.getName(), profile);
		pb.redirectError(ProcessBuilder.Redirect.appendTo(log));
		return pb.start();
	}

	/** counts what passes through, so the download can say how far it got */
	private static class CountingStream extends FilterInputStream {
		private final LongConsumer onRead;

		CountingStream(InputStream in, LongConsumer onRead) {
			super(in);
			this.onRead = onRead;
		}

		@Override
		public int read(byte[] b, int off, int len) throws IOException {
			int n = super.read(b, off, len);
			if (n > 0) {
				onRead.accept(n);
			}
			return n;
		}
	}
}
