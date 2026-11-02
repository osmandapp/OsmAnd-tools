package net.osmand.server.api.services;

import net.osmand.apkdelta.ApkArchive;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.ResponseStatus;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

/**
 * Deltas between what a phone has and the builds on this server. A request waits for its delta:
 * the first phone after a new build waits for the patches, the next ones find them ready.
 */
@Service
public class BuildDeltaService {

	private static final Log LOGGER = LogFactory.getLog(BuildDeltaService.class);

	private static final Pattern BUILD_PATH = Pattern.compile("(latest-night-build|night-builds)/[A-Za-z0-9._-]+\\.apk");
	private static final Pattern DELTA_KEY = Pattern.compile("[0-9a-f]{40}");
	private static final String DELTA_FOLDER = "apk-delta";
	private static final String ENTRY_PATCHES_FOLDER = "entries";
	private static final String DELTA_EXTENSION = ".delta";

	/** A delta close to the size of the APK is not worth building a new APK on the phone. */
	private static final double MAX_DELTA_RATIO = 0.6;
	private static final int MAX_BUILDS_TO_SEARCH = 120;
	private static final long MAX_FILE_AGE_MS = TimeUnit.DAYS.toMillis(8);
	// a patch of a large entry takes about 0.5 GB of heap
	private static final int PATCH_THREADS = 2;
	private static final int MAX_WAITING_REQUESTS = 4;
	private static final int MAX_BUILDS_TO_SIZE = 4;

	public record Delta(String key, long size, long targetSize) {
	}

	/** {@code exact} is false while the delta is still to be built and its size is an estimate. */
	public record BuildSize(String path, long size, long fullSize, boolean exact) {
	}

	@ResponseStatus(HttpStatus.BAD_REQUEST)
	public static class BrokenManifestException extends RuntimeException {
		BrokenManifestException(IOException cause) {
			super("Broken manifest", cause);
		}
	}

	public static class TooManyRequestsException extends IOException {
		public TooManyRequestsException() {
			super("Too many delta requests");
		}
	}

	@Value("${osmand.files.location}")
	private String filesLocation;

	@Value("${osmand.gen.location}")
	private String genLocation;

	private final ExecutorService executor = Executors.newFixedThreadPool(PATCH_THREADS, runnable -> {
		Thread thread = new Thread(runnable, "apk-delta");
		thread.setDaemon(true);
		return thread;
	});
	private final ThreadPoolExecutor prebuilds = new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS,
			new ArrayBlockingQueue<>(MAX_BUILDS_TO_SIZE), runnable -> {
		Thread thread = new Thread(runnable, "apk-delta-prebuild");
		thread.setDaemon(true);
		return thread;
	}, new ThreadPoolExecutor.DiscardPolicy());
	private final Semaphore building = new Semaphore(1, true);
	private final AtomicInteger waiting = new AtomicInteger();

	/** @return null if the whole APK is the better download */
	public Delta getDelta(String buildPath, InputStream manifest) throws IOException {
		if (buildPath == null || !BUILD_PATH.matcher(buildPath).matches()) {
			throw new IllegalArgumentException("Unknown build");
		}
		File target = new File(filesLocation, buildPath);
		if (!target.isFile()) {
			throw new IllegalArgumentException("Unknown build");
		}
		List<ApkArchive.Entry> entries = readManifest(manifest);
		String key = key(target, entries);
		File delta = deltaFile(key);
		if (!delta.exists()) {
			if (waiting.incrementAndGet() > MAX_WAITING_REQUESTS) {
				waiting.decrementAndGet();
				throw new TooManyRequestsException();
			}
			try {
				buildOnce(target, entries, delta);
			} finally {
				waiting.decrementAndGet();
			}
		}
		if (!isWorthIt(delta.length(), target)) {
			return null;
		}
		return new Delta(key, delta.length(), target.length());
	}

	/**
	 * Answers at once for the newest builds a delta is worth for: the size of a ready delta, or an
	 * estimate while the delta is built in the background for the download that may follow.
	 */
	public List<BuildSize> getSizes(InputStream manifest) throws IOException {
		List<ApkArchive.Entry> entries = readManifest(manifest);
		List<File> builds = builds();
		List<BuildSize> sizes = new ArrayList<>();
		for (File build : builds) {
			if (sizes.size() == MAX_BUILDS_TO_SIZE) {
				break;
			}
			if (build.isFile() && BUILD_PATH.matcher(path(build)).matches()) {
				BuildSize size = getSize(build, entries, builds);
				if (size != null) {
					sizes.add(size);
				}
			}
		}
		return sizes;
	}

	private static List<ApkArchive.Entry> readManifest(InputStream manifest) {
		try {
			return ApkArchive.readManifest(manifest);
		} catch (IOException e) {
			throw new BrokenManifestException(e);
		}
	}

	private BuildSize getSize(File build, List<ApkArchive.Entry> entries, List<File> builds) throws IOException {
		File delta = deltaFile(key(build, entries));
		if (delta.exists()) {
			return isWorthIt(delta.length(), build) ? new BuildSize(path(build), delta.length(), build.length(), true) : null;
		}
		long estimate = ApkDeltaBuilder.estimate(build, entries, ApkDeltaBuilder.findOldEntries(build, entries, builds));
		if (!isWorthIt(estimate, build)) {
			return null;
		}
		prebuilds.execute(() -> {
			try {
				buildOnce(build, entries, delta);
			} catch (IOException e) {
				LOGGER.error("Could not build the APK delta to " + build.getName(), e);
			}
		});
		return new BuildSize(path(build), estimate, build.length(), false);
	}

	private void buildOnce(File target, List<ApkArchive.Entry> entries, File delta) throws IOException {
		try {
			building.acquire();
			try {
				if (!delta.exists()) {
					build(target, entries, delta);
				}
			} finally {
				building.release();
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new IOException(e);
		}
	}

	private static boolean isWorthIt(long size, File target) {
		return size > 0 && size <= target.length() * MAX_DELTA_RATIO;
	}

	private static String path(File build) {
		return build.getParentFile().getName() + "/" + build.getName();
	}

	/** @return null if there is no such delta */
	public File getDeltaFile(String key) {
		if (key == null || !DELTA_KEY.matcher(key).matches()) {
			return null;
		}
		File delta = deltaFile(key);
		return delta.isFile() && delta.length() > 0 ? delta : null;
	}

	private void build(File target, List<ApkArchive.Entry> entries, File delta) throws IOException {
		long start = System.currentTimeMillis();
		File entryPatches = new File(folder(), ENTRY_PATCHES_FOLDER);
		Files.createDirectories(entryPatches.toPath());
		removeOldFiles(folder());
		removeOldFiles(entryPatches);
		Map<String, ApkDeltaBuilder.OldEntry> oldEntries = ApkDeltaBuilder.findOldEntries(target, entries, builds());
		File temp = File.createTempFile(delta.getName(), ".tmp", folder());
		try {
			ApkDeltaBuilder.Result result = new ApkDeltaBuilder(entryPatches, executor)
					.build(target, entries, oldEntries, temp);
			Files.move(temp.toPath(), delta.toPath(), StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
			LOGGER.info(String.format("APK delta to %s: %d of %d bytes, %d copied, %d patched, %d whole in %d ms",
					target.getName(), result.size(), result.targetSize(), result.copied(), result.patched(),
					result.full(), System.currentTimeMillis() - start));
		} finally {
			temp.delete();
		}
	}

	/** The old versions are looked for in the newest builds first. */
	private List<File> builds() {
		List<File> builds = new ArrayList<>();
		for (String folder : new String[] {"night-builds", "latest-night-build"}) {
			File[] files = new File(filesLocation, folder).listFiles();
			if (files != null) {
				builds.addAll(Arrays.asList(files));
			}
		}
		builds.sort(Comparator.comparingLong(File::lastModified).reversed());
		return builds.size() > MAX_BUILDS_TO_SEARCH ? builds.subList(0, MAX_BUILDS_TO_SEARCH) : builds;
	}

	private void removeOldFiles(File folder) {
		File[] files = folder.listFiles(File::isFile);
		if (files != null) {
			long oldest = System.currentTimeMillis() - MAX_FILE_AGE_MS;
			for (File file : files) {
				if (file.lastModified() < oldest) {
					file.delete();
				}
			}
		}
	}

	private File folder() {
		return new File(genLocation, DELTA_FOLDER);
	}

	private File deltaFile(String key) {
		return new File(folder(), key + DELTA_EXTENSION);
	}

	private static String key(File target, List<ApkArchive.Entry> entries) {
		try {
			MessageDigest digest = MessageDigest.getInstance("SHA-1");
			digest.update((target.getName() + ":" + target.length() + ":" + target.lastModified()).getBytes(StandardCharsets.UTF_8));
			TreeSet<String> lines = new TreeSet<>();
			for (ApkArchive.Entry entry : entries) {
				lines.add(entry.contentKey() + ":" + entry.compressedSize + ":" + entry.method);
			}
			for (String line : lines) {
				digest.update(line.getBytes(StandardCharsets.UTF_8));
			}
			return HexFormat.of().formatHex(digest.digest());
		} catch (NoSuchAlgorithmException e) {
			throw new IllegalStateException(e);
		}
	}
}
