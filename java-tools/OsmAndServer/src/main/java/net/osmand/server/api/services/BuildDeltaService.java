package net.osmand.server.api.services;

import net.osmand.apkdelta.ApkArchive;
import net.osmand.apkdelta.ApkDeltaFormat;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

/**
 * Deltas between what a phone has and the builds on this server. A patch of a large entry takes
 * about 0.5 GB of heap, so deltas are built one at a time on one thread, and the server refuses
 * what does not fit in a short queue. The phone then takes the whole APK.
 */
@Service
public class BuildDeltaService {

	private static final Log LOGGER = LogFactory.getLog(BuildDeltaService.class);

	private static final String NIGHT_BUILDS = "night-builds";
	private static final String LATEST_BUILDS = "latest-night-build";
	private static final String BUILD_EXTENSION = ".apk";
	private static final Pattern BUILD_PATH = Pattern.compile("(" + LATEST_BUILDS + "|" + NIGHT_BUILDS + ")/[A-Za-z0-9._-]+\\.apk");
	private static final String DELTA_FOLDER = "apk-delta";
	private static final String ENTRY_PATCHES_FOLDER = "entries";

	/** A real manifest takes less than 1 MB. */
	private static final int MAX_MANIFEST_SIZE = 2 * 1024 * 1024;
	/** A delta close to the size of the APK is not worth building a new APK on the phone. */
	private static final double MAX_DELTA_RATIO = 0.6;
	private static final int MAX_BUILDS_TO_SEARCH = 60;
	private static final int MAX_BUILDS_TO_SIZE = 4;
	private static final int MAX_QUEUED_DELTAS = 4;
	private static final int MAX_REQUESTS = 4;
	/** Just below the time the client waits. */
	private static final int MAX_BUILD_WAIT_SECONDS = 110;
	private static final long MAX_CACHE_AGE_MS = TimeUnit.DAYS.toMillis(8);
	private static final long MAX_CACHE_SIZE = 2L * 1024 * 1024 * 1024;

	/** {@code exact} is false while the delta is still to be built and its size is an estimate. */
	public record BuildSize(String path, long size, boolean exact) {
	}

	@Value("${osmand.files.location}")
	private String filesLocation;

	@Value("${osmand.gen.location}")
	private String genLocation;

	private final ThreadPoolExecutor deltaBuilds = new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS,
			new ArrayBlockingQueue<>(MAX_QUEUED_DELTAS), runnable -> {
		Thread thread = new Thread(runnable, "apk-delta");
		thread.setDaemon(true);
		return thread;
	});
	private final Map<File, Future<?>> runningBuilds = new HashMap<>();
	private final Semaphore requests = new Semaphore(MAX_REQUESTS);

	/** Patches need the builds of the previous days, latest-night-build holds only the new ones. */
	public boolean isSupported() {
		File[] builds = new File(filesLocation, NIGHT_BUILDS).listFiles(BuildDeltaService::isBuild);
		return builds != null && builds.length > 0;
	}

	/** @return null if the whole APK is the better download */
	public File getDelta(String path, InputStream manifestStream) throws IOException {
		File target = new File(filesLocation, path);
		if (!isBuildPath(path) || !target.isFile()) {
			throw new ResponseStatusException(HttpStatus.NOT_FOUND);
		}
		File delta;
		Future<?> build = null;
		acquireRequest();
		try {
			Map<String, ApkArchive.Entry> manifest = readManifest(manifestStream);
			delta = deltaFile(target, manifest);
			if (!delta.exists()) {
				ApkDeltaBuilder builder = new ApkDeltaBuilder(target, manifest, findOldBuilds(manifest));
				if (!isWorthIt(builder.estimate(), target)) {
					return null;
				}
				build = buildOnce(builder, target, delta);
			}
		} catch (RejectedExecutionException e) {
			throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE);
		} finally {
			requests.release();
		}
		if (build != null) {
			waitFor(build);
		}
		return isWorthIt(delta.length(), target) ? delta : null;
	}

	/** Answers at once: the size of a ready delta, or an estimate while it is built in the background. */
	public List<BuildSize> getSizes(List<String> paths, InputStream manifestStream) throws IOException {
		acquireRequest();
		try {
			Map<String, ApkArchive.Entry> manifest = readManifest(manifestStream);
			List<String> known = paths.stream().limit(MAX_BUILDS_TO_SIZE)
					.filter(path -> isBuildPath(path) && new File(filesLocation, path).isFile()).toList();
			boolean ready = known.stream().allMatch(path -> deltaFile(new File(filesLocation, path), manifest).exists());
			Map<String, File> oldBuilds = ready ? Map.of() : findOldBuilds(manifest);
			List<BuildSize> sizes = new ArrayList<>();
			for (String path : known) {
				try {
					BuildSize size = getSize(path, manifest, oldBuilds);
					if (size != null) {
						sizes.add(size);
					}
				} catch (IOException e) {
					LOGGER.warn("Skipping the unreadable build " + path + ": " + e.getMessage());
				}
			}
			return sizes;
		} finally {
			requests.release();
		}
	}

	private BuildSize getSize(String path, Map<String, ApkArchive.Entry> manifest, Map<String, File> oldBuilds)
			throws IOException {
		File target = new File(filesLocation, path);
		File delta = deltaFile(target, manifest);
		if (delta.exists()) {
			return isWorthIt(delta.length(), target) ? new BuildSize(path, delta.length(), true) : null;
		}
		ApkDeltaBuilder builder = new ApkDeltaBuilder(target, manifest, oldBuilds);
		long estimate = builder.estimate();
		if (!isWorthIt(estimate, target)) {
			return null;
		}
		// speculation never takes the place of a phone that is waiting
		if (deltaBuilds.getQueue().isEmpty()) {
			buildOnce(builder, target, delta);
		}
		return new BuildSize(path, estimate, false);
	}

	private synchronized Future<?> buildOnce(ApkDeltaBuilder builder, File target, File delta) {
		runningBuilds.values().removeIf(Future::isDone);
		return runningBuilds.computeIfAbsent(delta, file -> deltaBuilds.submit(() -> build(builder, target, delta)));
	}

	private void waitFor(Future<?> build) throws IOException {
		try {
			build.get(MAX_BUILD_WAIT_SECONDS, TimeUnit.SECONDS);
		} catch (TimeoutException e) {
			throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new IOException(e);
		} catch (ExecutionException e) {
			throw new IOException(e);
		}
	}

	private void acquireRequest() {
		if (!requests.tryAcquire()) {
			throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE);
		}
	}

	private void build(ApkDeltaBuilder builder, File target, File delta) {
		if (delta.exists()) {
			return;
		}
		long start = System.currentTimeMillis();
		File temp = null;
		try {
			File entryPatches = new File(deltaFolder(), ENTRY_PATCHES_FOLDER);
			Files.createDirectories(entryPatches.toPath());
			trimCache();
			temp = File.createTempFile(delta.getName(), ".tmp", deltaFolder());
			ApkDeltaBuilder.Result result = builder.build(entryPatches, temp);
			if (!isWorthIt(result.size(), target)) {
				// an empty file remembers the answer
				Files.write(temp.toPath(), new byte[0]);
			}
			Files.move(temp.toPath(), delta.toPath(), StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
			LOGGER.info(String.format("APK delta to %s: %d bytes, %d copied, %d patched, %d whole in %d ms",
					target.getName(), result.size(), result.copied(), result.patched(), result.full(),
					System.currentTimeMillis() - start));
		} catch (IOException | RuntimeException | OutOfMemoryError e) {
			LOGGER.error("Could not build the APK delta to " + target.getName(), e);
		} finally {
			if (temp != null) {
				temp.delete();
			}
		}
	}

	private void trimCache() throws IOException {
		List<File> files;
		try (Stream<Path> paths = Files.walk(deltaFolder().toPath())) {
			files = paths.map(Path::toFile).filter(File::isFile)
					.sorted(Comparator.comparingLong(File::lastModified).reversed()).toList();
		}
		long oldest = System.currentTimeMillis() - MAX_CACHE_AGE_MS;
		long size = 0;
		for (File file : files) {
			size += file.length();
			if (size > MAX_CACHE_SIZE || file.lastModified() < oldest) {
				file.delete();
			}
		}
	}

	private Map<String, File> findOldBuilds(Map<String, ApkArchive.Entry> manifest) {
		List<File> builds = new ArrayList<>();
		for (String folder : new String[] {NIGHT_BUILDS, LATEST_BUILDS}) {
			File[] files = new File(filesLocation, folder).listFiles(BuildDeltaService::isBuild);
			if (files != null) {
				builds.addAll(Arrays.asList(files));
			}
		}
		builds.sort(Comparator.comparingLong(File::lastModified).reversed());
		return ApkDeltaBuilder.findOldBuilds(manifest, builds.subList(0, Math.min(builds.size(), MAX_BUILDS_TO_SEARCH)));
	}

	private static boolean isBuildPath(String path) {
		return BUILD_PATH.matcher(path).matches();
	}

	private static boolean isBuild(File file) {
		return file.getName().endsWith(BUILD_EXTENSION);
	}

	private static boolean isWorthIt(long size, File target) {
		return size > 0 && size <= target.length() * MAX_DELTA_RATIO;
	}

	private File deltaFolder() {
		return new File(genLocation, DELTA_FOLDER);
	}

	private File deltaFile(File target, Map<String, ApkArchive.Entry> manifest) {
		String key = new String(ApkDeltaFormat.MAGIC, StandardCharsets.US_ASCII) + ":" + target.getName() + ":"
				+ target.length() + ":" + target.lastModified() + "\n"
				+ manifest.values().stream().map(entry -> entry.contentKey() + ":" + entry.compressedSize + ":" + entry.method)
				.sorted().collect(Collectors.joining("\n"));
		return new File(deltaFolder(), DigestUtils.sha1Hex(key) + ".delta");
	}

	/** The entries by name. It is what a client has sent: its size and format are not to be trusted. */
	static Map<String, ApkArchive.Entry> readManifest(InputStream gzipped) {
		try (InputStream input = new GZIPInputStream(gzipped)) {
			byte[] bytes = input.readNBytes(MAX_MANIFEST_SIZE + 1);
			if (bytes.length > MAX_MANIFEST_SIZE) {
				throw brokenManifest();
			}
			Map<String, ApkArchive.Entry> entries = new HashMap<>();
			for (String line : new String(bytes, StandardCharsets.UTF_8).split("\n")) {
				String[] fields = line.split(" ", 5);
				ApkArchive.Entry entry = new ApkArchive.Entry(fields[4], Long.parseLong(fields[0]), Long.parseLong(fields[1]),
						Long.parseLong(fields[2]), Integer.parseInt(fields[3]));
				if (entries.put(entry.name, entry) != null) {
					throw brokenManifest();
				}
			}
			return entries;
		} catch (IOException | NumberFormatException | ArrayIndexOutOfBoundsException e) {
			throw brokenManifest();
		}
	}

	private static ResponseStatusException brokenManifest() {
		return new ResponseStatusException(HttpStatus.BAD_REQUEST, "Broken manifest");
	}
}
