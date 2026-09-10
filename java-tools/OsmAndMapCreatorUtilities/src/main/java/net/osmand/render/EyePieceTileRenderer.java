package net.osmand.render;

import java.awt.image.BufferedImage;
import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.imageio.ImageIO;

/**
 * Renders map tiles with the <b>OpenGL (v2 / OsmAndCore)</b> engine, by driving the {@code eyepiece}
 * tool of core as a co-process - the counterpart of {@code NativeJavaRendering}, which is the legacy
 * (v1) engine.
 *
 * <p>eyepiece is started <i>once</i> and is fed the tiles one by one through its batch tile mode
 * ({@code -tiles=- -tilesOutputDir=...}): it reads {@code z/x/y} lines from stdin, renders one image
 * per tile and answers with a {@code TILE z/x/y <file>} line. That matters - a process per tile costs
 * 2 s with two maps and 9 s with 182 of them before it draws anything, all of it obf index scanning
 * and OpenGL setup, which would make a run of thousands of tiles impossible.
 *
 * <p>A tile is rendered from the centre of the tile in 31 bit coordinates with the window sized to
 * exactly one tile, which is what makes the result comparable with a raster tile server. Verified
 * pixel by pixel: two horizontally adjacent tiles rendered this way are identical to the two halves
 * of a single 512x256 render centred on their common edge.
 *
 * <p>The set of maps is fixed when the process starts, so {@link #setMaps} only records it and the
 * process is restarted before the next tile. The maps are handed over as a folder of symbolic links
 * ({@code -obfsPath=}), because a case that closes every map and a full server run with a thousand
 * of them are the same code path this way, and the command line stays short.
 */
class EyePieceTileRenderer implements Closeable {

	/**
	 * One tile could not be rendered because eyepiece died on it - the run goes on with the next
	 * tile on a restarted process. A crash of the renderer is a defect worth reporting, so such
	 * tiles are counted and named in the report instead of being dropped silently.
	 */
	static class TileRenderFailure extends IOException {
		private static final long serialVersionUID = 1L;

		TileRenderFailure(String message) {
			super(message);
		}
	}

	/** Thrown when the process is gone; {@link #render} decides whether that ends the run. */
	private static class EyePieceDied extends IOException {
		private static final long serialVersionUID = 1L;

		EyePieceDied(String message) {
			super(message);
		}
	}

	/** Answer of eyepiece for one tile - see the tiles mode of EyePiece.cpp. */
	private static final String TILE_PREFIX = "TILE ";

	/** Output lines kept for the error message when the process dies. */
	private static final int KEPT_LOG_LINES = 40;

	/** How long {@link #checkBatchTileMode} waits for eyepiece to print its usage and exit. */
	private static final int PROBE_TIMEOUT_SECONDS = 60;

	/** How long a process gets to leave on its own once its stdin is closed. */
	private static final int STOP_TIMEOUT_SECONDS = 30;

	/**
	 * Deaths of the process before the whole run is given up. A restart costs the obf scan of every
	 * map, so a binary that dies on every tile must not keep a run going for days.
	 */
	private static final int MAX_DEATHS = 50;

	private final File binary;
	private final File stylesPath;
	private final String styleName;
	private final int tileSize;
	private final boolean symbols;
	private final File mapsLinkDir;
	private final File tilesDir;
	private final boolean verbose;

	private final Set<File> maps = new LinkedHashSet<>();
	private final Deque<String> lastLines = new ArrayDeque<>();

	private Process process;
	private Writer toProcess;
	private BufferedReader fromProcess;
	/** The maps changed (or nothing was started yet), so the process has to be restarted. */
	private boolean restartNeeded = true;
	private int startCount;
	private int deathCount;

	EyePieceTileRenderer(File binary, File stylesPath, String styleName, int tileSize,
			boolean symbols, File workDir, boolean verbose) {
		this.binary = binary;
		this.stylesPath = stylesPath;
		this.styleName = styleName;
		this.tileSize = tileSize;
		this.symbols = symbols;
		this.mapsLinkDir = new File(workDir, "opengl-maps");
		this.tilesDir = new File(workDir, "opengl-tiles");
		this.verbose = verbose;
	}

	/** The maps eyepiece must see. Takes effect on the next tile, by restarting the process. */
	void setMaps(Collection<File> newMaps) {
		Set<File> wanted = new LinkedHashSet<>(newMaps);
		if (!wanted.equals(maps)) {
			maps.clear();
			maps.addAll(wanted);
			restartNeeded = true;
		}
	}

	/**
	 * Renders one tile of the {@code z/x/y} scheme, 256x256 by default.
	 *
	 * <p>eyepiece dying is not the end of the run: rendering a tile can hit a bug of core (a Qt
	 * assert, a segfault) and thousands of tiles must not be lost to one of them. The process is
	 * restarted and the tile is tried once more - a crash can come from the state left by the
	 * previous tiles and then does not repeat on a fresh process - and if it dies again the tile is
	 * reported as a {@link TileRenderFailure} and the run continues.
	 */
	BufferedImage render(int zoom, int x, int y) throws IOException {
		try {
			return renderOnce(zoom, x, y);
		} catch (EyePieceDied first) {
			deathCount++;
			System.err.println("  " + first.getMessage());
			if (deathCount > MAX_DEATHS) {
				throw new IOException("eyepiece died " + deathCount + " times, giving up", first);
			}
			System.err.printf("  restarting eyepiece and retrying %d/%d/%d%n", zoom, x, y);
			try {
				return renderOnce(zoom, x, y);
			} catch (EyePieceDied second) {
				deathCount++;
				throw new TileRenderFailure("eyepiece dies on " + zoom + "/" + x + "/" + y + " - "
						+ lastMeaningfulLine(second.getMessage()));
			}
		}
	}

	/** How many times eyepiece died during the run. */
	int deaths() {
		return deathCount;
	}

	/**
	 * The line of the output that says what went wrong - the assert or the signal, not the last
	 * {@code TILE} line before it, which is only the tile that went through.
	 */
	private static String lastMeaningfulLine(String output) {
		String[] lines = output.split("\n");
		for (int i = lines.length - 1; i >= 0; i--) {
			String l = lines[i].trim();
			if (!l.isEmpty() && !l.startsWith(TILE_PREFIX)) {
				return l;
			}
		}
		return output;
	}

	private BufferedImage renderOnce(int zoom, int x, int y) throws IOException {
		ensureStarted();
		String tile = zoom + "/" + x + "/" + y;
		try {
			toProcess.write(tile + "\n");
			toProcess.flush();
		} catch (IOException e) {
			// the process died between two tiles, its output went with it
			stop();
			throw new EyePieceDied("eyepiece is gone before " + tile + ": " + e.getMessage());
		}
		String answer = readAnswer(tile);
		File png = new File(answer);
		if (!png.isFile()) {
			throw new IOException("eyepiece did not write " + png.getAbsolutePath() + " for " + tile);
		}
		try {
			BufferedImage img = ImageIO.read(png);
			if (img == null) {
				throw new IOException("eyepiece wrote an unreadable image for " + tile);
			}
			return img;
		} finally {
			// the tiles worth keeping are written by the tester itself, into the report
			png.delete();
		}
	}

	/**
	 * Reads the output of eyepiece until the answer for {@code tile}. Everything the renderer logs
	 * goes to the same stream, hence the {@value #TILE_PREFIX} prefix; the stream must be consumed
	 * anyway, or the process blocks on a full pipe.
	 */
	private String readAnswer(String tile) throws IOException {
		String expected = TILE_PREFIX + tile + " ";
		for (;;) {
			String line = fromProcess.readLine();
			if (line == null) {
				int code = -1;
				try {
					code = process.waitFor();
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
				stop();
				throw new EyePieceDied("eyepiece died (exit code " + code + ") on tile " + tile
						+ ", last output:\n" + String.join("\n", lastLines));
			}
			keepLine(line);
			if (verbose) {
				System.out.println("  [eyepiece] " + line);
			}
			if (line.startsWith(expected)) {
				String rest = line.substring(expected.length()).trim();
				if (rest.startsWith("ERROR")) {
					throw new IOException("eyepiece could not render " + tile + ": " + rest);
				}
				return rest;
			}
			if (line.startsWith(TILE_PREFIX)) {
				// answers are in order, so this is an answer for a tile nobody is waiting for
				throw new IOException("eyepiece answered '" + line + "' while " + tile + " was asked");
			}
		}
	}

	private void keepLine(String line) {
		lastLines.addLast(line);
		while (lastLines.size() > KEPT_LOG_LINES) {
			lastLines.removeFirst();
		}
	}

	private void ensureStarted() throws IOException {
		if (process != null && process.isAlive() && !restartNeeded) {
			return;
		}
		stop();
		linkMaps();
		tilesDir.mkdirs();
		List<String> cmd = new ArrayList<>();
		cmd.add(binary.getAbsolutePath());
		cmd.add("-obfsPath=" + mapsLinkDir.getAbsolutePath());
		if (stylesPath != null) {
			cmd.add("-stylesPath=" + stylesPath.getAbsolutePath());
		}
		cmd.add("-styleName=" + styleName);
		cmd.add("-tiles=-");
		cmd.add("-tilesOutputDir=" + tilesDir.getAbsolutePath());
		cmd.add("-tileSize=" + tileSize);
		if (!symbols) {
			// labels and icons are 94% of the time of a v2 tile (measured 500 ms against 30 ms per
			// tile) and say nothing about a coastline, so they are off unless asked for
			cmd.add("-noSymbols");
		}
		process = start(cmd);
		toProcess = new OutputStreamWriter(process.getOutputStream(), StandardCharsets.UTF_8);
		fromProcess = new BufferedReader(new InputStreamReader(process.getInputStream(),
				StandardCharsets.UTF_8));
		lastLines.clear();
		restartNeeded = false;
		startCount++;
		System.out.println("Started eyepiece #" + startCount + " with " + maps.size() + " map(s)");
	}

	private Process start(List<String> cmd) throws IOException {
		ProcessBuilder pb = new ProcessBuilder(cmd);
		pb.redirectErrorStream(true);
		// the shared libraries of core and Qt normally sit next to the binary
		String libs = binary.getAbsoluteFile().getParent();
		pb.environment().merge("DYLD_LIBRARY_PATH", libs, (old, add) -> add + File.pathSeparator + old);
		pb.environment().merge("LD_LIBRARY_PATH", libs, (old, add) -> add + File.pathSeparator + old);
		return pb.start();
	}

	/**
	 * Fails when the binary has no batch tile mode, <i>before</i> the maps and the cases. An
	 * eyepiece older than <a href="https://github.com/osmandapp/OsmAnd-core/pull/1100">core#1100</a>
	 * answers "Unrecognized argument: '-tiles=-'" and dies on the first tile, with its whole usage
	 * text as the error - which is what a build server picking up a stale published binary looks
	 * like. The check costs 0.2 s: the argument parser answers before anything is rendered.
	 */
	void checkBatchTileMode() throws IOException {
		// "-tiles=-" without an output dir is rejected by the argument parser of either binary,
		// before anything is rendered: a new one asks for -tilesOutputDir, an old one does not know
		// the argument at all. Asking about the feature itself rather than reading the usage text
		// is what makes this work on a binary whose core is newer than its tools checkout.
		Process p = start(new ArrayList<>(List.of(binary.getAbsolutePath(), "-tiles=-")));
		StringBuilder usage = new StringBuilder();
		try (BufferedReader r = new BufferedReader(new InputStreamReader(p.getInputStream(),
				StandardCharsets.UTF_8))) {
			for (String line = r.readLine(); line != null; line = r.readLine()) {
				usage.append(line).append('\n');
			}
		}
		try {
			if (!p.waitFor(PROBE_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
				p.destroyForcibly();
				throw new IOException(binary.getAbsolutePath() + " does not answer, it printed:\n"
						+ usage);
			}
		} catch (InterruptedException e) {
			p.destroyForcibly();
			Thread.currentThread().interrupt();
			throw new IOException(e);
		}
		if (usage.indexOf("Unrecognized argument") >= 0) {
			throw new IOException(binary.getAbsolutePath() + " has no batch tile mode (-tiles),"
					+ " it is older than https://github.com/osmandapp/OsmAnd-core/pull/1100."
					+ " Take a newer build - the build server publishes one at"
					+ " https://builder.osmand.net/binaries/amd64-linux-clang/eyepiece_standalone -"
					+ " or build core from master. Pass -eyepieceCheck=false to skip this check."
					+ " It printed:\n" + usage);
		}
	}

	/**
	 * Rebuilds the folder of symbolic links that is handed to eyepiece as {@code -obfsPath}. A copy
	 * would cost gigabytes and the maps folder itself can not be used - it holds the overlays and
	 * the road/srtm/wiki files the tester excludes on purpose.
	 */
	private void linkMaps() throws IOException {
		mapsLinkDir.mkdirs();
		File[] existing = mapsLinkDir.listFiles();
		if (existing != null) {
			for (File f : existing) {
				Files.deleteIfExists(f.toPath());
			}
		}
		for (File map : maps) {
			Path link = new File(mapsLinkDir, map.getName()).toPath();
			try {
				Files.createSymbolicLink(link, map.getAbsoluteFile().toPath());
			} catch (IOException | UnsupportedOperationException e) {
				throw new IOException("Can't link " + map.getAbsolutePath() + " into "
						+ mapsLinkDir.getAbsolutePath() + ": " + e.getMessage(), e);
			}
		}
	}

	private void stop() {
		if (process == null) {
			return;
		}
		try {
			toProcess.close();
		} catch (IOException e) {
			// the process is going away anyway
		}
		try {
			// closed stdin ends the tile loop, and eyepiece releases the OpenGL context on its own
			if (!process.waitFor(STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
				process.destroyForcibly();
			}
		} catch (InterruptedException e) {
			process.destroyForcibly();
			Thread.currentThread().interrupt();
		}
		process = null;
		toProcess = null;
		fromProcess = null;
	}

	@Override
	public void close() {
		stop();
	}
}
