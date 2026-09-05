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

	/** Answer of eyepiece for one tile - see the tiles mode of EyePiece.cpp. */
	private static final String TILE_PREFIX = "TILE ";

	/** Output lines kept for the error message when the process dies. */
	private static final int KEPT_LOG_LINES = 40;

	private final File binary;
	private final File stylesPath;
	private final String styleName;
	private final int tileSize;
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

	EyePieceTileRenderer(File binary, File stylesPath, String styleName, int tileSize, File workDir,
			boolean verbose) {
		this.binary = binary;
		this.stylesPath = stylesPath;
		this.styleName = styleName;
		this.tileSize = tileSize;
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

	/** Renders one tile of the {@code z/x/y} scheme, 256x256 by default. */
	BufferedImage render(int zoom, int x, int y) throws IOException {
		ensureStarted();
		String tile = zoom + "/" + x + "/" + y;
		toProcess.write(tile + "\n");
		toProcess.flush();
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
				throw new IOException("eyepiece died (exit code " + code + ") on tile " + tile
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
		ProcessBuilder pb = new ProcessBuilder(cmd);
		pb.redirectErrorStream(true);
		// the shared libraries of core and Qt normally sit next to the binary
		String libs = binary.getAbsoluteFile().getParent();
		pb.environment().merge("DYLD_LIBRARY_PATH", libs, (old, add) -> add + File.pathSeparator + old);
		pb.environment().merge("LD_LIBRARY_PATH", libs, (old, add) -> add + File.pathSeparator + old);
		process = pb.start();
		toProcess = new OutputStreamWriter(process.getOutputStream(), StandardCharsets.UTF_8);
		fromProcess = new BufferedReader(new InputStreamReader(process.getInputStream(),
				StandardCharsets.UTF_8));
		lastLines.clear();
		restartNeeded = false;
		startCount++;
		System.out.println("Started eyepiece #" + startCount + " with " + maps.size() + " map(s)");
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
			if (!process.waitFor(30, java.util.concurrent.TimeUnit.SECONDS)) {
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
