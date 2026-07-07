package net.osmand.server.osmgpx;

import net.osmand.obf.ToolsOsmAndContextImpl;
import net.osmand.shared.gpx.GpxFile;
import net.osmand.shared.gpx.GpxUtilities;
import net.osmand.shared.io.KFile;

import java.io.File;

/**
 * Local check for track simplification + compact byte storage (no DB needed).
 * Usage: SimplifyTracksTest [inputDir] [outputDir] [zoom]
 * Defaults: ~/osmand/osmgpx-test/in -> ~/osmand/osmgpx-test/out, zoom = TrackSimplifyEncoder.SIMPLIFY_ZOOM
 */
public class SimplifyTracksTest {

	public static void main(String[] args) {
		net.osmand.shared.util.PlatformUtil.INSTANCE.initialize(new ToolsOsmAndContextImpl());

		String home = System.getProperty("user.home") + "/osmand/osmgpx-test";
		File inDir = new File(args.length > 0 ? args[0] : home + "/in");
		File outDir = new File(args.length > 1 ? args[1] : home + "/out");
		int zoom = args.length > 2 ? Integer.parseInt(args[2]) : TrackSimplifyEncoder.SIMPLIFY_ZOOM;
		outDir.mkdirs();

		File[] files = inDir.listFiles((d, name) -> name.toLowerCase().endsWith(".gpx"));
		if (files == null || files.length == 0) {
			System.out.println("No .gpx files in " + inDir.getAbsolutePath());
			return;
		}

		long totalOrigPts = 0, totalSimplePts = 0, totalRaw = 0, totalEncoded = 0;
		for (File f : files) {
			GpxFile gpx = GpxUtilities.INSTANCE.loadGpxFile(new KFile(f.getAbsolutePath()));
			if (gpx.getError() != null) {
				System.out.println("SKIP " + f.getName() + ": " + gpx.getError().getMessage());
				continue;
			}
			int origPts = gpx.getAllSegmentsPoints().size();

			GpxFile simplified = TrackSimplifyEncoder.simplifyGpx(gpx, zoom);
			byte[] encoded = TrackSimplifyEncoder.encodeGeometry(simplified);
			GpxFile decoded = TrackSimplifyEncoder.decodeGeometry(encoded);
			int simplePts = decoded.getAllSegmentsPoints().size();

			GpxUtilities.INSTANCE.writeGpxFile(new KFile(new File(outDir, f.getName()).getAbsolutePath()), decoded);

			long raw = f.length();
			totalOrigPts += origPts;
			totalSimplePts += simplePts;
			totalRaw += raw;
			totalEncoded += encoded.length;
			System.out.printf("%-40s pts %6d -> %5d  | gpx %7d B -> bytes %6d B  (x%.1f, %.2f B/pt)%n",
					f.getName(), origPts, simplePts, raw, encoded.length,
					encoded.length == 0 ? 0 : (double) raw / encoded.length,
					simplePts == 0 ? 0 : (double) encoded.length / simplePts);
		}
		System.out.printf("%n== TOTAL pts %d -> %d (%.1f%% kept) | gpx %d B -> bytes %d B (x%.1f) ==%n",
				totalOrigPts, totalSimplePts,
				totalOrigPts == 0 ? 0 : 100.0 * totalSimplePts / totalOrigPts,
				totalRaw, totalEncoded,
				totalEncoded == 0 ? 0 : (double) totalRaw / totalEncoded);
		System.out.println("Output: " + outDir.getAbsolutePath());
	}
}
