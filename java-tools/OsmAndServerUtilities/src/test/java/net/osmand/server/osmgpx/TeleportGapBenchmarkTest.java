package net.osmand.server.osmgpx;

import net.osmand.obf.ToolsOsmAndContextImpl;
import net.osmand.shared.gpx.GpxFile;
import net.osmand.shared.gpx.GpxUtilities;
import net.osmand.shared.io.KFile;

import java.io.File;

public class TeleportGapBenchmarkTest {

	public static void main(String[] args) {
		net.osmand.shared.util.PlatformUtil.INSTANCE.initialize(new ToolsOsmAndContextImpl());

		String home = System.getProperty("user.home") + "/osmand/osmgpx-test";
		File inDir = new File(args.length > 0 ? args[0] : home + "/teleport");
		int iterations = args.length > 1 ? Integer.parseInt(args[1]) : 1000;

		File[] files = inDir.listFiles((d, name) -> name.toLowerCase().endsWith(".gpx"));
		if (files == null || files.length == 0) {
			System.out.println("No .gpx files in " + inDir.getAbsolutePath());
			return;
		}

		for (File f : files) {
			GpxFile gpx = GpxUtilities.INSTANCE.loadGpxFile(new KFile(f.getAbsolutePath()));
			if (gpx.getError() != null) {
				System.out.println("SKIP " + f.getName() + ": " + gpx.getError().getMessage());
				continue;
			}
			int points = gpx.getAllSegmentsPoints().size();

			boolean teleport = GarbageClassifier.hasTeleportGap(gpx); // warm up + result

			long start = System.nanoTime();
			for (int i = 0; i < iterations; i++) {
				GarbageClassifier.hasTeleportGap(gpx);
			}
			long elapsedNs = System.nanoTime() - start;

			System.out.printf("%-40s pts %6d  teleport %-5b  %8.3f ms/call%n",
					f.getName(), points, teleport, elapsedNs / 1_000_000.0 / iterations);
		}
	}
}
