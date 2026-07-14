package net.osmand.server.osmgpx;

import net.osmand.obf.ToolsOsmAndContextImpl;
import net.osmand.server.osmgpx.DownloadOsmGPX.SpeedMetrics;
import net.osmand.shared.gpx.GpxFile;
import net.osmand.shared.gpx.GpxUtilities;
import net.osmand.shared.io.KFile;

import java.io.File;

public class SpeedMetricsTest {

	public static void main(String[] args) {
		net.osmand.shared.util.PlatformUtil.INSTANCE.initialize(new ToolsOsmAndContextImpl());

		String home = System.getProperty("user.home") + "/osmand/osmgpx-test";
		File inDir = new File(args.length > 0 ? args[0] : home + "/speed");

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
			SpeedMetrics m = DownloadOsmGPX.computeSpeedMetrics(gpx);
			System.out.printf("%-40s avg %6.1f km/h  max %8.1f km/h%n", f.getName(), m.avgKmh(), m.maxKmh());
		}
	}
}
