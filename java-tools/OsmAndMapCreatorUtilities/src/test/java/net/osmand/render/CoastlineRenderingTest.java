package net.osmand.render;

import java.io.File;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.Assume;
import org.junit.Ignore;
import org.junit.Test;

import net.osmand.render.CoastlineRenderingTester.CaseStats;
import net.osmand.render.CoastlineRenderingTester.RunResult;

/**
 * Runs the known coastline problems of
 * <a href="https://github.com/osmandapp/OsmAnd-Issues/issues/3291">Epic - Coastline issues</a>
 * through {@link CoastlineRenderingTester} - see that class for what is compared and for all the
 * options. The cases themselves live in {@code coastline-tests.json}.
 *
 * <p>Ignored by default: the issues are open, so it fails by design, and it needs the native
 * library plus a few hundred megabytes of maps. Run it explicitly when working on a coastline fix.
 * The normal way to run the same check is the standalone utility, which reports an exit code
 * (0 ok / 2 problems reproduced / 1 could not run) and is what the build server runs:
 * <pre>
 * OsmAndMapCreator/utilities.sh test-coastline-rendering -maps.dir=/var/maps
 * OsmAndMapCreator/utilities.sh test-coastline-rendering -scan -minzoom=1 -maxzoom=10 -maps.dir=/var/maps
 * </pre>
 */
public class CoastlineRenderingTest {

	@Test
	@Ignore("reproduces the open issues of the epic, run it manually or use test-coastline-rendering")
	public void testCoastlineOfTheEpic() throws Exception {
		File maps = new File(System.getProperty("maps.dir",
				new File(System.getProperty("user.home"), "osmand/maps").getAbsolutePath()));
		Assume.assumeTrue("Maps folder " + maps + " does not exist, pass -Dmaps.dir=...", maps.isDirectory());

		Map<String, String> options = new LinkedHashMap<>();
		options.put("maps.dir", maps.getAbsolutePath());
		options.put("out", new File("build/coastline-tiles").getAbsolutePath());
		// only the maps a case declares - the unit test should not depend on what else is downloaded
		options.put("load", "case");
		RunResult result;
		try {
			result = new CoastlineRenderingTester(options).run();
		} catch (IllegalStateException e) {
			Assume.assumeNoException("Coastline tester can not run: " + e.getMessage(), e);
			return;
		}
		Assume.assumeTrue("No reference tile could be downloaded", result.comparedTiles > 0);
		if (result.failedTiles > 0) {
			StringBuilder sb = new StringBuilder(result.failedTiles
					+ " tile(s) reproduce a coastline problem of the epic:");
			for (CaseStats s : result.cases) {
				if (s.failedTiles > 0) {
					sb.append(String.format("%n  - #%d %s: %d of %d tiles, worst %s extra water / "
									+ "%s missing water at %s", s.issue, s.title, s.failedTiles,
							s.comparedTiles, pct(s.worstExtraWater), pct(s.worstMissingWater), s.worstTile));
				}
			}
			sb.append("\n  see build/coastline-tiles/index.html");
			throw new AssertionError(sb.toString());
		}
	}

	private static String pct(double v) {
		return String.format("%.2f%%", v * 100);
	}
}
