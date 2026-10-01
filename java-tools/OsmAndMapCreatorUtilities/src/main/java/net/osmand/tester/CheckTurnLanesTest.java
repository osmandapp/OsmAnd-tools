package net.osmand.tester;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import com.google.gson.GsonBuilder;

import net.osmand.ResultMatcher;
import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.binary.BinaryMapIndexReader.SearchRequest;
import net.osmand.binary.BinaryMapRouteReaderAdapter.RouteRegion;
import net.osmand.binary.BinaryMapRouteReaderAdapter.RouteSubregion;
import net.osmand.binary.RouteDataObject;
import net.osmand.binary.ObfConstants;
import net.osmand.data.LatLon;
import net.osmand.router.RoutePlannerFrontEnd;
import net.osmand.router.RouteSegmentResult;
import net.osmand.router.RoutingConfiguration;
import net.osmand.router.RoutingConfiguration.RoutingMemoryLimits;
import net.osmand.router.RoutingContext;
import net.osmand.router.TurnType;
import net.osmand.util.MapUtils;

/**
 * Runs the recorded turn-lane cases of one obf through the turn preparation of the checkout this is
 * built against and writes what it said against what the file expects.
 *
 * The verdict is the one {@code RouteResultPreparationTest} gives, so a row marked OK here is a row
 * that test passes. That is the point of the pair: {@code GenerateTurnLanesTest} records the cases
 * on one branch, this one replays them on another, and every FAIL is a difference between the two.
 */
public class CheckTurnLanesTest {

	private static final String MUTE = "[MUTE] ";

	private static String profile = "car";

	/**
	 * What {@code RouteResultPreparationTest} would say about the row, and nothing finer: the test
	 * either passes an expectation or fails it, and a report that graded differences of its own
	 * would be describing something nobody runs.
	 */
	public enum Verdict {
		OK, FAIL
	}

	public static void main(String[] args) throws IOException, InterruptedException {
		if (args.length < 3) {
			System.out.println("check-turn-lanes-test <file.obf> <test.json> <report.html> [--profile=car]");
			return;
		}
		File obf = new File(args[0]);
		File json = new File(args[1]);
		File report = new File(args[2]);
		for (int i = 3; i < args.length; i++) {
			if (args[i].startsWith("--profile=")) {
				profile = args[i].substring(args[i].indexOf('=') + 1);
			}
		}
		FileReader reader = new FileReader(json);
		Case[] cases = new GsonBuilder().create().fromJson(reader, Case[].class);
		reader.close();

		RandomAccessFile raf = new RandomAccessFile(obf, "r");
		BinaryMapIndexReader[] readers = {new BinaryMapIndexReader(raf, obf)};
		List<Row> rows = new ArrayList<>();
		TreeSet<String> junctions = new TreeSet<>();
		Map<Verdict, Integer> totals = new LinkedHashMap<>();
		for (Case c : cases) {
			if (c.expectedResults == null || c.expectedResults.isEmpty()) {
				continue;
			}
			junctions.add(junctionOf(c.testName));
			Map<String, double[]> where = new LinkedHashMap<>();
			Drive drive = run(readers, c, where);
			for (Entry<String, String> expected : c.expectedResults.entrySet()) {
				String key = expected.getKey();
				List<Said> said = drive == null ? null : drive.turns.get(lookup(key, drive.turns));
				boolean reached = drive != null && drive.reached(key);
				Verdict verdict = verdict(expected.getValue(), said, reached);
				totals.put(verdict, (totals.containsKey(verdict) ? totals.get(verdict) : 0) + 1);
				String shown = said != null ? display(said) : reached ? "NULL" : null;
				Row row = new Row(verdict, c.testName, key, link(c), expected.getValue(), shown);
				row.at = where.containsKey(key) ? where.get(key)
						: where.get(key.indexOf(':') > 0 ? key.substring(0, key.indexOf(':')) : key);
				rows.add(row);
			}
		}
		locate(readers[0], rows);
		readers[0].close();
		write(report, obf, junctions.size(), cases.length, rows, totals);
		System.out.printf("%s: %d junctions, %d cases, %d expectations -> %s%n",
				obf.getName(), junctions.size(), cases.length, rows.size(), report);
	}

	// ------------------------------------------------------------------ the file being checked

	/** the fields of test_turn_lanes.json this tool reads */
	private static class Case {
		String testName;
		LatLon startPoint;
		LatLon endPoint;
		Map<String, String> params;
		Map<String, String> expectedResults;
	}

	private static class Row {
		final Verdict verdict;
		final String name;
		final String segment;
		final String link;
		final String expected;
		final String actual;
		double[] at;

		Row(Verdict verdict, String name, String segment, String link, String expected, String actual) {
			this.verdict = verdict;
			this.name = name;
			this.segment = segment;
			this.link = link;
			this.expected = expected;
			this.actual = actual;
		}
	}

	/** cases of one junction are named after it, with the drive's number on the end */
	private static String junctionOf(String testName) {
		return testName == null ? "" : testName.replaceAll("\\s+\\d+$", "");
	}

	private static String link(Case c) {
		// Locale.ROOT or a comma-decimal JVM writes "45,69" and tears every coordinate in half
		return String.format(Locale.ROOT, "https://osmand.net/map/navigate/?start=%f,%f&end=%f,%f&profile=%s",
				c.startPoint.getLatitude(), c.startPoint.getLongitude(),
				c.endPoint.getLatitude(), c.endPoint.getLongitude(), profile);
	}

	// ------------------------------------------------------------------ what the pass says

	/** what one drive said: the instruction of every road that carries one, and every road it touched */
	private static class Drive {
		/** by road, and by road and start point: a record naming only the road answers for all of them */
		final Map<String, List<Said>> turns = new LinkedHashMap<>();
		final Set<String> onTheRoute = new HashSet<>();

		/** a record naming a start point is reached by that point, one naming only the road by any of it */
		boolean reached(String key) {
			return onTheRoute.contains(key)
					|| onTheRoute.contains(key.indexOf(':') > 0 ? key.substring(0, key.indexOf(':')) : key);
		}
	}

	/** one instruction in the three pieces the test compares against */
	static class Said {
		final boolean mute;
		final String turn;
		final String lanes;

		Said(boolean mute, String turn, String lanes) {
			this.mute = mute;
			this.turn = turn;
			this.lanes = lanes;
		}

		/**
		 * The whole instruction, the lane row alone, or the manoeuvre alone - the test takes any of
		 * the three. A turn without lanes has null for the row, and the test's own concatenation puts
		 * that null into the first form, so only the manoeuvre alone can answer for it.
		 */
		boolean matches(String expected) {
			return expected.equals((mute ? MUTE : "") + turn + ":" + lanes)
					|| (lanes != null && expected.equals(lanes)) || expected.equals(turn);
		}

		String display() {
			return (mute ? MUTE : "") + turn + (lanes == null ? "" : ":" + lanes);
		}
	}

	private static Drive run(BinaryMapIndexReader[] readers, Case c,
			Map<String, double[]> where) throws IOException, InterruptedException {
		Map<String, String> params = c.params == null ? new HashMap<String, String>()
				: new HashMap<>(c.params);
		params.put(profile, "true");
		RoutingMemoryLimits limits = new RoutingMemoryLimits(
				RoutingConfiguration.DEFAULT_MEMORY_LIMIT * 3, RoutingConfiguration.DEFAULT_NATIVE_MEMORY_LIMIT);
		RoutingConfiguration config = RoutingConfiguration.getDefault().build(profile, limits, params);
		RoutePlannerFrontEnd fe = new RoutePlannerFrontEnd();
		RoutingContext ctx = fe.buildRoutingContext(config, null, readers,
				RoutePlannerFrontEnd.RouteCalculationMode.NORMAL);
		ctx.leftSideNavigation = "true".equals(params.get("leftSide"));
		List<RouteSegmentResult> route;
		try {
			route = fe.searchRoute(ctx, c.startPoint, c.endPoint, null).getList();
		} catch (RuntimeException e) {
			return null;
		}
		if (route == null || route.isEmpty()) {
			return null;
		}
		// searchRoute has already run the preparation of this checkout over the route
		Drive drive = new Drive();
		for (RouteSegmentResult segment : route) {
			long id = ObfConstants.getOsmObjectId(segment.getObject());
			drive.onTheRoute.add(id + ":" + segment.getStartPointIndex());
			drive.onTheRoute.add(String.valueOf(id));
			TurnType turn = segment.getTurnType();
			if (turn == null) {
				continue;
			}
			Said said = new Said(turn.isSkipToSpeak(), turn.toXmlString(),
					turn.getLanes() == null ? null : TurnType.lanesToString(turn.getLanes()));
			double[] at = {segment.getStartPoint().getLatitude(), segment.getStartPoint().getLongitude()};
			String point = id + ":" + segment.getStartPointIndex();
			drive.turns.put(point, new ArrayList<>(Collections.singletonList(said)));
			where.put(point, at);
			List<Said> road = drive.turns.get(String.valueOf(id));
			if (road == null) {
				drive.turns.put(String.valueOf(id), new ArrayList<>(Collections.singletonList(said)));
				where.put(String.valueOf(id), at);
			} else {
				road.add(said);
			}
		}
		return drive;
	}

	/** a record naming a start point is answered by that point, one naming only the road by any of it */
	private static String lookup(String key, Map<String, List<Said>> actual) {
		return actual.containsKey(key) ? key : key.indexOf(':') > 0 ? key.substring(0, key.indexOf(':')) : key;
	}

	/**
	 * Places for the rows that have none: a record can name a road the drive never touched, and
	 * then only the map knows where it is. One pass over the routing section answers all of them,
	 * so this costs a read of the file and not a read per row.
	 */
	private static void locate(BinaryMapIndexReader reader, List<Row> rows) throws IOException {
		final Map<Long, List<Row>> wanted = new HashMap<>();
		for (Row row : rows) {
			if (row.at != null) {
				continue;
			}
			String key = row.segment;
			int colon = key.indexOf(':');
			try {
				long id = Long.parseLong(colon > 0 ? key.substring(0, colon) : key);
				List<Row> asking = wanted.get(id);
				if (asking == null) {
					asking = new ArrayList<>();
					wanted.put(id, asking);
				}
				asking.add(row);
			} catch (NumberFormatException e) {
				// a key that is not an id: nothing to look up
			}
		}
		if (wanted.isEmpty()) {
			return;
		}
		for (RouteRegion region : reader.getRoutingIndexes()) {
			SearchRequest<RouteDataObject> request = BinaryMapIndexReader.buildSearchRouteRequest(
					0, Integer.MAX_VALUE, 0, Integer.MAX_VALUE, null);
			List<RouteSubregion> subregions = reader.searchRouteIndexTree(request, region.getSubregions());
			reader.loadRouteIndexData(subregions, new ResultMatcher<RouteDataObject>() {
				@Override
				public boolean publish(RouteDataObject road) {
					List<Row> asking = wanted.get(ObfConstants.getOsmObjectId(road));
					if (asking != null && road.getPointsLength() > 0) {
						double[] at = {MapUtils.get31LatitudeY(road.getPoint31YTile(0)),
								MapUtils.get31LongitudeX(road.getPoint31XTile(0))};
						for (Row row : asking) {
							row.at = at;
						}
					}
					return false;
				}

				@Override
				public boolean isCancelled() {
					return false;
				}
			});
		}
	}

	// ------------------------------------------------------------------ the report

	private static void write(File report, File obf, int junctions, int cases, List<Row> rows,
			Map<Verdict, Integer> totals) throws IOException {
		StringBuilder sb = new StringBuilder();
		sb.append("<!doctype html>\n<html><head><meta charset=\"utf-8\">\n");
		sb.append("<title>Turn lanes: ").append(html(obf.getName())).append("</title>\n");
		sb.append("<style>\n")
				.append("body{font:14px/1.45 -apple-system,Segoe UI,Roboto,sans-serif;margin:24px;color:#1b1b1b}\n")
				.append("h1{font-size:19px;margin:0 0 6px}\n")
				.append("p.sum{margin:0 0 6px;color:#555}\n")
				.append(".bar{display:flex;width:50%;height:10px;border-radius:5px;overflow:hidden;")
				.append("margin:0 0 20px;background:#eee}\n")
				.append(".bar span{display:block}\n")
				.append(".bar .bOK{background:#1a7f37}.bar .bFAIL{background:#c0392b}\n")
				.append("table{border-collapse:collapse;width:100%}\n")
				.append("th,td{border-bottom:1px solid #e3e3e3;padding:6px 10px;text-align:left;vertical-align:top}\n")
				.append("th{font-weight:600;color:#555;border-bottom:2px solid #ccc;white-space:nowrap}\n")
				.append("td.st{font-weight:600;white-space:nowrap}\n")
				.append(".OK{color:#1a7f37}.FAIL{color:#c0392b}\n")
				.append("td.lanes{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:12.5px;white-space:nowrap}\n")
				.append("td.lanes .a{color:#555}\n")
				.append("td.seg{font-family:ui-monospace,Menlo,monospace;font-size:12.5px;color:#555}\n")
				.append("tr:hover{background:#fafafa}\n")
				.append("</style>\n</head><body>\n");
		sb.append("<h1>Turn lanes: ").append(html(obf.getName())).append("</h1>\n");
		sb.append("<p class=\"sum\">").append(junctions).append(" junctions, ").append(cases)
				.append(" cases, ").append(rows.size()).append(" expectations — ")
				.append(share("OK", totals, Verdict.OK, rows.size()))
				.append(" · ").append(share("FAIL", totals, Verdict.FAIL, rows.size()))
				.append("</p>\n");
		sb.append(bar(totals, rows.size()));
		sb.append("<table>\n<tr><th>Status</th><th>Name</th><th>Segment</th>")
				.append("<th>Expected / actual</th><th>Link</th><th>Eyepiece</th></tr>\n");
		for (Row row : rows) {
			sb.append("<tr>")
					.append("<td class=\"st ").append(row.verdict).append("\">").append(row.verdict).append("</td>")
					.append("<td title=\"").append(html(row.name)).append("\">")
					.append(html(shorten(row.name))).append("</td>")
					.append("<td class=\"seg\">").append(html(row.segment)).append("</td>")
					.append("<td class=\"lanes\">").append(html(row.expected == null ? "—" : row.expected))
					.append("<br><span class=\"a\">").append(html(row.actual == null ? "—" : row.actual))
					.append("</span></td>")
					.append("<td><a href=\"").append(html(row.link)).append(anchor(row)).append("\">map</a></td>")
					.append("<td></td>")
					.append("</tr>\n");
		}
		sb.append("</table>\n</body></html>\n");
		FileWriter w = new FileWriter(report);
		w.write(sb.toString());
		w.close();
	}

	/**
	 * Where on the map to open: the instruction being judged, not the start of the drive. A route
	 * of a kilometre has several, and finding the one a row is about by eye is the slow part.
	 */
	private static String anchor(Row row) {
		return row.at == null ? "" : String.format(Locale.ROOT, "#20/%.5f/%.5f", row.at[0], row.at[1]);
	}

	private static String html(String s) {
		return s == null ? "" : s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
				.replace("\"", "&quot;");
	}

	/** the same numbers as a line the eye reads before the words */
	private static String bar(Map<Verdict, Integer> totals, int of) {
		StringBuilder sb = new StringBuilder("<div class=\"bar\">");
		for (Verdict verdict : new Verdict[] {Verdict.OK, Verdict.FAIL}) {
			int n = count(totals, verdict);
			if (n == 0) {
				continue;
			}
			double percent = of == 0 ? 0 : 100.0 * n / of;
			sb.append(String.format(Locale.ROOT,
					"<span class=\"b%s\" style=\"width:%.2f%%\" title=\"%s %d (%.1f%%)\"></span>",
					verdict, percent, verdict, n, percent));
		}
		return sb.append("</div>\n").toString();
	}

	private static String share(String label, Map<Verdict, Integer> totals, Verdict verdict, int of) {
		int n = count(totals, verdict);
		return String.format(Locale.ROOT, "%s %d (%.1f%%)", label, n, of == 0 ? 0.0 : 100.0 * n / of);
	}

	private static int count(Map<Verdict, Integer> totals, Verdict verdict) {
		return totals.containsKey(verdict) ? totals.get(verdict) : 0;
	}

	// ------------------------------------------------------------------ verdicts

	/**
	 * The judgement of {@code RouteResultPreparationTest} and nothing besides: a record passes when
	 * the drive reached the road and the expectation is one of the three forms that test accepts -
	 * the whole instruction, the lane row alone, or the manoeuvre alone.
	 */
	/** several instructions on one road are shown as they were driven */
	private static String display(List<Said> said) {
		StringBuilder sb = new StringBuilder();
		for (Said one : said) {
			sb.append(sb.length() > 0 ? " / " : "").append(one.display());
		}
		return sb.toString();
	}

	static Verdict verdict(String expected, List<Said> said, boolean reached) {
		if (!reached) {
			// "Segment ... was not reached in ..."
			return Verdict.FAIL;
		}
		if (expected == null) {
			// the record names the road and says nothing about it: reaching it is the whole test
			return Verdict.OK;
		}
		if (said == null) {
			// on the route but carrying no instruction: the test reads that as "NULL"
			return isEmpty(expected) ? Verdict.OK : Verdict.FAIL;
		}
		// a record naming only the road is asserted against every instruction that road carries
		for (Said one : said) {
			if (!one.matches(expected)) {
				return Verdict.FAIL;
			}
		}
		return Verdict.OK;
	}


	private static boolean isEmpty(String s) {
		return s == null || s.isEmpty();
	}

	private static String shorten(String name) {
		if (name == null) {
			return "";
		}
		String trimmed = name.trim();
		return trimmed.length() <= 10 ? trimmed : trimmed.substring(0, 10);
	}
}
