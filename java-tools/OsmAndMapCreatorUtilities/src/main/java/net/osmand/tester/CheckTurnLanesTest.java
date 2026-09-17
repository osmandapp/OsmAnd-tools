package net.osmand.tester;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
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
import net.osmand.router.lanes.LanePanelAI;
import net.osmand.router.lanes.TurnPrepareAI;
import net.osmand.router.lanes.TurnTypeAI;
import net.osmand.util.MapUtils;
import net.osmand.router.lanes.TurnTypeAI.TurnIndication;

/**
 * Runs the recorded turn-lane cases of one obf through {@link TurnPrepareAI} and writes what it
 * said against what the file expects.
 *
 * The verdicts are the test's own, read from {@link TurnLanesVerdictAI}, so a row marked OK here is
 * a row TurnPrepareTestAI would pass.
 */
public class CheckTurnLanesTest {

	private static final String MUTE = "[MUTE] ";

	private static String profile = "car";

	/**
	 * The same three answers TurnPrepareTestAI gives, and the same reading of what "the same" means.
	 * It is a copy of that test's judgement rather than a shared class: the judgement belongs to the
	 * test, and the pass under test should not carry it.
	 */
	public enum Verdict {
		/** the record names the road but says nothing about it: reached, never judged */
		SKIP, OK, SIMILAR, FAIL
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
			Map<String, String> actual = run(readers, c, where);
			for (Entry<String, String> expected : c.expectedResults.entrySet()) {
				String key = expected.getKey();
				String said = actual == null ? null : actual.get(lookup(key, actual));
				Verdict verdict = verdict(expected.getValue(), said);
				totals.put(verdict, (totals.containsKey(verdict) ? totals.get(verdict) : 0) + 1);
				Row row = new Row(verdict, c.testName, key, link(c), expected.getValue(), said);
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

	private static Map<String, String> run(BinaryMapIndexReader[] readers, Case c,
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
		new TurnPrepareAI().prepareTurnResults(ctx, route);
		Map<String, String> actual = new LinkedHashMap<>();
		for (RouteSegmentResult segment : route) {
			TurnTypeAI turn = segment.getTurnTypeAI();
			if (turn == null) {
				continue;
			}
			long id = ObfConstants.getOsmObjectId(segment.getObject());
			String said = format(turn);
			double[] at = {segment.getStartPoint().getLatitude(), segment.getStartPoint().getLongitude()};
			actual.put(id + ":" + segment.getStartPointIndex(), said);
			where.put(id + ":" + segment.getStartPointIndex(), at);
			if (!actual.containsKey(String.valueOf(id))) {
				actual.put(String.valueOf(id), said);
				where.put(String.valueOf(id), at);
			}
		}
		return actual;
	}

	/** a record naming a start point is answered by that point, one naming only the road by any of it */
	private static String lookup(String key, Map<String, String> actual) {
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
				.append(".bar .bOK{background:#1a7f37}.bar .bSIMILAR{background:#d4a72c}")
				.append(".bar .bFAIL{background:#c0392b}.bar .bSKIP{background:#bbb}\n")
				.append("table{border-collapse:collapse;width:100%}\n")
				.append("th,td{border-bottom:1px solid #e3e3e3;padding:6px 10px;text-align:left;vertical-align:top}\n")
				.append("th{font-weight:600;color:#555;border-bottom:2px solid #ccc;white-space:nowrap}\n")
				.append("td.st{font-weight:600;white-space:nowrap}\n")
				.append(".OK{color:#1a7f37}.SIMILAR{color:#9a6700}.FAIL{color:#c0392b}.SKIP{color:#777}\n")
				.append("td.lanes{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:12.5px;white-space:nowrap}\n")
				.append("td.lanes .a{color:#555}\n")
				.append("td.seg{font-family:ui-monospace,Menlo,monospace;font-size:12.5px;color:#555}\n")
				.append("tr:hover{background:#fafafa}\n")
				.append("</style>\n</head><body>\n");
		sb.append("<h1>Turn lanes: ").append(html(obf.getName())).append("</h1>\n");
		sb.append("<p class=\"sum\">").append(junctions).append(" junctions, ").append(cases)
				.append(" cases, ").append(rows.size()).append(" expectations — ")
				.append(share("OK", totals, Verdict.OK, rows.size()))
				.append(" · ").append(share("SIMILAR", totals, Verdict.SIMILAR, rows.size()))
				.append(" · ").append(share("FAIL", totals, Verdict.FAIL, rows.size()))
				.append(" · ").append(share("SKIP", totals, Verdict.SKIP, rows.size()))
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
		for (Verdict verdict : new Verdict[] {Verdict.OK, Verdict.SIMILAR, Verdict.FAIL, Verdict.SKIP}) {
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

	static Verdict verdict(String expected, String actual) {
		if (expected == null) {
			// null and "" are not the same sentence: null says "this road is on the route" and nothing more, while ""
			// says "and it carries no instruction"
			return Verdict.SKIP;
		}
		if (expected.isEmpty()) {
			// whether a plain "carry on" is worth an instruction at all is a judgement, not a fact: one side shows
			// the lanes and says nothing, the other says nothing at all
			return actual == null ? Verdict.OK : onlyCarriesOn(actual) ? Verdict.SIMILAR : Verdict.FAIL;
		}
		if (actual == null) {
			return onlyCarriesOn(expected) ? Verdict.SIMILAR : Verdict.FAIL;
		}
		if (expected.equals(actual)) {
			return Verdict.OK;
		}
		Parsed e = Parsed.of(expected);
		Parsed a = Parsed.of(actual);
		Verdict worst = Verdict.OK;
		if (e.turn != null) {
			worst = worse(worst, compareTurns(e.turn, a.turn));
		}
		if (e.lanes != null) {
			worst = worse(worst, compareLanes(e.lanes, a.lanes));
		}
		if (worst == Verdict.OK && e.mute != a.mute) {
			worst = Verdict.SIMILAR; // only the voice differs
		}
		return worst;
	}

	/** an instruction that announces nothing: carry on, or keep to one side, on lanes that carry on */
	static boolean onlyCarriesOn(String value) {
		Parsed parsed = Parsed.of(value);
		if (parsed.turn != null && !parsed.turn.equals("C") && !parsed.turn.equals("KL")
				&& !parsed.turn.equals("KR")) {
			return false;
		}
		if (parsed.lanes == null) {
			return true;
		}
		for (LaneShape lane : LaneShape.parse(parsed.lanes)) {
			if (lane.marked != null && !lane.marked.equals("C")) {
				return false;
			}
		}
		return true;
	}

	/**
	 * Neighbours on the left-to-right ladder are one judgement apart: straight and keep right are the same
	 * road read by two people.
	 */
	private static Verdict compareTurns(String expected, String actual) {
		if (expected.equals(actual)) {
			return Verdict.OK;
		}
		if (keepAndTurnOfTheSameSide(expected, actual)) {
			return Verdict.SIMILAR;
		}
		if (roundabout(expected) || roundabout(actual)) {
			return Verdict.FAIL; // another exit is another road, not a neighbour on the ladder
		}
		int e = TurnType.orderFromLeftToRight(TurnType.fromString(expected, false).getValue());
		int a = TurnType.orderFromLeftToRight(TurnType.fromString(actual, false).getValue());
		return Math.abs(e - a) <= 1 ? Verdict.SIMILAR : Verdict.FAIL;
	}

	private static boolean roundabout(String turn) {
		return turn != null && (turn.startsWith("RNDB") || turn.startsWith("RNLB"));
	}

	private static boolean keepAndTurnOfTheSameSide(String one, String other) {
		return pairOf("KL", "TL", one, other) || pairOf("KR", "TR", one, other);
	}

	private static boolean pairOf(String keep, String turn, String one, String other) {
		return (keep.equals(one) && turn.equals(other)) || (keep.equals(other) && turn.equals(one));
	}

	/** The lane structure is a fact of the map and has to match. */
	private static Verdict compareLanes(String expected, String actual) {
		List<LaneShape> e = LaneShape.parse(expected);
		List<LaneShape> a = LaneShape.parse(actual);
		if (e.size() != a.size()) {
			return Verdict.FAIL;
		}
		boolean sameLanes = true;
		boolean similarArrows = false;
		TreeSet<String> markedExpected = new TreeSet<>();
		TreeSet<String> markedActual = new TreeSet<>();
		TreeSet<Integer> activeExpected = new TreeSet<>();
		TreeSet<Integer> activeActual = new TreeSet<>();
		for (int i = 0; i < e.size(); i++) {
			if (!e.get(i).arrows.equals(a.get(i).arrows)) {
				if (!similarArrows(e.get(i).arrows, a.get(i).arrows)) {
					return Verdict.FAIL;
				}
				similarArrows = true;
			}
			sameLanes &= Objects.equals(e.get(i).marked, a.get(i).marked);
			if (e.get(i).marked != null) {
				markedExpected.add(e.get(i).marked);
				activeExpected.add(i);
			}
			if (a.get(i).marked != null) {
				markedActual.add(a.get(i).marked);
				activeActual.add(i);
			}
		}
		if (sameLanes && !similarArrows) {
			return Verdict.OK;
		}
		if (activeExpected.equals(activeActual)) {
			// the same lanes are active and only the arrow drawn as taken inside one of them differs.
			return Verdict.SIMILAR;
		}
		// marking a different NUMBER of lanes that lead the same way is a judgement; marking lanes that lead
		// somewhere else is an error, and a driver following it ends up in the wrong lane
		return markedExpected.equals(markedActual) ? Verdict.SIMILAR : Verdict.FAIL;
	}

	static String reason(String expected, String actual) {
		if (expected == null) {
			return "";
		}
		if (actual == null) {
			return onlyCarriesOn(expected) ? "CARRYON" : "MISSING";
		}
		if (isEmpty(expected)) {
			return onlyCarriesOn(actual) ? "CARRYON" : "EXTRA";
		}
		Parsed e = Parsed.of(expected);
		Parsed a = Parsed.of(actual);
		Verdict turns = e.turn == null ? Verdict.OK : compareTurns(e.turn, a.turn);
		Verdict lanes = e.lanes == null ? Verdict.OK : compareLanes(e.lanes, a.lanes);
		if (turns == Verdict.FAIL) {
			return "TURN " + a.turn;
		}
		if (lanes == Verdict.FAIL) {
			List<LaneShape> expectedLanes = LaneShape.parse(e.lanes);
			List<LaneShape> actualLanes = LaneShape.parse(a.lanes);
			if (expectedLanes.size() != actualLanes.size()) {
				return "LANES";
			}
			for (int i = 0; i < expectedLanes.size(); i++) {
				if (!similarArrows(expectedLanes.get(i).arrows, actualLanes.get(i).arrows)) {
					return "ARROWS"; // the lane carries different arrows, so nothing lines up
				}
			}
			return "MARKS"; // the same lanes, the wrong ones marked
		}
		if (turns == Verdict.SIMILAR) {
			return "TURN+-1";
		}
		if (lanes == Verdict.SIMILAR) {
			return "MARKS";
		}
		return "MUTE";
	}

	/**
	 * A keep in a lane is the arrow of a lane that divides: KL,KR is the lane read as TSLL,C when the left branch bends
	 * off and as C,TSLR when the right one does. Taken left to right, each keep may stand for the straight arrow or the
	 * slight turn of its own side.
	 */
	private static boolean similarArrows(TreeSet<String> expected, TreeSet<String> actual) {
		if (expected.equals(actual)) {
			return true;
		}
		if (expected.size() != actual.size()) {
			return false;
		}
		List<String> e = leftToRight(expected);
		List<String> a = leftToRight(actual);
		for (int i = 0; i < e.size(); i++) {
			if (!e.get(i).equals(a.get(i)) && !keepOfTheSameSide(e.get(i), a.get(i))
					&& !keepOfTheSameSide(a.get(i), e.get(i))) {
				return false;
			}
		}
		return true;
	}

	private static boolean keepOfTheSameSide(String keep, String arrow) {
		return ("KL".equals(keep) && ("TSLL".equals(arrow) || "C".equals(arrow)))
				|| ("KR".equals(keep) && ("C".equals(arrow) || "TSLR".equals(arrow)));
	}

	private static List<String> leftToRight(TreeSet<String> arrows) {
		List<String> sorted = new ArrayList<>(arrows);
		Collections.sort(sorted, new Comparator<String>() {
			@Override
			public int compare(String x, String y) {
				return Integer.compare(TurnType.orderFromLeftToRight(TurnType.fromString(x, false).getValue()),
						TurnType.orderFromLeftToRight(TurnType.fromString(y, false).getValue()));
			}
		});
		return sorted;
	}

	/** an expectation or a produced string, split into the three things it can carry */
	private static final class Parsed {
		final boolean mute;
		final String turn;
		final String lanes;

		private Parsed(boolean mute, String turn, String lanes) {
			this.mute = mute;
			this.turn = turn;
			this.lanes = lanes;
		}

		static Parsed of(String value) {
			boolean mute = value.startsWith(MUTE);
			String body = mute ? value.substring(MUTE.length()) : value;
			int colon = body.indexOf(':');
			if (colon >= 0) {
				return new Parsed(mute, body.substring(0, colon), body.substring(colon + 1));
			}
			boolean looksLikeLanes = body.indexOf('|') >= 0 || body.indexOf('+') >= 0 || body.indexOf(',') >= 0;
			return looksLikeLanes ? new Parsed(mute, null, body) : new Parsed(mute, body, null);
		}
	}

	/** one lane of either format: which arrows it carries, and which of them the route takes */
	private static final class LaneShape {
		final TreeSet<String> arrows = new TreeSet<>();
		String marked;

		static List<LaneShape> parse(String lanes) {
			List<LaneShape> parsed = new ArrayList<>();
			if (lanes == null || lanes.isEmpty()) {
				return parsed;
			}
			for (String lane : lanes.split("\\|", -1)) {
				LaneShape shape = new LaneShape();
				for (String arrow : lane.split(",", -1)) {
					boolean taken = arrow.startsWith("+");
					String code = taken ? arrow.substring(1) : arrow;
					shape.arrows.add(code);
					if (taken) {
						shape.marked = code;
					}
				}
				parsed.add(shape);
			}
			return parsed;
		}
	}

	private static Verdict worse(Verdict a, Verdict b) {
		return a.ordinal() >= b.ordinal() ? a : b;
	}

	private static boolean isEmpty(String s) {
		return s == null || s.isEmpty();
	}

	private static String quote(String s) {
		return s == null ? "NULL" : "'" + s + "'";
	}

	private static String format(TurnTypeAI turn) {
		StringBuilder sb = new StringBuilder();
		if (turn.skipToSpeak()) {
			sb.append(MUTE);
		}
		String maneuver = turn.getOldTurnType().toXmlString();
		sb.append(maneuver);
		String lanes = lanesOf(turn);
		if (!lanes.isEmpty()) {
			sb.append(':').append(lanes);
		}
		return sb.toString();
	}

	/** The lane row. */
	private static String lanesOf(TurnTypeAI turn) {
		LanePanelAI panel = turn.panel();
		if (panel.isEmpty()) {
			return "";
		}
		StringBuilder sb = new StringBuilder();
		for (LanePanelAI.Lane lane : panel.lanes()) {
			if (lane.index() > 0) {
				sb.append('|');
			}
			List<TurnIndication> arrows = lane.arrows();
			if (arrows.isEmpty()) {
				sb.append(lane.isActive() ? "+C" : "C");
				continue;
			}
			for (int i = 0; i < arrows.size(); i++) {
				boolean taken = lane.isActive() && (arrows.get(i) == lane.taken()
						|| (lane.taken() == null && i == 0));
				sb.append(i > 0 ? "," : "").append(taken ? "+" : "").append(code(arrows.get(i)));
			}
		}
		return sb.toString();
	}

	private static String code(TurnIndication arrow) {
		switch (arrow) {
			case LEFT:
				return "TL";
			case SLIGHT_LEFT:
				return "TSLL";
			case SHARP_LEFT:
				return "TSHL";
			case RIGHT:
				return "TR";
			case SLIGHT_RIGHT:
				return "TSLR";
			case SHARP_RIGHT:
				return "TSHR";
			case REVERSE:
				return "TU";
			default:
				return "C";
		}
	}

	private static String shorten(String name) {
		if (name == null) {
			return "";
		}
		String trimmed = name.trim();
		return trimmed.length() <= 10 ? trimmed : trimmed.substring(0, 10);
	}
}
