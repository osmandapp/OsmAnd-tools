package net.osmand.server.api.services;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import net.osmand.data.LatLon;
import net.osmand.server.api.services.TurnLanesCompareService.Compare;
import net.osmand.util.Algorithms;

/**
 * Manual check of a compare: a person looks at an instruction on the map and says RED (it got worse), YELLOW (it
 * changed, neither better nor worse) or GREEN (it got fixed). The answers of a compare are kept beside it in
 * review.csv.
 *
 * What a person has said the lanes there should be goes to common.csv, shared by every dataset and compare: the
 * turn_lanes typed in, whatever the verdict, or the checker's own answer when it was called GREEN. A drive is the same
 * drive in any dataset, so the same start, end and segment find it again.
 */
@Service
public class TurnLanesReviewService {

	public static final String REVIEW = "review.csv";
	public static final String COMMON = "common.csv";
	public static final String[] REVIEW_HEADER = {"num", "start", "end", "segment", "status", "expected", "actual",
			"verdict", "turn_lanes", "note", "user", "time"};
	public static final String[] COMMON_HEADER = {"start", "end", "segment", "turn_lanes", "note", "obf", "left_side",
			"dataset", "time"};
	public static final Set<String> VERDICTS = Set.of("RED", "YELLOW", "GREEN");

	public static class VerdictRequest {
		public String num;
		public String segment;
		public String verdict;
		public String turnLanes;
		public String note;
	}

	/** a drive added by hand on the manual check: one instruction, its actual taken to be the expected */
	public static class NewCaseRequest {
		public String start;
		public String end;
		public String segment;
		public String expected;
		public String status;
		public String verdict;
		public String obf;
		public boolean leftSide;
	}

	private static final java.util.regex.Pattern SEGMENT = java.util.regex.Pattern.compile("\\d+(:\\d+)?");
	private static final String NEW_CASE_NAME = "Manual case";

	@Autowired
	private TurnLanesService lanes;

	@Autowired
	private TurnLanesCompareService compares;

	/** start|end|segment -> its common.csv row; read once, written through */
	private Map<String, Map<String, String>> common;

	private static String rowKey(String num, String segment) {
		return num + "|" + segment;
	}

	private static String commonKey(String start, String end, String segment) {
		return start + "|" + end + "|" + segment;
	}

	private File reviewFile(String dataset, String id) {
		return new File(compares.getResultFile(dataset, id).getParentFile(), REVIEW);
	}

	public File getCommonFile() {
		return new File(lanes.getRoot(), COMMON);
	}

	public File getReviewFile(String dataset, String id) {
		if (compares.getResultFile(dataset, id) == null) {
			return null;
		}
		File f = reviewFile(dataset, id);
		return f.exists() ? f : null;
	}

	private Map<String, Map<String, String>> common() throws IOException {
		if (common == null) {
			Map<String, Map<String, String>> m = new LinkedHashMap<>();
			for (Map<String, String> row : TurnLanesFiles.readCsv(getCommonFile())) {
				m.put(commonKey(row.get("start"), row.get("end"), row.get("segment")), row);
			}
			common = m;
		}
		return common;
	}

	private Map<String, Map<String, String>> review(String dataset, String id) throws IOException {
		Map<String, Map<String, String>> m = new LinkedHashMap<>();
		for (Map<String, String> row : TurnLanesFiles.readCsv(reviewFile(dataset, id))) {
			m.put(rowKey(row.get("num"), row.get("segment")), row);
		}
		return m;
	}

	/** puts the verdict of each row of the compare that has one, RED, YELLOW or GREEN, into it as "verdict" */
	public synchronized void addVerdicts(String dataset, String id, List<Map<String, String>> rows) throws IOException {
		if (compares.getResultFile(dataset, id) == null) {
			return;
		}
		Map<String, Map<String, String>> review = review(dataset, id);
		for (Map<String, String> r : rows) {
			Map<String, String> v = review.get(rowKey(r.get("num"), r.get("segment")));
			if (v != null) {
				r.put("verdict", v.get("verdict"));
			}
		}
	}

	/**
	 * Every row of the compare with what was said about it: its verdict here, and what common.csv says the lanes
	 * there are. The page filters them itself, there are a few thousand at most.
	 */
	public synchronized List<Map<String, Object>> rows(String dataset, String id) throws IOException {
		if (compares.getResultFile(dataset, id) == null) {
			return null;
		}
		Map<String, Map<String, String>> review = review(dataset, id);
		Map<String, Map<String, String>> common = common();
		List<Map<String, Object>> out = new ArrayList<>();
		for (Map<String, String> r : compares.readRows(dataset, id, Set.of(), Integer.MAX_VALUE)) {
			Map<String, Object> row = new LinkedHashMap<>(r);
			Map<String, String> v = review.get(rowKey(r.get("num"), r.get("segment")));
			if (v != null) {
				row.put("verdict", v.get("verdict"));
				row.put("turn_lanes", v.get("turn_lanes"));
				row.put("review_note", v.get("note"));
				row.put("user", v.get("user"));
				row.put("time", v.get("time"));
			}
			row.put("common", common.get(commonKey(r.get("start"), r.get("end"), r.get("segment"))));
			out.add(row);
		}
		return out;
	}

	/**
	 * What the turn_lanes field shows before anyone types: the checker's answer - nothing, when it gives no
	 * instruction there - or the generator's, when the checker did not drive there at all (ERROR).
	 */
	private static String fieldDefault(Map<String, String> row) {
		if ("ERROR".equals(row.get("status"))) {
			return row.get("expected");
		}
		return row.get("actual") == null ? "" : row.get("actual");
	}

	/**
	 * Records a verdict on one row of the compare, and in common.csv what the lanes there should be - when the
	 * person typed them, or called the checker's answer GREEN.
	 *
	 * @return the counts of the compare's verdicts after this one
	 */
	public synchronized Map<String, Object> verdict(String dataset, String id, VerdictRequest req, String user)
			throws IOException {
		Compare c = compares.get(dataset, id);
		if (c == null || compares.getResultFile(dataset, id) == null) {
			throw new IllegalArgumentException("No compare " + dataset + "/" + id);
		}
		if (compares.isRunning(dataset, id)) {
			throw new IllegalArgumentException("Compare " + id + " is being recalculated");
		}
		if (req.verdict == null || !VERDICTS.contains(req.verdict)) {
			throw new IllegalArgumentException("Verdict: one of " + VERDICTS);
		}
		Map<String, String> row = null;
		Map<String, Map<String, String>> drives = new LinkedHashMap<>();
		for (Map<String, String> r : compares.readRows(dataset, id, Set.of(), Integer.MAX_VALUE)) {
			drives.putIfAbsent(r.get("num"), r);
			if (r.get("num").equals(req.num) && r.get("segment").equals(req.segment)) {
				row = r;
			}
		}
		if (row == null) {
			throw new IllegalArgumentException("No row " + req.num + " / " + req.segment);
		}
		String now = java.time.Instant.now().toString();
		String typed = req.turnLanes == null ? "" : req.turnLanes.trim();
		String note = req.note == null ? "" : req.note.trim();

		Map<String, Map<String, String>> common = common();
		String ck = commonKey(row.get("start"), row.get("end"), row.get("segment"));
		Map<String, String> had = common.get(ck);
		// what the field showed before anyone touched it: a typed value is one that differs from this
		String shown = had != null ? had.get("turn_lanes") : fieldDefault(row);
		boolean typedIn = !typed.isEmpty() && !typed.equals(shown);
		String lanesValue = typedIn ? typed : "GREEN".equals(req.verdict) ? row.get("actual") : null;
		if (lanesValue != null && !lanesValue.isEmpty()) {
			Map<String, String> cr = new LinkedHashMap<>();
			cr.put("start", row.get("start"));
			cr.put("end", row.get("end"));
			cr.put("segment", row.get("segment"));
			cr.put("turn_lanes", lanesValue);
			cr.put("note", note);
			cr.put("obf", row.get("obf"));
			cr.put("left_side", row.get("left_side"));
			cr.put("dataset", dataset);
			cr.put("time", now);
			common.put(ck, cr);
			TurnLanesFiles.writeCsv(getCommonFile(), COMMON_HEADER, common.values());
		}

		Map<String, Map<String, String>> review = review(dataset, id);
		Map<String, String> before = review.get(rowKey(row.get("num"), row.get("segment")));
		// GREEN taken back, the field left as it was: what that GREEN put in common.csv goes with it
		if (lanesValue == null && had != null && before != null && "GREEN".equals(before.get("verdict"))
				&& Algorithms.objectEquals(had.get("turn_lanes"), row.get("actual"))) {
			common.remove(ck);
			TurnLanesFiles.writeCsv(getCommonFile(), COMMON_HEADER, common.values());
		}
		Map<String, String> v = new LinkedHashMap<>();
		v.put("num", row.get("num"));
		v.put("start", row.get("start"));
		v.put("end", row.get("end"));
		v.put("segment", row.get("segment"));
		v.put("status", row.get("status"));
		v.put("expected", row.get("expected"));
		v.put("actual", row.get("actual"));
		v.put("verdict", req.verdict);
		// what this verdict says the lanes are, when it says anything: typed in, or the checker's own under GREEN
		v.put("turn_lanes", Algorithms.isEmpty(lanesValue) ? "" : lanesValue);
		v.put("note", note);
		v.put("user", user);
		v.put("time", now);
		review.put(rowKey(row.get("num"), row.get("segment")), v);
		// rows saved before start and end were written get them from the compare
		for (Map<String, String> r : review.values()) {
			Map<String, String> drive = drives.get(r.get("num"));
			if (Algorithms.isEmpty(r.get("start")) && drive != null) {
				r.put("start", drive.get("start"));
				r.put("end", drive.get("end"));
			}
		}
		TurnLanesFiles.writeCsv(reviewFile(dataset, id), REVIEW_HEADER, review.values());

		Map<String, Integer> counts = new LinkedHashMap<>();
		for (Map<String, String> r : review.values()) {
			counts.merge(r.get("verdict"), 1, Integer::sum);
		}
		compares.setReviewed(dataset, id, counts);
		Map<String, Object> out = new LinkedHashMap<>();
		out.put("counts", counts);
		out.put("common", common.get(ck));
		out.put("turn_lanes", v.get("turn_lanes"));
		out.put("time", now);
		out.put("user", user);
		return out;
	}

	/**
	 * After a recalc: a verdict stays where the compare says there what it said when the verdict was given - the same
	 * status and the same answer of the checker - and goes where that changed. common.csv is left as it is: what the
	 * lanes there should be does not depend on the build.
	 */
	public synchronized void keepUnchanged(Compare c) {
		try {
			Map<String, Map<String, String>> review = review(c.dataset, c.id);
			if (review.isEmpty()) {
				compares.setRecalc(c.dataset, c.id, null, null);
				return;
			}
			Map<String, Map<String, String>> now = new LinkedHashMap<>();
			for (Map<String, String> r : compares.readRows(c.dataset, c.id, Set.of(), Integer.MAX_VALUE)) {
				now.put(rowKey(r.get("num"), r.get("segment")), r);
			}
			Map<String, Map<String, String>> kept = new LinkedHashMap<>();
			for (Map.Entry<String, Map<String, String>> e : review.entrySet()) {
				Map<String, String> r = now.get(e.getKey());
				Map<String, String> v = e.getValue();
				if (r != null && Algorithms.objectEquals(r.get("status"), v.get("status"))
						&& Algorithms.objectEquals(nullToEmpty(r.get("actual")), nullToEmpty(v.get("actual")))) {
					kept.put(e.getKey(), v);
				}
			}
			TurnLanesFiles.writeCsv(reviewFile(c.dataset, c.id), REVIEW_HEADER, kept.values());
			Map<String, Integer> counts = new LinkedHashMap<>();
			for (Map<String, String> r : kept.values()) {
				counts.merge(r.get("verdict"), 1, Integer::sum);
			}
			compares.setRecalc(c.dataset, c.id, counts, "kept " + kept.size() + " of " + review.size() + " verdicts");
		} catch (IOException e) {
			throw new IllegalStateException("Cannot update the review of " + c.dataset + "/" + c.id, e);
		}
	}

	/**
	 * Adds a drive by hand to the compare's dataset and to the compare, nothing routed: one row, actual = expected,
	 * the status as given, then the verdict on it as any other.
	 *
	 * @return the drive's num and the counts of the compare's verdicts after it
	 */
	public synchronized Map<String, Object> newCase(String dataset, String id, NewCaseRequest req, String user)
			throws IOException {
		Compare c = compares.get(dataset, id);
		if (c == null || compares.getResultFile(dataset, id) == null || compares.isRunning(dataset, id)) {
			throw new IllegalArgumentException("No finished compare " + dataset + "/" + id);
		}
		String start = point(req.start, "Start");
		String end = point(req.end, "End");
		String segment = req.segment == null ? "" : req.segment.trim();
		if (!SEGMENT.matcher(segment).matches()) {
			throw new IllegalArgumentException("Segment: a way id, or id:point");
		}
		String expected = req.expected == null ? "" : req.expected.trim();
		if (expected.isEmpty()) {
			throw new IllegalArgumentException("Expected is empty");
		}
		if (!"DIFF".equals(req.status) && !"SAME".equals(req.status)) {
			throw new IllegalArgumentException("Status: DIFF or SAME");
		}
		if (req.verdict == null || !VERDICTS.contains(req.verdict)) {
			throw new IllegalArgumentException("Verdict: one of " + VERDICTS);
		}
		String obf = req.obf == null ? "" : req.obf.trim();
		if (!TurnLanesService.NAME.matcher(obf).matches()) {
			throw new IllegalArgumentException("No map");
		}
		String leftSide = String.valueOf(req.leftSide);
		int num = lanes.addCase(dataset, NEW_CASE_NAME, start, end, req.leftSide, obf, Map.of(segment, expected));
		compares.addRows(dataset, id, List.<String[]>of(new String[] {String.valueOf(num), NEW_CASE_NAME, start, end,
				segment, expected, expected, req.status, "", leftSide, obf, ""}));
		VerdictRequest v = new VerdictRequest();
		v.num = String.valueOf(num);
		v.segment = segment;
		v.verdict = req.verdict;
		Map<String, Object> out = new LinkedHashMap<>(verdict(dataset, id, v, user));
		out.put("num", v.num);
		out.put("segment", segment);
		return out;
	}

	/** "lat,lon" as the datasets write it, six digits */
	private static String point(String s, String what) {
		LatLon ll;
		try {
			ll = TurnLanesFiles.latLon(s == null ? "" : s.trim());
		} catch (RuntimeException e) {
			throw new IllegalArgumentException(what + ": lat,lon");
		}
		return String.format(java.util.Locale.US, "%.6f,%.6f", ll.getLatitude(), ll.getLongitude());
	}

	private static String nullToEmpty(String s) {
		return s == null ? "" : s;
	}

	/**
	 * The lanes a test of this row should expect, from what was said about it: common.csv first, then the
	 * turn_lanes typed into this review, then the verdict - GREEN and YELLOW keep the checker's answer, RED the
	 * generator's - and with nothing said, what the compare saw there.
	 */
	@SuppressWarnings("unchecked")
	static String expectation(Map<String, Object> row) {
		Map<String, String> common = (Map<String, String>) row.get("common");
		if (common != null && !Algorithms.isEmpty(common.get("turn_lanes"))) {
			return common.get("turn_lanes");
		}
		String expected = (String) row.get("expected");
		String actual = (String) row.get("actual");
		String verdict = (String) row.get("verdict");
		String typed = (String) row.get("turn_lanes");
		if (verdict != null && !Algorithms.isEmpty(typed)) {
			return typed;
		}
		if ("RED".equals(verdict)) {
			return expected;
		}
		if (verdict != null) {
			// GREEN or YELLOW: the checker's answer, nothing at all when it gave none
			return actual == null ? "" : actual;
		}
		return Algorithms.isEmpty(actual) ? expected : actual;
	}

	/** one drive of the compare as a test case, in the form test_turn_lanes.json has: a list of one */
	public Map<String, Object> caseJson(String dataset, String id, String num) throws IOException {
		List<Map<String, Object>> all = rows(dataset, id);
		if (all == null) {
			throw new IllegalArgumentException("No compare " + dataset + "/" + id);
		}
		List<Map<String, Object>> drive = new ArrayList<>();
		for (Map<String, Object> r : all) {
			if (num.equals(r.get("num"))) {
				drive.add(r);
			}
		}
		if (drive.isEmpty()) {
			throw new IllegalArgumentException("No drive " + num);
		}
		Map<String, String> expected = new LinkedHashMap<>();
		for (Map<String, Object> r : drive) {
			String e = expectation(r);
			// an instruction that is not to be given has no place in the test
			if (!Algorithms.isEmpty(e)) {
				expected.put((String) r.get("segment"), e);
			}
		}
		Map<String, Object> first = drive.get(0);
		Map<String, Object> entry = new LinkedHashMap<>();
		entry.put("testName", first.get("name") + " #" + num);
		entry.put("startPoint", point((String) first.get("start")));
		entry.put("endPoint", point((String) first.get("end")));
		Map<String, String> params = new LinkedHashMap<>();
		params.put("map", (String) first.get("obf"));
		if ("true".equals(first.get("left_side"))) {
			params.put("leftSide", "true");
		}
		entry.put("params", params);
		entry.put("expectedResults", expected);
		return Map.of("json", List.of(entry));
	}

	/** a point as test_turn_lanes.json writes it */
	private static Map<String, Object> point(String p) {
		LatLon ll = TurnLanesFiles.latLon(p);
		Map<String, Object> m = new LinkedHashMap<>();
		m.put("latitude", ll.getLatitude());
		m.put("longitude", ll.getLongitude());
		return m;
	}
}
