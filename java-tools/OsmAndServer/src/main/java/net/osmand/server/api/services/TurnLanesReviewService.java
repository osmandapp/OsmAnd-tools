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
	public static final String[] REVIEW_HEADER = {"num", "segment", "status", "expected", "actual", "verdict",
			"turn_lanes", "note", "user", "time"};
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
		if (req.verdict == null || !VERDICTS.contains(req.verdict)) {
			throw new IllegalArgumentException("Verdict: one of " + VERDICTS);
		}
		Map<String, String> row = null;
		for (Map<String, String> r : compares.readRows(dataset, id, Set.of(), Integer.MAX_VALUE)) {
			if (r.get("num").equals(req.num) && r.get("segment").equals(req.segment)) {
				row = r;
				break;
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

	/**
	 * One drive of the compare as a test case, in the form test_turn_lanes.json has: a list of one. With other
	 * points than the dataset's - moved on the map to make the drive shorter - it is routed again by the compare's
	 * build, and every instruction of the new route is expected; the ones a person said something about as they
	 * said it.
	 */
	public Map<String, Object> caseJson(String dataset, String id, String num, String start, String end)
			throws IOException {
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
		Map<String, Object> first = drive.get(0);
		// written as the dataset writes points, so that the same point is the same string
		String from = Algorithms.isEmpty(start) ? (String) first.get("start")
				: TurnLanesFiles.format(TurnLanesFiles.latLon(start));
		String to = Algorithms.isEmpty(end) ? (String) first.get("end") : TurnLanesFiles.format(TurnLanesFiles.latLon(end));
		boolean moved = !from.equals(first.get("start")) || !to.equals(first.get("end"));
		List<String> warnings = new ArrayList<>();
		Map<String, String> expected = new LinkedHashMap<>();
		if (!moved) {
			for (Map<String, Object> r : drive) {
				String e = expectation(r);
				// an instruction that is not to be given has no place in the test
				if (!Algorithms.isEmpty(e)) {
					expected.put((String) r.get("segment"), e);
				}
			}
		} else {
			Map<String, String> routed = compares.routeOnce(dataset, id, (String) first.get("obf"), from, to,
					(String) first.get("left_side"));
			Map<String, Map<String, Object>> bySegment = new LinkedHashMap<>();
			Map<String, String> asKeys = new LinkedHashMap<>();
			for (Map<String, Object> r : drive) {
				bySegment.put((String) r.get("segment"), r);
				asKeys.put((String) r.get("segment"), "");
			}
			int kept = 0;
			for (Map.Entry<String, String> e : routed.entrySet()) {
				// the new route keys a road by its id alone when it carries one instruction: find the row either way
				String segment = TurnLanesCompareService.findSegment(asKeys, e.getKey());
				Map<String, Object> r = segment == null ? null : bySegment.get(segment);
				if (r != null) {
					kept++;
				}
				String value = r == null ? e.getValue() : expectation(r);
				if (!Algorithms.isEmpty(value)) {
					expected.put(e.getKey(), value);
				}
			}
			if (kept < drive.size()) {
				warnings.add((drive.size() - kept) + " of " + drive.size()
						+ " checked instructions are not on the moved route");
			}
		}
		Map<String, Object> entry = new LinkedHashMap<>();
		entry.put("testName", first.get("name") + " #" + num);
		entry.put("startPoint", point(from));
		entry.put("endPoint", point(to));
		Map<String, String> params = new LinkedHashMap<>();
		params.put("map", (String) first.get("obf"));
		if ("true".equals(first.get("left_side"))) {
			params.put("leftSide", "true");
		}
		entry.put("params", params);
		entry.put("expectedResults", expected);
		Map<String, Object> out = new LinkedHashMap<>();
		out.put("json", List.of(entry));
		out.put("moved", moved);
		out.put("warnings", warnings);
		return out;
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
