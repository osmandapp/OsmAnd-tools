package net.osmand.server.controllers.user;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.Temporal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import net.osmand.server.DatasourceConfiguration;
import net.osmand.util.Algorithms;

/** admin pages of the top places photo scoring (LLM runs, duplicates, categories) over the wiki ClickHouse */
@Controller
@RequestMapping("/admin/top-photos")
public class TopPhotosController {

	private static final int PLACES_LIMIT = 200;
	private static final int IMAGES_LIMIT = 35;
	private static final int STAT_LIMIT = 300;
	private static final int DIFF_LIMIT = 200;
	private static final int MIN_ELO = 0;
	private static final String ALL = "All";
	private static final String TOPIC_CATEGORY = "topic-";
	private static final Set<String> SCORE_COLUMNS = Set.of("score", "elo_score", "safe_score", "value_score",
			"reality_score", "technical_score", "overview_score");
	private static final Set<String> DEV_COLUMNS = Set.of("safe", "value", "reality", "technical", "overview");
	private static final DateTimeFormatter CLICKHOUSE_DATETIME = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

	@Autowired
	@Qualifier("wikiJdbcTemplate")
	JdbcTemplate jdbcTemplate;

	@Autowired
	DatasourceConfiguration config;

	// ---------- pages

	@GetMapping
	public String index() {
		return "admin/top-photos/index";
	}

	@GetMapping("/per-place")
	public String perPlace() {
		return "admin/top-photos/per-place";
	}

	@GetMapping("/filter")
	public String filter() {
		return "admin/top-photos/filter";
	}

	@GetMapping("/stat")
	public String stat() {
		return "admin/top-photos/stat";
	}

	@GetMapping("/diff")
	public String diff() {
		return "admin/top-photos/diff";
	}

	// ---------- places (index page)

	@GetMapping(path = "/api/places", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<Object> places(@RequestParam(defaultValue = "") String shortlink,
			@RequestParam(required = false) String category, @RequestParam(required = false) String q,
			@RequestParam(required = false) String isProcessed) {
		List<Object> args = new ArrayList<>();
		args.add(shortlink);
		StringBuilder where = new StringBuilder();
		if (!Algorithms.isEmpty(category) && !"all".equals(category)) {
			if (category.startsWith(TOPIC_CATEGORY)) {
				where.append(" AND topic = ?");
				args.add(Integer.parseInt(category.substring(TOPIC_CATEGORY.length())));
			} else {
				List<String> categories = Arrays.stream(category.split(",")).map(String::trim).toList();
				where.append(" AND hasAny(categories, ").append(placeholders(categories.size())).append(")");
				args.addAll(categories);
			}
		}
		where.append(queryCondition(q, args));
		if ("true".equals(isProcessed)) {
			where.append(" AND E.id IN (SELECT wikidata_id FROM top_images_run)");
		}
		String query = """
				SELECT wikiTitle, id, lat, lon, shortlink, qrank, elo, rounds, rounds_win,
				       if(U.categories != [], U.categories, E.categories) AS categories,
				       if(U.topic != 0, U.topic, E.topic)                 AS topic,
				       osmtype, osmid, osmcnt, poitype, poisubtype
				FROM elo_rating E
				         LEFT JOIN (SELECT placeid,
				                           argMax(categories, updatetime) AS categories,
				                           argMax(topic, updatetime)      AS topic
				                    FROM wiki.top_places_categories_user
				                    GROUP BY placeid) U ON E.id = U.placeid
				WHERE startsWith(shortlink, ?) AND elo > %d%s
				ORDER BY elo DESC
				LIMIT %d""".formatted(MIN_ELO, where, PLACES_LIMIT);
		return json(() -> rows(query, args.toArray()));
	}

	/** +prefix keeps, -prefix drops places by poisubtype, terms are space separated */
	private static String queryCondition(String q, List<Object> args) {
		if (Algorithms.isEmpty(q)) {
			return "";
		}
		List<String> include = new ArrayList<>();
		List<String> exclude = new ArrayList<>();
		for (String term : q.trim().split(" ")) {
			if (term.length() < 2) {
				continue;
			}
			if (term.startsWith("+")) {
				include.add(term.substring(1));
			} else if (term.startsWith("-")) {
				exclude.add(term.substring(1));
			}
		}
		List<String> parts = new ArrayList<>();
		if (!include.isEmpty()) {
			parts.add(String.join(" OR ", Collections.nCopies(include.size(), "poisubtype LIKE ?")));
			include.forEach(t -> args.add(t + "%"));
		}
		if (!exclude.isEmpty()) {
			parts.add(String.join(" AND ", Collections.nCopies(exclude.size(), "poisubtype NOT LIKE ?")));
			exclude.forEach(t -> args.add(t + "%"));
		}
		if (parts.isEmpty()) {
			return "";
		}
		return " AND (" + String.join(" AND ", parts) + ")";
	}

	@GetMapping(path = "/api/categories", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<Object> categories(@RequestParam(defaultValue = "") String shortlink) {
		String query = """
				SELECT category, count() AS count, max(elo) AS max_elo
				FROM (SELECT arrayJoin(if(U.categories != [], U.categories, E.categories)) AS category, elo
				      FROM wiki.elo_rating E
				               LEFT JOIN (SELECT placeid,
				                                 argMax(categories, updatetime) AS categories,
				                                 argMax(topic, updatetime)      AS topic
				                          FROM wiki.top_places_categories_user
				                          GROUP BY placeid) U ON E.id = U.placeid
				      WHERE startsWith(shortlink, ?) AND elo > %d)
				GROUP BY category
				ORDER BY max_elo DESC""".formatted(MIN_ELO);
		return json(() -> Map.of("categories", rows(query, shortlink)));
	}

	@GetMapping(path = "/api/images", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<Object> images(@RequestParam long id) {
		String query = """
				SELECT i.imageTitle,
				       i.score, i.reality_reason, i.reality_score, i.technical_reason, i.technical_score,
				       i.overview_reason, i.overview_score, i.value_reason, i.value_score, i.safe_reason, i.safe_score,
				       arrayStringConcat(i.tags, ', ') AS tags
				FROM top_images_final i
				WHERE i.wikidata_id = ?
				ORDER BY score DESC
				LIMIT %d""".formatted(IMAGES_LIMIT);
		return json(() -> rows(query, id));
	}

	@PostMapping(path = "/api/edit-place", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<Object> editPlace(@RequestParam long placeid, @RequestParam String categories,
			@RequestParam int topic, Authentication auth) {
		List<String> list = Arrays.stream(categories.split(",")).map(String::trim).toList();
		List<Object> args = new ArrayList<>();
		args.add(auth.getName());
		args.add(LocalDateTime.now().format(CLICKHOUSE_DATETIME));
		args.add(placeid);
		args.add(topic);
		args.addAll(list);
		String query = "INSERT INTO wiki.top_places_categories_user (userid, updatetime, placeid, topic, categories) VALUES (?, ?, ?, ?, "
				+ placeholders(list.size()) + ")";
		return json(() -> {
			jdbcTemplate.update(query, args.toArray());
			return Map.of("success", true, "message", "Place updated successfully");
		});
	}

	@PostMapping(path = "/api/ban-image", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<Object> banImage(@RequestParam String imageTitle) {
		String query = "INSERT INTO wiki.blocked_images_pending (imageTitle, blockReason) VALUES (?, 'banned')";
		return json(() -> {
			jdbcTemplate.update(query, imageTitle);
			return Map.of("success", true);
		});
	}

	// ---------- photos of one place (per-place page)

	@GetMapping(path = "/api/run-places", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<Object> runPlaces(@RequestParam long placeId) {
		String query = """
				SELECT wikidata_id, run_id, any(version) AS version, any(wikititle) AS wikititle
				FROM top_images_run
				WHERE wikidata_id = ?
				GROUP BY wikidata_id, run_id
				ORDER BY wikidata_id, run_id DESC""";
		return json(() -> rows(query, placeId));
	}

	@GetMapping(path = "/api/run-images", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<Object> runImages(@RequestParam long runId, @RequestParam long placeId,
			@RequestParam(required = false) String imageTitle, @RequestParam(defaultValue = "false") boolean isFinal) {
		boolean byTitle = !Algorithms.isEmpty(imageTitle) && !"null".equals(imageTitle);
		if (isFinal) {
			String query = """
					SELECT F.*, score AS _score
					FROM top_images_final AS F
					         LEFT ANTI JOIN wiki.blocked_images_pending AS B ON F.imageTitle = B.imageTitle
					WHERE wikidata_id = ?%s
					ORDER BY score DESC""".formatted(byTitle ? " AND F.imageTitle = ?" : "");
			return json(() -> rows(query, byTitle ? new Object[] { placeId, imageTitle } : new Object[] { placeId }));
		}
		String query = """
				SELECT S.*, S.score * 10 AS _score, '' AS mediaId, D._dup_file AS dup_file, D._dup_size AS dup_size,
				       D._dup_sim AS dup_sim, D._image_size AS image_size
				FROM top_images_score AS S
				         LEFT JOIN (SELECT imageTitle, argMax(image_size, dup_sim) AS _image_size, argMax(dup_file, dup_sim) AS _dup_file,
				                           argMax(dup_size, dup_sim) AS _dup_size, max(dup_sim) AS _dup_sim
				                    FROM wiki.top_images_dups ARRAY JOIN
				                         dup_files AS dup_file,
				                         dup_sizes AS dup_size,
				                         similarity AS dup_sim
				                    WHERE wikidata_id = ?
				                    GROUP BY imageTitle) AS D ON S.imageTitle = D.imageTitle
				         LEFT ANTI JOIN (SELECT imageTitle FROM wiki.blocked_images
				                         UNION ALL SELECT imageTitle FROM wiki.blocked_images_pending) AS B ON S.imageTitle = B.imageTitle
				WHERE S.run_id = ? AND S.proc_id = ?%s
				ORDER BY S.score DESC, image_size DESC""".formatted(byTitle ? " AND S.imageTitle = ?" : "");
		return json(() -> rows(query,
				byTitle ? new Object[] { placeId, runId, placeId, imageTitle } : new Object[] { placeId, runId, placeId }));
	}

	@GetMapping(path = "/api/images-dups", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<Object> imagesDups(@RequestParam long placeId) {
		String query = """
				SELECT imageTitle, dup_file, sim
				FROM top_images_dups
				         ARRAY JOIN dup_files AS dup_file, similarity AS sim, dup_sizes
				WHERE wikidata_id = ? AND image_size > dup_sizes
				ORDER BY imageTitle""";
		return json(() -> rows(query, placeId));
	}

	// ---------- statistics over runs (stat page)

	@GetMapping(path = "/api/images-stat-sum", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<Object> imagesStatSum(@RequestParam(required = false) String runSelect) {
		List<Object> args = new ArrayList<>();
		String query = """
				SELECT sum(run_count)               AS run_count_sum,
				       round(sum(reality_dev), 2)   AS reality_dev_sum,
				       round(sum(technical_dev), 2) AS technical_dev_sum,
				       round(sum(overview_dev), 2)  AS overview_dev_sum,
				       round(sum(value_dev), 2)     AS value_dev_sum,
				       round(sum(safe_dev), 2)      AS safe_dev_sum,
				       round(sum(score_dev), 2)     AS score_dev_sum
				FROM (SELECT COUNT(*)                 AS run_count,
				             varSamp(reality_score)   AS reality_dev,
				             varSamp(technical_score) AS technical_dev,
				             varSamp(overview_score)  AS overview_dev,
				             varSamp(value_score)     AS value_dev,
				             varSamp(safe_score)      AS safe_dev,
				             stddevSamp(score)        AS score_dev
				      FROM top_images_score%s
				      GROUP BY imageTitle, proc_id
				      HAVING count(*) > 1)""".formatted(runsCondition(runSelect, args));
		return json(() -> rows(query, args.toArray()));
	}

	@GetMapping(path = "/api/images-stat-throughput", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<Object> imagesStatThroughput(@RequestParam(required = false) String runSelect) {
		List<Object> args = new ArrayList<>();
		String query = "SELECT sum(prompt_tokens) AS input, sum(completion_tokens) AS output FROM top_images_run"
				+ runsCondition(runSelect, args);
		return json(() -> rows(query, args.toArray()));
	}

	@GetMapping(path = "/api/images-stat", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<Object> imagesStat(@RequestParam(required = false) String sortCol,
			@RequestParam(required = false) String runSelect) {
		String order = "stddevSamp(score)";
		if ("elo_score".equals(sortCol)) {
			order = sortCol;
		} else if (sortCol != null && sortCol.endsWith("_dev") && DEV_COLUMNS.contains(sortCol.substring(0, sortCol.length() - 4))) {
			order = "varSamp(" + sortCol.substring(0, sortCol.length() - 4) + "_score)";
		}
		List<Object> args = new ArrayList<>();
		String query = """
				SELECT proc_id                                                       AS wikidata_id,
				       imageTitle,
				       round(elo_rating.elo, 0)                                      AS elo_score,
				       COUNT(*)                                                      AS run_count,
				       round(varSamp(reality_score), 2)                              AS reality_dev,
				       arrayStringConcat(groupArray(toString(reality_score)), ',')   AS reality_scores,
				       round(varSamp(technical_score), 2)                            AS technical_dev,
				       arrayStringConcat(groupArray(toString(technical_score)), ',') AS technical_scores,
				       round(varSamp(overview_score), 2)                             AS overview_dev,
				       arrayStringConcat(groupArray(toString(overview_score)), ',')  AS overview_scores,
				       round(varSamp(value_score), 2)                                AS value_dev,
				       arrayStringConcat(groupArray(toString(value_score)), ',')     AS value_scores,
				       round(varSamp(safe_score), 2)                                 AS safe_dev,
				       arrayStringConcat(groupArray(toString(safe_score)), ',')      AS safe_scores,
				       round(stddevSamp(score), 2)                                   AS score_dev,
				       arrayStringConcat(groupArray(toString(score)), ',')           AS scores
				FROM top_images_score
				         INNER JOIN elo_rating ON elo_rating.id = proc_id%s
				GROUP BY imageTitle, proc_id, elo_rating.elo
				ORDER BY %s DESC
				LIMIT %d""".formatted(runsCondition(runSelect, args), order, STAT_LIMIT);
		return json(() -> rows(query, args.toArray()));
	}

	/** run ids as "1,2,3", empty or All keeps every run */
	private static String runsCondition(String runSelect, List<Object> args) {
		if (Algorithms.isEmpty(runSelect) || ALL.equals(runSelect)) {
			return "";
		}
		List<Long> runs = Arrays.stream(runSelect.split(",")).map(String::trim).map(Long::parseLong).toList();
		args.addAll(runs);
		return " WHERE run_id IN (" + String.join(", ", Collections.nCopies(runs.size(), "?")) + ")";
	}

	// ---------- photos across places (filter page)

	@GetMapping(path = "/api/images-filter", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<Object> imagesFilter(@RequestParam(defaultValue = "score") String sortCol,
			@RequestParam(defaultValue = "true") String isAsc, @RequestParam(required = false) String tagFilter,
			@RequestParam(required = false) String quidFilter, @RequestParam(required = false) String placeFilter,
			@RequestParam(required = false) String runFilter) {
		if (!SCORE_COLUMNS.contains(sortCol)) {
			return ResponseEntity.badRequest().body(Map.of("error", "Unknown sort column: " + sortCol));
		}
		List<Object> args = new ArrayList<>();
		String where = tagCondition("top_images_score.tags", tagFilter, args) + quidCondition(quidFilter, args)
				+ idCondition("proc_id", placeFilter, args) + idCondition("run_id", runFilter, args);
		String query = """
				SELECT run_id,
				       proc_id                       AS wikidata_id,
				       shortlink,
				       imageTitle,
				       round(elo_rating.elo, 2)      AS elo_score,
				       reality_reason, reality_score, technical_reason, technical_score, overview_reason, overview_score,
				       value_reason, value_score, safe_reason, safe_score, score,
				       arrayStringConcat(tags, ', ') AS tags
				FROM top_images_score
				         INNER JOIN elo_rating ON elo_rating.id = proc_id
				WHERE 0 = 0%s
				ORDER BY %s %s, elo_rating.elo DESC
				LIMIT %d""".formatted(where, sortCol, "true".equals(isAsc) ? "DESC" : "ASC", PLACES_LIMIT);
		return json(() -> rows(query, args.toArray()));
	}

	@GetMapping(path = "/api/filter-tags", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<Object> filterTags(@RequestParam(required = false) String tagFilter,
			@RequestParam(required = false) String placeFilter, @RequestParam(required = false) String runFilter) {
		List<Object> args = new ArrayList<>();
		String where = tagCondition("tags", tagFilter, args) + idCondition("proc_id", placeFilter, args)
				+ idCondition("run_id", runFilter, args);
		String query = """
				SELECT name, count() AS cnt
				FROM top_images_score
				         ARRAY JOIN tags AS name
				WHERE 0 = 0%s
				GROUP BY name
				ORDER BY name""".formatted(where);
		return json(() -> rows(query, args.toArray()));
	}

	@GetMapping(path = "/api/filter-runs", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<Object> filterRuns(@RequestParam(required = false) String quidFilter,
			@RequestParam(required = false) String placeFilter, @RequestParam(required = false) String runFilter) {
		List<Object> args = new ArrayList<>();
		String where = quidCondition(quidFilter, args) + idCondition("wikidata_id", placeFilter, args)
				+ idCondition("run_id", runFilter, args);
		String query = """
				SELECT run_id AS name, count() AS cnt
				FROM top_images_run
				         INNER JOIN elo_rating ON elo_rating.id = wikidata_id
				WHERE 0 = 0%s
				GROUP BY run_id
				ORDER BY run_id DESC""".formatted(where);
		return json(() -> rows(query, args.toArray()));
	}

	@GetMapping(path = "/api/filter-places", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<Object> filterPlaces(@RequestParam(required = false) String tagFilter,
			@RequestParam(required = false) String quidFilter, @RequestParam(required = false) String placeFilter,
			@RequestParam(required = false) String runFilter) {
		List<Object> args = new ArrayList<>();
		String where = quidCondition(quidFilter, args) + tagCondition("tags", tagFilter, args)
				+ idCondition("proc_id", placeFilter, args) + idCondition("run_id", runFilter, args);
		String query = """
				SELECT proc_id AS name, count() AS cnt
				FROM top_images_score
				         INNER JOIN elo_rating ON elo_rating.id = proc_id
				WHERE 0 = 0%s
				GROUP BY proc_id
				ORDER BY proc_id""".formatted(where);
		return json(() -> rows(query, args.toArray()));
	}

	private static boolean isFilterSet(String value) {
		return !Algorithms.isEmpty(value) && !ALL.equals(value);
	}

	private static String tagCondition(String column, String tagFilter, List<Object> args) {
		if (!isFilterSet(tagFilter)) {
			return "";
		}
		args.add(tagFilter);
		return " AND has(" + column + ", ?)";
	}

	private static String quidCondition(String quidFilter, List<Object> args) {
		if (!isFilterSet(quidFilter)) {
			return "";
		}
		args.add(quidFilter);
		return " AND startsWith(elo_rating.shortlink, ?)";
	}

	private static String idCondition(String column, String idFilter, List<Object> args) {
		if (!isFilterSet(idFilter)) {
			return "";
		}
		args.add(Long.parseLong(idFilter.trim()));
		return " AND " + column + " = ?";
	}

	// ---------- near duplicates with different scores (diff page)

	@GetMapping(path = "/api/images-diff", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<Object> imagesDiff() {
		String query = """
				SELECT d.wikidata_id                 AS place_id,
				       left.imageTitle               AS left_photo,
				       left.score                    AS left_score,
				       right.imageTitle              AS right_photo,
				       right.score                   AS right_score,
				       round(sim, 2)                 AS similarity,
				       abs(left.score - right.score) AS difference,
				       left.*,
				       right.*
				FROM (SELECT wikidata_id, imageTitle, dup_file, sim
				      FROM top_images_dups ARRAY JOIN dup_files AS dup_file, similarity AS sim
				      WHERE sim >= 0.99) AS d
				         JOIN top_images_score AS left ON left.imageTitle = d.imageTitle
				         JOIN top_images_score AS right ON right.imageTitle = d.dup_file
				ORDER BY abs(left.score - right.score) DESC
				LIMIT %d""".formatted(DIFF_LIMIT);
		return json(() -> rows(query));
	}

	// ---------- helpers

	private ResponseEntity<Object> json(Supplier<Object> body) {
		if (!config.wikiInitialized()) {
			return notInitialized();
		}
		return ResponseEntity.ok(body.get());
	}

	private static ResponseEntity<Object> notInitialized() {
		return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(Map.of("error", "Wiki datasource is not initialized"));
	}

	private List<Map<String, Object>> rows(String query, Object... args) {
		return jdbcTemplate.query(query, (rs, rowNum) -> row(rs), args);
	}

	/** column label to value, the same JSON the node client produced: plain numbers, arrays, "yyyy-MM-dd HH:mm:ss" dates */
	private static Map<String, Object> row(ResultSet rs) throws SQLException {
		ResultSetMetaData md = rs.getMetaData();
		Map<String, Object> row = new LinkedHashMap<>();
		for (int i = 1; i <= md.getColumnCount(); i++) {
			row.put(md.getColumnLabel(i), jsonValue(rs.getObject(i)));
		}
		return row;
	}

	private static Object jsonValue(Object value) throws SQLException {
		if (value instanceof java.sql.Array array) {
			return jsonValue(array.getArray());
		}
		if (value instanceof Object[] items) {
			Object[] converted = new Object[items.length];
			for (int i = 0; i < items.length; i++) {
				converted[i] = jsonValue(items[i]);
			}
			return converted;
		}
		if (value instanceof LocalDateTime dateTime) {
			return dateTime.format(CLICKHOUSE_DATETIME);
		}
		if (value instanceof Temporal || value instanceof java.util.Date) {
			return value.toString();
		}
		if (value instanceof Number number && !(value instanceof Integer || value instanceof Long || value instanceof Double
				|| value instanceof Float || value instanceof BigDecimal)) {
			// ClickHouse UInt64 comes as its own Number class
			return new BigDecimal(number.toString());
		}
		return value;
	}

	private static String placeholders(int count) {
		return "[" + String.join(", ", Collections.nCopies(count, "?")) + "]";
	}
}
