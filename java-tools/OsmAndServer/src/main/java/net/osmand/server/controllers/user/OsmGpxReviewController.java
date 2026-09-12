package net.osmand.server.controllers.user;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import jakarta.servlet.http.HttpServletResponse;
import net.osmand.server.DatasourceConfiguration;

/**
 * Manual review of OsmGpx tracks from the heatmap page: one verdict per track in osm_gpx_data.manual_review, and a
 * csv.gz export of the reviewed tracks with their labels, track_stats and geometry. Admin only (/admin/**).
 */
@RestController
@RequestMapping("/admin/osmgpx")
public class OsmGpxReviewController {

	private static final Log LOG = LogFactory.getLog(OsmGpxReviewController.class);
	private static final String TABLE = "osm_gpx_data";
	private static final Set<String> VERDICTS = Set.of("ok", "wrong_activity", "garbage", "simulated", "bad_line");
	private static final int MAX_COMMENT = 2000;
	private static final int MAX_IDS = 1000;
	private static final int EXPORT_BATCH = 500;
	private static final String[] EXPORT_COLUMNS = {"id", "user", "date", "name", "description", "tags", "lat", "lon", "activity",
			"activity_source", "file_activity", "speed_matches_activity", "speed", "max_speed", "distance", "points", "time_minutes",
			"manual_review", "track_stats", "geometry_b64"};

	@Autowired
	@Qualifier("osmgpxJdbcTemplate")
	JdbcTemplate jdbcTemplate;

	@Autowired
	DatasourceConfiguration config;

	private final Gson gson = new Gson();

	public record ReviewRequest(Long id, String verdict, String activity, String comment) {
	}

	/** saves the verdict of one track; an empty verdict removes it */
	@PostMapping(path = "/review", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> review(@RequestBody ReviewRequest req, Authentication auth) {
		if (!config.osmgpxInitialized()) {
			return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body("OsmGpx datasource is not initialized");
		}
		if (req.id() == null) {
			return ResponseEntity.badRequest().body("id is required");
		}
		String json = null;
		if (!isBlank(req.verdict())) {
			if (!VERDICTS.contains(req.verdict())) {
				return ResponseEntity.badRequest().body("verdict must be one of " + VERDICTS);
			}
			if ("wrong_activity".equals(req.verdict()) && isBlank(req.activity())) {
				return ResponseEntity.badRequest().body("wrong_activity needs the right activity");
			}
			JsonObject review = new JsonObject();
			review.addProperty("verdict", req.verdict());
			if (!isBlank(req.activity())) {
				review.addProperty("activity", req.activity().trim());
			}
			if (!isBlank(req.comment())) {
				String comment = req.comment().trim();
				review.addProperty("comment", comment.length() > MAX_COMMENT ? comment.substring(0, MAX_COMMENT) : comment);
			}
			review.addProperty("author", auth == null ? null : auth.getName());
			review.addProperty("time", Instant.now().toString());
			json = gson.toJson(review);
		}
		int updated = jdbcTemplate.update("UPDATE " + TABLE + " SET manual_review = ?::json WHERE id = ?", json, req.id());
		if (updated == 0) {
			return ResponseEntity.notFound().build();
		}
		return ResponseEntity.ok(json == null ? "{}" : json);
	}

	/** verdicts of the given tracks, by id; tracks without one are left out */
	@GetMapping(path = "/reviews", produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> reviews(@RequestParam List<Long> ids) {
		if (!config.osmgpxInitialized()) {
			return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body("OsmGpx datasource is not initialized");
		}
		if (ids.isEmpty() || ids.size() > MAX_IDS) {
			return ResponseEntity.badRequest().body("1 to " + MAX_IDS + " ids");
		}
		Map<Long, Object> res = new LinkedHashMap<>();
		String in = String.join(",", Collections.nCopies(ids.size(), "?"));
		jdbcTemplate.query("SELECT id, manual_review::text FROM " + TABLE + " WHERE manual_review IS NOT NULL AND id IN (" + in + ")",
				(RowCallbackHandler) rs -> res.put(rs.getLong(1), gson.fromJson(rs.getString(2), Object.class)), ids.toArray());
		return ResponseEntity.ok(gson.toJson(res));
	}

	/** every reviewed track as one csv row, gzip; gpx=true adds the original GPX file (gzip, base64) */
	@GetMapping(path = "/reviews.csv.gz")
	public void export(@RequestParam(defaultValue = "false") boolean gpx, HttpServletResponse response) throws IOException {
		if (!config.osmgpxInitialized()) {
			response.sendError(HttpStatus.SERVICE_UNAVAILABLE.value(), "OsmGpx datasource is not initialized");
			return;
		}
		response.setContentType("application/gzip");
		response.setHeader(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"osmgpx-reviews.csv.gz\"");
		String select = "SELECT m.id, m.\"user\", m.date, m.name, m.description, m.tags, m.lat, m.lon, m.activity, m.activity_source, "
				+ "m.file_activity, m.speed_matches_activity, m.speed, m.max_speed, m.distance, m.points, m.time_minutes, "
				+ "m.manual_review::text AS manual_review, m.track_stats::text AS track_stats, m.simplified_geometry"
				+ (gpx ? ", f.data AS gpx FROM " + TABLE + " m LEFT JOIN osm_gpx_files f ON f.id = m.id" : " FROM " + TABLE + " m")
				+ " WHERE m.manual_review IS NOT NULL AND m.id > ? ORDER BY m.id LIMIT " + EXPORT_BATCH;
		int rows = 0;
		try (Writer w = new OutputStreamWriter(new GZIPOutputStream(response.getOutputStream(), 1 << 16), StandardCharsets.UTF_8)) {
			w.write(String.join(",", EXPORT_COLUMNS) + (gpx ? ",gpx_gz_b64" : "") + "\n");
			long lastId = -1;
			while (true) {
				long[] last = {-1};
				jdbcTemplate.query(select, (RowCallbackHandler) rs -> {
					try {
						writeRow(w, rs, gpx);
					} catch (IOException e) {
						throw new UncheckedIOException(e);
					}
					last[0] = rs.getLong("id");
				}, lastId);
				if (last[0] < 0) {
					break;
				}
				lastId = last[0];
				rows++;
			}
		}
		LOG.info("Exported reviewed OsmGpx tracks in " + rows + " batches");
	}

	private static void writeRow(Writer w, ResultSet rs, boolean gpx) throws IOException, SQLException {
		Array tags = rs.getArray("tags");
		byte[] geometry = rs.getBytes("simplified_geometry");
		String[] values = {rs.getString("id"), rs.getString("user"), rs.getString("date"), rs.getString("name"),
				rs.getString("description"), tags == null ? null : String.join("|", (String[]) tags.getArray()), rs.getString("lat"),
				rs.getString("lon"), rs.getString("activity"), rs.getString("activity_source"), rs.getString("file_activity"),
				rs.getString("speed_matches_activity"), rs.getString("speed"), rs.getString("max_speed"), rs.getString("distance"),
				rs.getString("points"), rs.getString("time_minutes"), rs.getString("manual_review"), rs.getString("track_stats"),
				geometry == null ? null : Base64.getEncoder().encodeToString(geometry)};
		StringBuilder line = new StringBuilder();
		for (String v : values) {
			line.append(line.length() == 0 ? "" : ",").append(csv(v));
		}
		if (gpx) {
			byte[] data = rs.getBytes("gpx");
			line.append(',').append(data == null ? "" : Base64.getEncoder().encodeToString(data));
		}
		w.write(line.append('\n').toString());
	}

	private static String csv(String v) {
		return v == null ? "" : "\"" + v.replace("\"", "\"\"") + "\"";
	}

	private static boolean isBlank(String s) {
		return s == null || s.isBlank();
	}
}
