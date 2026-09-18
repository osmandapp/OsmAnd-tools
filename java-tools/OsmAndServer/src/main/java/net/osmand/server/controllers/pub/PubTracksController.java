package net.osmand.server.controllers.pub;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
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
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import net.osmand.server.DatasourceConfiguration;
import net.osmand.server.WebSecurityConfiguration;
import net.osmand.server.WebSecurityConfiguration.OsmAndProUser;
import net.osmand.server.api.services.EmailSenderService;

/**
 * Reviews of OsmGpx tracks from the heatmap page. Anyone reads them and downloads the reviewed tracks; signed-in OsmAnd
 * users and admins write. Stored in osm_gpx_data.manual_review as {"admin": review, "users": {"cloud user id": review}};
 * what leaves the server has no admin e-mail and no user ids. Feedback about the page goes to osm_gpx_feedback without
 * any account; only admins read it.
 */
@RestController
@RequestMapping("/api/pubtracks")
public class PubTracksController {

	private static final Log LOG = LogFactory.getLog(PubTracksController.class);
	private static final String TABLE = "osm_gpx_data";
	// the quick actions of the heatmap page
	private static final Set<String> VERDICTS = Set.of("ok", "wrong_activity", "bad_quality", "bad_line", "simulated", "garbage");
	private static final int MAX_COMMENT = 2000;
	private static final int MAX_IDS = 1000;
	private static final int EXPORT_BATCH = 500;
	private static final String FEEDBACK_TABLE = "osm_gpx_feedback";
	private static final int MAX_TEXT = 5000;
	private static final int MAX_EMAIL = 200;
	private static final int MAX_PER_DAY = 20; // per address in the last 24 hours, anyone may write
	private static final int MAX_LIST = 5000;
	private static final Pattern NUMBER = Pattern.compile("[-+]?\\d+(\\.\\d+)?([eE][-+]?\\d+)?"); // negative coordinates are not formulas
	private static final int MAX_CONTEXT = 8000; // serialized view, filters and url; the page sends about 2 KB
	private static final String[] EXPORT_COLUMNS = {"id", "user", "date", "name", "description", "tags", "lat", "lon", "activity",
			"activity_source", "file_activity", "speed_matches_activity", "speed", "max_speed", "distance", "points", "time_minutes",
			"reviews", "track_stats", "geometry_b64"};

	@Autowired
	@Qualifier("osmgpxJdbcTemplate")
	JdbcTemplate jdbcTemplate;

	@Autowired
	DatasourceConfiguration config;

	@Autowired
	EmailSenderService emailSender;

	private final Gson gson = new Gson();
	private volatile boolean feedbackTableReady;

	public record ReviewRequest(Long id, String verdict, String activity, String comment) {
	}

	public record FeedbackRequest(String text, String email, Map<String, Object> view, Map<String, Object> filters, String url) {
	}

	/** whether the caller may send reviews; no ids */
	@GetMapping(path = "/me", produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> me(Authentication auth) {
		JsonObject me = new JsonObject();
		me.addProperty("signedIn", user(auth) != null);
		me.addProperty("admin", isAdmin(auth));
		return ResponseEntity.ok(gson.toJson(me));
	}

	/** saves the caller's verdict of one track (admins share one "admin" verdict); an empty verdict removes it */
	@PostMapping(path = "/review", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> review(@RequestBody ReviewRequest req, Authentication auth) {
		if (!config.osmgpxInitialized()) {
			return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body("OsmGpx datasource is not initialized");
		}
		OsmAndProUser user = user(auth);
		if (user == null) {
			return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body("Sign in to send a review");
		}
		if (req.id() == null) {
			return ResponseEntity.badRequest().body("id is required");
		}
		JsonObject review = null;
		if (!isBlank(req.verdict())) {
			if (!VERDICTS.contains(req.verdict())) {
				return ResponseEntity.badRequest().body("verdict must be one of " + VERDICTS);
			}
			review = new JsonObject();
			review.addProperty("verdict", req.verdict());
			if (!isBlank(req.activity())) {
				review.addProperty("activity", req.activity().trim());
			}
			if (!isBlank(req.comment())) {
				String comment = req.comment().trim();
				review.addProperty("comment", cut(comment, MAX_COMMENT));
			}
			review.addProperty("time", Instant.now().toString());
		}
		boolean admin = isAdmin(auth);
		String mine = admin ? "admin" : String.valueOf(user.getUserDevice().userid);
		int updated;
		if (admin && review != null) {
			review.addProperty("author", user.getUsername()); // kept in the database, never sent out
			updated = jdbcTemplate.update("UPDATE " + TABLE + " SET manual_review = COALESCE(manual_review, '{}'::jsonb)"
					+ " || jsonb_build_object('admin', ?::jsonb) WHERE id = ?", gson.toJson(review), req.id());
		} else if (admin) {
			updated = jdbcTemplate.update("UPDATE " + TABLE + " SET manual_review = NULLIF(COALESCE(manual_review, '{}'::jsonb) - 'admin',"
					+ " '{}'::jsonb) WHERE id = ?", req.id());
		} else if (review != null) {
			updated = jdbcTemplate.update("UPDATE " + TABLE + " SET manual_review = COALESCE(manual_review, '{}'::jsonb)"
					+ " || jsonb_build_object('users', COALESCE(manual_review->'users', '{}'::jsonb) || jsonb_build_object(?::text, ?::jsonb))"
					+ " WHERE id = ?", mine, gson.toJson(review), req.id());
		} else {
			String left = "(COALESCE(manual_review, '{}'::jsonb) #- ARRAY['users', ?::text])";
			updated = jdbcTemplate.update("UPDATE " + TABLE + " SET manual_review = NULLIF(CASE WHEN " + left + "->'users' = '{}'::jsonb THEN "
					+ left + " - 'users' ELSE " + left + " END, '{}'::jsonb) WHERE id = ?", mine, mine, mine, req.id());
		}
		if (updated == 0) {
			return ResponseEntity.notFound().build();
		}
		String stored = jdbcTemplate.query("SELECT manual_review::text FROM " + TABLE + " WHERE id = ?",
				(ResultSetExtractor<String>) rs -> rs.next() ? rs.getString(1) : null, req.id());
		return ResponseEntity.ok(gson.toJson(publicView(stored, mine)));
	}

	/** public reviews of the given tracks by id, with "mine" for a signed-in caller; tracks without reviews are left out */
	@GetMapping(path = "/reviews", produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> reviews(@RequestParam List<Long> ids, Authentication auth) {
		if (!config.osmgpxInitialized()) {
			return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body("OsmGpx datasource is not initialized");
		}
		if (ids.isEmpty() || ids.size() > MAX_IDS) {
			return ResponseEntity.badRequest().body("1 to " + MAX_IDS + " ids");
		}
		OsmAndProUser user = user(auth);
		String mine = user == null ? null : isAdmin(auth) ? "admin" : String.valueOf(user.getUserDevice().userid);
		Map<Long, JsonObject> res = new LinkedHashMap<>();
		String in = String.join(",", Collections.nCopies(ids.size(), "?"));
		jdbcTemplate.query("SELECT id, manual_review::text FROM " + TABLE + " WHERE manual_review IS NOT NULL AND id IN (" + in + ")",
				(RowCallbackHandler) rs -> res.put(rs.getLong(1), publicView(rs.getString(2), mine)), ids.toArray());
		return ResponseEntity.ok(gson.toJson(res));
	}

	/** every reviewed track as one csv row, gzip; gpx=true adds the original GPX file (gzip, base64); since keeps tracks with a review from that moment on */
	@GetMapping(path = "/reviews.csv.gz")
	public void export(@RequestParam(defaultValue = "false") boolean gpx, @RequestParam(required = false) String since,
			HttpServletResponse response) throws IOException {
		if (!config.osmgpxInitialized()) {
			response.sendError(HttpStatus.SERVICE_UNAVAILABLE.value(), "OsmGpx datasource is not initialized");
			return;
		}
		Instant from = parseSince(since);
		response.setContentType("application/gzip");
		response.setHeader(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"osmgpx-reviews.csv.gz\"");
		String select = "SELECT m.id, m.\"user\", m.date, m.name, m.description, m.tags, m.lat, m.lon, m.activity, m.activity_source, "
				+ "m.file_activity, m.speed_matches_activity, m.speed, m.max_speed, m.distance, m.points, m.time_minutes, "
				+ "m.manual_review::text AS manual_review, m.track_stats::text AS track_stats, m.simplified_geometry"
				+ (gpx ? ", f.data AS gpx FROM " + TABLE + " m LEFT JOIN osm_gpx_files f ON f.id = m.id" : " FROM " + TABLE + " m")
				+ " WHERE m.manual_review IS NOT NULL AND m.id > ?"
				+ (from == null ? "" : " AND jsonb_path_exists(m.manual_review, '$.**.time ? (@ >= $s)', jsonb_build_object('s', ?::text))")
				+ " ORDER BY m.id LIMIT " + EXPORT_BATCH;
		int batches = 0;
		try (Writer w = new OutputStreamWriter(new GZIPOutputStream(response.getOutputStream(), 1 << 16), StandardCharsets.UTF_8)) {
			w.write(String.join(",", EXPORT_COLUMNS) + (gpx ? ",gpx_gz_b64" : "") + "\n");
			long lastId = -1;
			while (true) {
				long[] last = {-1};
				Object[] args = from == null ? new Object[] {lastId} : new Object[] {lastId, from.toString()};
				jdbcTemplate.query(select, (RowCallbackHandler) rs -> {
					try {
						writeRow(w, rs, gpx);
					} catch (IOException e) {
						throw new UncheckedIOException(e);
					}
					last[0] = rs.getLong("id");
				}, args);
				if (last[0] < 0) {
					break;
				}
				lastId = last[0];
				batches++;
			}
		}
		LOG.info("Exported reviewed OsmGpx tracks in " + batches + " batches");
	}

	/** saves a free-form message with the map view, the filter and the page url it was written at; the e-mail is only what was typed */
	@PostMapping(path = "/feedback", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> feedback(@RequestBody FeedbackRequest req, HttpServletRequest request) {
		if (!config.osmgpxInitialized()) {
			return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body("OsmGpx datasource is not initialized");
		}
		if (isBlank(req.text())) {
			return ResponseEntity.badRequest().body("text is required");
		}
		String ip = request.getRemoteAddr();
		ensureFeedbackTable();
		int sent = jdbcTemplate.queryForObject("SELECT count(*) FROM " + FEEDBACK_TABLE + " WHERE ip = ? AND time > now() - interval '24 hours'",
				Integer.class, ip);
		if (sent >= MAX_PER_DAY) {
			return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS).body("Too many messages from your address today");
		}
		String email = isBlank(req.email()) ? null : req.email().trim();
		if (email != null && (email.length() > MAX_EMAIL || !emailSender.isEmail(email))) {
			return ResponseEntity.badRequest().body("e-mail looks wrong");
		}
		Map<String, Object> context = new LinkedHashMap<>();
		context.put("view", req.view());
		context.put("filters", req.filters());
		context.put("url", req.url());
		String contextJson = gson.toJson(context);
		if (contextJson.length() > MAX_CONTEXT) {
			return ResponseEntity.badRequest().body("context is too large");
		}
		jdbcTemplate.update("INSERT INTO " + FEEDBACK_TABLE + " (time, email, ip, text, context) VALUES (now(), ?, ?, ?, ?::jsonb)",
				email, ip, cut(req.text().trim(), MAX_TEXT), contextJson);
		return ResponseEntity.ok("{}");
	}

	/** every message as one csv row, newest first, admins only; since keeps those from that moment on */
	@GetMapping(path = "/feedback.csv")
	public void feedbackCsv(@RequestParam(defaultValue = "5000") int limit, @RequestParam(required = false) String since, Authentication auth,
			HttpServletResponse response) throws IOException {
		if (!config.osmgpxInitialized()) {
			response.sendError(HttpStatus.SERVICE_UNAVAILABLE.value(), "OsmGpx datasource is not initialized");
			return;
		}
		if (!isAdmin(auth)) {
			response.sendError(HttpStatus.FORBIDDEN.value(), "Admins only");
			return;
		}
		Instant from = parseSince(since);
		response.setContentType("text/csv; charset=utf-8");
		response.setHeader(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"heatmap-feedback.csv\"");
		ensureFeedbackTable();
		String where = from == null ? "" : " WHERE time >= ?::timestamptz";
		Object[] args = from == null ? new Object[] {Math.max(1, Math.min(limit, MAX_LIST))}
				: new Object[] {from.toString(), Math.max(1, Math.min(limit, MAX_LIST))};
		try (Writer w = new OutputStreamWriter(response.getOutputStream(), StandardCharsets.UTF_8)) {
			w.write("id,time,email,ip,text,url,view,filters\n");
			jdbcTemplate.query("SELECT id, time, email, ip, text, context::text FROM " + FEEDBACK_TABLE + where + " ORDER BY id DESC LIMIT ?", (RowCallbackHandler) rs -> {
				JsonObject c = rs.getString("context") == null ? new JsonObject() : new JsonParser().parse(rs.getString("context")).getAsJsonObject();
				try {
					w.write(csvLine(rs.getString("id"), rs.getTimestamp("time").toInstant().toString(), rs.getString("email"), rs.getString("ip"),
							rs.getString("text"), jsonText(c.get("url")), jsonText(c.get("view")), jsonText(c.get("filters"))).append('\n').toString());
				} catch (IOException e) {
					throw new UncheckedIOException(e);
				}
			}, args);
		}
	}


	/** null when absent; a malformed value answers 400 */
	private static Instant parseSince(String since) {
		try {
			return since == null ? null : Instant.parse(since);
		} catch (DateTimeParseException e) {
			throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "since must be an ISO-8601 instant");
		}
	}

	private static String jsonText(JsonElement e) {
		return e == null || e.isJsonNull() ? null : e.isJsonPrimitive() ? e.getAsString() : e.toString();
	}

	private synchronized void ensureFeedbackTable() {
		if (!feedbackTableReady) {
			jdbcTemplate.execute("CREATE TABLE IF NOT EXISTS " + FEEDBACK_TABLE + " (id bigserial PRIMARY KEY, time timestamptz NOT NULL,"
					+ " email text, ip text, text text NOT NULL, context jsonb)");
			jdbcTemplate.execute("CREATE INDEX IF NOT EXISTS " + FEEDBACK_TABLE + "_ip_time ON " + FEEDBACK_TABLE + " (ip, time)");
			feedbackTableReady = true;
		}
	}

	/** {"admin": review without author, "users": [reviews without ids], "mine": the caller's review} */
	private static JsonObject publicView(String stored, String mine) {
		JsonObject res = new JsonObject();
		JsonArray users = new JsonArray();
		res.add("users", users);
		if (stored == null) {
			return res;
		}
		JsonObject all = new JsonParser().parse(stored).getAsJsonObject();
		if (all.has("admin")) {
			JsonObject admin = all.getAsJsonObject("admin").deepCopy();
			admin.remove("author");
			res.add("admin", admin);
			if ("admin".equals(mine)) {
				res.add("mine", admin);
			}
		}
		if (all.has("users")) {
			for (Map.Entry<String, JsonElement> e : all.getAsJsonObject("users").entrySet()) {
				users.add(e.getValue());
				if (e.getKey().equals(mine)) {
					res.add("mine", e.getValue());
				}
			}
		}
		return res;
	}

	private void writeRow(Writer w, ResultSet rs, boolean gpx) throws IOException, SQLException {
		Array tags = rs.getArray("tags");
		byte[] geometry = rs.getBytes("simplified_geometry");
		StringBuilder line = csvLine(rs.getString("id"), rs.getString("user"), rs.getString("date"), rs.getString("name"),
				rs.getString("description"), tags == null ? null : String.join("|", (String[]) tags.getArray()), rs.getString("lat"),
				rs.getString("lon"), rs.getString("activity"), rs.getString("activity_source"), rs.getString("file_activity"),
				rs.getString("speed_matches_activity"), rs.getString("speed"), rs.getString("max_speed"), rs.getString("distance"),
				rs.getString("points"), rs.getString("time_minutes"), gson.toJson(publicView(rs.getString("manual_review"), null)),
				rs.getString("track_stats"), geometry == null ? null : Base64.getEncoder().encodeToString(geometry));
		if (gpx) {
			byte[] data = rs.getBytes("gpx");
			line.append(',').append(data == null ? "" : Base64.getEncoder().encodeToString(data));
		}
		w.write(line.append('\n').toString());
	}

	private static OsmAndProUser user(Authentication auth) {
		return auth != null && auth.getPrincipal() instanceof OsmAndProUser u ? u : null;
	}

	private static boolean isAdmin(Authentication auth) {
		return user(auth) != null && auth.getAuthorities().stream().anyMatch(a -> WebSecurityConfiguration.ROLE_ADMIN.equals(a.getAuthority()));
	}

	private static String cut(String s, int max) {
		if (s.length() <= max) {
			return s;
		}
		return s.substring(0, Character.isHighSurrogate(s.charAt(max - 1)) ? max - 1 : max);
	}

	private static StringBuilder csvLine(String... values) {
		StringBuilder line = new StringBuilder();
		for (String v : values) {
			line.append(line.isEmpty() ? "" : ",").append(csv(v));
		}
		return line;
	}

	/** quoted; text with a leading =, +, - or @ gets an apostrophe so a spreadsheet shows it instead of running it as a formula */
	private static String csv(String v) {
		if (v == null) {
			return "";
		}
		boolean formula = !v.isEmpty() && "=+-@".indexOf(v.charAt(0)) >= 0 && !NUMBER.matcher(v).matches();
		return "\"" + (formula ? "'" + v : v).replace("\"", "\"\"") + "\"";
	}

	private static boolean isBlank(String s) {
		return s == null || s.isBlank();
	}
}
