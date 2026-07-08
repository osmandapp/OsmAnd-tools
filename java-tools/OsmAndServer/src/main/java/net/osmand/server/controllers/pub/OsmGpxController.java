package net.osmand.server.controllers.pub;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import net.osmand.server.DatasourceConfiguration;
import net.osmand.server.api.services.GpxService;
import net.osmand.server.utils.WebGpxParser;
import net.osmand.shared.gpx.GpxFile;
import net.osmand.shared.gpx.GpxTrackAnalysis;
import net.osmand.shared.gpx.GpxUtilities;
import net.osmand.util.Algorithms;
import okio.Buffer;
import okio.Source;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.geojson.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.DataAccessException;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.*;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.GZIPInputStream;

import static net.osmand.server.api.services.UserdataService.BUFFER_SIZE;

@RestController
@RequestMapping("/osmgpx")
public class OsmGpxController {

	@Autowired
	@Qualifier("osmgpxJdbcTemplate")
	JdbcTemplate jdbcTemplate;

	@Autowired
	protected GpxService gpxService;

	@Autowired
	DatasourceConfiguration config;

	@Autowired
	Gson gson = new GsonBuilder().create();

	protected static final Log LOGGER = LogFactory.getLog(OsmGpxController.class);
	Gson gsonWithNans = new GsonBuilder().serializeSpecialFloatingPointValues().create();

	Map<String, RouteFile> routesCache = new ConcurrentHashMap<>();

	private final ReentrantLock lock = new ReentrantLock();

	private static final int MAX_RUNTIME_CACHE_SIZE = 5000;
	private static final int MAX_ROUTES_SUMMARY = 100000;
	private static final int MAX_TAGS_PER_BBOX = 1000;
	private final AtomicInteger cacheTouch = new AtomicInteger(0);

	private static final String GPX_METADATA_TABLE_NAME = "osm_gpx_data";
	private static final String GPX_FILES_TABLE_NAME = "osm_gpx_files";

	public record RoutesListRequest(
			List<String> activityArr,
			Integer year,
			String minLat,
			String maxLat,
			String minLon,
			String maxLon,
			List<String> tags,
			String tagMatchMode,
			List<Integer> distanceRange,
			List<Integer> speedRange,
			List<Integer> maxSpeedRange,
			List<Integer> maxDistBetweenPointsRange,
			List<Integer> timeMinutesRange,
			List<Integer> waypointsRange
	) {
	}

	@PostMapping(path = {"/get-routes-list"}, consumes = "application/json", produces = "application/json")
	public ResponseEntity<String> getRoutesPost(@RequestBody RoutesListRequest req) {
		if (!config.osmgpxInitialized()) {
			return ResponseEntity.ok("OsmGpx datasource is not initialized");
		}

		cleanupCache();

		StringBuilder conditions = new StringBuilder();
		List<Object> params = new ArrayList<>();

		ResponseEntity<String> error = addCoords(params, conditions, req.minLat(), req.maxLat(), req.minLon(), req.maxLon());
		if (error != null) {
			return error;
		}

		// skip garbage and error activities
		conditions.append(" AND (m.activity IS NULL OR (m.activity <> ? AND m.activity <> ?))");
		params.add("garbage");
		params.add("error");

		if (req.year() != null) {
			error = filterByYear(String.valueOf(req.year()), params, conditions);
			if (error != null) {
				return error;
			}
		}

		if (req.activityArr() != null && !req.activityArr().isEmpty()) {
			error = filterByActivity(req.activityArr(), params, conditions);
			if (error != null) {
				return error;
			}
		}

		if (req.distanceRange() != null && !req.distanceRange().isEmpty()) {
			error = filterByRange("m.distance", req.distanceRange(), params, conditions, "distance");
			if (error != null) {
				return error;
			}
		}

		if (req.speedRange() != null && !req.speedRange().isEmpty()) {
			error = filterByRange("m.speed", req.speedRange(), params, conditions, "speed");
			if (error != null) {
				return error;
			}
		}

		if (req.maxSpeedRange() != null && !req.maxSpeedRange().isEmpty()) {
			error = filterByRange("m.max_speed", req.maxSpeedRange(), params, conditions, "maxSpeed");
			if (error != null) {
				return error;
			}
		}

		if (req.maxDistBetweenPointsRange() != null && !req.maxDistBetweenPointsRange().isEmpty()) {
			error = filterByRange("m.max_dist_between_points", req.maxDistBetweenPointsRange(), params, conditions, "maxDistBetweenPoints");
			if (error != null) {
				return error;
			}
		}

		if (req.timeMinutesRange() != null && !req.timeMinutesRange().isEmpty()) {
			error = filterByRange("m.time_minutes", req.timeMinutesRange(), params, conditions, "time");
			if (error != null) {
				return error;
			}
		}

		if (req.waypointsRange() != null && !req.waypointsRange().isEmpty()) {
			error = filterByRange("m.waypoints", req.waypointsRange(), params, conditions, "waypoints");
			if (error != null) {
				return error;
			}
		}

		String tagMatchMode = Algorithms.isEmpty(req.tagMatchMode()) ? "OR" : req.tagMatchMode();
		applyTagsFilter(req.tags(), tagMatchMode, conditions, params);

		List<Feature> features = queryRouteFeatures(conditions, params);
		FeatureCollection featureCollection = new FeatureCollection();
		featureCollection.setFeatures(features);

		return ResponseEntity.ok(gson.toJson(featureCollection));
	}

	@GetMapping(path = {"/ranges"}, produces = "application/json")
	public ResponseEntity<String> getRanges(@RequestParam String minLat,
	                                        @RequestParam String maxLat,
	                                        @RequestParam String minLon,
	                                        @RequestParam String maxLon,
	                                        @RequestParam(required = false) Integer year,
	                                        @RequestParam(required = false) List<String> activityArr) {
		if (!config.osmgpxInitialized()) {
			return ResponseEntity.ok("OsmGpx datasource is not initialized");
		}

		StringBuilder conditions = new StringBuilder();
		List<Object> params = new ArrayList<>();

		ResponseEntity<String> error = addCoords(params, conditions, minLat, maxLat, minLon, maxLon);
		if (error != null) {
			return error;
		}

		// skip garbage and error activities
		conditions.append(" AND (m.activity IS NULL OR (m.activity <> ? AND m.activity <> ?))");
		params.add("garbage");
		params.add("error");

		if (year != null) {
			error = filterByYear(String.valueOf(year), params, conditions);
			if (error != null) {
				return error;
			}
		}

		if (activityArr != null && !activityArr.isEmpty()) {
			error = filterByActivity(activityArr, params, conditions);
			if (error != null) {
				return error;
			}
		}

		// only include records with valid distance or speed values
		conditions.append(" AND (m.distance > 0 OR m.speed > 0)");

		String query = "SELECT " +
				"MIN(CASE WHEN distance > 0 THEN distance END), " +
				"MAX(CASE WHEN distance > 0 THEN distance END), " +
				"MIN(CASE WHEN speed >= 0 THEN speed END), " +
				"MAX(CASE WHEN speed >= 0 THEN speed END), " +
				"MIN(CASE WHEN max_speed >= 0 THEN max_speed END), " +
				"MAX(CASE WHEN max_speed >= 0 THEN max_speed END), " +
				"MIN(CASE WHEN max_dist_between_points >= 0 THEN max_dist_between_points END), " +
				"MAX(CASE WHEN max_dist_between_points >= 0 THEN max_dist_between_points END), " +
				"MIN(CASE WHEN time_minutes >= 0 THEN time_minutes END), " +
				"MAX(CASE WHEN time_minutes >= 0 THEN time_minutes END), " +
				"MIN(CASE WHEN waypoints >= 0 THEN waypoints END), " +
				"MAX(CASE WHEN waypoints >= 0 THEN waypoints END) " +
				"FROM " + GPX_METADATA_TABLE_NAME + " m WHERE 1 = 1 " + conditions;

		Map<String, Integer> ranges = new LinkedHashMap<>();
		jdbcTemplate.query(query, ps -> {
			for (int i = 0; i < params.size(); i++) {
				ps.setObject(i + 1, params.get(i));
			}
		}, rs -> {
			float minDist = rs.getFloat(1);
			ranges.put("minDist", rs.wasNull() ? 0 : (int) minDist);
			float maxDist = rs.getFloat(2);
			ranges.put("maxDist", rs.wasNull() ? 0 : (int) maxDist);
			float minSpeed = rs.getFloat(3);
			ranges.put("minSpeed", rs.wasNull() ? 0 : (int) minSpeed);
			float maxSpeed = rs.getFloat(4);
			ranges.put("maxSpeed", rs.wasNull() ? 0 : (int) maxSpeed);
			float maxSpeedMin = rs.getFloat(5);
			ranges.put("maxSpeedMin", rs.wasNull() ? 0 : (int) maxSpeedMin);
			float maxSpeedMax = rs.getFloat(6);
			ranges.put("maxSpeedMax", rs.wasNull() ? 0 : (int) maxSpeedMax);
			float maxDistBetweenPointsMin = rs.getFloat(7);
			ranges.put("maxDistBetweenPointsMin", rs.wasNull() ? 0 : (int) maxDistBetweenPointsMin);
			float maxDistBetweenPointsMax = rs.getFloat(8);
			ranges.put("maxDistBetweenPointsMax", rs.wasNull() ? 0 : (int) maxDistBetweenPointsMax);
			int timeMinutesMin = rs.getInt(9);
			ranges.put("timeMinutesMin", rs.wasNull() ? 0 : timeMinutesMin);
			int timeMinutesMax = rs.getInt(10);
			ranges.put("timeMinutesMax", rs.wasNull() ? 0 : timeMinutesMax);
			int waypointsMin = rs.getInt(11);
			ranges.put("waypointsMin", rs.wasNull() ? 0 : waypointsMin);
			int waypointsMax = rs.getInt(12);
			ranges.put("waypointsMax", rs.wasNull() ? 0 : waypointsMax);
		});

		return ResponseEntity.ok(gson.toJson(ranges));
	}

	@GetMapping(path = {"/tags"}, produces = "application/json")
	public ResponseEntity<String> getTags(@RequestParam String minLat,
	                                      @RequestParam String maxLat,
	                                      @RequestParam String minLon,
	                                      @RequestParam String maxLon,
	                                      @RequestParam(required = false) Integer year,
	                                      @RequestParam(required = false) List<String> activityArr) {
		if (!config.osmgpxInitialized()) {
			return ResponseEntity.ok("OsmGpx datasource is not initialized");
		}

		StringBuilder conditions = new StringBuilder();
		List<Object> params = new ArrayList<>();

		ResponseEntity<String> error = addCoords(params, conditions, minLat, maxLat, minLon, maxLon);
		if (error != null) {
			return error;
		}

		// skip garbage and error activities
		conditions.append(" AND (m.activity IS NULL OR (m.activity <> ? AND m.activity <> ?))");
		params.add("garbage");
		params.add("error");

		if (year != null) {
			error = filterByYear(String.valueOf(year), params, conditions);
			if (error != null) {
				return error;
			}
		}

		if (activityArr != null && !activityArr.isEmpty()) {
			error = filterByActivity(activityArr, params, conditions);
			if (error != null) {
				return error;
			}
		}

		String query =
				"SELECT tag, count(*) AS cnt " +
						"FROM (" +
						"  SELECT unnest(m.tags) AS tag " +
						"  FROM " + GPX_METADATA_TABLE_NAME + " m " +
						"  WHERE 1 = 1 " + conditions +
						") t " +
						"WHERE tag IS NOT NULL AND tag <> '' " +
						"GROUP BY tag " +
						"ORDER BY cnt DESC " +
						"LIMIT " + MAX_TAGS_PER_BBOX;

		List<Map<String, Object>> rows = jdbcTemplate.queryForList(query, params.toArray());
		return ResponseEntity.ok(gson.toJson(rows));
	}

	@GetMapping(path = {"/activities"}, produces = "application/json")
	public ResponseEntity<String> getActivities(@RequestParam String minLat,
	                                            @RequestParam String maxLat,
	                                            @RequestParam String minLon,
	                                            @RequestParam String maxLon,
	                                            @RequestParam(required = false) Integer year) {
		if (!config.osmgpxInitialized()) {
			return ResponseEntity.ok("OsmGpx datasource is not initialized");
		}

		StringBuilder conditions = new StringBuilder();
		List<Object> params = new ArrayList<>();

		ResponseEntity<String> error = addCoords(params, conditions, minLat, maxLat, minLon, maxLon);
		if (error != null) {
			return error;
		}

		// only non-null, non-empty activities; exclude garbage and error
		conditions.append(" AND m.activity IS NOT NULL AND m.activity <> '' AND m.activity <> ? AND m.activity <> ?");
		params.add("garbage");
		params.add("error");

		if (year != null) {
			error = filterByYear(String.valueOf(year), params, conditions);
			if (error != null) {
				return error;
			}
		}

		String query =
				"SELECT m.activity AS id, COUNT(*) AS count " +
						"FROM " + GPX_METADATA_TABLE_NAME + " m " +
						"WHERE 1 = 1 " + conditions +
						" GROUP BY m.activity " +
						"ORDER BY count DESC";

		List<Map<String, Object>> rows = jdbcTemplate.queryForList(query, params.toArray());
		return ResponseEntity.ok(gson.toJson(rows));
	}

	private void applyTagsFilter(List<String> tags,
	                             String tagMatchMode,
	                             StringBuilder conditions,
	                             List<Object> params) {
		if (tags == null || tags.isEmpty()) {
			return;
		}
		List<String> normalized = new ArrayList<>();
		for (String tag : tags) {
			if (!Algorithms.isEmpty(tag)) {
				normalized.add(tag.trim());
			}
		}
		if (normalized.isEmpty()) {
			return;
		}
		String op = "AND".equalsIgnoreCase(tagMatchMode) ? "@>" : "&&";
		conditions.append(" AND m.tags ").append(op).append(" ARRAY[");
		conditions.append(String.join(",", Collections.nCopies(normalized.size(), "?")));
		conditions.append("]::text[]");
		params.addAll(normalized);
	}

	private List<Feature> queryRouteFeatures(StringBuilder conditions, List<Object> params) {
		String query = "SELECT m.id, m.name, m.description, m.user, m.date, m.activity, m.lat, m.lon, " +
				"m.speed, m.distance, m.points, m.simplified_geometry " +
				"FROM " + GPX_METADATA_TABLE_NAME + " m " +
				"WHERE 1 = 1 " + conditions +
				" AND m.simplified_geometry IS NOT NULL" +
				" LIMIT " + MAX_ROUTES_SUMMARY;

		List<Feature> features = new ArrayList<>();
		jdbcTemplate.query(query, ps -> {
			for (int i = 0; i < params.size(); i++) {
				ps.setObject(i + 1, params.get(i));
			}
		}, rs -> {
			byte[] simplifiedGeometry = rs.getBytes("simplified_geometry");
			if (simplifiedGeometry == null || simplifiedGeometry.length == 0) {
				return;
			}
			Feature feature = createBaseFeature(rs);
			feature.getProperties().put("geo_b64", Base64.getEncoder().encodeToString(simplifiedGeometry));
			features.add(feature);
		});
		return features;
	}

	private Feature createBaseFeature(ResultSet rs) throws SQLException {
		Feature feature = new Feature();

		feature.getProperties().put("id", rs.getLong("id"));
		feature.getProperties().put("name", rs.getString("name"));
		feature.getProperties().put("description", rs.getString("description"));
		feature.getProperties().put("user", rs.getString("user"));
		feature.getProperties().put("date", rs.getString("date"));
		feature.getProperties().put("activity", rs.getString("activity"));
		Map<String, Object> point = new LinkedHashMap<>();
		point.put("lat", rs.getDouble("lat"));
		point.put("lon", rs.getDouble("lon"));

		feature.getProperties().put("point", point);

		int speed = (int) rs.getFloat("speed");
		if (speed != 0) {
			feature.getProperties().put("speed", speed);
		}
		int dist = (int) rs.getFloat("distance");
		if (dist != 0) {
			feature.getProperties().put("dist", dist);
		}
		int points = rs.getInt("points");
		if (points != 0) {
			feature.getProperties().put("points", points);
		}

		return feature;
	}

	@GetMapping(path = {"/get-osm-route"}, produces = "application/json")
	public ResponseEntity<String> getRoute(@RequestParam Long id) throws IOException {
		RouteFile routeFile = routesCache.get(id.toString());
		// get from cache
		if (routeFile != null) {
			WebGpxParser.TrackData gpxData = gpxService.buildTrackDataFromGpxFile(routeFile.gpxFile.clone(), routeFile.analysis);
			if (gpxData != null) {
				return ResponseEntity.ok(gsonWithNans.toJson(Map.of("gpx_data", gpxData)));
			} else {
				return ResponseEntity.badRequest().body("Error loading GPX file");
			}
		}
		// get from DB
		String query = "SELECT id, data FROM " + GPX_FILES_TABLE_NAME + " WHERE id = ? LIMIT 1";
		try {
			GpxData resultData = jdbcTemplate.queryForObject(query, (rs, rowNum) -> {
				Long rId = rs.getLong("id");
				byte[] byteArray = rs.getBytes("data");
				return new GpxData(rId, byteArray);
			}, id);

			if (resultData != null && resultData.byteArray != null) {
				try (Source src = new Buffer().write(Objects.requireNonNull(Algorithms.gzipToString(resultData.byteArray)).getBytes())) {
					GpxFile gpxFile = GpxUtilities.INSTANCE.loadGpxFile(src);
					if (gpxFile.getError() == null) {
						GpxTrackAnalysis analysis = gpxFile.getAnalysis(System.currentTimeMillis());
						WebGpxParser.TrackData gpxData = gpxService.buildTrackDataFromGpxFile(gpxFile, analysis);
						if (gpxData != null) {
							return ResponseEntity.ok(gsonWithNans.toJson(Map.of("gpx_data", gpxData)));
						}
					}
				} catch (IOException e) {
					return ResponseEntity.badRequest().body("Error loading GPX file");
				}
			}
		} catch (DataAccessException e) {
			return ResponseEntity.badRequest().body("No records found");
		}
		return ResponseEntity.badRequest().body("No records found");
	}

	@GetMapping(path = {"/get-original-file"}, produces = "application/json")
	public void getFile(@RequestParam Long id, HttpServletRequest request, HttpServletResponse response) throws IOException {
		RouteFile routeFile = routesCache.get(id.toString());
		byte[] fileData;
		if (routeFile != null) {
			fileData = routeFile.bytes;
		} else {
			String query = "SELECT id, data FROM " + GPX_FILES_TABLE_NAME + " WHERE id = ? LIMIT 1";
			try {
				GpxData resultData = jdbcTemplate.queryForObject(query, (rs, rowNum) -> {
					Long rId = rs.getLong("id");
					byte[] byteArray = rs.getBytes("data");
					return new GpxData(rId, byteArray);
				}, id);

				if (resultData != null && resultData.byteArray != null) {
					fileData = resultData.byteArray;
				} else {
					response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
					response.getWriter().write("No records found");
					return;
				}
			} catch (DataAccessException e) {
				response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
				response.getWriter().write("Error loading GPX file");
				return;
			}
		}
		String acceptEncoding = request.getHeader("Accept-Encoding");
		boolean gzipSupported = acceptEncoding != null && acceptEncoding.contains("gzip");

		response.setHeader("Content-Disposition", "attachment; filename=\"file-" + id + ".gpx\"");
		response.setContentType("application/octet-stream");

		try (OutputStream outputStream = response.getOutputStream()) {
			if (gzipSupported) {
				response.setHeader("Content-Encoding", "gzip");
				outputStream.write(fileData);
			} else {
				try (GZIPInputStream gzipInputStream = new GZIPInputStream(new ByteArrayInputStream(fileData))) {
					byte[] buffer = new byte[BUFFER_SIZE];
					int bytesRead;
					while ((bytesRead = gzipInputStream.read(buffer)) != -1) {
						outputStream.write(buffer, 0, bytesRead);
					}
				}
			}
			outputStream.flush();
		} catch (IOException e) {
			response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
		}
	}

	private ResponseEntity<String> filterByActivity(List<String> activityArr, List<Object> params, StringBuilder conditions) {
		if (activityArr != null && !activityArr.isEmpty()) {
			conditions.append(" AND m.activity IN (");
			conditions.append(String.join(",", Collections.nCopies(activityArr.size(), "?")));
			conditions.append(")");
			params.addAll(activityArr);
			return null;
		} else {
			return ResponseEntity.badRequest().body("Activity parameter is required.");
		}
	}

	private ResponseEntity<String> filterByYear(String year, List<Object> params, StringBuilder conditions) {
		if (!Algorithms.isEmpty(year)) {
			try {
				Integer parsedYear = Integer.parseInt(year);
				conditions.append(" AND extract(year from m.date) = ?");
				params.add(parsedYear);
			} catch (NumberFormatException e) {
				return ResponseEntity.badRequest().body("Invalid year format.");
			}
		}
		return null;
	}

	private ResponseEntity<String> filterByRange(String column, List<Integer> range, List<Object> params, StringBuilder conditions, String fieldName) {
		if (range == null || range.isEmpty()) {
			return null;
		}
		if (range.size() != 2) {
			return ResponseEntity.badRequest().body("Invalid " + fieldName + " range format. Expected [min, max].");
		}
		Integer min = range.get(0);
		Integer max = range.get(1);
		if (min != null && max != null && min > max) {
			return ResponseEntity.badRequest().body("Invalid " + fieldName + " range: min cannot be greater than max.");
		}
		if (min != null) {
			conditions.append(" AND ").append(column).append(" >= ?");
			params.add(min);
		}
		if (max != null) {
			conditions.append(" AND ").append(column).append(" <= ?");
			params.add(max);
		}
		return null;
	}

	private ResponseEntity<String> addCoords(List<Object> params, StringBuilder conditions, String minLat, String maxLat, String minLon, String maxLon) {
		Float validatedMinLat = validateCoordinate(minLat, "minLat");
		Float validatedMaxLat = validateCoordinate(maxLat, "maxLat");
		Float validatedMinLon = validateCoordinate(minLon, "minLon");
		Float validatedMaxLon = validateCoordinate(maxLon, "maxLon");

		if (validatedMinLat == null || validatedMaxLat == null || validatedMinLon == null || validatedMaxLon == null) {
			return ResponseEntity.badRequest().body("Invalid latitude or longitude values.");
		}

		// Use the GiST spatial index (idx_osm_gpx_bbox_gist) via the bbox-overlap operator.
		conditions.append(" AND ST_MakeEnvelope(m.minlon, m.minlat, m.maxlon, m.maxlat, 4326)"
				+ " && ST_MakeEnvelope(?, ?, ?, ?, 4326)");
		params.add(validatedMinLon.doubleValue());
		params.add(validatedMinLat.doubleValue());
		params.add(validatedMaxLon.doubleValue());
		params.add(validatedMaxLat.doubleValue());

		return null;
	}

	private Float validateCoordinate(String coordinate, String name) {
		try {
			return Float.parseFloat(coordinate);
		} catch (NumberFormatException e) {
			LOGGER.warn("Invalid " + name + " coordinate: " + coordinate);
			return null;
		}
	}

	private record RouteFile(byte[] bytes, GpxFile gpxFile, GpxTrackAnalysis analysis) {
	}

	private void cleanupCache() {
		if (lock.tryLock()) {
			try {
				int version = cacheTouch.incrementAndGet();

				if (version % MAX_RUNTIME_CACHE_SIZE == 0 && version > 0) {
					cacheTouch.set(0);

					List<String> keysToRemove = new ArrayList<>(routesCache.keySet());

					// remove half of the cache
					if (routesCache.size() >= MAX_RUNTIME_CACHE_SIZE) {
						for (int i = 0; i < MAX_RUNTIME_CACHE_SIZE / 2; i++) {
							String key = keysToRemove.get(i);
							routesCache.remove(key);
						}
					}
				}
			} finally {
				lock.unlock();
			}
		}
	}

	private record GpxData(Long id, byte[] byteArray) {
	}

	// for testing purposes
	@GetMapping(path = {"/get-single-route"}, produces = "application/json")
	public ResponseEntity<String> getSingleRoute() {
		String query = "SELECT t.*, s.* from " + GPX_METADATA_TABLE_NAME + " t "
				+ "join " + GPX_FILES_TABLE_NAME + " s on s.id = t.id "
				+ "order by t.date asc limit 1";

		List<Map<String, Object>> result = jdbcTemplate.queryForList(query);

		if (result.isEmpty()) {
			return ResponseEntity.ok("No records found");
		} else {
			return ResponseEntity.ok(gson.toJson(result.get(0)));
		}
	}
}
