package net.osmand.server.controllers.pub;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import net.osmand.data.LatLon;
import net.osmand.server.DatasourceConfiguration;
import net.osmand.server.api.services.GpxService;
import net.osmand.server.utils.WebGpxParser;
import net.osmand.shared.gpx.GpxFile;
import net.osmand.shared.gpx.GpxTrackAnalysis;
import net.osmand.shared.gpx.GpxUtilities;
import net.osmand.shared.gpx.primitives.WptPt;
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

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
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
	private static final int MAX_ROUTES = 100;
	private static final int MIN_POINTS_SIZE = 100;
	private static final int MAX_DISTANCE_BETWEEN_POINTS = 1000;
	private final AtomicInteger cacheTouch = new AtomicInteger(0);

	private static final String GPX_METADATA_TABLE_NAME = "osm_gpx_data";
	private static final String GPX_FILES_TABLE_NAME = "osm_gpx_files";

	@GetMapping(path = {"/get-routes-list"}, produces = "application/json")
	public ResponseEntity<String> getRoutes(@RequestParam String activity,
	                                        @RequestParam(required = false) String year,
	                                        @RequestParam String minlat,
	                                        @RequestParam String maxlat,
	                                        @RequestParam String minlon,
	                                        @RequestParam String maxlon) {
		if (!config.osmgpxInitialized()) {
			return ResponseEntity.ok("OsmGpx datasource is not initialized");
		}
		cleanupCache();

		StringBuilder conditions = new StringBuilder();
		List<Object> params = new ArrayList<>();

		ResponseEntity<String> error = addCoords(params, conditions, minlat, maxlat, minlon, maxlon);
		if (error != null) {
			return error;
		}

		error = filterByYear(year, params, conditions);
		if (error != null) {
			return error;
		}

		error = filterByActivity(activity, params, conditions);
		if (error != null) {
			return error;
		}

		String query = "SELECT m.id, f.data AS bytes, m.name, m.description, m.\"user\", m.date, m.activity " +
				"FROM " + GPX_METADATA_TABLE_NAME + " m " +
				"JOIN " + GPX_FILES_TABLE_NAME + " f ON f.id = m.id " +
				"WHERE 1 = 1 " + conditions +
				" ORDER BY m.date DESC LIMIT " + MAX_ROUTES;

		List<Feature> features = new ArrayList<>();
		jdbcTemplate.query(query, ps -> {
			for (int i = 0; i < params.size(); i++) {
				ps.setObject(i + 1, params.get(i));
			}
		}, rs -> {
			Feature feature = new Feature();
			Long id = rs.getLong("id");
			feature.getProperties().put("id", id);
			feature.getProperties().put("name", rs.getString("name"));
			feature.getProperties().put("description", rs.getString("description"));
			feature.getProperties().put("user", rs.getString("user"));
			feature.getProperties().put("date", rs.getString("date"));
			String idKey = feature.getProperty("id").toString();
			byte[] bytes = rs.getBytes("bytes");
			RouteFile file = routesCache.computeIfAbsent(idKey, key -> {
				GpxFile gpxFile = null;
				try (Source src = new Buffer().write(Objects.requireNonNull(Algorithms.gzipToString(bytes)).getBytes())) {
					gpxFile = GpxUtilities.INSTANCE.loadGpxFile(src);
				} catch (IOException e) {
					LOGGER.error("Error loading GPX file", e);
				}
				if (gpxFile != null && gpxFile.getError() == null) {
					GpxTrackAnalysis analysis = gpxFile.getAnalysis(System.currentTimeMillis());
					return new RouteFile(bytes, gpxFile, analysis);
				}
				return null;
			});
			if (file != null) {
				addGeoDataToFeature(file, feature);
				if (feature.getProperty("geo") != null) {
					features.add(feature);
				}
			}
		});
		FeatureCollection featureCollection = new FeatureCollection();
		featureCollection.setFeatures(features);

		return ResponseEntity.ok(gson.toJson(featureCollection));
	}

	@GetMapping(path = {"/get-osm-route"}, produces = "application/json")
	public ResponseEntity<String> getRoute(@RequestParam Long id) throws IOException {
		RouteFile routeFile = routesCache.get(id.toString());
		// get from cache
		if (routeFile != null) {
			WebGpxParser.TrackData gpxData = gpxService.getTrackDataByGpxFile(routeFile.gpxFile.clone(), null, routeFile.analysis);
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
					if (gpxFile.getError() != null) {
						GpxTrackAnalysis analysis = gpxFile.getAnalysis(System.currentTimeMillis());
						WebGpxParser.TrackData gpxData = gpxService.getTrackDataByGpxFile(gpxFile, null, analysis);
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

	private ResponseEntity<String> filterByActivity(String activity, List<Object> params, StringBuilder conditions) {
		if (!Algorithms.isEmpty(activity)) {
			conditions.append(" AND m.activity = ?");
			params.add(activity);
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

	private void addGeoDataToFeature(RouteFile file, Feature feature) {
		GpxFile gpxFile = file.gpxFile;
		List<WptPt> points = gpxFile.getAllSegmentsPoints();
		GpxTrackAnalysis analysis = file.analysis;
		if (!points.isEmpty() && points.size() > MIN_POINTS_SIZE && analysis.getMaxDistanceBetweenPoints() < MAX_DISTANCE_BETWEEN_POINTS) {
			List<List<LatLon>> result = new ArrayList<>();
			gpxFile.getTracks().forEach(track -> {
				if (!track.getGeneralTrack()) {
					track.getSegments().forEach(segment -> {
						List<LatLon> segmentPoints = new ArrayList<>();
						segment.getPoints().forEach(point -> {
							if (point.hasLocation()) {
								segmentPoints.add(new LatLon(point.getLatitude(), point.getLongitude()));
							}
						});
						if (!segmentPoints.isEmpty()) {
							result.add(segmentPoints);
						}
					});
				}
			});
			feature.getProperties().put("geo", result);
			feature.getProperties().put("distance", analysis.getTotalDistance());
		}
	}

	private ResponseEntity<String> addCoords(List<Object> params, StringBuilder conditions, String minlat, String maxlat, String minlon, String maxlon) {
		Float validatedMinLat = validateCoordinate(minlat, "minlat");
		Float validatedMaxLat = validateCoordinate(maxlat, "maxlat");
		Float validatedMinLon = validateCoordinate(minlon, "minlon");
		Float validatedMaxLon = validateCoordinate(maxlon, "maxlon");

		if (validatedMinLat == null || validatedMaxLat == null || validatedMinLon == null || validatedMaxLon == null) {
			return ResponseEntity.badRequest().body("Invalid latitude or longitude values.");
		}

		conditions.append(" AND m.minlon <= ? AND m.maxlon >= ? AND m.minlat <= ? AND m.maxlat >= ?");
		params.add(validatedMaxLon);
		params.add(validatedMinLon);
		params.add(validatedMaxLat);
		params.add(validatedMinLat);

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
