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

import javax.servlet.http.HttpSession;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

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

	private static final int MAX_RUNTIME_CACHE_SIZE = 5000;
	private static final int MAX_ROUTES = 500;
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
				" ORDER BY m.date DESC";

		List<Feature> features = new ArrayList<>();
		jdbcTemplate.query(query, ps -> {
			for (int i = 0; i < params.size(); i++) {
				ps.setObject(i + 1, params.get(i));
			}
		}, rs -> {
			if (features.size() >= MAX_ROUTES) {
				return;
			}
			Feature feature = new Feature();
			Long id = rs.getLong("id");
			feature.getProperties().put("id", id);
			feature.getProperties().put("name", rs.getString("name"));
			feature.getProperties().put("description", rs.getString("description"));
			feature.getProperties().put("user", rs.getString("user"));
			feature.getProperties().put("date", rs.getString("date"));
			String idKey = feature.getProperty("id").toString();
			RouteFile file = routesCache.get(idKey);
			if (file == null) {
				file = getGpxFile(rs.getBytes("bytes"), idKey);
			}
			if (file != null) {
				addGeoDataToFeature(file, feature);
				features.add(feature);
			}
		});
		FeatureCollection featureCollection = new FeatureCollection();
		featureCollection.setFeatures(features);

		return ResponseEntity.ok(gson.toJson(featureCollection));
	}

	@GetMapping(path = {"/get-osm-route"}, produces = "application/json")
	public ResponseEntity<String> getRoute(@RequestParam Long id, HttpSession httpSession) throws IOException {
		RouteFile routeFile = routesCache.get(id.toString());
		// get from cache
		if (routeFile != null) {
			File tmpGpx = File.createTempFile("gpx_" + httpSession.getId(), ".gpx");
			WebGpxParser.TrackData gpxData = gpxService.getTrackDataByGpxFile(routeFile.gpxFile, tmpGpx);
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
						File tmpGpx = File.createTempFile("gpx_" + httpSession.getId(), ".gpx");
						WebGpxParser.TrackData gpxData = gpxService.getTrackDataByGpxFile(gpxFile, tmpGpx);
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

	private RouteFile getGpxFile(byte[] bytes, String idKey) {
		GpxFile gpxFile = null;
		RouteFile file = null;
		try (Source src = new Buffer().write(Objects.requireNonNull(Algorithms.gzipToString(bytes)).getBytes())) {
			gpxFile = GpxUtilities.INSTANCE.loadGpxFile(src);
		} catch (IOException e) {
			LOGGER.error("Error loading GPX file", e);
		}
		if (gpxFile != null) {
			GpxTrackAnalysis analysis = gpxFile.getAnalysis(System.currentTimeMillis());
			file = new RouteFile(gpxFile, analysis);
			routesCache.put(idKey, file);
		}
		return file;
	}

	private void addGeoDataToFeature(RouteFile file, Feature feature) {
		GpxFile gpxFile = file.gpxFile;
		List<WptPt> points = gpxFile.getAllSegmentsPoints();
		GpxTrackAnalysis analysis = file.analysis;
		if (!points.isEmpty() && points.size() > 100 && analysis.getMaxDistanceBetweenPoints() < 1000) {
			List<LatLon> latLonList = new ArrayList<>();
			for (WptPt point : points) {
				if (point.hasLocation()) {
					latLonList.add(new LatLon(point.getLatitude(), point.getLongitude()));
				}
			}
			feature.getProperties().put("geo", latLonList);
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

	private record RouteFile(GpxFile gpxFile, GpxTrackAnalysis analysis) {
	}

	private void cleanupCache() {
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
