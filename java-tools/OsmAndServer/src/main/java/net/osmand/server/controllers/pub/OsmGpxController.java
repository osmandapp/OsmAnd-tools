package net.osmand.server.controllers.pub;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import net.osmand.data.LatLon;
import net.osmand.server.DatasourceConfiguration;
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
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.sql.Array;
import java.sql.ResultSet;
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
	DatasourceConfiguration config;

	@Autowired
	Gson gson = new GsonBuilder().create();

	protected static final Log LOGGER = LogFactory.getLog(OsmGpxController.class);

	Map<String, RouteFile> routesCache = new ConcurrentHashMap<>();

	private static final int MAX_RUNTIME_CACHE_SIZE = 5000;
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

		Float validatedMinLat = validateCoordinate(minlat, "minlat");
		Float validatedMaxLat = validateCoordinate(maxlat, "maxlat");
		Float validatedMinLon = validateCoordinate(minlon, "minlon");
		Float validatedMaxLon = validateCoordinate(maxlon, "maxlon");

		if (validatedMinLat == null || validatedMaxLat == null || validatedMinLon == null || validatedMaxLon == null) {
			return ResponseEntity.badRequest().body("Invalid latitude or longitude values.");
		}

		StringBuilder conditions = new StringBuilder();
		conditions.append(" and m.maxlat >= ? and m.minlat <= ? and m.maxlon >= ? and m.minlon <= ?");

		List<Object> params = new ArrayList<>();
		params.add(validatedMinLat);
		params.add(validatedMaxLat);
		params.add(validatedMinLon);
		params.add(validatedMaxLon);

		// filter by year
		if (!Algorithms.isEmpty(year)) {
			try {
				Integer parsedYear = Integer.parseInt(year);
				conditions.append(" and extract(year from m.date) = ?");
				params.add(parsedYear);
			} catch (NumberFormatException e) {
				return ResponseEntity.badRequest().body("Invalid year format.");
			}
		}

		// filter by activity
		if (!Algorithms.isEmpty(activity)) {
			conditions.append(" and m.activity = ?");
			params.add(activity);
		}

		String query = "SELECT m.id, f.data AS bytes, m.name, m.description, m.\"user\", m.date, m.tags, m.activity " +
				"FROM " + GPX_METADATA_TABLE_NAME + " m " +
				"JOIN " + GPX_FILES_TABLE_NAME + " f ON f.id = m.id " +
				"WHERE 1 = 1 " + conditions +
				" ORDER BY m.date ASC LIMIT 500";


		List<Feature> features = jdbcTemplate.query(query, ps -> {
			for (int i = 0; i < params.size(); i++) {
				ps.setObject(i + 1, params.get(i));
			}
		}, (rs, rowNum) -> {
			Feature feature = new Feature();
			Long id = rs.getLong("id");
			feature.getProperties().put("id", id);
			feature.getProperties().put("name", rs.getString("name"));
			feature.getProperties().put("description", rs.getString("description"));
			feature.getProperties().put("user", rs.getString("user"));
			feature.getProperties().put("date", rs.getString("date"));

			Array tagsArray = rs.getArray("tags");
			List<String> tags = new ArrayList<>();
			if (tagsArray != null) {
				try (ResultSet tagRs = tagsArray.getResultSet()) {
					while (tagRs.next()) {
						String tag = tagRs.getString(2);
						if (tag != null) {
							tags.add(tag.toLowerCase());
						}
					}
				}
			}
			feature.getProperties().put("tags", tags.toArray(new String[0]));

			byte[] bytes = rs.getBytes("bytes");

			String idKey = feature.getProperty("id").toString();
			RouteFile file = routesCache.get(idKey);
			if (file == null) {
				GpxFile gpxFile = null;
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
			}
			if (file != null) {
				GpxFile gpxFile = file.gpxFile;
				List<WptPt> points = gpxFile.getAllPoints();
				if (!points.isEmpty() && points.size() > 100) {
					List<LatLon> latLonList = new ArrayList<>();
					for (WptPt point : points) {
						if (point.hasLocation()) {
							latLonList.add(new LatLon(point.getLatitude(), point.getLongitude()));
						}
					}
					feature.getProperties().put("geo", latLonList);
					GpxTrackAnalysis analysis = file.analysis;
					feature.getProperties().put("distance", analysis.getTotalDistance());
					return feature;
				}
			}
			return null;
		});

		FeatureCollection featureCollection = new FeatureCollection();
		features.removeIf(Objects::isNull);
		featureCollection.setFeatures(features);

		return ResponseEntity.ok(gson.toJson(featureCollection));
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
