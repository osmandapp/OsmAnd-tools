package net.osmand.server.controllers.pub;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.ObjectMapper;

import net.osmand.server.DatasourceConfiguration;
import net.osmand.server.osmgpx.SyncRemoteOsmGpx;
import net.osmand.server.osmgpx.SyncRemoteOsmGpx.MetadataColumns;
import net.osmand.server.osmgpx.SyncRemoteOsmGpx.Track;

@RestController
public class OsmGpxSyncController {

	private final JdbcTemplate jdbcTemplate;
	private final DatasourceConfiguration config;
	private final String apiKey;
	private final ObjectMapper json = new ObjectMapper();
	private MetadataColumns metadataColumns;

	public OsmGpxSyncController(@Qualifier("osmgpxJdbcTemplate") JdbcTemplate jdbcTemplate, DatasourceConfiguration config,
			@Value("${OSM_GPX_API_KEY:}") String apiKey) {
		this.jdbcTemplate = jdbcTemplate;
		this.config = config;
		this.apiKey = apiKey;

		// PostgreSQL row_to_json hex-encodes simplified_geometry (bytea): two characters per byte.
		// Large geometries exceed Jackson 2.15.0's 5,000,000-character string limit on the server.
		// The CLI uses Jackson 2.14.2, which has no such limit.
		StreamReadConstraints constraints = StreamReadConstraints.builder()
				.maxStringLength(Integer.MAX_VALUE)
				.build();
		json.getFactory().setStreamReadConstraints(constraints);
	}

	private synchronized String getMetadataColumns() {
		if (metadataColumns == null) {
			metadataColumns = jdbcTemplate.execute(SyncRemoteOsmGpx::getMetadataColumns);
		}
		return metadataColumns.sql();
	}

	@GetMapping(path = SyncRemoteOsmGpx.API_PATH, produces = MediaType.APPLICATION_JSON_VALUE)
	public List<Track> batch(@RequestHeader(value = SyncRemoteOsmGpx.API_KEY_HEADER, required = false) String key,
			@RequestParam(name = "afterId", defaultValue = "-1") long afterId,
			@RequestParam(name = "limit", defaultValue = "" + SyncRemoteOsmGpx.DEFAULT_BATCH_SIZE) int limit) {
		if (apiKey.isBlank() || key == null || !MessageDigest.isEqual(apiKey.getBytes(StandardCharsets.UTF_8),
				key.getBytes(StandardCharsets.UTF_8))) {
			throw new ResponseStatusException(HttpStatus.FORBIDDEN);
		}
		if (afterId < -1 || limit < 1 || limit > SyncRemoteOsmGpx.MAX_BATCH_SIZE) {
			throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid cursor or batch size");
		}
		if (!config.osmgpxInitialized()) {
			throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "OsmGpx datasource is not initialized");
		}
		String metadataColumns = getMetadataColumns();
		return jdbcTemplate.query("SELECT row_to_json(m)::text AS metadata, f.id AS file_id, f.data FROM"
				+ " (SELECT " + metadataColumns + " FROM osm_gpx_data WHERE id > ? ORDER BY id LIMIT ?) m"
				+ " LEFT JOIN osm_gpx_files f ON f.id = m.id ORDER BY m.id", (ResultSetExtractor<List<Track>>) rs -> {
			List<Track> tracks = new ArrayList<>();
			while (rs.next()) {
				// Download writes metadata and files separately. Never advance past an unfinished pair.
				if (rs.getObject("file_id") == null) {
					if (tracks.isEmpty()) {
						throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Next GPX file is not available yet");
					}
					break;
				}
				try {
					tracks.add(new Track(json.readTree(rs.getString("metadata")), rs.getBytes("data")));
				} catch (IOException e) {
					throw new IllegalStateException("Invalid OsmGpx metadata", e);
				}
			}
			return tracks;
		}, afterId, limit);
	}
}
