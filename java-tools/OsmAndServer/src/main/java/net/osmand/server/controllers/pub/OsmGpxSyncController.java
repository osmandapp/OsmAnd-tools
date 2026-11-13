package net.osmand.server.controllers.pub;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
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

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.util.RawValue;

import net.osmand.server.DatasourceConfiguration;
import net.osmand.server.osmgpx.SyncRemoteOsmGpx;
import net.osmand.server.osmgpx.SyncRemoteOsmGpx.Track;

import static net.osmand.server.controllers.pub.OsmGpxController.GPX_FILES_TABLE_NAME;
import static net.osmand.server.controllers.pub.OsmGpxController.GPX_METADATA_TABLE_NAME;

@RestController
public class OsmGpxSyncController {

	private final JdbcTemplate jdbcTemplate;
	private final DatasourceConfiguration config;
	private final String apiKey;

	@Autowired
	public OsmGpxSyncController(@Qualifier("osmgpxJdbcTemplate") JdbcTemplate jdbcTemplate, DatasourceConfiguration config,
			@Value("${" + SyncRemoteOsmGpx.API_KEY_ENV + ":}") String apiKey) {
		this.jdbcTemplate = jdbcTemplate;
		this.config = config;
		this.apiKey = apiKey;
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
		String metadataColumnsSql = jdbcTemplate.execute(SyncRemoteOsmGpx::getMetadataColumns).sql();
		return jdbcTemplate.query("SELECT row_to_json(m)::text AS metadata, f.data FROM"
				+ " (SELECT " + metadataColumnsSql + " FROM " + GPX_METADATA_TABLE_NAME
				+ " WHERE id > ?) m"
				+ " INNER JOIN " + GPX_FILES_TABLE_NAME + " f ON f.id = m.id ORDER BY m.id LIMIT ?", (ResultSetExtractor<List<Track>>) rs -> {
			List<Track> tracks = new ArrayList<>();
			while (rs.next()) {
				tracks.add(new Track(JsonNodeFactory.instance.rawValueNode(new RawValue(rs.getString("metadata"))),
						rs.getBytes("data")));
			}
			return tracks;
		}, afterId, limit);
	}
}
