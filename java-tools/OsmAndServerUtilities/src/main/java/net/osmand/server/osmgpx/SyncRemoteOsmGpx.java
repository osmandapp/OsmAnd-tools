package net.osmand.server.osmgpx;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Iterator;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;

import net.osmand.MainUtilities.CommandLineOpts;

import static net.osmand.server.osmgpx.DownloadOsmGPX.GPX_FILES_TABLE_NAME;
import static net.osmand.server.osmgpx.DownloadOsmGPX.GPX_METADATA_TABLE_NAME;
import static net.osmand.server.osmgpx.DownloadOsmGPX.JSON_MAPPER;

/** Copies new GPX and metadata; explicit inclusive id ranges also replace existing rows. */
public class SyncRemoteOsmGpx {

	public static final String API_PATH = "/osmgpx/sync-batch";
	public static final String API_KEY_HEADER = "X-OSM-GPX-API-KEY";
	public static final String API_KEY_ENV = "OSM_GPX_API_KEY";
	public static final int DEFAULT_BATCH_SIZE = 1000;
	public static final int MAX_BATCH_SIZE = 5000;

	// JSON preserves nullable coordinates, timestamp precision and bigint ids without manual conversions.
	public record Track(JsonNode metadata, byte[] gpx) {
	}

	public record MetadataColumns(String sql, Set<String> names, String updateSql) {
	}

	public static MetadataColumns getMetadataColumns(Connection connection) throws SQLException {
		try (Statement statement = connection.createStatement();
			 ResultSet rs = statement.executeQuery("SELECT string_agg(quote_ident(attname), ', ' ORDER BY attnum), array_agg(attname::text),"
					 + " string_agg(quote_ident(attname) || ' = EXCLUDED.' || quote_ident(attname), ', ' ORDER BY attnum)"
					 + " FILTER (WHERE attname <> 'id')"
					 + " FROM pg_attribute WHERE attrelid = '" + GPX_METADATA_TABLE_NAME + "'::regclass"
					 + " AND attnum > 0 AND NOT attisdropped AND attgenerated = ''")) {
			rs.next();
			return new MetadataColumns(rs.getString(1), Set.of((String[]) rs.getArray(2).getArray()), rs.getString(3));
		}
	}

	public static void main(String[] args) throws Exception {
		CommandLineOpts opts = new CommandLineOpts(args);
		if (opts.getStrings().size() != 1) {
			throw new IllegalArgumentException("""
					Usage: sync-remote-osm-gpx <builder-url> [options]
					  --batch-size=N   Tracks per batch (default: %d)
					  --max-batches=N  Batch limit (default: 0 = unlimited)
					  --from-id=N      First ID, inclusive (default: 0 = no lower bound)
					  --to-id=N        Last ID, inclusive (default: 0 = no upper bound)
					Both IDs at 0: sync new tracks. Otherwise: resync the range.

					Example: sync-remote-osm-gpx https://builder.osmand.net --from-id=1000 --to-id=2000
					""".formatted(DEFAULT_BATCH_SIZE));
		}
		int batchSize = opts.getIntOrDefault("--batch-size", DEFAULT_BATCH_SIZE);
		int maxBatches = opts.getIntOrDefault("--max-batches", 0);
		int fromId = opts.getIntOrDefault("--from-id", 0);
		int toId = opts.getIntOrDefault("--to-id", 0);
		if (batchSize < 1 || batchSize > MAX_BATCH_SIZE) {
			throw new IllegalArgumentException("Batch size must be 1.." + MAX_BATCH_SIZE);
		}
		if (maxBatches < 0) {
			throw new IllegalArgumentException("Max batches must be >= 0 (0 = unlimited)");
		}
		if (fromId < 0 || toId < 0 || (toId != 0 && fromId > toId)) {
			throw new IllegalArgumentException("IDs must be >= 0, with from-id <= to-id when to-id is set");
		}
		String apiKey = System.getenv(API_KEY_ENV);
		if (apiKey == null || apiKey.isBlank()) {
			throw new IllegalArgumentException(API_KEY_ENV + " is required");
		}
		String url = opts.getStrings().get(0).replaceAll("/+$", "") + API_PATH;
		HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(30)).build();
		try (Connection connection = DownloadOsmGPX.openDatabase()) {
			DownloadOsmGPX.ensureActivitySchema(connection);
			downloadAndInsertBatches(connection, client, url, apiKey, batchSize, maxBatches, fromId, toId);
		}
	}

	static void downloadAndInsertBatches(Connection connection, HttpClient client, String url, String apiKey,
			int batchSize, int maxBatches, long fromId, long toId) throws Exception {
		long startedNanos = System.nanoTime();
		long totalTracks = 0;
		MetadataColumns metadataColumns = getMetadataColumns(connection);
		boolean resync = fromId != 0 || toId != 0;
		if (!resync) {
			try (Statement statement = connection.createStatement();
				 ResultSet rs = statement.executeQuery("SELECT COALESCE(max(id), -1) FROM " + GPX_METADATA_TABLE_NAME)) {
				rs.next();
				long lastId = rs.getLong(1);
				if (lastId == Long.MAX_VALUE) {
					System.out.println("OsmGpx sync complete at id " + lastId);
					return;
				}
				fromId = lastId + 1;
			}
		}
		System.out.println((resync ? "Resyncing" : "Syncing") + " OsmGpx from id " + fromId
				+ (toId == 0 ? "" : ", through id " + toId));
		for (int batch = 0; maxBatches == 0 || batch < maxBatches; batch++) {
			HttpRequest request = HttpRequest.newBuilder(URI.create(url + "?fromId=" + fromId + "&limit=" + batchSize + "&toId=" + toId))
					.header(API_KEY_HEADER, apiKey).timeout(Duration.ofMinutes(5)).GET().build();
			HttpResponse<InputStream> response = client.send(request, HttpResponse.BodyHandlers.ofInputStream());
			Track[] tracks;
			try (InputStream body = response.body()) {
				if (response.statusCode() != 200) {
					throw new IOException("OsmGpx batch request failed: HTTP " + response.statusCode());
				}
				tracks = JSON_MAPPER.readValue(body, Track[].class);
			}
			if (tracks == null || tracks.length > batchSize) {
				throw new IOException("Invalid OsmGpx batch");
			}
			if (tracks.length == 0) {
				System.out.println("OsmGpx sync complete, next id " + fromId + ", total " + totalTracks + " tracks");
				return;
			}
			if (toId != 0 && tracks[tracks.length - 1].metadata().path("id").asLong() > toId) {
				throw new IOException("OsmGpx batch exceeds requested id range");
			}
			long lastId;
			if (resync) {
				lastId = updateBatch(connection, metadataColumns, tracks, fromId);
			} else {
				lastId = insertBatch(connection, metadataColumns, tracks, fromId);
			}
			totalTracks += tracks.length;
			double tracksPerHour = totalTracks * (double) Duration.ofHours(1).toNanos()
					/ Math.max(1, System.nanoTime() - startedNanos);
			System.out.printf("Synced %d tracks, total %d, average %.0f tracks/hour, last id %d%n",
					tracks.length, totalTracks, tracksPerHour, lastId);
			if (lastId == Long.MAX_VALUE || (toId != 0 && lastId == toId)) {
				System.out.println("OsmGpx sync complete at id " + lastId + ", total " + totalTracks + " tracks");
				return;
			}
			fromId = lastId + 1;
		}
		System.out.println("OsmGpx batch limit reached, total " + totalTracks + " tracks, next id " + fromId);
	}

	static long insertBatch(Connection connection, MetadataColumns metadataColumns, Track[] tracks, long fromId) throws SQLException {
		return saveBatch(connection, metadataColumns, tracks, fromId, "DO NOTHING", "DO NOTHING");
	}

	static long updateBatch(Connection connection, MetadataColumns metadataColumns, Track[] tracks, long fromId) throws SQLException {
		return saveBatch(connection, metadataColumns, tracks, fromId,
				"DO UPDATE SET " + metadataColumns.updateSql(), "DO UPDATE SET data = EXCLUDED.data");
	}

	private static long saveBatch(Connection connection, MetadataColumns metadataColumns, Track[] tracks, long fromId,
	                              String metadataConflict, String filesConflict) throws SQLException {
		long lastId = fromId - 1;
		connection.setAutoCommit(false);
		try (PreparedStatement metadata = connection.prepareStatement("INSERT INTO " + GPX_METADATA_TABLE_NAME + " (" + metadataColumns.sql() + ")"
				+ " SELECT " + metadataColumns.sql() + " FROM json_populate_record(NULL::" + GPX_METADATA_TABLE_NAME + ", ?::json)"
				+ " ON CONFLICT (id) " + metadataConflict);
			 PreparedStatement files = connection.prepareStatement("INSERT INTO " + GPX_FILES_TABLE_NAME + " (id, data) VALUES (?, ?)"
					 + " ON CONFLICT (id) " + filesConflict)) {
			for (Track track : tracks) {
				JsonNode id = track == null || track.metadata() == null ? null : track.metadata().get("id");
				if (id == null || !id.isIntegralNumber() || !id.canConvertToLong() || id.longValue() <= lastId) {
					throw new IllegalArgumentException("OsmGpx batch ids must be increasing bigint values");
				}
				for (Iterator<String> fields = track.metadata().fieldNames(); fields.hasNext();) {
					String field = fields.next();
					if (!metadataColumns.names().contains(field)) {
						throw new IllegalArgumentException("Unknown OsmGpx metadata column: " + field);
					}
				}
				lastId = id.longValue();
				metadata.setString(1, track.metadata().toString());
				metadata.addBatch();
				files.setLong(1, lastId);
				files.setBytes(2, track.gpx());
				files.addBatch();
			}
			metadata.executeBatch();
			files.executeBatch();
			connection.commit();
		} catch (SQLException | RuntimeException | Error error) {
			try {
				connection.rollback();
				connection.setAutoCommit(true);
			} catch (SQLException | RuntimeException | Error cleanupError) {
				error.addSuppressed(cleanupError);
			}
			throw error;
		}
		connection.setAutoCommit(true);
		return lastId;
	}
}
