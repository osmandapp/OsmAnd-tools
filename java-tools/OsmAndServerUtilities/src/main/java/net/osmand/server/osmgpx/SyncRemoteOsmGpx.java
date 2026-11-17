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

import javax.annotation.Nullable;

import com.fasterxml.jackson.databind.JsonNode;

import net.osmand.MainUtilities.CommandLineOpts;
import net.osmand.util.Algorithms;

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

	// JSON preserves nullable coordinates and timestamp precision without manual conversions.
	public record OsmGpxTrack(JsonNode metadata, byte[] gpx) {
	}

	public record MetadataColumns(String sql, Set<String> names) {
	}

	public static MetadataColumns getMetadataColumns(Connection connection) throws SQLException {
		try (Statement statement = connection.createStatement();
			 ResultSet rs = statement.executeQuery("SELECT string_agg(quote_ident(attname), ', ' ORDER BY attnum), array_agg(attname::text)"
					 + " FROM pg_attribute WHERE attrelid = '" + GPX_METADATA_TABLE_NAME + "'::regclass"
					 + " AND attnum > 0 AND NOT attisdropped AND attgenerated = ''")) {
			rs.next();
			return new MetadataColumns(rs.getString(1), Set.of((String[]) rs.getArray(2).getArray()));
		}
	}

	public static void main(String[] args) throws Exception {
		CommandLineOpts opts = new CommandLineOpts(args);
		if (opts.getStrings().size() != 1) {
			throw new IllegalArgumentException("""
					Usage: sync-remote-osm-gpx <builder-url> [options]
					  --batch-size=N   Tracks per batch (default: %d)
					  --max-batches=N  Batch limit (default: 0 = unlimited)
					  --from-id=N      First ID, inclusive (default: no lower bound)
					  --to-id=N        Last ID, inclusive (default: no upper bound)
					No ID options: sync new tracks. Otherwise: resync the range.

					Example: sync-remote-osm-gpx https://builder.osmand.net --from-id=1000 --to-id=2000
					""".formatted(DEFAULT_BATCH_SIZE));
		}
		int batchSize = opts.getIntOrDefault("--batch-size", DEFAULT_BATCH_SIZE);
		int maxBatches = opts.getIntOrDefault("--max-batches", 0);
		Integer fromId = parseId(opts.getOpt("--from-id"));
		Integer toId = parseId(opts.getOpt("--to-id"));
		if (batchSize < 1 || batchSize > MAX_BATCH_SIZE) {
			throw new IllegalArgumentException("Batch size must be 1.." + MAX_BATCH_SIZE);
		}
		if (maxBatches < 0) {
			throw new IllegalArgumentException("Max batches must be >= 0 (0 = unlimited)");
		}
		if (fromId != null && toId != null && fromId > toId) {
			throw new IllegalArgumentException("from-id must be <= to-id");
		}
		String apiKey = System.getenv(API_KEY_ENV);
		if (Algorithms.isBlank(apiKey)) {
			throw new IllegalArgumentException(API_KEY_ENV + " is required");
		}
		String url = opts.getStrings().get(0).replaceAll("/+$", "") + API_PATH;
		HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(30)).build();
		try (Connection connection = DownloadOsmGPX.openDatabase()) {
			DownloadOsmGPX.ensureActivitySchema(connection);
			downloadAndInsertBatches(connection, client, url, apiKey, batchSize, maxBatches, fromId, toId);
		}
	}

	@Nullable
	private static Integer parseId(String value) {
		if (value == null) {
			return null;
		}
		int id = Algorithms.parseIntSilently(value, -1);
		if (id < 0) {
			throw new IllegalArgumentException("IDs must be integers >= 0: " + value);
		}
		return id;
	}

	static void downloadAndInsertBatches(Connection connection, HttpClient client, String url, String apiKey,
			int batchSize, int maxBatches, Integer fromId, Integer toId) throws Exception {
		long startedNanos = System.nanoTime();
		long totalTracks = 0;
		MetadataColumns metadataColumns = getMetadataColumns(connection);
		boolean resync = fromId != null || toId != null;
		if (!resync) {
			try (Statement statement = connection.createStatement();
				 ResultSet rs = statement.executeQuery("SELECT COALESCE(max(id), -1) FROM " + GPX_METADATA_TABLE_NAME)) {
				rs.next();
				int lastId = rs.getInt(1);
				if (lastId == Integer.MAX_VALUE) {
					System.out.println("OsmGpx sync complete at id " + lastId);
					return;
				}
				fromId = lastId + 1;
			}
		}
		System.out.println((resync ? "Resyncing" : "Syncing") + " OsmGpx"
				+ (fromId == null ? "" : " from id " + fromId) + (toId == null ? "" : ", through id " + toId));
		for (int batch = 0; maxBatches == 0 || batch < maxBatches; batch++) {
			HttpRequest request = HttpRequest.newBuilder(URI.create(url + "?limit=" + batchSize
					+ (fromId == null ? "" : "&fromId=" + fromId) + (toId == null ? "" : "&toId=" + toId)))
					.header(API_KEY_HEADER, apiKey).timeout(Duration.ofMinutes(5)).GET().build();
			HttpResponse<InputStream> response = client.send(request, HttpResponse.BodyHandlers.ofInputStream());
			OsmGpxTrack[] tracks;
			try (InputStream body = response.body()) {
				if (response.statusCode() != 200) {
					throw new IOException("OsmGpx batch request failed: HTTP " + response.statusCode());
				}
				tracks = JSON_MAPPER.readValue(body, OsmGpxTrack[].class);
			}
			if (tracks == null || tracks.length > batchSize) {
				throw new IOException("Invalid OsmGpx batch");
			}
			if (tracks.length == 0) {
				System.out.println("OsmGpx sync complete, next id " + fromId + ", total " + totalTracks + " tracks");
				return;
			}
			if (toId != null && tracks[tracks.length - 1].metadata().path("id").asInt() > toId) {
				throw new IOException("OsmGpx batch exceeds requested id range");
			}
			int lastId = saveBatch(connection, metadataColumns, tracks, fromId, resync);
			totalTracks += tracks.length;
			double tracksPerHour = totalTracks * (double) Duration.ofHours(1).toNanos()
					/ Math.max(1, System.nanoTime() - startedNanos);
			System.out.printf("Synced %d tracks, total %d, average %.0f tracks/hour, last id %d%n",
					tracks.length, totalTracks, tracksPerHour, lastId);
			if (lastId == Integer.MAX_VALUE || (toId != null && lastId == toId)) {
				System.out.println("OsmGpx sync complete at id " + lastId + ", total " + totalTracks + " tracks");
				return;
			}
			fromId = lastId + 1;
		}
		System.out.println("OsmGpx batch limit reached, total " + totalTracks + " tracks, next id " + fromId);
	}

	static int saveBatch(Connection connection, MetadataColumns metadataColumns, OsmGpxTrack[] tracks, Integer fromId,
	                     boolean doUpdate) throws SQLException {
		// Missing JSON fields keep their current values during resync.
		String metadataConflict = doUpdate
				? "DO UPDATE SET (" + metadataColumns.sql() + ") = (SELECT " + metadataColumns.sql()
						+ " FROM json_populate_record(" + GPX_METADATA_TABLE_NAME + ", (SELECT metadata FROM incoming)))"
				: "DO NOTHING";
		String filesConflict = doUpdate ? "DO UPDATE SET data = EXCLUDED.data" : "DO NOTHING";
		int lastId = fromId == null ? -1 : fromId - 1;
		connection.setAutoCommit(false);
		try (PreparedStatement metadata = connection.prepareStatement("WITH incoming AS (SELECT ?::json AS metadata)"
				+ " INSERT INTO " + GPX_METADATA_TABLE_NAME + " (" + metadataColumns.sql() + ")"
				+ " SELECT " + metadataColumns.sql() + " FROM json_populate_record(NULL::" + GPX_METADATA_TABLE_NAME
				+ ", (SELECT metadata FROM incoming))"
				+ " ON CONFLICT (id) " + metadataConflict);
			 PreparedStatement files = connection.prepareStatement("INSERT INTO " + GPX_FILES_TABLE_NAME + " (id, data) VALUES (?, ?)"
					 + " ON CONFLICT (id) " + filesConflict)) {
			for (OsmGpxTrack track : tracks) {
				JsonNode id = track == null || track.metadata() == null ? null : track.metadata().get("id");
				if (id == null || !id.isIntegralNumber() || !id.canConvertToInt() || id.intValue() <= lastId) {
					throw new IllegalArgumentException("OsmGpx batch ids must be increasing int values");
				}
				for (Iterator<String> fields = track.metadata().fieldNames(); fields.hasNext();) {
					String field = fields.next();
					if (!metadataColumns.names().contains(field)) {
						throw new IllegalArgumentException("Unknown OsmGpx metadata column: " + field);
					}
				}
				lastId = id.intValue();
				metadata.setString(1, track.metadata().toString());
				metadata.addBatch();
				files.setInt(1, lastId);
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
