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
import com.fasterxml.jackson.databind.ObjectMapper;

import net.osmand.MainUtilities.CommandLineOpts;

/** Copies GPX and stored metadata once; subsequent analysis runs on the receiving server. */
public class SyncRemoteOsmGpx {

	public static final String API_PATH = "/osmgpx/sync-batch";
	public static final String API_KEY_HEADER = "X-OSM-GPX-API-KEY";
	public static final int DEFAULT_BATCH_SIZE = 1000;
	public static final int MAX_BATCH_SIZE = 5000;
	private static final ObjectMapper JSON = new ObjectMapper();

	// JSON preserves nullable coordinates, timestamp precision and bigint ids without manual conversions.
	public record Track(JsonNode metadata, byte[] gpx) {
	}

	public record MetadataColumns(String sql, Set<String> names) {
	}

	public static MetadataColumns getMetadataColumns(Connection connection) throws SQLException {
		try (Statement statement = connection.createStatement();
			 ResultSet rs = statement.executeQuery("SELECT string_agg(quote_ident(attname), ', ' ORDER BY attnum), array_agg(attname::text)"
					 + " FROM pg_attribute WHERE attrelid = 'osm_gpx_data'::regclass"
					 + " AND attnum > 0 AND NOT attisdropped AND attgenerated = ''")) {
			rs.next();
			return new MetadataColumns(rs.getString(1), Set.of((String[]) rs.getArray(2).getArray()));
		}
	}

	public static void main(String[] args) throws Exception {
		CommandLineOpts opts = new CommandLineOpts(args);
		if (opts.getStrings().size() != 1) {
			throw new IllegalArgumentException("Usage: sync-remote-osm-gpx <builder-url> [--batch-size="
					+ DEFAULT_BATCH_SIZE + "] [--max-batches=N] (0 = unlimited)");
		}
		int batchSize = opts.getIntOrDefault("--batch-size", DEFAULT_BATCH_SIZE);
		int maxBatches = opts.getIntOrDefault("--max-batches", 0);
		if (batchSize < 1 || batchSize > MAX_BATCH_SIZE) {
			throw new IllegalArgumentException("Batch size must be 1.." + MAX_BATCH_SIZE);
		}
		if (maxBatches < 0) {
			throw new IllegalArgumentException("Max batches must be >= 0 (0 = unlimited)");
		}
		String apiKey = System.getenv("OSM_GPX_API_KEY");
		if (apiKey == null || apiKey.isBlank()) {
			throw new IllegalArgumentException("OSM_GPX_API_KEY is required");
		}
		String url = opts.getStrings().get(0).replaceAll("/+$", "") + API_PATH;
		HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(30)).build();
		try (Connection connection = DownloadOsmGPX.openDatabase()) {
			DownloadOsmGPX.ensureActivitySchema(connection);
			downloadAndInsertBatches(connection, client, url, apiKey, batchSize, maxBatches);
		}
	}

	static void downloadAndInsertBatches(Connection connection, HttpClient client, String url, String apiKey,
			int batchSize, int maxBatches) throws Exception {
		long startedNanos = System.nanoTime();
		long totalTracks = 0;
		MetadataColumns metadataColumns = getMetadataColumns(connection);
		long afterId;
		try (Statement statement = connection.createStatement();
			 ResultSet rs = statement.executeQuery("SELECT COALESCE(max(id), -1) FROM osm_gpx_data")) {
			rs.next();
			afterId = rs.getLong(1);
		}
		System.out.println("Syncing OsmGpx after id " + afterId);
		for (int batch = 0; maxBatches == 0 || batch < maxBatches; batch++) {
			HttpRequest request = HttpRequest.newBuilder(URI.create(url + "?afterId=" + afterId + "&limit=" + batchSize))
					.header(API_KEY_HEADER, apiKey).timeout(Duration.ofMinutes(5)).GET().build();
			HttpResponse<InputStream> response = client.send(request, HttpResponse.BodyHandlers.ofInputStream());
			Track[] tracks;
			try (InputStream body = response.body()) {
				if (response.statusCode() != 200) {
					throw new IOException("OsmGpx batch request failed: HTTP " + response.statusCode());
				}
				tracks = JSON.readValue(body, Track[].class);
			}
			if (tracks == null || tracks.length > batchSize) {
				throw new IOException("Invalid OsmGpx batch");
			}
			if (tracks.length == 0) {
				System.out.println("OsmGpx sync complete at id " + afterId + ", total " + totalTracks + " tracks");
				return;
			}
			afterId = saveBatch(connection, metadataColumns, tracks, afterId);
			totalTracks += tracks.length;
			double tracksPerHour = totalTracks * (double) Duration.ofHours(1).toNanos()
					/ Math.max(1, System.nanoTime() - startedNanos);
			System.out.printf("Synced %d tracks, total %d, average %.0f tracks/hour, last id %d%n",
					tracks.length, totalTracks, tracksPerHour, afterId);
		}
		System.out.println("OsmGpx batch limit reached, total " + totalTracks + " tracks, last id " + afterId);
	}

	static long saveBatch(Connection connection, MetadataColumns metadataColumns, Track[] tracks, long afterId) throws SQLException {
		connection.setAutoCommit(false);
		boolean committed = false;
		try (PreparedStatement metadata = connection.prepareStatement("INSERT INTO osm_gpx_data (" + metadataColumns.sql() + ")"
				+ " SELECT " + metadataColumns.sql() + " FROM json_populate_record(NULL::osm_gpx_data, ?::json)"
				+ " ON CONFLICT (id) DO NOTHING");
			 PreparedStatement files = connection.prepareStatement("INSERT INTO osm_gpx_files (id, data) VALUES (?, ?)"
					 + " ON CONFLICT (id) DO NOTHING")) {
			for (Track track : tracks) {
				JsonNode id = track == null || track.metadata() == null ? null : track.metadata().get("id");
				if (id == null || !id.isIntegralNumber() || !id.canConvertToLong() || id.longValue() <= afterId) {
					throw new IllegalArgumentException("OsmGpx batch ids must be increasing bigint values");
				}
				for (Iterator<String> fields = track.metadata().fieldNames(); fields.hasNext();) {
					String field = fields.next();
					if (!metadataColumns.names().contains(field)) {
						throw new IllegalArgumentException("Unknown OsmGpx metadata column: " + field);
					}
				}
				afterId = id.longValue();
				metadata.setString(1, track.metadata().toString());
				metadata.addBatch();
				files.setLong(1, afterId);
				files.setBytes(2, track.gpx());
				files.addBatch();
			}
			metadata.executeBatch();
			files.executeBatch();
			connection.commit();
			committed = true;
			return afterId;
		} finally {
			if (!committed) {
				connection.rollback();
			}
			connection.setAutoCommit(true);
		}
	}
}
