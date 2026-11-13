package net.osmand.server.osmgpx;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.http.HttpClient;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;

import net.osmand.server.DatasourceConfiguration;
import net.osmand.server.controllers.pub.OsmGpxSyncController;
import net.osmand.server.osmgpx.SyncRemoteOsmGpx.Track;

import static org.junit.Assert.*;
import static org.junit.Assume.assumeNotNull;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.*;

/** PostgreSQL integration tests; set OSMGPX_TEST_DB_URL. Every connection uses temporary tables only. */
public class SyncRemoteOsmGpxTest {

	private static final ObjectMapper JSON = new ObjectMapper();
	private static final long ID = 9007199254740993L; // must not lose bigint precision in JSON
	private Connection source;
	private Connection target;
	private OsmGpxSyncController controller;
	private HttpServer server;

	@Before
	public void setUp() throws Exception {
		String url = System.getenv("OSMGPX_TEST_DB_URL");
		assumeNotNull(url);
		source = database(url);
		target = database(url);
		DatasourceConfiguration config = new DatasourceConfiguration() {
			@Override
			public boolean osmgpxInitialized() {
				return true;
			}
		};
		controller = new OsmGpxSyncController(new JdbcTemplate(new SingleConnectionDataSource(source, true)), config, "secret");
	}

	@After
	public void tearDown() throws SQLException {
		if (server != null) {
			server.stop(0);
		}
		if (source != null) {
			source.close();
		}
		if (target != null) {
			target.close();
		}
	}

	@Test
	public void copiesAllStoredMetadataAndExcludesGeneratedColumns() throws Exception {
		Track original = track(ID);
		saveBatch(source, new Track[] {original}, -1);
		execute(source, "UPDATE osm_gpx_data SET activity = 'cycling', speed = 12.5, distance = 1234.5, points = 100,"
				+ " max_speed = 20, max_dist_between_points = 30, time_minutes = 10, waypoints = 2,"
				+ " simplified_geometry = decode('00ff01', 'hex'), speed_matches_activity = true, file_activity = 'cycling',"
				+ " activity_source = 'file', track_stats = '{\"v\":1}', manual_review = '{\"admin\":{}}'");
		List<Track> batch = readBatch(-1, 100);
		assertEquals(1, batch.size());
		assertEquals("cycling", batch.get(0).metadata().get("activity").textValue());
		assertTrue(batch.get(0).metadata().has("manual_review"));
		assertFalse(batch.get(0).metadata().has("bbox"));
		assertEquals(ID, saveBatch(target, JSON.readValue(JSON.writeValueAsBytes(batch), Track[].class), -1));
		try (Statement statement = target.createStatement(); ResultSet rs = statement.executeQuery(
				"SELECT m.*, f.data FROM osm_gpx_data m JOIN osm_gpx_files f USING (id)")) {
			assertTrue(rs.next());
			assertEquals(ID, rs.getLong("id"));
			assertEquals("O'Brien — трек", rs.getString("name"));
			assertEquals("2026-11-12 01:02:03.123456", rs.getTimestamp("date").toString());
			assertNull(rs.getObject("minlat"));
			assertEquals(Double.NaN, rs.getDouble("maxlat"), 0);
			assertArrayEquals(new String[] {"hiking", "тест"}, (String[]) rs.getArray("tags").getArray());
			assertArrayEquals(original.gpx(), rs.getBytes("data"));
			assertEquals("cycling", rs.getString("activity"));
			assertEquals("O'Brien — трек:bbox", rs.getString("bbox"));
		}
		try (Statement statement = target.createStatement(); ResultSet rs = statement.executeQuery(
				"SELECT row_to_json(m)::text FROM (SELECT " + SyncRemoteOsmGpx.getMetadataColumns(target).sql() + " FROM osm_gpx_data) m")) {
			assertTrue(rs.next());
			assertEquals(batch.get(0).metadata(), JSON.readTree(rs.getString(1)));
		}
		assertTrue(controller.batch("secret", ID, 100).isEmpty());
	}

	@Test
	public void refreshesColumnsBetweenRequestsAndQuotesTheirNames() throws Exception {
		saveBatch(source, new Track[] {track(ID)}, -1);
		assertEquals(1, controller.batch("secret", -1, 100).size());
		for (Connection connection : new Connection[] {source, target}) {
			execute(connection, "ALTER TABLE osm_gpx_data ADD COLUMN \"future field\" text, ADD COLUMN obsolete text");
			execute(connection, "ALTER TABLE osm_gpx_data DROP COLUMN obsolete");
		}
		execute(source, "UPDATE osm_gpx_data SET \"future field\" = 'new metadata'");
		List<Track> batch = readBatch(-1, 100);
		assertEquals("new metadata", batch.get(0).metadata().get("future field").textValue());
		saveBatch(target, batch.toArray(Track[]::new), -1);
		try (Statement statement = target.createStatement();
			 ResultSet rs = statement.executeQuery("SELECT \"future field\" FROM osm_gpx_data")) {
			assertTrue(rs.next());
			assertEquals("new metadata", rs.getString(1));
		}
	}

	@Test
	public void rejectsSourceColumnsMissingOnTheTargetWithoutLosingData() throws Exception {
		execute(source, "ALTER TABLE osm_gpx_data ADD COLUMN source_only text");
		saveBatch(source, new Track[] {track(ID)}, -1);
		execute(source, "UPDATE osm_gpx_data SET source_only = 'must not be lost'");
		Track[] tracks = readBatch(-1, 100).toArray(Track[]::new);
		assertThrows(IllegalArgumentException.class, () -> saveBatch(target, tracks, -1));
		assertEquals(0, count(target, "osm_gpx_data"));
		assertEquals(0, count(target, "osm_gpx_files"));
		assertTrue(target.getAutoCommit());
	}

	@Test
	public void skipsMissingFilesAndLimitsJoinedRows() throws Exception {
		saveBatch(source, new Track[] {track(ID), track(ID + 1), new Track(track(ID + 2).metadata(), null)}, -1);
		execute(source, "DELETE FROM osm_gpx_files WHERE id = " + (ID + 1));
		List<Track> batch = readBatch(-1, 2);
		assertEquals(2, batch.size());
		assertEquals(ID, batch.get(0).metadata().get("id").longValue());
		assertEquals(ID + 2, batch.get(1).metadata().get("id").longValue());
		assertNull(batch.get(1).gpx());
		assertEquals(ID + 2, saveBatch(target, batch.toArray(Track[]::new), -1));
		assertEquals(2, count(target, "osm_gpx_data"));
		assertEquals(2, count(target, "osm_gpx_files"));
		List<Track> next = readBatch(ID, 1);
		assertEquals(1, next.size());
		assertEquals(ID + 2, next.get(0).metadata().get("id").longValue());
		assertTrue(controller.batch("secret", ID + 2, 1).isEmpty());
	}

	@Test
	public void failedFileInsertRollsBackMetadataAndAllowsRetry() throws Exception {
		execute(target, "ALTER TABLE osm_gpx_files ADD CONSTRAINT reject_file CHECK (id <> " + ID + ")");
		assertThrows(SQLException.class, () -> saveBatch(target, new Track[] {track(ID)}, -1));
		assertEquals(0, count(target, "osm_gpx_data"));
		assertEquals(0, count(target, "osm_gpx_files"));
		assertTrue(target.getAutoCommit());
		execute(target, "ALTER TABLE osm_gpx_files DROP CONSTRAINT reject_file");
		saveBatch(target, new Track[] {track(ID)}, -1);
		assertEquals(1, count(target, "osm_gpx_files"));
	}

	@Test
	public void outOfMemoryRollsBackMetadataAndAllowsRetry() throws Exception {
		OutOfMemoryError error = new OutOfMemoryError("Simulated OOM after metadata insert");
		Connection failing = mock(Connection.class, delegatesTo(target));
		doAnswer(invocation -> {
			PreparedStatement statement = target.prepareStatement(invocation.getArgument(0));
			PreparedStatement batch = mock(PreparedStatement.class, delegatesTo(statement));
			doAnswer(ignored -> {
				statement.executeBatch();
				throw error;
			}).when(batch).executeBatch();
			return batch;
		}).when(failing).prepareStatement(startsWith("INSERT INTO osm_gpx_data"));
		Track[] tracks = {track(ID), track(ID + 1)};
		assertSame(error, assertThrows(OutOfMemoryError.class, () -> saveBatch(failing, tracks, -1)));
		assertEquals(0, count(target, "osm_gpx_data"));
		assertEquals(0, count(target, "osm_gpx_files"));
		assertTrue(target.getAutoCommit());
		assertEquals(ID + 1, saveBatch(target, tracks, -1));
		assertEquals(2, count(target, "osm_gpx_data"));
		assertEquals(2, count(target, "osm_gpx_files"));
	}

	@Test
	public void failedRollbackDoesNotEnableAutocommit() throws Exception {
		execute(target, "ALTER TABLE osm_gpx_files ADD CONSTRAINT reject_file CHECK (id <> " + ID + ")");
		Connection failing = mock(Connection.class, delegatesTo(target));
		SQLException rollbackError = new SQLException("Simulated rollback failure");
		doThrow(rollbackError).when(failing).rollback();
		SQLException error = assertThrows(SQLException.class, () -> saveBatch(failing, new Track[] {track(ID)}, -1));
		assertEquals("23514", error.getSQLState()); // Original CHECK constraint violation.
		assertArrayEquals(new Throwable[] {rollbackError}, error.getSuppressed());
		verify(failing, never()).setAutoCommit(true);
		assertFalse(target.getAutoCommit());
	}

	@Test
	public void failedRollbackPreservesOutOfMemoryError() throws Exception {
		Connection failing = mock(Connection.class, delegatesTo(target));
		OutOfMemoryError error = new OutOfMemoryError("Simulated OOM preparing metadata insert");
		SQLException rollbackError = new SQLException("Simulated rollback failure");
		doThrow(error).when(failing).prepareStatement(startsWith("INSERT INTO osm_gpx_data"));
		doThrow(rollbackError).when(failing).rollback();
		assertSame(error, assertThrows(OutOfMemoryError.class, () -> saveBatch(failing, new Track[] {track(ID)}, -1)));
		assertArrayEquals(new Throwable[] {rollbackError}, error.getSuppressed());
		verify(failing, never()).setAutoCommit(true);
		assertFalse(target.getAutoCommit());
	}

	@Test
	public void failedAutocommitRestorePreservesOriginalError() throws Exception {
		Connection failing = mock(Connection.class, delegatesTo(target));
		SQLException error = new SQLException("Simulated metadata insert failure");
		SQLException restoreError = new SQLException("Simulated autocommit restore failure");
		doThrow(error).when(failing).prepareStatement(startsWith("INSERT INTO osm_gpx_data"));
		doThrow(restoreError).when(failing).setAutoCommit(true);
		assertSame(error, assertThrows(SQLException.class, () -> saveBatch(failing, new Track[] {track(ID)}, -1)));
		assertArrayEquals(new Throwable[] {restoreError}, error.getSuppressed());
		verify(failing).rollback();
		assertFalse(target.getAutoCommit());
	}

	@Test
	public void exportsGeometryLargerThanDefaultJsonStringLimit() throws Exception {
		saveBatch(source, new Track[] {track(ID)}, -1);
		// Hex in JSON exceeds Jackson 2.15's default 5,000,000-character string limit.
		byte[] geometry = new byte[3_000_000];
		Arrays.fill(geometry, (byte) 0xab);
		try (PreparedStatement update = source.prepareStatement("UPDATE osm_gpx_data SET simplified_geometry = ? WHERE id = ?")) {
			update.setBytes(1, geometry);
			update.setLong(2, ID);
			update.executeUpdate();
		}
		String response = JSON.writeValueAsString(controller.batch("secret", -1, 1));
		try (Statement statement = source.createStatement();
			 ResultSet rs = statement.executeQuery("SELECT row_to_json(m)::text FROM (SELECT "
					 + SyncRemoteOsmGpx.getMetadataColumns(source).sql() + " FROM osm_gpx_data) m")) {
			assertTrue(rs.next());
			assertTrue(response.contains("\"metadata\":" + rs.getString(1)));
		}
		assertTrue(response.contains("\"gpx\":" + JSON.writeValueAsString(track(ID).gpx())));
	}

	@Test
	public void resumesAfterHttpFailureAndBatchLimitAndPreservesLocalChanges() throws Exception {
		saveBatch(source, new Track[] {track(ID), track(ID + 1), track(ID + 2)}, -1);
		AtomicBoolean fail = new AtomicBoolean(true);
		AtomicInteger requests = new AtomicInteger();
		server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
		server.createContext(SyncRemoteOsmGpx.API_PATH, exchange -> {
			requests.incrementAndGet();
			String[] params = exchange.getRequestURI().getQuery().split("&");
			long after = Long.parseLong(params[0].split("=")[1]);
			int limit = Integer.parseInt(params[1].split("=")[1]);
			try (exchange) {
				if (after == ID && fail.getAndSet(false)) {
					exchange.sendResponseHeaders(503, -1);
					return;
				}
				byte[] body = JSON.writeValueAsBytes(controller.batch(exchange.getRequestHeaders().getFirst(SyncRemoteOsmGpx.API_KEY_HEADER), after, limit));
				exchange.sendResponseHeaders(200, body.length);
				exchange.getResponseBody().write(body);
			}
		});
		server.start();
		String url = "http://127.0.0.1:" + server.getAddress().getPort() + SyncRemoteOsmGpx.API_PATH;
		HttpClient client = HttpClient.newHttpClient();
		assertThrows(IOException.class, () -> SyncRemoteOsmGpx.downloadAndInsertBatches(target, client, url, "secret", 1, 0));
		assertEquals(1, count(target, "osm_gpx_data"));
		execute(target, "UPDATE osm_gpx_data SET activity = 'walking', name = 'local'");
		SyncRemoteOsmGpx.downloadAndInsertBatches(target, client, url, "secret", 1, 1);
		assertEquals(2, count(target, "osm_gpx_data"));
		assertEquals(2, count(target, "osm_gpx_files"));
		assertEquals(3, requests.get()); // No extra request after reaching the batch limit.
		SyncRemoteOsmGpx.downloadAndInsertBatches(target, client, url, "secret", 1, 0);
		assertEquals(3, count(target, "osm_gpx_data"));
		saveBatch(target, new Track[] {track(ID)}, -1);
		try (Statement statement = target.createStatement();
			 ResultSet rs = statement.executeQuery("SELECT name, activity FROM osm_gpx_data WHERE id = " + ID)) {
			assertTrue(rs.next());
			assertEquals("local", rs.getString(1));
			assertEquals("walking", rs.getString(2));
		}
	}

	private static long saveBatch(Connection connection, Track[] tracks, long afterId) throws SQLException {
		return SyncRemoteOsmGpx.saveBatch(connection, SyncRemoteOsmGpx.getMetadataColumns(connection), tracks, afterId);
	}

	private List<Track> readBatch(long afterId, int limit) throws IOException {
		return Arrays.asList(JSON.readValue(JSON.writeValueAsBytes(controller.batch("secret", afterId, limit)), Track[].class));
	}

	private static Track track(long id) throws IOException {
		return new Track(JSON.readTree("{\"id\":" + id + ",\"user\":\"author\",\"name\":\"O'Brien — трек\","
				+ "\"date\":\"2026-11-12T01:02:03.123456\",\"minlat\":null,\"maxlat\":\"NaN\",\"tags\":[\"hiking\",\"тест\"]}"),
				new byte[] {31, -117, 0, -1});
	}

	private static Connection database(String url) throws SQLException {
		Connection connection = DriverManager.getConnection(url);
		execute(connection, "CREATE TEMP TABLE osm_gpx_data (id bigint PRIMARY KEY, \"user\" text, date timestamp, name text,"
				+ "lat float, lon float, minlat float, minlon float, maxlat float, maxlon float, pending boolean,"
				+ "visibility text, tags text[], description text, activity text, speed float, distance float, points integer,"
				+ "max_speed float, max_dist_between_points float, time_minutes integer, waypoints integer, simplified_geometry bytea,"
				+ "speed_matches_activity boolean, file_activity text, activity_source text, track_stats json, manual_review jsonb,"
				+ "bbox text GENERATED ALWAYS AS (name || ':bbox') STORED)");
		execute(connection, "CREATE TEMP TABLE osm_gpx_files (id bigint PRIMARY KEY, data bytea)");
		return connection;
	}

	private static void execute(Connection connection, String sql) throws SQLException {
		try (Statement statement = connection.createStatement()) {
			statement.execute(sql);
		}
	}

	private static int count(Connection connection, String table) throws SQLException {
		try (Statement statement = connection.createStatement(); ResultSet rs = statement.executeQuery("SELECT count(*) FROM " + table)) {
			rs.next();
			return rs.getInt(1);
		}
	}
}
