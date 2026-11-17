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
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.util.MultiValueMap;
import org.springframework.web.util.UriComponentsBuilder;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;

import net.osmand.server.DatasourceConfiguration;
import net.osmand.server.controllers.pub.OsmGpxSyncController;
import net.osmand.server.osmgpx.SyncRemoteOsmGpx.OsmGpxTrack;

import static org.junit.Assert.*;
import static org.junit.Assume.assumeNotNull;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.*;

/** PostgreSQL integration tests; set OSMGPX_TEST_DB_URL. Every connection uses temporary tables only. */
public class SyncRemoteOsmGpxTest {

	private static final ObjectMapper JSON = new ObjectMapper();
	private static final int ID = 12_504_745;
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
		controller = new OsmGpxSyncController();
		ReflectionTestUtils.setField(controller, "jdbcTemplate", new JdbcTemplate(new SingleConnectionDataSource(source, true)));
		ReflectionTestUtils.setField(controller, "config", config);
		ReflectionTestUtils.setField(controller, "apiKey", "secret");
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
		OsmGpxTrack original = track(ID);
		saveBatch(source, new OsmGpxTrack[] {original}, 0);
		execute(source, "UPDATE osm_gpx_data SET activity = 'cycling', speed = 12.5, distance = 1234.5, points = 100,"
				+ " max_speed = 20, max_dist_between_points = 30, time_minutes = 10, waypoints = 2,"
				+ " simplified_geometry = decode('00ff01', 'hex'), speed_matches_activity = true, file_activity = 'cycling',"
				+ " activity_source = 'file', track_stats = '{\"v\":1}', manual_review = '{\"admin\":{}}'");
		List<OsmGpxTrack> batch = readBatch(0, 100);
		assertEquals(1, batch.size());
		assertEquals("cycling", batch.get(0).metadata().get("activity").textValue());
		assertTrue(batch.get(0).metadata().has("manual_review"));
		assertFalse(batch.get(0).metadata().has("bbox"));
		assertEquals(ID, saveBatch(target, JSON.readValue(JSON.writeValueAsBytes(batch), OsmGpxTrack[].class), 0));
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
		assertTrue(controller.batch("secret", ID + 1, 100, null).isEmpty());
	}

	@Test
	public void refreshesColumnsBetweenRequestsAndQuotesTheirNames() throws Exception {
		saveBatch(source, new OsmGpxTrack[] {track(ID)}, 0);
		assertEquals(1, controller.batch("secret", null, 100, null).size());
		for (Connection connection : new Connection[] {source, target}) {
			execute(connection, "ALTER TABLE osm_gpx_data ADD COLUMN \"future field\" text, ADD COLUMN obsolete text");
			execute(connection, "ALTER TABLE osm_gpx_data DROP COLUMN obsolete");
		}
		execute(source, "UPDATE osm_gpx_data SET \"future field\" = 'new metadata'");
		List<OsmGpxTrack> batch = readBatch(0, 100);
		assertEquals("new metadata", batch.get(0).metadata().get("future field").textValue());
		saveBatch(target, batch.toArray(OsmGpxTrack[]::new), 0);
		try (Statement statement = target.createStatement();
			 ResultSet rs = statement.executeQuery("SELECT \"future field\" FROM osm_gpx_data")) {
			assertTrue(rs.next());
			assertEquals("new metadata", rs.getString(1));
		}
	}

	@Test
	public void rejectsSourceColumnsMissingOnTheTargetWithoutLosingData() throws Exception {
		execute(source, "ALTER TABLE osm_gpx_data ADD COLUMN source_only text");
		saveBatch(source, new OsmGpxTrack[] {track(ID)}, 0);
		execute(source, "UPDATE osm_gpx_data SET source_only = 'must not be lost'");
		OsmGpxTrack[] tracks = readBatch(0, 100).toArray(OsmGpxTrack[]::new);
		assertThrows(IllegalArgumentException.class, () -> saveBatch(target, tracks, 0));
		assertEquals(0, count(target, "osm_gpx_data"));
		assertEquals(0, count(target, "osm_gpx_files"));
		assertTrue(target.getAutoCommit());
	}

	@Test
	public void skipsMissingFilesAndLimitsJoinedRows() throws Exception {
		saveBatch(source, new OsmGpxTrack[] {track(ID), track(ID + 1), new OsmGpxTrack(track(ID + 2).metadata(), null)}, 0);
		execute(source, "DELETE FROM osm_gpx_files WHERE id = " + (ID + 1));
		List<OsmGpxTrack> batch = readBatch(0, 2);
		assertEquals(2, batch.size());
		assertEquals(ID, batch.get(0).metadata().get("id").longValue());
		assertEquals(ID + 2, batch.get(1).metadata().get("id").longValue());
		assertNull(batch.get(1).gpx());
		assertEquals(ID + 2, saveBatch(target, batch.toArray(OsmGpxTrack[]::new), 0));
		assertEquals(2, count(target, "osm_gpx_data"));
		assertEquals(2, count(target, "osm_gpx_files"));
		List<OsmGpxTrack> next = readBatch(ID + 1, 1);
		assertEquals(1, next.size());
		assertEquals(ID + 2, next.get(0).metadata().get("id").longValue());
		assertTrue(controller.batch("secret", ID + 3, 1, null).isEmpty());
	}

	@Test
	public void failedFileInsertRollsBackMetadataAndAllowsRetry() throws Exception {
		execute(target, "ALTER TABLE osm_gpx_files ADD CONSTRAINT reject_file CHECK (id <> " + ID + ")");
		assertThrows(SQLException.class, () -> saveBatch(target, new OsmGpxTrack[] {track(ID)}, 0));
		assertEquals(0, count(target, "osm_gpx_data"));
		assertEquals(0, count(target, "osm_gpx_files"));
		assertTrue(target.getAutoCommit());
		execute(target, "ALTER TABLE osm_gpx_files DROP CONSTRAINT reject_file");
		saveBatch(target, new OsmGpxTrack[] {track(ID)}, 0);
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
		}).when(failing).prepareStatement(contains("INSERT INTO osm_gpx_data "));
		OsmGpxTrack[] tracks = {track(ID), track(ID + 1)};
		assertSame(error, assertThrows(OutOfMemoryError.class, () -> saveBatch(failing, tracks, 0)));
		assertEquals(0, count(target, "osm_gpx_data"));
		assertEquals(0, count(target, "osm_gpx_files"));
		assertTrue(target.getAutoCommit());
		assertEquals(ID + 1, saveBatch(target, tracks, 0));
		assertEquals(2, count(target, "osm_gpx_data"));
		assertEquals(2, count(target, "osm_gpx_files"));
	}

	@Test
	public void failedRollbackDoesNotEnableAutocommit() throws Exception {
		execute(target, "ALTER TABLE osm_gpx_files ADD CONSTRAINT reject_file CHECK (id <> " + ID + ")");
		Connection failing = mock(Connection.class, delegatesTo(target));
		SQLException rollbackError = new SQLException("Simulated rollback failure");
		doThrow(rollbackError).when(failing).rollback();
		SQLException error = assertThrows(SQLException.class, () -> saveBatch(failing, new OsmGpxTrack[] {track(ID)}, 0));
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
		doThrow(error).when(failing).prepareStatement(contains("INSERT INTO osm_gpx_data "));
		doThrow(rollbackError).when(failing).rollback();
		assertSame(error, assertThrows(OutOfMemoryError.class, () -> saveBatch(failing, new OsmGpxTrack[] {track(ID)}, 0)));
		assertArrayEquals(new Throwable[] {rollbackError}, error.getSuppressed());
		verify(failing, never()).setAutoCommit(true);
		assertFalse(target.getAutoCommit());
	}

	@Test
	public void failedAutocommitRestorePreservesOriginalError() throws Exception {
		Connection failing = mock(Connection.class, delegatesTo(target));
		SQLException error = new SQLException("Simulated metadata insert failure");
		SQLException restoreError = new SQLException("Simulated autocommit restore failure");
		doThrow(error).when(failing).prepareStatement(contains("INSERT INTO osm_gpx_data "));
		doThrow(restoreError).when(failing).setAutoCommit(true);
		assertSame(error, assertThrows(SQLException.class, () -> saveBatch(failing, new OsmGpxTrack[] {track(ID)}, 0)));
		assertArrayEquals(new Throwable[] {restoreError}, error.getSuppressed());
		verify(failing).rollback();
		assertFalse(target.getAutoCommit());
	}

	@Test
	public void exportsGeometryLargerThanDefaultJsonStringLimit() throws Exception {
		saveBatch(source, new OsmGpxTrack[] {track(ID)}, 0);
		// Hex in JSON exceeds Jackson 2.15's default 5,000,000-character string limit.
		byte[] geometry = new byte[3_000_000];
		Arrays.fill(geometry, (byte) 0xab);
		try (PreparedStatement update = source.prepareStatement("UPDATE osm_gpx_data SET simplified_geometry = ? WHERE id = ?")) {
			update.setBytes(1, geometry);
			update.setInt(2, ID);
			update.executeUpdate();
		}
		String response = JSON.writeValueAsString(controller.batch("secret", null, 1, null));
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
		saveBatch(source, new OsmGpxTrack[] {track(ID), track(ID + 1), track(ID + 2)}, 0);
		AtomicBoolean fail = new AtomicBoolean(true);
		AtomicInteger requests = new AtomicInteger();
		String url = startServer(fail, requests);
		HttpClient client = HttpClient.newHttpClient();
		assertThrows(IOException.class, () -> SyncRemoteOsmGpx.downloadAndInsertBatches(target, client, url, "secret", 1, 0, null, null));
		assertEquals(1, count(target, "osm_gpx_data"));
		execute(target, "UPDATE osm_gpx_data SET activity = 'walking', name = 'local'");
		SyncRemoteOsmGpx.downloadAndInsertBatches(target, client, url, "secret", 1, 1, null, null);
		assertEquals(2, count(target, "osm_gpx_data"));
		assertEquals(2, count(target, "osm_gpx_files"));
		assertEquals(3, requests.get()); // No extra request after reaching the batch limit.
		SyncRemoteOsmGpx.downloadAndInsertBatches(target, client, url, "secret", 1, 0, null, null);
		assertEquals(3, count(target, "osm_gpx_data"));
		saveBatch(target, new OsmGpxTrack[] {track(ID)}, 0);
		try (Statement statement = target.createStatement();
			 ResultSet rs = statement.executeQuery("SELECT name, activity FROM osm_gpx_data WHERE id = " + ID)) {
			assertTrue(rs.next());
			assertEquals("local", rs.getString(1));
			assertEquals("walking", rs.getString(2));
		}
	}

	@Test
	public void startsWithZeroAndAdvancesToTheNextId() throws Exception {
		saveBatch(source, new OsmGpxTrack[] {track(0), track(1)}, 0);
		String url = startServer(new AtomicBoolean(false), new AtomicInteger());
		SyncRemoteOsmGpx.downloadAndInsertBatches(target, HttpClient.newHttpClient(), url, "secret", 1, 0, null, null);
		assertEquals(2, count(target, "osm_gpx_data"));
		assertEquals(2, count(target, "osm_gpx_files"));
		assertEquals(0, readBatch(0, 1).get(0).metadata().get("id").longValue());
		assertEquals(1, readBatch(1, 1).get(0).metadata().get("id").longValue());
		assertTrue(readBatch(2, 1).isEmpty());
	}

	@Test
	public void distinguishesOmittedBoundsFromZero() throws Exception {
		OsmGpxTrack[] tracks = {track(0), track(1)};
		saveBatch(source, tracks, 0);
		saveBatch(target, tracks, 0);
		assertEquals(2, controller.batch("secret", null, 100, null).size());
		assertEquals(1, controller.batch("secret", null, 100, 0).size());
		assertEquals(1, controller.batch("secret", 0, 100, 0).size());
		assertEquals(1, controller.batch("secret", 1, 100, null).size());
		execute(source, "UPDATE osm_gpx_data SET name = 'remote'");
		AtomicInteger requests = new AtomicInteger();
		String url = startServer(new AtomicBoolean(false), requests);
		SyncRemoteOsmGpx.downloadAndInsertBatches(target, HttpClient.newHttpClient(), url, "secret", 100, 0, 0, 0);
		assertEquals(1, requests.get());
		try (Statement statement = target.createStatement(); ResultSet rs = statement.executeQuery(
				"SELECT id FROM osm_gpx_data WHERE name = 'remote'")) {
			assertTrue(rs.next());
			assertEquals(0, rs.getInt(1));
			assertFalse(rs.next());
		}
	}

	@Test
	public void resynchronizesInclusiveRangeAndRepairsMissingRows() throws Exception {
		for (Connection connection : new Connection[] {source, target}) {
			execute(connection, "ALTER TABLE osm_gpx_data ADD COLUMN \"future \"\"field\" text");
		}
		saveBatch(source, new OsmGpxTrack[] {track(ID - 1), track(ID), track(ID + 2), track(ID + 3)}, 0);
		saveBatch(target, new OsmGpxTrack[] {track(ID - 1), track(ID), track(ID + 1), track(ID + 2), track(ID + 3)}, 0);
		execute(source, "UPDATE osm_gpx_data SET name = 'remote', activity = 'cycling', \"future \"\"field\" = 'remote'");
		execute(source, "UPDATE osm_gpx_files SET data = decode('010203', 'hex')");
		execute(target, "UPDATE osm_gpx_data SET name = 'local', activity = 'walking', \"future \"\"field\" = 'local'");
		execute(target, "DELETE FROM osm_gpx_files WHERE id = " + ID);
		execute(target, "DELETE FROM osm_gpx_data WHERE id = " + (ID + 2));
		String url = startServer(new AtomicBoolean(false), new AtomicInteger());
		SyncRemoteOsmGpx.downloadAndInsertBatches(target, HttpClient.newHttpClient(), url, "secret", 1, 0, ID, ID + 2);
		assertEquals(5, count(target, "osm_gpx_data"));
		assertEquals(5, count(target, "osm_gpx_files"));
		try (Statement statement = target.createStatement(); ResultSet rs = statement.executeQuery(
				"SELECT m.id, name, activity, bbox, \"future \"\"field\", f.data FROM osm_gpx_data m JOIN osm_gpx_files f USING (id)")) {
			while (rs.next()) {
				boolean resynced = rs.getLong("id") == ID || rs.getLong("id") == ID + 2;
				assertEquals(resynced ? "remote" : "local", rs.getString("name"));
				assertEquals(resynced ? "cycling" : "walking", rs.getString("activity"));
				assertEquals(resynced ? "remote:bbox" : "local:bbox", rs.getString("bbox"));
				assertEquals(resynced ? "remote" : "local", rs.getString("future \"field"));
				assertArrayEquals(resynced ? new byte[] {1, 2, 3} : track(ID).gpx(), rs.getBytes("data"));
			}
		}
	}

	@Test
	public void resyncPreservesMissingFieldsAndAppliesExplicitNulls() throws Exception {
		OsmGpxTrack[] tracks = {track(ID), track(ID + 1)};
		saveBatch(source, tracks, 0);
		execute(target, "ALTER TABLE osm_gpx_data ADD COLUMN \"local \"\"field\" text");
		saveBatch(target, tracks, 0);
		execute(source, "UPDATE osm_gpx_data SET name = 'remote'");
		execute(target, "UPDATE osm_gpx_data SET name = 'local', activity = 'walking', \"local \"\"field\" = 'keep'");
		OsmGpxTrack partial = new OsmGpxTrack(JSON.createObjectNode().put("id", ID + 1).putNull("activity"), track(ID + 1).gpx());
		SyncRemoteOsmGpx.saveBatch(target, SyncRemoteOsmGpx.getMetadataColumns(target),
				new OsmGpxTrack[] {readBatch(ID, 1).get(0), partial}, ID, true);
		try (Statement statement = target.createStatement(); ResultSet rs = statement.executeQuery(
				"SELECT id, name, activity, \"local \"\"field\" FROM osm_gpx_data ORDER BY id")) {
			for (int id : new int[] {ID, ID + 1}) {
				assertTrue(rs.next());
				assertEquals(id, rs.getInt("id"));
				assertEquals(id == ID ? "remote" : "local", rs.getString("name"));
				assertNull(rs.getString("activity"));
				assertEquals("keep", rs.getString("local \"field"));
			}
			assertFalse(rs.next());
		}
	}

	@Test
	public void resynchronizesOpenBoundariesAndHonorsBatchLimit() throws Exception {
		OsmGpxTrack[] tracks = {track(ID), track(ID + 1), track(ID + 2)};
		saveBatch(source, tracks, 0);
		saveBatch(target, tracks, 0);
		execute(source, "UPDATE osm_gpx_data SET name = 'remote'");
		execute(target, "UPDATE osm_gpx_data SET name = 'local'");
		AtomicInteger requests = new AtomicInteger();
		String url = startServer(new AtomicBoolean(false), requests);
		HttpClient client = HttpClient.newHttpClient();
		SyncRemoteOsmGpx.downloadAndInsertBatches(target, client, url, "secret", 1, 1, null, ID);
		assertEquals(1, requests.get());
		SyncRemoteOsmGpx.downloadAndInsertBatches(target, client, url, "secret", 1, 1, ID + 1, null);
		assertEquals(2, requests.get());
		try (Statement statement = target.createStatement();
			 ResultSet rs = statement.executeQuery("SELECT name FROM osm_gpx_data ORDER BY id")) {
			assertTrue(rs.next());
			assertEquals("remote", rs.getString(1));
			assertTrue(rs.next());
			assertEquals("remote", rs.getString(1));
			assertTrue(rs.next());
			assertEquals("local", rs.getString(1));
		}
		SyncRemoteOsmGpx.downloadAndInsertBatches(target, client, url, "secret", 1, 0, ID + 1, null);
		try (Statement statement = target.createStatement();
			 ResultSet rs = statement.executeQuery("SELECT count(*) FROM osm_gpx_data WHERE name = 'remote'")) {
			assertTrue(rs.next());
			assertEquals(3, rs.getInt(1));
		}
	}

	@Test
	public void resynchronizesSingleIdAtIntLimit() throws Exception {
		saveBatch(source, new OsmGpxTrack[] {track(Integer.MAX_VALUE)}, 0);
		saveBatch(target, new OsmGpxTrack[] {track(Integer.MAX_VALUE)}, 0);
		execute(source, "UPDATE osm_gpx_data SET name = 'remote'");
		AtomicInteger requests = new AtomicInteger();
		String url = startServer(new AtomicBoolean(false), requests);
		SyncRemoteOsmGpx.downloadAndInsertBatches(target, HttpClient.newHttpClient(), url, "secret", 100, 0,
				Integer.MAX_VALUE, Integer.MAX_VALUE);
		assertEquals(1, requests.get());
		SyncRemoteOsmGpx.downloadAndInsertBatches(target, HttpClient.newHttpClient(), url, "secret", 100, 0, null, null);
		assertEquals(1, requests.get());
		try (Statement statement = target.createStatement(); ResultSet rs = statement.executeQuery("SELECT id, name FROM osm_gpx_data")) {
			assertTrue(rs.next());
			assertEquals(Integer.MAX_VALUE, rs.getInt(1));
			assertEquals("remote", rs.getString(2));
			assertFalse(rs.next());
		}
	}

	@Test
	public void failedResyncRollsBackMetadataUpdates() throws Exception {
		OsmGpxTrack original = track(ID);
		saveBatch(target, new OsmGpxTrack[] {original}, 0);
		execute(target, "ALTER TABLE osm_gpx_files ADD CONSTRAINT reject_data CHECK (data <> decode('010203', 'hex'))");
		OsmGpxTrack replacement = new OsmGpxTrack(JSON.createObjectNode().put("id", ID).put("name", "remote"), new byte[] {1, 2, 3});
		assertThrows(SQLException.class, () -> SyncRemoteOsmGpx.saveBatch(target, SyncRemoteOsmGpx.getMetadataColumns(target),
				new OsmGpxTrack[] {replacement}, ID, true));
		try (Statement statement = target.createStatement(); ResultSet rs = statement.executeQuery(
				"SELECT name, data FROM osm_gpx_data JOIN osm_gpx_files USING (id)")) {
			assertTrue(rs.next());
			assertEquals(original.metadata().get("name").textValue(), rs.getString(1));
			assertArrayEquals(original.gpx(), rs.getBytes(2));
		}
		assertTrue(target.getAutoCommit());
	}

	@Test
	public void rejectsTracksOutsideRequestedRange() throws Exception {
		controller = spy(controller);
		doReturn(List.of(track(ID), track(ID + 1))).when(controller).batch("secret", ID, 100, ID);
		String url = startServer(new AtomicBoolean(false), new AtomicInteger());
		IOException error = assertThrows(IOException.class, () -> SyncRemoteOsmGpx.downloadAndInsertBatches(
				target, HttpClient.newHttpClient(), url, "secret", 100, 0, ID, ID));
		assertEquals("OsmGpx batch exceeds requested id range", error.getMessage());
		assertEquals(0, count(target, "osm_gpx_data"));
		assertEquals(0, count(target, "osm_gpx_files"));
		assertTrue(target.getAutoCommit());
	}

	@Test
	public void rejectsInvalidIdRangeOptionsBeforeOpeningDatabase() {
		for (String[] range : new String[][] {{"-1", "0"}, {"0", "-1"}, {"101", "100"}}) {
			assertThrows(IllegalArgumentException.class, () -> SyncRemoteOsmGpx.main(
					new String[] {"https://builder.osmand.net", "--from-id=" + range[0], "--to-id=" + range[1]}));
		}
		for (String option : new String[] {"--from-id", "--to-id"}) {
			for (String value : new String[] {"-2", "invalid", "2147483648"}) {
				assertThrows(IllegalArgumentException.class, () -> SyncRemoteOsmGpx.main(
						new String[] {"https://builder.osmand.net", option + "=" + value}));
			}
		}
	}

	private String startServer(AtomicBoolean fail, AtomicInteger requests) throws IOException {
		server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
		server.createContext(SyncRemoteOsmGpx.API_PATH, exchange -> {
			requests.incrementAndGet();
			MultiValueMap<String, String> params = UriComponentsBuilder.fromUri(exchange.getRequestURI()).build().getQueryParams();
			Integer fromId = params.containsKey("fromId") ? Integer.valueOf(params.getFirst("fromId")) : null;
			int limit = Integer.parseInt(params.getFirst("limit"));
			Integer toId = params.containsKey("toId") ? Integer.valueOf(params.getFirst("toId")) : null;
			try (exchange) {
				if (fromId != null && fromId == ID + 1 && fail.getAndSet(false)) {
					exchange.sendResponseHeaders(503, 0);
					return;
				}
				byte[] body = JSON.writeValueAsBytes(controller.batch(exchange.getRequestHeaders().getFirst(SyncRemoteOsmGpx.API_KEY_HEADER), fromId, limit, toId));
				exchange.sendResponseHeaders(200, body.length);
				exchange.getResponseBody().write(body);
			}
		});
		server.start();
		return "http://127.0.0.1:" + server.getAddress().getPort() + SyncRemoteOsmGpx.API_PATH;
	}

	private static int saveBatch(Connection connection, OsmGpxTrack[] tracks, int fromId) throws SQLException {
		return SyncRemoteOsmGpx.saveBatch(connection, SyncRemoteOsmGpx.getMetadataColumns(connection), tracks, fromId, false);
	}

	private List<OsmGpxTrack> readBatch(Integer fromId, int limit) throws IOException {
		return Arrays.asList(JSON.readValue(JSON.writeValueAsBytes(controller.batch("secret", fromId, limit, null)), OsmGpxTrack[].class));
	}

	private static OsmGpxTrack track(int id) throws IOException {
		return new OsmGpxTrack(JSON.readTree("{\"id\":" + id + ",\"user\":\"author\",\"name\":\"O'Brien — трек\","
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
