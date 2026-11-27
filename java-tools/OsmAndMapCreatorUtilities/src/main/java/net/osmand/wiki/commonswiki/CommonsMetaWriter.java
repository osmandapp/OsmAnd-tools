package net.osmand.wiki.commonswiki;

import net.osmand.PlatformUtil;
import net.osmand.obf.preparation.DBDialect;
import org.apache.commons.logging.Log;

import java.sql.*;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

class CommonsMetaWriter {
	private static final Log log = PlatformUtil.getLog(CommonsMetaWriter.class);

	private static final int BATCH_SIZE = 5000;
	private static final int QUEUE_SIZE = 10_000;
	private static final Article END_SIGNAL = new Article(-1L, "", "", "", "", "", "");

	private final String sqliteFileName;
	private final boolean showLog;
	private final BlockingQueue<Article> queue = new LinkedBlockingQueue<>(QUEUE_SIZE);
	private final Thread writerThread;
	private long writtenCount;
	private long changedLicenseCount;
	private Exception writerError;

	record Article(Long id, String title, String author, String date, String license, String description,
	               String source) {
	}

	CommonsMetaWriter(String sqliteFileName, boolean recreateDb, boolean showLog) throws SQLException {
		this.sqliteFileName = sqliteFileName;
		this.showLog = showLog;
		initDatabase(recreateDb);
		writerThread = new Thread(this::writeQueue);
		writerThread.start();
	}

	void add(Article article) throws InterruptedException {
		queue.put(article);
	}

	void finish() throws InterruptedException {
		queue.put(END_SIGNAL);
		writerThread.join();
		if (writerError != null) {
			throw new RuntimeException("Error during writing to " + sqliteFileName, writerError);
		}
	}

	void abort() {
		writerThread.interrupt();
	}

	long getWrittenCount() {
		return writtenCount;
	}

	long getChangedLicenseCount() {
		return changedLicenseCount;
	}

	private void initDatabase(boolean recreateDb) throws SQLException {
		try (Connection conn = DBDialect.SQLITE.getDatabaseConnection(sqliteFileName, log);
		     Statement stmt = conn.createStatement()) {
			stmt.execute("PRAGMA journal_mode = WAL;");
			stmt.execute("PRAGMA synchronous = OFF;");
			stmt.execute("PRAGMA busy_timeout = 5000;");
			if (recreateDb) {
				log.info("========= DROP old TABLE common_meta =========");
				stmt.execute("DROP TABLE IF EXISTS common_meta");
			}
			log.info("========= CREATE TABLE common_meta =========");
			stmt.execute("""
					CREATE TABLE IF NOT EXISTS common_meta(
					id long PRIMARY KEY,
					name text,
					author text,
					date text,
					license text,
					description text,
					source text)""");
		}
	}

	private void writeQueue() {
		log.info("Writer started...");
		boolean endReceived = false;
		try (Connection conn = DBDialect.SQLITE.getDatabaseConnection(sqliteFileName, log)) {
			conn.setAutoCommit(false);
			try (PreparedStatement insertStatement = conn.prepareStatement("""
					INSERT INTO common_meta(id, name, author, date, license, description, source) VALUES (?, ?, ?, ?, ?, ?, ?)
					ON CONFLICT(id) DO UPDATE SET
					name = excluded.name,
					author = excluded.author,
					date = excluded.date,
					license = excluded.license,
					description = excluded.description,
					source = excluded.source;""");
			     PreparedStatement selectStatement = conn
					     .prepareStatement("SELECT id, name, license FROM common_meta WHERE id = ?")) {
				try (Statement st = conn.createStatement();
				     ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM common_meta")) {
					rs.next();
					log.info("Rows in common_meta at start: %d".formatted(rs.getInt(1)));
				}
				int counter = 0;
				while (true) {
					Article a = queue.take();
					if (a == END_SIGNAL) {
						endReceived = true;
						break;
					}
					if (showLog) {
						logLicenseChange(selectStatement, a);
					}
					insertStatement.setLong(1, a.id());
					insertStatement.setString(2, a.title());
					insertStatement.setString(3, a.author());
					insertStatement.setString(4, a.date());
					insertStatement.setString(5, a.license());
					insertStatement.setString(6, a.description());
					insertStatement.setString(7, a.source());
					insertStatement.addBatch();
					counter++;

					if (counter % BATCH_SIZE == 0) {
						insertStatement.executeBatch();
						conn.commit();
						writtenCount += counter;
						counter = 0;
					}
				}

				if (counter > 0) {
					insertStatement.executeBatch();
					conn.commit();
					writtenCount += counter;
				}
			}
		} catch (InterruptedException e) {
			writerError = e;
		} catch (Exception e) {
			log.error("Writer failed: " + e.getMessage());
			writerError = e;
			if (!endReceived) {
				skipQueueUntilEnd();
			}
		}
		log.info("Writer finished (queue empty).");
	}

	private void logLicenseChange(PreparedStatement selectStatement, Article a) throws SQLException {
		selectStatement.setLong(1, a.id());
		try (ResultSet rs = selectStatement.executeQuery()) {
			if (rs.next()) {
				String oldLicense = rs.getString("license");
				if (!Objects.equals(oldLicense, a.license())) {
					changedLicenseCount++;
					log.info("==== Article %s (%s) license changed from %s to %s"
							.formatted(rs.getString("name"), rs.getString("id"), oldLicense, a.license()));
				}
			}
		}
	}

	private void skipQueueUntilEnd() {
		try {
			while (queue.take() != END_SIGNAL) {
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

	void createIndex() {
		log.info("========= DONE =========");
		log.info("Create indexes");

		try (Connection conn = DBDialect.SQLITE.getDatabaseConnection(sqliteFileName, log);
		     Statement stmt = conn.createStatement()) {
			stmt.execute("CREATE INDEX IF NOT EXISTS name_common_meta_index ON common_meta(name)");

		} catch (SQLException e) {
			log.error("Failed to create index: " + e.getMessage(), e);
		}
	}
}
