package net.osmand.obf.preparation;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import net.osmand.util.Algorithms;

import org.apache.commons.logging.Log;
import org.sqlite.SQLiteJDBCLoader;


public enum DBDialect {
	SQLITE,
	SQLITE_IN_MEMORY;

	public void deleteTableIfExists(String table, Statement stat) throws SQLException {
		stat.executeUpdate("drop table if exists " + table); //$NON-NLS-1$
	}
	
	public boolean checkTableIfExists(String table, Statement stat) throws SQLException {
		ResultSet rs = stat.getConnection().getMetaData().getTables(null, null, table, null);
		boolean next = rs.next();
		rs.close();
		return next;
//		return stat.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='"+table+"'");
	}

	public boolean databaseFileExists(File dbFile) {
		return dbFile.exists();
	}

	public void removeDatabase(File file) {
		Algorithms.removeAllFiles(file);
	}

	public void commitDatabase(Object connection) throws SQLException {
		if (!((Connection) connection).getAutoCommit()) {
			((Connection) connection).commit();
		}
	}

	public void closeDatabase(Object dbConn) throws SQLException {
		((Connection) dbConn).close();
	}

    public Connection getDatabaseConnection(String fileName, Log log) throws SQLException {
		if (DBDialect.SQLITE == this || DBDialect.SQLITE_IN_MEMORY == this) {
			if(System.getProperty("os.name").toLowerCase().contains("mac")) {
				System.setProperty("org.sqlite.lib.name", "libsqlitejdbc.jnilib");
			}
			try {
				Class.forName("org.sqlite.JDBC");
			} catch (ClassNotFoundException e) {
				log.error("Illegal configuration", e);
				throw new IllegalStateException(e);
			}
			Connection connection = DriverManager.getConnection("jdbc:sqlite:" + (DBDialect.SQLITE_IN_MEMORY == this? ":memory:":
					fileName));
			Statement statement = connection.createStatement();
			statement.executeUpdate("PRAGMA synchronous = 0");
			//no journaling, saves some I/O access, but database can go corrupt
			statement.executeQuery("PRAGMA journal_mode = OFF");
			//we are exclusive, some speed increase ( no need to get and release logs
			statement.executeQuery("PRAGMA locking_mode = EXCLUSIVE");
			//increased cache_size, by default it is 2000 and we have quite huge files...
			//statement.executeUpdate("PRAGMA cache_size = 10000"); cache size could be probably contraproductive on slower disks?
			statement.close();
			try {
				log.info(String.format("SQLITE running in %s mode", SQLiteJDBCLoader.isNativeMode() ? "native" : "pure-java"));
			} catch (Exception e) {
				e.printStackTrace();
			}
			return connection;
		} else {
			throw new UnsupportedOperationException();
		}

	}
}
