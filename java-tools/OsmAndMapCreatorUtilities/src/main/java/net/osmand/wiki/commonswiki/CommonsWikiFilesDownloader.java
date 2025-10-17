package net.osmand.wiki.commonswiki;

import net.osmand.PlatformUtil;
import net.osmand.obf.preparation.DBDialect;
import net.osmand.wiki.AbstractWikiFilesDownloader;
import org.apache.commons.logging.Log;

import java.io.*;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class CommonsWikiFilesDownloader extends AbstractWikiFilesDownloader {
	private static final Log log = PlatformUtil.getLog(CommonsWikiFilesDownloader.class);

	public CommonsWikiFilesDownloader(File wikidataDB, boolean daily) {
		super(wikidataDB, daily);
	}

	public String getFilePrefix() {
		return "commonswiki";
	}

	public long getMaxIdFromDb(File wikiSqlite) throws SQLException {
		DBDialect dialect = DBDialect.SQLITE;
		Connection conn = dialect.getDatabaseConnection(wikiSqlite.getAbsolutePath(), log);
		ResultSet rs = conn.createStatement().executeQuery("SELECT max(id) FROM common_meta");
		long maxId = 0;
		if (rs.next()) {
			maxId = rs.getLong(1);
		}
		return maxId;
	}

	public long getMaxPageId() throws IOException {
		return getMaxId();
	}
}
