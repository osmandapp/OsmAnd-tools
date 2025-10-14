package net.osmand.wiki;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.osmand.PlatformUtil;
import net.osmand.obf.preparation.DBDialect;
import org.apache.commons.logging.Log;

import java.io.*;
import java.net.URL;
import java.net.URLConnection;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class WikidataFilesDownloader extends AbstractWikiFilesDownloader {

	private static final Log log = PlatformUtil.getLog(WikidataFilesDownloader.class);

	public WikidataFilesDownloader(File wikiDB, boolean daily) {
		super(wikiDB, daily);
	}

	public String getFilePrefix() {
		return "wikidatawiki";
	}

	public long getMaxIdFromDb(File wikiSqlite) throws SQLException {
		DBDialect dialect = DBDialect.SQLITE;
		Connection conn = dialect.getDatabaseConnection(wikiSqlite.getAbsolutePath(), log);
		ResultSet rs = conn.createStatement().executeQuery("SELECT max(id) FROM wiki_coords");
		long maxQId = 0;
		if (rs.next()) {
			maxQId = rs.getLong(1);
		}
		rs = conn.createStatement().executeQuery("SELECT max(id) FROM wikidata_properties");
		if (rs.next()) {
			maxQId = Math.max(rs.getLong(1), maxQId);
		}
		conn.close();
		if (maxQId == 0) {
			throw new RuntimeException("Could not get max QiD from " + wikiSqlite.getAbsolutePath());
		}
		return maxQId;
	}

	public long getMaxPageId() throws IOException {
		String s = "https://www.wikidata.org/wiki/Special:EntityData/Q" + getMaxId() + ".json";
		URL url = new URL(s);
		URLConnection connection = url.openConnection();
		connection.setRequestProperty("User-Agent", USER_AGENT);
		connection.connect();
		InputStream inputStream = connection.getInputStream();
		ObjectMapper mapper = new ObjectMapper();
		JsonNode json = mapper.readTree(inputStream);
		if (json != null) {
			JsonNode jsonPageId = json.findValue("pageid");
			if (jsonPageId != null) {
				return jsonPageId.asLong();
			}
		}
		throw new RuntimeException("Could not get max id for updating from " + s);
	}
}
