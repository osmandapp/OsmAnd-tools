package net.osmand.osm.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.logging.Log;

import net.osmand.PlatformUtil;
import net.osmand.data.preparation.DBDialect;

public class SearchDBCreator {
	
	private static final Log log = PlatformUtil.getLog(SearchDBCreator.class);

	public static void main(String[] args) throws SQLException {
		String pathTodb = "/home/paul/osmand/wikivoyage/wikivoyage.sqlite";
		if(args.length > 0) {
			pathTodb = args[0];
		}
		DBDialect dialect = DBDialect.SQLITE;
		Connection conn = (Connection) dialect.getDatabaseConnection(pathTodb, log);
		generateIdsIfMissing(conn, pathTodb.substring(0, pathTodb.lastIndexOf("/") + 1));
		conn.createStatement().execute("DROP TABLE IF EXISTS wikivoyage_search;");
		conn.createStatement().execute("CREATE TABLE wikivoyage_search(search_term text, city_id long, article_title text, lang text)");
		conn.createStatement().execute("CREATE INDEX IF NOT EXISTS index_word ON wikivoyage_search(search_term);");
		PreparedStatement ps = conn.prepareStatement("INSERT INTO wikivoyage_search VALUES (?, ?, ?, ?)");
		PreparedStatement data = conn.prepareStatement("SELECT title, city_id, lang FROM wikivoyage_articles");
		ResultSet rs = data.executeQuery();
		int batch = 0;
		while (rs.next()) {
			String title = rs.getString("title");
			long id = rs.getLong("city_id");
			for (String s : title.split(" ")) {
				ps.setString(1, s.replaceAll("[\\)\\(]", "").toLowerCase());
				ps.setLong(2, id);
				ps.setString(3, title);
				ps.setString(4, rs.getString("lang"));
				ps.addBatch();
				if (batch++ > 500) {
					ps.executeBatch();
					batch = 0;
				}
			}
		}
		ps.addBatch();
		ps.executeBatch();
		ps.close();
		data.close();
		rs.close();
		conn.close();
	}

	private static void generateIdsIfMissing(Connection conn, String workingDir) throws SQLException {
		long maxId = 0;
		DBDialect dialect = DBDialect.SQLITE;
		Connection langConn = (Connection) dialect.getDatabaseConnection(workingDir + "langlink.sqlite", log);
		PreparedStatement st = langConn.prepareStatement("SELECT MAX(id) FROM langlinks");
		ResultSet rs = st.executeQuery();
		while (rs.next()) {
			maxId = rs.getLong(1) + 1;
		}
		st.close();
		rs.close();
		langConn.close();
		if (maxId == 0) {
			return;
		}
		int batch = 0;
		PreparedStatement ps = conn.prepareStatement("SELECT title FROM wikivoyage_articles WHERE city_id = 0");
		PreparedStatement prep = conn.prepareStatement("UPDATE wikivoyage_articles SET city_id = ? WHERE title = ?");
		ResultSet res = ps.executeQuery();
		while (res.next()) {
			String title = res.getString("title");
			prep.setLong(1, maxId++);
			prep.setString(2, title);
			prep.addBatch();
			if (batch++ > 500) {
				prep.executeBatch();
				batch = 0;
			}
		}
		prep.addBatch();
		prep.executeBatch();
		prep.close();
		res.close();		
	}
}
