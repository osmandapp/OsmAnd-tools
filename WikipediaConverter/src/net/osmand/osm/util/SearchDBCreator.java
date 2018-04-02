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
		String pathTodb = "/home/user/osmand/wikivoyage/wikivoyage.sqlite";
		if(args.length > 0) {
			pathTodb = args[0];
		}
		DBDialect dialect = DBDialect.SQLITE;
		Connection conn = (Connection) dialect.getDatabaseConnection(pathTodb, log);
		generateIdsIfMissing(conn, pathTodb.substring(0, pathTodb.lastIndexOf("/") + 1));
		conn.createStatement().execute("DROP TABLE IF EXISTS wikivoyage_search;");
		conn.createStatement().execute("CREATE TABLE wikivoyage_search(search_term text, city_id long, article_title text, lang text)");
		conn.createStatement().execute("CREATE INDEX IF NOT EXISTS index_search_term ON wikivoyage_search(search_term);");
		conn.createStatement().execute("CREATE INDEX IF NOT EXISTS index_search_city ON wikivoyage_search(city_id)");
		conn.createStatement().execute("ALTER TABLE wikivoyage_articles ADD COLUMN aggregated_part_of");
		
		PreparedStatement partOf = conn.prepareStatement("UPDATE wikivoyage_articles SET aggregated_part_of = ? WHERE title = ? AND lang = ?");
		PreparedStatement ps = conn.prepareStatement("INSERT INTO wikivoyage_search VALUES (?, ?, ?, ?)");
		PreparedStatement data = conn.prepareStatement("SELECT title, city_id, lang, is_part_of FROM wikivoyage_articles");
		ResultSet rs = data.executeQuery();
		int batch = 0;
		while (rs.next()) {
			String title = rs.getString("title");
			String titleToSplit = title.replaceAll("[/\\)\\(-]", " ").replaceAll(" +", " ");
			long id = rs.getLong("city_id");
			String lang = rs.getString("lang");
			for (String s : titleToSplit.split(" ")) {
				ps.setString(1, s.toLowerCase());
				ps.setLong(2, id);
				ps.setString(3, title);
				ps.setString(4, lang);
				partOf.setString(1, getAggregatedPartOf(conn, rs.getString("is_part_of"), lang));
				partOf.setString(2, title);
				partOf.setString(3, lang);
				partOf.addBatch();
				ps.addBatch();
				if (batch++ > 500) {
					partOf.executeBatch();
					ps.executeBatch();
					batch = 0;
				}
			}
		}
		finishPrep(ps);
		finishPrep(partOf);
		data.close();
		rs.close();
		conn.close();
	}

	private static void finishPrep(PreparedStatement ps) throws SQLException {
		ps.addBatch();
		ps.executeBatch();
		ps.close();
	}

	private static String getAggregatedPartOf(Connection conn, String partOf, String lang) throws SQLException {
		if (partOf.isEmpty()) {
			return "";
		}
		StringBuilder res = new StringBuilder();
		res.append(partOf);
		res.append(",");
		PreparedStatement ps = conn.prepareStatement("SELECT is_part_of FROM wikivoyage_articles WHERE title = ? AND lang = '" + lang + "'");
		String prev = "";
		while (true) {
			ps.setString(1, partOf);
			ResultSet rs = ps.executeQuery();
			String buf = "";
			while (rs.next()) {
				buf = rs.getString(1);
			}
			if (buf.equals("") || buf.equals(partOf) || buf.equals(prev)) {
				ps.close();
				rs.close();
				return res.toString().substring(0, res.length() - 1);
			} else {
				rs.close();
				ps.clearParameters();
				res.append(buf);
				res.append(',');
				prev = partOf;
				partOf = buf;
			}
		}
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
