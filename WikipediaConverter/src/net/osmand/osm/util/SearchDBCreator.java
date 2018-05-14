package net.osmand.osm.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import org.apache.commons.logging.Log;

import net.osmand.PlatformUtil;
import net.osmand.data.preparation.DBDialect;

public class SearchDBCreator {
	
	private static final Log log = PlatformUtil.getLog(SearchDBCreator.class);

	public static void main(String[] args) throws SQLException, IOException {
		boolean uncompressed = false;
		String workingDir = "/home/user/osmand/wikivoyage/";
		if(args.length > 1) {
			workingDir = args[0];
			uncompressed = Boolean.parseBoolean(args[1]);
		}
		String pathTodb = workingDir + (uncompressed ? "full_wikivoyage.sqlite" : "wikivoyage.sqlite");
		final File langlinkFolder = new File(workingDir + "langlinks");
		final File langlinkFile = new File(workingDir + "langlink.sqlite");
		DBDialect dialect = DBDialect.SQLITE;
		Connection conn = (Connection) dialect.getDatabaseConnection(pathTodb, log);
		createLangLinksIfMissing(langlinkFile, langlinkFolder, conn);
		generateIdsIfMissing(conn, pathTodb.substring(0, pathTodb.lastIndexOf("/") + 1));
		Connection langlinkConn = (Connection) dialect.getDatabaseConnection(langlinkFile.getAbsolutePath(), log);
		conn.createStatement().execute("DROP TABLE IF EXISTS travel_search;");
		conn.createStatement().execute("CREATE TABLE travel_search(search_term text, trip_id long, article_title text, lang text)");
		conn.createStatement().execute("CREATE INDEX IF NOT EXISTS index_search_term ON travel_search(search_term);");
		conn.createStatement().execute("CREATE INDEX IF NOT EXISTS index_search_city ON travel_search(trip_id)");
		try {
			conn.createStatement().execute("ALTER TABLE travel_articles ADD COLUMN aggregated_part_of");
		} catch(Exception e) {
			System.err.println("Column aggregated_part_of already exists" );
		}
		
		PreparedStatement partOf = conn.prepareStatement("UPDATE travel_articles SET aggregated_part_of = ?, trip_id = ? WHERE title = ? AND lang = ?");
		PreparedStatement ps = conn.prepareStatement("INSERT INTO travel_search VALUES (?, ?, ?, ?)");
		PreparedStatement data = conn.prepareStatement("SELECT title, lang, is_part_of FROM travel_articles");
		PreparedStatement langlinkStatement = langlinkConn.prepareStatement("SELECT id FROM langlinks WHERE title = ? AND lang = ?");
		ResultSet rs = data.executeQuery();
		int batch = 0;
		while (rs.next()) {
			String title = rs.getString("title");
			String titleToSplit = title.replaceAll("[/\\)\\(-]", " ").replaceAll(" +", " ");
			String lang = rs.getString("lang");
			long id = getCityId(langlinkStatement, title, lang);
			for (String s : titleToSplit.split(" ")) {
				ps.setString(1, s.toLowerCase());
				ps.setLong(2, id);
				ps.setString(3, title);
				ps.setString(4, lang);
				partOf.setString(1, getAggregatedPartOf(conn, rs.getString("is_part_of"), lang));
				partOf.setLong(2, id);
				partOf.setString(3, title);
				partOf.setString(4, lang);
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
		langlinkStatement.close();
		langlinkConn.close();
		data.close();
		rs.close();
		conn.close();
	}

	private static long getCityId(PreparedStatement langlinkStatement, String title, String lang) throws SQLException {
		langlinkStatement.setString(1, title);
		langlinkStatement.setString(2, lang);
		ResultSet rs = langlinkStatement.executeQuery();
		long result = 0;
		while (rs.next()) {
			result = rs.getLong("id");
		}
		return result;
	}

	private static void createLangLinksIfMissing(File langlinkFile, File langlinkFolder, Connection conn) throws IOException, SQLException {
		if (langlinkFolder.exists() && !langlinkFile.exists()) {
			processLangLinks(langlinkFolder, langlinkFile, conn);
		}
	}
	
	private static void processLangLinks(File langlinkFolder, File langlinkFile, Connection wikivoyageConnection) throws IOException, SQLException {
		if (!langlinkFolder.isDirectory()) {
			System.err.println("Specified langlink folder is not a directory");
			System.exit(-1);
		}
		DBDialect dialect = DBDialect.SQLITE;
		Connection conn = (Connection) dialect.getDatabaseConnection(langlinkFile.getAbsolutePath(), log);
		conn.createStatement().execute("CREATE TABLE langlinks (id long NOT NULL DEFAULT 0, lang text NOT NULL DEFAULT '', "
				+ "title text NOT NULL DEFAULT '', UNIQUE (lang, title) ON CONFLICT IGNORE)");
		conn.createStatement().execute("CREATE INDEX IF NOT EXISTS index_title ON langlinks(title);");
		conn.createStatement().execute("CREATE INDEX IF NOT EXISTS index_lang ON langlinks(lang);");
		PreparedStatement prep = conn.prepareStatement("INSERT OR IGNORE INTO langlinks VALUES (?, ?, ?)");
		PreparedStatement articleQuery = wikivoyageConnection.prepareStatement("SELECT title FROM travel_articles WHERE original_id = ? AND lang = ?");
		int batch = 0;
		long maxId = 0;
		Set<Long> ids = new HashSet<>();
		Map<Long, Long> currMapping = new HashMap<>();
		File[] files = langlinkFolder.listFiles();
		for (File f : files) {
			String lang = f.getName().replace("wikivoyage-latest-langlinks.sql.gz", "");
			InputStream fis = new FileInputStream(f);
			if(f.getName().endsWith("gz")) {
				fis = new GZIPInputStream(fis);
			}
	    	InputStreamReader read = new InputStreamReader(fis, "UTF-8");
	    	char[] cbuf = new char[1000];
	    	int cnt;
	    	boolean values = false;
	    	String buf = ""	;
	    	List<String> insValues = new ArrayList<String>();
	    	while((cnt = read.read(cbuf)) >= 0) {
	    		String str = new String(cbuf, 0, cnt);
	    		buf += str;
	    		if(!values) {
	    			if(buf.contains("VALUES")) {
	    				buf = buf.substring(buf.indexOf("VALUES") + "VALUES".length());
	    				values = true;
	    			}
	    		} else {
	    			boolean openString = false;
	    			int word = -1;
	    			int last = 0;
	    			for(int k = 0; k < buf.length(); k++) {
	    				if(openString ) {
							if (buf.charAt(k) == '\'' && (buf.charAt(k - 1) != '\\'
									|| buf.charAt(k - 2) == '\\')) {
	    						openString = false;
	    					}
	    				} else if(buf.charAt(k) == ',' && word == -1) {
	    					continue;
	    				} else if(buf.charAt(k) == '(') {
	    					word = k;
	    					insValues.clear();
	    				} else if(buf.charAt(k) == ')' || buf.charAt(k) == ',') {
	    					String vl = buf.substring(word + 1, k).trim();
	    					if(vl.startsWith("'")) {
	    						vl = vl.substring(1, vl.length() - 1);
	    					}
	    					insValues.add(vl);
	    					if(buf.charAt(k) == ')') {
	    						long id = Long.valueOf(insValues.get(0));
	    						articleQuery.setLong(1, id);
	    						articleQuery.setString(2, lang);
	    						ResultSet rs = articleQuery.executeQuery();
	    						String thisTitle = "";
	    						while (rs.next()) {
	    							thisTitle = rs.getString("title");
	    						}
	    						articleQuery.clearParameters();
				    			maxId = Math.max(maxId, id);
				    			Long genId = currMapping.get(id);
				    			if (genId == null) {
									if (ids.contains(id)) {
										genId = maxId++;
										currMapping.put(id, genId);
									}
				    			}
				    			id = genId == null ? id : genId;
				    			ids.add(id);
				    			if (!thisTitle.isEmpty()) {
				    				prep.setLong(1, id);
				    				prep.setString(2, lang);
						    		prep.setString(3, thisTitle);
						    		prep.addBatch();
						    		batch++;
				    			}
				    			prep.setLong(1, id);
				    			prep.setString(2, insValues.get(1));
					    		prep.setString(3, insValues.get(2));
					    		prep.addBatch();
					    		if (batch++ > 500) {
					    			prep.executeBatch();
									batch = 0;
					    		}
	    						last = k + 1;
	    						word = -1;
	    					} else {
	    						word = k;
	    					}
	    				} else if(buf.charAt(k) == '\'') {
	    					openString = true;
	    				}
	    			}
	    			buf = buf.substring(last);
	    		}
	    	}
	    	currMapping.clear();
	    	read.close();
		}
		prep.addBatch();
    	prep.executeBatch();
    	prep.close();
    	conn.createStatement().execute("DROP INDEX IF EXISTS index_orig_id;");
    	articleQuery.close();
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
		PreparedStatement ps = conn.prepareStatement("SELECT is_part_of FROM travel_articles WHERE title = ? AND lang = '" + lang + "'");
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
			throw new IllegalStateException();
		}
		int batch = 0;
		PreparedStatement ps = conn.prepareStatement("SELECT title, lang FROM travel_articles WHERE trip_id = 0");
		PreparedStatement prep = conn.prepareStatement("UPDATE travel_articles SET trip_id = ? WHERE title = ? AND lang = ?");
		ResultSet res = ps.executeQuery();
		while (res.next()) {
			String title = res.getString("title");
			String lang = res.getString("lang");
			prep.setLong(1, maxId++);
			prep.setString(2, title);
			prep.setString(3, lang);
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
