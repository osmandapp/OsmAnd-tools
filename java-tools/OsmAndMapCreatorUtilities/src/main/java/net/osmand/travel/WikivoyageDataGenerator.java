package net.osmand.travel;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.logging.Log;

import net.osmand.PlatformUtil;
import net.osmand.obf.preparation.DBDialect;
import net.osmand.util.Algorithms;

public class WikivoyageDataGenerator {

	private static final Log log = PlatformUtil.getLog(WikivoyageDataGenerator.class);
	private static final int BATCH_SIZE = 500;

	public static void main(String[] args) throws SQLException, IOException {
		boolean uncompressed = false;
		File wikivoyageFile = new File(args[0]);
		if(!wikivoyageFile.exists()) {
			throw new IllegalArgumentException("Wikivoyage file doesn't exist: " + args[0]);
		}
		File workingDir = wikivoyageFile.getParentFile();

		for(int i = 1; i < args.length; i++) {
			String val = args[i].substring(args[i].indexOf('=') + 1);
			if(args[i].startsWith("--uncompressed=")) {
				uncompressed = Boolean.parseBoolean(val);
			}
		}
		System.out.println("Process " + wikivoyageFile.getName() + " " + (uncompressed ? "uncompressed" : ""));

		Connection conn = DBDialect.SQLITE.getDatabaseConnection(wikivoyageFile.getAbsolutePath(), log);
		WikivoyageDataGenerator generator = new WikivoyageDataGenerator();
		printStep("Preparing indexes");
		conn.createStatement().execute("CREATE INDEX IF NOT EXISTS index_title_lang ON travel_articles(title,lang);");
		conn.createStatement().execute("CREATE INDEX IF NOT EXISTS index_tripid_lang ON travel_articles(trip_id,lang);");
		conn.createStatement().execute("CREATE INDEX IF NOT EXISTS index_partof_lang ON travel_articles(is_part_of,lang);");
		conn.createStatement().execute("CREATE INDEX IF NOT EXISTS index_image_title ON travel_articles(image_title);");
		conn.createStatement().execute("CREATE INDEX IF NOT EXISTS index_banner_title ON travel_articles(banner_title);");

		printStep("Download/Copy proper headers for articles");
		generator.updateSourceImageForArticles(conn, workingDir);
		conn.createStatement().execute("CREATE INDEX IF NOT EXISTS index_srcbanner_title ON travel_articles(src_banner_title);");
		
		printStep("Copy headers between lang");
		generator.copyImagesBetweenArticles(conn, "image_title");
		generator.copyImagesBetweenArticles(conn, "src_banner_title");
		
		printStep("Generate agg part of");
		generator.generateAggPartOf(conn);
		
		printStep("Generate is parent of");
		generator.generateIsParentOf(conn);
		
		conn.createStatement().execute("DROP INDEX IF EXISTS index_title_lang ");
		conn.createStatement().execute("DROP INDEX IF EXISTS index_image_title ");
		conn.createStatement().execute("DROP INDEX IF EXISTS index_partof_lang ");
		conn.createStatement().execute("DROP INDEX IF EXISTS index_banner_title ");
		conn.createStatement().execute("DROP INDEX IF EXISTS index_srcbanner_title ");
		conn.close();
	}

	private static void printStep(String step) {
		System.out.println("########## " + step + " ########## " + new Date());
	}

	private void updateSourceImageForArticles(Connection conn, File workingDir) throws SQLException {
		final File imagesMetadata = new File(workingDir, "image_sources.sqlite");
		Connection imagesConn = DBDialect.SQLITE.getDatabaseConnection(imagesMetadata.getAbsolutePath(), log);
		imagesConn.createStatement()
				.execute("CREATE TABLE IF NOT EXISTS images(file text, url text, metadata text, sourcefile text)");
		conn.createStatement().execute("DROP TABLE IF EXISTS source_image;");
		conn.createStatement().execute("CREATE TABLE IF NOT EXISTS source_image(img text, source_image text)");
		conn.createStatement().execute("CREATE INDEX IF NOT EXISTS index_source_image ON source_image(img);");
		
		Map<String, String> existingImagesMapping = new LinkedHashMap<String, String>();
		ResultSet images = imagesConn.createStatement().executeQuery("SELECT name, source FROM images");
		while (images.next()) {
			String sourceFile = images.getString("source");
			existingImagesMapping.put(images.getString("name"), sourceFile);
		}
		images.close();
		updateImagesToSource(conn, existingImagesMapping, "banner_title");
//		updateImagesToSource(conn, existingImagesMapping, "image_title");
		imagesConn.close();
	}

	private void updateImagesToSource(Connection wikivoyageConn, Map<String, String> existingImagesMapping, String imageColumn) throws SQLException {
		try {
			wikivoyageConn.createStatement().execute("ALTER TABLE travel_articles ADD COLUMN src_"+imageColumn);
		} catch (Exception e) {
			System.err.println("Column src_"+imageColumn+" already exists");
		}
		wikivoyageConn.createStatement().executeUpdate(String.format("UPDATE travel_articles SET src_%s = %s", imageColumn, imageColumn));
		ResultSet rs = wikivoyageConn.createStatement().executeQuery("SELECT distinct " + imageColumn
				+ ", title, lang FROM travel_articles where " + imageColumn + " <> ''");
		PreparedStatement pInsertSource = wikivoyageConn.prepareStatement("INSERT INTO source_image(img, source_image) VALUES(?, ?)");
		int imagesProcessed = 0;
		int imagesToUpdate = 0;
		while (rs.next()) {
			String imageTitle = rs.getString(1);
			if (imageTitle == null || imageTitle.isEmpty()) {
				continue;
			}
			if (++imagesProcessed % 5000 == 0) {
				System.out.println("Images metadata processed: " + imagesProcessed);
			}
			String sourceFile = existingImagesMapping.get(imageTitle);
			if (sourceFile != null && !trim(sourceFile).isEmpty()) {
				pInsertSource.setString(1, imageTitle);
				pInsertSource.setString(2, sourceFile);
				pInsertSource.executeUpdate();
				imagesToUpdate++;
			}
		}
		rs.close();
		System.out.printf("Updating images %d (from %d).\n", imagesToUpdate, imagesProcessed);
		String sql = String.format("UPDATE travel_articles SET src_%s = " + 
						" (SELECT img from source_image s where s.img = travel_articles.%s) " + 
						" WHERE %s IN (SELECT distinct img from source_image)",
						imageColumn, imageColumn, imageColumn);
		int updated = wikivoyageConn.createStatement().executeUpdate(sql);
		System.out.println("Update to full size images finished, updated: " + updated);
	}

	private static String trim(String s) {
		return s.trim().replaceAll("[\\p{Cf}]", "");
	}

	private void copyImagesBetweenArticles(Connection conn, String imageColumn) throws SQLException {
		Statement statement = conn.createStatement();
		System.out.println("Copying headers from english language to others... " + imageColumn);
		String sql = String.format(
				"UPDATE or IGNORE travel_articles set %s = "
				+ "(SELECT %s FROM travel_articles t WHERE t.trip_id = travel_articles.trip_id and t.lang = 'en') "
				+ "WHERE (travel_articles.%s is null or travel_articles.%s = '') and travel_articles.lang <>'en'",
				imageColumn, imageColumn, imageColumn, imageColumn);
		boolean update = statement.execute(sql);
		System.out.println("Copied headers from english language to others: " + update);
        statement.close();
        statement = conn.createStatement();
		System.out.println("Articles without image (" + imageColumn + "):");
		ResultSet rs = statement.executeQuery(
				"select count(*), lang from travel_articles where " + imageColumn + " = '' or " + imageColumn
						+ " is null group by lang");
		while (rs.next()) {
			System.out.println("\t" + rs.getString(2) + " " + rs.getInt(1));
		}
		rs.close();
		statement.close();
	}

	private static class AggArticle {
		String title;
		String lang;
		long wid;
		String partOf;
//		long partOfWid;
	}
	
	private void addColumn(Connection conn, String col) {
   	 try {
            conn.createStatement().execute(String.format("ALTER TABLE travel_articles ADD COLUMN %s", col));
        } catch (Exception e) {
            System.err.printf("Column %s already exists\n", col);
        }
   }
	private void generateAggPartOf(Connection conn) throws SQLException {
		addColumn(conn, "aggregated_part_of");
		addColumn(conn, "agg_part_of_wid");
		PreparedStatement updatePartOf = conn
					.prepareStatement("UPDATE travel_articles SET aggregated_part_of = ?, agg_part_of_wid = ? WHERE title = ? AND lang = ?");
		PreparedStatement deleteOf = conn.prepareStatement("DELETE FROM travel_articles WHERE title = ? AND lang = ?");
		PreparedStatement data = conn.prepareStatement("SELECT trip_id, title, lang, is_part_of, is_part_of_wid FROM travel_articles");
		ResultSet rs = data.executeQuery();
		int batch = 0;
		Map<String, AggArticle> articles = new HashMap<>();
		Map<String, AggArticle> articlesWid = new HashMap<>();
		while (rs.next()) {
			AggArticle art = new AggArticle();
			art.title = rs.getString("title");
			art.lang = rs.getString("lang");
			art.partOf = rs.getString("is_part_of");
//			art.partOfWid = rs.getLong("is_part_of_wid");
			art.wid = rs.getLong("trip_id");
			articles.put(art.lang + ":" + art.title, art);
			articlesWid.put(art.lang + ":" + art.wid, art);
		}
		long time = System.currentTimeMillis();
		for (AggArticle a : articles.values()) {
			StringBuilder agg = new StringBuilder();
			StringBuilder aggWid = new StringBuilder();
			String lang = a.lang;
			String partOf = a.partOf;
			AggArticle parent = getParent(articles, lang, partOf);
			int iterations = 0;
			while (parent != null) {
				if (agg.length() > 0) {
					agg.append(",");
					aggWid.append(",");
				}
				if (articlesWid.containsKey(lang + ":" + parent.wid)) {
					// switch back to local
					parent = articlesWid.get(lang + ":" + parent.wid);
				}
				if (parent.lang.equals(lang)) {
					agg.append(parent.title);
				} else {
					agg.append(parent.lang + ":" + parent.title);
				}
				aggWid.append(parent.wid == 0 ? "" : parent.wid);
				partOf = parent.partOf;
				parent = getParent(articles, parent.lang, partOf);
				if(iterations++ > 25) {
					System.out.println(parent.title + " " + parent.lang);
					if(iterations > 30) {
						System.out.println("! ERROR LOOP DETECTED ERROR !");
						break;
					}
				}
			}
			if (!Algorithms.isEmpty(partOf) && parent == null) {
				System.out.printf("Error parent not reached (delete): %s from %s %s\n", partOf, a.lang, a.title);
				deleteOf.setString(1, a.title);
				deleteOf.setString(2, a.lang);
				deleteOf.execute();
				continue;
			}
			updatePartOf.setString(1, agg.toString());
			updatePartOf.setString(2, aggWid.toString());
			updatePartOf.setString(3, a.title);
			updatePartOf.setString(4, a.lang);
			updatePartOf.addBatch();
			if (++batch% BATCH_SIZE == 0) {
				System.out.printf("Processsed %d %d ms...\n", batch, System.currentTimeMillis() - time);
				time = System.currentTimeMillis();
				updatePartOf.executeBatch();
			}
		}
		updatePartOf.executeBatch();
		finishPrep(updatePartOf);
		data.close();
		rs.close();
	}

	private AggArticle getParent(Map<String, AggArticle> articles, String lang, String partOf) {
		AggArticle parent = articles.get(lang + ":" + partOf);
		if (parent == null) {
			parent = articles.get(partOf); // en case
		}
		return parent;
	}

	public void generateIsParentOf(Connection conn) throws SQLException {
		addColumn(conn, "is_parent_of");
		PreparedStatement updateIsParentOf = conn
				.prepareStatement("UPDATE travel_articles SET is_parent_of = ? WHERE title = ? AND lang = ?");
		PreparedStatement data = conn.prepareStatement("SELECT trip_id, title, lang FROM travel_articles");
		ResultSet rs = data.executeQuery();
		int batch = 0;
		while (rs.next()) {
			String title = rs.getString("title");
			String lang = rs.getString("lang");
			updateIsParentOf.setString(1, getParentOf(conn, rs.getString("title"), lang));
			updateIsParentOf.setString(2, title);
			updateIsParentOf.setString(3, lang);
			updateIsParentOf.addBatch();
			if (batch++ > BATCH_SIZE) {
				updateIsParentOf.executeBatch();
				batch = 0;
			}
		}
		finishPrep(updateIsParentOf);
		data.close();
		rs.close();
	}

	
	public void finishPrep(PreparedStatement ps) throws SQLException {
		ps.addBatch();
		ps.executeBatch();
		ps.close();
	}


	public String getParentOf(Connection conn, String title, String lang) throws SQLException {
		if (title.isEmpty()) {
			return "";
		}
		StringBuilder res = new StringBuilder();
		PreparedStatement ps = conn
				.prepareStatement("SELECT title FROM travel_articles WHERE is_part_of = ? AND lang = '" + lang + "'");
		ps.setString(1, title);
		ResultSet rs = ps.executeQuery();
		String buf = "";

		while (rs.next()) {
			buf = rs.getString(1);
			res.append(buf);
			res.append(';');
		}
		return res.length() > 0 ? res.substring(0, res.length() - 1) : "";
	}
	
}
