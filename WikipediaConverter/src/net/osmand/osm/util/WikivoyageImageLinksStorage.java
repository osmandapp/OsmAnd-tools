package net.osmand.osm.util;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import net.osmand.PlatformUtil;
import net.osmand.data.preparation.DBDialect;

public class WikivoyageImageLinksStorage {
	
	private static final Log log = PlatformUtil.getLog(WikivoyageImageLinksStorage.class);
	
	private Connection conn;
	private PreparedStatement prep;
	private String lang;
	private String folder;
	private String sqliteName;
	private int batch = 0;
	private final static int BATCH_SIZE = 500;
	private DBDialect dialect = DBDialect.SQLITE;
	private Set<String> savedNames;
	
	public WikivoyageImageLinksStorage(String lang, String folder) throws SQLException {
		this.lang = lang;
		this.folder = folder;
		sqliteName = "imageData.sqlite";
		new File(folder + sqliteName).delete();
		conn = (Connection) dialect.getDatabaseConnection(folder + sqliteName, log);
		init();
	}
	
	private void init() throws SQLException {
		savedNames = new HashSet<>();
		conn.createStatement().execute("CREATE TABLE image_links(image_title text, image_url text)");
		prep = conn.prepareStatement("INSERT INTO image_links VALUES (?, ?)");
	}
	
	public void savePageBanner(String filename) throws UnsupportedEncodingException {
		if (savedNames.contains(filename)) {
			return;
		}
		String urlBase = "https://" + lang + ".wikivoyage.org/w/api.php?action=query&titles=File:";
		String urlEnd = "&prop=imageinfo&&iiprop=url&&format=json";
		String json = WikiVoyagePreparation.WikiOsmHandler.readUrl(urlBase + URLEncoder.encode(filename, "UTF-8") + urlEnd);
		if (!json.isEmpty()) {
			Gson gson = new Gson();
			try {
				JsonObject obj = gson.fromJson(json, JsonObject.class);
				JsonObject query = obj.getAsJsonObject("query");
				JsonObject pages = query.getAsJsonObject("pages");
				JsonObject minOne = pages.getAsJsonObject("-1");
				JsonArray imageInfo = minOne.getAsJsonArray("imageinfo");
				JsonObject urls = (JsonObject) imageInfo.get(0);
				String url = urls.get("url").getAsString();
				addToDB(url, filename);
				savedNames.add(filename);
				System.out.println("Processed page banner: " + filename);
			} catch (Exception e) {

			}
		}
	}
	
	public void saveImageLinks(String title) throws SQLException, UnsupportedEncodingException {
		if (title != null) {
			String url = "";
			String urlStart = "https://" + lang + ".wikivoyage.org/w/api.php?action=query&prop=info%7Cimageinfo&titles=";
			String urlEnd = "&generator=images&inprop=url&iiprop=url&format=json";
			String json = WikiVoyagePreparation.WikiOsmHandler.readUrl(urlStart + URLEncoder.encode(title, "UTF-8") + urlEnd);
			String name = "";
			System.out.println("Processing urls for title: " + title);
			if (!json.isEmpty()) {
				Gson gson = new Gson();
				try {
					JsonObject obj = gson.fromJson(json, JsonObject.class);
					JsonObject query = obj.getAsJsonObject("query");
					JsonObject pages = query.getAsJsonObject("pages");
					JsonObject objcts = pages.getAsJsonObject();
					for (String s : objcts.keySet()) {
						JsonObject item = objcts.getAsJsonObject(s);
						name = item.get("title").getAsString().replaceFirst("File:", "");
						if (name != null && !name.isEmpty() && !savedNames.contains(name)) {
							JsonArray imageInfo = item.getAsJsonArray("imageinfo");
							JsonObject urls = (JsonObject) imageInfo.get(0);
							url = urls.get("url").getAsString();
							addToDB(url, name);
							savedNames.add(name);
						}
						
					}
					
				} catch (Exception e) {
					e.printStackTrace();
				}
			}			
		}
	}

	private void addToDB(String url, String name) throws SQLException {
		if (!url.isEmpty() && !name.isEmpty()) {
			prep.setString(1, name);
			prep.setString(2, url);
			addBatch();
		}
	}
	
	public void addBatch() throws SQLException { 
		prep.addBatch();
		if(batch++ > BATCH_SIZE) {
			prep.executeBatch();
			batch = 0;
		}
	}
	
	public void finish() throws SQLException {
		prep.executeBatch();
		if(!conn.getAutoCommit()) {
			conn.commit();
		}
		prep.close();
		conn.close();
	}
}
