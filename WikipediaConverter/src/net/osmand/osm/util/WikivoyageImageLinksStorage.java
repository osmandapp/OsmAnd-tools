package net.osmand.osm.util;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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
	private final Map<String, List<String>> map = new ConcurrentHashMap<>();
	
	public WikivoyageImageLinksStorage(String lang, String folder) throws SQLException {
		this.lang = lang;
		this.folder = folder;
		sqliteName = this.lang + "_imageData.sqlite";
		new File(folder + sqliteName).delete();
		conn = (Connection) dialect.getDatabaseConnection(folder + sqliteName, log);
		init();
	}
	
	private void init() throws SQLException {
		conn.createStatement().execute("CREATE TABLE image_links(article_title text, images_json_array text)");
		prep = conn.prepareStatement("INSERT INTO image_links VALUES (?, ?)");
	}
	
	public void writeData() throws SQLException {
		Gson gson = new Gson();
		if (!map.isEmpty()) {
			System.out.println("Image map size " + map.size());
			for (String s : map.keySet()) {
				prep.setString(1, s);
				prep.setString(2, gson.toJson(map.get(s)));
				addBatch();
			}
		}
		finish();
	}
	
	public void saveImageLinks(String title) throws SQLException {
		if (title != null) {
			List<String> res = new ArrayList<>();
			String urlStart = "https://" + lang + ".wikivoyage.org/w/api.php?action=query&prop=info%7Cimageinfo&titles=";
			String urlEnd = "&generator=images&inprop=url&iiprop=url&format=json";
			String json = WikiVoyagePreparation.WikiOsmHandler.readUrl(urlStart + title + urlEnd);
			if (!json.isEmpty()) {
				Gson gson = new Gson();
				try {
					JsonObject obj = gson.fromJson(json, JsonObject.class);
					JsonObject query = obj.getAsJsonObject("query");
					JsonObject pages = query.getAsJsonObject("pages");
					JsonObject objcts = pages.getAsJsonObject();
					for (String s : objcts.keySet()) {
						JsonArray imageInfo = objcts.getAsJsonObject(s).getAsJsonArray("imageinfo");
						JsonObject urls = (JsonObject) imageInfo.get(0);
						String url = urls.get("url").getAsString();
						res.add(url);
					}
					
				} catch (Exception e) {
//					e.printStackTrace();
				}
			}
			if (!res.isEmpty()) {
				map.putIfAbsent(title, res);
			}
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
