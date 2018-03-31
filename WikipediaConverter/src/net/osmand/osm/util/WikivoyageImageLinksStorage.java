package net.osmand.osm.util;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
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
	private String sqliteName;
	private int batch = 0;
	private final static int BATCH_SIZE = 500;
	private DBDialect dialect = DBDialect.SQLITE;
	private Set<String> savedNames;
	
	public WikivoyageImageLinksStorage(String lang, String folder) throws SQLException {
		this.lang = lang;
		sqliteName = "imageData.sqlite";
		conn = (Connection) dialect.getDatabaseConnection(folder + sqliteName, log);
		init();
	}
	
	private void init() throws SQLException {
		savedNames = new HashSet<>();
		conn.createStatement().execute("CREATE TABLE IF NOT EXISTS image_links(image_title text UNIQUE, image_url text, thumb_url text)");
		conn.createStatement().execute("CREATE INDEX IF NOT EXISTS index_title ON image_links (image_title);");
		prep = conn.prepareStatement("INSERT OR IGNORE INTO image_links VALUES (?, ?, ?)");
	}
		
	public void saveImageLinks(String title) throws SQLException, UnsupportedEncodingException {
		if (title != null) {
			String url = "";
			String thumb = "";
			String urlStart = "https://" + lang + ".wikivoyage.org/w/api.php?action=query&prop=info%7Cimageinfo&titles=";
			String urlEnd = "&generator=images&inprop=url&iiprop=url&iiurlwidth=300&format=json";
			String json = readUrl(urlStart + URLEncoder.encode(title, "UTF-8") + urlEnd);
			String name = "";
			if (batch % 200 == 0) {
				System.out.println("Processing urls for title: " + title);
			}
			if (!json.isEmpty()) {
				Gson gson = new Gson();
				try {
					JsonObject obj = gson.fromJson(json, JsonObject.class);
					JsonObject query = obj.getAsJsonObject("query");
					JsonObject pages = query.getAsJsonObject("pages");
					JsonObject objcts = pages.getAsJsonObject();
					for (String s : objcts.keySet()) {
						JsonObject item = objcts.getAsJsonObject(s);
						name = item.get("title").getAsString();
						name = name.substring(name.indexOf(":") + 1);
						if (name != null && !name.isEmpty() && !savedNames.contains(name)) {
							JsonArray imageInfo = item.getAsJsonArray("imageinfo");
							JsonObject urls = (JsonObject) imageInfo.get(0);
							url = urls.get("url").getAsString();
							thumb = urls.get("thumburl").getAsString();
							addToDB(url, name, thumb);
							savedNames.add(name);
						}
						
					}
					
				} catch (Exception e) {
					// Most likely imageinfo is null meaning there are no images for this article.
				}
			}			
		}
	}

	private void addToDB(String url, String name, String thumbUrl) throws SQLException {
		if (!url.isEmpty() && !name.isEmpty()) {
			prep.setString(1, name);
			prep.setString(2, url);
			prep.setString(3, thumbUrl);
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
	
	public void saveageBanner(String filename) throws UnsupportedEncodingException {
		if (savedNames.contains(filename)) {
			return;
		}
		if (batch % 200 == 0) {
			System.out.println("Saving pagebanner: " + filename);
		}
		String urlBase = "https://" + lang + ".wikivoyage.org/w/api.php?action=query&titles=File:";
		String urlEnd = "&prop=imageinfo&&iiprop=url&&format=json";
		String json = readUrl(urlBase + URLEncoder.encode(filename, "UTF-8") + urlEnd);
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
				addToDB(url, filename, "");
				savedNames.add(filename);
			} catch (Exception e) {
				System.out.println("Request returned error: " + json);
			}
		}
	}
	
	private String readUrl(String urlString) {
		BufferedReader reader = null;
		try {
			URL url = new URL(urlString);
			reader = new BufferedReader(new InputStreamReader(url.openStream()));
			StringBuffer buffer = new StringBuffer();
			int read;
			char[] chars = new char[1024];
			while ((read = reader.read(chars)) != -1)
				buffer.append(chars, 0, read);
			return buffer.toString();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (reader != null)
				try {
					reader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
		}
		return "";
	}
	// method to download the images (unused)
	@SuppressWarnings("unused")
	private byte[] downloadFromUrl(String urlString) {
		URL url = null;
		try {
			url = new URL(urlString);
		} catch (MalformedURLException e1) {
			e1.printStackTrace();
		}
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		InputStream is = null;
		try {
			is = url.openStream();
			byte[] byteChunk = new byte[4096];
			int n;

			while ((n = is.read(byteChunk)) > 0) {
				baos.write(byteChunk, 0, n);
			}
		} catch (IOException e) {
			System.err.printf("Failed while reading bytes from %s: %s", url.toExternalForm(), e.getMessage());
			e.printStackTrace();
		} finally {
			if (is != null) {
				try {
					is.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return baos.toByteArray();
	}
}
