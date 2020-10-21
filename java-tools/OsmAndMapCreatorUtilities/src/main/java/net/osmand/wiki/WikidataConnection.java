package net.osmand.wiki;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.net.URL;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.logging.Log;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;

import net.osmand.PlatformUtil;
import net.osmand.obf.preparation.DBDialect;

public class WikidataConnection {
	private static final Log log = PlatformUtil.getLog(WikidataConnection.class);
	private Connection conn;
	private PreparedStatement pselect;
	private PreparedStatement pinsert;
	private int downloadMetadata = 0;

	public WikidataConnection(File f) throws SQLException {
		conn = (Connection) DBDialect.SQLITE.getDatabaseConnection(f.getAbsolutePath(), log);
		conn.createStatement().execute("CREATE TABLE IF NOT EXISTS wikidata(wikidataid text, metadata text)");
		pselect = conn.prepareStatement("SELECT metadata FROM wikidata where wikidataid = ? ");
		pinsert = conn.prepareStatement("INSERT INTO wikidata( wikidataid, metadata) VALUES(?, ?) ");
	}

	public JsonObject downloadMetadata(String id) {
		JsonObject obj = null;
		try {
			if (++downloadMetadata % 50 == 0) {
				System.out.println("Download wiki metadata " + downloadMetadata);
			}
			StringBuilder metadata = new StringBuilder();
			String metadataUrl = "https://www.wikidata.org/wiki/Special:EntityData/" + id + ".json";
			URL url = new URL(metadataUrl);
			BufferedReader reader = new BufferedReader(new InputStreamReader(url.openStream()));
			String s;
			while ((s = reader.readLine()) != null) {
				metadata.append(s).append("\n");
			}
			obj = new JsonParser().parse(metadata.toString()).getAsJsonObject();
			pinsert.setString(1, id);
			pinsert.setString(2, metadata.toString());
			pinsert.execute();
		} catch (Exception e) {
			System.err.println("Error downloading wikidata " + id + " " + e.getMessage());
		}
		return obj;
	}

	public JsonObject getMetadata(String id) throws SQLException {
		pselect.setString(1, id);
		ResultSet rs = pselect.executeQuery();
		try {
			if (rs.next()) {
				return new JsonParser().parse(rs.getString(1)).getAsJsonObject();
			}
		} catch (JsonSyntaxException e) {
			e.printStackTrace();
			return null;
		} finally {
			rs.close();
		}
		return null;
	}
	
	
	public String getWikipediaTitleByWid(String lang, String wikiDataId) throws SQLException {
		JsonObject metadata = getMetadata(wikiDataId);
		if (metadata == null) {
			metadata = downloadMetadata(wikiDataId);
		}
		if (metadata == null) {
			return "";
		}
		try {
			JsonObject ks = metadata.get("entities").getAsJsonObject();
			JsonElement siteLinksElement = ks.get(ks.entrySet().iterator().next().getKey()).getAsJsonObject()
					.get("sitelinks");
			if (siteLinksElement.isJsonObject() && siteLinksElement.getAsJsonObject().has(lang + "wiki")) {

				return siteLinksElement.getAsJsonObject().get(lang + "wiki").getAsJsonObject().get("title")
						.getAsString();
			}
		} catch (IllegalStateException e) {
			System.err.println("Error parsing wikidata Json " + wikiDataId + " " + metadata);
		}
		return "";
	}

	public void close() throws SQLException {
		conn.close();
	}
}