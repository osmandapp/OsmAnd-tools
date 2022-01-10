package net.osmand.wiki;

import com.google.gson.*;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

public class WikiImageUrlStorage {
	private final PreparedStatement urlSelectStat;
	private final PreparedStatement urlInsertStat;
	private int downloadImages = 0;
	private String title;
	private String lang;

	public WikiImageUrlStorage(Connection conn) throws SQLException {
		conn.createStatement().execute("CREATE TABLE IF NOT EXISTS image(name text unique, thumb_url text)");
		urlSelectStat = conn.prepareStatement("SELECT thumb_url FROM image where name = ? ");
		urlInsertStat = conn.prepareStatement("INSERT INTO image(name, thumb_url) VALUES(?, ?) ");
	}

	public void setArticleTitle(String title) {
		this.title = title;
	}

	public void setLang(String lang) {
		this.lang = lang;
	}

	public String getThumbUrl(String imageFileName) {
		String url = null;
		try {
			url = getUrl(imageFileName);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		if (url == null) {
			url = downloadImagesUrl(imageFileName);
		}
		return url;
	}

	private String getUrl(String imageFileName) throws SQLException {
		urlSelectStat.setString(1, imageFileName);
		try (ResultSet rs = urlSelectStat.executeQuery()) {
			if (rs.next()) {
				return rs.getString(1);
			}
		} catch (JsonSyntaxException e) {
			e.printStackTrace();
		}
		return null;
	}

	public String downloadImagesUrl(String imageFileName) {
		JsonObject imagesDataJson;
		String res = "";
		try {
			if (++downloadImages % 50 == 0) {
				System.out.println("Download wiki images " + downloadImages);
			}
			StringBuilder imagesData = new StringBuilder();
			String imageUrl = "https://" + lang + ".wikipedia.org/w/api.php?action=query&prop=imageinfo&iiprop=url"
					+ "&generator=images&iiurlwidth=320&gimlimit=max&format=json&titles="
					+ URLEncoder.encode(title, StandardCharsets.UTF_8.toString());
			InputStream inputStream = new URL(imageUrl).openConnection().getInputStream();
			BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
			String s;
			while ((s = reader.readLine()) != null) {
				imagesData.append(s).append("\n");
			}
			imagesDataJson = new JsonParser().parse(imagesData.toString()).getAsJsonObject();
			JsonObject pages = imagesDataJson.getAsJsonObject("query").getAsJsonObject("pages");
			for (Map.Entry<String, JsonElement> object : pages.entrySet()) {
				String title = object.getValue().getAsJsonObject().getAsJsonPrimitive("title").getAsString();
				String fileName = title.substring(title.indexOf(":") + 1);
				String thumbUrl = object.getValue().getAsJsonObject().getAsJsonArray("imageinfo")
						.get(0).getAsJsonObject().getAsJsonPrimitive("thumburl").getAsString();
				if (fileName.equals(imageFileName)) {
					res = thumbUrl;
					insertUrl(fileName, thumbUrl);
				} else {
					if (getUrl(fileName) == null) {
						insertUrl(fileName, thumbUrl);
					}
				}
			}
		} catch (Exception e) {
			System.err.println("Error downloading wikidata " + imageFileName + " " + e.getMessage());
		}
		return res;
	}

	private void insertUrl(String fileName, String thumbUrl) throws SQLException {
		urlInsertStat.setString(1, fileName);
		urlInsertStat.setString(2, thumbUrl);
		urlInsertStat.execute();
	}
}
