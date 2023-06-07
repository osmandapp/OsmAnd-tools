package net.osmand.wiki.wikidata;

import com.google.gson.*;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;

import net.osmand.PlatformUtil;

public class ArticleMapper implements JsonDeserializer<ArticleMapper.Article> {
	private static final long ERROR_BATCH_SIZE = 5000L;
	private static int errorCount;
	private static final Log log = PlatformUtil.getLog(ArticleMapper.class);

	@Override
    public Article deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
		Article article = new Article();
		try {
			JsonObject obj = (JsonObject) json;
			Object oClaims = obj.get("claims");
			if (oClaims instanceof JsonObject) {
				JsonObject claims = obj.getAsJsonObject("claims");
				JsonArray prop625 = claims.getAsJsonArray("P625");
				if (prop625 != null) {
					JsonObject coordinatesDataValue = prop625.get(0).getAsJsonObject().getAsJsonObject("mainsnak")
							.getAsJsonObject("datavalue");
					if (coordinatesDataValue != null) {
						JsonObject coordinates = coordinatesDataValue.getAsJsonObject("value");
						double lat = coordinates.getAsJsonPrimitive("latitude").getAsDouble();
						double lon = coordinates.getAsJsonPrimitive("longitude").getAsDouble();
						article.setLat(lat);
						article.setLon(lon);
					}
				}
			}
			Object links = obj.get("sitelinks");
			if (links != null) {
				List<SiteLink> siteLinks = new ArrayList<>();
				if (links instanceof JsonObject) {
					for (Map.Entry<String, JsonElement> entry : ((JsonObject) links).getAsJsonObject().entrySet()) {
						String lang = entry.getKey().replace("wiki", "");
						if (lang.equals("commons")) {
							continue;
						}
						String title = entry.getValue().getAsJsonObject().getAsJsonPrimitive("title").getAsString();
						siteLinks.add(new SiteLink(lang, title));
					}
				} else if (links instanceof JsonArray && ((JsonArray) links).size() > 0) {
					throw new IllegalArgumentException();
				}
				article.setSiteLinks(siteLinks);
			}
		} catch (Exception e) {
			errorCount++;
			if (errorCount == ERROR_BATCH_SIZE) {
				log.error(e.getMessage(), e);
			}
			if (errorCount % ERROR_BATCH_SIZE == 0) {
				log.error(String.format("Error json pages %s (total %d)", json.toString(), errorCount));
			}
		}
		return article;
	}

	public static class Article {
		private List<SiteLink> siteLinks = new ArrayList<>();
		private double lat;
		private double lon;

		public List<SiteLink> getSiteLinks() {
			return siteLinks;
		}

		public double getLon() {
			return lon;
		}

		public double getLat() {
			return lat;
		}

		public void setLon(double lon) {
			this.lon = lon;
		}

		public void setLat(double lat) {
			this.lat = lat;
		}

		public void setSiteLinks(List<SiteLink> siteLinks) {
			this.siteLinks = siteLinks;
		}
	}

	static class SiteLink {
		String lang;
		String title;

		public SiteLink(String lang, String title) {
			this.lang = lang;
			this.title = title;
		}
	}
}
