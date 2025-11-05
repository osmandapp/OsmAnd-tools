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
	public static final String[] PROP_IMAGE = {"P18", "P180"};
	public static final String PROP_COMMON_CAT = "P373";
	private final String PROP_COMMON_COORDS = "P625";

	@Override
    public Article deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
		Article article = new Article();
		try {
			JsonObject obj = (JsonObject) json;
			Object oClaims = obj.get("claims");
			if (oClaims instanceof JsonObject) {
				JsonObject claims = obj.getAsJsonObject("claims");
				JsonArray propCoords = claims.getAsJsonArray(PROP_COMMON_COORDS);
				if (propCoords != null) {
					JsonObject coordinatesDataValue = propCoords.get(0).getAsJsonObject().getAsJsonObject("mainsnak")
							.getAsJsonObject("datavalue");
					if (coordinatesDataValue != null) {
						JsonObject coordinates = coordinatesDataValue.getAsJsonObject("value");
						double lat = coordinates.getAsJsonPrimitive("latitude").getAsDouble();
						double lon = coordinates.getAsJsonPrimitive("longitude").getAsDouble();
						article.setLat(lat);
						article.setLon(lon);
					}
				}
				JsonArray propImage = null;
				for (String property : PROP_IMAGE) {
					propImage = claims.getAsJsonArray(property);
					if (propImage != null) {
						JsonObject imageDataValue = propImage.get(0).getAsJsonObject().getAsJsonObject("mainsnak")
								.getAsJsonObject("datavalue");
						if (imageDataValue != null) {
							String image = imageDataValue.getAsJsonPrimitive("value").getAsString();
							article.setImage(image);
							article.setImageProp(property);
						}
						break;
					}
				}
				JsonArray propCommonCat = claims.getAsJsonArray(PROP_COMMON_CAT);
				if (propCommonCat != null) {
					JsonObject ccDataValue = propCommonCat.get(0).getAsJsonObject().getAsJsonObject("mainsnak")
							.getAsJsonObject("datavalue");
					if (ccDataValue != null) {
						String commonCat = ccDataValue.getAsJsonPrimitive("value").getAsString();
						article.setCommonCat(commonCat);
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
		private String image;
		private String imageProp;
		private String commonCat;

		public List<SiteLink> getSiteLinks() {
			return siteLinks;
		}

		public double getLon() {
			return lon;
		}

		public double getLat() {
			return lat;
		}

		public String getImage() {
			return image;
		}

		public String getCommonCat() {
			return commonCat;
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

		public void setImage(String img) {
			this.image = img;
		}

		public void setCommonCat(String cc) {
			this.commonCat = cc;
		}

		public String getImageProp() {
			return imageProp;
		}

		public void setImageProp(String imageProp) {
			this.imageProp = imageProp;
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
