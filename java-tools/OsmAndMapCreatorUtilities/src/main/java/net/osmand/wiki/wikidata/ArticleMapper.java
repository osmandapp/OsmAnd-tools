package net.osmand.wiki.wikidata;

import com.google.gson.*;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;

import net.osmand.PlatformUtil;

public class ArticleMapper implements JsonDeserializer<ArticleMapper.Article> {
	private static final long ERROR_BATCH_SIZE = 5000L;
	public static final String LABELS_KEY = "labels";
	public static final String SITELINKS_KEY = "sitelinks";
	public static final String TITLE_KEY = "title";
	public static final String CLAIMS_KEY = "claims";
	public static final String MAINSNAK_KEY = "mainsnak";
	public static final String DATAVALUE_KEY = "datavalue";
	public static final String VALUE_KEY = "value";
	public static final String LATITUDE_KEY = "latitude";
	public static final String LONGITUDE_KEY = "longitude";
	public static final String LANGUAGE_KEY = "language";
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
			Object oClaims = obj.get(CLAIMS_KEY);
			if (oClaims instanceof JsonObject) {
				JsonObject claims = obj.getAsJsonObject(CLAIMS_KEY);
				JsonArray propCoords = claims.getAsJsonArray(PROP_COMMON_COORDS);
				if (propCoords != null) {
					JsonObject coordinatesDataValue = propCoords.get(0).getAsJsonObject().getAsJsonObject(MAINSNAK_KEY)
							.getAsJsonObject(DATAVALUE_KEY);
					if (coordinatesDataValue != null) {
						JsonObject coordinates = coordinatesDataValue.getAsJsonObject(VALUE_KEY);
						double lat = coordinates.getAsJsonPrimitive(LATITUDE_KEY).getAsDouble();
						double lon = coordinates.getAsJsonPrimitive(LONGITUDE_KEY).getAsDouble();
						article.setLat(lat);
						article.setLon(lon);
					}
				}
				for (String property : PROP_IMAGE) {
					JsonArray propImage = claims.getAsJsonArray(property);
					if (propImage != null) {
						JsonObject imageDataValue = propImage.get(0).getAsJsonObject().getAsJsonObject(MAINSNAK_KEY)
								.getAsJsonObject(DATAVALUE_KEY);
						if (imageDataValue != null) {
							String image = imageDataValue.getAsJsonPrimitive(VALUE_KEY).getAsString();
							article.setImage(image);
							article.setImageProp(property);
						}
						break;
					}
				}
				JsonArray propCommonCat = claims.getAsJsonArray(PROP_COMMON_CAT);
				if (propCommonCat != null) {
					JsonObject ccDataValue = propCommonCat.get(0).getAsJsonObject().getAsJsonObject(MAINSNAK_KEY)
							.getAsJsonObject(DATAVALUE_KEY);
					if (ccDataValue != null) {
						String commonCat = ccDataValue.getAsJsonPrimitive(VALUE_KEY).getAsString();
						article.setCommonCat(commonCat);
					}
				}
			}
			Object object = obj.get(SITELINKS_KEY);
			if (object instanceof JsonObject links) {
				List<SiteLink> siteLinks = new ArrayList<>();
				for (Map.Entry<String, JsonElement> entry : links.entrySet()) {
					String lang = entry.getKey().replace("wiki", "");
					if ("commons".equals(lang)) {
						continue;
					}
					JsonObject jsonObject = entry.getValue().getAsJsonObject();
					JsonPrimitive title = jsonObject.getAsJsonPrimitive(TITLE_KEY);
					if (title == null) {
						continue;
					}
					siteLinks.add(new SiteLink(lang, title.getAsString()));
				}
				article.setSiteLinks(siteLinks);
			} else if (object instanceof JsonArray links && !links.isEmpty()) {
				throw new IllegalArgumentException("Unexpected sitelinks array format");
			}
			object = obj.get(LABELS_KEY);
			if (object instanceof JsonObject labels) {
				Map<String, String> labelMap = new LinkedHashMap<>();
				for (Map.Entry<String, JsonElement> entry : labels.entrySet()) {
					JsonObject jsonObject = entry.getValue().getAsJsonObject();
					String lang = jsonObject.getAsJsonPrimitive(LANGUAGE_KEY).getAsString();
					String value = jsonObject.getAsJsonPrimitive(VALUE_KEY).getAsString();
					if (!lang.isEmpty() && !value.isEmpty()) {
						labelMap.put(lang, value);
					}
				}
				article.setLabels(labelMap);
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
		private Map<String, String> labels;

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

		public Map<String, String> getLabels() {
			return labels;
		}

		public void setLabels(Map<String, String> labels) {
			this.labels = labels;
		}
	}

	record SiteLink(String lang, String title) {
	}
}
