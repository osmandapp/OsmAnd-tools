package net.osmand.wiki.wikidata;

import com.google.gson.*;

import java.lang.reflect.Type;

import org.apache.commons.logging.Log;

import net.osmand.PlatformUtil;

public class ArticleMapper implements JsonDeserializer<ArticleMapper.Article> {
    private static final long ERROR_BATCH_SIZE = 5000l;
	private static int errorCount;
	private static Log log = PlatformUtil.getLog(ArticleMapper.class);

	@Override
    public Article deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
        Article article = new Article();
        try {
            JsonObject obj = (JsonObject) json;
            JsonArray prop625 = obj.getAsJsonObject("claims").getAsJsonArray("P625");
			if (prop625 != null) {
				JsonObject coordinates = prop625.get(0).getAsJsonObject().getAsJsonObject("mainsnak")
						.getAsJsonObject("datavalue").getAsJsonObject("value");
				double lat = coordinates.getAsJsonPrimitive("latitude").getAsDouble();
				double lon = coordinates.getAsJsonPrimitive("longitude").getAsDouble();
				JsonObject labels = obj.getAsJsonObject("labels");
				article.setLabels(labels);
				article.setLat(lat);
				article.setLon(lon);
			}
        } catch (Exception e) {
        	errorCount++;
			if(errorCount == ERROR_BATCH_SIZE) {
				log.error(e.getMessage(), e);
			}
			if(errorCount % ERROR_BATCH_SIZE == 0) {
				log.error(String.format("Error json pages %s (total %d)", json.toString(), errorCount));
			}
        }
        return article;
    }

    public class Article {
        private JsonObject labels;
        private double lat;
        private double lon;

        public JsonObject getLabels() {
            return labels;
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

        public void setLabels(JsonObject labels) {
            this.labels = labels;
        }
    }
}
