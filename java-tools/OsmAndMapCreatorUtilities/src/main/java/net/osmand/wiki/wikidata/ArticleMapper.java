package net.osmand.wiki.wikidata;

import com.google.gson.*;

import java.lang.reflect.Type;

public class ArticleMapper implements JsonDeserializer<ArticleMapper.Article> {
    @Override
    public Article deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
        Article article = new Article();
        try {
            JsonObject obj = (JsonObject) json;
            JsonObject labels = obj.getAsJsonObject("labels");
            JsonObject coordinates = obj.getAsJsonObject("claims").getAsJsonArray("P625")
                    .get(0).getAsJsonObject().getAsJsonObject("mainsnak").getAsJsonObject("datavalue")
                    .getAsJsonObject("value");
            double lat = coordinates.getAsJsonPrimitive("latitude").getAsDouble();
            double lon = coordinates.getAsJsonPrimitive("longitude").getAsDouble();
            article.setLabels(labels);
            article.setLat(lat);
            article.setLon(lon);
        } catch (Exception e) {
            // Missing the required fields or has invalid structure
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
