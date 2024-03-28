package net.osmand;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import net.osmand.util.MapUtils;

import java.util.List;

public class RenderableObject {
    
    private long id;
    private String type;
    private List<List<Integer>> points;
    private List<TypeValue> types;
    private List<TypeValue> additionalTypes;
    private String mainIcon;
    private List<String> additionalIcons;
    private String shield;
    private int iconOrder;
    private float iconSize;
    private int iconX;
    private int iconY;
    private String text;
    private int textSize;
    private int textOnPath;
    private int textColor;
    private int textShadow;
    private int textShadowColor;
    private boolean bold;
    private boolean italic;
    private String shieldRes;
    private String shieldResIcon;
    
    public static class TypeValue {
        public String tag;
        public String value;
    }
    
    public static RenderableObject fromJson(String json) {
        return new Gson().fromJson(json, RenderableObject.class);
    }
    
    public static JsonObject createGeoJson(List<RenderableObject> renderableObjects) {
        JsonArray featuresArray = new JsonArray();
        
        for (RenderableObject obj : renderableObjects) {
            JsonObject featureObject = getJsonObject(obj);
            JsonObject geometryObject = getGeometryObject(obj);
            featureObject.add("geometry", geometryObject);
            
            featuresArray.add(featureObject);
        }
        JsonObject geoJson = new JsonObject();
        geoJson.addProperty("type", "FeatureCollection");
        geoJson.add("features", featuresArray);
        
        return geoJson;
    }
    
    private static JsonObject getGeometryObject(RenderableObject obj) {
        JsonObject geometryObject = new JsonObject();
        String geometryType = obj.type;
        geometryObject.addProperty("type", geometryType);
        
        if (!obj.points.isEmpty()) {
            JsonArray coordinatesArray = new JsonArray();
            for (List<Integer> point : obj.points) {
                if (point.size() >= 2) {
                    double lat = MapUtils.get31LatitudeY(point.get(1));
                    double lon = MapUtils.get31LongitudeX(point.get(0));
                    JsonArray pointArray = new JsonArray();
                    pointArray.add(lat);
                    pointArray.add(lon);
                    coordinatesArray.add(pointArray);
                }
            }
            geometryObject.add("coordinates", coordinatesArray);
        }
        return geometryObject;
    }
    
    private static JsonObject getJsonObject(RenderableObject obj) {
        JsonObject featureObject = getProperties(obj);
        
        if (obj.types != null && !obj.types.isEmpty()) {
            JsonArray typesArray = new JsonArray();
            for (TypeValue typeValue : obj.types) {
                JsonObject typeObject = new JsonObject();
                typeObject.addProperty("tag", typeValue.tag);
                typeObject.addProperty("value", typeValue.value);
                typesArray.add(typeObject);
            }
            featureObject.add("types", typesArray);
        }
        
        if (obj.additionalTypes != null && !obj.additionalTypes.isEmpty()) {
            JsonArray additionalTypesArray = new JsonArray();
            for (TypeValue typeValue : obj.additionalTypes) {
                JsonObject typeObject = new JsonObject();
                typeObject.addProperty("tag", typeValue.tag);
                typeObject.addProperty("value", typeValue.value);
                additionalTypesArray.add(typeObject);
            }
            featureObject.add("additionalTypes", additionalTypesArray);
        }
        
        if (obj.additionalIcons != null && !obj.additionalIcons.isEmpty()) {
            JsonArray additionalIconsArray = new JsonArray();
            for (String additionalIcon : obj.additionalIcons) {
                additionalIconsArray.add(additionalIcon);
            }
            featureObject.add("additionalIcons", additionalIconsArray);
        }
        
        return featureObject;
    }
    
    private static JsonObject getProperties(RenderableObject obj) {
        JsonObject featureObject = new JsonObject();
        
        featureObject.addProperty("id", obj.id);
        featureObject.addProperty("type", obj.type);
        featureObject.addProperty("mainIcon", obj.mainIcon);
        featureObject.addProperty("shield", obj.shield);
        featureObject.addProperty("iconOrder", obj.iconOrder);
        featureObject.addProperty("iconSize", obj.iconSize);
        featureObject.addProperty("iconX", MapUtils.get31LongitudeX(obj.iconX));
        featureObject.addProperty("iconY", MapUtils.get31LatitudeY(obj.iconY));
        featureObject.addProperty("text", obj.text);
        featureObject.addProperty("textSize", obj.textSize);
        featureObject.addProperty("textOnPath", obj.textOnPath);
        featureObject.addProperty("textColor", obj.textColor);
        featureObject.addProperty("textShadow", obj.textShadow);
        featureObject.addProperty("textShadowColor", obj.textShadowColor);
        featureObject.addProperty("bold", obj.bold);
        featureObject.addProperty("italic", obj.italic);
        featureObject.addProperty("shieldRes", obj.shieldRes);
        featureObject.addProperty("shieldResIcon", obj.shieldResIcon);
        
        return featureObject;
    }
}
