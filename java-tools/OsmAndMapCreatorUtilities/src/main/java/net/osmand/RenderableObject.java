package net.osmand;

import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;

import java.util.List;

public class RenderableObject {
    
    @SerializedName("id")
    private long id;
    
    @SerializedName("type")
    private String type;
    
    @SerializedName("points")
    private List<List<Double>> points;
    
    @SerializedName("types")
    private List<TypeValue> types;
    
    @SerializedName("additionalTypes")
    private List<TypeValue> additionalTypes;
    
    @SerializedName("mainIcon")
    private String mainIcon;
    
    @SerializedName("additionalIcons")
    private List<String> additionalIcons;
    
    @SerializedName("shield")
    private String shield;
    
    @SerializedName("iconX")
    private double iconX;
    
    @SerializedName("iconY")
    private double iconY;
    
    @SerializedName("iconOrder")
    private int iconOrder;
    
    @SerializedName("iconSize")
    private int iconSize;
    
    @SerializedName("text")
    private String text;
    
    @SerializedName("textSize")
    private int textSize;
    
    @SerializedName("textOnPath")
    private int textOnPath;
    
    public static class TypeValue {
        @SerializedName("tag")
        public String tag;
        
        @SerializedName("value")
        public String value;
    }
    
    public static RenderableObject fromJson(String json) {
        return new Gson().fromJson(json, RenderableObject.class);
    }
}
