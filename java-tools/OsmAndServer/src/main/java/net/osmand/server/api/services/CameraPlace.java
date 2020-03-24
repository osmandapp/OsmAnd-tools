package net.osmand.server.api.services;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class CameraPlace {
	
    private String type;
    private Double lat;
    private Double lon;
    private String timestamp;
    private String key;
    private String title;
    private String url;
    private Boolean externalLink;
    private Double ca;
    private String username;
    private Double distance;
    private Double bearing;
    private String imageUrl;
    private String imageHiresUrl;
    private String topIcon;
    private String buttonIcon;
    private String buttonText;
    private String buttonIconColor;
    private String buttonColor;
    private String buttonTextColor;
    private boolean is360 = false;

    public String getType() {
        return type;
    }

    public Double getLat() {
        return lat;
    }

    public Double getLon() {
        return lon;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public boolean is360() {
		return is360;
	}
    
    public String getKey() {
        return key;
    }

    public String getTitle() {
        return title;
    }

    public String getUrl() {
        return url;
    }

    public Boolean isExternalLink() {
        return externalLink;
    }

    public Double getCa() {
        return ca;
    }

    public String getUsername() {
        return username;
    }

    public Double getDistance() {
        return distance;
    }

    public Double getBearing() {
        return bearing;
    }

    public String getImageUrl() {
        return imageUrl;
    }

    public String getImageHiresUrl() {
        return imageHiresUrl;
    }

    public String getTopIcon() {
        return topIcon;
    }

    public String getButtonIcon() {
        return buttonIcon;
    }

    public String getButtonText() {
        return buttonText;
    }

    public String getButtonIconColor() {
        return buttonIconColor;
    }

    public String getButtonColor() {
        return buttonColor;
    }

    public String getButtonTextColor() {
        return buttonTextColor;
    }

    public void setType(String type) {
        this.type = type;
    }
    
    public void setIs360(boolean is360) {
		this.is360 = is360;
	}

    public void setLat(double lat) {
        this.lat = lat;
    }

    public void setLon(double lon) {
        this.lon = lon;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public void setExternalLink(boolean externalLink) {
        this.externalLink = externalLink;
    }

    public void setCa(double ca) {
        this.ca = ca;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public void setDistance(double distance) {
        this.distance = distance;
    }

    public void setBearing(double bearing) {
        this.bearing = bearing;
    }

    public void setImageUrl(String imageUrl) {
        this.imageUrl = imageUrl;
    }

    public void setImageHiresUrl(String imageHiresUrl) {
        this.imageHiresUrl = imageHiresUrl;
    }

    public void setTopIcon(String topIcon) {
        this.topIcon = topIcon;
    }

    public void setButtonIcon(String buttonIcon) {
        this.buttonIcon = buttonIcon;
    }

    public void setButtonText(String buttonText) {
        this.buttonText = buttonText;
    }

    public void setButtonIconColor(String buttonIconColor) {
        this.buttonIconColor = buttonIconColor;
    }

    public void setButtonColor(String buttonColor) {
        this.buttonColor = buttonColor;
    }

    public void setButtonTextColor(String buttonTextColor) {
        this.buttonTextColor = buttonTextColor;
    }
}
