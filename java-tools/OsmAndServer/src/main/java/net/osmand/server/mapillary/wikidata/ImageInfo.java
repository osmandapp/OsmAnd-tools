package net.osmand.server.mapillary.wikidata;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class ImageInfo {
    private String timestamp;
    private String user;
    private String thumburl;
    private Integer thumbwidth;
    private Integer thumbheight;
    private String url;
    private String descriptionurl;
    private String descriptionshorturl;

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getThumburl() {
        return thumburl;
    }

    public void setThumburl(String thumburl) {
        this.thumburl = thumburl;
    }

    public Integer getThumbwidth() {
        return thumbwidth;
    }

    public void setThumbwidth(Integer thumbwidth) {
        this.thumbwidth = thumbwidth;
    }

    public Integer getThumbheight() {
        return thumbheight;
    }

    public void setThumbheight(Integer thumbheight) {
        this.thumbheight = thumbheight;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getDescriptionurl() {
        return descriptionurl;
    }

    public void setDescriptionurl(String descriptionurl) {
        this.descriptionurl = descriptionurl;
    }

    public String getDescriptionshorturl() {
        return descriptionshorturl;
    }

    public void setDescriptionshorturl(String descriptionshorturl) {
        this.descriptionshorturl = descriptionshorturl;
    }
}
