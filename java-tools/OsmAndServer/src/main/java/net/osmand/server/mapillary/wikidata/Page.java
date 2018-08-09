package net.osmand.server.mapillary.wikidata;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.List;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class Page {
    private Integer ns;
    private String title;
    private Boolean missing;
    @JsonInclude(JsonInclude.Include.ALWAYS)
    private String imagerepository;
    private Long pageid;
    private List<ImageInfo> imageinfo;


    public Integer getNs() {
        return ns;
    }

    public void setNs(Integer ns) {
        this.ns = ns;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getImagerepository() {
        return imagerepository;
    }

    public void setImagerepository(String imagerepository) {
        this.imagerepository = imagerepository;
    }

    public Long getPageid() {
        return pageid;
    }

    public void setPageid(Long pageid) {
        this.pageid = pageid;
    }

    public List<ImageInfo> getImageinfo() {
        return imageinfo;
    }

    public void setImageinfo(List<ImageInfo> imageinfo) {
        this.imageinfo = imageinfo;
    }

    public Boolean getMissing() {
        return missing;
    }

    public void setMissing(Boolean missing) {
        this.missing = missing;
    }
}
