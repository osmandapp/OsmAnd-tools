package net.osmand.server.mapillary.wikidata;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.ArrayList;
import java.util.List;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class Query {

    private List<WikiPage> pages;

    public List<WikiPage> getPages() {
        return pages;
    }

    public void setPages(List<WikiPage> pages) {
        this.pages = pages;
    }
}
