package net.osmand.server.mapillary.wikidata;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class WikiBatch {

    private Boolean batchcomplete;
    private Query query;

    public Boolean getBatchcomplete() {
        return batchcomplete;
    }

    public void setBatchcomplete(Boolean batchcomplete) {
        this.batchcomplete = batchcomplete;
    }

    public Query getQuery() {
        return query;
    }

    public void setQuery(Query query) {
        this.query = query;
    }
}
