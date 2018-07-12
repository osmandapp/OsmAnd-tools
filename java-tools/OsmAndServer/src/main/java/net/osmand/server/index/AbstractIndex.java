package net.osmand.server.index;

import java.text.SimpleDateFormat;

public abstract class AbstractIndex implements Index{
    protected final String indexName;

    protected AbstractIndex(String indexName) {
        this.indexName = indexName;
    }
}
