package net.osmand.server.api.test.dto;

import java.util.Collections;
import java.util.Map;

/**
 * Represents a single record from a dataset with dynamic columns.
 * This is not a JPA entity but a data holder for use with JDBC.
 */
public class DatasetRecord {

    private final Map<String, Object> data;

    public DatasetRecord(Map<String, Object> data) {
        this.data = data;
    }

    public Map<String, Object> getData() {
        return Collections.unmodifiableMap(data);
    }

    public Object getColumn(String columnName) {
        return data.get(columnName);
    }
}
