package net.osmand.server.index;

public class IndexBuilder {
    private String type;
    private String containerSize;
    private String contentSize;
    private String timestamp;
    private String date;
    private String size;
    private String targetsize;
    private String name;
    private String description;

    public IndexBuilder setType(String type) {
        this.type = type;
        return this;
    }

    public IndexBuilder setContainerSize(String containerSize) {
        this.containerSize = containerSize;
        return this;
    }

    public IndexBuilder setContentSize(String contentSize) {
        this.contentSize = contentSize;
        return this;
    }

    public IndexBuilder setTimestamp(String timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    public IndexBuilder setDate(String date) {
        this.date = date;
        return this;
    }

    public IndexBuilder setSize(String size) {
        this.size = size;
        return this;
    }

    public IndexBuilder setTargetsize(String targetsize) {
        this.targetsize = targetsize;
        return this;
    }

    public IndexBuilder setName(String name) {
        this.name = name;
        return this;
    }

    public IndexBuilder setDescription(String description) {
        this.description = description;
        return this;
    }

    public Index createIndex() {
        return new Index(type, containerSize, contentSize, timestamp, date, size, targetsize, name, description);
    }
}