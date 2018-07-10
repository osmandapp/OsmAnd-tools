package net.osmand.server.index;

public class Index {
    private final String type; //
    private final String containerSize; //
    private final String contentSize; //
    private final String timestamp; //
    private final String date; //
    private final String size; //
    private final String targetsize; //
    private final String name; //
    private final String description;

    public Index(String type, String containerSize, String contentSize, String timestamp, String date, String size,
                 String targetsize, String name, String description) {
        this.type = type;
        this.containerSize = containerSize;
        this.contentSize = contentSize;
        this.timestamp = timestamp;
        this.date = date;
        this.size = size;
        this.targetsize = targetsize;
        this.name = name;
        this.description = description;
    }

    public String getType() {
        return type;
    }

    public String getContainerSize() {
        return containerSize;
    }

    public String getContentSize() {
        return contentSize;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public String getDate() {
        return date;
    }

    public String getSize() {
        return size;
    }

    public String getTargetsize() {
        return targetsize;
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }
}
