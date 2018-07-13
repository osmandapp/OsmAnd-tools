package net.osmand.server.index.type;

public interface Type {
    String getType();
    String getContainerSize();
    void setContainerSize(long containerSize);
    String getContentSize();
    void setContentSize(long contentSize);
    String getTimestamp();
    void setTimestamp(long timestamp);
    String getDate();
    String getSize();
    void setSize(long size);
    String getTargetSize();
    void setTargetSize(long targetSize);
    String getName();
    void setName(String name);
    String getDescription();
    String getElementName();

}
