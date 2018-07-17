package net.osmand.server.index.type;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TypeImpl implements Type {
    private static final Log LOGGER = LogFactory.getLog(TypeImpl.class);
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("dd.MM.yy");
    private static final double MBS = 1024d * 1024d;

    private final String type;
    private final String descriptionPattern;
    private final String elementName;
    private final String descriptionPrefix;

    private long containerSize;
    private long contentSize;
    private long timestamp;
    private long size;
    private long targetSize;
    private String name;

    public TypeImpl(String elementName, String type, String descriptionPattern) {
        this(elementName, type, descriptionPattern, "");
    }

    public TypeImpl(String elementName, String type,  String descriptionPattern, String descriptionPrefix) {
        this.elementName = elementName;
        this.type = type;
        this.descriptionPrefix = descriptionPrefix;
        this.descriptionPattern = descriptionPattern;
    }

    private double bytesToMbs(long bytes) {
        return bytes / MBS;
    }

    private String createDescription(String name) {
        Pattern p = Pattern.compile(descriptionPattern);
        Matcher m = p.matcher(name);
        if (m.find()) {
            String description = m.group("d");
            description = description.replaceAll("_", " ");
            String descriptionMsg;
            if (!descriptionPrefix.isEmpty()) {
                return String.format("%s %s", descriptionPrefix, description.trim());
            }
            return description.trim();
        }
        String msg = String.format("Cannot create description from %s", name);
        LOGGER.error(msg);
        throw new IllegalArgumentException(msg);
    }

    private String formatDate() {
        return DATE_FORMAT.format(new Date(timestamp));
    }

    @Override
    public String getType() {
        return type;
    }

    @Override
    public String getContainerSize() {
        return String.format("%d", containerSize);
    }

    @Override
    public void setContainerSize(long containerSize) {
        this.containerSize = containerSize;
    }

    @Override
    public String getContentSize() {
        return String.format("%d", contentSize);
    }

    @Override
    public void setContentSize(long contentSize) {
        this.contentSize = contentSize;
    }

    @Override
    public String getTimestamp() {
        return String.format("%d", timestamp);
    }

    @Override
    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String getDate() {
        return formatDate();
    }

    @Override
    public String getSize() {
        return String.format("%.1f", bytesToMbs(size));
    }

    @Override
    public void setSize(long size) {
        this.size = size;
    }

    @Override
    public String getTargetSize() {
        return String.format("%.1f", bytesToMbs(targetSize));
    }

    @Override
    public void setTargetSize(long targetSize) {
        this.targetSize = targetSize;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String getDescription() {
        return createDescription(name);
    }

    @Override
    public String getElementName() {
        return elementName;
    }
}
