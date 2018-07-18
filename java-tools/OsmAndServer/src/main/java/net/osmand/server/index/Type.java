package net.osmand.server.index;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public enum Type {
    MAP (TypeElementNames.REGION, TypeNames.MAP, TypeDescriptionPatterns.FILES_PATTERN, TypeDescriptionPrefixes.MAP),
    VOICE (TypeElementNames.REGION, TypeNames.VOICE, TypeDescriptionPatterns.FILES_PATTERN, TypeDescriptionPrefixes.VOICE),
    DEPTH (TypeElementNames.INAPP, TypeNames.DEPTH, TypeDescriptionPatterns.FILES_PATTERN, TypeDescriptionPrefixes.DEPTH),
    FONTS (TypeElementNames.FONTS, TypeNames.FONTS, TypeDescriptionPatterns.FONTS_PATTERN, TypeDescriptionPrefixes.FONTS),
    WIKI (TypeElementNames.WIKI, TypeNames.WIKIMAP, TypeDescriptionPatterns.FILES_PATTERN, TypeDescriptionPrefixes.WIKIMAP),
    WIKIVOYAGE (TypeElementNames.WIKIVOYAGE, TypeNames.WIKIVOYAGE, TypeDescriptionPatterns.FILES_PATTERN, TypeDescriptionPrefixes.WIKIVOYAGE),
    ROAD_MAP (TypeElementNames.ROAD_REGION, TypeNames.ROAD_MAP, TypeDescriptionPatterns.FILES_PATTERN, TypeDescriptionPrefixes.ROAD_MAP),
    HILLSHADE (TypeElementNames.HILLSHADE, TypeNames.HILLSHADE, TypeDescriptionPatterns.FILES_PATTERN, TypeDescriptionPrefixes.HILLSHADE),
    SRTM_COUNTRY (TypeElementNames.SRTMCOUNTRY, TypeNames.SRTM_MAP, TypeDescriptionPatterns.FILES_PATTERN, TypeDescriptionPrefixes.SRTM_MAP);

    private final String elementName;
    private final String type;
    private final String descriptionPattern;
    private final String descriptionPrefix;

    private long containerSize;
    private long contentSize;
    private long timestamp;
    private long size;
    private long targetSize;
    private String name;

    Type(String elementName, String type, String descriptionPattern, String descriptionPrefix) {
        this.elementName = elementName;
        this.type = type;
        this.descriptionPattern = descriptionPattern;
        this.descriptionPrefix = descriptionPrefix;
    }

    private double bytesToMbs(long bytes) {
        return bytes / TypeConstants.MBS;
    }

    private String formatDate() {
        return TypeConstants.DATE_FORMAT.format(new Date(timestamp));
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
        //LOGGER.error(msg);
        throw new IllegalArgumentException(msg);
    }

    public String getType() {
        return type;
    }

    public String getDate() {
        return formatDate();
    }

    public String getElementName() {
        return elementName;
    }

    public void setContainerSize(long containerSize) {
        this.containerSize = containerSize;
    }

    public String getContainerSize() {
        return String.format("%d", containerSize);
    }

    public void setContentSize(long contentSize) {
        this.contentSize = contentSize;
    }

    public String getContentSize() {
        return String.format("%d", contentSize);
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getTimestamp() {
        return String.format("%d", timestamp);
    }

    public void setSize(long size) {
        this.size = size;
    }

    public String getSize() {
        return String.format("%.1f", bytesToMbs(size));
    }

    public void setTargetSize(long targetSize) {
        this.targetSize = targetSize;
    }

    public String getTargetSize() {
        return String.format("%.1f", bytesToMbs(targetSize));
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return createDescription(name);
    }

    private static class TypeConstants {
        //private static final Log LOGGER = LogFactory.getLog();
        private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("dd.MM.yy");
        private static final double MBS = 1024d * 1024d;
    }

    private static class TypeElementNames {
        private static final String REGION = "region";
        private static final String INAPP = "inapp";
        private static final String FONTS = "fonts";
        private static final String WIKI = "wiki";
        private static final String WIKIVOYAGE = "wikivoyage";
        private static final String ROAD_REGION = "road_region";
        private static final String SRTMCOUNTRY = "srtmcountry";
        private static final String HILLSHADE = "hillshade";
    }

    private static class TypeNames {
        private static final String MAP = "map";
        private static final String VOICE = "voice";
        private static final String FONTS = "fonts";
        private static final String DEPTH = "depth";
        private static final String WIKIMAP = "wikimap";
        private static final String WIKIVOYAGE = "wikivoyage";
        private static final String ROAD_MAP = "road_map";
        private static final String SRTM_MAP = "srtm_map";
        private static final String HILLSHADE = "hillshade";
    }

    private static class TypeDescriptionPatterns {
        private static final String FILES_PATTERN = "(?<d>\\D+)\\d?(?:\\.\\w+){1,3}";
        private static final String FONTS_PATTERN = "(?<d>\\D+)(\\.\\w+\\.\\w+)";
    }

    private static class TypeDescriptionPrefixes {
        private static final String MAP = "Map, Roads, POI, Transport, Address data for";
        private static final String VOICE = "Voice Data";
        private static final String FONTS = "";
        private static final String DEPTH = "Depth contours";
        private static final String WIKIMAP = "POI data for";
        private static final String WIKIVOYAGE = "";
        private static final String ROAD_MAP = "Map, Roads, POI, Transport, Address data for";
        private static final String SRTM_MAP = "SRTM data for";
        private static final String HILLSHADE = "";
    }
}
