package net.osmand.server.index.type;

public class TypeFactory {
    private static final String FILES_PATTERN = "(?<d>\\D+)\\d?(?:\\.\\w+){1,3}";
    private static final String FONTS_PATTERN = "(?<d>\\D+)(\\.\\w+\\.\\w+)";

    public Type newMapType() {
        return new TypeImpl(TypeNames.MAP, FILES_PATTERN, "region");
    }

    public Type newVoiceType() {
        return new TypeImpl(TypeNames.VOICE, FILES_PATTERN, "region");
    }

    public Type newDepthType() {
        return new TypeImpl(TypeNames.DEPTH, FILES_PATTERN, "inapp");
    }

    public Type newFontsType() {
        return new TypeImpl(TypeNames.FONTS, FONTS_PATTERN, "fonts");
    }

    public Type newWikimapType() {
        return new TypeImpl(TypeNames.WIKIMAP, FILES_PATTERN, "wiki");
    }

    public Type newWikivoyageType() {
        return new TypeImpl(TypeNames.WIKIVOYAGE, FILES_PATTERN, "wikivoyage");
    }

    public Type newRoadmapType() {
        return new TypeImpl(TypeNames.ROAD_MAP, FILES_PATTERN, "road_region");
    }

    public Type newSrtmMapType() {
        return new TypeImpl(TypeNames.SRTM_MAP, FILES_PATTERN, "srtmcountry");
    }

    public Type newHillshadeType() {
        return new TypeImpl(TypeNames.HILLSHADE, FILES_PATTERN, "hillshade");
    }







}
