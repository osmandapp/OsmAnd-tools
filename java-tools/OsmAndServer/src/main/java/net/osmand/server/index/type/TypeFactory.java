package net.osmand.server.index.type;

import org.springframework.stereotype.Component;

@Component
public class TypeFactory {
    private static final String FILES_PATTERN = "(?<d>\\D+)\\d?(?:\\.\\w+){1,3}";
    private static final String FONTS_PATTERN = "(?<d>\\D+)(\\.\\w+\\.\\w+)";

    public Type newMapType() {
        return new TypeImpl( "region", TypeNames.MAP, FILES_PATTERN);
    }

    public Type newVoiceType() {
        return new TypeImpl(  "region",TypeNames.VOICE, FILES_PATTERN);
    }

    public Type newDepthType() {
        return new TypeImpl("inapp", TypeNames.DEPTH, FILES_PATTERN);
    }

    public Type newFontsType() {
        return new TypeImpl("fonts", TypeNames.FONTS, FONTS_PATTERN);
    }

    public Type newWikimapType() {
        return new TypeImpl("wiki",TypeNames.WIKIMAP, FILES_PATTERN);
    }

    public Type newWikivoyageType() {
        return new TypeImpl("wikivoyage",TypeNames.WIKIVOYAGE, FILES_PATTERN);
    }

    public Type newRoadmapType() {
        return new TypeImpl("road_region",TypeNames.ROAD_MAP, FILES_PATTERN);
    }

    public Type newSrtmMapType() {
        return new TypeImpl("srtmcountry",TypeNames.SRTM_MAP, FILES_PATTERN);
    }

    public Type newHillshadeType() {
        return new TypeImpl("hillshade", TypeNames.HILLSHADE, FILES_PATTERN);
    }







}
