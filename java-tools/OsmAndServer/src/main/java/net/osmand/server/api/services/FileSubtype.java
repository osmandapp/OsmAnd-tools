package net.osmand.server.api.services;

import net.osmand.IndexConstants;
import net.osmand.util.Algorithms;

import java.io.File;

public enum FileSubtype {
    UNKNOWN("", null),
    OTHER("other", ""),
    ROUTING_CONFIG("routing_config", IndexConstants.ROUTING_PROFILES_DIR),
    RENDERING_STYLE("rendering_style", IndexConstants.RENDERERS_DIR),
    WIKI_MAP("wiki_map", IndexConstants.WIKI_INDEX_DIR),
    SRTM_MAP("srtm_map", IndexConstants.SRTM_INDEX_DIR),
    OBF_MAP("obf_map", IndexConstants.MAPS_PATH),
    TILES_MAP("tiles_map", IndexConstants.TILES_INDEX_DIR),
    ROAD_MAP("road_map", IndexConstants.ROADS_INDEX_DIR),
    GPX("gpx", IndexConstants.GPX_INDEX_DIR),
    TTS_VOICE("tts_voice", IndexConstants.VOICE_INDEX_DIR),
    VOICE("voice", IndexConstants.VOICE_INDEX_DIR),
    TRAVEL("travel", IndexConstants.WIKIVOYAGE_INDEX_DIR),
    MULTIMEDIA_NOTES("multimedia_notes", IndexConstants.AV_INDEX_DIR),
    NAUTICAL_DEPTH("nautical_depth", IndexConstants.NAUTICAL_INDEX_DIR),
    FAVORITES_BACKUP("favorites_backup", IndexConstants.BACKUP_INDEX_DIR);
    
    private final String subtypeName;
    private final String subtypeFolder;
    
    FileSubtype(String subtypeName, String subtypeFolder) {
        this.subtypeName = subtypeName;
        this.subtypeFolder = subtypeFolder;
    }
    
    public boolean isMap() {
        return this == OBF_MAP || this == WIKI_MAP || this == SRTM_MAP || this == TILES_MAP || this == ROAD_MAP || this == NAUTICAL_DEPTH;
    }
    
    public String getSubtypeName() {
        return subtypeName;
    }
    
    public String getSubtypeFolder() {
        return subtypeFolder;
    }
    
    
    public static FileSubtype getSubtypeByFileName(String fileName) {
        String name = fileName;
        if (fileName.startsWith(File.separator)) {
            name = fileName.substring(1);
        }
        for (FileSubtype subtype : values()) {
            switch (subtype) {
                case UNKNOWN:
                case OTHER:
                    break;
                case SRTM_MAP:
                    if (isSrtmFile(name)) {
                        return subtype;
                    }
                    break;
                case WIKI_MAP:
                    if (name.endsWith(IndexConstants.BINARY_WIKI_MAP_INDEX_EXT)) {
                        return subtype;
                    }
                    break;
                case OBF_MAP:
                    if (name.endsWith(IndexConstants.BINARY_MAP_INDEX_EXT) && !name.contains(File.separator)) {
                        return subtype;
                    }
                    break;
                case TTS_VOICE:
                    if (name.startsWith(subtype.subtypeFolder)) {
                        if (name.endsWith(IndexConstants.VOICE_PROVIDER_SUFFIX)) {
                            return subtype;
                        } else if (name.endsWith(IndexConstants.TTSVOICE_INDEX_EXT_JS)) {
                            int lastPathDelimiter = name.lastIndexOf('/');
                            if (lastPathDelimiter != -1 && name.substring(0, lastPathDelimiter).endsWith(IndexConstants.VOICE_PROVIDER_SUFFIX)) {
                                return subtype;
                            }
                        }
                    }
                    break;
                case NAUTICAL_DEPTH:
                    if (name.endsWith(IndexConstants.BINARY_DEPTH_MAP_INDEX_EXT)) {
                        return subtype;
                    }
                    break;
                default:
                    if (name.startsWith(subtype.subtypeFolder)) {
                        return subtype;
                    }
                    break;
            }
        }
        return UNKNOWN;
    }
    
    public static boolean isSrtmFile(String fileName) {
        return Algorithms.endsWithAny(fileName,
                IndexConstants.BINARY_SRTM_MAP_INDEX_EXT,
                IndexConstants.BINARY_SRTM_FEET_MAP_INDEX_EXT);
    }
    
    @Override
    public String toString() {
        return subtypeName;
    }
}