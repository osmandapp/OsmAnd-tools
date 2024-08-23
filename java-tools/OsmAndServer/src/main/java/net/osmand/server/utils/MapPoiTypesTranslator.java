package net.osmand.server.utils;

import net.osmand.osm.AbstractPoiType;
import net.osmand.osm.MapPoiTypes;
import net.osmand.util.Algorithms;

import java.util.Map;

public class MapPoiTypesTranslator implements MapPoiTypes.PoiTranslator {
    
    private final Map<String, String> enPhrases;
    private final Map<String, String> phrases;
    
    public MapPoiTypesTranslator(Map<String, String> phrases, Map<String, String> enPhrases) {
        this.phrases = phrases;
        this.enPhrases = enPhrases;
    }
    
    @Override
    public String getTranslation(AbstractPoiType type) {
        AbstractPoiType baseLangType = type.getBaseLangType();
        if (baseLangType != null) {
            return getTranslation(baseLangType) + " (" + type.getLang().toLowerCase() + ")";
        }
        return getTranslation(type.getIconKeyName());
    }
    
    @Override
    public String getTranslation(String keyName) {
        String val = phrases.get("poi_" + keyName);
        if (val != null) {
            int ind = val.indexOf(';');
            if (ind > 0) {
                return val.substring(0, ind);
            }
        }
        return val;
    }
    
    @Override
    public String getEnTranslation(AbstractPoiType type) {
        AbstractPoiType baseLangType = type.getBaseLangType();
        if (baseLangType != null) {
            return getEnTranslation(baseLangType) + " (" + type.getLang().toLowerCase() + ")";
        }
        return getEnTranslation(type.getIconKeyName());
    }
    
    @Override
    public String getEnTranslation(String keyName) {
        if (enPhrases.isEmpty()) {
            return Algorithms.capitalizeFirstLetter(keyName.replace('_', ' '));
        }
        String val = enPhrases.get("poi_" + keyName);
        if (val != null) {
            int ind = val.indexOf(';');
            if (ind > 0) {
                return val.substring(0, ind);
            }
        }
        return val;
    }
    
    @Override
    public String getSynonyms(AbstractPoiType type) {
        AbstractPoiType baseLangType = type.getBaseLangType();
        if (baseLangType != null) {
            return getSynonyms(baseLangType);
        }
        return getSynonyms(type.getIconKeyName());
    }
    
    @Override
    public String getSynonyms(String keyName) {
        String val = phrases.get("poi_" + keyName);
        if (val != null) {
            int ind = val.indexOf(';');
            if (ind > 0) {
                return val.substring(ind + 1);
            }
            return "";
        }
        return null;
    }
    
    @Override
    public String getAllLanguagesTranslationSuffix() {
        return "all languages";
    }
}
