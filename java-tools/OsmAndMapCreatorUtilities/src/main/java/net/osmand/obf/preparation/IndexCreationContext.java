package net.osmand.obf.preparation;

import net.osmand.map.OsmandRegions;
import net.osmand.map.WorldRegion;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.annotation.Nullable;
import java.io.IOException;

public class IndexCreationContext {
    private static final Log log = LogFactory.getLog(IndexCreationContext.class);

    public String regionName;
    public OsmandRegions allRegions;
    public boolean translitJapaneseNames = false;
    public String regionLang = null;
    public boolean decryptAbbreviations = false;
    public boolean basemap;


    IndexCreationContext(String regionName, boolean basemap) {
        this.regionName = regionName;
        this.allRegions = prepareRegions();
        if (regionName != null) {
            this.translitJapaneseNames = regionName.startsWith("Japan");
            this.regionLang = allRegions != null ? getRegionLang(allRegions) : null;
            this.decryptAbbreviations = needDecryptAbbreviations();
        }
        this.basemap = basemap;
    }

    @Nullable
    private OsmandRegions prepareRegions() {
        OsmandRegions or = new OsmandRegions();
        try {
            or.prepareFile();
            or.cacheAllCountries();
        } catch (IOException e) {
            log.error("Error preparing regions", e);
            return null;
        }
        return or;
    }

    private String getRegionLang(OsmandRegions osmandRegions) {
        WorldRegion wr = osmandRegions.getRegionDataByDownloadName(regionName);
        if (wr != null) {
            return wr.getParams().getRegionLang();
        } else {
            return null;
        }
    }

    private boolean needDecryptAbbreviations() {
        if (regionLang != null) {
            String[] langArr = regionLang.split(",");
            for (String lang : langArr) {
                if (lang.equals("en")) {
                    return true;
                }
            }
        }
        return false;
    }
}
