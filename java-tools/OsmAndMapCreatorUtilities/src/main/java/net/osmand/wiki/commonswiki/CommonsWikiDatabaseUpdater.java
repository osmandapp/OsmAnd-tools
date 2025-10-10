package net.osmand.wiki.commonswiki;

import net.osmand.wiki.WikiFilesDownloader;

import java.io.*;

public class CommonsWikiDatabaseUpdater extends WikiFilesDownloader {
    
    public static final String DUMPS_WIKIMEDIA_URL = "https://dumps.wikimedia.org/";
    public static final String INCR_WIKIDATA_URL = "other/incr/commonswiki/";
    public static final String LATEST_WIKIDATA_URL = "commonswiki/latest/";
    private final String WIKIDATA_LATEST_URL = DUMPS_WIKIMEDIA_URL + LATEST_WIKIDATA_URL;
    private final String WIKIDATA_INCR_URL = DUMPS_WIKIMEDIA_URL + INCR_WIKIDATA_URL;

    public CommonsWikiDatabaseUpdater(File wikidataDB, boolean daily) {
        super(wikidataDB, daily);
    }
    
    public String getWikiLatestDirURL() {
        return WIKIDATA_LATEST_URL;
    }

    public String getFilePrefix() {
        return "commonswiki";
    }

    public String getWikiIncrDirURL() {
        return WIKIDATA_INCR_URL;
    }

    
}
