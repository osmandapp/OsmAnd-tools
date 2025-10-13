package net.osmand.wiki;

import java.io.*;

public class WikidataFilesDownloader extends AbstractWikiFilesDownloader {

	public static final String DUMPS_WIKIMEDIA_URL = "https://dumps.wikimedia.org/";
	public static final String INCR_WIKIDATA_URL = "other/incr/wikidatawiki/";
	public static final String LATEST_WIKIDATA_URL = "wikidatawiki/latest/";

	public WikidataFilesDownloader(File wikiDB, boolean daily) {
		super(wikiDB, daily);
	}

	public String getWikiLatestDirURL() {
		return DUMPS_WIKIMEDIA_URL + LATEST_WIKIDATA_URL;
	}

	public String getFilePrefix() {
		return "wikidatawiki";
	}

	public String getWikiIncrDirURL() {
		return DUMPS_WIKIMEDIA_URL + INCR_WIKIDATA_URL;
	}
}
