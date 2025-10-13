package net.osmand.wiki.commonswiki;

import net.osmand.wiki.AbstractWikiFilesDownloader;

import java.io.*;

public class CommonsWikiFilesDownloader extends AbstractWikiFilesDownloader {
	
	public static final String DUMPS_WIKIMEDIA_URL = "https://dumps.wikimedia.org/";
	public static final String INCR_COMMONSWIKI_URL = "other/incr/commonswiki/";
	public static final String LATEST_COMMONSWIKI_URL = "commonswiki/latest/";

	public CommonsWikiFilesDownloader(File wikidataDB, boolean daily) {
		super(wikidataDB, daily);
	}
	
	public String getWikiLatestDirURL() {
		return DUMPS_WIKIMEDIA_URL + LATEST_COMMONSWIKI_URL;
	}

	public String getFilePrefix() {
		return "commonswiki";
	}

	public String getWikiIncrDirURL() {
		return DUMPS_WIKIMEDIA_URL + INCR_COMMONSWIKI_URL;
	}
}
