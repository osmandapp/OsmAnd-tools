package net.osmand.wiki.commonswiki;

import net.osmand.wiki.AbstractWikiFilesDownloader;

import java.io.*;
import java.sql.SQLException;

public class CommonsWikiFilesDownloader extends AbstractWikiFilesDownloader {

	public CommonsWikiFilesDownloader(File wikidataDB, boolean daily, DownloadHandler pt) {
		super(wikidataDB, daily, pt);
	}

	public String getFilePrefix() {
		return "commonswiki";
	}

	public long getMaxIdFromDb(File wikiSqlite) throws SQLException {
		return 0;
	}

	public long getMaxPageId() {
		return getMaxQId();
	}
}
