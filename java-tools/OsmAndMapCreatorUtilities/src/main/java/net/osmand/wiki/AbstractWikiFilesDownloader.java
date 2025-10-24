package net.osmand.wiki;

import net.osmand.PlatformUtil;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.logging.Log;

import java.io.*;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPOutputStream;

public abstract class AbstractWikiFilesDownloader {
	private static final Log log = PlatformUtil.getLog(AbstractWikiFilesDownloader.class);

	public static final String DUMPS_WIKIMEDIA_URL = "https://dumps.wikimedia.org/";
	public static final String OTHER_INCR_URL = "other/incr/";
	public static final String LATEST_URL = "/latest/";
	private static final String PAGES_INCR_XML_BZ_2_SUFFIX = "-pages-meta-hist-incr.xml.bz2";
	private static final String LATEST_PAGES_XML_BZ_2_PATTERNS = "-latest-pages-articles\\d+\\.xml.+bz2\"";
	private final List<String> downloadedPageFiles = new ArrayList<>();
	private long maxId = 0;
	public final String USER_AGENT = "OsmAnd-Bot/1.0 (+https://osmand.net; support@osmand.net) OsmAndJavaServer/1.0";
	private static final String TIMESTAMP_FILE = "timestamp.txt";

	public AbstractWikiFilesDownloader(File wikiDB, boolean daily, DownloadHandler dh) {
		try {
			log.info("Start %s download".formatted(getFilePrefix()));
			maxId = daily ? 0 : getMaxIdFromDb(wikiDB);
			List<FileForDBUpdate> updateFileList;
			String destFolder = wikiDB.getParent();
			if (daily) {
				Instant instant = readTimestampFile(destFolder);
				LocalDate lastModifiedDate = instant.atZone(ZoneId.systemDefault()).toLocalDate();
				updateFileList = getLatestFilesURL(getWikiIncrDirURL(), lastModifiedDate);
			} else {
				updateFileList = readFilesUrl(getWikiLatestDirURL());
			}
			downloadPageFiles(destFolder, updateFileList, dh);
			if (daily) {
				writeTimestampFile(destFolder);
			}
			log.info("Finish %s download".formatted(getFilePrefix()));
		} catch (SQLException | IOException e) {
			e.printStackTrace();
		}
	}

	public abstract String getFilePrefix();

	public abstract long getMaxPageId() throws IOException;

	public abstract long getMaxIdFromDb(File wikiSqlite) throws SQLException;

	public String getWikiLatestDirURL() {
		return DUMPS_WIKIMEDIA_URL + getFilePrefix() + LATEST_URL;
	}

	public String getWikiIncrDirURL() {
		return DUMPS_WIKIMEDIA_URL + OTHER_INCR_URL + getFilePrefix() + "/";
	}

	private String getLatestFilesPattern() {
		return getFilePrefix() + LATEST_PAGES_XML_BZ_2_PATTERNS;
	}

	public long getMaxQId() {
		return maxId;
	}

	public List<String> getDownloadedPageFiles() {
		return downloadedPageFiles;
	}

	Instant readTimestampFile(String pathToTimestamp) {
		Path timestampFile = Path.of(pathToTimestamp, TIMESTAMP_FILE);
		if (!Files.exists(timestampFile)) {
			return Instant.EPOCH;
		}
		try (BufferedReader reader = Files.newBufferedReader(timestampFile)) {
			String line = reader.readLine();
			if (line == null || line.isBlank()) {
				return Instant.EPOCH;
			}
			return Instant.parse(line.trim());
		} catch (IOException | DateTimeParseException e) {
			return Instant.EPOCH;
		}
	}

	void writeTimestampFile(String pathToTimestamp) {
		Instant now = Instant.now();
		try (FileWriter writer = new FileWriter(new File(pathToTimestamp, TIMESTAMP_FILE))) {
			writer.write(now.toString()); // e.g. "2025-10-22T11:24:56.123Z"
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private void downloadPageFiles(String destFolder, List<FileForDBUpdate> updateList, DownloadHandler dh) throws IOException {
		for (FileForDBUpdate p : updateList) {
			String fileName = p.url.substring(p.url.lastIndexOf("/") + 1);
			String gzFile = fileName.replace(".bz2", ".gz");
			gzFile = destFolder + "/" + gzFile;
			File gz = new File(gzFile);
			if (gz.exists()) {
				System.out.println(gzFile + " already downloaded");
				downloadedPageFiles.add(gzFile);
				if (dh != null) {
					dh.onFinishDownload(gzFile);
				}
				continue;
			}
			try (InputStream in = new BufferedInputStream(new URL(p.url).openStream());
				 BZip2CompressorInputStream bzIn = new BZip2CompressorInputStream(in);
				 GZIPOutputStream gzOut = new GZIPOutputStream(new FileOutputStream(gzFile))) {
				byte[] buffer = new byte[8192];
				int n;
				while ((n = bzIn.read(buffer)) != -1) {
					gzOut.write(buffer, 0, n);
				}
			}
			System.out.println(gzFile + " downloading is finished");
			downloadedPageFiles.add(gzFile);
			if (dh != null) {
				dh.onFinishDownload(gzFile);
			}
		}
	}

	public void removeDownloadedPages() {
		for (String s : downloadedPageFiles) {
			File f = new File(s);
			if (f.exists()) {
				f.delete();
			}
		}
	}

	private List<FileForDBUpdate> readFilesUrl(String wikiUrl) throws IOException {

		Pattern pattern = Pattern.compile(getLatestFilesPattern());
		URLConnection connection = new URL(wikiUrl).openConnection();
		connection.setRequestProperty("User-Agent", USER_AGENT);
		connection.connect();
		long maxPageId = getMaxPageId();
		InputStream inputStream = connection.getInputStream();
		BufferedReader r = new BufferedReader(new InputStreamReader(inputStream, Charset.forName("UTF-8")));

		String line;
		List<FileForDBUpdate> result = new ArrayList<>();
		while ((line = r.readLine()) != null) {
			Matcher matcher = pattern.matcher(line);
			if (matcher.find()) {
				String data = line.replace("<a href=\"", "").replaceAll("\">.+$", "");
				String p = data.replaceAll(".+xml-", "").replaceAll("\\.bz2", "");
				String[] pages = p.split("p");
				int min = Integer.parseInt(pages[1]);
				int max = Integer.parseInt(pages[2]);
				FileForDBUpdate fileForUpdate = new FileForDBUpdate();
				fileForUpdate.url = wikiUrl + data;
				if (maxPageId < max || maxPageId < min) {
					result.add(fileForUpdate);
				}
			}
		}
		return result;
	}

	private List<FileForDBUpdate> getLatestFilesURL(String wikiUrl, LocalDate lastUpdateDate) throws IOException {
		// https://dumps.wikimedia.org/other/incr/commonswiki/
		// pattern for <a href="20250926/">20250926/</a> find subfolder whose name is a date
		Pattern pattern = Pattern.compile("<a href=\"\\d{8}/\">\\d{8}/</a>");
		URLConnection connection = new URL(wikiUrl).openConnection();
		connection.setRequestProperty("User-Agent", USER_AGENT);
		connection.connect();
		InputStream inputStream = connection.getInputStream();
		BufferedReader r = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
		String line;
		List<FileForDBUpdate> result = new ArrayList<>();
		while ((line = r.readLine()) != null) {
			Matcher matcher = pattern.matcher(line);
			if (matcher.find()) {
				FileForDBUpdate fileForUpdate = getIncrFile(wikiUrl, line, lastUpdateDate);
				if (fileForUpdate != null) {
					result.add(fileForUpdate);
				}
			}
		}
		return result;
	}

	private FileForDBUpdate getIncrFile(String wikiUrl, String line, LocalDate lastUpdateDate) {
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
		String fileDate = line.replace("<a href=\"", "").replaceAll("/\">.+$", "");
		LocalDate date = LocalDate.parse(fileDate, formatter);
		FileForDBUpdate fileForUpdate = null;
		if (date.isAfter(lastUpdateDate) || date.isEqual(lastUpdateDate)) {
			fileForUpdate = new FileForDBUpdate();
			fileForUpdate.url = wikiUrl + fileDate + "/" + getFilePrefix() + "-" + fileDate + PAGES_INCR_XML_BZ_2_SUFFIX;
		}
		return fileForUpdate;
	}

	public interface DownloadHandler {
		void onFinishDownload(String fileName);
	}

	private static class FileForDBUpdate {
		String url;
	}
}
