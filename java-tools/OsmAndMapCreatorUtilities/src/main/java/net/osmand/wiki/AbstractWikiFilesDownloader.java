package net.osmand.wiki;

import net.osmand.PlatformUtil;
import net.osmand.util.Algorithms;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.logging.Log;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Comparator;
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
	private String destFolder;
	private Instant nextTimestamp;
	private boolean removeTimestampAfterUpdate;
	public final String USER_AGENT = "OsmAnd-Bot/1.0 (+https://osmand.net; support@osmand.net) OsmAndJavaServer/1.0";
	private static final String TIMESTAMP_FILE = "timestamp.txt";
	private static final int INITIAL_WAIT_TIME_MS = 1000;
	private static final int MAX_RETRIES = 5;
	private static final int CONNECT_TIMEOUT = 30000;
	private static final int READ_TIMEOUT = CONNECT_TIMEOUT * 2;

	public AbstractWikiFilesDownloader(File wikiDB, boolean daily, DownloadHandler dh) {
		try {
			log.info("Start %s download".formatted(getFilePrefix()));
			maxId = daily ? 0 : getMaxIdFromDb(wikiDB);
			List<FileForDBUpdate> updateFileList;
			destFolder = wikiDB.getParent();
			if (daily) {
				Instant instant = readTimestampFile(destFolder);
				LocalDate lastModifiedDate = instant.atZone(ZoneOffset.UTC).toLocalDate();
				boolean failOnMissingDays = !Instant.EPOCH.equals(instant);
				updateFileList = getLatestFilesURL(getWikiIncrDirURL(), lastModifiedDate, failOnMissingDays);
			} else {
				updateFileList = readFilesUrl(getWikiLatestDirURL());
			}
			downloadPageFiles(updateFileList, dh);
			if (!Algorithms.isEmpty(updateFileList)) {
				if (daily) {
					nextTimestamp = getNextDailyTimestamp(updateFileList);
				} else {
					removeTimestampAfterUpdate = downloadsFullLatestDump();
				}
			}
			log.info("Finish %s download".formatted(getFilePrefix()));
		} catch (SQLException | IOException | InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	public abstract String getFilePrefix();

	public abstract long getMaxPageId() throws IOException;

	public abstract long getMaxIdFromDb(File wikiSqlite) throws SQLException;

	protected boolean downloadsFullLatestDump() {
		return true;
	}

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

	static Instant readTimestampFile(String pathToTimestamp) {
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

	static void writeTimestampFile(String pathToTimestamp, Instant timestamp) {
		try (FileWriter writer = new FileWriter(new File(pathToTimestamp, TIMESTAMP_FILE))) {
			writer.write(timestamp.toString()); // e.g. "2025-10-22T11:24:56.123Z"
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public void saveUpdateTimestamp() {
		if (nextTimestamp != null) {
			writeTimestampFile(destFolder, nextTimestamp);
		} else if (removeTimestampAfterUpdate) {
			removeTimestampFile(destFolder);
		}
	}

	private Instant getNextDailyTimestamp(List<FileForDBUpdate> updateFileList) {
		LocalDate lastDumpDate = updateFileList.get(updateFileList.size() - 1).date;
		return lastDumpDate.plusDays(1).atStartOfDay(ZoneOffset.UTC).toInstant();
	}

	static void removeTimestampFile(String pathToTimestamp) {
		try {
			Files.deleteIfExists(Path.of(pathToTimestamp, TIMESTAMP_FILE));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private void downloadPageFiles(List<FileForDBUpdate> updateList, DownloadHandler dh)
			throws IOException, InterruptedException {
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
			File partFile = new File(gzFile + ".part");
			try (InputStream in = new BufferedInputStream(openConnectionWithRetry(p.url).getInputStream());
				 BZip2CompressorInputStream bzIn = new BZip2CompressorInputStream(in);
				 GZIPOutputStream gzOut = new GZIPOutputStream(new FileOutputStream(partFile))) {
				byte[] buffer = new byte[8192];
				int n;
				while ((n = bzIn.read(buffer)) != -1) {
					gzOut.write(buffer, 0, n);
				}
			}
			Files.move(partFile.toPath(), gz.toPath(), StandardCopyOption.REPLACE_EXISTING);
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

	private List<FileForDBUpdate> readFilesUrl(String wikiUrl) throws IOException, InterruptedException {
		Pattern pattern = Pattern.compile(getLatestFilesPattern());
		HttpURLConnection connection = openConnectionWithRetry(wikiUrl);
		long maxPageId = getMaxPageId();
		InputStream inputStream = connection.getInputStream();
		BufferedReader r = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
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
		connection.disconnect();
		return result;
	}

	private List<FileForDBUpdate> getLatestFilesURL(String wikiUrl, LocalDate lastUpdateDate, boolean failOnMissingDays)
			throws IOException, InterruptedException {
		// https://dumps.wikimedia.org/other/incr/commonswiki/
		// pattern for <a href="20250926/">20250926/</a> find subfolder whose name is a date
		Pattern pattern = Pattern.compile("<a href=\"\\d{8}/\">\\d{8}/</a>");
		HttpURLConnection connection = openConnectionWithRetry(wikiUrl);
		InputStream inputStream = connection.getInputStream();
		BufferedReader r = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
		String line;
		List<FileForDBUpdate> result = new ArrayList<>();
		LocalDate firstAvailableDate = null;
		while ((line = r.readLine()) != null) {
			Matcher matcher = pattern.matcher(line);
			if (matcher.find()) {
				String fileDate = line.replace("<a href=\"", "").replaceAll("/\">.+$", "");
				LocalDate date = LocalDate.parse(fileDate, DateTimeFormatter.BASIC_ISO_DATE);
				if (firstAvailableDate == null || date.isBefore(firstAvailableDate)) {
					firstAvailableDate = date;
				}
				if (date.isAfter(lastUpdateDate) || date.isEqual(lastUpdateDate)) {
					result.add(getIncrFile(wikiUrl, date));
				}
			}
		}
		connection.disconnect();
		if (failOnMissingDays) {
			if (firstAvailableDate == null) {
				throw new IOException("No daily dumps found at %s".formatted(wikiUrl));
			}
			if (firstAvailableDate.isAfter(lastUpdateDate)) {
				throw new IOException("Daily dumps since %s are no longer available at %s (the oldest is %s): recreate the database from a full dump"
						.formatted(lastUpdateDate, wikiUrl, firstAvailableDate));
			}
		}
		result.sort(Comparator.comparing(f -> f.date));
		return result;
	}

	private FileForDBUpdate getIncrFile(String wikiUrl, LocalDate date) {
		String fileDate = date.format(DateTimeFormatter.BASIC_ISO_DATE);
		FileForDBUpdate fileForUpdate = new FileForDBUpdate();
		fileForUpdate.url = wikiUrl + fileDate + "/" + getFilePrefix() + "-" + fileDate + PAGES_INCR_XML_BZ_2_SUFFIX;
		fileForUpdate.date = date;
		return fileForUpdate;
	}

	public interface DownloadHandler {
		void onFinishDownload(String fileName);
	}

	private static class FileForDBUpdate {
		String url;
		LocalDate date;
	}

	public HttpURLConnection openConnectionWithRetry(String urlString)
			throws IOException, InterruptedException {
		int attempt = 0;
		long waitTime = INITIAL_WAIT_TIME_MS;

		while (true) {
			attempt++;
			HttpURLConnection connection = (HttpURLConnection) new URL(urlString).openConnection();
			connection.setRequestProperty("User-Agent", USER_AGENT);
			connection.setConnectTimeout(CONNECT_TIMEOUT);
			connection.setReadTimeout(READ_TIMEOUT);

			try {
				int code = connection.getResponseCode();
				if (code == HttpURLConnection.HTTP_OK) {
					return connection;
				} else if (code == 429) {
					String retryAfter = connection.getHeaderField("Retry-After");
					long waitSeconds = retryAfter != null ? Long.parseLong(retryAfter) : waitTime / 1000;
					System.err.printf("429 Too Many Requests (attempt %d). Waiting %ds...%n", attempt, waitSeconds);
					Thread.sleep(waitSeconds * 1000);
					waitTime *= 2;
				} else if (code >= 500 && code < 600) {
					System.err.printf("Server error %d (attempt %d). Retrying in %d ms...%n",
							code, attempt, waitTime);
					Thread.sleep(waitTime);
					waitTime *= 2;
				} else {
					throw new IOException("HTTP error " + code + " for URL " + urlString);
				}
			} catch (IOException e) {
				connection.disconnect();
				if (attempt >= MAX_RETRIES)
					throw new IOException("Failed after retries: " + e.getMessage(), e);
				System.err.printf("Connection error (attempt %d): %s%n", attempt, e.getMessage());
				Thread.sleep(waitTime);
				waitTime *= 2;
			}
		}
	}
}
