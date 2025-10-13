package net.osmand.wiki;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.osmand.PlatformUtil;
import net.osmand.obf.preparation.DBDialect;
import org.apache.commons.logging.Log;

import java.io.*;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public abstract class AbstractWikiFilesDownloader {

	private static final Log log = PlatformUtil.getLog(WikiDatabasePreparation.class);
	public static final String PAGES_INCR_XML_BZ_2_SUFFIX = "-pages-meta-hist-incr.xml.bz2";
	private final List<String> downloadedPageFiles = new ArrayList<>();
	private long maxId = 0;
	private final String USER_AGENT = "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.95 Safari/537.11";

	public AbstractWikiFilesDownloader(File wikiDB, boolean daily) {
		try {
			maxId = daily ? 0 : getMaxQIdFromDb(wikiDB);
			List<FileForDBUpdate> updateFileList = new ArrayList<>();

			if (daily) {
				long lastModifiedMillis = wikiDB.lastModified();
				Instant instant = Instant.ofEpochMilli(lastModifiedMillis);
				LocalDate lastModifiedDate = instant.atZone(ZoneId.systemDefault()).toLocalDate();
				updateFileList = getLatestFilesURL(getWikiIncrDirURL(), lastModifiedDate);
			} else {
				long maxPageId = getMaxPageId();
				List<FileForDBUpdate> pages = readFilesUrl(getWikiLatestDirURL());
				for (FileForDBUpdate p : pages) {
					if (maxPageId < p.maxPageId || maxPageId < p.minPageId) {
						updateFileList.add(p);
					}
				}
				updateFileList = getWithoutRepeats(updateFileList);
			}
			String destFolder = wikiDB.getParent();
			downloadPageFiles(destFolder, updateFileList);
		} catch (SQLException | IOException e) {
			e.printStackTrace();
		}
	}

	public abstract String getWikiLatestDirURL();

	public abstract String getFilePrefix();

	public abstract String getWikiIncrDirURL();

	private String getLatestFilesPattern() {
		return getFilePrefix() + "-latest-pages-articles\\d+\\.xml.+bz2\"";
	}

	public long getMaxId() {
		return maxId;
	}

	public List<String> getDownloadedPageFiles() {
		return downloadedPageFiles;
	}

	private List<FileForDBUpdate> getWithoutRepeats(List<FileForDBUpdate> updateList) {
		List<Long> min = new ArrayList<>();
		long repeatedMin = 0;
		for (FileForDBUpdate p : updateList) {
			if (min.contains(p.minPageId)) {
				repeatedMin = p.minPageId;
				break;
			}
			min.add(p.minPageId);
		}
		if (repeatedMin > 0) {
			List<FileForDBUpdate> res = new ArrayList<>();
			long max = 0;
			for (FileForDBUpdate p : updateList) {
				if (p.minPageId != repeatedMin) {
					res.add(p);
				} else {
					max = Math.max(max, p.maxPageId);
				}
			}
			for (FileForDBUpdate p : updateList) {
				if (p.maxPageId == max) {
					res.add(p);
					break;
				}
			}
			return res;
		}
		return updateList;
	}

	private void downloadPageFiles(String destFolder, List<FileForDBUpdate> updateList) throws IOException {
		for (FileForDBUpdate p : updateList) {
			String fileName = p.url.substring(p.url.lastIndexOf("/") + 1);
			String gzFile = fileName.replace(".bz2", ".gz");
			gzFile = destFolder + "/" + gzFile;
			File gz = new File(gzFile);
			if (gz.exists()) {
				System.out.println(gzFile + " already downloaded");
				downloadedPageFiles.add(gzFile);
				continue;
			}
			String cmd = "curl -A \"" + USER_AGENT + "\" " + p.url + " | bzcat | gzip -1 ";
			System.out.println("Download " + p.url);
			System.out.println(cmd);
			Process process = Runtime.getRuntime().exec(new String[]{"bash", "-c", cmd});
			BufferedReader reader = new BufferedReader(
					new InputStreamReader(new GZIPInputStream(process.getInputStream())));

			GZIPOutputStream gzout = new GZIPOutputStream(new FileOutputStream(gzFile));
			String line;
			while ((line = reader.readLine()) != null) {
				line = line + System.lineSeparator();
				gzout.write(line.getBytes(StandardCharsets.UTF_8));
			}
			gzout.close();
			System.out.println(gzFile + " downloading is finished");
			downloadedPageFiles.add(gzFile);
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

		InputStream inputStream = connection.getInputStream();
		BufferedReader r = new BufferedReader(new InputStreamReader(inputStream, Charset.forName("UTF-8")));

		String line;
		List<FileForDBUpdate> result = new ArrayList<>();
		while ((line = r.readLine()) != null) {
			Matcher matcher = pattern.matcher(line);
			if (matcher.find()) {
				FileForDBUpdate fileForUpdate = getLatestFile(wikiUrl, line);
				result.add(fileForUpdate);
			}
		}
		if (result.isEmpty()) {
			throw new RuntimeException("Could not download list from " + wikiUrl);
		}
		return result;
	}

	private List<FileForDBUpdate> getLatestFilesURL(String wikiUrl, LocalDate lastUpdateDate) throws IOException {
		Pattern pattern = Pattern.compile("<a href=\"\\d{8}/\">\\d{8}/</a>");
		URLConnection connection = new URL(wikiUrl).openConnection();
		connection.setRequestProperty("User-Agent", USER_AGENT);
		connection.connect();
		InputStream inputStream = connection.getInputStream();
		BufferedReader r = new BufferedReader(new InputStreamReader(inputStream, Charset.forName("UTF-8")));
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
		if (result.isEmpty()) {
			throw new RuntimeException("Could not download list from " + wikiUrl);
		}
		return result;
	}

	private FileForDBUpdate getLatestFile(String wikiUrl, String line) {
		String data = line.replace("<a href=\"", "").replaceAll("\">.+$", "");
		String p = data.replaceAll(".+xml-", "").replaceAll("\\.bz2", "");
		String[] pagesId = p.split("p");
		FileForDBUpdate fileForUpdate = new FileForDBUpdate();
		fileForUpdate.minPageId = Integer.parseInt(pagesId[1]);
		fileForUpdate.maxPageId = Integer.parseInt(pagesId[2]);
		fileForUpdate.url = wikiUrl + data;
		return fileForUpdate;
	}

	private FileForDBUpdate getIncrFile(String wikiUrl, String line, LocalDate lastUpdateDate) {
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
		String fileDate = line.replace("<a href=\"", "").replaceAll("/\">.+$", "");
		LocalDate date = LocalDate.parse(fileDate, formatter);
		FileForDBUpdate fileForUpdate = null;
		if (date.isAfter(lastUpdateDate)) {
			fileForUpdate = new FileForDBUpdate();
			fileForUpdate.minPageId = 0;
			fileForUpdate.maxPageId = Integer.MAX_VALUE;
			fileForUpdate.url = wikiUrl + fileDate + "/" + getFilePrefix() + "-" + fileDate + PAGES_INCR_XML_BZ_2_SUFFIX;
		}
		return fileForUpdate;
	}

	private long getMaxQIdFromDb(File wikiSqlite) throws SQLException {
		DBDialect dialect = DBDialect.SQLITE;
		Connection conn = dialect.getDatabaseConnection(wikiSqlite.getAbsolutePath(), log);
		ResultSet rs = conn.createStatement().executeQuery("SELECT max(id) FROM wiki_coords");
		long maxQId = 0;
		if (rs.next()) {
			maxQId = rs.getLong(1);
		}
		rs = conn.createStatement().executeQuery("SELECT max(id) FROM wikidata_properties");
		if (rs.next()) {
			maxQId = Math.max(rs.getLong(1), maxQId);
		}
		conn.close();
		if (maxQId == 0) {
			throw new RuntimeException("Could not get max QiD from " + wikiSqlite.getAbsolutePath());
		}
		return maxQId;
	}

	private long getMaxPageId() throws IOException {
		String s = "https://www.wikidata.org/wiki/Special:EntityData/Q" + maxId + ".json";
		URL url = new URL(s);
		URLConnection connection = url.openConnection();
		connection.setRequestProperty("User-Agent", USER_AGENT);
		connection.connect();
		InputStream inputStream = connection.getInputStream();
		ObjectMapper mapper = new ObjectMapper();
		JsonNode json = mapper.readTree(inputStream);
		if (json != null) {
			JsonNode jsonPageId = json.findValue("pageid");
			if (jsonPageId != null) {
				return jsonPageId.asLong();
			}
		}
		throw new RuntimeException("Could not get max id for updating from " + s);
	}

	private static class FileForDBUpdate {
		long minPageId;
		long maxPageId;
		String url;
	}
}
