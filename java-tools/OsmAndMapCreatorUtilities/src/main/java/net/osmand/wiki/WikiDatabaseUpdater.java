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
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class WikiDatabaseUpdater {

    private static final Log log = PlatformUtil.getLog(WikiDatabasePreparation.class);
    private final String WIKIDATA_URL = "https://dumps.wikimedia.org/wikidatawiki/latest/";
    private final List<String> downloadedPages = new ArrayList<>();
    private long maxQId = 0;
    private final String USER_AGENT = "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.95 Safari/537.11";

    public WikiDatabaseUpdater(File wikidataDB, boolean daily) {
        try {
            maxQId = getMaxQIdFromDb(wikidataDB);
            List<Page> updateList = new ArrayList<>();

            if (daily) {
                long modifiedDate = wikidataDB.lastModified();
                //todo daily url list
                
            } else {
                long maxId = getMaxId();
                List<Page> pages = readUrl(WIKIDATA_URL, false);
                for (Page p : pages) {
                    if (maxId < p.max || maxId < p.min) {
                        updateList.add(p);
                    }
                }
                updateList = getWithoutRepeats(updateList);
            }
            String destFolder = wikidataDB.getParent();
            downloadPages(destFolder, updateList);
        } catch (SQLException | IOException e) {
            e.printStackTrace();
        }
    }

    public long getMaxQId() {
        return maxQId;
    }

    public List<String> getDownloadedPages() {
        return downloadedPages;
    }

    private List<Page> getWithoutRepeats(List<Page> updateList) {
        List<Long> min = new ArrayList<>();
        long repeatedMin = 0;
        for (Page p : updateList) {
            if (min.contains(p.min)) {
                repeatedMin = p.min;
                break;
            }
            min.add(p.min);
        }
        if (repeatedMin > 0) {
            List<Page> res = new ArrayList<>();
            long max = 0;
            for (Page p : updateList) {
                if (p.min != repeatedMin) {
                    res.add(p);
                } else {
                    max = Math.max(max, p.max);
                }
            }
            for (Page p : updateList) {
                if (p.max == max) {
                    res.add(p);
                    break;
                }
            }
            return res;
        }
        return updateList;
    }

    private void downloadPages(String destFolder, List<Page> updateList) throws IOException {
        for (Page p : updateList) {
            String gzFile = p.url.replace(".bz2", ".gz");
            gzFile = destFolder + "/" + gzFile;
            File gz = new File(gzFile);
            if (gz.exists()) {
                System.out.println(gzFile + " already downloaded");
                downloadedPages.add(gzFile);
                continue;
            }
            String cmd = "curl -A \"" + USER_AGENT + "\" "
                    + WIKIDATA_URL + p.url + " | bzcat | gzip -1 ";
            System.out.println("Download " + WIKIDATA_URL + p.url);
            System.out.println(cmd);
            Process process = Runtime.getRuntime().exec(new String[] { "bash", "-c", cmd });
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
            downloadedPages.add(gzFile);
        }
    }

    public void removeDownloadedPages() {
        for (String s : downloadedPages) {
            File f = new File(s);
            if (f.exists()) {
                f.delete();
            }
        }
    }

    private List<Page> readUrl(String wikidataUrl, boolean daily) throws IOException {

        Pattern pattern = daily
                ? Pattern.compile("wikidatawiki-\\d+-pages-meta-hist-incr.xml.bz2\"")
                : Pattern.compile("wikidatawiki-latest-pages-articles\\d+\\.xml.+bz2\"");
        URLConnection connection = new URL(wikidataUrl).openConnection();
        connection
                .setRequestProperty("User-Agent", USER_AGENT);
        connection.connect();

        BufferedReader r = new BufferedReader(new InputStreamReader(connection.getInputStream(),
                Charset.forName("UTF-8")));

        String line;
        List<Page> result = new ArrayList<>();
        while ((line = r.readLine()) != null) {
            Matcher matcher = pattern.matcher(line);
            if (matcher.find()) {
                String data = line.replace("<a href=\"", "").replaceAll("\">.+$", "");
                String p = data.replaceAll(".+xml-", "").replaceAll("\\.bz2", "");
                String[] pages = p.split("p");
                Page page = new Page();
                page.min = Integer.parseInt(pages[1]);
                page.max = Integer.parseInt(pages[2]);
                page.url = data;
                result.add(page);
            }
        }
        if (result.isEmpty()) {
            throw new RuntimeException("Could not download list from " + wikidataUrl);
        }
        return result;
    }

    private long getMaxQIdFromDb(File wikidataSqlite) throws SQLException {
        DBDialect dialect = DBDialect.SQLITE;
        Connection conn = dialect.getDatabaseConnection(wikidataSqlite.getAbsolutePath(), log);
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
            throw new RuntimeException("Could not get max QiD from " + wikidataSqlite.getAbsolutePath());
        }
        return maxQId;
    }

    private long getMaxId() throws IOException {
        String s = "https://www.wikidata.org/wiki/Special:EntityData/Q"+maxQId+".json";
        URL url = new URL(s);
        ObjectMapper mapper = new ObjectMapper();
        JsonNode json = mapper.readTree(url);
        if (json != null) {
            JsonNode jsonPageId = json.findValue("pageid");
            if (jsonPageId != null) {
                return jsonPageId.asLong();
            }
        }
        throw new RuntimeException("Could not get max id for updating from " + s);
    }

    private class Page {
        long min;
        long max;
        String url;
    }

}
