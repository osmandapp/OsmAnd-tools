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
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WikiDatabaseUpdater {

    private static final Log log = PlatformUtil.getLog(WikiDatabasePreparation.class);
    private final String WIKIDATA_URL = "https://dumps.wikimedia.org/wikidatawiki/latest/";
    private List<String> downloadedPages = new ArrayList<>();
    private long maxQId = 0;

    public WikiDatabaseUpdater(File db) {
        try {
            maxQId = getMaxQIdFromDb(db);
            long maxId = getMaxId();
            Pattern pattern = Pattern.compile("wikidatawiki-latest-pages-articles\\d+\\.xml.+bz2\"");
            List<Page> pages = readUrl(pattern);
            List<Page> updateList =  new ArrayList<>();
            for (Page p : pages) {
                if (maxId < p.max || maxId < p.min) {
                    updateList.add(p);
                }
            }
            updateList = getWithoutRepeats(updateList);
            String folder = db.getParent();
            downloadPages(folder, updateList);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } catch (IOException e) {
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

    private void downloadPages(String folder, List<Page> updateList) throws IOException {
        for (Page p : updateList) {
            String gzFile = p.url.replace(".bz2", ".gz");
            gzFile = folder + "/" + gzFile;
            File gz = new File(gzFile);
            if (gz.exists()) {
                continue;
            }
            String cmd = "curl " + WIKIDATA_URL + p.url + " | bzcat | gzip -1 > " + gzFile;
            System.out.println(cmd);
            Runtime.getRuntime().exec(new String[] { "bash", "-c", cmd });
            System.out.println(gzFile + " downloaded");
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

    private List<Page> readUrl(Pattern pattern) throws IOException {
        URLConnection connection = new URL(WIKIDATA_URL).openConnection();
        connection
                .setRequestProperty("User-Agent",
                        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.95 Safari/537.11");
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
        if (result.size() == 0) {
            throw new RuntimeException("Could not download list from " + WIKIDATA_URL);
        }
        return result;
    }

    private long getMaxQIdFromDb(File wikidataSqlite) throws SQLException {
        DBDialect dialect = DBDialect.SQLITE;
        dialect.removeDatabase(wikidataSqlite);
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
