package net.osmand.wiki;

import net.osmand.PlatformUtil;
import net.osmand.impl.FileProgressImplementation;
import net.osmand.obf.preparation.DBDialect;
import net.osmand.util.SqlInsertValuesReader;
import org.apache.commons.logging.Log;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;


public class CommonsWikimediaPreparation {

    public static final String COMMONSWIKI_ARTICLES_GZ = "commonswiki-latest-pages-articles.xml.gz";
    public static final String COMMONSWIKI_CATEGORYLINKS_GZ = "commonswiki-latest-categorylinks.sql.gz";
    public static final String COMMONSWIKI_SQLITE = "wikidata_commons_osm.sqlitedb";
    public static final String CATEGORY_NAMESPACE = "14";
    public static final String FILE_NAMESPACE = "6";
    public static final String FILE = "File:";
    public static final String DEPICT_KEY = "[[d:Special:EntityPage/";
    public static final String DEPICT_KEY_END = "]]";
    private static final Log log = PlatformUtil.getLog(CommonsWikimediaPreparation.class);
    private final HashSet<String> filter = new HashSet<>(){{
        add("GFDL");
        add("GFDL_en");
        add("Files_with_no_machine-readable_source");
        add("Self-published_work");
        add("Files_with_no_machine-readable_author");
        add("Media_missing_infobox_template");
        add("License_migration_redundant");
        add("License_migration_completed");
        add("Assumed_own_work");
    }};
    private Map<String, Long> categoryCache = new HashMap<>();

    public static void main(String[] args) {
        String folder = "";
        String mode = "";
        String database = "";
        boolean recreateDb = false;

        for (String arg : args) {
            String val = arg.substring(arg.indexOf("=") + 1);
            if (arg.startsWith("--dir=")) {
                folder = val;
            } else if (arg.startsWith("--mode=")) {
                mode = val;
            } else if (arg.startsWith("--result_db=")) {
                database = val;
            } else if (arg.startsWith("--recreate_db")) {
                recreateDb = true;
            }
        }

        if (mode.isEmpty() || folder.isEmpty()) {
            throw new RuntimeException("Correct arguments weren't supplied");
        }

        final String commonWikiArticles = folder + COMMONSWIKI_ARTICLES_GZ;
        final String categoryLinksSql = folder + COMMONSWIKI_CATEGORYLINKS_GZ;
        final String sqliteFileName = database.isEmpty() ? folder + COMMONSWIKI_SQLITE : database;
        CommonsWikimediaPreparation p = new CommonsWikimediaPreparation();
        try {
            switch (mode) {
                case "parse-commonswiki-articles":
                    p.parseCommonArticles(commonWikiArticles, sqliteFileName, false, recreateDb);
                    break;
                case "parse-categorylinks-sql":
                    p.parseCategoryLinksSQL(categoryLinksSql, sqliteFileName);
                    // will be parse category_links.sql here
                    break;
                case "parse-commonswiki-img-meta":
                    p.parseCommonArticles(commonWikiArticles, sqliteFileName, true, recreateDb);
                    break;
	            default:
		            throw new RuntimeException("Unknown mode: " + mode);
            }
        } catch (ParserConfigurationException e) {
            e.printStackTrace();
        } catch (SAXException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    /** Parse page <id> and <title> for File: and Category: namespaces
     *  Parse depict from <comment>
    */
    private void parseCommonArticles(String articles, String sqliteFileName, boolean parseMeta, boolean recreateDb) throws ParserConfigurationException, SAXException, IOException, SQLException {
        SAXParser sx = SAXParserFactory.newInstance().newSAXParser();
        FileProgressImplementation progress = new FileProgressImplementation("Read commonswiki articles file", new File(articles));
        InputStream streamFile = progress.openFileInputStream();
        InputSource is = getInputSource(streamFile);

        final CommonsWikiHandler handler = new CommonsWikiHandler(sx, progress, new File(sqliteFileName), parseMeta, recreateDb);
        sx.parse(is, handler);
        handler.finish();
    }


    private class CommonsWikiHandler extends DefaultHandler {
        private final SAXParser saxParser;
        private DBDialect dialect = DBDialect.SQLITE;
        private Connection conn;
        private PreparedStatement prepDepict;
        private PreparedStatement prepContent;
        private int[] depictBatch = new int[]{0};
        private int[] contentBatch = new int[]{0};
        private boolean page = false;
        private boolean idPage = false;
        private StringBuilder ctext = null;
        private final StringBuilder title = new StringBuilder();
        private final StringBuilder comment = new StringBuilder();
        private final StringBuilder format = new StringBuilder();
        private final StringBuilder ns = new StringBuilder();
        private final StringBuilder id = new StringBuilder();
        private final FileProgressImplementation progress;
        public static final int BATCH_SIZE = 5000;
        private final StringBuilder textContent = new StringBuilder();
        private final boolean parseMeta;


        CommonsWikiHandler(SAXParser saxParser, FileProgressImplementation progress, File sqliteFile, boolean parseMeta, boolean recreateDb) throws SQLException {
            this.saxParser = saxParser;
            this.progress = progress;
            this.parseMeta = parseMeta;
            conn = dialect.getDatabaseConnection(sqliteFile.getAbsolutePath(), log);
            if (parseMeta) {
                log.info("========= CREATE TABLE common_meta =========");
                if (recreateDb) {
                    log.info("========= DROP old TABLE common_meta =========");
                    conn.createStatement().execute("DROP TABLE IF EXISTS common_meta");
                }
                conn.createStatement().execute(
                        "CREATE TABLE IF NOT EXISTS common_meta(" +
                                "id long, " +
                                "name text, " +
                                "author text, " +
                                "date text, " +
                                "license text, " +
                                "description text)"
                );
                conn.createStatement().execute("DELETE FROM common_meta");
                prepContent = conn.prepareStatement(
                        "INSERT INTO common_meta(id, name, author,  date, license, description) VALUES (?, ?, ?, ?, ?, ?)"
                );
            } else {
                if (recreateDb) {
                    log.info("========= DROP old TABLES common_depict, common_content =========");
                    conn.createStatement().execute("DROP TABLE IF EXISTS common_depict");
                    conn.createStatement().execute("DROP TABLE IF EXISTS common_content");
                }
                log.info("========= CREATE TABLES common_depict, common_content =========");
                conn.createStatement().execute("CREATE TABLE IF NOT EXISTS common_depict(id long, depict_qid long, depict_type text)");
                conn.createStatement().execute(
                        "CREATE TABLE IF NOT EXISTS common_content(" +
                                "id long, " +
                                "name text, " +
                                "ns int)"
                );
                conn.createStatement().execute("DELETE FROM common_depict");
                conn.createStatement().execute("DELETE FROM common_content");
                prepDepict = conn.prepareStatement("INSERT INTO common_depict(id, depict_qid, depict_type) VALUES (?, ?, ?)");
                prepContent = conn.prepareStatement(
                        "INSERT INTO common_content(id, name, ns) VALUES (?, ?, ?)"
                );

            }
        }

        public void finish() throws SQLException {
            log.info("========= DONE =========");
            log.info("Create indexes");
            if (parseMeta) {
                conn.createStatement().execute("CREATE INDEX IF NOT EXISTS id_common_meta_index on common_meta(id)");
                conn.createStatement().execute("CREATE INDEX IF NOT EXISTS name_common_meta_index on common_meta(name)");
            } else {
                conn.createStatement().execute("CREATE INDEX IF NOT EXISTS id_common_depict_index on common_depict(id)");
                conn.createStatement().execute("CREATE INDEX IF NOT EXISTS qid_common_depict_index on common_depict(depict_qid)");
                conn.createStatement().execute("CREATE INDEX IF NOT EXISTS id_common_content on common_content(id)");
                conn.createStatement().execute("CREATE INDEX IF NOT EXISTS category_name_common_content on common_content(name)");
                conn.createStatement().execute("CREATE INDEX IF NOT EXISTS id_ns_common_content on common_content(id, ns)");
                prepDepict.executeBatch();
                prepDepict.close();
            }
            prepContent.executeBatch();
            if (!conn.getAutoCommit()) {
                conn.commit();
            }
            prepContent.close();
            conn.close();
        }

        @Override
        public void startElement(String uri, String localName, String qName, Attributes attributes) {
            String name = saxParser.isNamespaceAware() ? localName : qName;
            if (!page) {
                page = name.equals("page");
            } else {
                if (name.equals("title")) {
                    title.setLength(0);
                    ctext = title;
                } else if (name.equals("comment")) {
                    comment.setLength(0);
                    ctext = comment;
                } else if (name.equals("format")) {
                    format.setLength(0);
                    ctext = format;
                } else if (name.equals("ns")) {
                    ns.setLength(0);
                    ctext = ns;
                } else if (name.equals("id") && !idPage) {
                    id.setLength(0);
                    ctext = id;
                    idPage = true;
                } else if (name.equals("text")) {
                    textContent.setLength(0);
                    ctext = textContent;
                }
            }
        }

        @Override
        public void characters(char[] ch, int start, int length) {
            if (page) {
                if (ctext != null) {
                    ctext.append(ch, start, length);
                }
            }
        }

        @Override
        public void endElement(String uri, String localName, String qName) {
            String name = saxParser.isNamespaceAware() ? localName : qName;
            if (page) {
                switch (name) {
                    case "page":
                        page = false;
                        idPage = false;
                        progress.update();
                        break;
                    case "title":
                    case "format":
                    case "ns":
                    case "comment":
                    case "id":
                        ctext = null;
                        break;
                    case "text":
                        String nameSpace = ns.toString();

                        if (!FILE_NAMESPACE.equals(nameSpace) && !CATEGORY_NAMESPACE.equals(nameSpace)) {
                            break;
                        }

                        if (parseMeta) {
                            parseMeta(nameSpace);
                        } else {
                            parseDepict(nameSpace);
                        }
                        break;
                }
            }
        }

        public void addBatch(PreparedStatement prep, int[] bt) throws SQLException {
            prep.addBatch();
            bt[0] = bt[0] + 1;
            int batch = bt[0];
            if (batch > BATCH_SIZE) {
                prep.executeBatch();
                bt[0] = 0;
            }
        }

        private void parseMeta(String nameSpace) {
            try {
                if (FILE_NAMESPACE.equals(nameSpace)) {
                    String imageTitle = title.toString().startsWith(FILE) ? title.substring(FILE.length()) : null;
                    if (imageTitle == null) {
                        return;
                    }
                    Map<String, String> meta = new HashMap<>();
                    WikiDatabasePreparation.removeMacroBlocks(textContent, meta, new HashMap<>(), null, "en", imageTitle, null, true);
                    WikiDatabasePreparation.prepareMetaData(meta);
                    String author = meta.getOrDefault("author", "");
                    String license = meta.getOrDefault("license", "");
                    String description = meta.getOrDefault("description", "");
                    String date = meta.getOrDefault("date", "");
                    prepContent.setLong(1, Long.parseLong(id.toString()));
                    prepContent.setString(2, imageTitle.replace(" ", "_"));
                    prepContent.setString(3, author);
                    prepContent.setString(4, date);
                    prepContent.setString(5, license);
                    prepContent.setString(6, description);
                    addBatch(prepContent, contentBatch);
                }
            } catch (SQLException | IOException exception) {
                log.error(exception.getMessage(), exception);
            }
        }

        private void parseDepict(String nameSpace) {
            try {
                String n = title.toString().replace("File:", "");
                n = n.replace("Category:", "");
                prepContent.setLong(1, Long.parseLong(id.toString()));
                prepContent.setString(2, n);
                prepContent.setInt(3, Integer.parseInt(nameSpace));
                addBatch(prepContent, contentBatch);

                if (FILE_NAMESPACE.equals(nameSpace) && comment.toString().contains(DEPICT_KEY)) {
                    String depictType = null;
                    String depictQid = null;
                    int l = DEPICT_KEY.length();
                    String c = comment.toString();
                    int s = c.indexOf(DEPICT_KEY);
                    int e = c.indexOf(DEPICT_KEY_END, s + l);
                    if (s != -1 && e != -1) {
                        depictType = c.substring(s + l, e);
                        s = c.indexOf(DEPICT_KEY, e);
                        e = c.indexOf(DEPICT_KEY_END, s + l);
                        if (s != -1 && e != -1) {
                            depictQid = c.substring(s + l, e);
                        }
                    }
                    if (depictQid != null) {
                        prepDepict.setLong(1, Long.parseLong(id.toString()));
                        prepDepict.setLong(2, Long.parseLong(depictQid.substring(1)));
                        prepDepict.setString(3, depictType);
                        addBatch(prepDepict, depictBatch);
                    }
                }

            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }
    }

    private static InputSource getInputSource(InputStream streamFile) throws IOException {
        GZIPInputStream zis = new GZIPInputStream(streamFile);
        Reader reader = new InputStreamReader(zis,"UTF-8");
        InputSource is = new InputSource(reader);
        is.setEncoding("UTF-8");
        return is;
    }

    private void parseCategoryLinksSQL(final String sqlFile, String sqliteFileName) throws SQLException, IOException {
        CategoryLinksSqlHandler handler = new CategoryLinksSqlHandler(new File(sqliteFileName));
        SqlInsertValuesReader.readInsertValuesFile(sqlFile, handler);
        handler.finish();
    }

    private class CategoryLinksSqlHandler implements SqlInsertValuesReader.InsertValueProcessor {
        private DBDialect dialect = DBDialect.SQLITE;
        private Connection conn;
        private PreparedStatement prepCategory;
        private PreparedStatement selectPrep;
        private PreparedStatement selectCatPrep;
        private int[] categoryBatch = new int[]{0};
        public final static int BATCH_SIZE = 5000;
        private long cnt = 0;

        CategoryLinksSqlHandler(File sqliteFile) throws SQLException {
            conn = dialect.getDatabaseConnection(sqliteFile.getAbsolutePath(), log);
            log.info("========= CREATE TABLE common_category_links =========");
            conn.createStatement().execute("CREATE TABLE IF NOT EXISTS common_category_links(id long, category_id long)");
            conn.createStatement().execute("DELETE FROM common_category_links");
            prepCategory = conn.prepareStatement("INSERT INTO common_category_links(id, category_id) VALUES (?, ?)");
            selectPrep = conn.prepareStatement("SELECT count(*) as cnt FROM common_content WHERE common_content.id=? AND common_content.ns=" + FILE_NAMESPACE);
            selectCatPrep = conn.prepareStatement("SELECT common_content.id FROM common_content WHERE common_content.name=? AND common_content.ns=" + CATEGORY_NAMESPACE);
        }

        @Override
        public void process(List<String> vs) {
            long id = Long.parseLong(vs.get(0));
            String catName = vs.get(1);
            String cl_type = vs.get(6);
            if (!cl_type.equals("file")
                    || filter.contains(catName)
                    || catName.startsWith("CC-")
                    || catName.startsWith("PD-")) {
                return;
            }
            try {
                selectPrep.setLong(1, id);
                ResultSet rs = selectPrep.executeQuery();
                int count = 0;
                if (rs.next()) {
                    count = rs.getInt(1);
                }
                selectPrep.clearParameters();
                if (count > 0) {
                    long catId = 0;
                    if (categoryCache.containsKey(catName)) {
                        catId = categoryCache.get(catName);
                    } else {
                        selectCatPrep.setString(1, catName.replace("_", " "));
                        rs = selectCatPrep.executeQuery();
                        if (rs.next()) {
                            catId = rs.getLong(1);
                            categoryCache.put(catName, catId);
                        }
                        selectCatPrep.clearParameters();
                    }
                    if (catId > 0) {
                        prepCategory.setLong(1, id);
                        prepCategory.setLong(2, catId);
                        addBatch(prepCategory, categoryBatch);
                    }
                }
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
            cnt++;
            if (cnt % 100000 == 0) {
                System.out.println(cnt + " id:" + id + " cat:" + catName);
            }
            if (categoryCache.size() > 1000) {
                categoryCache.clear();
            }
        }

        public void addBatch(PreparedStatement prep, int[] bt) throws SQLException {
            prep.addBatch();
            bt[0] = bt[0] + 1;
            int batch = bt[0];
            if (batch > BATCH_SIZE) {
                prep.executeBatch();
                bt[0] = 0;
            }
        }

        public void finish() throws SQLException {
            System.out.println("Create indexes");
            conn.createStatement().execute("CREATE INDEX IF NOT EXISTS id_common_category_links_index on common_category_links(id)");
            conn.createStatement().execute("CREATE INDEX IF NOT EXISTS category_id_common_category_links on common_category_links(category_id)");
            prepCategory.executeBatch();
            if (!conn.getAutoCommit()) {
                conn.commit();
            }
            prepCategory.close();
            selectPrep.close();
            conn.close();
        }
    }
}
