package net.osmand.wiki;

import net.osmand.PlatformUtil;
import net.osmand.impl.FileProgressImplementation;
import net.osmand.obf.preparation.DBDialect;
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
import java.sql.SQLException;
import java.util.zip.GZIPInputStream;

public class CommonsWikimediaPreparation {

    public static final String COMMONSWIKI_ARTICLES_GZ = "commonswiki-latest-pages-articles.xml.gz";
    public static final String COMMONSWIKI_SQLITE = "commons_wiki.sqlite";
    public static final String CATEGORY_NAMESPACE = "14";
    public static final String FILE_NAMESPACE = "6";
    public static final String DEPICT_KEY = "[[d:Special:EntityPage/";
    public static final String DEPICT_KEY_END = "]]";
    private static final Log log = PlatformUtil.getLog(CommonsWikimediaPreparation.class);

    public static void main(String[] args) {
        String folder = "";
        String mode = "";
        String database = "";

        for (String arg : args) {
            String val = arg.substring(arg.indexOf("=") + 1);
            if (arg.startsWith("--dir=")) {
                folder = val;
            } else if (arg.startsWith("--mode=")) {
                mode = val;
            } else if (arg.startsWith("--result_db=")) {
                database = val;
            }
        }

        if (mode.isEmpty() || folder.isEmpty()) {
            throw new RuntimeException("Correct arguments weren't supplied");
        }

        final String commonWikiArticles = folder + COMMONSWIKI_ARTICLES_GZ;
        final String sqliteFileName = database.isEmpty() ? folder + COMMONSWIKI_SQLITE : database;
        CommonsWikimediaPreparation p = new CommonsWikimediaPreparation();
        try {
            switch (mode) {
                case "parse-commonswiki-articles":
                    p.parseCommonArticles(commonWikiArticles, sqliteFileName);
                    break;
                case "parse-categorylinks-sql":
                    // will be parse category_links.sql here
                    break;
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
    private void parseCommonArticles(String articles, String sqliteFileName) throws ParserConfigurationException, SAXException, IOException, SQLException {
        SAXParser sx = SAXParserFactory.newInstance().newSAXParser();
        FileProgressImplementation progress = new FileProgressImplementation("Read commonswiki articles file", new File(articles));
        InputStream streamFile = progress.openFileInputStream();
        InputSource is = getInputSource(streamFile);

        final CommonsWikiHandler handler = new CommonsWikiHandler(sx, progress, new File(sqliteFileName));
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
        private FileProgressImplementation progress;
        public final static int BATCH_SIZE = 5000;


        CommonsWikiHandler(SAXParser saxParser, FileProgressImplementation progress, File sqliteFile) throws SQLException {
            this.saxParser = saxParser;
            this.progress = progress;
            conn = dialect.getDatabaseConnection(sqliteFile.getAbsolutePath(), log);
            log.info("========= CREATE TABLES cw_depict, cw_content =========");
            conn.createStatement().execute("CREATE TABLE IF NOT EXISTS common_depict(id long, depict_qid long, depict_type text)");
            conn.createStatement().execute("CREATE TABLE IF NOT EXISTS common_content(id long, name text)");
            prepDepict = conn.prepareStatement("INSERT INTO common_depict(id, depict_qid, depict_type) VALUES (?, ?, ?)");
            prepContent = conn.prepareStatement("INSERT INTO common_content(id, name) VALUES (?, ?)");
        }

        public void finish() throws SQLException {
            //log.info("Total accepted: " + count);
            conn.createStatement().execute("CREATE INDEX IF NOT EXISTS id_common_depict_index on common_depict(id)");
            conn.createStatement().execute("CREATE INDEX IF NOT EXISTS qid_common_depict_index on common_depict(depict_qid)");
            conn.createStatement().execute("CREATE INDEX IF NOT EXISTS id_common_content on common_content(id)");
            conn.createStatement().execute("CREATE INDEX IF NOT EXISTS category_name_common_content on common_content(name)");

            prepDepict.executeBatch();
            prepContent.executeBatch();
            if (!conn.getAutoCommit()) {
                conn.commit();
            }
            prepDepict.close();
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

                        try {
                            prepContent.setLong(1, Long.parseLong(id.toString()));
                            prepContent.setString(2, title.toString());
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
    }

    private static InputSource getInputSource(InputStream streamFile) throws IOException {
        GZIPInputStream zis = new GZIPInputStream(streamFile);
        Reader reader = new InputStreamReader(zis,"UTF-8");
        InputSource is = new InputSource(reader);
        is.setEncoding("UTF-8");
        return is;
    }
}
