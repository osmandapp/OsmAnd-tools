package net.osmand.wiki.wikidata;

import net.osmand.PlatformUtil;
import net.osmand.impl.ConsoleProgressImplementation;
import net.osmand.obf.preparation.DBDialect;
import org.apache.commons.logging.Log;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import javax.xml.parsers.SAXParser;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;


public class WikiDataHandler extends DefaultHandler {

    private static final Log log = PlatformUtil.getLog(WikiDataHandler.class);

    private final SAXParser saxParser;
    private boolean page = false;
    private StringBuilder ctext = null;

    private StringBuilder title = new StringBuilder();
    private StringBuilder text = new StringBuilder();

    private final InputStream progIS;
    private ConsoleProgressImplementation progress = new ConsoleProgressImplementation();
    private Connection conn;
    private PreparedStatement coordsPrep;
    private PreparedStatement mappingPrep;
    private int batch = 0;
    private final static int BATCH_SIZE = 500;

    public WikiDataHandler(SAXParser saxParser, InputStream progIS, File sqliteFile)
            throws IOException, SQLException {
        this.saxParser = saxParser;
        this.progIS = progIS;
        DBDialect dialect = DBDialect.SQLITE;
        dialect.removeDatabase(sqliteFile);
        conn = dialect.getDatabaseConnection(sqliteFile.getAbsolutePath(), log);
        conn.createStatement().execute("CREATE TABLE wiki_coords(id text, lat double, lon double)");
        conn.createStatement().execute("CREATE TABLE wiki_mapping(id text, lang text, title text)");
        conn.createStatement().execute("CREATE INDEX IF NOT EXISTS index_lang_title ON wiki_mapping(lang, title)");
        coordsPrep = conn.prepareStatement("INSERT INTO wiki_coords VALUES (?, ?, ?)");
        mappingPrep = conn.prepareStatement("INSERT INTO wiki_mapping VALUES (?, ?, ?)");


        progress.startTask("Parse wiki xml", progIS.available());
    }

    public void addBatch() throws SQLException {
        coordsPrep.addBatch();
        if (batch++ > BATCH_SIZE) {
            coordsPrep.executeBatch();
            mappingPrep.executeBatch();
            batch = 0;
        }
    }

    public void finish() throws SQLException {
        coordsPrep.executeBatch();
        mappingPrep.executeBatch();
        if (!conn.getAutoCommit()) {
            conn.commit();
        }
        mappingPrep.close();
        coordsPrep.close();
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
            } else if (name.equals("text")) {
                text.setLength(0);
                ctext = text;
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
    public void endElement(String uri, String localName, String qName) throws SAXException {
        String name = saxParser.isNamespaceAware() ? localName : qName;
        try {
            if (page) {
                if (name.equals("page")) {
                    page = false;
                    progress.remaining(progIS.available());
                } else if (name.equals("title")) {
                    ctext = null;
                } else if (name.equals("text")) {
                    try {
                        JSONObject object = new JSONObject(new JSONTokener(ctext.toString()));
                        JSONObject labels = object.getJSONObject("labels");
                        JSONObject claims = object.getJSONObject("claims");
                        JSONArray coordinatesField = claims.getJSONArray("P625");
                        JSONObject coordVals = null;
                        for (int i = 0; i < coordinatesField.length(); i++) {
                            JSONObject obj = coordinatesField.getJSONObject(i);
                            if (obj.keySet().contains("mainsnak")) {
                                coordVals = obj;
                                break;
                            }
                        }
                        Double lat = null;
                        Double lon = null;
                        if (coordVals != null) {
                            JSONObject coordinates = coordVals.getJSONObject("mainsnak").getJSONObject("datavalue")
                                    .getJSONObject("value");
                            lat = (Double) coordinates.get("latitude");
                            lon = (Double) coordinates.get("longitude");
                        }
                        if (lat != null && lon != null) {
                            coordsPrep.setString(1, title.toString());
                            coordsPrep.setDouble(2, lat);
                            coordsPrep.setDouble(3, lon);
                            addTranslationMappings(labels);
                            addBatch();
                        }
                    } catch (JSONException | ClassCastException | SQLException e) {
                        // Generally means that the field is missing in the json or the incorrect data is supplied
                    }


                }
            }
        } catch (IOException e) {
            throw new SAXException(e);
        }
    }

    private void addTranslationMappings(JSONObject labels) throws SQLException {
        for (Object key : labels.keySet()) {
            String lang = (String) key;
            String article = labels.getJSONObject(lang).getString("value");
            mappingPrep.setString(1, title.toString());
            mappingPrep.setString(2, lang);
            mappingPrep.setString(3, article);
            mappingPrep.addBatch();
        }
    }
}

