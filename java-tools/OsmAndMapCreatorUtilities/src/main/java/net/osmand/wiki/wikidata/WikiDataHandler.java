package net.osmand.wiki.wikidata;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

import javax.xml.parsers.SAXParser;

import net.osmand.PlatformUtil;
import net.osmand.impl.FileProgressImplementation;
import net.osmand.obf.preparation.DBDialect;

import org.apache.commons.logging.Log;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;


public class WikiDataHandler extends DefaultHandler {

    private static final Log log = PlatformUtil.getLog(WikiDataHandler.class);

    private final SAXParser saxParser;
    private boolean page = false;
    private StringBuilder ctext = null;

    private StringBuilder title = new StringBuilder();
    private StringBuilder text = new StringBuilder();

    private FileProgressImplementation progress;
    private Connection conn;
    private PreparedStatement coordsPrep;
    private PreparedStatement mappingPrep;
    private int coordsBatch = 0;
    private int mappingBatch = 0;
    private final static int BATCH_SIZE = 5000;
	private final static int ARTICLE_BATCH_SIZE = 10000;

    private int count = 0;

	private Gson gson;

    public WikiDataHandler(SAXParser saxParser, FileProgressImplementation progress, File sqliteFile)
            throws IOException, SQLException {
        this.saxParser = saxParser;
        this.progress = progress;
        DBDialect dialect = DBDialect.SQLITE;
        dialect.removeDatabase(sqliteFile);
        conn = dialect.getDatabaseConnection(sqliteFile.getAbsolutePath(), log);
        conn.createStatement().execute("CREATE TABLE wiki_coords(id text, lat double, lon double)");
        conn.createStatement().execute("CREATE TABLE wiki_mapping(id text, lang text, title text)");
        coordsPrep = conn.prepareStatement("INSERT INTO wiki_coords VALUES (?, ?, ?)");
        mappingPrep = conn.prepareStatement("INSERT INTO wiki_mapping VALUES (?, ?, ?)");
        gson = new GsonBuilder().registerTypeAdapter(ArticleMapper.Article.class, new ArticleMapper()).create();
    }

    public void addBatch(PreparedStatement prep, boolean coords) throws SQLException {
        prep.addBatch();
        int batch = coords ? ++coordsBatch : ++mappingBatch;
        if (batch > BATCH_SIZE) {
            prep.executeBatch();
            if (coords) {
                coordsBatch = 0;
            } else {
                mappingBatch = 0;
            }
        }
    }

    public void finish() throws SQLException {
        log.info("Total accepted: " + count);
        conn.createStatement().execute("CREATE INDEX IF NOT EXISTS index_lang_title ON wiki_mapping(lang, title)");
        conn.createStatement().execute("CREATE INDEX IF NOT EXISTS id_mapping_index on wiki_mapping(id)");
        conn.createStatement().execute("CREATE INDEX IF NOT EXISTS id_coords_index on wiki_coords(id)");

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
		if (page) {
			if (name.equals("page")) {
				page = false;
				progress.update();
			} else if (name.equals("title")) {
				ctext = null;
			} else if (name.equals("text")) {
				try {
					ArticleMapper.Article article = gson.fromJson(ctext.toString(), ArticleMapper.Article.class);
					if (article.getLabels() != null && article.getLat() != 0 && article.getLon() != 0) {
						if (count++ % ARTICLE_BATCH_SIZE == 0) {
							log.info("Article accepted " + title.toString() + " free memory: "
									+ (Runtime.getRuntime().freeMemory() / (1024 * 1024)));
						}
						coordsPrep.setString(1, title.toString());
						coordsPrep.setDouble(2, article.getLat());
						coordsPrep.setDouble(3, article.getLon());
						addTranslationMappings(article.getLabels());
						addBatch(coordsPrep, true);
					}
				} catch (Exception e) {
					// Generally means that the field is missing in the json or the incorrect data is supplied
				}
			}
		}
	}

    private void addTranslationMappings(JsonObject labels) throws SQLException {
        for (Map.Entry<String, JsonElement> entry : labels.entrySet()) {
            String lang = entry.getKey();
            String article = entry.getValue().getAsJsonObject().getAsJsonPrimitive("value").getAsString();
            mappingPrep.setString(1, title.toString());
            mappingPrep.setString(2, lang);
            mappingPrep.setString(3, article);
            addBatch(mappingPrep, false);
        }
    }
}

