package net.osmand.wiki.wikidata;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.SAXParser;

import net.osmand.PlatformUtil;
import net.osmand.impl.FileProgressImplementation;
import net.osmand.map.OsmandRegions;
import net.osmand.obf.preparation.DBDialect;

import org.apache.commons.logging.Log;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;


public class WikiDataHandler extends DefaultHandler {

    private static final Log log = PlatformUtil.getLog(WikiDataHandler.class);

    private final SAXParser saxParser;
    private boolean page = false;
    private StringBuilder ctext = null;

    private StringBuilder title = new StringBuilder();
    private StringBuilder text = new StringBuilder();
    private StringBuilder format = new StringBuilder();

    private FileProgressImplementation progress;
    private Connection conn;
    private PreparedStatement coordsPrep;
    private PreparedStatement mappingPrep;
    private PreparedStatement wikiRegionPrep;
    private int[] mappingBatch = new int[]{0};
    private int[] coordsBatch = new int[]{0};
    private int[] regionBatch = new int[]{0};
    
    public final static int BATCH_SIZE = 5000;
	private final static int ARTICLE_BATCH_SIZE = 10000;
	private static final int ERROR_BATCH_SIZE = 200;

    private int count = 0;
    private int errorCount = 0;

	private Gson gson;
	
	private OsmandRegions regions;
	private List<String> keyNames = new ArrayList<String>();


    public WikiDataHandler(SAXParser saxParser, FileProgressImplementation progress, File sqliteFile, OsmandRegions regions)
            throws IOException, SQLException {
        this.saxParser = saxParser;
        this.regions = regions;
        this.progress = progress;
        DBDialect dialect = DBDialect.SQLITE;
        dialect.removeDatabase(sqliteFile);
        conn = dialect.getDatabaseConnection(sqliteFile.getAbsolutePath(), log);
        conn.createStatement().execute("CREATE TABLE wiki_coords(id long, originalId text, lat double, lon double)");
        conn.createStatement().execute("CREATE TABLE wiki_mapping(id long, lang text, title text)");
        conn.createStatement().execute("CREATE TABLE wiki_region(id long, regionName text)");
        coordsPrep = conn.prepareStatement("INSERT INTO wiki_coords(id, originalId, lat, lon) VALUES (?, ?, ?, ?)");
        mappingPrep = conn.prepareStatement("INSERT INTO wiki_mapping(id, lang, title) VALUES (?, ?, ?)");
        wikiRegionPrep = conn.prepareStatement("INSERT INTO wiki_region(id, regionName) VALUES(?, ? )");
        gson = new GsonBuilder().registerTypeAdapter(ArticleMapper.Article.class, new ArticleMapper()).create();
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
        log.info("Total accepted: " + count);
        conn.createStatement().execute("CREATE INDEX IF NOT EXISTS map_lang_title_idx ON wiki_mapping(lang, title)");
        conn.createStatement().execute("CREATE INDEX IF NOT EXISTS id_mapping_index on wiki_mapping(id)");
        conn.createStatement().execute("CREATE INDEX IF NOT EXISTS id_coords_idx on wiki_coords(id)");
        conn.createStatement().execute("CREATE INDEX IF NOT EXISTS id_region_idx on wiki_region(id)");
        conn.createStatement().execute("CREATE INDEX IF NOT EXISTS reg_region_idx on wiki_region(regionName)");

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
            } else if (name.equals("format")) {
                format.setLength(0);
                ctext = format;
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
			} else if (name.equals("format")) {
				ctext = null;
			} else if (name.equals("text")) {
				if (!format.toString().equals("application/json")) {
					return;
				}
				try {
					ArticleMapper.Article article = gson.fromJson(ctext.toString(), ArticleMapper.Article.class);
					if (article.getLinks() != null && article.getLat() != 0 && article.getLon() != 0) {
						if (++count % ARTICLE_BATCH_SIZE == 0) {
							log.info(String.format("Article accepted %s (%d)", title.toString(), count));
						}
						long id = Long.parseLong(title.toString().substring(1));
						coordsPrep.setLong(1, id);
						coordsPrep.setString(2, title.toString());
						coordsPrep.setDouble(3, article.getLat());
						coordsPrep.setDouble(4, article.getLon());
						addBatch(coordsPrep, coordsBatch);
						List<String> rgs = regions.getRegionsToDownload(article.getLat(), article.getLon(), keyNames);
						for (String reg : rgs) {
							wikiRegionPrep.setLong(1, id);
							wikiRegionPrep.setString(2, reg);
							addBatch(wikiRegionPrep, regionBatch);
						}
						for (Map.Entry<String, JsonElement> entry : article.getLinks().entrySet()) {
				            String lang = entry.getKey().replace("wiki", "");
                            if (lang.equals("commons")) {
                                continue;
                            }
				            String articleV = entry.getValue().getAsJsonObject().getAsJsonPrimitive("title").getAsString();
				            mappingPrep.setLong(1, id);
				            mappingPrep.setString(2, lang);
				            mappingPrep.setString(3, articleV);
				            addBatch(mappingPrep, mappingBatch);
				        }
					}
				} catch (Exception e) {
					// Generally means that the field is missing in the json or the incorrect data is supplied
					errorCount++;
					if(errorCount == ERROR_BATCH_SIZE) {
						log.error(e.getMessage(), e);
					}
					if(errorCount % ERROR_BATCH_SIZE == 0) {
						log.error(String.format("Error pages %s (total %d)", title.toString(), errorCount));
					}
				}
			}
		}
	}

    
    
}

