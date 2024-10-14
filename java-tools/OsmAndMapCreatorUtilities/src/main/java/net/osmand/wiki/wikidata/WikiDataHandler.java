package net.osmand.wiki.wikidata;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.SAXParser;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.logging.Log;
import org.xml.sax.Attributes;
import org.xml.sax.helpers.DefaultHandler;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import net.osmand.PlatformUtil;
import net.osmand.impl.FileProgressImplementation;
import net.osmand.map.OsmandRegions;
import net.osmand.obf.preparation.DBDialect;
import net.osmand.wiki.OsmCoordinatesByTag;
import net.osmand.wiki.OsmCoordinatesByTag.OsmLatLonId;

public class WikiDataHandler extends DefaultHandler {

	private static final Log log = PlatformUtil.getLog(WikiDataHandler.class);

	private final SAXParser saxParser;
	private boolean page = false;
	private StringBuilder ctext = null;

	private final StringBuilder title = new StringBuilder();
	private final StringBuilder text = new StringBuilder();
	private final StringBuilder format = new StringBuilder();

	private FileProgressImplementation progress;
	private Connection conn;
	private PreparedStatement coordsPrep;
	private PreparedStatement mappingPrep;
	private PreparedStatement wikiRegionPrep;
	private PreparedStatement wikidataPropPrep;
	private int[] mappingBatch = new int[]{0};
	private int[] coordsBatch = new int[]{0};
	private int[] regionBatch = new int[]{0};
	private int[] wikidataPropBatch = new int[]{0};

	public final static int BATCH_SIZE = 5000;
	private final static int ARTICLE_BATCH_SIZE = 10000;
	private static final int ERROR_BATCH_SIZE = 200;

	private int count = 0;
	private int errorCount = 0;

	private Gson gson;

	private OsmandRegions regions;
	private List<String> keyNames = new ArrayList<>();

	OsmCoordinatesByTag osmWikiCoordinates;
	private long lastProcessedId;


	public WikiDataHandler(SAXParser saxParser, FileProgressImplementation progress, File wikidataSqlite,
	                       OsmCoordinatesByTag osmWikiCoordinates, OsmandRegions regions, long lastProcessedId)
			throws SQLException {
		this.saxParser = saxParser;
		this.osmWikiCoordinates = osmWikiCoordinates;
		this.regions = regions;
		this.progress = progress;
		this.lastProcessedId = lastProcessedId;
		DBDialect dialect = DBDialect.SQLITE;
		conn = dialect.getDatabaseConnection(wikidataSqlite.getAbsolutePath(), log);
		conn.createStatement().execute("CREATE TABLE IF NOT EXISTS wiki_coords(id bigint, originalId text, lat double, lon double, wlat double, wlon double,  "
				+ " osmtype int, osmid bigint, poitype text, poisubtype text)");
		conn.createStatement().execute("CREATE TABLE IF NOT EXISTS wiki_mapping(id bigint, lang text, title text)");
		conn.createStatement().execute("CREATE TABLE IF NOT EXISTS wiki_region(id bigint, regionName text)");
		conn.createStatement().execute("CREATE TABLE IF NOT EXISTS wikidata_properties(id bigint, type text, value text)");
		coordsPrep = conn.prepareStatement("INSERT INTO wiki_coords(id, originalId, lat, lon, wlat, wlon, osmtype, osmid, poitype, poisubtype) "
				+ " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
		mappingPrep = conn.prepareStatement("INSERT INTO wiki_mapping(id, lang, title) VALUES (?, ?, ?)");
		wikiRegionPrep = conn.prepareStatement("INSERT OR IGNORE INTO wiki_region(id, regionName) VALUES(?, ? )");
		wikidataPropPrep = conn.prepareStatement("INSERT INTO wikidata_properties(id, type, value) VALUES(?, ?, ?)");
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
        conn.createStatement().execute("CREATE INDEX IF NOT EXISTS id_coords_originalId on wiki_coords(originalId)");
        conn.createStatement().execute("CREATE INDEX IF NOT EXISTS id_region_idx on wiki_region(id)");
        conn.createStatement().execute("CREATE INDEX IF NOT EXISTS reg_region_idx on wiki_region(regionName)");
	    conn.createStatement().execute("CREATE UNIQUE INDEX IF NOT EXISTS unique_region_idx on wiki_region(id, regionName)");
		conn.createStatement().execute("CREATE INDEX IF NOT EXISTS wikidata_properties_idx on wikidata_properties(id)");

        coordsPrep.executeBatch();
        mappingPrep.executeBatch();
        if (!conn.getAutoCommit()) {
            conn.commit();
        }
        mappingPrep.close();
        coordsPrep.close();
        conn.close();
    }

    public void setLastProcessedId(Long lastProcessedId) {
		this.lastProcessedId = lastProcessedId;
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
	public void endElement(String uri, String localName, String qName) {
		String name = saxParser.isNamespaceAware() ? localName : qName;
		if (page) {
			switch (name) {
				case "page":
					page = false;
					progress.update();
					break;
				case "title":
				case "format":
					ctext = null;
					break;
				case "text":
					if (!format.toString().equals("application/json")) {
						return;
					}
					try {
						String t = title.toString();
						if (t.startsWith("Lexeme:")) {
							return;
						}
						long id = Long.parseLong(title.substring(1));
						if (id < lastProcessedId) {
							return;
						}
						processJsonPage(id, ctext.toString());
					} catch (Exception e) {
						// Generally means that the field is missing in the json or the incorrect data is supplied
						errorCount++;
						if (errorCount == ERROR_BATCH_SIZE) {
							log.error(e.getMessage(), e);
						}
						if (errorCount % ERROR_BATCH_SIZE == 0) {
							log.error(String.format("Error pages %s (total %d)", title, errorCount));
						}
					}
					break;
			}
		}
	}



	public void processJsonPage(long id, String json) throws SQLException, IOException {
		ArticleMapper.Article article = gson.fromJson(json, ArticleMapper.Article.class);
		OsmLatLonId osmCoordinates = null;
		double wlat = article.getLat();
		double wlon = article.getLon();
		osmCoordinates = getOsmCoordinates(id, article, osmCoordinates);
		if (osmCoordinates != null) {
			article.setLat(osmCoordinates.lat);
			article.setLon(osmCoordinates.lon);
		}

		if (article.getLat() != 0 || article.getLon() != 0) {
			if (++count % ARTICLE_BATCH_SIZE == 0) {
				log.info(String.format("Article accepted %s (%d)", title, count));
			}
			int ind = 0;
			coordsPrep.setLong(++ind, id);
			coordsPrep.setString(++ind, title.toString());
			coordsPrep.setDouble(++ind, article.getLat());
			coordsPrep.setDouble(++ind, article.getLon());
			coordsPrep.setDouble(++ind, wlat);
			coordsPrep.setDouble(++ind, wlon);
			coordsPrep.setInt(++ind, osmCoordinates != null ? (osmCoordinates.type + 1) : 0);
			coordsPrep.setLong(++ind, osmCoordinates != null ? osmCoordinates.id : 0);
			coordsPrep.setString(++ind, osmCoordinates != null &&  osmCoordinates.amenity != null ? osmCoordinates.amenity.getType().getKeyName(): null);
			coordsPrep.setString(++ind, osmCoordinates != null &&  osmCoordinates.amenity != null ? osmCoordinates.amenity.getSubType() : null );

			addBatch(coordsPrep, coordsBatch);
			List<String> rgs = regions.getRegionsToDownload(article.getLat(), article.getLon(), keyNames);
			for (String reg : rgs) {
				wikiRegionPrep.setLong(1, id);
				wikiRegionPrep.setString(2, reg);
				addBatch(wikiRegionPrep, regionBatch);
			}
			for (ArticleMapper.SiteLink siteLink : article.getSiteLinks()) {
				mappingPrep.setLong(1, id);
				mappingPrep.setString(2, siteLink.lang);
				mappingPrep.setString(3, siteLink.title);
				addBatch(mappingPrep, mappingBatch);
			}
			if (article.getImage() != null) {
				String image = StringEscapeUtils.unescapeJava(article.getImage());
				wikidataPropPrep.setLong(1, id);
				wikidataPropPrep.setString(2, article.getImageProp());
				wikidataPropPrep.setString(3, image);
				addBatch(wikidataPropPrep, wikidataPropBatch);
			}
			if (article.getCommonCat() != null) {
				String commonCat = StringEscapeUtils.unescapeJava(article.getCommonCat());
				wikidataPropPrep.setLong(1, id);
				wikidataPropPrep.setString(2, ArticleMapper.PROP_COMMON_CAT);
				wikidataPropPrep.setString(3, commonCat);
				addBatch(wikidataPropPrep, wikidataPropBatch);
			}
		}
	}



	private OsmLatLonId getOsmCoordinates(long wid, ArticleMapper.Article article, OsmLatLonId osmCoordinates) {
		for (ArticleMapper.SiteLink siteLink : article.getSiteLinks()) {
			String articleTitle = siteLink.title;
			String articleLang = siteLink.lang;
			osmCoordinates = osmWikiCoordinates.getCoordinates("wikipedia:" + articleLang, articleTitle);
			if (osmCoordinates == null) {
				osmCoordinates = osmWikiCoordinates.getCoordinates("wikipedia", articleLang + ":" + articleTitle);
			}
			if (osmCoordinates == null) {
				osmCoordinates = osmWikiCoordinates.getCoordinates("wikipedia", articleTitle);
			}
			if (osmCoordinates != null) {
				return osmCoordinates;
			}
		}
		return osmWikiCoordinates.getCoordinates("wikidata", "Q"+wid);
	}
}

