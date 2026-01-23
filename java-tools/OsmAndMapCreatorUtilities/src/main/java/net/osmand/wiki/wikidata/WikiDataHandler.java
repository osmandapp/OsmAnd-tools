package net.osmand.wiki.wikidata;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import javax.xml.parsers.SAXParser;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.logging.Log;
import org.xml.sax.Attributes;
import org.xml.sax.helpers.DefaultHandler;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.SerializedName;
import com.google.gson.reflect.TypeToken;

import net.osmand.PlatformUtil;
import net.osmand.impl.FileProgressImplementation;
import net.osmand.map.OsmandRegions;
import net.osmand.obf.preparation.DBDialect;
import net.osmand.util.Algorithms;
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
	private PreparedStatement wikidataBlobPrep;
	private final PreparedStatement deleteWikidataPropPrep;
	private final PreparedStatement deleteMappingPrep;
	private final PreparedStatement deleteWikiRegionPrep;
	private final PreparedStatement deleteWikidataBlobPrep;
	private int[] mappingBatch = new int[]{0};
	private int[] coordsBatch = new int[]{0};
	private int[] regionBatch = new int[]{0};
	private int[] wikidataPropBatch = new int[]{0};
	private int[] wikidataBlobBatch = new int[]{0};

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
	private final boolean update;
	
	private long limit = -1;

	private Map<Long, String> allAstroWids;


	public WikiDataHandler(SAXParser saxParser, FileProgressImplementation progress, File wikidataSqlite,
						   OsmCoordinatesByTag osmWikiCoordinates, OsmandRegions regions, long lastProcessedId, boolean update)
			throws SQLException {
		this.saxParser = saxParser;
		this.osmWikiCoordinates = osmWikiCoordinates;
		this.regions = regions;
		this.progress = progress;
		this.lastProcessedId = lastProcessedId;
		this.update = update;
		DBDialect dialect = DBDialect.SQLITE;
		conn = dialect.getDatabaseConnection(wikidataSqlite.getAbsolutePath(), log);
		conn.createStatement().execute("CREATE TABLE IF NOT EXISTS wiki_coords(id bigint PRIMARY KEY, originalId text, lat double, lon double, wlat double, wlon double,  "
				+ " osmtype int, osmid bigint, poitype text, poisubtype text, labelsJson text)");
		conn.createStatement().execute("CREATE TABLE IF NOT EXISTS wiki_mapping(id bigint, lang text, title text)");
		conn.createStatement().execute("CREATE TABLE IF NOT EXISTS wiki_region(id bigint, regionName text)");
		conn.createStatement().execute("CREATE TABLE IF NOT EXISTS wikidata_properties(id bigint, type text, value text)");
		conn.createStatement().execute("CREATE TABLE IF NOT EXISTS wikidata_blobs(id bigint PRIMARY KEY, page blob)");
		coordsPrep = conn.prepareStatement("""
				INSERT INTO wiki_coords(id, originalId, lat, lon, wlat, wlon, osmtype, osmid, poitype, poisubtype, labelsJson)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
				ON CONFLICT(id) DO UPDATE SET
				originalId = excluded.originalId,
				lat = excluded.lat,
				lon = excluded.lon,
				wlat = excluded.wlat,
				wlon = excluded.wlon,
				osmtype = excluded.osmtype,
				osmid = excluded.osmid,
				poitype = excluded.poitype,
				poisubtype = excluded.poisubtype,
				labelsJson = excluded.labelsJson;
				""");
		mappingPrep = conn.prepareStatement("INSERT INTO wiki_mapping(id, lang, title) VALUES (?, ?, ?)");
		wikiRegionPrep = conn.prepareStatement("INSERT OR IGNORE INTO wiki_region(id, regionName) VALUES(?, ? )");
		wikidataPropPrep = conn.prepareStatement("INSERT INTO wikidata_properties(id, type, value) VALUES(?, ?, ?)");
		wikidataBlobPrep = conn.prepareStatement("INSERT INTO wikidata_blobs(id, page) VALUES(?, ?)");
		deleteWikiRegionPrep = conn.prepareStatement("DELETE FROM wiki_region WHERE id = ?");
		deleteMappingPrep = conn.prepareStatement("DELETE FROM wiki_mapping WHERE id = ?");
		deleteWikidataPropPrep = conn.prepareStatement("DELETE FROM wikidata_properties WHERE id = ?");
		deleteWikidataBlobPrep = conn.prepareStatement("DELETE FROM wikidata_blobs WHERE id = ?");
		gson = new GsonBuilder().registerTypeAdapter(ArticleMapper.Article.class, new ArticleMapper()).create();
		
		allAstroWids = getAllAstroWids();
	}
	
	
	public void setLimit(long limit) {
		this.limit = limit;
		
	}
	
	
	public class AstroObject {
	    @SerializedName("wid")
	    public String wid;
	    
	    @Override
	    public String toString() {
	    	return wid;
	    }
	}
	
	
	public static Map<Long, String> getAllAstroWids() {
		Map<Long, String> wids = new HashMap<>();
        Gson gson = new Gson();
        @SuppressWarnings("unused")
		Type listType = new TypeToken<List<AstroObject>>() {}.getType();
        String path = "/astro";
        try {
        	URL url = WikiDataHandler.class.getResource(path);
            if (url == null) return wids;
            List<String> jsonPaths = new ArrayList<>();

            if (url.getProtocol().equals("jar")) {
                String jarPath = url.getPath().substring(5, url.getPath().indexOf("!"));
                try (ZipInputStream zip = new ZipInputStream(Files.newInputStream(Paths.get(jarPath)))) {
                    ZipEntry entry;
                    while ((entry = zip.getNextEntry()) != null) {
                        // Check if entry is in the 'astro' folder and is a JSON
                        if (!entry.isDirectory() && entry.getName().startsWith("astro/") && entry.getName().endsWith(".json")) {
                            jsonPaths.add("/" + entry.getName());
                        }
                    }
                }
            } else {
                // SCENARIO B: Running from IDE / File System
                try (Stream<Path> walk = Files.walk(Paths.get(url.toURI()), 1)) {
                    walk.filter(p -> p.toString().endsWith(".json"))
                        .forEach(p -> jsonPaths.add(path + "/" + p.getFileName().toString()));
                }
            }
			for (String resourcePath : jsonPaths) {
				try (InputStreamReader reader = new InputStreamReader(
						WikiDataHandler.class.getResourceAsStream(resourcePath))) {
					List<AstroObject> objects = gson.fromJson(reader, new TypeToken<List<AstroObject>>() {
					}.getType());
					if (objects != null) {
						String type = resourcePath.substring(resourcePath.lastIndexOf('/') + 1, resourcePath.lastIndexOf('.'));
						for (AstroObject obj : objects) {
							if (obj.wid != null && obj.wid.startsWith("Q")) {
								wids.put(Long.parseLong(obj.wid.substring(1)), type);
							}
						}
					}
				}
			}
        } catch (Exception e) {
            e.printStackTrace();
        }
        return wids;
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
        wikiRegionPrep.executeBatch();
    	wikidataPropPrep.executeBatch();
    	wikidataBlobPrep.executeBatch();
        if (!conn.getAutoCommit()) {
            conn.commit();
        }
        mappingPrep.close();
        coordsPrep.close();
        wikiRegionPrep.close();
    	wikidataPropPrep.close();
    	wikidataBlobPrep.close();
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
					if (limit > 0 && count >= limit) {
						throw new IllegalStateException();
					}
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
						if (t.startsWith("Lexeme:") || t.startsWith("Property:")) {
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
		String starType = allAstroWids.get(id);
		boolean storeJson = starType != null;
		ArticleMapper.Article article = gson.fromJson(json, ArticleMapper.Article.class);
		
		OsmLatLonId osmCoordinates = null;
		double wlat = article.getLat();
		double wlon = article.getLon();
		osmCoordinates = getOsmCoordinates(id, article, osmCoordinates);
		if (osmCoordinates != null) {
			article.setLat(osmCoordinates.lat);
			article.setLon(osmCoordinates.lon);
		}

		if (article.getLat() != 0 || article.getLon() != 0 || starType != null) {
			if (++count % ARTICLE_BATCH_SIZE == 0) {
				log.info(String.format("Article accepted %s (%d)", title, count));
			}
			Map<String, String> labels = article.getLabels();
			Map<String, String> merged = null;
			if (osmCoordinates != null && osmCoordinates.amenity != null) {
				Map<String, String> names = osmCoordinates.amenity.getNamesMap(true);
				if (names != null) {
					merged = new LinkedHashMap<>(names);
				}
			}
			if (labels != null) {
				if (merged == null) merged = new LinkedHashMap<>();
				merged.putAll(labels);
			}
			String labelsJson = merged != null ? gson.toJson(merged) : null;
			int ind = 0;
			coordsPrep.setLong(++ind, id);
			coordsPrep.setString(++ind, title.toString());
			coordsPrep.setDouble(++ind, article.getLat());
			coordsPrep.setDouble(++ind, article.getLon());
			coordsPrep.setDouble(++ind, wlat);
			coordsPrep.setDouble(++ind, wlon);
			coordsPrep.setInt(++ind, osmCoordinates != null ? (osmCoordinates.type + 1) : 0);
			coordsPrep.setLong(++ind, osmCoordinates != null ? osmCoordinates.id : 0);
			String type = osmCoordinates != null &&  osmCoordinates.amenity != null ? osmCoordinates.amenity.getType().getKeyName(): null;
			coordsPrep.setString(++ind, starType != null ? "starmap" : type);
			String subtype = osmCoordinates != null &&  osmCoordinates.amenity != null ? osmCoordinates.amenity.getSubType() : null;
			coordsPrep.setString(++ind,  starType != null ? starType : subtype);
			coordsPrep.setString(++ind, labelsJson);
			addBatch(coordsPrep, coordsBatch);
			if (update) {
				deleteWikiRegionPrep.setLong(1, id);
				deleteWikiRegionPrep.execute();
				deleteMappingPrep.setLong(1, id);
				deleteMappingPrep.execute();
				deleteWikidataPropPrep.setLong(1, id);
				deleteWikidataPropPrep.execute();
				deleteWikidataBlobPrep.setLong(1, id);
				deleteWikidataBlobPrep.execute();
			}
			List<String> rgs = regions.getRegionsToDownload(article.getLat(), article.getLon(), keyNames);
			for (String reg : rgs) {
				wikiRegionPrep.setLong(1, id);
				wikiRegionPrep.setString(2, reg);
				addBatch(wikiRegionPrep, regionBatch);
			}
			for (ArticleMapper.SiteLink siteLink : article.getSiteLinks()) {
				mappingPrep.setLong(1, id);
				mappingPrep.setString(2, siteLink.lang());
				mappingPrep.setString(3, siteLink.title());
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
			if (storeJson) {
				wikidataBlobPrep.setLong(1, id);
				wikidataBlobPrep.setBytes(2, Algorithms.stringToGzip(json));
				addBatch(wikidataBlobPrep, wikidataBlobBatch);
			}
		}
	}



	private OsmLatLonId getOsmCoordinates(long wid, ArticleMapper.Article article, OsmLatLonId osmCoordinates) {
		for (ArticleMapper.SiteLink siteLink : article.getSiteLinks()) {
			String articleTitle = siteLink.title();
			String articleLang = siteLink.lang();
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

