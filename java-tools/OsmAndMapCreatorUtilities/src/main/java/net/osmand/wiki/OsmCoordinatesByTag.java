package net.osmand.wiki;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.zip.GZIPInputStream;

import net.osmand.data.LatLon;
import org.apache.commons.logging.Log;
import org.xmlpull.v1.XmlPullParserException;

import com.google.gson.Gson;

import net.osmand.PlatformUtil;
import net.osmand.impl.ConsoleProgressImplementation;
import net.osmand.obf.preparation.DBDialect;
import net.osmand.obf.preparation.OsmDbAccessor;
import net.osmand.obf.preparation.OsmDbAccessor.OsmDbVisitor;
import net.osmand.obf.preparation.OsmDbAccessorContext;
import net.osmand.obf.preparation.OsmDbCreator;
import net.osmand.osm.edit.Entity;
import net.osmand.osm.edit.Entity.EntityId;
import net.osmand.osm.edit.Entity.EntityType;
import net.osmand.osm.edit.Node;
import net.osmand.osm.edit.OsmMapUtils;
import net.osmand.osm.edit.Relation;
import net.osmand.osm.edit.Way;
import net.osmand.osm.io.IOsmStorageFilter;
import net.osmand.osm.io.OsmBaseStorage;
import net.osmand.osm.io.OsmBaseStoragePbf;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;

import static net.osmand.wiki.WikiDatabasePreparation.OSM_WIKI_FILE_PREFIX;

public class OsmCoordinatesByTag {

	DBDialect osmDBdialect = DBDialect.SQLITE;
	private static final Log log = PlatformUtil.getLog(OsmCoordinatesByTag.class);
	private final Set<String> filterExactTags;
	private final String[] filterStartsWithTags;
	private final Map<String, OsmLatLonId> coordinates = new HashMap<>();
	private int registeredNodes = 0;
	private int registeredWays = 0;
	private int registeredRelations = 0;
	private PreparedStatement selectCoordsByID;
	private Connection commonsWikiConn;
	private Gson gson;

	public static class OsmLatLonId {
		public double lat;
		public double lon;
		public long id; // 
		public int type; // 0 - node
		public long wikidataId;
		public String tags;
		public String tagsJson;
		public OsmLatLonId next;
	}
	
	public OsmCoordinatesByTag(String[] filterExactTags, String[] filterStartsWithTags) {
		gson = new Gson();
		this.filterExactTags = new TreeSet<>(Arrays.asList(filterExactTags));
		this.filterStartsWithTags = filterStartsWithTags;
	}
	
	public OsmCoordinatesByTag parse(File wikidataSqlite, boolean initFromOsm) throws SQLException {
		if (!wikidataSqlite.exists() || initFromOsm) {
			initCoordinates(wikidataSqlite.getParentFile());
		} else {
			commonsWikiConn = DBDialect.SQLITE.getDatabaseConnection(wikidataSqlite.getAbsolutePath(), log);
			selectCoordsByID = commonsWikiConn.prepareStatement("SELECT lat, lon FROM wiki_coords where originalId = ?");
		}
		return this;
	}

	private void initCoordinates(File wikiFolder) {
		File[] listFiles = wikiFolder.listFiles();
		if (listFiles != null) {
			for (File f : listFiles) {
				if (f.getName().startsWith(OSM_WIKI_FILE_PREFIX) && !f.getName().endsWith(".db")) {
					boolean parseRelations = f.getName().contains("multi");
					try {
						parseOSMCoordinates(f, null, parseRelations);
					} catch (IOException | SQLException | XmlPullParserException | InterruptedException e) {
						throw new IllegalArgumentException("Error parsing " + f.getName() + " file", e);
					}
				}
			}
		} else {
			log.error("osm_wiki_*.gz files is absent");
		}
	}

	public static void main(String[] args) throws IOException, SQLException, XmlPullParserException, InterruptedException {
		File osmGz = new File("/Users/victorshcherb/Desktop/osm_wiki_waynodes.osm.gz");
//		File osmGz = new File("/Users/victorshcherb/Desktop/osm_wiki_buildings_multipolygon.osm.gz");
		OsmCoordinatesByTag o = new OsmCoordinatesByTag(new String[]{"wikipedia", "wikidata"},
				new String[] { "wikipedia:" }).parse(osmGz.getParentFile(), false);
	}
	
	
	public void createOSMWikidataTable(File wikidataSqlite) throws SQLException {
		commonsWikiConn = DBDialect.SQLITE.getDatabaseConnection(wikidataSqlite.getAbsolutePath(), log);
		Statement st = commonsWikiConn.createStatement();
		ResultSet rs = st.executeQuery("SELECT id,lang,title FROM wiki_mapping ");
		Map<Long, OsmLatLonId> res = new TreeMap<>();
		while(rs.next()) {
			long wid = rs.getLong(1);
			String articleLang = rs.getString(2);
			String articleTitle = rs.getString(3);
			setWikidataId(res, getCoordinates("wikipedia:" + articleLang, articleTitle), wid);
			setWikidataId(res, getCoordinates("wikipedia" + articleLang, articleLang + ":" + articleTitle), wid);
			setWikidataId(res, getCoordinates("wikipedia", articleTitle), wid);
			setWikidataId(res, getCoordinates("wikidata", "Q" + wid), wid);
		}
		try {
			commonsWikiConn.createStatement().executeQuery("DROP TABLE osm_wikidata");
		} catch (SQLException e) {
			// ignore
			System.err.println("Table osm_wikidata doesn't exist");
		}
		st.execute("CREATE TABLE osm_wikidata(osmid long, osmtype int, wikidataid long,  lat double, long double, tags string, tag string)");
		st.close();
		PreparedStatement ps = commonsWikiConn.prepareStatement("INSERT INTO osm_wikidata VALUES(?, ?, ?, ?, ?, ?, ?)");
		int batch = 0;
		for (OsmLatLonId o : res.values()) {
			ps.setLong(1, o.id);
			ps.setInt(2, o.type);
			ps.setLong(3, o.wikidataId);
			ps.setDouble(4, o.lat);
			ps.setDouble(5, o.lon);
			ps.setString(6, o.tagsJson);
			ps.setString(7, o.tags);
			ps.addBatch();
			if (batch++ >= 1000) {
				ps.executeBatch();
			}
		}
		ps.executeBatch();
		ps.close();
	}
	
	private void setWikidataId(Map<Long, OsmLatLonId> mp, OsmLatLonId c, long wid) {
		if (c != null) {
			c.wikidataId = wid;
			setWikidataId(mp, c.next, wid);
			mp.put(c.type + (c.id << 2), c);
		}
	}
	
	private static String combineTagValue(String tag, String value) {
		return tag + "___" + value;
	}
	
	public OsmLatLonId getCoordinates(String tag, String value) {
		return coordinates.get(combineTagValue(tag, value));
	}

	public LatLon getCoordinatesFromCommonsWikiDB(String wikidataQId) throws SQLException {
		if (selectCoordsByID != null) {
			selectCoordsByID.setString(1, wikidataQId);
			ResultSet rs = selectCoordsByID.executeQuery();
			if (rs.next()) {
				LatLon l = new LatLon(rs.getDouble(1), rs.getDouble(2));
				if (l.getLatitude() != 0 || l.getLongitude() != 0) {
					return l;
				}
			}
		}
		return null;
	}

	private boolean checkIfTagsSuitable(Entity entity) {
		for (String t : entity.getTagKeySet()) {
			if (checkTagSuitable(t)) {
				return true;
			}
		}
		return false;
	}

	private boolean checkTagSuitable(String tag) {
		if (filterExactTags.contains(tag)) {
			return true;
		}
		for (String fl : filterStartsWithTags) {
			if (tag.startsWith(fl)) {
				return true;
			}
		}
		return false;
	}

	private void registerEntity(Entity entity) {
		LatLon center = OsmMapUtils.getCenter(entity);
		if(center == null) {
			return;
		}
		for (String t : entity.getTagKeySet()) {
			if (checkTagSuitable(t)) {
				if (entity instanceof Node) {
					registeredNodes++;
				} else if (entity instanceof Way) {
					registeredWays++;
				} else if (entity instanceof Relation) {
					registeredRelations++;
				}
				String key = combineTagValue(t, entity.getTag(t));
				OsmLatLonId osmLatLonId = new OsmLatLonId();
				osmLatLonId.lat = center.getLatitude();
				osmLatLonId.lon = center.getLongitude();
				osmLatLonId.id = entity.getId();
				osmLatLonId.type = EntityType.valueOf(entity).ordinal();
				osmLatLonId.tagsJson = gson.toJson(entity.getTags());
				if (entity.getTag("place") != null || entity.getTag("admin_level") != null) {
					osmLatLonId.tags = "administrative";
				} else if (entity.getTag("office") != null) {
					osmLatLonId.tags = "office";
				} else {
					String[] lst = { "tourism", "amenity", "historic", "natural", "aeroway", "waterway" };
					for (String l : lst) {
						if (entity.getTag(l) != null) {
							osmLatLonId.tags = entity.getTag(l);
							break;
						}
					}
				}
				OsmLatLonId oldValue = coordinates.put(key, osmLatLonId);
				osmLatLonId.next = oldValue;
//				System.out.println(OsmMapUtils.getCenter(entity) + " " + entity.getTags());
			}
		}

	}

	public void parseOSMCoordinates(File readFile, ConsoleProgressImplementation progress, boolean parseRelations) throws IOException, SQLException, XmlPullParserException, InterruptedException {
		if (progress == null) {
			progress = new ConsoleProgressImplementation();
		}
		File dbFile = new File(readFile.getParentFile(), readFile.getName() + ".db");
		OsmDbAccessor accessor = new OsmDbAccessor();
		boolean[] hasRelations = new boolean[] {false};
		progress.setGeneralProgress("Start reading " + readFile.getName() + "... " );
		
		try {
			if (osmDBdialect.databaseFileExists(dbFile)) {
				osmDBdialect.removeDatabase(dbFile);
			}
			boolean pbfFile = false;
			InputStream stream = new BufferedInputStream(new FileInputStream(readFile), 8192 * 4);
			InputStream streamFile = stream;
			if (readFile.getName().endsWith(".bz2")) { //$NON-NLS-1$
				stream = new BZip2CompressorInputStream(stream);
			} else if (readFile.getName().endsWith(".gz")) { //$NON-NLS-1$
				stream = new GZIPInputStream(stream);
			} else if (readFile.getName().endsWith(".pbf")) { //$NON-NLS-1$
				pbfFile = true;
			}
			OsmBaseStorage storage = pbfFile ? new OsmBaseStoragePbf() : new OsmBaseStorage();
			
			storage.getFilters().add(new IOsmStorageFilter() {

				@Override
				public boolean acceptEntityToLoad(OsmBaseStorage storage, EntityId entityId, Entity entity) {
					if (entityId.getType() == EntityType.NODE) {
						if (checkIfTagsSuitable(entity)) {
							registerEntity(entity);
						}
					}
					if (checkIfTagsSuitable(entity)) {
						return true;
					}
					if (entityId.getType() == EntityType.RELATION) {
						hasRelations[0] = true;
					}
					if (parseRelations) {
						return true;
					}
					return entityId.getType() == EntityType.NODE;
				}
			});

			Connection dbConn = (Connection) osmDBdialect.getDatabaseConnection(dbFile.getAbsolutePath(), log);
			accessor.setDbConn(dbConn, osmDBdialect);
			OsmDbCreator dbCreator = new OsmDbCreator(false);
			dbCreator.initDatabase(osmDBdialect, dbConn, true, null);
			storage.getFilters().add(dbCreator);
			progress.startTask("Reading osm file " + readFile.getName(), -1);
			if (pbfFile) {
				((OsmBaseStoragePbf) storage).parseOSMPbf(stream, progress, false);
			} else {
				storage.parseOSM(stream, progress, streamFile, false);
			}
			dbCreator.finishLoading();
			osmDBdialect.commitDatabase(accessor.getDbConn());
			accessor.initDatabase();
			if (!hasRelations[0] || parseRelations) {
				progress.startTask("Iterate over ways", -1);
				accessor.iterateOverEntities(progress, EntityType.WAY, new OsmDbVisitor() {

					@Override
					public void iterateEntity(Entity e, OsmDbAccessorContext ctx) throws SQLException {
						ctx.loadEntityWay((Way) e);
						if (checkIfTagsSuitable(e)) {
							registerEntity(e);
						}
					}
				});
				progress.finishTask();
				if (parseRelations) {
					progress.startTask("Iterate over relations", -1);
					accessor.iterateOverEntities(progress, EntityType.RELATION, new OsmDbVisitor() {

						@Override
						public void iterateEntity(Entity e, OsmDbAccessorContext ctx) throws SQLException {
							ctx.loadEntityRelation((Relation) e);
							if (checkIfTagsSuitable(e)) {
								registerEntity(e);
							}
						}
					});
					progress.finishTask();
				}
			}

		} finally {
			if (accessor.getDbConn() != null) {
				osmDBdialect.closeDatabase(accessor.getDbConn());
			}
			osmDBdialect.removeDatabase(dbFile);
		}
		if (hasRelations[0] && !parseRelations) {
			// parse once again
			parseOSMCoordinates(readFile, progress, true);
		}
		System.out.printf("Total %d registered (%d nodes, %d ways, %d relations)%n", coordinates.size(),
				registeredNodes, registeredWays, registeredRelations);
		printMemoryConsumption("");
	}
	
	private static void printMemoryConsumption(String string) {
		Runtime runtime = Runtime.getRuntime();
		runtime.runFinalization();
		runtime.gc();
		Thread.yield();
		long usedMem1 = runtime.totalMemory() - runtime.freeMemory();
		float mb = (1 << 20);
		log.warn(string + usedMem1 / mb);
	}

	public void closeConnection() throws SQLException {
		if (commonsWikiConn != null) {
			commonsWikiConn.close();
		}
	}
}
