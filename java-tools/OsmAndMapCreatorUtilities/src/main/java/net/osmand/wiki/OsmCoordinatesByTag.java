package net.osmand.wiki;

import static net.osmand.wiki.WikiDatabasePreparation.OSM_WIKI_FILE_PREFIX;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.zip.GZIPInputStream;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.logging.Log;
import org.xmlpull.v1.XmlPullParserException;

import com.google.gson.Gson;

import net.osmand.PlatformUtil;
import net.osmand.data.Amenity;
import net.osmand.data.LatLon;
import net.osmand.impl.ConsoleProgressImplementation;
import net.osmand.map.OsmandRegions;
import net.osmand.obf.preparation.DBDialect;
import net.osmand.obf.preparation.OsmDbAccessor;
import net.osmand.obf.preparation.OsmDbAccessor.OsmDbVisitor;
import net.osmand.obf.preparation.OsmDbAccessorContext;
import net.osmand.obf.preparation.OsmDbCreator;
import net.osmand.osm.MapPoiTypes;
import net.osmand.osm.MapRenderingTypesEncoder;
import net.osmand.osm.MapRenderingTypesEncoder.EntityConvertApplyType;
import net.osmand.osm.edit.Entity;
import net.osmand.osm.edit.Entity.EntityId;
import net.osmand.osm.edit.Entity.EntityType;
import net.osmand.osm.edit.EntityParser;
import net.osmand.osm.edit.Node;
import net.osmand.osm.edit.OsmMapUtils;
import net.osmand.osm.edit.Relation;
import net.osmand.osm.edit.Way;
import net.osmand.osm.io.IOsmStorageFilter;
import net.osmand.osm.io.OsmBaseStorage;
import net.osmand.osm.io.OsmBaseStoragePbf;
import net.osmand.util.Algorithms;
import net.osmand.wiki.wikidata.WikiDataHandler;

public class OsmCoordinatesByTag {

	DBDialect osmDBdialect = DBDialect.SQLITE;
	private static final Log log = PlatformUtil.getLog(OsmCoordinatesByTag.class);
	private final Set<String> filterExactTags;
	private final String[] filterStartsWithTags;
	private final Map<String, OsmLatLonId> coordinates = new HashMap<>();
	private int registeredNodes = 0;
	private int registeredWays = 0;
	private int registeredRelations = 0;
	private Gson gson;
	private MapRenderingTypesEncoder renderingTypes;
	private MapPoiTypes poiTypes;

	public static class OsmLatLonId {
		public double lat;
		public double lon;
		public long id; // 
		public int type; // 0 - node
		public long wikidataId;
		public String wikiCommonsImg;
		public String wikiCommonsCat;
		public String tagsJson;
		public OsmLatLonId next;
		public Amenity amenity;
		
		public String toString(String key) {
			OsmLatLonId ol = this;
			return String.format("osmid=%d %d wikidataid=%d key=%s amenity(%s)",
					ol.type, ol.id, ol.wikidataId, key, ol.amenity);
		}
		
		@Override
		public String toString() {
			return toString("");
		}
	}
	
	public OsmCoordinatesByTag(String[] filterExactTags, String[] filterStartsWithTags) {
		gson = new Gson();
		this.filterExactTags = new TreeSet<>(Arrays.asList(filterExactTags));
		this.filterStartsWithTags = filterStartsWithTags;
		renderingTypes = new MapRenderingTypesEncoder("basemap");
		poiTypes = MapPoiTypes.getDefault();
		
	}
	
	public OsmCoordinatesByTag parse(File folderWithSql) throws SQLException {
		File[] listFiles = folderWithSql.listFiles();
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
		return this;
	}

	public static void main(String[] args) throws IOException, SQLException, XmlPullParserException, InterruptedException {
		File osmGz = new File("/Users/victorshcherb/Desktop/");
		OsmCoordinatesByTag otag = new OsmCoordinatesByTag(new String[]{"wikipedia", "wikidata"},
				new String[] { "wikipedia:" }).parse(osmGz);
		Iterator<Entry<String, OsmLatLonId>> it = otag.coordinates.entrySet().iterator();
		while(it.hasNext()) {
			Entry<String, OsmLatLonId> e = it.next();
			System.out.println(e.getValue().toString(e.getKey()) + " " + e.getValue().lat + " " + e.getValue().lon);
		}
		File wikidataDb = new File(osmGz, "wikidata_osm.sqlitedb");
		OsmandRegions or = new OsmandRegions();
		or.prepareFile();
		WikiDataHandler wdh = new WikiDataHandler(null, null, wikidataDb, otag, or, 0);
		long testwid = 2051638;
		StringBuilder sb;
		if (testwid > 0) {
			URL url = new URL("https://www.wikidata.org/w/api.php?action=wbgetentities&ids=Q2051638&format=json&props=labels");
			sb = Algorithms.readFromInputStream(url.openStream());
		} else {
			sb = Algorithms.readFromInputStream(OsmCoordinatesByTag.class.getResourceAsStream("/Q"+testwid+".json"));
		}
		wdh.processJsonPage(testwid, sb.toString());
		wdh.finish();
		WikiDatabasePreparation.createOSMWikidataTable(wikidataDb, otag);

	}
	
	
	
	private static String combineTagValue(String tag, String value) {
		return tag + "___" + value;
	}
	
	public OsmLatLonId getCoordinates(String tag, String value) {
		return coordinates.get(combineTagValue(tag, value));
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
		List<Amenity> alist = new ArrayList<>(); 
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
				Map<String, String> etags = renderingTypes.transformTags(entity.getTags(), EntityType.valueOf(entity), EntityConvertApplyType.POI);
				Map<String, String> wikiCommonsTags = getWikiCommonsTag(etags);
				osmLatLonId.wikiCommonsImg = wikiCommonsTags.get("img");
				osmLatLonId.wikiCommonsCat = wikiCommonsTags.get("cat");
				osmLatLonId.tagsJson = gson.toJson(etags);
				alist.clear();
				alist = EntityParser.parseAmenities(poiTypes, entity, etags, alist);
				if (alist.size() > 0) {
					osmLatLonId.amenity = alist.get(0);
				}
				OsmLatLonId oldValue = coordinates.put(key, osmLatLonId);
				osmLatLonId.next = oldValue;
//				System.out.println(OsmMapUtils.getCenter(entity) + " " + entity.getTags());
			}
		}

	}

	private Map<String, String> getWikiCommonsTag(Map<String, String> etags) {
		Map<String, String> tags = new HashMap<>();
		String wikiCommonsTag = etags.get("wikimedia_commons");
		if (wikiCommonsTag == null) {
			return Collections.emptyMap();
		}
		if (wikiCommonsTag.startsWith("File:")) {
			tags.put("img", wikiCommonsTag.substring("File:".length()));
		}
		if (wikiCommonsTag.startsWith("Category:")) {
			tags.put("cat", wikiCommonsTag.substring("Category:".length()));
		}
		return tags;
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
			OsmDbCreator dbCreator = new OsmDbCreator(true);
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
	}
}
