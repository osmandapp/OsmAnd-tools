package net.osmand.wiki;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.zip.GZIPInputStream;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.logging.Log;
import org.xmlpull.v1.XmlPullParserException;

import net.osmand.PlatformUtil;
import net.osmand.data.LatLon;
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

public class OsmCoordinatesByTag {

	DBDialect osmDBdialect = DBDialect.SQLITE;
	private static final Log log = PlatformUtil.getLog(OsmCoordinatesByTag.class);
	private final Set<String> filterExactTags;
	private final String[] filterStartsWithTags;
	
	Map<String, LatLon> coordinates = new HashMap<String, LatLon>();  
	int registeredNodes = 0;
	int registeredWays = 0;
	int registeredRelations = 0;
	
	public OsmCoordinatesByTag(String[] filterExactTags, String[] filterStartsWithTags) {
		this.filterExactTags = new TreeSet<>(Arrays.asList(filterExactTags)) ;
		this.filterStartsWithTags = filterStartsWithTags;
	}
	
	public static void main(String[] args) throws IOException, SQLException, XmlPullParserException, InterruptedException {
		File osmGz = new File("/Users/victorshcherb/Desktop/osm_wiki_waynodes.osm.gz");
//		File osmGz = new File("/Users/victorshcherb/Desktop/osm_wiki_buildings_multipolygon.osm.gz");
		ConsoleProgressImplementation progress = new ConsoleProgressImplementation();
		OsmCoordinatesByTag o = new OsmCoordinatesByTag(new String[] { "wikipedia", "wikidata" },
				new String[] { "wikipedia:" });
		o.parseWikiOSMCoordinates(osmGz, progress, false);
		
	}
	
	private static String combineTagValue(String tag, String value) {
		return tag + "___" + value;
	}
	
	public LatLon getCoordinates(String tag, String value) {
		return coordinates.get(combineTagValue(tag, value));
	}
	
	private boolean checkIfTagsSuitable(Entity entity) {
		for (String t : entity.getTagKeySet()) {
			if(checkTagSuitable(t)) {
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
		for (String t : entity.getTagKeySet()) {
			if (checkTagSuitable(t)) {
				if (entity instanceof Node) {
					registeredNodes++;
				} else if (entity instanceof Way) {
					registeredWays++;
				} else if (entity instanceof Relation) {
					registeredRelations++;
				}
				coordinates.put(combineTagValue(t, entity.getTag(t)), center);
//				System.out.println(OsmMapUtils.getCenter(entity) + " " + entity.getTags());
			}
		}

	}

	public void parseWikiOSMCoordinates(File readFile, ConsoleProgressImplementation progress, boolean parseRelations) throws IOException, SQLException, XmlPullParserException, InterruptedException {
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
			OsmDbCreator dbCreator = new OsmDbCreator(0, 0, false);
			dbCreator.initDatabase(osmDBdialect, dbConn, true);
			storage.getFilters().add(dbCreator);
			progress.startTask("Reading osm file " + readFile.getName(), -1);
			if (pbfFile) {
				((OsmBaseStoragePbf) storage).parseOSMPbf(stream, progress, false);
			} else {
				storage.parseOSM(stream, progress, streamFile, false);
			}
			dbCreator.finishLoading();
			osmDBdialect.commitDatabase(accessor.getDbConn());
			accessor.initDatabase(null);
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

				osmDBdialect.closeDatabase(accessor.getDbConn());
				osmDBdialect.removeDatabase(dbFile);

			}

		} finally {
			osmDBdialect.closeDatabase(accessor.getDbConn());
			osmDBdialect.removeDatabase(dbFile);
		}
		if (hasRelations[0] && !parseRelations) {
			// parse once again
			parseWikiOSMCoordinates(readFile, progress, true);
		}
		System.out.println(String.format("Total %d registered (%d nodes, %d ways, %d relations)", coordinates.size(), 
				registeredNodes, registeredWays, registeredRelations));
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
}
