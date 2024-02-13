package net.osmand.routes;


import net.osmand.IProgress;
import net.osmand.impl.ConsoleProgressImplementation;
import net.osmand.obf.preparation.DBDialect;
import net.osmand.obf.preparation.OsmDbAccessor;
import net.osmand.obf.preparation.OsmDbAccessorContext;
import net.osmand.obf.preparation.OsmDbCreator;
import net.osmand.osm.edit.Entity;
import net.osmand.osm.edit.Entity.EntityId;
import net.osmand.osm.edit.Node;
import net.osmand.osm.edit.Relation;
import net.osmand.osm.edit.Way;
import net.osmand.osm.io.OsmBaseStorage;
import net.osmand.osm.io.OsmBaseStoragePbf;
import net.osmand.osm.io.OsmStorageWriter;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.xmlpull.v1.XmlPullParserException;

import javax.xml.stream.XMLStreamException;
import java.io.*;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class RouteRelationExtractor {
//	on 2024-02-01, the plain OSM XML variant takes over 1854.0 GB
//	Report run at 2024-02-09
//	Number of nodes	8_942_882_403
//	Number of ways	999_718_343
//	Number of relations	11_853_665

//	Africa	[.osm.pbf]	(6.2 GB)
//	Antarctica	[.osm.pbf]	(31.3 MB)
//	Asia	[.osm.pbf]	(12.6 GB)
//	Australia and Oceania	[.osm.pbf]	(1.1 GB)
//	Central America	[.osm.pbf]	(648 MB)
//	Europe	[.osm.pbf]	(28.1 GB)
//	North America	[.osm.pbf]	(13.5 GB)
//	South America	[.osm.pbf]	(3.2 GB)

	private static final Log log = LogFactory.getLog(RouteRelationExtractor.class);
	DBDialect osmDBdialect = DBDialect.SQLITE;
	private final String[] filteredTags = {
			"-bus",
			"-road",
			"hiking",
			"bicycle",
			"foot",
			"-ferry",
			"mtb",
			"-power",
			"-railway",
			"piste",
			"-train",
			"ski",
			"-tram",
			"-detour",
			"-tracks",
			"-trolleybus",
			"horse",
			"-share_taxi",
			"-subway",
			"-emergency_access",
			"snowmobile",
			"historic",
			"running",
			"fitness_trail",
			"-light_rail",
			"canoe",
			"-pipeline",
			"canyoning",
			"junction",
			"motorboat",
			"-yes",
			"waterway",
			"boat",
			"worship",
			"aerialway",
			"inline_skates",
			"transhumance",
			"historic_railway",
			"via_ferrata",
			"-monorail",
			"hiking;mtb",
			"-funicular" // count 200
			// "walking" count 164
	};

	public static void main(String[] args) {

		if (args.length == 1 && args[0].equals("test")) {
			List<String> s = new ArrayList<>();
			s.add("source.osm");
			s.add("result.osm");
			args = s.toArray(new String[0]);
		} else if (args.length < 1) {
			System.out.println("Usage: <path source.osm|.gz|.pbf file> " +
					"<path result.osm|.gz|.bz2 file> ");
			System.exit(1);
		}

		String sourceFilePath = args[0];
		String resultFilePath = args[1];

		try {
			RouteRelationExtractor rdg = new RouteRelationExtractor();
			File sourceFile = new File(sourceFilePath);
			File resultFile = new File(resultFilePath);
			rdg.extractRoutes(sourceFile, resultFile);
		} catch (SQLException | IOException | XmlPullParserException | XMLStreamException | InterruptedException e) {
			log.error("Extract routes error: ", e);
		}
	}

	private void extractRoutes(File sourceFile, File resultFile) throws IOException, XmlPullParserException,
			XMLStreamException, SQLException, InterruptedException {
		File dbFile = new File(sourceFile.getParentFile(), sourceFile.getName() + ".db");
		long startTime = System.currentTimeMillis();
		long endTime, deltaTime;
		InputStream sourceIs;
		if (sourceFile.getName().endsWith(".gz")) {
			sourceIs = new GZIPInputStream(new FileInputStream(sourceFile));
		} else {
			sourceIs = new FileInputStream(sourceFile);
		}
		boolean pbfFile = sourceFile.getName().endsWith(".pbf");
		OsmBaseStorage endStorage = new OsmBaseStorage();

		OsmBaseStorage startStorage = pbfFile ? new OsmBaseStoragePbf() : new OsmBaseStorage();

		Connection dbConn;
		if (!dbFile.exists()) {
			dbConn = osmDBdialect.getDatabaseConnection(dbFile.getAbsolutePath(), log);

			InputStream startStream = new BufferedInputStream(sourceIs, 8192 * 4);
			OsmDbCreator dbCreator = new OsmDbCreator(false);
			dbCreator.initDatabase(osmDBdialect, dbConn, true, null);

			startStorage.getFilters().add(dbCreator);
			if (pbfFile) {
				((OsmBaseStoragePbf) startStorage).parseOSMPbf(startStream, IProgress.EMPTY_PROGRESS, true);
			} else {
				startStorage.parseOSM(startStream, IProgress.EMPTY_PROGRESS);
			}
			endTime = System.currentTimeMillis();
			deltaTime = (endTime - startTime) / 1000;
			startTime = endTime;
			System.out.println("Parse " + sourceFile.getName() + ": " + deltaTime + " sec.");
			startStream.close();
		} else {
			dbConn = osmDBdialect.getDatabaseConnection(dbFile.getAbsolutePath(), log);
		}
		AccessorForRelationExtract accessor = new AccessorForRelationExtract();
		accessor.setDbConn(dbConn, osmDBdialect);
		accessor.initDatabase();
		List<Relation> resultRelations = new ArrayList<>();

		accessor.iterateOverEntities(new ConsoleProgressImplementation(1), Entity.EntityType.RELATION, new OsmDbAccessor.OsmDbVisitor() {
			@Override
			public void iterateEntity(Entity e, OsmDbAccessorContext ctx) throws SQLException {
				String route = e.getTag("route");
				if (route != null) {
					if (accessedRoute(route)) {
						ctx.loadEntityRelation((Relation) e);
						resultRelations.add((Relation) e);
					}
				}
			}

			private boolean accessedRoute(String route) {
				for (String value : filteredTags) {
					if (route.endsWith(value)) {
						return true;
					}
				}
				return false;
			}
		});
		for (Entity e : resultRelations) {
			endStorage.registerEntity(e, null);
		}
		for (Entity e : accessor.getAdditionalEntities().values()) {
			if (e != null) {
				endStorage.registerEntity(e, null);
			}
		}

		endTime = System.currentTimeMillis();
		deltaTime = (endTime - startTime) / 1000;
		startTime = endTime;
		System.out.println("Process all entities: " + deltaTime + " sec.");

		resultFile.delete();
		OutputStream outputStream = new FileOutputStream(resultFile);
		if (resultFile.getName().endsWith(".gz")) {
			outputStream = new GZIPOutputStream(outputStream);
		} else if (resultFile.getName().endsWith(".bz2")) {
			outputStream = new BZip2CompressorOutputStream(outputStream);
		}
		new OsmStorageWriter().saveStorage(outputStream, endStorage, null, true);
		outputStream.close();
		endTime = System.currentTimeMillis();
		deltaTime = (endTime - startTime) / 1000;
		System.out.println("Wrote result into " + resultFile.getName() + " in " + deltaTime + " sec.");
	}

	static class AccessorForRelationExtract extends OsmDbAccessor {
		public static final int MAX_CHILD_LEVEL = 5;
		Map<EntityId, Entity> additionalEntities = new LinkedHashMap<>();

		public Map<EntityId, Entity> getAdditionalEntities() {
			return additionalEntities;
		}

		@Override
		public void loadEntityRelation(Relation e) throws SQLException {
			loadEntityRelation(e, 5);
		}

		@Override
		public void loadEntityRelation(Relation e, int level) throws SQLException {
			if (e.isDataLoaded()) {
				return;
			}
			Map<EntityId, Entity> map = new LinkedHashMap<>();
			if (e.getMembers().isEmpty()) {
				pselectRelation.setLong(1, e.getId());
				if (pselectRelation.execute()) {
					ResultSet rs = pselectRelation.getResultSet();
					while (rs.next()) {
						int ord = rs.getInt(4);
						if (ord == 0) {
							readTags(e, rs.getBytes(5));
						}
						e.addMember(rs.getLong(1), Entity.EntityType.values()[rs.getInt(2)], rs.getString(3));
					}
					rs.close();
				}
			}
			Collection<Relation.RelationMember> ids = e.getMembers();
			if (level > 0) {
				for (Relation.RelationMember i : ids) {
					if (i.getEntityId().getType() == Entity.EntityType.NODE) {
						pselectNode.setLong(1, i.getEntityId().getId());
						if (pselectNode.execute()) {
							ResultSet rs = pselectNode.getResultSet();
							Node n = null;
							while (rs.next()) {
								if (n == null) {
									n = new Node(rs.getDouble(1), rs.getDouble(2), i.getEntityId().getId());
									readTags(n, rs.getBytes(3));
								}
							}
							map.put(i.getEntityId(), n);
							rs.close();
						}
					} else if (i.getEntityId().getType() == Entity.EntityType.WAY) {
						if (i.getEntityId().getId() == 303751529) {
							System.out.println("==== test 303751529");
						}
						Way way = (Way) additionalEntities.get(i.getEntityId());
						if (way == null) {
							way = new Way(i.getEntityId().getId());
							loadEntityWay(way);
						}
						way.putTag("relation_ref_" + e.getId(), Long.toString(e.getId()));
						if (level != MAX_CHILD_LEVEL) {
							way.putTag("relation_level_" + e.getId(), Integer.toString(MAX_CHILD_LEVEL - level));
						}
						map.put(i.getEntityId(), way);
						for (Node n : way.getNodes()) {
							map.put(Entity.EntityId.valueOf(n), n);
						}
					} else if (i.getEntityId().getType() == Entity.EntityType.RELATION) {
						Relation rel = new Relation(i.getEntityId().getId());
						loadEntityRelation(rel, level - 1);
						map.put(i.getEntityId(), rel);
					}
				}

				e.initializeLinks(map);
				e.entityDataLoaded();
				additionalEntities.putAll(map);
			}
		}

		@Override
		public void loadEntityWay(Way e) throws SQLException {
			if (e.getEntityIds().isEmpty()) {
				pselectWay.setLong(1, e.getId());
				if (pselectWay.execute()) {
					ResultSet rs = pselectWay.getResultSet();
					while (rs.next()) {
						int ord = rs.getInt(2);
						if (ord == 0) {
							readTags(e, rs.getBytes(3));
						}
						if (rs.getObject(5) != null) {
							Node n = new Node(rs.getDouble(4), rs.getDouble(5), rs.getLong(1));
							e.addNode(n);
							readTags(n, rs.getBytes(6));
						} else {
							e.addNode(rs.getLong(1));
						}
					}
					rs.close();
				}
			}
		}
	}
}
