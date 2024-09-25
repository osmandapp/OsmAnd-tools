package net.osmand.routes;


import net.osmand.IProgress;
import net.osmand.gpx.GPXFile;
import net.osmand.gpx.GPXUtilities;
import net.osmand.impl.ConsoleProgressImplementation;
import net.osmand.obf.OsmGpxWriteContext;
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
import net.osmand.shared.io.KFile;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.xmlpull.v1.XmlPullParserException;

import javax.xml.stream.XMLStreamException;
import java.io.*;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static net.osmand.IndexConstants.GPX_GZ_FILE_EXT;
import static net.osmand.gpx.GPXUtilities.OSMAND_EXTENSIONS_PREFIX;
import static net.osmand.gpx.GPXUtilities.writeNotNullText;
import static net.osmand.router.RouteExporter.OSMAND_ROUTER_V2;

public class RouteRelationExtractor {

	private boolean DEBUG = false;
	private static final Log log = LogFactory.getLog(RouteRelationExtractor.class);
	int countFiles;
	int countWays;
	int countNodes;
	DBDialect osmDBdialect = DBDialect.SQLITE;
	private final double precisionLatLonEquals = 0.00001;
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
//			s.add("germany-latest.osm.gz");
			s.add("slovakia-latest.osm.gz");
//			s.add("malta-latest.osm.gz");
			args = s.toArray(new String[0]);
		} else if (args.length < 1) {
			// TODO specify source file, tmp folder, result file // finally clean up the folder
			System.err.println("Usage: country.osm(|.gz|.bz2|.pbf) [result.osm(|.gz|.bz2)] [result.travel.obf]");
			System.exit(1);
		}

		String sourceFilePath = args[0];

		String resultFilePath = args.length > 1 ? args[1]
				: sourceFilePath.replace(".osm", ".relations.osm").replace(".pbf", "");

		String obfFilePath = args.length > 2 ? args[2]
				: sourceFilePath.replace(".osm", ".travel.obf")
				.replace(".pbf", "").replace(".gz", "").replace(".bz2", "");

		try {
			RouteRelationExtractor rdg = new RouteRelationExtractor();
			File sourceFile = new File(sourceFilePath);
			File resultFile = new File(resultFilePath);
			rdg.extractRoutes(sourceFile, resultFile);
			rdg.gpxDirectoryToObfFile(getGpxDirectory(resultFile), new File(obfFilePath));
		} catch (SQLException | IOException | XmlPullParserException | XMLStreamException | InterruptedException e) {
			log.error("Extract routes error: ", e);
		}
	}

	private static File getGpxDirectory(File sourceFile) {
		return new File(sourceFile.getPath()
				.replace(".relations", ".gpx.files")
				.replace(".osm", "")
				.replace(".pbf", "")
				.replace(".bz2", "")
				.replace(".gz", ""));
	}

	private void gpxDirectoryToObfFile(File gpxDirectory, File obfFile) throws IOException, SQLException, XmlPullParserException, InterruptedException {
		if (gpxDirectory.isDirectory()) {
			List<KFile> kFiles = new ArrayList<>();
			for (File file : gpxDirectory.listFiles()) {
				if (file.getAbsolutePath().endsWith(GPX_GZ_FILE_EXT)) {
					kFiles.add(new KFile(file.getAbsolutePath()));
				}
			}
			if (kFiles.isEmpty()) {
				throw new RuntimeException("No GPX-gz files in directory: " + gpxDirectory.getAbsolutePath());
			}
			String osmFileName = Algorithms.getFileNameWithoutExtension(obfFile) + ".osm";
			OsmGpxWriteContext.QueryParams qp = new OsmGpxWriteContext.QueryParams();
			qp.osmFile = new File(osmFileName);
			OsmGpxWriteContext ctx = new OsmGpxWriteContext(qp);
			File tmpFolder = new File(gpxDirectory, "tmp");
			ctx.writeObf(null, kFiles, tmpFolder, osmFileName, obfFile);
			// TODO remove osmFileName finally?
		} else {
			throw new RuntimeException("Wrong GPX directory: " + gpxDirectory.getAbsolutePath());
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
		} else if (sourceFile.getName().endsWith(".bz2")) {
				sourceIs = new BZip2CompressorInputStream(new FileInputStream(sourceFile));
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
			dbCreator.finishLoading();
			osmDBdialect.commitDatabase(dbConn);
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
						saveGpx(e, accessor.getAdditionalEntities(), resultFile);
						accessor.additionalEntities.clear();
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
		log.info("Process all entities: " + deltaTime + " sec.");
		log.info(String.format("Process %d files %d ways %d nodes\n ", countFiles, countWays, countNodes));

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

	private void saveGpx(Entity e, Map<EntityId, Entity> additionalEntities, File resultFile) {
		GPXFile gpxFile = new GPXFile(OSMAND_ROUTER_V2);
		gpxFile.metadata.name = Objects.requireNonNullElse(e.getTag("name"), String.valueOf(e.getId()));
		gpxFile.metadata.desc = e.getTag("description"); // nullable
		gpxFile.metadata.getExtensionsToWrite().putAll(e.getTags());
		gpxFile.metadata.getExtensionsToWrite().put("osmid", String.valueOf(e.getId()));
		File gpxDir = getGpxDirectory(resultFile);

//		DEBUG = e.getId() == 16676577; // TODO remove debug

		try {
			if (!gpxDir.exists()) {
				Files.createDirectory(gpxDir.toPath());
			}
		} catch (IOException ex) {
			throw new RuntimeException(ex);
		}

		GPXUtilities.Track track = new GPXUtilities.Track();
		track.name = gpxFile.metadata.name;
		gpxFile.tracks.add(track);

		List<Way> waysToJoin = new ArrayList<>();
		for (Map.Entry<EntityId, Entity> entry : additionalEntities.entrySet()) {
			if (entry.getKey().getType() == Entity.EntityType.WAY) {
				if ("yes".equals(entry.getValue().getTag("area"))) {
					continue;
				}
				waysToJoin.add((Way) entry.getValue());
			} else if (entry.getKey().getType() == Entity.EntityType.NODE) {
				addNode(gpxFile, (Node) entry.getValue());
			}
		}

		GPXUtilities.TrkSegment[] currentSegment = {null};
		double[] lastLatLon = {Double.NaN, Double.NaN};
		for (int i = 0; i < waysToJoin.size(); i++) {
			addAndJoinWay(track, currentSegment, lastLatLon, waysToJoin.get(i),
					i > 0 ? waysToJoin.get(i - 1) : null,
					i < waysToJoin.size() - 1 ? waysToJoin.get(i + 1) : null);
		}

		File outFile = new File(gpxDir, e.getId() + GPX_GZ_FILE_EXT);
		try {
			OutputStream outputStream = new FileOutputStream(outFile);
			outputStream = new GZIPOutputStream(outputStream);
			GPXUtilities.writeGpx(new OutputStreamWriter(outputStream), gpxFile, null);
			outputStream.close();
		} catch (IOException ex) {
			throw new RuntimeException(ex);
		}
		countFiles++;
		if (countFiles % 100 == 0) {
			log.info(countFiles + " File " + outFile.getName() + " saved ");
		}
	}

	private boolean shouldReversePoints(Way current, Way prev, Way next) {
		if (prev != null && !prev.getNodes().isEmpty()) {
			Node currentLast = current.getLastNode();
			if (eqNodes(currentLast, prev.getFirstNode()) || eqNodes(currentLast, prev.getLastNode())) {
				return true;
			}
		}
		if (next != null && !next.getNodes().isEmpty()) {
			Node currentFirst = current.getFirstNode();
			if (eqNodes(currentFirst, next.getFirstNode()) || eqNodes(currentFirst, next.getLastNode())) {
				return true;
			}
		}
		return false;
	}

	private boolean eqNodes(Node n1, Node n2) {
		return MapUtils.areLatLonEqual(n1.getLatLon(), n2.getLatLon(), precisionLatLonEquals);
	}

	private void addAndJoinWay(GPXUtilities.Track track, GPXUtilities.TrkSegment[] currentSegment, double[] lastLatLon,
	                              Way current, Way prev, Way next) {
		if (current.getNodes().isEmpty()) {
			log.info("==== Empty Nodes in the Way " + current.getId());
			return;
		}

		boolean reverse = shouldReversePoints(current, prev, next);

		List<GPXUtilities.WptPt> points = new ArrayList<>();

		for (Node n : current.getNodes()) {
			points.add(new GPXUtilities.WptPt(n.getLatitude(), n.getLongitude()));
		}

		if (reverse) {
			Collections.reverse(points);
		}

		if (!MapUtils.areLatLonEqual(lastLatLon[0], lastLatLon[1],
				points.get(0).getLatitude(), points.get(0).getLongitude(), precisionLatLonEquals)) {
			GPXUtilities.TrkSegment trkSegment = new GPXUtilities.TrkSegment();
			trkSegment.getExtensionsToWrite().put("osmid", String.valueOf(current.getId()));
			track.segments.add(trkSegment);
			currentSegment[0] = trkSegment;
		}

		lastLatLon[0] = points.get(points.size() - 1).getLatitude();
		lastLatLon[1] = points.get(points.size() - 1).getLongitude();

		currentSegment[0].points.addAll(points);
		countWays++;
	}

	private void addNode(GPXFile gpxFile, Node node) {
		if (node != null) {
			GPXUtilities.WptPt wptPt = new GPXUtilities.WptPt();
			wptPt.lat = node.getLatitude();
			wptPt.lon = node.getLongitude();
			wptPt.getExtensionsToWrite().put("osmid", String.valueOf(node.getId()));
			wptPt.setExtensionsWriter("route_relation_node", serializer -> {
				for (Map.Entry<String, String> entry1 : node.getTags().entrySet()) {
					String key = entry1.getKey().replace(":", "_-_");
					if (!key.startsWith(OSMAND_EXTENSIONS_PREFIX)) {
						key = OSMAND_EXTENSIONS_PREFIX + key;
					}
					try {
						writeNotNullText(serializer, key, entry1.getValue());
					} catch (IOException ex) {
						throw new RuntimeException(ex);
					}
				}
			});
			wptPt.name = node.getTags().get("name");
			gpxFile.addPoint(wptPt);
			countNodes++;
		}
	}

	static class AccessorForRelationExtract extends OsmDbAccessor {
		Map<EntityId, Entity> additionalEntities = new LinkedHashMap<>();

		public Map<EntityId, Entity> getAdditionalEntities() {
			return additionalEntities;
		}

		@Override
		public void loadEntityRelation(Relation e) throws SQLException {
			List<Entity> parentRelations = new ArrayList<>();
			loadEntityRelation(e, parentRelations);
		}

		public void loadEntityRelation(Relation e, List<Entity> parentRelations) throws SQLException {
			if (e.isDataLoaded()) {
				return;
			}
			if (parentRelations.contains(e)) {
				log.info("==== circular relation link " + e.getId() + " on parent " + parentRelations);
				return;
			}
			parentRelations.add(e);
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
			if (parentRelations.size() > 0) {
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
						Way way = (Way) additionalEntities.get(i.getEntityId());
						if (way == null) {
							way = new Way(i.getEntityId().getId());
							loadEntityWay(way);
							if (!way.getNodes().isEmpty()) {
								map.put(i.getEntityId(), way);
							}
						}
						for (Entity pr : parentRelations) {
							if (way.getTag("route_relation_ref:" + pr.getId()) == null) {
								way.putTag("route_relation_ref:" + pr.getId(), Long.toString(pr.getId()));
							}
						}

					} else if (i.getEntityId().getType() == Entity.EntityType.RELATION) {
						Relation rel = new Relation(i.getEntityId().getId());
						loadEntityRelation(rel, parentRelations);
						map.put(i.getEntityId(), rel);
					}
				}

				e.initializeLinks(map);
				e.entityDataLoaded();
				parentRelations.remove(e);
				additionalEntities.putAll(map);
			}
		}
	}
}
