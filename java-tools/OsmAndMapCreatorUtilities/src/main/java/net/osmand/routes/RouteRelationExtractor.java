package net.osmand.routes;

import net.osmand.IProgress;
import net.osmand.data.LatLon;
import net.osmand.gpx.GPXFile;
import net.osmand.gpx.GPXUtilities;
import net.osmand.impl.ConsoleProgressImplementation;
import net.osmand.obf.OsmGpxWriteContext;
import net.osmand.obf.preparation.DBDialect;
import net.osmand.obf.preparation.OsmDbAccessor;
import net.osmand.obf.preparation.OsmDbAccessorContext;
import net.osmand.obf.preparation.OsmDbCreator;
import net.osmand.osm.MapRenderingTypesEncoder;
import net.osmand.osm.RelationTagsPropagation;
import net.osmand.osm.edit.Entity;
import net.osmand.osm.edit.Entity.EntityId;
import net.osmand.osm.edit.Node;
import net.osmand.osm.edit.Relation;
import net.osmand.osm.edit.Way;
import net.osmand.osm.io.OsmBaseStorage;
import net.osmand.osm.io.OsmBaseStoragePbf;
import net.osmand.osm.io.OsmStorageWriter;
import net.osmand.render.FindByRenderingTypesRules;
import net.osmand.shared.io.KFile;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.xmlpull.v1.XmlPullParserException;

import javax.annotation.Nonnull;
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
import static net.osmand.obf.OsmGpxWriteContext.OSM_TAG_PREFIX;
import static net.osmand.router.RouteExporter.OSMAND_ROUTER_V2;

public class RouteRelationExtractor {
//	private boolean DEBUG = false;
	private static final Log log = LogFactory.getLog(RouteRelationExtractor.class);
	int countFiles;
	int countWays;
	int countNodes;
	DBDialect osmDBdialect = DBDialect.SQLITE;
	private final double precisionLatLonEquals = 1e-5;

	private String[] customStyles = {
			"default.render.xml",
			"routes.addon.render.xml"
			// "skimap.render.xml" // could work instead of default.render.xml but not together
	};
	private static final Map<String, String> customProperties = Map.of(
			// routes.addon.render.xml
			"showCycleRoutes", "true",
			"showMtbRoutes", "true",
			"hikingRoutesOSMC", "walkingRoutesOSMC",
			"showDirtbikeTrails", "true",
			"horseRoutes", "true",
			"showFitnessTrails", "true",
			"showRunningRoutes", "true",
			"whiteWaterSports", "true" // default.render.xml
			// "pisteRoutes", "true" // skimap.render.xml conflicts with default
	);
	private FindByRenderingTypesRules finder = new FindByRenderingTypesRules(customStyles, customProperties);
	private final String[] filteredTags = {
			"hiking", // 244k
			"bicycle", // 119k
			"foot", // 63k
			"mtb", // 29k
			"piste", // 14k
			"ski", // 8k
			"horse", // 4k
			"running", // 1k
			"snowmobile", // 1k
			"fitness_trail", // 1k
			"canoe", // 0.8k
			"canyoning", // 0.6k
			"motorboat", // 0.4k
			"boat", // 0.3k
			"waterway", // 0.3k
			"inline_skates", // 0.2k
			"via_ferrata", // 0.2k
			"walking", // 0.2k
			"ferrata", // proposed
			// bus detour emergency_access evacuation ferry funicular historic light_rail motorcycle
			// power railway road share_taxi subway taxi tracks train tram transhumance trolleybus worship
	};

	public static void main(String[] args) {
		if (args.length == 1 && args[0].equals("test")) {
			List<String> s = new ArrayList<>();
			s.add("slovakia-latest.osm.pbf");
//			s.add("czech-republic-latest.osm.pbf");
//			s.add("netherlands-latest.osm.pbf");
//			s.add("germany-latest.osm.gz");
//			s.add("malta-latest.osm.gz");
//			s.add("andorra-latest.osm.gz");
//			s.add("italy_sicilia.osm.pbf");
			args = s.toArray(new String[0]);
		} else if (args.length < 1) {
			// TODO specify source file, tmp folder, result file // finally clean up the folder
			System.err.println("Usage: country.osm(|.gz|.bz2|.pbf) [result.osm(|.gz|.bz2)] [result.travel.obf]");
			System.exit(1);
		}

		String sourceFilePath = args[0];

		final String RELATIONS_OSM_EXT = ".relations.osm";
		String resultFilePath = args.length > 1 ? args[1]
				: sourceFilePath.replace(".osm", RELATIONS_OSM_EXT).replace(".pbf", "");
		if (!resultFilePath.contains(RELATIONS_OSM_EXT)) {
			resultFilePath += RELATIONS_OSM_EXT;
		}

		final String TRAVEL_OBF_EXT = ".travel.obf";
		String obfFilePath = args.length > 2 ? args[2]
				: sourceFilePath.replace(".osm", TRAVEL_OBF_EXT)
				.replace(".pbf", "").replace(".gz", "").replace(".bz2", "");
		if (!obfFilePath.contains(TRAVEL_OBF_EXT)) {
			obfFilePath += TRAVEL_OBF_EXT;
		}

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

			private boolean accessedRoute(@Nonnull String route) {
				for (String tag : route.split("[;, ]")) {
					for (String value : filteredTags) {
						if (tag.startsWith(value) || tag.endsWith(value)) {
							return true;
						}
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

	private void saveGpx(Entity relation, Map<EntityId, Entity> children, File resultFile) {
//		DEBUG = relation.getId() == 13168625;

		GPXFile gpxFile = new GPXFile(OSMAND_ROUTER_V2);

		String id = String.valueOf(relation.getId());
		String ref = relation.getTag("ref");
		String mainName = relation.getTag("name");
		String enName = relation.getTag("name:en");
		String description = relation.getTag("description");
		final int MAX_DESC_NAME_LENGTH = 80;

		if (mainName != null) {
			gpxFile.metadata.name = mainName;
		} else if (enName != null) {
			gpxFile.metadata.name = enName;
		} else if (description != null && description.length() < MAX_DESC_NAME_LENGTH && !description.contains("\n")) {
			gpxFile.metadata.name = description; // use short description when no name defined
		} else if (ref != null) {
			gpxFile.metadata.name = ref;
		} else {
			gpxFile.metadata.name = id;
		}

		gpxFile.metadata.desc = relation.getTag("description"); // nullable

		Map <String, String> metadataExtensions = gpxFile.metadata.getExtensionsToWrite();

		for (String key : relation.getTagKeySet()) {
			metadataExtensions.put(OSM_TAG_PREFIX + key, relation.getTag(key));
		}

		metadataExtensions.put(OSM_TAG_PREFIX + "id", String.valueOf(relation.getId()));
		metadataExtensions.put("relation_gpx", "yes"); // render route:segment distinctly

		if (relation.getTags().containsKey("colour")) {
			metadataExtensions.remove("colour");
			metadataExtensions.put("color", relation.getTags().get("colour"));
		}

		File gpxDir = getGpxDirectory(resultFile);

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

		RelationTagsPropagation transformer = new RelationTagsPropagation();
		try {
			transformer.handleRelationPropogatedTags((Relation)relation, finder.renderingTypes, null,
					MapRenderingTypesEncoder.EntityConvertApplyType.MAP);
		} catch (SQLException ex) {
			throw new RuntimeException(ex);
		}

		List<Way> waysToJoin = new ArrayList<>();
		for (Map.Entry<EntityId, Entity> entry : children.entrySet()) {
			if (entry.getKey().getType() == Entity.EntityType.WAY) {
				Way way = (Way) entry.getValue();
				if ("yes".equals(way.getTag("area"))) {
					continue; // skip (eg https://www.openstreetmap.org/way/746544031)
				}
				waysToJoin.add(way);
				transformer.addPropogatedTags(finder.renderingTypes,
						MapRenderingTypesEncoder.EntityConvertApplyType.MAP, way, way.getModifiableTags());
				Map<String, String> props = getShieldTagsFromOsmcTags(way.getTags());
				if (!Algorithms.isEmpty(props)) {
					if (props.containsKey("color")) {
						// color is forced by osmc_waycolor
						metadataExtensions.remove("colour");
					}
					if ((props.containsKey("shield_text") || props.containsKey("shield_textcolor"))
							&& !metadataExtensions.containsKey(OSM_TAG_PREFIX + "osmc:symbol")) {
						// avoid synthetic osmc
						props.remove("shield_text");
						props.remove("shield_textcolor");
					}
					metadataExtensions.putAll(props);
				}
			} else if (entry.getKey().getType() == Entity.EntityType.NODE) {
				addNode(gpxFile, (Node) entry.getValue());
			}
		}

		joinWaysIntoTrackSegments(track, waysToJoin);

		File outFile = new File(gpxDir, relation.getId() + GPX_GZ_FILE_EXT);
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

	private void joinWaysIntoTrackSegments(GPXUtilities.Track track, List<Way> ways) {
		boolean[] done = new boolean[ways.size()];
		while (true) {
			long osmId = 0;
			List<GPXUtilities.WptPt> wpts = new ArrayList<>();
			for (int i = 0; i < ways.size(); i++) {
				if (!done[i]) {
					done[i] = true;
					osmId = ways.get(i).getId(); // osm_id tag (optional)
					addWayToPoints(wpts, false, ways.get(i), false); // "head" way
					while (true) {
						boolean stop = true;
						for (int j = 0; j < ways.size(); j++) {
							if (!done[j] && considerCandidateToJoin(wpts, ways.get(j))) {
								done[j] = true;
								stop = false;
							}
						}
						if (stop) {
							break; // nothing joined
						}
					}
					break; // segment is done
				}
			}
			if (wpts.isEmpty()) {
				break; // all done
			}
			GPXUtilities.TrkSegment segment = new GPXUtilities.TrkSegment();
			// segment.getExtensionsToWrite().put(OSM_TAG_PREFIX + "id", String.valueOf(osmId));
			// segment.getExtensionsToWrite().put("relation_track", "yes");
			segment.points.addAll(wpts);
			track.segments.add(segment);
		}
	}

	private boolean considerCandidateToJoin(List<GPXUtilities.WptPt> wpts, Way candidate) {
		if (wpts.isEmpty() || candidate.getNodes().isEmpty()) {
			return true;
		}

		GPXUtilities.WptPt firstWpt = wpts.get(0);
		GPXUtilities.WptPt lastWpt = wpts.get(wpts.size() - 1);
		LatLon firstCandidateLL = candidate.getNodes().get(0).getLatLon();
		LatLon lastCandidateLL = candidate.getNodes().get(candidate.getNodes().size() - 1).getLatLon();

		if (eqWptToLatLon(lastWpt, firstCandidateLL)) {
			addWayToPoints(wpts, false, candidate, false); // wpts + Candidate
		} else if (eqWptToLatLon(lastWpt, lastCandidateLL)) {
			addWayToPoints(wpts, false, candidate, true); // wpts + etadidnaC
		} else if (eqWptToLatLon(firstWpt, firstCandidateLL)) {
			addWayToPoints(wpts, true, candidate, true); // etadidnaC + wpts
		} else if (eqWptToLatLon(firstWpt, lastCandidateLL)) {
			addWayToPoints(wpts, true, candidate, false); // Candidate + wpts
		} else {
			return false;
		}

		return true;
	}

	private void addWayToPoints(List<GPXUtilities.WptPt> wpts, boolean insert, Way way, boolean reverse) {
		List<GPXUtilities.WptPt> points = new ArrayList<>();
		for (Node n : way.getNodes()) {
			points.add(new GPXUtilities.WptPt(n.getLatitude(), n.getLongitude()));
		}
		if (reverse) {
			Collections.reverse(points);
		}
		wpts.addAll(insert ? 0 : wpts.size(), points);
	}

	private boolean eqWptToLatLon(GPXUtilities.WptPt wpt, LatLon ll) {
		return MapUtils.areLatLonEqual(new LatLon(wpt.getLatitude(), wpt.getLongitude()), ll, precisionLatLonEquals);
	}

	private void addNode(GPXFile gpxFile, Node node) {
		final Map<String, String> skipNodeByTags = Map.of(
				"information", "guidepost"
		);
		if (node != null && !node.getTags().isEmpty()) {
			for (String k : skipNodeByTags.keySet()) {
				final String nodeTagValue = node.getTags().get(k);
				if (nodeTagValue != null && nodeTagValue.equals(skipNodeByTags.get(k))) {
					return;
				}
			}
			String gpxIcon = finder.searchGpxIconByNode(node);
			if (gpxIcon == null) {
				return;
			}
			GPXUtilities.WptPt wptPt = new GPXUtilities.WptPt();
			wptPt.lat = node.getLatitude();
			wptPt.lon = node.getLongitude();
//			wptPt.getExtensionsToWrite().put("relation_point", "yes");
			wptPt.getExtensionsToWrite().put("gpx_icon", gpxIcon);
			wptPt.getExtensionsToWrite().put(OSM_TAG_PREFIX + "id", String.valueOf(node.getId()));
			wptPt.setExtensionsWriter("route_relation_node", serializer -> {
				for (Map.Entry<String, String> entry1 : node.getTags().entrySet()) {
					String key = entry1.getKey().replace(":", "_-_");
					if (!key.startsWith(OSMAND_EXTENSIONS_PREFIX)) {
						key = OSMAND_EXTENSIONS_PREFIX + OSM_TAG_PREFIX + key;
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

	private static final Map<String, String> osmcTagsToShieldProps = Map.of(
			"osmc_text", "shield_text",
			"osmc_background", "shield_bg",
			"osmc_foreground", "shield_fg",
			"osmc_foreground2", "shield_fg_2",
			"osmc_textcolor", "shield_textcolor",
			"osmc_waycolor", "color" // waycolor is a part of osmc:symbol and must be applied to whole way
	);

	private static final String OSMC_ICON_PREFIX = "osmc_";
	private static final String OSMC_ICON_BG_SUFFIX = "_bg";
	private static final Set<String> shieldBgIcons = Set.of("shield_bg");
	private static final Set<String> shieldFgIcons = Set.of("shield_fg", "sheld_fg_2");

	@Nonnull
	public static Map<String, String> getShieldTagsFromOsmcTags(@Nonnull Map<String, String> tags) {
		Map<String, String> result = new HashMap<>();
		for (String tag : tags.keySet()) {
			for (String match : osmcTagsToShieldProps.keySet()) {
				if (tag.endsWith(match)) {
					final String key = osmcTagsToShieldProps.get(match);
					final String prefix =
							(shieldBgIcons.contains(key) || shieldFgIcons.contains(key)) ? OSMC_ICON_PREFIX : "";
					final String suffix = shieldBgIcons.contains(key) ? OSMC_ICON_BG_SUFFIX : "";
					final String val = prefix + tags.get(tag) + suffix;
					result.putIfAbsent(key, val); // prefer 1st
				}
			}
		}
		return result;
	}
}
