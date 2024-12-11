package net.osmand.routes;

import net.osmand.IProgress;
import net.osmand.MainUtilities.CommandLineOpts;
import net.osmand.data.Amenity;
import net.osmand.data.LatLon;
import net.osmand.impl.ConsoleProgressImplementation;
import net.osmand.obf.OsmGpxWriteContext;
import net.osmand.obf.preparation.DBDialect;
import net.osmand.obf.preparation.OsmDbAccessor;
import net.osmand.obf.preparation.OsmDbAccessorContext;
import net.osmand.obf.preparation.OsmDbCreator;
import net.osmand.osm.MapRenderingTypesEncoder;
import net.osmand.osm.OsmRouteType;
import net.osmand.osm.RelationTagsPropagation;
import net.osmand.osm.edit.Entity;
import net.osmand.osm.edit.Entity.EntityId;
import net.osmand.osm.edit.Node;
import net.osmand.osm.edit.Relation;
import net.osmand.osm.edit.Way;
import net.osmand.osm.io.OsmBaseStorage;
import net.osmand.osm.io.OsmBaseStoragePbf;
import net.osmand.osm.io.OsmStorageWriter;
import net.osmand.render.RenderingRuleSearchRequest;
import net.osmand.render.RenderingRulesStorage;
import net.osmand.shared.gpx.GpxFile;
import net.osmand.shared.gpx.GpxUtilities;
import net.osmand.shared.gpx.primitives.Track;
import net.osmand.shared.gpx.primitives.TrkSegment;
import net.osmand.shared.gpx.primitives.WptPt;
import net.osmand.shared.io.KFile;
import net.osmand.util.MapUtils;
import okio.Okio;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.xmlpull.v1.XmlPullParserException;

import javax.annotation.Nonnull;
import javax.xml.stream.XMLStreamException;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static net.osmand.IndexConstants.GPX_GZ_FILE_EXT;
import static net.osmand.obf.OsmGpxWriteContext.ROUTE_ID_TAG;
import static net.osmand.router.RouteExporter.OSMAND_ROUTER_V2;
import static net.osmand.shared.gpx.GpxUtilities.OSMAND_EXTENSIONS_PREFIX;

public class RouteRelationExtractor {
	private static final Log log = LogFactory.getLog(RouteRelationExtractor.class);
	int countFiles;
	int countWays;
	int countNodes;
	DBDialect osmDBdialect = DBDialect.SQLITE;
	private final double precisionLatLonEquals = 1e-5;

	private String[] customStyles = {
			"default.render.xml",
			"routes.addon.render.xml"
			// "skimap.render.xml" // ski-style could work instead of default.render.xml but not together
	};
	private static final Map<String, String> customProperties = Map.of(
			// default.render.xml:
			"whiteWaterSports", "true",
			// routes.addon.render.xml:
			"showCycleRoutes", "true",
			"showMtbRoutes", "true",
			"hikingRoutesOSMC", "walkingRoutesOSMC",
			"showDirtbikeTrails", "true",
			"horseRoutes", "true",
			"showFitnessTrails", "true",
			"showRunningRoutes", "true"
			// "pisteRoutes", "true" // skimap.render.xml conflicts with default
	);
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
			// Ignored: bus detour emergency_access evacuation ferry funicular historic light_rail motorcycle
			// Ignored: power railway road share_taxi subway taxi tracks train tram transhumance trolleybus worship
	};

	private final int ICON_SEARCH_ZOOM = 19;
	private final RenderingRulesStorage renderingRules;
	private final MapRenderingTypesEncoder renderingTypes;
	private final RenderingRuleSearchRequest searchRequest;

	private final boolean keepTmpFiles;
	private final String inFilePath, outFilePath, tmpDirectoryPath;
	private final String generationWorkingDirectory, gpxDirectoryPath;
	private final String dbFilePath, travelOsmFilePath, relationsOsmFilePath;

	public RouteRelationExtractor(String[] args) {
		CommandLineOpts opts = new CommandLineOpts(args);

		inFilePath = opts.getOpt("--in");
		outFilePath = opts.getOpt("--out");
		keepTmpFiles = opts.getBoolean("--keep-tmp-files");
		tmpDirectoryPath = Objects.requireNonNullElse(opts.getOpt("--tmp"), "/tmp");

		if (opts.getBoolean("--help") || inFilePath == null || outFilePath == null) {
			System.err.printf("%s\n", String.join("\n",
					"",
					"Usage: route-relation-extractor [--options]",
					"",
					"--in=/path/to/input_osm_file.(gz|bz2|pbf)",
					"--out=/path/to/output_obf_file.obf",
					"--tmp=/path/to/tmp (/tmp)",
					"--keep-tmp-files",
					"--help",
					""
			));
			System.exit(0);
		}

		String basename = Paths.get(inFilePath).getFileName().toString().replace('.', '-');

		dbFilePath = tmpDirectoryPath + "/" + basename + ".db";
		travelOsmFilePath = tmpDirectoryPath + "/" + basename + ".travel.osm";
		relationsOsmFilePath = tmpDirectoryPath + "/" + basename + ".relations.osm";
		generationWorkingDirectory = tmpDirectoryPath + "/" + basename + "-obf";
		gpxDirectoryPath = tmpDirectoryPath + "/" + basename + "-gpx";

		renderingTypes = new MapRenderingTypesEncoder("basemap");
		renderingRules = RenderingRulesStorage.initWithStylesFromResources(customStyles);
		searchRequest = RenderingRuleSearchRequest.initWithCustomProperties(renderingRules, ICON_SEARCH_ZOOM, customProperties);
	}

	private void cleanupTmpFiles() throws IOException {
		class remover extends SimpleFileVisitor<Path> {
			@Override
			public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
				String name = file.getFileName().toString();
				if (name.endsWith(".gpx.gz") || name.endsWith(".obf")) {
					Files.delete(file);
				}
				return FileVisitResult.CONTINUE;
			}
		}
		if (!keepTmpFiles) {
			if (Files.exists(Paths.get(generationWorkingDirectory))) {
				Files.walkFileTree(Paths.get(generationWorkingDirectory), new remover());
				Files.deleteIfExists(Paths.get(generationWorkingDirectory));
			}
			if (Files.exists(Paths.get(gpxDirectoryPath))) {
				Files.walkFileTree(Paths.get(gpxDirectoryPath), new remover());
				Files.deleteIfExists(Paths.get(gpxDirectoryPath));
			}
			Files.deleteIfExists(Paths.get(dbFilePath));
			Files.deleteIfExists(Paths.get(travelOsmFilePath));
			Files.deleteIfExists(Paths.get(relationsOsmFilePath));
			try {
				Files.deleteIfExists(Paths.get(tmpDirectoryPath));
			} catch (IOException e) {
				log.info("Unable to remove " + tmpDirectoryPath); // could be common such as /tmp
			}
		}
	}

	private void initTmpFiles() throws IOException {
		cleanupTmpFiles(); // always do cleanup before
		Files.createDirectories(Paths.get(gpxDirectoryPath));
		Files.createDirectories(Paths.get(generationWorkingDirectory));
	}

	public static void main(String[] args) {
		RouteRelationExtractor rre = new RouteRelationExtractor(args);
		try {
			rre.initTmpFiles();
			rre.extractRoutes();
			rre.gpxDirectoryToObfFile();
		} catch (SQLException | IOException | XmlPullParserException | XMLStreamException | InterruptedException e) {
			log.error("Extract routes error: ", e);
		} finally {
			try {
				rre.cleanupTmpFiles();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

	private File getGpxDirectory() {
		return new File(gpxDirectoryPath);
	}

	private void gpxDirectoryToObfFile() throws IOException, SQLException, XmlPullParserException, InterruptedException {
		File gpxDirectory = getGpxDirectory();
		File obfFile = new File(outFilePath);
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
			OsmGpxWriteContext.QueryParams qp = new OsmGpxWriteContext.QueryParams();
			qp.osmFile = new File(travelOsmFilePath);
			OsmGpxWriteContext ctx = new OsmGpxWriteContext(qp);
			File tmpFolder = new File(generationWorkingDirectory);
			String obfFileBaseName = Paths.get(outFilePath).getFileName().toString();
			ctx.writeObf(null, kFiles, tmpFolder, obfFileBaseName, obfFile);
		} else {
			throw new RuntimeException("Wrong GPX directory: " + gpxDirectory.getAbsolutePath());
		}
	}

	private void extractRoutes() throws IOException, XmlPullParserException,
			XMLStreamException, SQLException, InterruptedException {

		File sourceFile = new File(inFilePath);
		File resultFile = new File(relationsOsmFilePath);
		File dbFile = new File(dbFilePath);

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
		GpxFile gpxFile = new GpxFile(OSMAND_ROUTER_V2);

		String mainName = relation.getTag("name");
		String enName = relation.getTag("name:en");

		if (mainName != null) {
			gpxFile.getMetadata().setName(mainName);
		} else if (enName != null) {
			gpxFile.getMetadata().setName(enName);
		}

		gpxFile.getMetadata().setDesc(relation.getTag("description")); // nullable

		Map <String, String> gpxExtensions = gpxFile.getExtensionsToWrite();

		gpxExtensions.putAll(relation.getTags());

		gpxExtensions.put("flexible_line_width", "yes");
		gpxExtensions.put("translucent_line_colors", "yes");
		gpxExtensions.put(ROUTE_ID_TAG, Amenity.ROUTE_ID_OSM_PREFIX + relation.getId());

		File gpxDir = getGpxDirectory();

		try {
			if (!gpxDir.exists()) {
				Files.createDirectory(gpxDir.toPath());
			}
		} catch (IOException ex) {
			throw new RuntimeException(ex);
		}

		Track track = new Track();
		track.setName(gpxFile.getMetadata().getName()); // nullable
		gpxFile.getTracks().add(track);

		RelationTagsPropagation transformer = new RelationTagsPropagation();
		try {
			transformer.handleRelationPropogatedTags((Relation)relation, renderingTypes, null,
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
				transformer.addPropogatedTags(renderingTypes,
						MapRenderingTypesEncoder.EntityConvertApplyType.MAP, way, way.getModifiableTags());
				gpxExtensions.putAll(getShieldTagsFromOsmcTags(way.getTags()));
			} else if (entry.getKey().getType() == Entity.EntityType.NODE) {
				addNode(gpxFile, (Node) entry.getValue());
			}
		}

		joinWaysIntoTrackSegments(track, waysToJoin);

		String routeTag = gpxExtensions.get("route"); // from OSM relation

		if (routeTag != null) {
			OsmRouteType routeType = null;
			for (String tag : routeTag.split("[;, ]")) {
				routeType = OsmRouteType.convertFromOsmGPXTag(tag);
				if (routeType != null) {
					break; // consider 1st found as main type
				}
			}
			gpxExtensions.put("route_type", routeType.getName());
		}

		File outFile = new File(gpxDir, relation.getId() + GPX_GZ_FILE_EXT);
		try {
			OutputStream outputStream = new FileOutputStream(outFile);
			outputStream = new GZIPOutputStream(outputStream);
			Exception ex = GpxUtilities.INSTANCE.writeGpx(null, Okio.buffer(Okio.sink(outputStream)), gpxFile, null);
			if (ex != null) {
				throw new RuntimeException(ex);
			}
			outputStream.close();
		} catch (IOException ex) {
			throw new RuntimeException(ex);
		}
		countFiles++;
		if (countFiles % 100 == 0) {
			log.info(countFiles + " File " + outFile.getName() + " saved ");
		}
	}

	private void joinWaysIntoTrackSegments(Track track, List<Way> ways) {
		boolean[] done = new boolean[ways.size()];
		while (true) {
			List<WptPt> wpts = new ArrayList<>();
			for (int i = 0; i < ways.size(); i++) {
				if (!done[i]) {
					done[i] = true;
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
			TrkSegment segment = new TrkSegment();
			segment.getPoints().addAll(wpts);
			track.getSegments().add(segment);
		}
	}

	private boolean considerCandidateToJoin(List<WptPt> wpts, Way candidate) {
		if (wpts.isEmpty() || candidate.getNodes().isEmpty()) {
			return true;
		}

		WptPt firstWpt = wpts.get(0);
		WptPt lastWpt = wpts.get(wpts.size() - 1);
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

	private void addWayToPoints(List<WptPt> wpts, boolean insert, Way way, boolean reverse) {
		List<WptPt> points = new ArrayList<>();
		for (Node n : way.getNodes()) {
			points.add(new WptPt(n.getLatitude(), n.getLongitude()));
		}
		if (reverse) {
			Collections.reverse(points);
		}
		wpts.addAll(insert ? 0 : wpts.size(), points);
	}

	private boolean eqWptToLatLon(WptPt wpt, LatLon ll) {
		return MapUtils.areLatLonEqual(new LatLon(wpt.getLatitude(), wpt.getLongitude()), ll, precisionLatLonEquals);
	}

	final String[] nodeNameTags = { "name", "name:en" }; // no more ref here

	final Map<String, String> skipNodeByTags = Map.of(
			"information", "guidepost"
			// ...
	);

	private void addNode(GpxFile gpxFile, Node node) {
		if (node != null && !node.getTags().isEmpty()) {
			for (String k : skipNodeByTags.keySet()) {
				final String nodeTagValue = node.getTags().get(k);
				if (nodeTagValue != null && nodeTagValue.equals(skipNodeByTags.get(k))) {
					return;
				}
			}
			final Map<String, String> transformedTags = renderingTypes.transformTags(node.getTags(),
					Entity.EntityType.NODE, MapRenderingTypesEncoder.EntityConvertApplyType.MAP);
			String gpxIcon = searchRequest.searchIconByTags(transformedTags);
			if (gpxIcon == null) {
				return;
			}
			WptPt wptPt = new WptPt();
			wptPt.setLat(node.getLatitude());
			wptPt.setLon(node.getLongitude());
			wptPt.getExtensionsToWrite().put("icon", gpxIcon);
			wptPt.setExtensionsWriter("route_relation_node", serializer -> {
				for (Map.Entry<String, String> entry : node.getTags().entrySet()) {
					String key = entry.getKey().replace(":", "_-_");
					if (!key.startsWith(OSMAND_EXTENSIONS_PREFIX)) {
						key = OSMAND_EXTENSIONS_PREFIX + key;
					}
					GpxUtilities.INSTANCE.writeNotNullText(serializer, key, entry.getValue());
				}
			});
			Map<String, String> tags = node.getTags();
			for (String tag : nodeNameTags) {
				String val = tags.get(tag);
				if (val != null) {
					wptPt.setName(val);
					break;
				}
			}
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
			"osmc_waycolor", "shield_waycolor" // waycolor is a part of osmc:symbol and must be applied to whole way
	);

	private static final String OSMC_ICON_PREFIX = "osmc_";
	private static final String OSMC_ICON_BG_SUFFIX = "_bg";
	private static final Set<String> shieldBgIcons = Set.of("shield_bg");
	private static final Set<String> shieldFgIcons = Set.of("shield_fg", "sheld_fg_2");

	@Nonnull
	public static Map<String, String> getShieldTagsFromOsmcTags(@Nonnull Map<String, String> tags) {
		Map<String, String> result = new LinkedHashMap<>();
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
