package net.osmand.obf.preparation;

import net.osmand.IProgress;
import net.osmand.IndexConstants;
import net.osmand.binary.MapZooms;
import net.osmand.binary.ObfConstants;
import net.osmand.impl.ConsoleProgressImplementation;
import net.osmand.obf.preparation.OsmDbAccessor.OsmDbVisitor;
import net.osmand.osm.MapPoiTypes;
import net.osmand.osm.MapRenderingTypesEncoder;
import net.osmand.osm.edit.Entity;
import net.osmand.osm.edit.Entity.EntityId;
import net.osmand.osm.edit.Entity.EntityType;
import net.osmand.osm.edit.Node;
import net.osmand.osm.edit.Relation;
import net.osmand.osm.edit.Way;
import net.osmand.osm.io.IOsmStorageFilter;
import net.osmand.osm.io.OsmBaseStorage;
import net.osmand.osm.io.OsmBaseStoragePbf;
import net.osmand.util.Algorithms;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.xmlpull.v1.XmlPullParserException;
import rtree.RTreeException;

import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import java.io.*;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.zip.GZIPInputStream;

/**
 * http://wiki.openstreetmap.org/wiki/OSM_tags_for_routing#Is_inside.2Foutside
 * http://wiki.openstreetmap.org/wiki/Relations/Proposed/Postal_Addresses
 * http://wiki.openstreetmap.org/wiki/Proposed_features/House_numbers/Karlsruhe_Schema#Tags (node, way)
 *
 * That data extraction has aim, save runtime memory and generate indexes on the fly. It will be longer than load in
 * memory (needed part) and save into index.
 */
public class IndexCreator {
	private static final Log log = LogFactory.getLog(IndexCreator.class);

	// ONLY derby.jar needed for derby dialect
	// (NOSQL is the fastest but is supported only on linux 32)
	// Sqlite better to use only for 32-bit machines
	private DBDialect osmDBdialect = DBDialect.SQLITE;
	private DBDialect mapIndexDBDialect = DBDialect.SQLITE;
	public static boolean REMOVE_POI_DB = true;

	public static final int BATCH_SIZE = 5000;
	public static final int BATCH_SIZE_OSM = 10000;
	public static final String TEMP_NODES_DB = "nodes.tmp.odb";

	public static final int STEP_MAIN = 4;

	private File workingDir = null;
	private String regionName;
	private String mapFileName = null;
	private Long lastModifiedDate = null;
	private IndexCreatorSettings settings;

	IndexTransportCreator indexTransportCreator;
	IndexPoiCreator indexPoiCreator;
	IndexAddressCreator indexAddressCreator;
	IndexVectorMapCreator indexMapCreator;
	// v2 indexes combined route relations as a long chain of ways
	IndexRouteRelationCreator indexRouteRelationCreatorV2;
	// v1 indexes propagate route relations into ways with tag prefixes
	IndexRouteRelationCreatorV1 indexRouteRelationCreatorV1;
	IndexRouteCreator indexRouteCreator;
	IndexHeightData heightData = null;
	PropagateToNodes propagateToNodes;

	private File dbFile;
	private File mapFile;
	private RandomAccessFile mapRAFile;
	private Connection mapConnection;

	// constants to start process from the middle and save temporary results
	private boolean recreateOnlyBinaryFile = false; // false;
	private boolean deleteOsmDB = true;
	private boolean deleteDatabaseIndexes = true;
    private String TIGER_OSMAND_TAG = "tiger:osmand";

	public IndexCreator(File workingDir, IndexCreatorSettings settings) {
		this.workingDir = workingDir;
		this.settings = settings;
		if (settings.srtmDataFolderUrl == null && new File(workingDir, "srtm").exists()) {
			settings.srtmDataFolderUrl = new File(workingDir, "srtm").getAbsolutePath();
		}
		if (settings.srtmDataFolderUrl != null) {
			Iterator<ImageReader> readers = ImageIO.getImageReadersByFormatName("TIFF");
			while (readers.hasNext()) {
				System.out.println("Tiff reader: " + readers.next());
			}
			heightData = new IndexHeightData();
			heightData.setSrtmData(settings.srtmDataFolderUrl, workingDir);
		}
	}

	public IndexCreatorSettings getSettings() {
		return settings;
	}

	public String getRegionName() {
		if (regionName == null) {
			return "Region"; //$NON-NLS-1$
		}
		return regionName;
	}

	public void setRegionName(String regionName) {
		this.regionName = regionName;
	}

	private Object getDatabaseConnection(String fileName, DBDialect dialect) throws SQLException {
		return dialect.getDatabaseConnection(fileName, log);
	}

	public void setNodesDBFile(File file) {
		dbFile = file;
	}

	public void setDeleteOsmDB(boolean deleteOsmDB) {
		this.deleteOsmDB = deleteOsmDB;
	}

	public void setMapFileName(String mapFileName) {
		this.mapFileName = mapFileName;
	}

	public String getMapFileName() {
		if (mapFileName == null) {
			return getRegionName() + IndexConstants.BINARY_MAP_INDEX_EXT;
		}
		return mapFileName;
	}

	public String getTempMapDBFileName() {
		return getMapFileName() + ".tmp"; //$NON-NLS-1$
	}

	public void setDialects(DBDialect osmDBdialect, DBDialect mapIndexDBDialect) {
		if (osmDBdialect != null) {
			this.osmDBdialect = osmDBdialect;
		}
		if (mapIndexDBDialect != null) {
			this.mapIndexDBDialect = mapIndexDBDialect;
		}
	}

	public Long getLastModifiedDate() {
		return lastModifiedDate;
	}

	public void setLastModifiedDate(Long lastModifiedDate) {
		this.lastModifiedDate = lastModifiedDate;
	}

	public static String getPoiFileName(String regionName) {
		return regionName + IndexConstants.POI_INDEX_EXT;
	}

	public String getPoiFileName() {
		return getPoiFileName(getRegionName());
	}

	private File getPoiFile() {
		return new File(workingDir, getPoiFileName());
	}

	public String getRTreeMapIndexNonPackFileName() {
		return mapFile.getAbsolutePath() + ".rtree"; //$NON-NLS-1$
	}

	public String getRTreeRouteIndexNonPackFileName() {
		return mapFile.getAbsolutePath() + ".rte"; //$NON-NLS-1$
	}

	public String getRTreeRouteIndexPackFileName() {
		return mapFile.getAbsolutePath() + ".prte"; //$NON-NLS-1$
	}

	public String getRTreeTransportStopsFileName() {
		return mapFile.getAbsolutePath() + ".trans"; //$NON-NLS-1$
	}

	public String getRTreeTransportStopsPackFileName() {
		return mapFile.getAbsolutePath() + ".ptrans"; //$NON-NLS-1$
	}

	public String getRTreeMapIndexPackFileName() {
		return mapFile.getAbsolutePath() + ".prtree"; //$NON-NLS-1$
	}

	/* ***** END OF GETTERS/SETTERS ***** */

	private void iterateMainEntity(Entity e, OsmDbAccessorContext ctx, IndexCreationContext icc) throws SQLException {
		calculateRegionTagAndTransliterate(e, icc);
        if (e.getTag(TIGER_OSMAND_TAG) != null) {
			if (settings.indexAddress) {
				indexAddressCreator.iterateMainEntity(e, ctx, icc);
			}
            return;
        }
		if (heightData != null && e instanceof Way) {
			if (!settings.keepOnlyRouteRelationObjects) {// small speedup
				heightData.proccess((Way) e);
			}
		}
		if (propagateToNodes != null && e instanceof Node) {
			propagateToNodes.propagateTagsToNode((Node) e, true);
		}
		if (settings.indexPOI) {
			indexPoiCreator.iterateEntity(e, ctx, icc);
		}
		if (settings.indexTransport) {
			indexTransportCreator.iterateMainEntity(e, ctx, icc);
		}
		if (settings.indexMap) {
			if (settings.boundary == null || checkBoundary(e)) {
				indexMapCreator.iterateMainEntity(e, ctx, icc);
				indexRouteRelationCreatorV1.iterateMainEntity(e, ctx, icc);
			}
		}
		if (settings.indexAddress) {
			indexAddressCreator.iterateMainEntity(e, ctx, icc);
		}
		if (settings.indexRouting) {
			indexRouteCreator.iterateMainEntity(e, ctx, icc);
		}
	}

	private void calculateRegionTagAndTransliterate(Entity e, IndexCreationContext icc) {
		if (settings.addRegionTag) {
            icc.calcRegionTag(e, true);
        }
		icc.translitJapaneseNames(e);
		icc.translitChineseNames(e);
	}

	private boolean checkBoundary(Entity e) {
		if (settings.boundary != null) {
			if (e instanceof Way) {
				for (Node n : ((Way) e).getNodes()) {
					if (settings.boundary.containsPoint(((Node) n).getLatitude(), ((Node) n).getLongitude())) {
						return true;
					}
				}
				return false;
			} else if (e instanceof Node) {
				return settings.boundary.containsPoint(((Node) e).getLatitude(), ((Node) e).getLongitude());
			}
		}
		return true;
	}

	private OsmDbCreator extractOsmToNodesDB(OsmDbAccessor accessor, File readFile, IProgress progress,
			IOsmStorageFilter addFilter, int idSourceMapInd, int idShift,
			boolean generateNewIds, OsmDbCreator previous) throws IOException, SQLException, XmlPullParserException {
		boolean pbfFile = false;
		InputStream stream = new BufferedInputStream(new FileInputStream(readFile), 8192 * 4);
		InputStream streamFile = stream;
		long st = System.currentTimeMillis();
		if (readFile.getName().endsWith(".bz2")) { //$NON-NLS-1$
			stream = new BZip2CompressorInputStream(stream);
		} else if (readFile.getName().endsWith(".gz")) { //$NON-NLS-1$
			stream = new GZIPInputStream(stream);
		} else if (readFile.getName().endsWith(".pbf")) { //$NON-NLS-1$
			pbfFile = true;
		}

		OsmBaseStorage storage = pbfFile ? new OsmBaseStoragePbf() : new OsmBaseStorage();
		storage.setSupressWarnings(settings.suppressWarningsForDuplicateIds);
		if (addFilter != null) {
			storage.getFilters().add(addFilter);
		}

		storage.getFilters().add(new IOsmStorageFilter() {

			@Override
			public boolean acceptEntityToLoad(OsmBaseStorage storage, EntityId entityId, Entity entity) {
				if (indexAddressCreator != null && entityId.getType() == EntityType.NODE) {
					Node n = (Node) entity;
					if (!generateNewIds) {
						n = new Node(n, n.getId() << ObfConstants.SHIFT_ID);
					}
					indexAddressCreator.registerCityNodes(n);
				}
				// accept to allow db creator parse it
				return true;
			}
		});

		// 1. Loading osm file
		OsmDbCreator dbCreator = generateNewIds ? new OsmDbCreator(idSourceMapInd, idShift) : new OsmDbCreator();
		if (!this.settings.ignorePropagate) {
			dbCreator.setPropagateToNodes(propagateToNodes);
		}
		accessor.setCreator(dbCreator);

		try {
			setGeneralProgress(progress, "[15 / 100]"); //$NON-NLS-1$
			progress.startTask(settings.getString("IndexCreator.LOADING_FILE") + readFile.getAbsolutePath(), -1); //$NON-NLS-1$
			// 1 init database to store temporary data
			dbCreator.initDatabase(osmDBdialect, accessor.getDbConn(), idSourceMapInd == 0, previous);
			storage.getFilters().add(dbCreator);
			if (pbfFile) {
				((OsmBaseStoragePbf) storage).parseOSMPbf(stream, progress, false);
			} else {
				storage.parseOSM(stream, progress, streamFile, false);
			}
			dbCreator.finishLoading();
			osmDBdialect.commitDatabase(accessor.getDbConn());

			if (log.isInfoEnabled()) {
				log.info("File parsed : " + (System.currentTimeMillis() - st)); //$NON-NLS-1$
			}
			progress.finishTask();
			return dbCreator;
		} finally {
			if (log.isInfoEnabled()) {
				log.info("File indexed : " + (System.currentTimeMillis() - st)); //$NON-NLS-1$
			}
		}
	}

	private OsmDbAccessor initDbAccessor(File[] readFile, IProgress progress, IOsmStorageFilter addFilter,
			boolean generateUniqueIdsForEachFile) throws IOException, SQLException, InterruptedException, XmlPullParserException {
		OsmDbAccessor accessor = new OsmDbAccessor();
		if (settings.wikidataMappingUrl != null) {
			accessor.setTagsPrepration(new MissingWikiTagsProcessor(settings.wikidataMappingUrl, settings.wikirankingMappingUrl));
		} else if (!Algorithms.isEmpty(System.getenv("WIKIDATA_MAPPING_URL"))) {
			accessor.setTagsPrepration(new MissingWikiTagsProcessor(System.getenv("WIKIDATA_MAPPING_URL"), System.getenv("WIKIRANKING_MAPPING_URL")));
		} else {
			log.info("Not using wikidata database to map missing wikidata tags");
		}
		if (dbFile == null) {
			dbFile = new File(workingDir, TEMP_NODES_DB);
		}
		if (osmDBdialect.databaseFileExists(dbFile)) {
			osmDBdialect.removeDatabase(dbFile);
		}

		Connection dbConn = (Connection) getDatabaseConnection(dbFile.getAbsolutePath(), osmDBdialect);
		accessor.setDbConn(dbConn, osmDBdialect);
		OsmDbCreator dbCreator = null;
		int idShift = readFile.length < 16 ? 4 : (readFile.length < 64 ? 6 : 11);
		if (readFile.length > (1 << 11)) {
			throw new UnsupportedOperationException();
		}
		int idSourceMapInd = 0;
		for (File read : readFile) {
			dbCreator = extractOsmToNodesDB(accessor, read, progress, addFilter, idSourceMapInd, idShift, generateUniqueIdsForEachFile, null);
			accessor.updateCounts(dbCreator);
			if (readFile.length > 1) {
				log.info("Processing " + (idSourceMapInd + 1) + " file out of " + readFile.length);
			}
			idSourceMapInd++;
		}
		osmDBdialect.commitDatabase(dbConn);
		accessor.initDatabase();
		return accessor;
	}

	private void createDatabaseIndexesStructure() throws SQLException, IOException {
		// 2.1 create temporary sqlite database to put temporary results to it
		mapFile = new File(workingDir, getMapFileName());
		// to save space
		mapFile.getParentFile().mkdirs();
		File tempDBMapFile = new File(workingDir, getTempMapDBFileName());
		mapIndexDBDialect.removeDatabase(tempDBMapFile);
		mapConnection = (Connection) getDatabaseConnection(tempDBMapFile.getAbsolutePath(), mapIndexDBDialect);
		mapConnection.setAutoCommit(false);

		// 2.2 create rtree map
		if (settings.indexMap) {
			indexMapCreator.createDatabaseStructure(mapConnection, mapIndexDBDialect,
					getRTreeMapIndexNonPackFileName());
		}
		if (settings.indexRouting) {
			indexRouteCreator.createDatabaseStructure(mapConnection, mapIndexDBDialect,
					getRTreeRouteIndexNonPackFileName());
		}
		if (settings.indexAddress) {
			indexAddressCreator.createDatabaseStructure(mapConnection, mapIndexDBDialect);
		}
		if (settings.indexPOI) {
			indexPoiCreator.createDatabaseStructure(getPoiFile());
		}
		if (settings.indexTransport) {
			indexTransportCreator.createDatabaseStructure(mapConnection, mapIndexDBDialect,
					getRTreeTransportStopsFileName());
		}
	}

	public void generateBasemapIndex(boolean mini, IProgress progress, IOsmStorageFilter addFilter, MapZooms mapZooms,
			MapRenderingTypesEncoder renderingTypes, Log logMapDataWarn, String regionName, File... readFiles)
			throws IOException, SQLException, InterruptedException, XmlPullParserException {
		if (logMapDataWarn == null) {
			logMapDataWarn = log;
		}
		if (renderingTypes == null) {
			renderingTypes = new MapRenderingTypesEncoder("basemap");
		}
		if (mapZooms == null) {
			mapZooms = MapZooms.getDefault();
		}
		// clear previous results and setting variables
		try {

			final BasemapProcessor processor = new BasemapProcessor(logMapDataWarn, mapZooms, renderingTypes,
					settings.zoomWaySmoothness);
			final IndexPoiCreator poiCreator = settings.indexPOI ? new IndexPoiCreator(settings, renderingTypes)
					: null;
			if (settings.indexPOI) {
				poiCreator.createDatabaseStructure(getPoiFile());
			}
			OsmDbAccessor accessor = initDbAccessor(readFiles, progress, addFilter, true);
			// 2. Create index connections and index structure

			IndexCreationContext icc = new IndexCreationContext(this, regionName, true);

			setGeneralProgress(progress, "[50 / 100]");
			progress.startTask(settings.getString("IndexCreator.PROCESS_OSM_NODES"), accessor.getAllNodes());
			accessor.iterateOverEntities(progress, EntityType.NODE, new OsmDbVisitor() {
				@Override
				public void iterateEntity(Entity e, OsmDbAccessorContext ctx) throws SQLException {
					calculateRegionTagAndTransliterate(e, icc);
					processor.processEntity(mini, e);
					if (settings.indexPOI) {
						poiCreator.iterateEntity(e, ctx, icc);
					}
				}
			});
			setGeneralProgress(progress, "[70 / 100]");
			progress.startTask(settings.getString("IndexCreator.PROCESS_OSM_WAYS"), accessor.getAllWays());
			accessor.iterateOverEntities(progress, EntityType.WAY, new OsmDbVisitor() {
				@Override
				public void iterateEntity(Entity e, OsmDbAccessorContext ctx) throws SQLException {
					calculateRegionTagAndTransliterate(e, icc);
					processor.processEntity(mini, e);
					if (settings.indexPOI) {
						poiCreator.iterateEntity(e, ctx, icc);
					}
				}
			});
			setGeneralProgress(progress, "[90 / 100]");

			progress.startTask(settings.getString("IndexCreator.PROCESS_OSM_REL"), accessor.getAllRelations());
			accessor.iterateOverEntities(progress, EntityType.RELATION, new OsmDbVisitor() {
				@Override
				public void iterateEntity(Entity e, OsmDbAccessorContext ctx) throws SQLException {
					ctx.loadEntityRelation((Relation) e);
					calculateRegionTagAndTransliterate(e, icc);
					processor.processEntity(mini, e);
				}
			});
			accessor.closeReadingConnection();

			mapFile = new File(workingDir, getMapFileName());
			// to save space
			mapFile.getParentFile().mkdirs();
			if (mapFile.exists()) {
				mapFile.delete();
			}
			mapRAFile = new RandomAccessFile(mapFile, "rw");
			BinaryMapIndexWriter writer = new BinaryMapIndexWriter(mapRAFile,
					lastModifiedDate == null ? System.currentTimeMillis() : lastModifiedDate.longValue());

			setGeneralProgress(progress, "[95 of 100]");
			progress.startTask("Writing map index to binary file...", -1);
			processor.writeBasemapFile(writer, regionName);
			if (settings.indexPOI) {
				poiCreator.writeBinaryPoiIndex(null, writer, regionName, progress);
			}
			progress.finishTask();
			writer.close();
			mapRAFile.close();
			log.info("Finish writing binary file"); //$NON-NLS-1$
		} catch (RuntimeException e) {
			log.error("Log exception", e); //$NON-NLS-1$
			throw e;
		} catch (SQLException e) {
			log.error("Log exception", e); //$NON-NLS-1$
			throw e;
		} catch (IOException e) {
			log.error("Log exception", e); //$NON-NLS-1$
			throw e;
		} catch (XmlPullParserException e) {
			log.error("Log exception", e); //$NON-NLS-1$
			throw e;
		}
	}

	public File generateIndexes(File readFile, IProgress progress, IOsmStorageFilter addFilter, MapZooms mapZooms,
			MapRenderingTypesEncoder renderingTypes, Log logMapDataWarn)
			throws IOException, SQLException, InterruptedException, XmlPullParserException {
		return generateIndexes(new File[] { readFile }, progress, addFilter, mapZooms, renderingTypes, logMapDataWarn, false);
	}

	public File generateIndexes(File[] readFile, IProgress progress, IOsmStorageFilter addFilter, MapZooms mapZooms,
			MapRenderingTypesEncoder renderingTypes, Log logMapDataWarn, boolean generateUniqueIds) throws IOException, SQLException, InterruptedException, XmlPullParserException {
		if (logMapDataWarn == null) {
			logMapDataWarn = log;
		}

		if (mapZooms == null) {
			mapZooms = MapZooms.getDefault();
		}

		// clear previous results and setting variables
		if (readFile != null && readFile.length > 0 && regionName == null) {
			int i = readFile[0].getName().indexOf('.');
			if (i > -1) {
				regionName = Algorithms.capitalizeFirstLetterAndLowercase(readFile[0].getName().substring(0, i));
			}
		}

		IndexCreationContext icc = new IndexCreationContext(this, regionName, false);

		if (renderingTypes == null) {
			renderingTypes = new MapRenderingTypesEncoder(null, regionName);
		}

		this.propagateToNodes = new PropagateToNodes(renderingTypes);
		this.indexTransportCreator = new IndexTransportCreator(settings);
		this.indexPoiCreator = new IndexPoiCreator(settings, renderingTypes);
		this.indexAddressCreator = new IndexAddressCreator(logMapDataWarn, settings);
		this.indexMapCreator = new IndexVectorMapCreator(logMapDataWarn, mapZooms, renderingTypes, settings, propagateToNodes, getLastModifiedDate());
		this.indexRouteCreator = new IndexRouteCreator(renderingTypes, logMapDataWarn, settings, propagateToNodes);
		this.indexRouteRelationCreatorV1 = new IndexRouteRelationCreatorV1(logMapDataWarn, mapZooms, renderingTypes, settings);
		this.indexRouteRelationCreatorV2 = new IndexRouteRelationCreator(indexPoiCreator, indexMapCreator, getLastModifiedDate());

		if (!settings.extraRelations.isEmpty()) {
			for (File inputFile : settings.extraRelations) {
				OsmBaseStorage reader = new OsmBaseStorage();
				InputStream fis = new FileInputStream(inputFile);
				if (inputFile.getName().endsWith(".gz")) {
					fis = new GZIPInputStream(fis);
				} else if (inputFile.getName().endsWith(".bz2")) {
					fis = new BZip2CompressorInputStream(fis);
				}
				reader.parseOSM(fis, progress);
				fis.close();
				indexRouteCreator.indexExtraRelations(reader);
			}
		}

		// Main generation method
		try {
			// ////////////////////////////////////////////////////////////////////////
			// 1. creating nodes db to fast access for all nodes and simply import all relations, ways, nodes to it

			// do not create temp map file and rtree files
			if (recreateOnlyBinaryFile) {
				mapFile = new File(workingDir, getMapFileName());
				File tempDBMapFile = new File(workingDir, getTempMapDBFileName());
				mapConnection = (Connection) getDatabaseConnection(tempDBMapFile.getAbsolutePath(), mapIndexDBDialect);
				mapConnection.setAutoCommit(false);
				try {
					if (settings.indexMap) {
						indexMapCreator.createRTreeFiles(getRTreeMapIndexPackFileName());
					}
					if (settings.indexRouting) {
						indexRouteCreator.createRTreeFiles(getRTreeRouteIndexPackFileName());
					}
					if (settings.indexTransport) {
						indexTransportCreator.createRTreeFile(getRTreeTransportStopsPackFileName());
					}
				} catch (RTreeException e) {
					log.error("Error flushing", e); //$NON-NLS-1$
					throw new IOException(e);
				}
			} else {
				// 2. Create index connections and index structure
				createDatabaseIndexesStructure();
				OsmDbAccessor accessor = initDbAccessor(readFile, progress, addFilter, generateUniqueIds);

				// 3. Processing all entries
				// 3.1 write all cities
				writeAllCities(accessor, progress);
				// 3.2 index address relations
				indexRelations(accessor, progress, icc);
				// 3.3 MAIN iterate over all entities
				iterateMainEntities(accessor, progress, icc);
				accessor.closeReadingConnection();
				// do not delete first db connection
				if (accessor.getDbConn() != null) {
					osmDBdialect.commitDatabase(accessor.getDbConn());
					osmDBdialect.closeDatabase(accessor.getDbConn());
				}
				if (deleteOsmDB) {
					osmDBdialect.removeDatabase(dbFile);
				}

				// 3.4 combine all low level ways and simplify them
				if (settings.indexMap || settings.indexRouting) {
					setGeneralProgress(progress, "[90 / 100]");
					if (settings.indexMap) {
						progress.startTask(settings.getString("IndexCreator.INDEX_LO_LEVEL_WAYS"),
								indexMapCreator.getLowLevelWays());
						indexMapCreator.processingLowLevelWays(progress);
					}
					if (settings.indexRouting) {
						progress.startTask(settings.getString("IndexCreator.INDEX_LO_LEVEL_WAYS"), -1);
						indexRouteCreator.processingLowLevelWays(progress);
					}

				}

				// 4. packing map rtree indexes
				if (settings.indexMap) {
					setGeneralProgress(progress, "[90 / 100]"); //$NON-NLS-1$
					progress.startTask(settings.getString("IndexCreator.PACK_RTREE_MAP"), -1); //$NON-NLS-1$
					indexMapCreator.packRtreeFiles(getRTreeMapIndexNonPackFileName(), getRTreeMapIndexPackFileName());
				}
				if (settings.indexRouting) {
					indexRouteCreator.packRtreeFiles(getRTreeRouteIndexNonPackFileName(),
							getRTreeRouteIndexPackFileName());
				}

				if (settings.indexTransport) {
					setGeneralProgress(progress, "[90 / 100]"); //$NON-NLS-1$
					progress.startTask(settings.getString("IndexCreator.PACK_RTREE_TRANSP"), -1); //$NON-NLS-1$
					indexTransportCreator.packRTree(getRTreeTransportStopsFileName(),
							getRTreeTransportStopsPackFileName());
				}
			}

			// 5. Writing binary file
			if (settings.indexMap || settings.indexAddress || settings.indexTransport || settings.indexPOI
					|| settings.indexRouting) {
				if (mapFile.exists()) {
					mapFile.delete();
				}
				mapRAFile = new RandomAccessFile(mapFile, "rw");
				BinaryMapIndexWriter writer = new BinaryMapIndexWriter(mapRAFile,
						lastModifiedDate == null ? System.currentTimeMillis() : lastModifiedDate.longValue());
				if (settings.indexMap) {
					setGeneralProgress(progress, "[95 of 100]");
					progress.startTask("Writing map index to binary file...", -1);
					indexMapCreator.writeBinaryMapIndex(writer, regionName);
				}
				if (settings.indexRouting) {
					setGeneralProgress(progress, "[95 of 100]");
					progress.startTask("Writing route index to binary file...", -1);
					indexRouteCreator.writeBinaryRouteIndex(mapFile, writer, regionName, settings.generateLowLevel);
				}

				if (settings.indexAddress) {
					setGeneralProgress(progress, "[95 of 100]");
					progress.startTask("Writing address index to binary file...", -1);
					indexAddressCreator.writeBinaryAddressIndex(writer, regionName, progress);
				}

				// Order matters! Write POI after address / routing to allow use geocoding for POI
				if (settings.indexPOI) {
					setGeneralProgress(progress, "[95 of 100]");
					progress.startTask("Writing poi index to binary file...", -1);
					indexPoiCreator.writeBinaryPoiIndex(mapFile, writer, regionName, progress);
				}

				if (settings.indexTransport) {
					setGeneralProgress(progress, "[95 of 100]");
					progress.startTask("Writing transport index to binary file...", -1);
					indexTransportCreator.writeBinaryTransportIndex(writer, regionName, mapConnection);
				}
				progress.finishTask();
				writer.close();
				mapRAFile.close();
				log.info("Finish writing binary file"); //$NON-NLS-1$
			}
		} catch (RuntimeException e) {
			log.error("Log exception", e); //$NON-NLS-1$
			throw e;
		} catch (SQLException e) {
			log.error("Log exception", e); //$NON-NLS-1$
			throw e;
		} catch (IOException e) {
			log.error("Log exception", e); //$NON-NLS-1$
			throw e;
		} catch (XmlPullParserException e) {
			log.error("Log exception", e); //$NON-NLS-1$
			throw e;
		} finally {
			try {
				indexPoiCreator.commitAndClosePoiFile(lastModifiedDate);
				if (REMOVE_POI_DB) {
					indexPoiCreator.removePoiFile();
				}
				indexRouteRelationCreatorV1.closeAllStatements();
				indexRouteRelationCreatorV2.closeAllStatements();
				indexAddressCreator.closeAllPreparedStatements();
				indexTransportCreator.commitAndCloseFiles(getRTreeTransportStopsFileName(),
						getRTreeTransportStopsPackFileName(), deleteDatabaseIndexes);
				indexMapCreator.commitAndCloseFiles(getRTreeMapIndexNonPackFileName(), getRTreeMapIndexPackFileName(),
						deleteDatabaseIndexes);
				indexRouteCreator.commitAndCloseFiles(getRTreeRouteIndexNonPackFileName(),
						getRTreeRouteIndexPackFileName(), deleteDatabaseIndexes);

				if (mapConnection != null) {
					mapConnection.commit();
					mapConnection.close();
					mapConnection = null;
					File tempDBFile = new File(workingDir, getTempMapDBFileName());
					if (mapIndexDBDialect.databaseFileExists(tempDBFile) && deleteDatabaseIndexes) {
						// do not delete it for now
						mapIndexDBDialect.removeDatabase(tempDBFile);
					}
				}

			} catch (SQLException e) {
				e.printStackTrace();
			} catch (RuntimeException e) {
				e.printStackTrace();
			}
		}
		return mapFile;
	}

	private void iterateMainEntities(OsmDbAccessor accessor, IProgress progress, IndexCreationContext icc)
			throws SQLException, InterruptedException {
		setGeneralProgress(progress, "[50 / 100]");
		progress.startTask(settings.getString("IndexCreator.PROCESS_OSM_NODES"), accessor.getAllNodes());
		accessor.iterateOverEntities(progress, EntityType.NODE, new OsmDbVisitor() {
			@Override
			public void iterateEntity(Entity e, OsmDbAccessorContext ctx) throws SQLException {
				iterateMainEntity(e, ctx, icc);
			}
		});
		setGeneralProgress(progress, "[70 / 100]");
		progress.startTask(settings.getString("IndexCreator.PROCESS_OSM_WAYS"), accessor.getAllWays());
		accessor.iterateOverEntities(progress, EntityType.WAY, new OsmDbVisitor() {
			@Override
			public void iterateEntity(Entity e, OsmDbAccessorContext ctx) throws SQLException {
				Way w = (Way) e;
				propagateToNodes.calculateBorderPoints(w);
				iterateMainEntity(e, ctx, icc);
			}
		});
		setGeneralProgress(progress, "[85 / 100]");
		progress.startTask(settings.getString("IndexCreator.PROCESS_OSM_REL"), accessor.getAllRelations());
		accessor.iterateOverEntities(progress, EntityType.RELATION, new OsmDbVisitor() {
			@Override
			public void iterateEntity(Entity e, OsmDbAccessorContext ctx) throws SQLException {
				iterateMainEntity(e, ctx, icc);
			}
		});
	}

	private void indexRelations(OsmDbAccessor accessor, IProgress progress, IndexCreationContext icc)
			throws SQLException, InterruptedException {
		if (settings.indexAddress || settings.indexMap || settings.indexRouting || settings.indexPOI
				|| settings.indexTransport) {
			setGeneralProgress(progress, "[30 / 100]"); //$NON-NLS-1$
			progress.startTask(settings.getString("IndexCreator.PREINDEX_BOUNDARIES_RELATIONS"), //$NON-NLS-1$
					accessor.getAllRelations());
			accessor.iterateOverEntities(progress, EntityType.RELATION, new OsmDbVisitor() {
				@Override
				public void iterateEntity(Entity e, OsmDbAccessorContext ctx) throws SQLException {
					calculateRegionTagAndTransliterate(e, icc);
					if (settings.indexAddress) {
						// indexAddressCreator.indexStreetRelation((Relation) e, ctx); streets needs loaded boundaries first !!!
						indexAddressCreator.indexBoundaries(e, ctx);
					}
					if (settings.indexMap) {
						if (!settings.keepOnlyRouteRelationObjects) {
							indexMapCreator.indexMapRelationsAndMultiPolygons(e, ctx, icc);
						} else {
							indexRouteRelationCreatorV1.iterateRelation(e, ctx, icc);
						}
						if (settings.indexPOI && settings.indexMap) {
							if (settings.indexRouteRelations) {
								indexRouteRelationCreatorV2.iterateRelation((Relation) e, ctx, icc);
							}
						}
					}
					if (settings.indexRouting) {
						indexRouteCreator.indexRelations(e, ctx);
						indexRouteCreator.indexLowEmissionZones(e, ctx);
					}
					if (settings.indexPOI) {
						indexPoiCreator.iterateRelation((Relation) e, ctx);
					}
					if (settings.indexTransport) {
						indexTransportCreator.indexRelations((Relation) e, ctx);
					}
				}
			});
			if (settings.indexMap) {
				indexMapCreator.createMapIndexTableIndexes(mapConnection);
			}
			if (settings.indexAddress || settings.indexRouting) {
				setGeneralProgress(progress, "[40 / 100]"); //$NON-NLS-1$
				progress.startTask(settings.getString("IndexCreator.PREINDEX_BOUNDARIES_WAYS"), accessor.getAllWays()); //$NON-NLS-1$
				accessor.iterateOverEntities(progress, EntityType.WAY_BOUNDARY, new OsmDbVisitor() {
					@Override
					public void iterateEntity(Entity e, OsmDbAccessorContext ctx) throws SQLException {
						if (settings.indexAddress) {
							indexAddressCreator.indexBoundaries(e, ctx);
						}
						if (settings.indexRouting) {
							indexRouteCreator.indexLowEmissionZones(e, ctx);
						}
					}
				});
			}

			if (settings.indexAddress) {
				setGeneralProgress(progress, "[42 / 100]"); //$NON-NLS-1$
				progress.startTask(settings.getString("IndexCreator.BIND_CITIES_AND_BOUNDARIES"), 100); //$NON-NLS-1$
				// finish up the boundaries and cities
				indexAddressCreator.tryToAssignBoundaryToFreeCities(progress);

				setGeneralProgress(progress, "[45 / 100]"); //$NON-NLS-1$
				progress.startTask(settings.getString("IndexCreator.PREINDEX_ADRESS_MAP"), accessor.getAllRelations()); //$NON-NLS-1$
				accessor.iterateOverEntities(progress, EntityType.RELATION, new OsmDbVisitor() {
					@Override
					public void iterateEntity(Entity e, OsmDbAccessorContext ctx) throws SQLException {
						indexAddressCreator.indexStreetRelation((Relation) e, ctx, icc);
					}
				});

				indexAddressCreator.commitToPutAllCities();
			}

			if (settings.indexAddress && settings.indexPOI) {
				indexPoiCreator.storeCities(indexAddressCreator.getCityDataStorage());
			}
		}
	}

	private void writeAllCities(OsmDbAccessor accessor, IProgress progress) throws SQLException, InterruptedException {
		if (settings.indexAddress) {
			setGeneralProgress(progress, "[20 / 100]"); //$NON-NLS-1$
			progress.startTask(settings.getString("IndexCreator.INDEX_CITIES"), accessor.getAllNodes()); //$NON-NLS-1$
			indexAddressCreator.writeCitiesIntoDb();
		}
	}

	private void setGeneralProgress(IProgress progress, String genProgress) {
		progress.setGeneralProgress(genProgress);
	}

	public static void main(String[] args)
			throws IOException, SQLException, InterruptedException, XmlPullParserException {
		long time = System.currentTimeMillis();

		// if(true){ generateRegionsFile(); return;}
		String rootFolder = System.getProperty("maps.dir");
		IndexCreatorSettings settings = new IndexCreatorSettings();
		// settings.poiZipLongStrings = true;
//		settings.indexMap = true;
		settings.indexAddress = true;
		settings.indexPOI = true;
		// settings.indexTransport = true;
//		settings.indexRouting = true;
		// settings.keepOnlySeaObjects = true;
		// settings.srtmDataFolder = new File(rootFolder + "/maps/srtm/");
		// settings.gtfsData = new File(rootFolder + "/maps/transport/Netherlands.sqlite");
		settings.srtmDataFolderUrl  = null;

		// settings.zoomWaySmoothness = 2;

		IndexCreator creator = new IndexCreator(new File(rootFolder), settings); //$NON-NLS-1$

		// creator.deleteDatabaseIndexes = false;
		// creator.recreateOnlyBinaryFile = true;
		// creator.deleteOsmDB = false;

		MapZooms zooms = MapZooms.getDefault(); // MapZooms.parseZooms("15-");

		String file = rootFolder + "../temp/us_penn.osm";
//		String file = rootFolder + "../temp/london.osm";
//		String file = rootFolder + "../temp/andorra_europe.pbf";
//		String file = rootFolder + "../temp/Routing_test_76.osm";
//		String file = rootFolder + "../repos/resources/test-resources/alarm.osm";
		// String file = rootFolder + "../repos/resources/test-resources/turn_lanes_test.osm";
//		String file = rootFolder + "/maps/routes/nl_routes.osm.gz";

//		settings.keepOnlyRouteRelationObjects = true;
		int st = file.lastIndexOf('/');
		int e = file.indexOf('.', st);
		String name = file.substring(st, e);


//		creator.setMapFileName(name + ".travel.obf");

		creator.setNodesDBFile(new File(rootFolder + name + ".tmp.odb"));

		MapPoiTypes.setDefault(new MapPoiTypes(rootFolder + "../repos/resources/poi/poi_types.xml"));
		MapRenderingTypesEncoder rt = new MapRenderingTypesEncoder(
				rootFolder + "../repos/resources/obf_creation/rendering_types.xml", new File(file).getName());
		// creator.setLastModifiedDate(1504224000000l);
		creator.generateIndexes(new File(file), new ConsoleProgressImplementation(1), null, zooms, rt, log);
		// new File(file),

		log.info("WHOLE GENERATION TIME :  " + (System.currentTimeMillis() - time)); //$NON-NLS-1$
		log.info("COORDINATES_SIZE " + BinaryMapIndexWriter.COORDINATES_SIZE + " count " //$NON-NLS-1$ //$NON-NLS-2$
				+ BinaryMapIndexWriter.COORDINATES_COUNT);
		log.info("LABEL_COORDINATES_SIZE " + BinaryMapIndexWriter.LABEL_COORDINATES_SIZE); //$NON-NLS-1$
		log.info("TYPES_SIZE " + BinaryMapIndexWriter.TYPES_SIZE); //$NON-NLS-1$
		log.info("ID_SIZE " + BinaryMapIndexWriter.ID_SIZE); //$NON-NLS-1$
		log.info("- COORD_TYPES_ID SIZE " + (BinaryMapIndexWriter.COORDINATES_SIZE + BinaryMapIndexWriter.TYPES_SIZE //$NON-NLS-1$
				+ BinaryMapIndexWriter.ID_SIZE));
		log.info("- MAP_DATA_SIZE " + BinaryMapIndexWriter.MAP_DATA_SIZE); //$NON-NLS-1$
		log.info("- STRING_TABLE_SIZE " + BinaryMapIndexWriter.STRING_TABLE_SIZE); //$NON-NLS-1$
		log.info("-- MAP_DATA_AND_STRINGS SIZE " //$NON-NLS-1$
				+ (BinaryMapIndexWriter.MAP_DATA_SIZE + BinaryMapIndexWriter.STRING_TABLE_SIZE));

	}

	public static void generateRegionsFile()
			throws IOException, SQLException, InterruptedException, XmlPullParserException {
		MapRenderingTypesEncoder rt = new MapRenderingTypesEncoder("regions");
		String file = "/home/victor/projects/osmand/repo/resources/osmand_regions.osm";
		String folder = "/home/victor/projects/osmand/repo/resources/countries-info/";
		IndexCreatorSettings settings = new IndexCreatorSettings();
		settings.indexMap = true;
		settings.indexAddress = false;
		settings.indexPOI = false;
		settings.indexTransport = false;
		settings.indexRouting = false;
		settings.zoomWaySmoothness = 1;
		IndexCreator creator = new IndexCreator(new File(folder), settings); // $NON-NLS-1$

		creator.setMapFileName("regions.ocbf");

		MapZooms zooms = MapZooms.parseZooms("5-6");
		int st = file.lastIndexOf('/');
		int e = file.indexOf('.', st);
		creator.setNodesDBFile(new File(folder + file.substring(st, e) + ".tmp.odb"));
		creator.generateIndexes(new File(file), new ConsoleProgressImplementation(1), null, zooms, rt, log);
	}

}
