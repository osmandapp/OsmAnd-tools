package net.osmand.data.preparation;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import net.osmand.IProgress;
import net.osmand.IndexConstants;
import net.osmand.data.preparation.OsmDbAccessor.OsmDbVisitor;
import net.osmand.data.preparation.address.IndexAddressCreator;
import net.osmand.impl.ConsoleProgressImplementation;
import net.osmand.osm.MapRenderingTypes;
import net.osmand.osm.MapRenderingTypesEncoder;
import net.osmand.osm.edit.Entity;
import net.osmand.osm.edit.Relation;
import net.osmand.osm.edit.Entity.EntityId;
import net.osmand.osm.edit.Entity.EntityType;
import net.osmand.osm.io.IOsmStorageFilter;
import net.osmand.osm.io.OsmBaseStorage;
import net.osmand.osm.io.OsmBaseStoragePbf;
import net.osmand.swing.DataExtractionSettings;
import net.osmand.swing.Messages;
import net.osmand.swing.ProgressDialog;
import net.osmand.util.Algorithms;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tools.bzip2.CBZip2InputStream;
import org.xml.sax.SAXException;

import rtree.RTreeException;

/**
 * http://wiki.openstreetmap.org/wiki/OSM_tags_for_routing#Is_inside.2Foutside
 * http://wiki.openstreetmap.org/wiki/Relations/Proposed/Postal_Addresses
 * http://wiki.openstreetmap.org/wiki/Proposed_features/House_numbers/Karlsruhe_Schema#Tags (node, way)
 * 
 * That data extraction has aim, save runtime memory and generate indexes on the fly. It will be longer than load in memory (needed part)
 * and save into index.
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

	private boolean indexMap;
	private boolean indexPOI;
	private boolean indexTransport;
	private boolean indexAddress;
	private boolean indexRouting = true;

	private boolean normalizeStreets = true; // true by default
	private int zoomWaySmothness = 2;

	private String regionName;
	private String mapFileName = null;
	private Long lastModifiedDate = null;
	
	
	private IndexTransportCreator indexTransportCreator;
	private IndexPoiCreator indexPoiCreator;
	private IndexAddressCreator indexAddressCreator;
	private IndexVectorMapCreator indexMapCreator;
	private IndexRouteCreator indexRouteCreator;
	private OsmDbAccessor accessor;
	// constants to start process from the middle and save temporary results
	private boolean recreateOnlyBinaryFile = false; // false;
	private boolean deleteOsmDB = true;
	private boolean deleteDatabaseIndexes = true;

	private Object dbConn;
	private File dbFile;

	private File mapFile;
	private RandomAccessFile mapRAFile;
	private Connection mapConnection;

	public static final int DEFAULT_CITY_ADMIN_LEVEL = 8;
	private String cityAdminLevel = "" + DEFAULT_CITY_ADMIN_LEVEL;


	public IndexCreator(File workingDir) {
		this.workingDir = workingDir;
	}

	public void setIndexAddress(boolean indexAddress) {
		this.indexAddress = indexAddress;
	}
	
	public void setIndexRouting(boolean indexRouting) {
		this.indexRouting = indexRouting;
	}

	public void setIndexMap(boolean indexMap) {
		this.indexMap = indexMap;
	}

	public void setIndexPOI(boolean indexPOI) {
		this.indexPOI = indexPOI;
	}

	public void setIndexTransport(boolean indexTransport) {
		this.indexTransport = indexTransport;
	}

	public void setNormalizeStreets(boolean normalizeStreets) {
		this.normalizeStreets = normalizeStreets;
	}
	
	public void setZoomWaySmothness(int zoomWaySmothness) {
		this.zoomWaySmothness = zoomWaySmothness;
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
		if(osmDBdialect != null) {
			this.osmDBdialect = osmDBdialect;
		}
		if(mapIndexDBDialect != null) {
			this.mapIndexDBDialect = mapIndexDBDialect;
		}
	}

	public Long getLastModifiedDate() {
		return lastModifiedDate;
	}

	public void setLastModifiedDate(Long lastModifiedDate) {
		this.lastModifiedDate = lastModifiedDate;
	}

	public String getPoiFileName() {
		return getRegionName() + IndexConstants.POI_INDEX_EXT;
	}
	
	public String getCityAdminLevel() {
		return cityAdminLevel;
	}
	
	public void setCityAdminLevel(String cityAdminLevel) {
		this.cityAdminLevel = cityAdminLevel;
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
	
	private void iterateMainEntity(Entity e, OsmDbAccessorContext ctx) throws SQLException {
		if (indexPOI) {
			indexPoiCreator.iterateEntity(e, ctx);
		}
		if (indexTransport) {
			indexTransportCreator.visitEntityMainStep(e, ctx);
		}
		if (indexMap) {
			indexMapCreator.iterateMainEntity(e, ctx);
		}
		if (indexAddress) {
			indexAddressCreator.iterateMainEntity(e, ctx);
		}
		if (indexRouting) {
			indexRouteCreator.iterateMainEntity(e, ctx);
		}
	}

	private OsmDbCreator extractOsmToNodesDB(File readFile, IProgress progress, IOsmStorageFilter addFilter) throws FileNotFoundException,
			IOException, SQLException, SAXException {
		boolean pbfFile = false;
		InputStream stream = new BufferedInputStream(new FileInputStream(readFile), 8192 * 4);
		InputStream streamFile = stream;
		long st = System.currentTimeMillis();
		if (readFile.getName().endsWith(".bz2")) { //$NON-NLS-1$
			if (stream.read() != 'B' || stream.read() != 'Z') {
//				throw new RuntimeException("The source stream must start with the characters BZ if it is to be read as a BZip2 stream."); //$NON-NLS-1$
			} else {
				stream = new CBZip2InputStream(stream);
			}
		} else if (readFile.getName().endsWith(".pbf")) { //$NON-NLS-1$
			pbfFile = true;
		}

		OsmBaseStorage storage = pbfFile? new OsmBaseStoragePbf() : new OsmBaseStorage();
		storage.setSupressWarnings(DataExtractionSettings.getSettings().isSupressWarningsForDuplicatedId());
		if (addFilter != null) {
			storage.getFilters().add(addFilter);
		}
		
		storage.getFilters().add(new IOsmStorageFilter() {
			
			@Override
			public boolean acceptEntityToLoad(OsmBaseStorage storage, EntityId entityId, Entity entity) {
				if(indexAddressCreator != null) {
					indexAddressCreator.registerCityIfNeeded(entity);
				}
				// accept to allow db creator parse it
				return true;
			}
		});

		// 1. Loading osm file
		OsmDbCreator dbCreator = new OsmDbCreator(this);
		try {
			setGeneralProgress(progress,"[15 / 100]"); //$NON-NLS-1$
			progress.startTask(Messages.getString("IndexCreator.LOADING_FILE") + readFile.getAbsolutePath(), -1); //$NON-NLS-1$
			// 1 init database to store temporary data
			dbCreator.initDatabase(osmDBdialect, dbConn);
			storage.getFilters().add(dbCreator);
			if (pbfFile) {
				((OsmBaseStoragePbf) storage).parseOSMPbf(stream, progress, false);
			} else {
				storage.parseOSM(stream, progress, streamFile, false);
			}
			dbCreator.finishLoading();
			osmDBdialect.commitDatabase(dbConn);

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
	
	private boolean createPlainOsmDb(IProgress progress, File readFile, IOsmStorageFilter addFilter, boolean deletePrevious) throws SQLException, FileNotFoundException, IOException, SAXException{
//		dbFile = new File(workingDir, TEMP_NODES_DB);
		// initialize db file
		boolean loadFromExistingFile = dbFile != null && osmDBdialect.databaseFileExists(dbFile) && !deletePrevious;
		if (dbFile == null || deletePrevious) {
			dbFile = new File(workingDir, TEMP_NODES_DB);
			// to save space
			if (osmDBdialect.databaseFileExists(dbFile)) {
				osmDBdialect.removeDatabase(dbFile);
			}
		}
		dbConn = getDatabaseConnection(dbFile.getAbsolutePath(), osmDBdialect);
		int allRelations = 100000;
		int allWays = 1000000;
		int allNodes = 10000000;
		if (!loadFromExistingFile) {
			OsmDbCreator dbCreator = extractOsmToNodesDB(readFile, progress, addFilter);
			if (dbCreator != null) {
				allNodes = dbCreator.getAllNodes();
				allWays = dbCreator.getAllWays();
				allRelations = dbCreator.getAllRelations();
			}
		} else {
			if (DBDialect.NOSQL != osmDBdialect) {
				Connection dbc = (Connection) dbConn;
				final Statement stmt = dbc.createStatement();
				accessor.computeRealCounts(stmt);
				allRelations = accessor.getAllRelations();
				allNodes = accessor.getAllNodes();
				allWays = accessor.getAllWays();
				stmt.close();
			}
		}
		accessor.initDatabase(dbConn, osmDBdialect, allNodes, allWays, allRelations);
		return loadFromExistingFile;
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
		if (indexMap) {
			indexMapCreator.createDatabaseStructure(mapConnection, mapIndexDBDialect, getRTreeMapIndexNonPackFileName());
		}
		if (indexRouting) {
			indexRouteCreator.createDatabaseStructure(mapConnection, mapIndexDBDialect, getRTreeRouteIndexNonPackFileName());
		}
		if (indexAddress) {
			indexAddressCreator.createDatabaseStructure(mapConnection, mapIndexDBDialect);
		}
		if (indexPOI) {
			indexPoiCreator.createDatabaseStructure(new File(workingDir, getPoiFileName()));
		}
		if (indexTransport) {
			indexTransportCreator.createDatabaseStructure(mapConnection, mapIndexDBDialect, getRTreeTransportStopsFileName());
		}
	}
	
	public void generateBasemapIndex(IProgress progress, IOsmStorageFilter addFilter, MapZooms mapZooms,
			MapRenderingTypesEncoder renderingTypes, Log logMapDataWarn, String regionName, File... readFiles) throws IOException, SAXException, SQLException, InterruptedException {
		if (logMapDataWarn == null) {
			logMapDataWarn = log;
		}
		if (logMapDataWarn == null) {
			logMapDataWarn = log;
		}

		if (renderingTypes == null) {
			renderingTypes = MapRenderingTypesEncoder.getDefault();
		}
		if (mapZooms == null) {
			mapZooms = MapZooms.getDefault();
		}

		// clear previous results and setting variables
		try {
			
			final BasemapProcessor processor = new BasemapProcessor(logMapDataWarn, mapZooms, renderingTypes, zoomWaySmothness);
			
			for (File readFile : readFiles) {
				this.accessor = new OsmDbAccessor();
				createPlainOsmDb(progress, readFile, addFilter, true);
				// 2. Create index connections and index structure

				setGeneralProgress(progress,"[50 / 100]");
				progress.startTask(Messages.getString("IndexCreator.PROCESS_OSM_NODES"), accessor.getAllNodes());
				accessor.iterateOverEntities(progress, EntityType.NODE, new OsmDbVisitor() {
					@Override
					public void iterateEntity(Entity e, OsmDbAccessorContext ctx) throws SQLException {
						processor.processEntity(e);
					}
				});
				setGeneralProgress(progress,"[70 / 100]");
				progress.startTask(Messages.getString("IndexCreator.PROCESS_OSM_WAYS"), accessor.getAllWays());
				accessor.iterateOverEntities(progress, EntityType.WAY, new OsmDbVisitor() {
					@Override
					public void iterateEntity(Entity e, OsmDbAccessorContext ctx) throws SQLException {
						processor.processEntity(e);
					}
				});
				accessor.closeReadingConnection();
			}
			
			
			mapFile = new File(workingDir, getMapFileName());
			// to save space
			mapFile.getParentFile().mkdirs();
			if (mapFile.exists()) {
				mapFile.delete();
			}
			mapRAFile = new RandomAccessFile(mapFile, "rw");
			BinaryMapIndexWriter writer = new BinaryMapIndexWriter(mapRAFile);

			setGeneralProgress(progress,"[95 of 100]");
			progress.startTask("Writing map index to binary file...", -1);
			processor.writeBasemapFile(writer, regionName);
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
		} catch (SAXException e) {
			log.error("Log exception", e); //$NON-NLS-1$
			throw e;
		}
	}
	
	public void generateIndexes(File readFile, IProgress progress, IOsmStorageFilter addFilter, MapZooms mapZooms,
			MapRenderingTypesEncoder renderingTypes, Log logMapDataWarn) throws IOException, SAXException, SQLException, InterruptedException {
//		if(LevelDBAccess.load()){
//			dialect = DBDialect.NOSQL;
//		}
		if(logMapDataWarn == null) {
			logMapDataWarn = log ;
		}
		
		if (renderingTypes == null) {
			renderingTypes = MapRenderingTypesEncoder.getDefault();
		}
		if (mapZooms == null) {
			mapZooms = MapZooms.getDefault();
		}

		// clear previous results and setting variables
		if (readFile != null && regionName == null) {
			int i = readFile.getName().indexOf('.');
			if (i > -1) {
				regionName = Algorithms.capitalizeFirstLetterAndLowercase(readFile.getName().substring(0, i));
			}
		}
		this.indexTransportCreator = new IndexTransportCreator();
		this.indexPoiCreator = new IndexPoiCreator(renderingTypes);
		this.indexAddressCreator = new IndexAddressCreator(logMapDataWarn);
		this.indexMapCreator = new IndexVectorMapCreator(logMapDataWarn, mapZooms, renderingTypes, zoomWaySmothness);
		this.indexRouteCreator = new IndexRouteCreator(renderingTypes, logMapDataWarn);
		this.accessor = new OsmDbAccessor();

		// init address
		String[] normalizeDefaultSuffixes = null;
		String[] normalizeSuffixes = null;
		if (normalizeStreets) {
			normalizeDefaultSuffixes = DataExtractionSettings.getSettings().getDefaultSuffixesToNormalizeStreets();
			normalizeSuffixes = DataExtractionSettings.getSettings().getSuffixesToNormalizeStreets();
		}
		indexAddressCreator.initSettings(normalizeStreets, normalizeDefaultSuffixes, normalizeSuffixes, cityAdminLevel);

		// Main generation method
		try {
			// ////////////////////////////////////////////////////////////////////////
			// 1. creating nodes db to fast access for all nodes and simply import all relations, ways, nodes to it
			boolean loadFromExistingFile = createPlainOsmDb(progress, readFile, addFilter, false);
			
			// do not create temp map file and rtree files
			if (recreateOnlyBinaryFile) {
				mapFile = new File(workingDir, getMapFileName());
				File tempDBMapFile = new File(workingDir, getTempMapDBFileName());
				mapConnection = (Connection) getDatabaseConnection(tempDBMapFile.getAbsolutePath(), mapIndexDBDialect);
				mapConnection.setAutoCommit(false);
				try {
					if (indexMap) {
						indexMapCreator.createRTreeFiles(getRTreeMapIndexPackFileName());
					}
					if (indexRouting) {
						indexRouteCreator.createRTreeFiles(getRTreeRouteIndexPackFileName());
					}
					if (indexTransport) {
						indexTransportCreator.createRTreeFile(getRTreeTransportStopsPackFileName());
					}
				} catch (RTreeException e) {
					log.error("Error flushing", e); //$NON-NLS-1$
					throw new IOException(e);
				}
			} else {

				// 2. Create index connections and index structure
				createDatabaseIndexesStructure();

				// 3. Processing all entries
				
				// 3.1 write all cities
				if (indexAddress) {
					setGeneralProgress(progress,"[20 / 100]"); //$NON-NLS-1$
					progress.startTask(Messages.getString("IndexCreator.INDEX_CITIES"), accessor.getAllNodes()); //$NON-NLS-1$
					if (loadFromExistingFile) {
						// load cities names
						accessor.iterateOverEntities(progress, EntityType.NODE,  new OsmDbVisitor() {
							@Override
							public void iterateEntity(Entity e, OsmDbAccessorContext ctx) {
								indexAddressCreator.registerCityIfNeeded(e);
							}
						});
					}
					indexAddressCreator.writeCitiesIntoDb();
				}

				// 3.2 index address relations
				if (indexAddress || indexMap || indexRouting || indexPOI) {
					setGeneralProgress(progress,"[30 / 100]"); //$NON-NLS-1$
					progress.startTask(Messages.getString("IndexCreator.PREINDEX_BOUNDARIES_RELATIONS"), accessor.getAllRelations()); //$NON-NLS-1$
					accessor.iterateOverEntities(progress, EntityType.RELATION, new OsmDbVisitor() {
						@Override
						public void iterateEntity(Entity e, OsmDbAccessorContext ctx) throws SQLException {
							if (indexAddress) {
								//indexAddressCreator.indexAddressRelation((Relation) e, ctx); streets needs loaded boundaries !!!
								indexAddressCreator.indexBoundariesRelation(e, ctx);
							}
							if (indexMap) {
								indexMapCreator.indexMapRelationsAndMultiPolygons(e, ctx);
							}
							if (indexRouting) {
								indexRouteCreator.indexRelations(e, ctx);
							}
							if (indexPOI) {
								indexPoiCreator.iterateRelation((Relation) e, ctx);
							}
							if (indexTransport) {
								indexTransportCreator.indexRelations((Relation) e, ctx);
							}
						}
					});
					if (indexAddress) {
						setGeneralProgress(progress,"[40 / 100]"); //$NON-NLS-1$
						progress.startTask(Messages.getString("IndexCreator.PREINDEX_BOUNDARIES_WAYS"), accessor.getAllWays()); //$NON-NLS-1$
						accessor.iterateOverEntities(progress, EntityType.WAY_BOUNDARY, new OsmDbVisitor() {
							@Override
							public void iterateEntity(Entity e, OsmDbAccessorContext ctx) throws SQLException {
								indexAddressCreator.indexBoundariesRelation(e, ctx);
							}
						});

						setGeneralProgress(progress,"[42 / 100]"); //$NON-NLS-1$
						progress.startTask(Messages.getString("IndexCreator.BIND_CITIES_AND_BOUNDARIES"), 100); //$NON-NLS-1$
						//finish up the boundaries and cities
						indexAddressCreator.tryToAssignBoundaryToFreeCities(progress);
						
						setGeneralProgress(progress,"[45 / 100]"); //$NON-NLS-1$
						progress.startTask(Messages.getString("IndexCreator.PREINDEX_ADRESS_MAP"), accessor.getAllRelations()); //$NON-NLS-1$
						accessor.iterateOverEntities(progress, EntityType.RELATION, new OsmDbVisitor() {
							@Override
							public void iterateEntity(Entity e, OsmDbAccessorContext ctx) throws SQLException {
								indexAddressCreator.indexAddressRelation((Relation) e, ctx);
							}
						});
						
						indexAddressCreator.commitToPutAllCities();
					}
				}

				// 3.3 MAIN iterate over all entities
				setGeneralProgress(progress,"[50 / 100]");
				progress.startTask(Messages.getString("IndexCreator.PROCESS_OSM_NODES"), accessor.getAllNodes());
				accessor.iterateOverEntities(progress, EntityType.NODE, new OsmDbVisitor() {
					@Override
					public void iterateEntity(Entity e, OsmDbAccessorContext ctx) throws SQLException {
						iterateMainEntity(e, ctx);
					}
				});
				setGeneralProgress(progress,"[70 / 100]");
				progress.startTask(Messages.getString("IndexCreator.PROCESS_OSM_WAYS"), accessor.getAllWays());
				accessor.iterateOverEntities(progress, EntityType.WAY, new OsmDbVisitor() {
					@Override
					public void iterateEntity(Entity e, OsmDbAccessorContext ctx) throws SQLException {
						iterateMainEntity(e, ctx);
					}
				});
				setGeneralProgress(progress,"[85 / 100]");
				progress.startTask(Messages.getString("IndexCreator.PROCESS_OSM_REL"), accessor.getAllRelations());
				accessor.iterateOverEntities(progress, EntityType.RELATION, new OsmDbVisitor() {
					@Override
					public void iterateEntity(Entity e, OsmDbAccessorContext ctx) throws SQLException {
						iterateMainEntity(e, ctx);
					}
				});

				// 3.4 combine all low level ways and simplify them
				if (indexMap || indexRouting) {
					setGeneralProgress(progress,"[90 / 100]");
					if(indexMap) {
						progress.startTask(Messages.getString("IndexCreator.INDEX_LO_LEVEL_WAYS"), indexMapCreator.getLowLevelWays());
						indexMapCreator.processingLowLevelWays(progress);
					}
					if(indexRouting) {
						progress.startTask(Messages.getString("IndexCreator.INDEX_LO_LEVEL_WAYS"), -1);
						indexRouteCreator.processingLowLevelWays(progress);
					}
					
				}

				// 3.5 update all postal codes from relations
				if (indexAddress) {
					setGeneralProgress(progress,"[90 / 100]");
					progress.startTask(Messages.getString("IndexCreator.REGISTER_PCODES"), -1);
					indexAddressCreator.processingPostcodes();
				}

				// 4. packing map rtree indexes
				if (indexMap) {
					setGeneralProgress(progress,"[90 / 100]"); //$NON-NLS-1$
					progress.startTask(Messages.getString("IndexCreator.PACK_RTREE_MAP"), -1); //$NON-NLS-1$
					indexMapCreator.packRtreeFiles(getRTreeMapIndexNonPackFileName(), getRTreeMapIndexPackFileName());
				}
				if(indexRouting) {
					indexRouteCreator.packRtreeFiles(getRTreeRouteIndexNonPackFileName(), getRTreeRouteIndexPackFileName());
				}

				if (indexTransport) {
					setGeneralProgress(progress,"[90 / 100]"); //$NON-NLS-1$
					progress.startTask(Messages.getString("IndexCreator.PACK_RTREE_TRANSP"), -1); //$NON-NLS-1$
					indexTransportCreator.packRTree(getRTreeTransportStopsFileName(), getRTreeTransportStopsPackFileName());
				}
			}

			// 5. Writing binary file
			if (indexMap || indexAddress || indexTransport || indexPOI || indexRouting) {
				if (mapFile.exists()) {
					mapFile.delete();
				}
				mapRAFile = new RandomAccessFile(mapFile, "rw");
				BinaryMapIndexWriter writer = new BinaryMapIndexWriter(mapRAFile);
				if (indexMap) {
					setGeneralProgress(progress,"[95 of 100]");
					progress.startTask("Writing map index to binary file...", -1);
					indexMapCreator.writeBinaryMapIndex(writer, regionName);
				}
				if (indexRouting) {
					setGeneralProgress(progress,"[95 of 100]");
					progress.startTask("Writing route index to binary file...", -1);
					indexRouteCreator.writeBinaryRouteIndex(writer, regionName);
				}

				if (indexAddress) {
					setGeneralProgress(progress,"[95 of 100]");
					progress.startTask("Writing address index to binary file...", -1);
					indexAddressCreator.writeBinaryAddressIndex(writer, regionName, progress);
				}
				
				if (indexPOI) {
					setGeneralProgress(progress,"[95 of 100]");
					progress.startTask("Writing poi index to binary file...", -1);
					indexPoiCreator.writeBinaryPoiIndex(writer, regionName, progress);
				}

				if (indexTransport) {
					setGeneralProgress(progress,"[95 of 100]");
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
		} catch (SAXException e) {
			log.error("Log exception", e); //$NON-NLS-1$
			throw e;
		} finally {
			try {
				accessor.closeReadingConnection();

				indexPoiCreator.commitAndClosePoiFile(lastModifiedDate);
				if(REMOVE_POI_DB) {
					indexPoiCreator.removePoiFile();
				}
				indexAddressCreator.closeAllPreparedStatements();
				indexTransportCreator.commitAndCloseFiles(getRTreeTransportStopsFileName(), getRTreeTransportStopsPackFileName(),
						deleteDatabaseIndexes);
				indexMapCreator.commitAndCloseFiles(getRTreeMapIndexNonPackFileName(), getRTreeMapIndexPackFileName(),
						deleteDatabaseIndexes);
				indexRouteCreator.commitAndCloseFiles(getRTreeRouteIndexNonPackFileName(), getRTreeRouteIndexPackFileName(),
						deleteDatabaseIndexes);

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

				// do not delete first db connection
				if (dbConn != null) {
					osmDBdialect.commitDatabase(dbConn);
					osmDBdialect.closeDatabase(dbConn);
					dbConn = null;
				}
				if (deleteOsmDB) {
					if (DBDialect.DERBY == osmDBdialect) {
						try {
							DriverManager.getConnection("jdbc:derby:;shutdown=true"); //$NON-NLS-1$
						} catch (SQLException e) {
							// ignore exception
						}
					}
					osmDBdialect.removeDatabase(dbFile);
				}
			} catch (SQLException e) {
				e.printStackTrace();
			} catch (RuntimeException e) {
				e.printStackTrace();
			}
		}
	}


	private void setGeneralProgress(IProgress progress, String genProgress) {
		if(progress instanceof ProgressDialog) {
			((ProgressDialog) progress).setGeneralProgress(genProgress);
		}
	}

	public static void main(String[] args) throws IOException, SAXException, SQLException, InterruptedException {
		long time = System.currentTimeMillis();
		IndexCreator creator = new IndexCreator(new File("/home/victor/projects/OsmAnd/data/osm-gen/")); //$NON-NLS-1$
		creator.setIndexMap(true);
		creator.setIndexAddress(true);
		creator.setIndexPOI(true);
		creator.setIndexTransport(true);
		creator.setIndexRouting(true);

//		creator.deleteDatabaseIndexes = false;
//		creator.recreateOnlyBinaryFile = true;
//		creator.deleteOsmDB = false;
				
		creator.setZoomWaySmothness(2);
		MapRenderingTypesEncoder rt = MapRenderingTypesEncoder.getDefault();
		MapZooms zooms = MapZooms.getDefault(); // MapZooms.parseZooms("15-");

		String file = "/home/victor/projects/OsmAnd/temp/map.osm";
//		String file = "/home/victor/projects/OsmAnd/temp/luxembourg.osm.pbf";
		int st = file.lastIndexOf('/');
		int e = file.indexOf('.', st);
		creator.setNodesDBFile(new File("/home/victor/projects/OsmAnd/data/osm-gen/"+file.substring(st, e) + ".tmp.odb"));
		creator.generateIndexes(new File(file),
				new ConsoleProgressImplementation(1), null, zooms, rt, log);
		
		
		
		// BASEMAP generation
//		zooms = MapZooms.parseZooms("1-2;3;4-5;6-7;8-9;10-");
//		creator.setMapFileName("World_basemap_2.obf");
//		File basemapParent = new File("/home/victor/projects/OsmAnd/data/basemap/ready/");
//		creator.generateBasemapIndex(new ConsoleProgressImplementation(1), null, zooms, rt, log, "basemap", 
//				new File(basemapParent, "10m_coastline_out.osm"),
//				new File(basemapParent, "10m_admin_level.osm"),
//				new File(basemapParent, "10m_rivers.osm"),
//				new File(basemapParent, "10m_lakes.osm"),
//				new File(basemapParent, "10m_populated_places.osm")
//		);
		

		log.info("WHOLE GENERATION TIME :  " + (System.currentTimeMillis() - time)); //$NON-NLS-1$
		log.info("COORDINATES_SIZE " + BinaryMapIndexWriter.COORDINATES_SIZE + " count " + BinaryMapIndexWriter.COORDINATES_COUNT); //$NON-NLS-1$ //$NON-NLS-2$
		log.info("TYPES_SIZE " + BinaryMapIndexWriter.TYPES_SIZE); //$NON-NLS-1$
		log.info("ID_SIZE " + BinaryMapIndexWriter.ID_SIZE); //$NON-NLS-1$
		log.info("- COORD_TYPES_ID SIZE " + (BinaryMapIndexWriter.COORDINATES_SIZE + BinaryMapIndexWriter.TYPES_SIZE + BinaryMapIndexWriter.ID_SIZE)); //$NON-NLS-1$
		log.info("- MAP_DATA_SIZE " + BinaryMapIndexWriter.MAP_DATA_SIZE); //$NON-NLS-1$
		log.info("- STRING_TABLE_SIZE " + BinaryMapIndexWriter.STRING_TABLE_SIZE); //$NON-NLS-1$
		log.info("-- MAP_DATA_AND_STRINGS SIZE " + (BinaryMapIndexWriter.MAP_DATA_SIZE + BinaryMapIndexWriter.STRING_TABLE_SIZE)); //$NON-NLS-1$

	}
}
