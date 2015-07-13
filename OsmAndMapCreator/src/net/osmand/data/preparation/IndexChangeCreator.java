package net.osmand.data.preparation;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.zip.GZIPInputStream;

import net.osmand.IProgress;
import net.osmand.impl.ConsoleProgressImplementation;
import net.osmand.osm.io.OsmBaseStorage;
import net.osmand.swing.DataExtractionSettings;
import net.osmand.swing.Messages;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tools.bzip2.CBZip2InputStream;
import org.xml.sax.SAXException;

public class IndexChangeCreator {
	private File workingDir;
	private File dbFile;
	private DBDialect osmDBdialect = DBDialect.SQLITE;
	public static final String TEMP_NODES_DB = "nodes.tmp.odb";
	private static final Log log = LogFactory.getLog(IndexChangeCreator.class);
	public IndexChangeCreator(File workingDir) {
		this.workingDir = workingDir;
	}
	
	private Object getDatabaseConnection(String fileName, DBDialect dialect) throws SQLException {
		return dialect.getDatabaseConnection(fileName, log);
	}
	
	private OsmDbCreator extractOsmToNodesDB(OsmDbAccessor accessor, File readFile, IProgress progress) throws FileNotFoundException,
			IOException, SQLException, SAXException {
		InputStream stream = new BufferedInputStream(new FileInputStream(readFile), 8192 * 4);
		InputStream streamFile = stream;
		long st = System.currentTimeMillis();
		if (readFile.getName().endsWith(".bz2")) { //$NON-NLS-1$
			if (stream.read() != 'B' || stream.read() != 'Z') {
//				throw new RuntimeException("The source stream must start with the characters BZ if it is to be read as a BZip2 stream."); //$NON-NLS-1$
			} else {
				stream = new CBZip2InputStream(stream);
			}
		} else if (readFile.getName().endsWith(".gz")) { //$NON-NLS-1$
			stream = new GZIPInputStream(stream);
		}

		OsmBaseStorage storage = new OsmBaseStorage();
		storage.setSupressWarnings(DataExtractionSettings.getSettings().isSupressWarningsForDuplicatedId());

		// 1. Loading osm file
		OsmDbCreator dbCreator = new OsmDbCreator();
		try {
			progress.startTask(Messages.getString("IndexCreator.LOADING_FILE") + readFile.getAbsolutePath(), -1); //$NON-NLS-1$
			// 1 init database to store temporary data
			dbCreator.initDatabase(osmDBdialect, accessor.getDbConn());
			storage.getFilters().add(dbCreator);
			storage.parseOSM(stream, progress, streamFile, false);
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
	
	private void createPlainOsmDb(OsmDbAccessor accessor, IProgress progress, File readFile) throws SQLException,
			IOException, SAXException {
		// initialize db file
		if (dbFile == null) {
			dbFile = new File(workingDir, TEMP_NODES_DB);
		}
		// to save space
		if (osmDBdialect.databaseFileExists(dbFile)) {
			osmDBdialect.removeDatabase(dbFile);
		}
		Object dbConn = getDatabaseConnection(dbFile.getAbsolutePath(), osmDBdialect);
		accessor.setDbConn(dbConn);

		int allRelations = 100000;
		int allWays = 1000000;
		int allNodes = 10000000;
		OsmDbCreator dbCreator = extractOsmToNodesDB(accessor, readFile, progress);
		if (dbCreator != null) {
			allNodes = dbCreator.getAllNodes();
			allWays = dbCreator.getAllWays();
			allRelations = dbCreator.getAllRelations();
		}
		accessor.initDatabase(dbConn, osmDBdialect, allNodes, allWays, allRelations);
	}
	
	public void setNodesDBFile(File file) {
		dbFile = file;
	}
	
	public static void main(String[] args) throws IOException, SAXException, SQLException, InterruptedException {
		long time = System.currentTimeMillis();
		String rootFolder = "/Users/victorshcherb/osmand/";		
		IndexChangeCreator creator = new IndexChangeCreator(new File(rootFolder + "/osm-gen/")); //$NON-NLS-1$
		String file = rootFolder + "/temp/032.osc.gz";
		int st = file.lastIndexOf('/');
		int e = file.indexOf('.', st);
		creator.setNodesDBFile(new File(rootFolder + "/osm-gen/"+file.substring(st, e) + ".tmp.odb"));
		OsmDbAccessor accessor = new OsmDbAccessor();
		creator.createPlainOsmDb(accessor, new ConsoleProgressImplementation(1), new File(file));
		accessor.closeReadingConnection();
		System.out.println("Finished in " + (System.currentTimeMillis() - time) + " ms");
	}
}
