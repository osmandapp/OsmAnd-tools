package net.osmand.osm.util;

import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TLongObjectHashMap;
import net.osmand.IProgress;
import net.osmand.PlatformUtil;
import net.osmand.data.LatLon;
import net.osmand.data.preparation.DBDialect;
import net.osmand.data.preparation.OsmDbAccessor;
import net.osmand.data.preparation.OsmDbAccessorContext;
import net.osmand.data.preparation.OsmDbCreator;
import net.osmand.impl.ConsoleProgressImplementation;
import net.osmand.osm.edit.*;
import net.osmand.osm.edit.Entity.EntityId;
import net.osmand.osm.edit.Entity.EntityType;
import net.osmand.osm.io.IOsmStorageFilter;
import net.osmand.osm.io.OsmBaseStorage;
import net.osmand.osm.io.OsmBaseStoragePbf;
import net.osmand.osm.io.OsmStorageWriter;
import net.osmand.swing.DataExtractionSettings;
import net.osmand.swing.Messages;
import net.osmand.util.MapUtils;
import org.apache.commons.logging.Log;
import org.apache.tools.bzip2.CBZip2InputStream;
import org.xml.sax.SAXException;

import javax.xml.stream.XMLStreamException;
import java.io.*;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

public class KeepBigPolygons {
    private static final Log log = PlatformUtil.getLog(KeepBigPolygons.class);
    private DBDialect osmDBdialect;
    private Object dbConn;

    public static void main(String[] args) throws IOException, SAXException, XMLStreamException, SQLException, InterruptedException {
		String fileToRead = args != null && args.length > 0 ? args[0] : null; 
		File read = new File(fileToRead);
		String fileToWrite =  args != null && args.length > 1 ? args[1] : null;
		if(fileToWrite != null){
		} else {
			String fileName = read.getName();
			int i = fileName.indexOf('.');
			fileToWrite = read.getParentFile().getParentFile() + "/" + fileName.substring(0, i) + "_out";
		}
		

        boolean keepOldNodes = true; // by default

        new KeepBigPolygons().process(read, fileToWrite, keepOldNodes);
	}
    private Object getDatabaseConnection(String fileName, DBDialect dialect) throws SQLException {
        return dialect.getDatabaseConnection(fileName, log);
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


        OsmDbCreator dbCreator = new OsmDbCreator();
        // 1. Loading osm file
        try {
            progress.startTask(Messages.getString("IndexCreator.LOADING_FILE") + readFile.getAbsolutePath(), -1); //$NON-NLS-1$
            // 1 init database to store temporary data
            dbCreator.initDatabase(osmDBdialect, dbConn);
            storage.getFilters().add(dbCreator);
            storage.parseOSM(stream, progress, streamFile, false);
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
	
	private void process(File read, final String write, boolean keepOldNodes) throws IOException, SAXException, XMLStreamException, SQLException, InterruptedException {
        osmDBdialect = DBDialect.SQLITE;
        OsmDbAccessor accessor = new OsmDbAccessor();
        File dbFile =  new File(read.getParentFile(), "temp.nodes");;
        // to save space
        if (!keepOldNodes && osmDBdialect.databaseFileExists(dbFile)) {
            osmDBdialect.removeDatabase(dbFile);
        }
        boolean loadFromExistingFile = osmDBdialect.databaseFileExists(dbFile);
        dbConn = getDatabaseConnection(dbFile.getAbsolutePath(), osmDBdialect);
        int allRelations = 100000;
        int allWays = 1000000;
        int allNodes = 10000000;
        if (!loadFromExistingFile) {
            OsmDbCreator dbCreator = extractOsmToNodesDB(read, new ConsoleProgressImplementation(), null);
            if (dbCreator != null) {
                allNodes = dbCreator.getAllNodes();
                allWays = dbCreator.getAllWays();
                allRelations = dbCreator.getAllRelations();
            }
        }

		final int[] index = new int[] {1};
        final OsmDbAccessor access = new OsmDbAccessor();
        access.initDatabase(dbConn, osmDBdialect, allNodes, allWays, allRelations);
        final OsmBaseStorage storage = new OsmBaseStorage();
        access.iterateOverEntities(new ConsoleProgressImplementation(), null, new OsmDbAccessor.OsmDbVisitor() {
            @Override
            public void iterateEntity(Entity e, OsmDbAccessorContext ctx) throws SQLException {
                List<Node> nodes = new ArrayList<Node>();
                if (e instanceof Relation) {
                    access.loadEntityRelation((Relation) e);
                    for (Entity es : ((Relation) e).getMemberEntities().keySet()) {
                        if (es instanceof Way) {
                            nodes.addAll(((Way) es).getNodes());
                        }
                    }
                } else if (e instanceof Way) {
                    nodes.addAll(((Way) e).getNodes());
                }
                if (OsmMapUtils.polygonAreaPixels(nodes, 8) > 24) {
                    storage.registerEntity(e, null);
                    for (Node nt : nodes) {
                        if (nt != null) {
                            storage.registerEntity(nt, null);
                        }
                    }
                    if (e instanceof Relation) {
                        for (Entity es : ((Relation) e).getMemberEntities().keySet()) {
                            storage.registerEntity(es, null);
                        }
                    }

                }
	            if(storage.getRegisteredEntities().size() > 10000) {
		            try {
			            writeStorage(write, index[0]++, storage);
		            } catch (Exception e1) {
			            throw new RuntimeException(e1);
		            }
	            }

            }
        });
		writeStorage(write, index[0]++, storage);

	}

	private void writeStorage(String write, int ind, OsmBaseStorage storage) throws IOException, XMLStreamException {
		if(write.endsWith(".osm")) {
			write = write.substring(0, write.length() - 4);
		}
		write =  write + "_" + ind + ".osm";
		OsmStorageWriter writer = new OsmStorageWriter();
		writer.saveStorage(new FileOutputStream(write), storage, null, true);
		storage.getRegisteredEntities().clear();
	}
}
