package net.osmand.data.diff;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.zip.GZIPOutputStream;

import net.osmand.data.preparation.DBDialect;
import net.osmand.data.preparation.IndexCreator;
import net.osmand.data.preparation.MapZooms;
import net.osmand.impl.ConsoleProgressImplementation;
import net.osmand.osm.MapRenderingTypesEncoder;
import net.osmand.util.Algorithms;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.xmlpull.v1.XmlPullParserException;

import rtree.RTree;

public class GenerateDailyObf {
	private static final Log log = LogFactory.getLog(GenerateDailyObf.class);
	public static void main(String[] args) {
		try {
			File dir = new File(args[0]);
			iterateOverDir(dir);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	private static void iterateOverDir(File dir) throws IOException, SQLException, InterruptedException, XmlPullParserException {
		for(File countryF : dir.listFiles()) {
			if(!countryF.isDirectory()) {
				continue;
			}
			for(File date : countryF.listFiles()) {
				if(date.getName().length() == 10) {
					File targetObf = new File(date, countryF.getName() + ".obf.gz");
					long targetTimestamp = 0;
					List<File> osmFiles = new ArrayList<File>();
					for(File f : date.listFiles()) {
						if(f.getName().endsWith(".osm.gz")) {
							targetTimestamp = Math.max(targetTimestamp, f.lastModified());
							osmFiles.add(f);
						}
					}
					if (!targetObf.exists() || targetObf.lastModified() != targetTimestamp) {
						if(!targetObf.exists()) {
							log.info("The file " + targetObf.getName() + " doesn't exist");
						} else {
							log.info("The file " + targetObf.getName() + " was updated for " + (targetObf.lastModified() - targetTimestamp) / 1000
									+ " seconds");
						}
						Collections.sort(osmFiles, new Comparator<File>(){
							@Override
							public int compare(File o1, File o2) {
								return o1.getName().compareTo(o2.getName());
							}
						});
						generateCountry(countryF.getName(), 
								targetObf, osmFiles.toArray(new File[osmFiles.size()]), targetTimestamp);
					}
				}
			}
		}
		
	}

	private static void generateCountry(String name, File targetObfZip, File[] array, long targetTimestamp) throws IOException, SQLException, InterruptedException, XmlPullParserException {
		RTree.clearCache();
		IndexCreator ic = new IndexCreator(targetObfZip.getParentFile());
		ic.setIndexAddress(false);
		ic.setBackwardComptibleIds(true);
		ic.setIndexPOI(true);
		ic.setIndexRouting(true);
		ic.setIndexMap(true);
		ic.setGenerateLowLevelIndexes(false);
		ic.setDialects(DBDialect.SQLITE_IN_MEMORY, DBDialect.SQLITE_IN_MEMORY);
		ic.setLastModifiedDate(targetTimestamp);
		File tmpFile = new File(targetObfZip.getName() + ".tmp.odb");
		tmpFile.delete();
		ic.setRegionName(Algorithms.capitalizeFirstLetterAndLowercase(name));
		ic.setNodesDBFile(tmpFile);
		ic.generateIndexes(array, new ConsoleProgressImplementation(), null,
				MapZooms.parseZooms("13-14;15-"), new MapRenderingTypesEncoder(name), log, false, true);
		File targetFile = new File(targetObfZip.getParentFile(), ic.getMapFileName());
		targetFile.setLastModified(targetTimestamp);
		FileInputStream fis = new FileInputStream(targetFile);
		GZIPOutputStream gzout = new GZIPOutputStream(new FileOutputStream(targetObfZip));
		Algorithms.streamCopy(fis, gzout);
		fis.close();
		gzout.close();
		targetObfZip.setLastModified(targetTimestamp);
		targetFile.delete();

	}
}
