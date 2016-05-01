package net.osmand.data.diff;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
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
	private static final String TOTAL_SIZE = "totalsize";
	public static void main(String[] args) {
		try {
			File dir = new File(args[0]);
//			fixTimestamps(dir);
			
			iterateOverDir(dir);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	public static void fixObfTimestamps(File dir) throws IOException {
		for(File countryF : dir.listFiles()) {
			if(!countryF.isDirectory()) {
				continue;
			}
			for(File date : countryF.listFiles()) {
				if(date.getName().length() == 10) {
					String name = countryF.getName() + "_" + date.getName().substring(2).replace('-', '_');
					name = Algorithms.capitalizeFirstLetterAndLowercase(name);
					File targetObf = new File(countryF, name + ".obf.gz");
					long targetTimestamp = 0;
					List<File> osmFiles = new ArrayList<File>();
					long totalSize = 0;
					for(File f : date.listFiles()) {
						if(f.getName().endsWith(".osm.gz")) {
							targetTimestamp = Math.max(targetTimestamp, f.lastModified());
							osmFiles.add(f);
							totalSize += f.length();
						}
					}
					if (targetObf.exists() && targetObf.lastModified() == targetTimestamp) {
						writeTotalSize(date, totalSize);
					}
				}
			}
		}		
	}
	
	public static void fixTimestamps(File dir) throws IOException {
		for(File countryF : dir.listFiles()) {
			if(!countryF.isDirectory()) {
				continue;
			}
			for(File date : countryF.listFiles()) {
				if(date.getName().length() == 10) {
					String name = countryF.getName() + "_" + date.getName().substring(2).replace('-', '_');
					name = Algorithms.capitalizeFirstLetterAndLowercase(name);
					for(File f : date.listFiles()) {
						if(f.getName().endsWith(".osm.gz")) {
							
						}
					}
				}
			}
		}		
	}

	private static void writeTotalSize(File date, long totalSize) throws IOException {
		File fl = new File(date, TOTAL_SIZE);
		FileWriter fw = new FileWriter(fl);
		fw.write(Long.toString(totalSize));
		fw.close();
	}

	private static void iterateOverDir(File dir) throws IOException, SQLException, InterruptedException, XmlPullParserException {
		for(File countryF : dir.listFiles()) {
			if(!countryF.isDirectory()) {
				continue;
			}
			for(File date : countryF.listFiles()) {
				if(date.getName().length() == 10) {
					String name = countryF.getName() + "_" + date.getName().substring(2).replace('-', '_');
					name = Algorithms.capitalizeFirstLetterAndLowercase(name);
					File targetObf = new File(countryF, name + ".obf.gz");
					long targetTimestamp = 0;
					List<File> osmFiles = new ArrayList<File>();
					long totalSize = 0;
					long procSize = 0;
					for(File f : date.listFiles()) {
						if(f.getName().endsWith(".osm.gz")) {
							targetTimestamp = Math.max(targetTimestamp, f.lastModified());
							osmFiles.add(f);
							totalSize += f.length();
						}
						if(f.getName().equals(TOTAL_SIZE)) {
							FileReader fr = new FileReader(f);
							BufferedReader br = new BufferedReader(fr);
							procSize = Long.parseLong(br.readLine());
							fr.close();
						}
					}
					if (!targetObf.exists() || procSize != totalSize) {
						if(!targetObf.exists()) {
							log.info("The file " + targetObf.getName() + " doesn't exist");
						} else {
							log.info("The file " + targetObf.getName() + " was updated for " + (targetObf.lastModified() - targetTimestamp) / 1000
									+ " seconds");
						}
						System.out.println("Processing " + targetObf.getName() + " " + new Date());
						Collections.sort(osmFiles, new Comparator<File>(){
							@Override
							public int compare(File o1, File o2) {
								return o1.getName().compareTo(o2.getName());
							}
						});
						writeTotalSize(date, totalSize);
						generateCountry(name, 
								targetObf, osmFiles.toArray(new File[osmFiles.size()]), targetTimestamp);
					}
				}
			}
		}
		
	}

	public static void generateCountry(String name, File targetObfZip, File[] array, long targetTimestamp) throws IOException, SQLException, InterruptedException, XmlPullParserException {
		RTree.clearCache();
		IndexCreator ic = new IndexCreator(targetObfZip.getParentFile());
		ic.setIndexAddress(false);
		ic.setIndexPOI(true);
		ic.setIndexRouting(true);
		ic.setIndexMap(true);
		ic.setLastModifiedDate(targetTimestamp);
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
