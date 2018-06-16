package net.osmand.obf.diff;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.zip.GZIPOutputStream;

import net.osmand.binary.MapZooms;
import net.osmand.impl.ConsoleProgressImplementation;
import net.osmand.obf.preparation.DBDialect;
import net.osmand.obf.preparation.IndexCreator;
import net.osmand.obf.preparation.IndexCreatorSettings;
import net.osmand.osm.MapRenderingTypesEncoder;
import net.osmand.util.Algorithms;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.xmlpull.v1.XmlPullParserException;

import rtree.RTree;

public class GenerateDailyObf {
	private static final Log log = LogFactory.getLog(GenerateDailyObf.class);
	private static final String TOTAL_SIZE = "totalsize";
	public static final String OSM_ODB_FILE = "osm.odb";
	public static void main(String[] args) {
		try {
			File dir = new File(args[0]);
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
	
	public static void fixTimestamps(File dir) throws IOException, ParseException {
		SimpleDateFormat day = new SimpleDateFormat("yyyy_MM_dd_HH_mm");
		day.setTimeZone(TimeZone.getTimeZone("UTC"));
		for(File countryF : dir.listFiles()) {
			if(!countryF.isDirectory()) {
				continue;
			}
			for(File date : countryF.listFiles()) {
				if(date.getName().length() == 10) {
					for(File f : date.listFiles()) {
						if(f.getName().endsWith(".osm.gz")) {
							String hourt = f.getName().substring(f.getName().length() - ".osm.gz".length() - 5, 
									f.getName().length() - ".osm.gz".length());
							Date tm = day.parse(date.getName() +"_"+hourt);
							f.setLastModified(tm.getTime());
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
		List<File> cnt = sortFiles(dir);
		int i = 0;
		for(File countryF : cnt) {
			if(!countryF.isDirectory()) {
				continue;
			}
			i++;
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
						System.out.println("Processing " + targetObf.getName() + " " + new Date() + " " + (i * 100.f / cnt.size()) +"%");
						Collections.sort(osmFiles, new Comparator<File>(){
							@Override
							public int compare(File o1, File o2) {
								return o1.getName().compareTo(o2.getName());
							}
						});
						generateCountry(name, 
								targetObf, osmFiles.toArray(new File[osmFiles.size()]), targetTimestamp, new File(date, OSM_ODB_FILE));
						writeTotalSize(date, totalSize);
					}
				}
			}
		}
		
	}

	public static List<File> sortFiles(File dir) {
		File[] fs = dir.listFiles();
		List<File> cnt = new ArrayList<File>(Arrays.asList(fs));
		Collections.sort(cnt, new Comparator<File>() {

			@Override
			public int compare(File o1, File o2) {
				return o1.getName().compareTo(o2.getName());
			}
		});
		return cnt;
	}

	public static void generateCountry(String name, File targetObfZip, File[] array, long targetTimestamp, File nodesFile) 
 throws IOException, SQLException, InterruptedException, XmlPullParserException {
		boolean exception = true;
		try {
			RTree.clearCache();
			IndexCreatorSettings settings = new IndexCreatorSettings();
			settings.indexMap = true;
			settings.indexAddress = false;
			settings.indexPOI = true;
			settings.indexTransport = false;
			settings.indexRouting = false;
			settings.generateLowLevel = false;
			
			IndexCreator ic = new IndexCreator(targetObfZip.getParentFile(), settings);
			ic.setLastModifiedDate(targetTimestamp);
			ic.setDialects(DBDialect.SQLITE, DBDialect.SQLITE_IN_MEMORY);
			ic.setLastModifiedDate(targetTimestamp);
			ic.setRegionName(Algorithms.capitalizeFirstLetterAndLowercase(name));
			ic.setNodesDBFile(nodesFile);
			ic.setDeleteOsmDB(false);
			ic.generateIndexes(array, new ConsoleProgressImplementation(), null, MapZooms.parseZooms("13-14;15-"),
					new MapRenderingTypesEncoder(name), log, false, true);
			File targetFile = new File(targetObfZip.getParentFile(), ic.getMapFileName());
			targetFile.setLastModified(targetTimestamp);
			FileInputStream fis = new FileInputStream(targetFile);
			GZIPOutputStream gzout = new GZIPOutputStream(new FileOutputStream(targetObfZip));
			Algorithms.streamCopy(fis, gzout);
			fis.close();
			gzout.close();
			targetObfZip.setLastModified(targetTimestamp);
			targetFile.delete();
			exception = false;
		} finally {
			if (exception) {
				nodesFile.delete();
				nodesFile.deleteOnExit();
			}
		}

	}
}
