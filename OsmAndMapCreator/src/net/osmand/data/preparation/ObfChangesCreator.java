package net.osmand.data.preparation;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import net.osmand.impl.ConsoleProgressImplementation;
import net.osmand.osm.MapRenderingTypesEncoder;
import net.osmand.util.Algorithms;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import rtree.RTree;

public class ObfChangesCreator {
	private static final Log log = LogFactory.getLog(ObfChangesCreator.class);

	private class GroupFiles implements Comparable<GroupFiles> {
		List<File> osmGzFiles = new ArrayList<File>();
		long maxTimestamp = 0;
		String basedate;

		GroupFiles(String basedate) {
			this.basedate = basedate;

		}

		public void addOsmFile(File f) {
			maxTimestamp = Math.max(maxTimestamp, f.lastModified());
			osmGzFiles.add(f);
		}

		public File getObfFileName(File country) {
			return new File(country, basedate + ".obf");
		}

		@Override
		public int compareTo(GroupFiles o) {
			return basedate.compareTo(o.basedate);
		}

		public File[] getSortedFiles() {
			Collections.sort(osmGzFiles, new Comparator<File>(){
				@Override
				public int compare(File o1, File o2) {
					return o1.getName().compareTo(o2.getName());
				}
			});
			return osmGzFiles.toArray(new File[0]);
		}

	}
	
//	private class CombineCountryContext {
//		Map<String, GroupFiles> dayChange = new TreeMap<String, ObfChangesCreator.GroupFiles>();
//		Map<String, GroupFiles> monthChange = new TreeMap<String, ObfChangesCreator.GroupFiles>();
//	}

	private void process(String location) throws Exception {
		File mainDir = new File(location);
		for (File country : mainDir.listFiles()) {
			if(country.getName().startsWith("_")) {
				continue;
			}
			
			Map<String, GroupFiles> gf = combineChanges(country);
			for (GroupFiles g : gf.values()) {
				createObfFiles(country, g);
			}
		}
	}

	private void createObfFiles(File country, GroupFiles g) throws Exception {
		File obf = g.getObfFileName(country);
		if (!obf.exists() || g.maxTimestamp - obf.lastModified() > 1000) {
			if (obf.exists()) {
				System.out.println("The file was updated for " + (g.maxTimestamp - obf.lastModified()) / 1000
						+ " seconds");
			}
			RTree.clearCache();
			IndexCreator ic = new IndexCreator(country);
			ic.setIndexAddress(false);
			ic.setIndexPOI(true);
			ic.setIndexRouting(true);
			ic.setIndexMap(true);
			ic.setGenerateLowLevelIndexes(false);
			ic.setDialects(DBDialect.SQLITE_IN_MEMORY, DBDialect.SQLITE_IN_MEMORY);
			File tmpFile = new File(g.basedate + ".tmp.odb");
			tmpFile.delete();
			ic.setRegionName(Algorithms.capitalizeFirstLetterAndLowercase(g.basedate));
			ic.setNodesDBFile(tmpFile);
			log.info("Processing " + country.getName() + " " + g.basedate + " " + g.osmGzFiles.size() + " files");
			ic.generateIndexes(g.getSortedFiles(), new ConsoleProgressImplementation(), null,
					MapZooms.parseZooms("13-14;15-"), MapRenderingTypesEncoder.getDefault(), log, false);
			File targetFile = new File(country, ic.getMapFileName());
			targetFile.setLastModified(g.maxTimestamp);
		}
	}

	private Map<String, GroupFiles> combineChanges(File country) {
		Map<String, GroupFiles> change = new TreeMap<String, ObfChangesCreator.GroupFiles>();
		SimpleDateFormat sdf = new SimpleDateFormat("yy_MM_dd");
		String today = sdf.format(new Date());
		File[] changes = country.listFiles();
		if (changes != null) {
			for (File fileChange : changes) {
				if (fileChange.getName().endsWith("osm.gz")) {
					String date = fileChange.getName().substring(0, 8);
					String dayName = country.getName() + "_" + date;
					add(change, fileChange, dayName);
					if(!today.equals(date)) {
						String monthName = country.getName() + "_" + date + "00";
						add(change, fileChange, monthName);
					}
				}
			}
		}
		return change;
	}

	private void add(Map<String, GroupFiles> dayChange, File fileChange, String dayName) {
		if (!dayChange.containsKey(dayName)) {
			dayChange.put(dayName, new GroupFiles(dayName));
		}
		dayChange.get(dayName).addOsmFile(fileChange);
	}

	public static void main(String[] args) throws Exception {
		String location = "/Users/victorshcherb/osmand/temp/osmc/";
		if (args.length > 0) {
			location = args[0];
		}
		new ObfChangesCreator().process(location);
	}

}
