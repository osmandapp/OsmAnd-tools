package net.osmand.data.diff;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import net.osmand.util.Algorithms;
import rtree.RTreeException;

public class ObfDiffMerger {
	static SimpleDateFormat day = new SimpleDateFormat("yyyy_MM_dd");
	static SimpleDateFormat month = new SimpleDateFormat("yyyy_MM");
	static {
		day.setTimeZone(TimeZone.getTimeZone("UTC"));
		month.setTimeZone(TimeZone.getTimeZone("UTC"));
	}
	
	public static void main(String[] args) {
		try {
			ObfDiffMerger merger = new ObfDiffMerger();
			merger.mergeChanges(args);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	
	public static void mergeBulkOsmLiveDay(String location) {
		try {
			File folder = new File(location);
			for (File region : getSortedFiles(folder)) {
				if (!region.isDirectory()) {
					continue;
				}
				String regionName = Algorithms.capitalizeFirstLetter(region.getName());
				if (regionName.equals("_diff")) {
					regionName = "World";
				}
				for (File date : getSortedFiles(region)) {
					if (!date.isDirectory()) {
						continue;
					}
					File flToMerge = new File(region, regionName + "_" + date.getName() + ".obf.gz");
					boolean processed = new ObfDiffMerger().process(flToMerge, Arrays.asList(date), true);
					if(processed) {
						System.out.println("Processed " + region + ".");
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	private static List<File> getSortedFiles(File region) {
		List<File> f = new ArrayList<>();
		for(File l : region.listFiles()) {
			f.add(l);
		}
		f.sort(new Comparator<File>() {

			@Override
			public int compare(File o1, File o2) {
				return o1.getName().compareTo(o2.getName());
			}
			
		});
		
		return f;
	}


	public static void mergeBulkOsmLiveMonth(String location) {
		try {
			Date currentDate = new Date();
			String cdate = day.format(currentDate).substring(2);
			System.out.println("Current date: " + cdate + ", file ends with will be ignored: " + cdate + ".obf.gz");
			File folder = new File(location);
			for (File region : getSortedFiles(folder)) {
				if (!region.isDirectory()) {
					continue;
				}
				String regionName = Algorithms.capitalizeFirstLetter(region.getName());
				if (regionName.startsWith("_")) {
					continue;
				}
				List<File> days = getSortedFiles(region);
				
				Map<String, List<File>> fls = groupFilesByMonth(regionName, days, cdate);
				for (String fl : fls.keySet()) {
					File flToMerge = new File(region, fl);
					boolean processed = new ObfDiffMerger().process(flToMerge, fls.get(fl), true);
					if(processed) {
						String s = "";
						for(File f: fls.get(fl)) {
							s += f.getName() + " ";
						}
						System.out.println("Processed " + flToMerge + " with " + s);
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}		
	}
	
	private static Map<String, List<File>> groupFilesByMonth(String regionName, List<File> days, String cdate) {
		Map<String, List<File>> grpFiles = new LinkedHashMap<String, List<File>>();
		for (File d : days) {
			if (!d.isFile() || !d.getName().startsWith(regionName + "_") || !d.getName().endsWith(".obf.gz")) {
				continue;
			}
			// month
			if (d.getName().endsWith("_00.obf.gz")) {
				continue;
			}
			// current date
			if (d.getName().endsWith(cdate + ".obf.gz")) {
				continue;
			}
			String date = d.getName().substring(regionName.length() + 1, d.getName().length() - ".obf.gz".length());
			String mnth = date.substring(0, date.length() - 2) + "00";
			String mnthFile = regionName + "_" + mnth +".obf.gz";
			if(!grpFiles.containsKey(mnthFile)) {
				grpFiles.put(mnthFile, new ArrayList<File>());
			}
			grpFiles.get(mnthFile).add(d);

		}
		return grpFiles;
	}


	private void sortByDate(List<File> f) {
		f.sort(new Comparator<File>() {

			@Override
			public int compare(File o1, File o2) {
				long l1 = o1.lastModified();
				long l2 = o2.lastModified();
				return Long.compare(l1, l2);
			}
		});
	}

	private void mergeChanges(String[] args) throws IOException, RTreeException, SQLException {
		File result = new File(args[0]);
		List<File> inputDiffs = new ArrayList<>();
		boolean checkTimestamps = false;
		for (int i = 1; i < args.length; i++) {
			if(args[i].equals("--check-timestamp")) {
				checkTimestamps = true;
				continue;
			}
			File fl = new File(args[i]);
			if(!fl.exists()) {
				throw new IllegalArgumentException("File not found: " + fl.getAbsolutePath());
			}
			inputDiffs.add(fl);
		}
		process(result, inputDiffs, checkTimestamps);
	}

	public boolean process(File result, List<File> inputDiffs, boolean checkTimestamps) throws IOException,
			RTreeException, SQLException {
		List<File> diffs = new ArrayList<>();
		for(File fl : inputDiffs) {
			if (fl.isDirectory()) {
				File[] lf = fl.listFiles();
				List<File> odiffs = new ArrayList<>();
				if (lf != null) {
					for (File f : lf) {
						if (f.getName().endsWith(".obf") || f.getName().endsWith(".obf.gz")) {
							odiffs.add(f);
						}
					}
				}
				sortByDate(odiffs);
				diffs.addAll(odiffs);
			} else {
				diffs.add(fl);
			}
		}
		if(checkTimestamps && result.exists()) {
			boolean skipEditing = true;
			long lastModified = result.lastModified();
			for(File f : diffs) {
				if(f.lastModified() > lastModified) {
					skipEditing = false;
					break;
				}
			}
			if(skipEditing) {
				return false;
			}
		}
		ObfFileInMemory context = new ObfFileInMemory();
		context.readObfFiles(diffs);
		context.writeFile(result);
		return true;
	}


	
	
}
