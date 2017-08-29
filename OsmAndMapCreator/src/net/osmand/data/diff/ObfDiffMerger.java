package net.osmand.data.diff;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import rtree.RTreeException;

public class ObfDiffMerger {
	
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
		// TODO Auto-generated method stub
		
	}
	
	public static void mergeBulkOsmLiveMonth(String location) {
		// TODO Auto-generated method stub
		
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

	private void mergeChanges(String[] args) throws IOException, RTreeException {
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

	public void process(File result, List<File> inputDiffs, boolean checkTimestamps) throws IOException,
			RTreeException {
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
				return;
			}
		}
		ObfFileInMemory context = new ObfFileInMemory();
		context.readObfFiles(diffs);
		context.writeFile(result);
	}


	
	
}
