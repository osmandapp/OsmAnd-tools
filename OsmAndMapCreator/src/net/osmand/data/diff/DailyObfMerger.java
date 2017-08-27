package net.osmand.data.diff;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.hash.TLongObjectHashMap;
import net.osmand.ResultMatcher;
import net.osmand.binary.BinaryIndexPart;
import net.osmand.binary.BinaryMapDataObject;
import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.binary.BinaryMapIndexReader.MapIndex;
import net.osmand.binary.BinaryMapIndexReader.SearchFilter;
import net.osmand.binary.BinaryMapIndexReader.SearchRequest;
import net.osmand.util.MapUtils;
import rtree.RTreeException;

public class DailyObfMerger {
	
	private double lattop = 85;
	private double latbottom = -85;
	private double lonleft = -179.9;
	private double lonright = 179.9;
	private static final int ZOOM_LEVEL = 15;
	
	public static void main(String[] args) {
		DailyObfMerger merger = new DailyObfMerger();
		if (args[0].equals("day")) {
			merger.mergeChanges(args);
		}
	}

	private void mergeChanges(String[] args) {
		File result = new File(args[0]);
		File[] diffs = new File[args.length - 1];
		for (int i = 1; i < args.length; i++) {
			diffs[i - 1] = new File(args[i]);
		}
		writeFile(result, diffs);
		
	}

	private void writeFile(File result, File[] diffs) {
		Arrays.sort(diffs, new Comparator<File>() {
		    @Override
			public int compare(File f1, File f2) {
		        return Long.compare(f1.lastModified(), f2.lastModified());
		    }
		});
		TLongObjectHashMap<BinaryMapDataObject> list = null;
		try {
			list = fetchAllMapObjects(diffs);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}
		
		File end = diffs[diffs.length - 1];
		RandomAccessFile raf;
		BinaryMapIndexReader indexReader = null;
		try {
			raf = new RandomAccessFile(end, "r");
			indexReader = new BinaryMapIndexReader(raf, end);
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
			System.exit(1);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}
		 
		MapIndex mi = indexReader.getMapIndexes().get(0);
		
		try {
			ObfDiffGenerator.generateFinalObf(result, indexReader, list, mi, indexReader.getDateCreated());
		} catch (IOException | RTreeException e) {
			e.printStackTrace();
			System.exit(1);
		}
		
	}

	private TLongObjectHashMap<BinaryMapDataObject> fetchAllMapObjects(File[] diffs) throws IOException {
		List<TLongObjectHashMap<BinaryMapDataObject>> result = new ArrayList<>();
		TLongObjectHashMap<BinaryMapDataObject> res = new TLongObjectHashMap<BinaryMapDataObject>();
		for (File f : diffs) {
			RandomAccessFile raf = new RandomAccessFile(f, "r");
			BinaryMapIndexReader indexReader = new BinaryMapIndexReader(raf, f);
			result.add(getBinaryMapData(indexReader));
		}
		for (TLongObjectHashMap<BinaryMapDataObject> l : result) {
			res.putAll(l);
		}
		return res;
	}
	
	private TLongObjectHashMap<BinaryMapDataObject> getBinaryMapData(BinaryMapIndexReader index) throws IOException {
		final TLongObjectHashMap<BinaryMapDataObject> result = new TLongObjectHashMap<>();
		for (BinaryIndexPart p : index.getIndexes()) {
			if(p instanceof MapIndex) {
				MapIndex m = ((MapIndex) p);
				final SearchRequest<BinaryMapDataObject> req = BinaryMapIndexReader.buildSearchRequest(
						MapUtils.get31TileNumberX(lonleft),
						MapUtils.get31TileNumberX(lonright),
						MapUtils.get31TileNumberY(lattop),
						MapUtils.get31TileNumberY(latbottom),
						ZOOM_LEVEL,
						new SearchFilter() {
							@Override
							public boolean accept(TIntArrayList types, MapIndex index) {
								return true;
							}
						},
						new ResultMatcher<BinaryMapDataObject>() {
							@Override
							public boolean publish(BinaryMapDataObject obj) {
								result.put(obj.getId(), obj);
								return false;
							}

							@Override
							public boolean isCancelled() {
								return false;
							}
						});
				index.searchMapIndex(req, m);
			} 
		}
		return result;
	}
}
