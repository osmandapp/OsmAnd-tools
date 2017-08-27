package net.osmand.data.diff;

import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.hash.TLongObjectHashMap;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import net.osmand.ResultMatcher;
import net.osmand.binary.BinaryIndexPart;
import net.osmand.binary.BinaryMapDataObject;
import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.binary.BinaryMapIndexReader.MapIndex;
import net.osmand.binary.BinaryMapIndexReader.MapRoot;
import net.osmand.binary.BinaryMapIndexReader.SearchFilter;
import net.osmand.binary.BinaryMapIndexReader.SearchRequest;
import net.osmand.binary.MapZooms;
import net.osmand.util.MapUtils;
import rtree.RTreeException;

public class ObfDiffMerger {
	
	private double lattop = 85;
	private double latbottom = -85;
	private double lonleft = -179.9;
	private double lonright = 179.9;
	
	public static void main(String[] args) {
		try {
			ObfDiffMerger merger = new ObfDiffMerger();
			merger.mergeChanges(args);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
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
		List<File> diffs = new ArrayList<>();
		boolean checkTimestamps = false;
		for (int i = 1; i < args.length; i++) {
			if(args[i].equals("--check-timestamp")) {
				checkTimestamps = true;
				continue;
			}
			File fl = new File(args[i]);
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
			}
		}
		// TODO check timestamps
		ObfFileInMemory context = fetchAllObjects(diffs);
		context.writeFile(result);
	}


	
	private ObfFileInMemory fetchAllObjects(List<File> diffs) throws IOException {
		// TODO POI, Routing, Transport
		ObfFileInMemory m = new ObfFileInMemory();
		for (int i = diffs.size() - 1; i >= 0; i--) {
			File f = diffs.get(i);
			RandomAccessFile raf = new RandomAccessFile(f, "r");
			BinaryMapIndexReader indexReader = new BinaryMapIndexReader(raf, f);
			for (BinaryIndexPart p : indexReader.getIndexes()) {
				if(p instanceof MapIndex) {
					MapIndex mi = (MapIndex) p;
					for(MapRoot mr : mi.getRoots()) {
						MapZooms.MapZoomPair pair = new MapZooms.MapZoomPair(mr.getMinZoom(), mr.getMaxZoom());
						TLongObjectHashMap<BinaryMapDataObject> res = m.get(pair);
						TLongObjectHashMap<BinaryMapDataObject> objects = getBinaryMapData(indexReader, mr.getMinZoom());
						res.putAll(objects);
					}
				}
			}
			m.updateTimestamp(indexReader.getDateCreated());
			
			indexReader.close();
			raf.close();
		}
		return m;
	}
	
	private TLongObjectHashMap<BinaryMapDataObject> getBinaryMapData(BinaryMapIndexReader index, int zoom) throws IOException {
		final TLongObjectHashMap<BinaryMapDataObject> result = new TLongObjectHashMap<>();
		for (BinaryIndexPart p : index.getIndexes()) {
			if(p instanceof MapIndex) {
				MapIndex m = ((MapIndex) p);
				final SearchRequest<BinaryMapDataObject> req = BinaryMapIndexReader.buildSearchRequest(
						MapUtils.get31TileNumberX(lonleft),
						MapUtils.get31TileNumberX(lonright),
						MapUtils.get31TileNumberY(lattop),
						MapUtils.get31TileNumberY(latbottom),
						zoom,
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
