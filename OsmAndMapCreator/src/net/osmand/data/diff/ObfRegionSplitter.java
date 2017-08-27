package net.osmand.data.diff;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

import com.google.protobuf.CodedOutputStream;

import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.hash.TLongObjectHashMap;
import net.osmand.ResultMatcher;
import net.osmand.binary.BinaryIndexPart;
import net.osmand.binary.BinaryMapDataObject;
import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.binary.OsmandOdb;
import net.osmand.binary.BinaryMapIndexReader.MapIndex;
import net.osmand.binary.BinaryMapIndexReader.MapRoot;
import net.osmand.binary.BinaryMapIndexReader.SearchFilter;
import net.osmand.binary.BinaryMapIndexReader.SearchRequest;
import net.osmand.data.index.IndexUploader;
import net.osmand.data.preparation.AbstractIndexPartCreator;
import net.osmand.data.preparation.BinaryFileReference;
import net.osmand.data.preparation.BinaryMapIndexWriter;
import net.osmand.data.preparation.IndexVectorMapCreator;
import net.osmand.map.OsmandRegions;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;
import rtree.LeafElement;
import rtree.RTree;
import rtree.RTreeException;
import rtree.Rect;

public class ObfRegionSplitter {
	
	private double lattop = 85;
	private double latbottom = -85;
	private double lonleft = -179.9;
	private double lonright = 179.9;
	private static final int ZOOM_LEVEL = 15;
	private static String todaysDateFolder;
	private static String currentTime;
	
	public static void main(String[] args) throws IOException {
		if (args.length != 3) {
			System.out.println("Usage: <path_to_world_obf_diff> <path_to_result_folder> <path_to_regions.ocbf>");
		}
		
		ObfRegionSplitter thisGenerator = new ObfRegionSplitter();
		thisGenerator.init(args);
	}

	private void init(String[] args) throws IOException {
		File worldObf = new File(args[0]);
		File ocbfFile = new File(args[2]);
		File dir = new File(args[1]);
		todaysDateFolder = worldObf.getName().substring(0, 10);
		currentTime = worldObf.getName().substring(11, 16);
		if (!worldObf.exists() || !ocbfFile.exists()) {
			System.out.println("Incorrect file!");
			System.exit(1);
		}
		if (!dir.exists()) {
			dir.mkdir();
		}
		RandomAccessFile raf = new RandomAccessFile(worldObf.getAbsolutePath(), "r");
		BinaryMapIndexReader indexReader = new BinaryMapIndexReader(raf, worldObf);
		List<BinaryMapDataObject> allMapObjects = getMapObjects(indexReader);
		OsmandRegions osmandRegions = new OsmandRegions();
		try {
			osmandRegions.prepareFile(ocbfFile.getAbsolutePath());
			osmandRegions.cacheAllCountries();
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}
		Map<String, TLongObjectHashMap<BinaryMapDataObject>> regionsData = splitRegions(allMapObjects, osmandRegions);
		writeSplitedFiles(indexReader, regionsData, dir);		
	}

	private void writeSplitedFiles(BinaryMapIndexReader indexReader,
			Map<String, TLongObjectHashMap<BinaryMapDataObject>> regionsData, File dir) {
		for (String regionName : regionsData.keySet()) {
			File f = new File(dir.getAbsolutePath() + "/" + regionName + "/" + todaysDateFolder);
			f.mkdirs();
			try {
				writeData(indexReader, f, regionsData.get(regionName), regionName);
			} catch (IOException | RTreeException e) {
				e.printStackTrace();
				System.exit(1);
			}
			
		}
		
	}

	private void writeData(BinaryMapIndexReader indexReader, File f, TLongObjectHashMap<BinaryMapDataObject> list, String regionName) throws IOException, RTreeException {
		File result = new File(f.getAbsolutePath(), Algorithms.capitalizeFirstLetter(regionName) + "_" + currentTime + ".obf");
		final RandomAccessFile raf = new RandomAccessFile(result, "rw");
		MapIndex part = indexReader.getMapIndexes().get(0);
		// write files
		CodedOutputStream ous = CodedOutputStream.newInstance(new OutputStream() {
			@Override
			public void write(int b) throws IOException {
				raf.write(b);
			}

			@Override
			public void write(byte[] b) throws IOException {
				raf.write(b);
			}

			@Override
			public void write(byte[] b, int off, int len) throws IOException {
				raf.write(b, off, len);
			}

		});
		
		
		int version = indexReader.getVersion();
		ous.writeInt32(OsmandOdb.OsmAndStructure.VERSION_FIELD_NUMBER, version);
		ous.writeInt64(OsmandOdb.OsmAndStructure.DATECREATED_FIELD_NUMBER, System.currentTimeMillis());
		writeMapData(ous, indexReader, raf, part, result, list);
//		writePoiData(ous);
	
		ous.writeInt32(OsmandOdb.OsmAndStructure.VERSIONCONFIRM_FIELD_NUMBER, version);
		ous.flush();
		
		FileInputStream fis = new FileInputStream(result);
		GZIPOutputStream gzout = new GZIPOutputStream(new FileOutputStream(new File(result.getAbsolutePath() + ".gz")));
		Algorithms.streamCopy(fis, gzout);
		fis.close();
		gzout.close();
		result.delete();
	}

	private static void writeMapData(CodedOutputStream ous,
			BinaryMapIndexReader index,
			RandomAccessFile raf,
			MapIndex part,
			File fileToExtract,
			TLongObjectHashMap<BinaryMapDataObject> objects) throws IOException, RTreeException {
		BinaryMapIndexWriter writer = new BinaryMapIndexWriter(raf, ous);
		writer.startWriteMapIndex(part.getName());
		boolean first = true;
		MapRoot r = part.getRoots().get(0);
		File nonpackRtree = new File(fileToExtract.getParentFile(), "nonpack" + r.getMinZoom() + "."
				+ fileToExtract.getName() + ".rtree");
		File packRtree = new File(fileToExtract.getParentFile(), "pack" + r.getMinZoom() + "."
				+ fileToExtract.getName() + ".rtree");
		RTree rtree = null;
		try {
			rtree = new RTree(nonpackRtree.getAbsolutePath());
			for (BinaryMapDataObject obj : objects.valueCollection()) {
				int minX = obj.getPoint31XTile(0);
				int maxX = obj.getPoint31XTile(0);
				int maxY = obj.getPoint31YTile(0);
				int minY = obj.getPoint31YTile(0);
				for (int i = 1; i < obj.getPointsLength(); i++) {
					minX = Math.min(minX, obj.getPoint31XTile(i));
					minY = Math.min(minY, obj.getPoint31YTile(i));
					maxX = Math.max(maxX, obj.getPoint31XTile(i));
					maxY = Math.max(maxY, obj.getPoint31YTile(i));
				}
				try {
					rtree.insert(new LeafElement(new Rect(minX, minY, maxX, maxY), obj.getId()));
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
			rtree = AbstractIndexPartCreator.packRtreeFile(rtree, nonpackRtree.getAbsolutePath(),
					packRtree.getAbsolutePath());
			TLongObjectHashMap<BinaryFileReference> treeHeader = new TLongObjectHashMap<BinaryFileReference>();

			long rootIndex = rtree.getFileHdr().getRootIndex();
			rtree.Node root = rtree.getReadNode(rootIndex);
			Rect rootBounds = IndexUploader.calcBounds(root);
			if (rootBounds != null) {
				if(first) {
					writer.writeMapEncodingRules(index, part);
					first = false;
				}
				writer.startWriteMapLevelIndex(r.getMinZoom(), r.getMaxZoom(), rootBounds.getMinX(),
						rootBounds.getMaxX(), rootBounds.getMinY(), rootBounds.getMaxY());
				IndexVectorMapCreator.writeBinaryMapTree(root, rootBounds, rtree, writer, treeHeader);

				IndexUploader.writeBinaryMapBlock(root, rootBounds, rtree, writer, treeHeader, objects, r);
				writer.endWriteMapLevelIndex();
									
			}
		} finally {
			if (rtree != null) {
				RandomAccessFile file = rtree.getFileHdr().getFile();
				file.close();
			}
			nonpackRtree.delete();
			packRtree.delete();
			RTree.clearCache();
		}
	
		writer.endWriteMapIndex();
	}		

	private Map<String, TLongObjectHashMap<BinaryMapDataObject>> splitRegions(List<BinaryMapDataObject> allMapObjects,
			OsmandRegions osmandRegions) throws IOException {
		Map<String, TLongObjectHashMap<BinaryMapDataObject>> result = new HashMap<>();
		for (BinaryMapDataObject obj : allMapObjects) {
			int x = obj.getPoint31XTile(0);
			int y = obj.getPoint31YTile(0);
			List<BinaryMapDataObject> l = osmandRegions.query(x, y);
			for (BinaryMapDataObject b : l) {
				if (osmandRegions.contain(b, x, y)) {
					String dw = osmandRegions.getDownloadName(b);
					if (!Algorithms.isEmpty(dw) && osmandRegions.isDownloadOfType(b, OsmandRegions.MAP_TYPE)) {
						if (result.get(dw) != null) {
							result.get(dw).put(obj.getId(), obj);
						}
						TLongObjectHashMap<BinaryMapDataObject> resultList = new TLongObjectHashMap<>();
						resultList.put(obj.getId(), obj);
						result.put(dw, resultList);
					}
				}
			}
		}
		return result;
	}

	private List<BinaryMapDataObject> getMapObjects(BinaryMapIndexReader indexReader) throws IOException {
		final List<BinaryMapDataObject> result = new ArrayList<>();
		for (BinaryIndexPart p : indexReader.getIndexes()) {
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
								result.add(obj);
								return false;
							}

							@Override
							public boolean isCancelled() {
								return false;
							}
						});
				indexReader.searchMapIndex(req, m);
			} 
		}
		return result;
	}
}
