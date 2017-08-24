package net.osmand.data.diff;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.WireFormat;

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
import net.osmand.binary.BinaryMapPoiReaderAdapter.PoiRegion;
import net.osmand.data.Amenity;
import net.osmand.data.index.IndexUploader;
import net.osmand.data.preparation.AbstractIndexPartCreator;
import net.osmand.data.preparation.BinaryFileReference;
import net.osmand.data.preparation.BinaryMapIndexWriter;
import net.osmand.data.preparation.IndexVectorMapCreator;
import net.osmand.util.MapUtils;
import rtree.LeafElement;
import rtree.RTree;
import rtree.RTreeException;
import rtree.Rect;

public class ObfDiffGenerator {
	
	
	private static final String OSMAND_CHANGE_VALUE = "delete";
	private static final String OSMAND_CHANGE_TAG = "osmand_change";
	private double lattop = 85;
	private double latbottom = -85;
	private double lonleft = -179.9;
	private double lonright = 179.9;
	private static final int ZOOM_LEVEL = 15;
	public static final int BUFFER_SIZE = 1 << 20;
	
	public static void main(String[] args) throws IOException, RTreeException {
		if (args.length != 3) {
			System.out.println("Usage: <path to old obf> <path to new obf> <result file name>");
			System.exit(1);
			return;
		}
		try {
			ObfDiffGenerator generator = new ObfDiffGenerator();
			generator.initialize(args);
		} catch(Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	private void initialize(String[] args) throws IOException, RTreeException {
		File start = new File(args[1]);
		File end = new File(args[2]);
		File result  = new File(args[3]);
		if (!start.exists() || !end.exists()) {
			System.exit(1);
			System.err.println("Input Obf file doesn't exist");
			return;
		}
		generateDiff(start, end, result);
	}

	private void generateDiff(File start, File end, File result) throws IOException, RTreeException {
		RandomAccessFile s = new RandomAccessFile(start.getAbsolutePath(), "r");
		RandomAccessFile e = new RandomAccessFile(end.getAbsolutePath(), "r");
		BinaryMapIndexReader indexS = new BinaryMapIndexReader(s, start);
		BinaryMapIndexReader indexE = new BinaryMapIndexReader(e, end);
		List<MapIndex> endIndexes = indexE.getMapIndexes();
		MapIndex mapIdx = endIndexes.get(0);
		Map<Long, BinaryMapDataObject> removeList = new HashMap<>();
		TLongObjectHashMap<BinaryMapDataObject> startData = getBinaryMapData(indexS);
		TLongObjectHashMap<BinaryMapDataObject> endData = getBinaryMapData(indexE);
//		List<Amenity> startPoi = getPoiData(indexS);
//		List<Amenity> endPoi = getPoiData(indexE);
//		List<Amenity> newPoi = comparePoi(startPoi, endPoi);
		System.out.println("Comparing the files...");
		for(Long idx : startData.keys()) {
			BinaryMapDataObject objE = endData.get(idx);
			if (objE != null) {
				BinaryMapDataObject objS = startData.get(idx);
				if (!objE.compareBinary(objS)) {
					removeList.put(idx, objS);					
				} 
			}
		}
		System.out.println("Finished comparing.");
		mapIdx.initMapEncodingRule(0, mapIdx.decodingRules.size() + 1, OSMAND_CHANGE_TAG, OSMAND_CHANGE_VALUE);
		for (long id : removeList.keySet()) {
			BinaryMapDataObject toDelete = removeList.get(id);
			BinaryMapDataObject obj = new BinaryMapDataObject(id, 
					toDelete.getCoordinates(), 
					null, 
					toDelete.getObjectType(), 
					toDelete.isArea(), 
					new int[]{mapIdx.decodingRules.size()},
					null);
			endData.put(id, obj);
		}
		if (result.exists()) {
			result.delete();
		}
		generateFinalObf(result, indexE, endData, mapIdx);
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
	
	private List<Amenity> comparePoi(List<Amenity> startPoi, List<Amenity> endPoi) {
		for (Amenity a : startPoi) {
			endPoi.remove(a);
		}
		return endPoi;
	}
	
	private List<Amenity> getPoiData(BinaryMapIndexReader index) throws IOException {
		final List<Amenity> amenities = new ArrayList<>();
		for (BinaryIndexPart p : index.getIndexes()) {
			if (p instanceof PoiRegion) {				
				SearchRequest<Amenity> req = BinaryMapIndexReader.buildSearchPoiRequest(
					MapUtils.get31TileNumberX(lonleft),
					MapUtils.get31TileNumberX(lonright),
					MapUtils.get31TileNumberY(lattop),
					MapUtils.get31TileNumberY(latbottom),
					ZOOM_LEVEL,
					BinaryMapIndexReader.ACCEPT_ALL_POI_TYPE_FILTER,
					new ResultMatcher<Amenity>() {
						@Override
						public boolean publish(Amenity object) {
							amenities.add(object);
							return false;
						}

						@Override
						public boolean isCancelled() {
							return false;
						}
					});
				index.initCategories((PoiRegion) p);
				index.searchPoi((PoiRegion) p, req);
			}
		}
		return amenities;
	}
	
	private static void writePoiData(CodedOutputStream ous) throws IOException {
		ous.writeTag(OsmandOdb.OsmAndStructure.POIINDEX_FIELD_NUMBER,
				WireFormat.WIRETYPE_FIXED32_LENGTH_DELIMITED);
	}
	
	public static void generateFinalObf(File fileToExtract, BinaryMapIndexReader index, TLongObjectHashMap<BinaryMapDataObject> objects, MapIndex part) throws IOException, RTreeException {
		final RandomAccessFile raf = new RandomAccessFile(fileToExtract, "rw");
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
		
		
		int version = index.getVersion();
		ous.writeInt32(OsmandOdb.OsmAndStructure.VERSION_FIELD_NUMBER, version);
		ous.writeInt64(OsmandOdb.OsmAndStructure.DATECREATED_FIELD_NUMBER, System.currentTimeMillis());
		writeMapData(ous, index, raf, part, fileToExtract, objects);
//		writePoiData(ous);
	
		ous.writeInt32(OsmandOdb.OsmAndStructure.VERSIONCONFIRM_FIELD_NUMBER, version);
		ous.flush();
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
		for (MapRoot r : part.getRoots()) {
			File nonpackRtree = new File(fileToExtract.getParentFile(), "nonpack" + r.getMinZoom() + "."
					+ fileToExtract.getName() + ".rtree");
			File packRtree = new File(fileToExtract.getParentFile(), "pack" + r.getMinZoom() + "."
					+ fileToExtract.getName() + ".rtree");
			RTree rtree = null;
			try {
				rtree = new RTree(nonpackRtree.getAbsolutePath());
				for (long key : objects.keys()) {
					BinaryMapDataObject obj = objects.get(key);
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
		}
		writer.endWriteMapIndex();
	}
}
