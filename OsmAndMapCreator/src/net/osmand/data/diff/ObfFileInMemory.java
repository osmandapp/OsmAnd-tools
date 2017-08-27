package net.osmand.data.diff;

import gnu.trove.map.hash.TLongObjectHashMap;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import net.osmand.IndexConstants;
import net.osmand.binary.BinaryMapDataObject;
import net.osmand.binary.BinaryMapIndexReader.MapIndex;
import net.osmand.binary.MapZooms;
import net.osmand.binary.MapZooms.MapZoomPair;
import net.osmand.binary.OsmandOdb;
import net.osmand.data.index.IndexUploader;
import net.osmand.data.preparation.AbstractIndexPartCreator;
import net.osmand.data.preparation.BinaryFileReference;
import net.osmand.data.preparation.BinaryMapIndexWriter;
import net.osmand.data.preparation.IndexVectorMapCreator;
import net.osmand.util.Algorithms;
import rtree.LeafElement;
import rtree.RTree;
import rtree.RTreeException;
import rtree.Rect;

import com.google.protobuf.CodedOutputStream;

public class ObfFileInMemory {
	private Map<MapZooms.MapZoomPair, TLongObjectHashMap<BinaryMapDataObject>> mapObjects = new LinkedHashMap<>();
	private long timestamp = 0;
	private String name; 

	public TLongObjectHashMap<BinaryMapDataObject> get(MapZooms.MapZoomPair zoom) {
		if (mapObjects.containsKey(zoom)) {
			mapObjects.put(zoom, new TLongObjectHashMap<BinaryMapDataObject>());
		}
		return mapObjects.get(zoom);
	}

	public void writeFile(File f) throws IOException, RTreeException {
		final RandomAccessFile raf = new RandomAccessFile(f, "rw");
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

		timestamp = timestamp == 0 ? System.currentTimeMillis() : timestamp;
		int version = IndexConstants.BINARY_MAP_VERSION;
		ous.writeInt32(OsmandOdb.OsmAndStructure.VERSION_FIELD_NUMBER, version);
		ous.writeInt64(OsmandOdb.OsmAndStructure.DATECREATED_FIELD_NUMBER, timestamp);
		BinaryMapIndexWriter writer = new BinaryMapIndexWriter(raf, ous);
		if (mapObjects.size() > 0) {
			if(name == null) {
				name = f.getName().substring(0, f.getName().indexOf('.'));
			}
			writer.startWriteMapIndex(Algorithms.capitalizeFirstLetter(name));
			// TODO
			writer.writeMapEncodingRules(mapIndex);
			Iterator<Entry<MapZoomPair, TLongObjectHashMap<BinaryMapDataObject>>> it = mapObjects.entrySet().iterator();
			while (it.hasNext()) {
				Entry<MapZoomPair, TLongObjectHashMap<BinaryMapDataObject>> n = it.next();
				writeMapData(writer, n.getKey(), n.getValue(), f);
			}
			writer.endWriteMapIndex();
		}

		// writePoiData(ous);
		ous.writeInt32(OsmandOdb.OsmAndStructure.VERSIONCONFIRM_FIELD_NUMBER, version);
		ous.flush();
		raf.close();
		f.setLastModified(timestamp);
	}

	private void writeMapData(BinaryMapIndexWriter writer, MapZoomPair mapZoomPair,
			TLongObjectHashMap<BinaryMapDataObject> objects, File fileToWrite) throws IOException, RTreeException {
		File nonpackRtree = new File(fileToWrite.getParentFile(), "nonpack" + mapZoomPair.getMinZoom() + "."
				+ fileToWrite.getName() + ".rtree");
		File packRtree = new File(fileToWrite.getParentFile(), "pack" + mapZoomPair.getMinZoom() + "."
				+ fileToWrite.getName() + ".rtree");
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
				writer.startWriteMapLevelIndex(mapZoomPair.getMinZoom(), mapZoomPair.getMaxZoom(),
						rootBounds.getMinX(), rootBounds.getMaxX(), rootBounds.getMinY(), rootBounds.getMaxY());
				IndexVectorMapCreator.writeBinaryMapTree(root, rootBounds, rtree, writer, treeHeader);

				IndexUploader.writeBinaryMapBlock(root, rootBounds, rtree, writer, treeHeader, objects, mapZoomPair);
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

	public void updateTimestamp(long dateCreated) {
		timestamp = Math.max(timestamp, dateCreated);
	}
}