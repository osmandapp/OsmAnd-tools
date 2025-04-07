package net.osmand.obf.diff;

import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Message;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.TLongLongMap;
import gnu.trove.map.hash.TLongLongHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import net.osmand.IndexConstants;
import net.osmand.PlatformUtil;
import net.osmand.ResultMatcher;
import net.osmand.binary.*;
import net.osmand.binary.BinaryMapIndexReader.MapIndex;
import net.osmand.binary.BinaryMapIndexReader.MapRoot;
import net.osmand.binary.BinaryMapIndexReader.SearchFilter;
import net.osmand.binary.BinaryMapIndexReader.SearchRequest;
import net.osmand.binary.BinaryMapPoiReaderAdapter.PoiRegion;
import net.osmand.binary.BinaryMapRouteReaderAdapter.RouteRegion;
import net.osmand.binary.BinaryMapRouteReaderAdapter.RouteSubregion;
import net.osmand.binary.BinaryMapTransportReaderAdapter.TransportIndex;
import net.osmand.binary.MapZooms.MapZoomPair;
import net.osmand.data.Amenity;
import net.osmand.data.TransportRoute;
import net.osmand.data.TransportStop;
import net.osmand.obf.preparation.*;
import net.osmand.obf.preparation.IndexRouteCreator.RouteWriteContext;
import net.osmand.osm.MapRenderingTypesEncoder;
import net.osmand.osm.edit.Way;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;
import org.apache.commons.logging.Log;
import rtree.LeafElement;
import rtree.RTree;
import rtree.RTreeException;
import rtree.Rect;

import java.io.*;
import java.sql.SQLException;
import java.util.*;
import java.util.Map.Entry;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class ObfFileInMemory {
	private static final int ZOOM_LEVEL_POI = 15;
	private static final int ZOOM_LEVEL_ROUTING = 15;
	private double lattop = 85;
	private double latbottom = -85;
	private double lonleft = -179.9;
	private double lonright = 179.9;
	private static final Log LOG = PlatformUtil.getLog(ObfFileInMemory.class);

	private Map<MapZooms.MapZoomPair, TLongObjectHashMap<BinaryMapDataObject>> mapObjects = new LinkedHashMap<>();
	private TLongObjectHashMap<RouteDataObject> routeObjects = new TLongObjectHashMap<>();
	private long timestamp = 0;
	private BinaryMapIndexReader.OsmAndOwner osmAndOwner;
	private MapIndex mapIndex = new MapIndex();
	private RouteRegion routeIndex = new RouteRegion();

	private TLongObjectHashMap<Map<String, Amenity>> poiObjects = new TLongObjectHashMap<>();

	private TLongObjectHashMap<TransportStop> transportStops = new TLongObjectHashMap<>();
	private TLongObjectHashMap<TransportRoute> transportRoutes = new TLongObjectHashMap<>();

	public TLongObjectHashMap<BinaryMapDataObject> get(MapZooms.MapZoomPair zoom) {
		if (!mapObjects.containsKey(zoom)) {
			mapObjects.put(zoom, new TLongObjectHashMap<BinaryMapDataObject>());
		}
		return mapObjects.get(zoom);
	}

	public Collection<MapZooms.MapZoomPair> getZooms() {
		return mapObjects.keySet();
	}

	public TLongObjectHashMap<RouteDataObject> getRoutingData() {
		return routeObjects;
	}

	public MapIndex getMapIndex() {
		return mapIndex;
	}

	public RouteRegion getRouteIndex() {
		return routeIndex;
	}

	public TLongObjectHashMap<Map<String, Amenity>> getPoiObjects() {
		return poiObjects;
	}

	public TLongObjectHashMap<TransportStop> getTransportStops() {
		return transportStops;
	}

	public TLongObjectHashMap<TransportRoute> getTransportRoutes() {
		return transportRoutes;
	}

	public void setTransportRoutes(TLongObjectHashMap<TransportRoute> transportRoutes) {
		this.transportRoutes = transportRoutes;
	}

	public void putMapObjects(MapZoomPair pair, Collection<BinaryMapDataObject> objects, boolean override) {
		TLongObjectHashMap<BinaryMapDataObject> res = get(pair);
		for (BinaryMapDataObject o : objects) {
			o = mapIndex.adoptMapObject(o);
			if (override) {
				res.put(o.getId(), o);
			} else if (!res.containsKey(o.getId())) {
				res.put(o.getId(), o);
			}

		}
	}

	public void writeFile(File targetFile, boolean doNotSimplifyObjects) throws IOException, RTreeException, SQLException {
		boolean gzip = targetFile.getName().endsWith(".gz");
		File nonGzip = targetFile;
		if(gzip) {
			nonGzip = new File(targetFile.getParentFile(),
				targetFile.getName().substring(0, targetFile.getName().length() - 3));
		}
		final RandomAccessFile raf = new RandomAccessFile(nonGzip, "rw");
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
		String defName = targetFile.getName().substring(0, targetFile.getName().indexOf('.'));
		if (mapObjects.size() > 0) {
			String name = mapIndex.getName();
			if(Algorithms.isEmpty(name)) {
				name = defName;
			}
			writer.startWriteMapIndex(Algorithms.capitalizeFirstLetter(name));
			writer.writeMapEncodingRules(mapIndex.decodingRules);
			Iterator<Entry<MapZoomPair, TLongObjectHashMap<BinaryMapDataObject>>> it = mapObjects.entrySet().iterator();
			while (it.hasNext()) {
				Entry<MapZoomPair, TLongObjectHashMap<BinaryMapDataObject>> n = it.next();
				writeMapData(writer, n.getKey(), n.getValue(), targetFile, doNotSimplifyObjects);
			}
			writer.endWriteMapIndex();
		}
		if (routeObjects.size() > 0) {
			String name = mapIndex.getName();
			if(Algorithms.isEmpty(name)) {
				name = defName;
			}
			writer.startWriteRouteIndex(name);
			writer.writeRouteRawEncodingRules(routeIndex.routeEncodingRules);
			writeRouteData(writer, routeObjects, targetFile);

			writer.endWriteRouteIndex();
		}
		if (poiObjects.size() > 0) {
			String name = "";
			if(Algorithms.isEmpty(name)) {
				name = defName;
			}
			MapRenderingTypesEncoder renderingTypes = new MapRenderingTypesEncoder(null, name);
			IndexCreatorSettings settings = new IndexCreatorSettings();
			settings.indexPOI = true;
			final IndexPoiCreator indexPoiCreator = new IndexPoiCreator(settings, renderingTypes);
			File poiFile = new File(targetFile.getParentFile(), IndexCreator.getPoiFileName(name));
			indexPoiCreator.createDatabaseStructure(poiFile);
			for (Map<String, Amenity> mp : poiObjects.valueCollection()) {
				for (Amenity a : mp.values()) {
					indexPoiCreator.insertAmenityIntoPoi(a);
				}
			}
			indexPoiCreator.writeBinaryPoiIndex(writer, name, null);
			indexPoiCreator.commitAndClosePoiFile(System.currentTimeMillis());
			indexPoiCreator.removePoiFile();
		}
		if (transportStops.size() > 0) {
			String name = mapIndex.getName();
			if(Algorithms.isEmpty(name)) {
				name = defName;
			}

			IndexCreatorSettings settings = new IndexCreatorSettings();
			settings.indexTransport = true;
			IndexTransportCreator indexCreator = new IndexTransportCreator(settings);
			Map<String, Integer> stringTable = indexCreator.createStringTableForTransport();
			Map<Long, Long> newRoutesIds = new LinkedHashMap<>();

			writer.startWriteTransportIndex(Algorithms.capitalizeFirstLetter(name));
			TLongObjectHashMap<TransportRoute> transportRoutes = new TLongObjectHashMap<>();
			for (TransportStop transportStop : transportStops.valueCollection()) {
				long[] routesIds = transportStop.getRoutesIds();
				if (routesIds != null) {
					for (long routeId : routesIds) {
						TransportRoute route = this.transportRoutes.get(routeId);
						if (route != null) {
							transportRoutes.put(routeId, route);
						}
					}
				}
			}
			if (transportRoutes.size() > 0) {
				writer.startWriteTransportRoutes();
				ByteArrayOutputStream ows = new ByteArrayOutputStream();
				List<byte[]> directGeometry = new ArrayList<>();
				TransportTags transportTags = new TransportTags();
				for (TransportRoute route : transportRoutes.valueCollection()) {
					directGeometry.clear();
					List<Way> ways = route.getForwardWays();
					if (ways != null && ways.size() > 0) {
						for (Way w : ways) {
							if (w.getNodes().size() > 0) {
								indexCreator.writeWay(ows, w);
								directGeometry.add(ows.toByteArray());
							}
						}
					}
					transportTags.registerTagValues(route.getId(), route.getTags());
					writer.writeTransportRoute(route.getId(), route.getName(), route.getEnName(false),
							route.getRef(), route.getOperator(), route.getType(), route.getDistance(),
							route.getColor(), route.getForwardStops(), directGeometry,
							stringTable, newRoutesIds, route.getSchedule(),
							transportTags);
				}
				writer.endWriteTransportRoutes();
			}
			for (TransportStop stop : transportStops.valueCollection()) {
				long[] routesIds = stop.getRoutesIds();
				long[] nrefs = null;
				if (routesIds != null) {
					nrefs = new long[routesIds.length];
					for (int i = 0; i < routesIds.length; i++) {
						Long vl = newRoutesIds.get(routesIds[i]);
						if(vl == null) {
							throw new IllegalStateException(
									String.format("Transport stop (%s) has reference to route %d but it wasn't found in the list",
									stop, routesIds[i] / 2));
						}
						nrefs[i] = vl.intValue();
					}
				}
				stop.setReferencesToRoutes(nrefs);
			}

			writeTransportStops(indexCreator, writer, transportStops, stringTable, targetFile);
			writer.writeTransportStringTable(stringTable);
			writer.endWriteTransportIndex();
		}

		if (osmAndOwner != null) {
			OsmandOdb.OsmAndOwner.Builder b = OsmandOdb.OsmAndOwner.newBuilder();
			b.setName(osmAndOwner.getName());
			if (!osmAndOwner.getResource().isEmpty()) {
				b.setResource(osmAndOwner.getResource());
			}
			if (!osmAndOwner.getDescription().isEmpty()) {
				b.setDescription(osmAndOwner.getDescription());
			}
			if (!osmAndOwner.getPluginid().isEmpty()) {
				b.setPluginid(osmAndOwner.getPluginid());
			}
			Message m = b.build();
			ous.writeMessage(OsmandOdb.OsmAndStructure.OWNER_FIELD_NUMBER, m);
		}

		ous.writeInt32(OsmandOdb.OsmAndStructure.VERSIONCONFIRM_FIELD_NUMBER, version);
		ous.flush();
		raf.close();

		if (gzip) {
			nonGzip.setLastModified(timestamp);

			FileInputStream fis = new FileInputStream(nonGzip);
			GZIPOutputStream gzout = new GZIPOutputStream(new FileOutputStream(targetFile));
			Algorithms.streamCopy(fis, gzout);
			fis.close();
			gzout.close();
			nonGzip.delete();
		}
		targetFile.setLastModified(timestamp);
	}

	private void writeTransportStops(IndexTransportCreator indexCreator, BinaryMapIndexWriter writer,
									 TLongObjectHashMap<TransportStop> transportStops, Map<String, Integer> stringTable,
									 File fileToWrite) throws IOException, RTreeException, SQLException {
		File nonpackRtree = new File(fileToWrite.getParentFile(), "nonpacktrans." + fileToWrite.getName() + ".rtree");
		File packRtree = new File(fileToWrite.getParentFile(), "packtrans." + fileToWrite.getName() + ".rtree");
		RTree rtree = null;
		try {
			rtree = new RTree(nonpackRtree.getAbsolutePath());
			for (TransportStop s : transportStops.valueCollection()) {
				int x = (int) MapUtils.getTileNumberX(24, s.getLocation().getLongitude());
				int y = (int) MapUtils.getTileNumberY(24, s.getLocation().getLatitude());
				try {
					rtree.insert(new LeafElement(new Rect(x, y, x, y), s.getId()));
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
			rtree = AbstractIndexPartCreator.packRtreeFile(rtree, nonpackRtree.getAbsolutePath(),
					packRtree.getAbsolutePath());

			long rootIndex = rtree.getFileHdr().getRootIndex();
			rtree.Node root = rtree.getReadNode(rootIndex);
			Rect rootBounds = IndexVectorMapCreator.calcBounds(root);
			if (rootBounds != null) {
				writer.startTransportTreeElement(rootBounds.getMinX(), rootBounds.getMaxX(), rootBounds.getMinY(), rootBounds.getMaxY());
				indexCreator.writeBinaryTransportTree(root, rtree, writer, transportStops, stringTable);
				writer.endWriteTransportTreeElement();
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

	private void writeRouteData(BinaryMapIndexWriter writer, TLongObjectHashMap<RouteDataObject> routeObjs, File fileToWrite) throws IOException, RTreeException, SQLException {
		File nonpackRtree = new File(fileToWrite.getParentFile(), "nonpackroute."
				+ fileToWrite.getName() + ".rtree");
		File packRtree = new File(fileToWrite.getParentFile(), "packroute."
				+ fileToWrite.getName() + ".rtree");
		RTree rtree = null;
		try {
			rtree = new RTree(nonpackRtree.getAbsolutePath());
			for (long key : routeObjs.keys()) {
				RouteDataObject obj = routeObjs.get(key);
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
			Rect rootBounds = IndexVectorMapCreator.calcBounds(root);
			if (rootBounds != null) {
				IndexRouteCreator.writeBinaryRouteTree(root, rootBounds, rtree, writer, treeHeader, false);
				RouteWriteContext wc = new RouteWriteContext(null, treeHeader, null, routeObjs);
				IndexRouteCreator.writeBinaryMapBlock(root, rootBounds, rtree, writer, wc, false);
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

	private void writeMapData(BinaryMapIndexWriter writer, MapZoomPair mapZoomPair,
			TLongObjectHashMap<BinaryMapDataObject> objects, File fileToWrite, boolean doNotSimplify) throws IOException, RTreeException {
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
			Rect rootBounds = IndexVectorMapCreator.calcBounds(root);
			if (rootBounds != null) {
				writer.startWriteMapLevelIndex(mapZoomPair.getMinZoom(), mapZoomPair.getMaxZoom(),
						rootBounds.getMinX(), rootBounds.getMaxX(), rootBounds.getMinY(), rootBounds.getMaxY());
				IndexVectorMapCreator.writeBinaryMapTree(root, rootBounds, rtree, writer, treeHeader);

				IndexVectorMapCreator.writeBinaryMapBlock(root, rootBounds, rtree, writer, treeHeader, objects, mapZoomPair,
						doNotSimplify);
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

	public long getTimestamp() {
		return timestamp;
	}

	public void readObfFiles(List<File> files) throws IOException {
		for (int i = 0; i < files.size(); i++) {
			File inputFile = files.get(i);
			File nonGzip = inputFile;
			boolean gzip = false;
			if (inputFile == null) {
				continue;
			}
			File parentFile = inputFile.getParentFile();
			LOG.info(String.format("Reading %s / %s ", parentFile == null ? "" : parentFile.getName(),
					inputFile.getName()));
			if (inputFile.getName().endsWith(".gz")) {
				nonGzip = new File(inputFile.getParentFile(), inputFile.getName().substring(0, inputFile.getName().length() - 3));
				GZIPInputStream gzin = new GZIPInputStream(new FileInputStream(inputFile));
				FileOutputStream fous = new FileOutputStream(nonGzip);
				Algorithms.streamCopy(gzin, fous);
				fous.close();
				gzin.close();
				gzip = true;
			}
			RandomAccessFile raf = new RandomAccessFile(nonGzip, "r");
			BinaryMapIndexReader indexReader = new BinaryMapIndexReader(raf, nonGzip);
			for (BinaryIndexPart p : indexReader.getIndexes()) {
				if (p instanceof MapIndex) {
					MapIndex mi = (MapIndex) p;
					for (MapRoot mr : mi.getRoots()) {
						MapZooms.MapZoomPair pair = new MapZooms.MapZoomPair(mr.getMinZoom(), mr.getMaxZoom());
						TLongObjectHashMap<BinaryMapDataObject> objects = readBinaryMapData(indexReader, mi,
								mr.getMinZoom());
						putMapObjects(pair, objects.valueCollection(), true);
					}
				} else if (p instanceof RouteRegion) {
					RouteRegion rr = (RouteRegion) p;
					readRoutingData(indexReader, rr, ZOOM_LEVEL_ROUTING, true);
				} else if (p instanceof PoiRegion) {
					PoiRegion pr = (PoiRegion) p;
					TLongObjectHashMap<Map<String, Amenity>> rr = readPoiData(indexReader, pr, ZOOM_LEVEL_POI, true);
					putPoiData(rr, true);
				} else if (p instanceof TransportIndex) {
					readTransportData(indexReader, (TransportIndex) p, true);
				}
			}

			updateTimestamp(indexReader.getDateCreated());
			setOsmAndOwner(indexReader.getOwner());
			indexReader.close();
			raf.close();
			if(gzip) {
				nonGzip.delete();
			}
		}
	}

	public void readTransportData(BinaryMapIndexReader indexReader, TransportIndex ind, boolean override) throws IOException {
		SearchRequest<TransportStop> sr = BinaryMapIndexReader.buildSearchTransportRequest(
				MapUtils.get31TileNumberX(lonleft),
				MapUtils.get31TileNumberX(lonright),
				MapUtils.get31TileNumberY(lattop),
				MapUtils.get31TileNumberY(latbottom),
				-1, null);
		List<TransportStop> sti = indexReader.searchTransportIndex(ind, sr);
		// merged could not be overridden
		TLongLongMap routesData = putTransportStops(sti, override);
		if (routesData.size() > 0) {
			long[] filePointers = routesData.keys();
			// reads all route
			TLongObjectHashMap<TransportRoute> transportRoutes = indexReader.getTransportRoutes(filePointers);
			TLongIterator it = transportRoutes.keySet().iterator();
			while (it.hasNext()) {
				long offset = it.next();
				TransportRoute route = transportRoutes.get(offset);
				Long routeId = route.getId();
				if (override || !this.transportRoutes.containsKey(routeId)) {
					this.transportRoutes.put(routeId, route);
				}
			}
		}
	}

	public TLongLongMap putTransportStops(Collection<TransportStop> newData, boolean override) {
		TLongLongMap routesStopsData = new TLongLongHashMap();
		for (TransportStop ts : newData) {
			Long tid = ts.getId();
			if (routesStopsData != null) {
				long[] referencesToRoutes = ts.getReferencesToRoutes();
				if (referencesToRoutes != null && referencesToRoutes.length > 0) {
					for (long ref : referencesToRoutes) {
						routesStopsData.put(ref, tid);
					}
				}
			}
			TransportStop previousStop = transportStops.get(tid);
			if (previousStop != null) {
				if(override) {
					mergeStop(ts, previousStop);
					transportStops.put(tid, ts);
				} else {
					mergeStop(previousStop, ts);
				}

			} else {
				transportStops.put(tid, ts);
			}
		}
		return routesStopsData;
	}

	private void mergeStop(TransportStop stopToMergeInto, TransportStop stopToMergeFrom) {
		boolean ridsNotEmpty = stopToMergeInto.getRoutesIds() != null && stopToMergeInto.getRoutesIds().length > 0;
		boolean rdidsNotEmpty = stopToMergeInto.getDeletedRoutesIds() != null
				&& stopToMergeInto.getDeletedRoutesIds().length > 0;
		// merge into only for new stops that have route ids (!)
		if (!stopToMergeInto.isDeleted() && (!ridsNotEmpty || !rdidsNotEmpty)) {
			if (stopToMergeFrom.getRoutesIds() != null) {
				for (long routeId : stopToMergeFrom.getRoutesIds()) {
					if (!stopToMergeInto.hasRoute(routeId) && !stopToMergeInto.isRouteDeleted(routeId)) {
						stopToMergeInto.addRouteId(routeId);
					}
				}
			}
			if (stopToMergeFrom.getDeletedRoutesIds() != null) {
				for (long routeId : stopToMergeFrom.getDeletedRoutesIds()) {
					if (!stopToMergeInto.hasRoute(routeId) && !stopToMergeInto.isRouteDeleted(routeId)) {
						stopToMergeInto.addDeletedRouteId(routeId);
					}
				}
			}
		}
	}

	public TLongObjectHashMap<Map<String, Amenity>> readPoiData(BinaryMapIndexReader indexReader, PoiRegion pr, int zoomLevelPoi, final boolean override) throws IOException {
		final TLongObjectHashMap<Map<String, Amenity>> local = new TLongObjectHashMap<>();
		SearchRequest<Amenity> req = BinaryMapIndexReader.buildSearchPoiRequest(
			MapUtils.get31TileNumberX(lonleft),	MapUtils.get31TileNumberX(lonright),
			MapUtils.get31TileNumberY(lattop), MapUtils.get31TileNumberY(latbottom),
			ZOOM_LEVEL_POI,	BinaryMapIndexReader.ACCEPT_ALL_POI_TYPE_FILTER,
			new ResultMatcher<Amenity>() {
				@Override
				public boolean publish(Amenity object) {
                    int order = 0;
					if(!local.containsKey(object.getId())) {
						local.put(object.getId(), new LinkedHashMap<String, Amenity>());
					} else {
                        Map<String, Amenity> map = local.get(object.getId());
                        for (Amenity am : map.values()) {
                            order = Math.max(am.getOrder(), order);
                        }
                    }
                    if (object.getOrder() == 0) {
                        object.setOrder(order + 1);
                    }
					local.get(object.getId()).put(object.getType().getKeyName(), object);
					return false;
				}

				@Override
				public boolean isCancelled() {
					return false;
				}
			});
		indexReader.initCategories(pr);
		indexReader.searchPoi(pr, req);
		return local;
	}

	public void putPoiData(TLongObjectHashMap<Map<String, Amenity>> newData, boolean override) {
		TLongObjectIterator<Map<String, Amenity>> it = newData.iterator();
		while(it.hasNext()) {
			it.advance();
			if (override || !poiObjects.containsKey(it.key())) {
				poiObjects.put(it.key(), it.value());
			}
		}
	}

	public void putRoutingData(TLongObjectHashMap<RouteDataObject> ro, boolean override) {
		for (RouteDataObject obj : ro.valueCollection()) {
			if (override || !routeObjects.containsKey(obj.getId())) {
				routeObjects.put(obj.getId(), routeIndex.adopt(obj));
			}
		}
	}

	public void readRoutingData(BinaryMapIndexReader indexReader, RouteRegion rr, int zm, final boolean override) throws IOException {
		List<RouteSubregion> regions = indexReader.searchRouteIndexTree(
				BinaryMapIndexReader.buildSearchRequest(MapUtils.get31TileNumberX(lonleft),
						MapUtils.get31TileNumberX(lonright), MapUtils.get31TileNumberY(lattop),
						MapUtils.get31TileNumberY(latbottom), zm, null),
				rr.getSubregions());

		indexReader.loadRouteIndexData(regions, new ResultMatcher<RouteDataObject>() {
			@Override
			public boolean publish(RouteDataObject obj) {
				if(override || !routeObjects.containsKey(obj.getId())) {
					RouteDataObject ad = routeIndex.adopt(obj);
					routeObjects.put(ad.getId(), ad);
				}
				return true;
			}

			@Override
			public boolean isCancelled() {
				return false;
			}
		});
	}

	private TLongObjectHashMap<BinaryMapDataObject> readBinaryMapData(BinaryMapIndexReader index, MapIndex mi, int zoom)
			throws IOException {
		final TLongObjectHashMap<BinaryMapDataObject> result = new TLongObjectHashMap<>();
		final SearchRequest<BinaryMapDataObject> req = BinaryMapIndexReader.buildSearchRequest(
				MapUtils.get31TileNumberX(lonleft), MapUtils.get31TileNumberX(lonright),
				MapUtils.get31TileNumberY(lattop), MapUtils.get31TileNumberY(latbottom),
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
		index.searchMapIndex(req, mi);
		return result;
	}


	public void filterAllZoomsBelow(int zm) {
		for(MapZoomPair mz : new ArrayList<>(getZooms())) {
			if(mz.getMaxZoom() < zm) {
				mapObjects.remove(mz);
			}
		}
	}

	public void setOsmAndOwner(BinaryMapIndexReader.OsmAndOwner owner) {
		osmAndOwner = owner;
	}
	
}
