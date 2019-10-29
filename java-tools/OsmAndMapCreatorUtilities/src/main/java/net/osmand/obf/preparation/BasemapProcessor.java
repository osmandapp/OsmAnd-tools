package net.osmand.obf.preparation;


import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.hash.TLongObjectHashMap;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import javax.xml.stream.XMLStreamException;

import net.osmand.PlatformUtil;
import net.osmand.binary.MapZooms;
import net.osmand.binary.MapZooms.MapZoomPair;
import net.osmand.binary.OsmandOdb.MapData;
import net.osmand.binary.OsmandOdb.MapDataBlock;
import net.osmand.data.QuadRect;
import net.osmand.impl.ConsoleProgressImplementation;
import net.osmand.osm.MapRenderingTypes.MapRulType;
import net.osmand.osm.MapRenderingTypesEncoder;
import net.osmand.osm.WayChain;
import net.osmand.osm.edit.Entity;
import net.osmand.osm.edit.Node;
import net.osmand.osm.edit.OsmMapUtils;
import net.osmand.osm.edit.Relation;
import net.osmand.osm.edit.Relation.RelationMember;
import net.osmand.osm.edit.Way;
import net.osmand.util.Algorithms;
import net.osmand.util.MapAlgorithms;
import net.osmand.util.MapUtils;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.logging.Log;
import org.xmlpull.v1.XmlPullParserException;

public class BasemapProcessor {
    TLongObjectHashMap<WayChain> coastlinesEndPoint = new TLongObjectHashMap<WayChain>();
    TLongObjectHashMap<WayChain> coastlinesStartPoint = new TLongObjectHashMap<WayChain>();

    private static final byte SEA = 0x2;
    private static final byte LAND = 0x1;
    private static final Log log = PlatformUtil.getLog(BasemapProcessor.class);
	public static final int PIXELS_THRESHOLD_AREA = 24;


    /**
     * The zoom level for which the tile info is valid.
     */
    public static final byte TILE_ZOOMLEVEL = 12;
    private static final byte BITMASK = 0x3;
    private static final int BITS_COUNT = (1 << TILE_ZOOMLEVEL) * (1 << TILE_ZOOMLEVEL);

    private BitSet seaTileInfo = new BitSet(BITS_COUNT);
    private BitSet landTileInfo = new BitSet(BITS_COUNT);
    private TIntArrayList typeUse = new TIntArrayList();
    List<MapRulType> tempNameUse = new ArrayList<MapRulType>();
    TIntArrayList addtypeUse = new TIntArrayList(8);
    TreeMap<MapRulType, String> namesUse = new TreeMap<MapRulType, String>(new Comparator<MapRulType>() {

		@Override
		public int compare(MapRulType o1, MapRulType o2) {
			int lhs = o1.getOrder();
			int rhs = o2.getOrder();
			if(lhs == rhs) {
				lhs = o1.getInternalId();
				rhs = o2.getInternalId();
			}
			return lhs < rhs ? -1 : (lhs == rhs ? 0 : 1);
		}
	});

    private final int zoomWaySmoothness;
    private final MapRenderingTypesEncoder renderingTypes;
    private final MapZooms mapZooms;
    private final Log logMapDataWarn;
    private SimplisticQuadTree[] quadTrees;
    private static int MOST_DETAILED_APPROXIMATION = 9;

    protected static class SimplisticQuadTree {
        int zoom;
        int x;
        int y;
        float seaCharacteristic;

        public SimplisticQuadTree(int x, int y, int zoom) {
            this.x = x;
            this.y = y;
            this.zoom = zoom;
        }

        SimplisticQuadTree[] children = null;
        Map<MapZoomPair, List<SimplisticBinaryData>> dataObjects = null;


        public SimplisticQuadTree[] getAllChildren() {
            initChildren();
            return children;
        }

        public boolean areChildrenDefined() {
            return children != null;
        }

        public void addQuadData(MapZoomPair p, SimplisticBinaryData w) {

            if (dataObjects == null) {
                dataObjects = new LinkedHashMap<MapZooms.MapZoomPair, List<SimplisticBinaryData>>();
            }
            if (!dataObjects.containsKey(p)) {
                dataObjects.put(p, new ArrayList<SimplisticBinaryData>());
            }
            dataObjects.get(p).add(w);
        }

        public boolean dataIsDefined(MapZoomPair p) {
            return dataObjects != null && dataObjects.get(p) != null;
        }

        public List<SimplisticBinaryData> getData(MapZoomPair p) {
            return dataObjects.get(p);
        }

        public SimplisticQuadTree getOrCreateSubTree(int x, int y, int zm) {
            if (zm <= zoom) {
                return this;
            } else {
                initChildren();
                int nx = (x >> (zm - zoom - 1)) - (this.x << 1);
                int ny = (y >> (zm - zoom - 1)) - (this.y << 1);
                if (nx > 1 || nx < 0 || ny > 1 || ny < 0) {
                    return null;
                }
                return children[nx * 2 + ny].getOrCreateSubTree(x, y, zm);
            }
        }

        private void initChildren() {
            if (children == null) {
                children = new SimplisticQuadTree[4];
                for (int i = 0; i < 2; i++) {
                    for (int j = 0; j < 2; j++) {
                        children[i * 2 + j] = new SimplisticQuadTree(((this.x << 1) + i), ((this.y << 1) + j), zoom + 1);
                        children[i * 2 + j].seaCharacteristic = seaCharacteristic;
                    }
                }
            }
        }
    }


    private static class SimplisticBinaryData {
        // consequent 31 coordinates
        public byte[] coordinates;
	    public byte[] innerCoordinates;
        public int[] types;
        public int[] addTypes;
        public long id = -500;
        public Map<MapRulType, String> names;
    }

    protected BasemapProcessor() {
        logMapDataWarn = null;
        zoomWaySmoothness = 0;
        renderingTypes = null;
        mapZooms = null;
    }

    public BasemapProcessor(Log logMapDataWarn, MapZooms mapZooms, MapRenderingTypesEncoder renderingTypes, int zoomWaySmoothness) {
        this.logMapDataWarn = logMapDataWarn;
        this.mapZooms = mapZooms;
        this.renderingTypes = renderingTypes;
        this.zoomWaySmoothness = zoomWaySmoothness;
        constructBitSetInfo(null);
        quadTrees = new SimplisticQuadTree[mapZooms.getLevels().size()];
        for (int i = 0; i < mapZooms.getLevels().size(); i++) {
            MapZoomPair p = mapZooms.getLevels().get(i);
            quadTrees[i] = constructTilesQuadTree(Math.min(p.getMaxZoom(), 11));
        }
    }

    protected void constructBitSetInfo(String datFile) {
        try {
        	InputStream dis;
			if (datFile == null) {
				dis = new BZip2CompressorInputStream(BasemapProcessor.class.getResourceAsStream("oceantiles_12.dat.bz2"));
			} else {
        		dis = new FileInputStream(datFile);
        	}
            int currentByte;
            for (int i = 0; i < BITS_COUNT / 4; i++) {
                currentByte = dis.read();
                if (((currentByte >> 6) & BITMASK) == SEA) {
                    seaTileInfo.set(i * 4);
                } else if (((currentByte >> 6) & BITMASK) == LAND) {
                    landTileInfo.set(i * 4);
                }
                if (((currentByte >> 4) & BITMASK) == SEA) {
                    seaTileInfo.set(i * 4 + 1);
                } else if (((currentByte >> 4) & BITMASK) == LAND) {
                    landTileInfo.set(i * 4 + 1);
                }
                if (((currentByte >> 2) & BITMASK) == SEA) {
                    seaTileInfo.set(i * 4 + 2);
                } else if (((currentByte >> 2) & BITMASK) == LAND) {
                    landTileInfo.set(i * 4 + 2);
                }
                if ((currentByte & BITMASK) == SEA) {
                    seaTileInfo.set(i * 4 + 3);
                } else if ((currentByte & BITMASK) == LAND) {
                    landTileInfo.set(i * 4 + 3);
                }
            }
            dis.close();
        } catch (IOException e) {
            throw new RuntimeException("File with coastline tiles was not found ");
        }
    }

    public int getTileZoomLevel() {
        return TILE_ZOOMLEVEL;
    }

    public boolean isWaterTile(int x, int y, int zoom) {
        if (zoom >= TILE_ZOOMLEVEL) {
            int x1 = x >> (zoom - TILE_ZOOMLEVEL);
            int y1 = y >> (zoom - TILE_ZOOMLEVEL);
            if (!seaTileInfo.get(y1 * 4096 + x1)) {
                return false;
            }
            return true;
        } else {
            int x1 = x << (TILE_ZOOMLEVEL - zoom);
            int y1 = y << (TILE_ZOOMLEVEL - zoom);
            int max = 1 << TILE_ZOOMLEVEL - zoom;
            for (int i = 0; i < max; i++) {
                for (int j = 0; j < max; j++) {
                    if (!seaTileInfo.get((y1 + j) * 4096 + (x1 + i))) {
                        return false;
                    }
                }
            }
            return true;
        }
    }

    // get isLand returns > 0 for land and < 0 for water
    public float getSeaTile(int x, int y, int zoom) {
        if (zoom >= TILE_ZOOMLEVEL) {
            int x1 = x >> (zoom - TILE_ZOOMLEVEL);
            int y1 = y >> (zoom - TILE_ZOOMLEVEL);
//            if (landTileInfo.get(y1 * 4096 + x1)) {
//                return 1;
//            }
            if (seaTileInfo.get(y1 * 4096 + x1)) {
                return 1;
            }
            return 0;
        } else {
            int x1 = x << (TILE_ZOOMLEVEL - zoom);
            int y1 = y << (TILE_ZOOMLEVEL - zoom);
            int max = 1 << (TILE_ZOOMLEVEL - zoom);
            int c = 0;
            for (int i = 0; i < max; i++) {
                for (int j = 0; j < max; j++) {
//                    if (landTileInfo.get((y1 + i) * 4096 + (x1 + j))) {
//                        c++;
//                    }
                    if (seaTileInfo.get((y1 + i) * 4096 + (x1 + j))) {
                        c++;
                    }

                }
            }

            return ((float)c) / ((float) max * (float) max);
        }
    }

    public boolean isLandTile(int x, int y, int zoom) {
        if (zoom >= TILE_ZOOMLEVEL) {
            int x1 = x >> (zoom - TILE_ZOOMLEVEL);
            int y1 = y >> (zoom - TILE_ZOOMLEVEL);
            if (!landTileInfo.get(y1 * 4096 + x1)) {
                return false;
            }
            return true;
        } else {
            int x1 = x << (TILE_ZOOMLEVEL - zoom);
            int y1 = y << (TILE_ZOOMLEVEL - zoom);
            int max = 1 << (TILE_ZOOMLEVEL - zoom);
            for (int i = 0; i < max; i++) {
                for (int j = 0; j < max; j++) {
                    if (!landTileInfo.get((y1 + i) * 4096 + (x1 + j))) {
                        return false;
                    }
                }
            }
            return true;
        }
    }

    public SimplisticQuadTree constructTilesQuadTree(int maxZoom) {
        SimplisticQuadTree rootTree = new SimplisticQuadTree(0, 0, 0);


        int baseZoom = 2;
        int tiles = 1 << baseZoom;
        LinkedList<SimplisticQuadTree> toVisit = new LinkedList<SimplisticQuadTree>();
        for (int x = 0; x < tiles; x++) {
            for (int y = 0; y < tiles; y++) {
                toVisit.add(rootTree.getOrCreateSubTree(x, y, baseZoom));
            }
        }
        initializeQuadTree(rootTree, baseZoom, maxZoom, toVisit);
        return rootTree;

    }

    protected void initializeQuadTree(SimplisticQuadTree rootTree, int baseZoom, int maxZoom,
			LinkedList<SimplisticQuadTree> toVisit) {
		while (!toVisit.isEmpty()) {
			SimplisticQuadTree subtree = toVisit.poll();
			int x = subtree.x;
			int y = subtree.y;
			int zoom = subtree.zoom;
			SimplisticQuadTree st = rootTree.getOrCreateSubTree(x, y, zoom);
			st.seaCharacteristic = getSeaTile(x, y, zoom);
			if (zoom < maxZoom && !isWaterTile(x, y, zoom) && !isLandTile(x, y, zoom)) {
				SimplisticQuadTree[] vis = st.getAllChildren();
				Collections.addAll(toVisit, vis);
			}
		}
	}


    public void writeBasemapFile(BinaryMapIndexWriter writer, String regionName) throws IOException {
        writer.startWriteMapIndex(regionName);
        // write map encoding rules
        writer.writeMapEncodingRules(renderingTypes.getEncodingRuleTypes());

        int i = 0;
        for (MapZoomPair p : mapZooms.getLevels()) {
            // write map levels and map index
            writer.startWriteMapLevelIndex(p.getMinZoom(), p.getMaxZoom(), 0, (1 << 31) - 1, 0, (1 << 31) - 1);

            Map<SimplisticQuadTree, BinaryFileReference> refs = new LinkedHashMap<BasemapProcessor.SimplisticQuadTree, BinaryFileReference>();
            writeBinaryMapTree(quadTrees[i], writer, refs, p);

            // without data blocks
            writeBinaryMapBlock(quadTrees[i], writer, refs, p);

            writer.endWriteMapLevelIndex();
            i++;
        }
        writer.endWriteMapIndex();
        writer.flush();

    }

    private void writeBinaryMapBlock(SimplisticQuadTree simplisticQuadTree, BinaryMapIndexWriter writer,
                                     Map<SimplisticQuadTree, BinaryFileReference> refs, MapZoomPair level) throws IOException {
        Iterator<Entry<SimplisticQuadTree, BinaryFileReference>> it = refs.entrySet().iterator();

        while (it.hasNext()) {
            Entry<SimplisticQuadTree, BinaryFileReference> e = it.next();
            MapDataBlock.Builder dataBlock = MapDataBlock.newBuilder();
            SimplisticQuadTree quad = e.getKey();
            Map<String, Integer> stringTable = new LinkedHashMap<String, Integer>();
            long baseId = 0;
            for (SimplisticBinaryData w : quad.getData(level)) {
            	baseId = Math.min(w.id, baseId);
            }
            dataBlock.setBaseId(baseId);
            for (SimplisticBinaryData w : quad.getData(level)) {
                int[] wts = null;
                int[] wats = null;
                if (w.types != null) {
                    wts = new int[w.types.length];
                    for (int j = 0; j < w.types.length; j++) {
                        wts[j] = renderingTypes.getTypeByInternalId(w.types[j]).getTargetId();
                    }
                }
                if (w.addTypes != null) {
                    wats = new int[w.addTypes.length];
                    for (int j = 0; j < w.addTypes.length; j++) {
                        wats[j] = renderingTypes.getTypeByInternalId(w.addTypes[j]).getTargetId();
                    }
                }
                MapData mapData = writer.writeMapData(w.id - baseId,
                        quad.x << (31 - quad.zoom), quad.y << (31 - quad.zoom), false,
                        w.coordinates, w.innerCoordinates, wts, wats, w.names, null, stringTable, dataBlock, level.getMaxZoom() > 15);
                if (mapData != null) {
                    dataBlock.addDataObjects(mapData);
                }
            }

            writer.writeMapDataBlock(dataBlock, stringTable, e.getValue());
        }
    }

    private void writeBinaryMapTree(SimplisticQuadTree quadTree, BinaryMapIndexWriter writer,
                                    Map<SimplisticQuadTree, BinaryFileReference> refs, MapZoomPair p) throws IOException {
        int xL = (quadTree.x) << (31 - quadTree.zoom);
        int xR = ((quadTree.x + 1) << (31 - quadTree.zoom)) - 1;
        int yT = (quadTree.y) << (31 - quadTree.zoom);
        int yB = ((quadTree.y + 1) << (31 - quadTree.zoom)) - 1;
        boolean defined = quadTree.dataIsDefined(p);
        int sea = 0;
        if(!quadTree.areChildrenDefined()) {
        	sea = quadTree.seaCharacteristic > 0.5 ? -1 : 1;
        }
        BinaryFileReference ref = writer.startMapTreeElement(xL, xR, yT, yB, defined, sea);
        if (ref != null) {
            refs.put(quadTree, ref);
        }

        if (quadTree.areChildrenDefined()) {
            SimplisticQuadTree[] allChildren = quadTree.getAllChildren();

            for (SimplisticQuadTree ch : allChildren) {
                writeBinaryMapTree(ch, writer, refs, p);
            }
        }
        writer.endWriteMapTreeElement();

    }


	private static long ID = -20;
	public void processEntity(boolean mini, Entity e) {
		if (e instanceof Way) {
			if ("reverse_coastline".equals(((Way) e).getModifiableTags().get("natural"))) {
				((Way) e).putTag("natural", "coastline");
				Collections.reverse(((Way) e).getNodes());
				((Way) e).getNodeIds().reverse();
			}
		}
		long refId = -Math.abs(e.getId());
		
		boolean coastline = "coastline".equals(e.getTag("natural"));
		// save space with ids
		for (int level = 0; level < mapZooms.getLevels().size(); level++) {
			boolean mostDetailed = level == 0;
			MapZoomPair zoomPair = mapZooms.getLevel(level);
			if (mostDetailed) {
				if (!(e instanceof Node) && !(coastline)) {
					// store only coastline for mini basemap
					continue;
				}
			}
			int zoomToEncode = mostDetailed ? Math.max(MOST_DETAILED_APPROXIMATION, zoomPair.getMinZoom() + 1) : zoomPair.getMaxZoom();
			if (mostDetailed && zoomPair.getMaxZoom() < 10) {
				throw new IllegalStateException("Zoom pair is not detailed " + zoomPair);
			}
			renderingTypes.encodeEntityWithType(e, zoomToEncode, typeUse, addtypeUse, namesUse, tempNameUse);
			if (typeUse.isEmpty()) {
				continue;
			}
			if (e instanceof Relation) {
				Relation r = (Relation) e;
				Iterator<RelationMember> it = r.getMembers().iterator();
				List<Node> outer = null;
				List<List<Node>> inner = new ArrayList<List<Node>>();
				while (it.hasNext()) {
					RelationMember n = it.next();
					if (n.getRole().equals("outer")) {
						if (outer != null) {
							throw new IllegalStateException("2 outer lines for relation = " + e.getId());
						}
						outer = ((Way) n.getEntity()).getNodes();
					} else if (n.getRole().equals("inner")) {
						inner.add(((Way) n.getEntity()).getNodes());
					}

				}
				if (outer == null || OsmMapUtils.polygonAreaPixels(outer, zoomToEncode) < PIXELS_THRESHOLD_AREA) {
					continue;
				}
				addObject(refId, level, zoomPair, zoomToEncode, outer, inner);
			} else if (e instanceof Way) {
				if (((Way) e).getNodes().size() < 2) {
					continue;
				}
				double dist = OsmMapUtils.getDistance(((Way) e).getFirstNode(), ((Way) e).getLastNode());
				boolean polygon = dist < 100;
				if ("coastline".equals(e.getTag("natural"))) {
					if (polygon && !mostDetailed) {
						if (OsmMapUtils.polygonAreaPixels(((Way) e).getNodes(), zoomToEncode) < PIXELS_THRESHOLD_AREA) {
							continue;
						}
					}
					splitContinuousWay(((Way) e).getNodes(), typeUse.toArray(),
							!addtypeUse.isEmpty() ? addtypeUse.toArray() : null,
							zoomPair, zoomToEncode, quadTrees[level], refId);
				} else {
					List<Node> ns = ((Way) e).getNodes();
					if (!polygon) {
						QuadRect qr = ((Way) e).getLatLonBBox();
						if(qr == null) {
							continue;
						}
						double mult = 1 / MapUtils.getPowZoom(Math.max(31 - (zoomToEncode + 8), 0));
						int rx = MapUtils.get31TileNumberX(qr.right);
						int lx = MapUtils.get31TileNumberX(qr.left);
						int by = MapUtils.get31TileNumberY(qr.bottom);
						int ty = MapUtils.get31TileNumberY(qr.top);
						if(mult * (rx - lx) < PIXELS_THRESHOLD_AREA &&
								mult * (by - ty) < PIXELS_THRESHOLD_AREA ) {
							continue;
						}
					} else {
						if(OsmMapUtils.polygonAreaPixels(ns, zoomToEncode) < PIXELS_THRESHOLD_AREA){
							continue;
						}
					}

					addObject(refId, level, zoomPair, zoomToEncode, ns, null);
				}
			} else {
				int z = getViewZoom(zoomPair.getMinZoom(), zoomToEncode);
				int tilex = (int) MapUtils.getTileNumberX(z, ((Node) e).getLongitude());
				int tiley = (int) MapUtils.getTileNumberY(z, ((Node) e).getLatitude());
				addRawData(Collections.singletonList((Node) e), null, typeUse.toArray(), !addtypeUse.isEmpty() ? addtypeUse.toArray() : null, zoomPair,
						quadTrees[level], z, tilex, tiley, namesUse.isEmpty() ? null : new LinkedHashMap<MapRulType, String>(namesUse), refId);
			}

		}
	}

	private void addObject(long refId, int level, MapZoomPair zoomPair, int zoomToEncode, List<Node> way, List<List<Node>> inner) {
		int z = getViewZoom(zoomPair.getMinZoom(), zoomToEncode);
		int tilex = 0;
		int tiley = 0;
		boolean sameTile = false;
		while (!sameTile) {
		    tilex = (int) MapUtils.getTileNumberX(z, way.get(0).getLongitude());
		    tiley = (int) MapUtils.getTileNumberY(z, way.get(0).getLatitude());
		    sameTile = true;
		    for (int i = 1; i < way.size(); i++) {
		        int tx = (int) MapUtils.getTileNumberX(z, way.get(i).getLongitude());
		        int ty = (int) MapUtils.getTileNumberY(z, way.get(i).getLatitude());
		        if (tx != tilex || ty != tiley) {
		            sameTile = false;
		            break;
		        }
		    }
		    if (!sameTile) {
		        z--;
		    }
		}
		List<Node> res = new ArrayList<Node>();
		OsmMapUtils.simplifyDouglasPeucker(way, zoomToEncode - 1 + 8 + zoomWaySmoothness, 3, res, false);
		if(inner != null) {
			Iterator<List<Node>> it = inner.iterator();
			while(it.hasNext()) {
				List<Node> list = it.next();
				if (OsmMapUtils.polygonAreaPixels(list, zoomToEncode) < PIXELS_THRESHOLD_AREA) {
					it.remove();
				} else {
					OsmMapUtils.simplifyDouglasPeucker(list, zoomToEncode - 1 + 8 + zoomWaySmoothness, 4, list, false);
					if (list.size() <= 3) {
						it.remove();
					}
				}
			}
		}
		addRawData(res, inner, typeUse.toArray(), !addtypeUse.isEmpty() ? addtypeUse.toArray() : null, zoomPair,
				quadTrees[level], z, tilex, tiley, namesUse.isEmpty() ? null : new LinkedHashMap<MapRulType, String>(namesUse), refId);
	}

	public void splitContinuousWay(List<Node> ns, int[] types, int[] addTypes, MapZoomPair zoomPair, int zoomToEncode,
                                   SimplisticQuadTree quadTree, long refId) {
        int z = getViewZoom(zoomPair.getMinZoom(), zoomToEncode);
        int i = 1;
        Node prevNode = ns.get(0);
        int px31 = MapUtils.get31TileNumberX(prevNode.getLongitude());
        int py31 = MapUtils.get31TileNumberY(prevNode.getLatitude());
        while (i < ns.size()) {
            List<Node> w = new ArrayList<Node>();
            w.add(prevNode);
            int tilex = px31 >> (31 - z);
            int tiley = py31 >> (31 - z);
            boolean sameTile = true;
            wayConstruct:
            while (sameTile && i < ns.size()) {
                Node next = ns.get(i);
                int ntilex = (int) MapUtils.getTileNumberX(z, next.getLongitude());
                int ntiley = (int) MapUtils.getTileNumberY(z, next.getLatitude());
                if (ntilex == tilex && tiley == ntiley) {
                    sameTile = true;
                    w.add(next);
                    prevNode = next;
                    px31 = MapUtils.get31TileNumberX(prevNode.getLongitude());
                    py31 = MapUtils.get31TileNumberY(prevNode.getLatitude());
                    i++;
                } else {
                    int nx31 = MapUtils.get31TileNumberX(next.getLongitude());
                    int ny31 = MapUtils.get31TileNumberY(next.getLatitude());
                    // increase boundaries to drop into another tile
                    int leftX = (tilex << (31 - z)) - 1;
                    int rightX = (tilex + 1) << (31 - z);
                    if (rightX < 0) {
                        rightX = Integer.MAX_VALUE;
                    }
                    int topY = (tiley << (31 - z)) - 1;
                    int bottomY = (tiley + 1) << (31 - z);
                    if (bottomY < 0) {
                        bottomY = Integer.MAX_VALUE;
                    }

                    long inter = MapAlgorithms.calculateIntersection(px31, py31, nx31, ny31, leftX, rightX, bottomY, topY);
                    int cy31 = (int) inter;
                    int cx31 = (int) (inter >> 32l);
                    if (inter == -1) {
                        cx31 = nx31;
                        cy31 = ny31;
                        i++;
                        logMapDataWarn.warn("Can't find intersection for " + MapUtils.get31LongitudeX(px31) + ","
                                + MapUtils.get31LatitudeY(py31) + " - " + MapUtils.get31LongitudeX(nx31) + ","
                                + MapUtils.get31LatitudeY(ny31));
	                    logMapDataWarn.warn("Values " +px31 +  "," + py31 + " -- " +nx31+ ","+ny31+
			                    "   boundary " + leftX + "," + topY + "," + rightX  + "," + bottomY);
                    }

                    prevNode = new Node(MapUtils.get31LatitudeY(cy31), MapUtils.get31LongitudeX(cx31), -1000);
                    px31 = cx31;
                    py31 = cy31;
                    w.add(prevNode);
                    break wayConstruct;
                }
            }
            List<Node> res = new ArrayList<Node>();
            OsmMapUtils.simplifyDouglasPeucker(w, zoomToEncode - 1 + 8 + zoomWaySmoothness, 3, res, true);
            addRawData(res, null, types, addTypes, zoomPair, quadTree, z, tilex, tiley, null, refId);
        }
    }

    private void addRawData(List<Node> res, List<List<Node>> inner, int[] types, int[] addTypes, MapZoomPair zoomPair, SimplisticQuadTree quadTree, int z, int tilex,
                            int tiley, Map<MapRulType, String> names, long id) {
        SimplisticQuadTree quad = quadTree.getOrCreateSubTree(tilex, tiley, z);
        if (quad == null) {
            if (logMapDataWarn != null) {
                logMapDataWarn.error("Tile " + tilex + " / " + tiley + " at " + z + " can not be found");
            } else {
                System.err.println("Tile " + tilex + " / " + tiley + " at " + z + " can not be found");
            }
        }

        ByteArrayOutputStream bcoordinates = new ByteArrayOutputStream();
        for (Node n : res) {
            if (n != null) {
                int y = MapUtils.get31TileNumberY(n.getLatitude());
                int x = MapUtils.get31TileNumberX(n.getLongitude());
                try {
                    Algorithms.writeInt(bcoordinates, x);
                    Algorithms.writeInt(bcoordinates, y);
                } catch (IOException e1) {
                    throw new IllegalStateException(e1);
                }
            }
        }
        SimplisticBinaryData data = new SimplisticBinaryData();
        // not needed
        data.id = ID--; // don't use ref id
        data.coordinates = bcoordinates.toByteArray();
        data.types = types;
        data.addTypes = addTypes;
        data.names = names;
	    if(inner != null && inner.size() > 0){
		    bcoordinates = new ByteArrayOutputStream();
		    for (List<Node> ln : inner) {
			    try {
				    for (Node n : ln) {
					    if (n != null) {
						    int y = MapUtils.get31TileNumberY(n.getLatitude());
						    int x = MapUtils.get31TileNumberX(n.getLongitude());
						    Algorithms.writeInt(bcoordinates, x);
						    Algorithms.writeInt(bcoordinates, y);

					    }
				    }
				    Algorithms.writeInt(bcoordinates, 0);
				    Algorithms.writeInt(bcoordinates, 0);
			    } catch (IOException e1) {
				    throw new IllegalStateException(e1);
			    }
		    }
	    }
        quad.addQuadData(zoomPair, data);
    }

    private int getViewZoom(int minZoom, int maxZoom) {
        return Math.min((minZoom + maxZoom) / 2 - 1, minZoom + 1);
    }


    public static void main(String[] args) throws InterruptedException, SQLException, IOException, XMLStreamException, XmlPullParserException {
        if (args.length == 0) {
	        System.out.println("Please specify folder with basemap *.osm or *.osm.bz2 files");
		} else {
			boolean mini = false;
			MapRenderingTypesEncoder rt = new MapRenderingTypesEncoder("basemap");
			// BASEMAP generation
			File folder = new File(args[0]);
			if (args.length >= 2 && args[1].equals("mini")) {
				mini = true;
			}
			// MapZooms zooms = MapZooms.parseZooms("1-2;3;4-5;6-7;8-9;10-");
			int zoomSmoothness = mini ? 2 : 2;
			MapZooms zooms = mini ? MapZooms.parseZooms("1-2;3;4-5;6-8;9-") : MapZooms.parseZooms("1-2;3;4-5;6-8;9-");
			MOST_DETAILED_APPROXIMATION = mini ? 9 : 9;
			IndexCreatorSettings settings = new IndexCreatorSettings();
			settings.indexMap = true;
			settings.indexAddress = false;
//			settings.indexPOI = mini ? false : true;
			settings.indexPOI = true;
			settings.indexTransport = false;
			settings.indexRouting = false;
			settings.zoomWaySmoothness = zoomSmoothness;
			
			IndexCreator creator = new IndexCreator(folder, settings); //$NON-NLS-1$
			creator.setDialects(DBDialect.SQLITE_IN_MEMORY, DBDialect.SQLITE_IN_MEMORY);
			creator.setMapFileName(mini ? "World_basemap_mini_2.obf" : "World_basemap_2.obf");
			List<File> src = new ArrayList<File>();
			parseFiles(folder, src);

			// BASEMAP generation
			// creator.generateBasemapIndex(new ConsoleProgressImplementation(1), null, zooms, rt, log, "basemap",
			// new File(basemapParent, "10m_coastline_out_fix_caspean_arctic.osm")
			// ,new File(basemapParent, "roads_gen.osm")
			// ,new File(basemapParent, "10m_admin_level.osm")
			// ,new File(basemapParent, "10m_rivers.osm")
			// ,new File(basemapParent, "10m_lakes.osm")
			// ,new File(basemapParent, "10m_populated_places.osm")
			// );

			creator.generateBasemapIndex(mini, new ConsoleProgressImplementation(1), null, zooms, rt, log, "basemap",
					src.toArray(new File[src.size()]));
		}
    }

	private static void parseFiles(File folder, List<File> src) {
		for (File f : folder.listFiles()) {
			if(f.isDirectory() && f.getName().startsWith("proc_")) {
				parseFiles(f, src);
			}
			if (f.getName().endsWith(".osm") || 
					f.getName().endsWith(".osm.bz2") || 
					f.getName().endsWith(".osm.gz")) {
				src.add(f);
			}
		}
	}


}
