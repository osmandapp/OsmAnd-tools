package net.osmand.data.preparation;

import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.hash.TLongObjectHashMap;

import java.io.*;
import java.sql.SQLException;
import java.util.*;
import java.util.Map.Entry;

import net.osmand.PlatformUtil;
import net.osmand.binary.OsmandOdb.MapData;
import net.osmand.binary.OsmandOdb.MapDataBlock;
import net.osmand.data.preparation.MapZooms.MapZoomPair;
import net.osmand.impl.ConsoleProgressImplementation;
import net.osmand.osm.MapRenderingTypesEncoder;
import net.osmand.osm.MapRenderingTypesEncoder.MapRulType;
import net.osmand.osm.WayChain;
import net.osmand.osm.edit.Entity;
import net.osmand.osm.edit.Node;
import net.osmand.osm.edit.OsmMapUtils;
import net.osmand.osm.edit.Way;
import net.osmand.osm.io.OsmBaseStorage;
import net.osmand.osm.io.OsmStorageWriter;
import net.osmand.util.Algorithms;
import net.osmand.util.MapAlgorithms;
import net.osmand.util.MapUtils;

import org.apache.commons.logging.Log;
import org.apache.tools.bzip2.CBZip2InputStream;
import org.xml.sax.SAXException;

import javax.xml.stream.XMLStreamException;

public class BasemapProcessor {
    TLongObjectHashMap<WayChain> coastlinesEndPoint = new TLongObjectHashMap<WayChain>();
    TLongObjectHashMap<WayChain> coastlinesStartPoint = new TLongObjectHashMap<WayChain>();

    private static final byte SEA = 0x2;
    private static final byte LAND = 0x1;
    private static final Log log = PlatformUtil.getLog(BasemapProcessor.class);

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
    Map<MapRulType, String> namesUse = new LinkedHashMap<MapRulType, String>();

    private final int zoomWaySmothness;
    private final MapRenderingTypesEncoder renderingTypes;
    private final MapZooms mapZooms;
    private final Log logMapDataWarn;
    private SimplisticQuadTree[] quadTrees;

    private static class SimplisticQuadTree {
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
                    }
                }
            }
        }
    }


    private static class SimplisticBinaryData {
        // consequent 31 coordinates
        public byte[] coordinates;
        public int[] types;
        public int[] addTypes;
        public long id = -500;
        public Map<MapRulType, String> names;
    }

    private BasemapProcessor() {
        logMapDataWarn = null;
        zoomWaySmothness = 0;
        renderingTypes = null;
        mapZooms = null;
    }

    public BasemapProcessor(Log logMapDataWarn, MapZooms mapZooms, MapRenderingTypesEncoder renderingTypes, int zoomWaySmothness) {
        this.logMapDataWarn = logMapDataWarn;
        this.mapZooms = mapZooms;
        this.renderingTypes = renderingTypes;
        this.zoomWaySmothness = zoomWaySmothness;
        constructBitSetInfo();
        quadTrees = new SimplisticQuadTree[mapZooms.getLevels().size()];
        for (int i = 0; i < mapZooms.getLevels().size(); i++) {
            MapZoomPair p = mapZooms.getLevels().get(i);
            quadTrees[i] = constructTilesQuadTree(Math.min(p.getMaxZoom(), 11));
        }
    }

    private void constructBitSetInfo() {
        try {

            InputStream stream = BasemapProcessor.class.getResourceAsStream("oceantiles_12.dat.bz2");
            if (stream.read() != 'B' || stream.read() != 'Z') {
                throw new RuntimeException("The source stream must start with the characters BZ if it is to be read as a BZip2 stream."); //$NON-NLS-1$
            }
            InputStream dis = new CBZip2InputStream(stream);
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
        ArrayList<SimplisticQuadTree> toVisit = new ArrayList<SimplisticQuadTree>();
        for (int x = 0; x < tiles; x++) {
            for (int y = 0; y < tiles; y++) {
                toVisit.add(rootTree.getOrCreateSubTree(x, y, baseZoom));
            }
        }
        initializeQuadTree(rootTree, baseZoom, maxZoom, toVisit);
        return rootTree;

    }

    protected ArrayList<SimplisticQuadTree> initializeQuadTree(SimplisticQuadTree rootTree, int baseZoom, int maxZoom,
                                                               ArrayList<SimplisticQuadTree> toVisit) {
        for (int zoom = baseZoom; zoom <= maxZoom && !toVisit.isEmpty(); zoom++) {
            ArrayList<SimplisticQuadTree> newToVisit = new ArrayList<SimplisticQuadTree>();
            for (SimplisticQuadTree subtree : toVisit) {
                int x = subtree.x;
                int y = subtree.y;
                SimplisticQuadTree st = rootTree.getOrCreateSubTree(x, y, zoom);
                st.seaCharacteristic = getSeaTile(x, y, zoom);
                if (zoom < maxZoom && !isWaterTile(x, y, zoom) && !isLandTile(x, y, zoom)) {
                    SimplisticQuadTree[] vis = st.getAllChildren();
                    Collections.addAll(newToVisit, vis);
                }
            }
            toVisit = newToVisit;
        }
        return toVisit;
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
            dataBlock.setBaseId(0);
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
                MapData mapData = writer.writeMapData(w.id,
                        quad.x << (31 - quad.zoom), quad.y << (31 - quad.zoom), false,
                        w.coordinates, null, wts, wats, w.names, stringTable, dataBlock, level.getMaxZoom() > 15);
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
        BinaryFileReference ref = writer.startMapTreeElement(xL, xR, yT, yB, defined, quadTree.seaCharacteristic > 0.5 ? -1 : 1);
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

    public void processEntity(Entity e) {
        if (e instanceof Way || e instanceof Node) {
            long id = - Math.abs(e.getId());
            for (int level = 0; level < mapZooms.getLevels().size(); level++) {
                boolean mostDetailed = level == 0;
                MapZoomPair zoomPair = mapZooms.getLevel(level);
                int zoomToEncode = mostDetailed ? zoomPair.getMinZoom() + 1 : zoomPair.getMaxZoom();
                if (mostDetailed && zoomPair.getMaxZoom() < 10) {
                    throw new IllegalStateException("Zoom pair is not detailed " + zoomPair);
                }
                renderingTypes.encodeEntityWithType(e, zoomToEncode, typeUse, addtypeUse, namesUse, tempNameUse);
                if (typeUse.isEmpty()) {
                    continue;
                }
                if (e instanceof Way) {
                    if (((Way) e).getNodes().size() < 2) {
                        continue;
                    }
                    if (((Way) e).getFirstNodeId() == ((Way) e).getLastNodeId() && !mostDetailed) {
                        if (OsmMapUtils.polygonAreaPixels(((Way) e).getNodes(), zoomToEncode) < 24) {
                            continue;
                        }
                    }
                    if ("coastline".equals(e.getTag("natural")) || !Algorithms.isEmpty(e.getTag("admin_level"))) {
                        splitContinuousWay(((Way) e).getNodes(), typeUse.toArray(),
                                !addtypeUse.isEmpty() ? addtypeUse.toArray() : null,
                                zoomPair, zoomToEncode, quadTrees[level], id);
                    } else {
                        List<Node> ns = ((Way) e).getNodes();
                        int z = getViewZoom(zoomPair.getMinZoom(), zoomToEncode);
                        int tilex = 0;
                        int tiley = 0;
                        boolean sameTile = false;
                        while (!sameTile) {
                            tilex = (int) MapUtils.getTileNumberX(z, ns.get(0).getLongitude());
                            tiley = (int) MapUtils.getTileNumberY(z, ns.get(0).getLatitude());
                            sameTile = true;
                            for (int i = 1; i < ns.size(); i++) {
                                int tx = (int) MapUtils.getTileNumberX(z, ns.get(i).getLongitude());
                                int ty = (int) MapUtils.getTileNumberY(z, ns.get(i).getLatitude());
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
                        OsmMapUtils.simplifyDouglasPeucker(ns, zoomToEncode - 1 + 8 + zoomWaySmothness, 3, res);
                        addSimplisticData(res, typeUse.toArray(), !addtypeUse.isEmpty() ? addtypeUse.toArray() : null, zoomPair,
                                quadTrees[level], z, tilex, tiley, namesUse.isEmpty() ? null : new LinkedHashMap<MapRulType, String>(namesUse), id);
                    }
                } else {
                    int z = getViewZoom(zoomPair.getMinZoom(), zoomToEncode);
                    int tilex = (int) MapUtils.getTileNumberX(z, ((Node) e).getLongitude());
                    int tiley = (int) MapUtils.getTileNumberY(z, ((Node) e).getLatitude());
                    addSimplisticData(Collections.singletonList((Node) e), typeUse.toArray(), !addtypeUse.isEmpty() ? addtypeUse.toArray() : null, zoomPair,
                            quadTrees[level], z, tilex, tiley, namesUse.isEmpty() ? null : new LinkedHashMap<MapRulType, String>(namesUse),id);
                }

            }
        }
    }

    public void splitContinuousWay(List<Node> ns, int[] types, int[] addTypes, MapZoomPair zoomPair, int zoomToEncode,
                                   SimplisticQuadTree quadTree, long id) {
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
                    }

                    prevNode = new Node(MapUtils.get31LatitudeY(cy31), MapUtils.get31LongitudeX(cx31), -1000);
                    px31 = cx31;
                    py31 = cy31;
                    w.add(prevNode);
                    break wayConstruct;
                }
            }
            List<Node> res = new ArrayList<Node>();
            OsmMapUtils.simplifyDouglasPeucker(w, zoomToEncode - 1 + 8 + zoomWaySmothness, 3, res);
            addSimplisticData(res, types, addTypes, zoomPair, quadTree, z, tilex, tiley, null, id);
        }
    }

    private void addSimplisticData(List<Node> res, int[] types, int[] addTypes, MapZoomPair zoomPair, SimplisticQuadTree quadTree, int z, int tilex,
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
        data.id = id;
        data.coordinates = bcoordinates.toByteArray();
        data.types = types;
        data.addTypes = addTypes;
        data.names = names;
        quad.addQuadData(zoomPair, data);
    }

    private int getViewZoom(int minZoom, int maxZoom) {
        return Math.min((minZoom + maxZoom) / 2 - 1, minZoom + 1);
    }


    public static void main(String[] p) throws InterruptedException, SAXException, SQLException, IOException, XMLStreamException {
        if (p.length == 0) {
            System.out.println("Please specify folder with basemap *.osm or *.osm.bz2 files");
        } else {
            long time = System.currentTimeMillis();
            MapRenderingTypesEncoder rt = MapRenderingTypesEncoder.getDefault();
            // BASEMAP generation
            File folder = new File(p[0]);
            MapZooms zooms = MapZooms.parseZooms("1-2;3;4-5;6-7;8-9;10-");
            IndexCreator creator = new IndexCreator(folder); //$NON-NLS-1$
            creator.setIndexMap(true);
            creator.setZoomWaySmothness(2);
            creator.setMapFileName("World_basemap_2.obf");
            ArrayList<File> src = new ArrayList<File>();
            for (File f : folder.listFiles()) {
                if (f.getName().endsWith(".osm") || f.getName().endsWith(".osm.bz2")) {
                    src.add(f);
                }
            }
            creator.generateBasemapIndex(new ConsoleProgressImplementation(1), null, zooms, rt, log, "basemap",
                    src.toArray(new File[src.size()])
            );
        }
        /*BasemapProcessor bmp = new BasemapProcessor();
        bmp.constructBitSetInfo();
        SimplisticQuadTree quadTree = bmp.constructTilesQuadTree(7);
        SimplisticQuadTree ts = quadTree.getOrCreateSubTree(43, 113, 7);
        System.out.println(ts.seaCharacteristic);
        ts = quadTree.getOrCreateSubTree(44, 113, 7);
        System.out.println(ts.seaCharacteristic);
        ts = quadTree.getOrCreateSubTree(45, 112, 7);

        createJOSMFile();

        runFixOceanTiles();*/
    }

    private static void runFixOceanTiles() throws IOException {
        int land[]  = new int[] {34,29, 20,35, 20,37,21,37, 10, 15, 10, 16, 10, 17, 10, 18, 11, 16, 11, 17,
        11, 18, 11, 19, 11, 20, 12, 17, 12, 18, 12, 19, 12, 20, 13, 17, 13, 19, 13, 20, 13, 21, 14, 16,
        14, 17, 14, 19, 14, 20, 14, 21, 15, 20, 15, 21, 15, 22, 16, 21, 16, 22, 16, 23, 17, 22, 17, 23, 9, 16, 9, 19};
        fixOceanTiles(new FixTileData() {
            int c = 0;
            @Override
            public int compareTileData(int x, int y, int z, int origValue) {
                int sh = z - 7;
                int ty = y >> sh;
                int tx = x >> sh;
                if((tx == 44 && ty == 113)
                        || (tx == 45 && ty == 112)
                        ) {
                    if(origValue != SEA) {
                        c++;
                        System.out.println("S "+c + ". " + ty + " " + y + " " + x);
                        return SEA;
                    }
                }
                if((tx == 21 && ty == 32)
                        || (tx == 32 && ty == 44)
                        ) {
                    if(origValue != LAND) {
                        c++;
                        System.out.println("L "+c + ". " + ty + " " + y + " " + x);
                        return LAND;
                    }
                }
                return 0;
            }
        }, false);
    }

    private static void createJOSMFile() throws XMLStreamException, IOException {
        int z = 6;
        BasemapProcessor bmp = new BasemapProcessor();
        bmp.constructBitSetInfo();
        SimplisticQuadTree quadTree = bmp.constructTilesQuadTree(z);
        int pz = 1 << z;
        OsmBaseStorage st = new OsmBaseStorage();
        Set<Entity.EntityId> s = new LinkedHashSet();
        for(int i = 0; i < pz; i++) {
            for(int j = 0; j < pz; j++) {
                if(quadTree.getOrCreateSubTree(i, j, z).seaCharacteristic > 0 ||
                        bmp.isWaterTile(i, j, z)) {
                    Way w = new Way(-(i * pz + j + 1));
                    w.addNode(i * pz + j + 1);
                    w.addNode((i + 1) * pz + j + 1);
                    w.addNode((i + 1) * pz + j + 1 + 1);
                    w.addNode(i * pz + j + 1 + 1);
                    w.addNode(i * pz + j + 1);
                    w.putTag("place", "island");
                    w.putTag("name", i+" " + j + " " + z + " " + quadTree.getOrCreateSubTree(i, j, z).seaCharacteristic);
                    s.addAll(w.getEntityIds());

                    s.add(Entity.EntityId.valueOf(w));
                    st.registerEntity(w, null);
                }
                Node nod = new Node(
                        MapUtils.getLatitudeFromTile(z, j), MapUtils.getLongitudeFromTile(z, i),
                        i * pz + j + 1);
                st.registerEntity(nod, null);

            }
        }
        new OsmStorageWriter().saveStorage(new FileOutputStream("/home/victor/projects/osmand/data/basemap/ready/grid.osm"),
                st, s, true);
    }

    private static int getTileX(int i) {
        return i % 4096;
    }

    private static int getTileY(int i) {
        return i / 4096;
    }

    private static interface FixTileData {

        public int compareTileData(int x, int y, int z, int origValue);
    }

    private static void fixOceanTiles(FixTileData fx, boolean dryRun) throws IOException {
        int currentByte;
        RandomAccessFile rf = new RandomAccessFile("../tools/OsmAndMapCreator/oceantiles_12.dat", "rw");
        int[] cs = new int[4];
        int[] vs = new int[4];
        int changedT = 0;
        for (int i = 0; i < BITS_COUNT / 4; i++) {
            currentByte = rf.readByte();
            int x = getTileX(i);
            int y = getTileY(i);
            vs[0] = ((currentByte >> 6) & BITMASK);
            vs[1] = ((currentByte >> 4) & BITMASK);
            vs[2] = ((currentByte >> 2) & BITMASK);
            vs[3] = ((currentByte >> 0) & BITMASK);
            cs[0] = i * 4;
            cs[1] = i * 4 + 1;
            cs[2] = i * 4 + 2;
            cs[3] = i * 4 + 3;
            boolean changed = false;
            for (int k = 0; k < cs.length; k++) {
                int tx = getTileX(cs[k]);
                int ty = getTileY(cs[k]);
                int c = fx.compareTileData(tx, ty, TILE_ZOOMLEVEL, vs[k]);
                if (c != 0) {
                    vs[k] = c;
                    changed = true;
                }

            }
            if (changed && !dryRun) {
                currentByte = 0;
                currentByte = (currentByte << 2) | vs[0];
                currentByte = (currentByte << 2) | vs[1];
                currentByte = (currentByte << 2) | vs[2];
                currentByte = (currentByte << 2) | vs[3];
                rf.seek(rf.getFilePointer() - 1);
                rf.writeByte(currentByte);
                changedT++;
            }
        }
        System.out.println("Changed " + changedT + " bytes");
    }
}
