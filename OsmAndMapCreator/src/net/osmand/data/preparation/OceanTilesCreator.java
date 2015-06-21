package net.osmand.data.preparation;


import gnu.trove.map.hash.TLongObjectHashMap;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import javax.xml.stream.XMLStreamException;

import net.osmand.IProgress;
import net.osmand.osm.edit.Entity;
import net.osmand.osm.edit.Node;
import net.osmand.osm.edit.Way;
import net.osmand.osm.io.OsmBaseStorage;
import net.osmand.osm.io.OsmStorageWriter;
import net.osmand.util.MapUtils;

import org.apache.tools.bzip2.CBZip2InputStream;
import org.xml.sax.SAXException;

public class OceanTilesCreator {
    public static final byte TILE_ZOOMLEVEL = 12;
    private static final byte BITMASK = 0x3;
    private static final int BITS_COUNT = (1 << TILE_ZOOMLEVEL) * (1 << TILE_ZOOMLEVEL);
    private static final byte SEA = 0x2;
    private static final byte LAND = 0x1;


    /**
     * @param args
     * @throws IOException
     * @throws XMLStreamException
     * @throws SAXException
     */
    /**
     * @param args
     * @throws IOException
     * @throws XMLStreamException
     * @throws SAXException
     */
    public static void main(String[] args) throws IOException, XMLStreamException, SAXException {

        BasemapProcessor bmp = new BasemapProcessor();
        bmp.constructBitSetInfo();
        if(args[0].equals("generate")) {
        	createTilesFile(args[1], args[2]);
		} else {
			double[] lat = new double[] { 41.4537, 33.4 };
			double[] lon = new double[] { 41.45, 15.5 };
			int zoom = 11;
			for (int i = 0; i < lat.length && i < lon.length; i++) {
				int x = (int) MapUtils.getTileNumberX(zoom, lon[i]);
				int y = (int) MapUtils.getTileNumberY(zoom, lat[i]);
				System.out.println(bmp.getSeaTile(x, y, zoom));
			}
		}
        
//        BasemapProcessor.SimplisticQuadTree quadTree = bmp.constructTilesQuadTree(11);
//        BasemapProcessor.SimplisticQuadTree ts = quadTree.getOrCreateSubTree(x, y, 11);
//        System.out.println(ts.seaCharacteristic);


//        createJOSMFile(bmp, args[1]);
    }
    
    public static void checkOceanTile(String[] args) {
    	double[] lat = new double[] { Double.parseDouble(args[0])};
    	double[] lon = new double[] { Double.parseDouble(args[1])};
    	int zoom = 11;
    	if(args.length > 2) {
    		zoom = Integer.parseInt(args[2]);
    	}
    	BasemapProcessor bmp = new BasemapProcessor();
        bmp.constructBitSetInfo();
		for (int i = 0; i < lat.length && i < lon.length; i++) {
			int x = (int) MapUtils.getTileNumberX(zoom, lon[i]);
			int y = (int) MapUtils.getTileNumberY(zoom, lat[i]);
			System.out.println("Tile is sea (1) or land (0): " + bmp.getSeaTile(x, y, zoom));
		}
    }

    static class OceanTileInfo {
        int linesIntersectMedian = 0;
        static int UNDEFINED = 0;
        static int MIXED = 1;
        int type = UNDEFINED;
        boolean rightMedianSea ;
    }

    public static boolean ccw(double ax, double ay, double bx, double by, double cx, double cy) {
        return (cy - ay) * (bx - ax) > (by -ay) *(cx - ax);
    }

    // Return true if line segments AB and CD intersect
    public static boolean intersect2Segments(double ax, double ay, double bx, double by, double cx, double cy,
                                             double dx, double dy) {
        return ccw(ax, ay, cx, cy, dx, dy) != ccw(bx, by, cx, cy, dx, dy) &&
                ccw(ax, ay, bx, by, cx, cy) != ccw(ax, ay, bx, by, dx, dy);
    }


    public static void createTilesFile(String coastlinesInput, String result) throws IOException, SAXException {
    	if(result == null ) {
    		result = "oceantiles_12.dat";
    	}
        File readFile = new File(coastlinesInput);
        InputStream stream = new BufferedInputStream(new FileInputStream(readFile), 8192 * 4);
        InputStream streamFile = stream;
        long st = System.currentTimeMillis();
        if (readFile.getName().endsWith(".bz2")) { //$NON-NLS-1$
            if (stream.read() != 'B' || stream.read() != 'Z') {
//				throw new RuntimeException("The source stream must start with the characters BZ if it is to be read as a BZip2 stream."); //$NON-NLS-1$
            } else {
                stream = new CBZip2InputStream(stream);
            }
        }
        OsmBaseStorage bs = new OsmBaseStorage();
        bs.parseOSM(stream, IProgress.EMPTY_PROGRESS);


        int c = 0;
        int ns = 0;
        TLongObjectHashMap<OceanTileInfo> map = new TLongObjectHashMap<OceanTileInfo>();
        for(Entity e : bs.getRegisteredEntities().values()) {
            if(e instanceof Way) {
                Way w = (Way) e;
                List<Node> nodes = w.getNodes();
                for(int i = 1; i < nodes.size(); i++) {
                    double tx = MapUtils.getTileNumberX(TILE_ZOOMLEVEL, nodes.get(i).getLongitude());
                    double ty = MapUtils.getTileNumberY(TILE_ZOOMLEVEL, nodes.get(i).getLatitude());
                    double px = MapUtils.getTileNumberX(TILE_ZOOMLEVEL, nodes.get(i-1).getLongitude());
                    double py = MapUtils.getTileNumberY(TILE_ZOOMLEVEL, nodes.get(i-1).getLatitude());
                    for(int x = (int) Math.min(tx, px); x <= Math.max(tx, px); x++){
                        for(int y = (int) Math.min(ty, py); y <= Math.max(ty, py); y++){
                            // check if intersects (x-1,y+0.5) & (x,y+0.5)
                            long key = ((long) x << TILE_ZOOMLEVEL) + (long)y;
                            if(intersect2Segments(tx, ty, px, py, x, y+0.5d, x+1, y+0.5d)) {
                                getOrCreate(map, key).linesIntersectMedian ++;
                                getOrCreate(map, key).type = OceanTileInfo.MIXED;
                            } else if(intersect2Segments(tx, ty, px, py, x, y, x+1, y)) {
                                getOrCreate(map, key).type = OceanTileInfo.MIXED;
                            } else if(intersect2Segments(tx, ty, px, py, x, y+1, x+1, y+1)) {
                                getOrCreate(map, key).type = OceanTileInfo.MIXED;
                            } else if(intersect2Segments(tx, ty, px, py, x, y, x, y+1)) {
                                getOrCreate(map, key).type = OceanTileInfo.MIXED;
                            } else if(intersect2Segments(tx, ty, px, py, x+1, y, x+1, y+1)) {
                                getOrCreate(map, key).type = OceanTileInfo.MIXED;
                            }
                        }
                    }
                }
                c++;
                ns += w.getNodeIds().size();
            }
        }
        writeResult(map, result);

        System.out.println(c + " " + ns + " coastlines " + map.size());
    }

    private static void writeResult(TLongObjectHashMap<OceanTileInfo> map, String result) throws IOException {
        int currentByte = 0;
        FileOutputStream rf = new FileOutputStream(result);
//        int[] cs = new int[4];
//        int[] vs = new int[4];

        int maxT = 1 << TILE_ZOOMLEVEL;
        double antarcticaStart = MapUtils.getTileNumberY(TILE_ZOOMLEVEL, -84.35);
        for(int y = 0; y < maxT; y++ ) {
            boolean previousSea = true;
            if( y  >= antarcticaStart) {
                // antarctica
                previousSea = false;
            }
            for(int x = 0; x < maxT; x++ ) {
                long key = ((long) x << TILE_ZOOMLEVEL) + (long)y;
                OceanTileInfo oc = map.get(key);
                int vl = 0;
                if(oc == null || oc.type == OceanTileInfo.UNDEFINED) {
                    vl = previousSea ? SEA : LAND;
                } else {
                    vl = 3;
                    boolean odd = oc.linesIntersectMedian % 2 == 0;
                    if(!odd) {
                        previousSea = !previousSea;
                    }
                }
                currentByte = (currentByte << 2) | (vl & BITMASK);
                if(x % 4 == 3) {
                    rf.write(currentByte);
                    currentByte = 0;
                }
            }
        }
        rf.close();


    }

    private static OceanTileInfo getOrCreate(TLongObjectHashMap<OceanTileInfo> map, long key) {
        if(!map.containsKey(key)) {
            map.put(key, new OceanTileInfo());
        }
        return map.get(key );
    }

    private static void createJOSMFile(String fileLocation , BasemapProcessor bmp ) throws XMLStreamException, IOException {
        int z = 8;
        BasemapProcessor.SimplisticQuadTree quadTree = bmp.constructTilesQuadTree(z);
        int pz = 1 << z;
        OsmBaseStorage st = new OsmBaseStorage();
        Set<Entity.EntityId> s = new LinkedHashSet();
        for(int i = 0; i < pz; i++) {
            for(int j = 0; j < pz; j++) {
//                if((quadTree.getOrCreateSubTree(i, j, z).seaCharacteristic < 0.9 && !bmp.isWaterTile(i, j, z))||
//                        bmp.isLandTile(i, j, z) ) {
                if((quadTree.getOrCreateSubTree(i, j, z).seaCharacteristic > 0.8 || bmp.isWaterTile(i, j, z))) {
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
        new OsmStorageWriter().saveStorage(new FileOutputStream(fileLocation),
                st, s, true);
    }


    private static void runFixOceanTiles() throws IOException {
        final int land[]  = new int[] {};
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
                for(int i = 0; i < land.length; i+= 2) {
                    if (land[i] == tx && land[i] == ty) {
                        if (origValue != LAND) {
                            c++;
                            System.out.println("L " + c + ". " + ty + " " + y + " " + x);
                            return LAND;
                        }
                    }
                }
                return 0;
            }
        }, false);
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
