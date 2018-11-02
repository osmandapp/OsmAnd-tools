package net.osmand.obf.preparation;


import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.set.hash.TLongHashSet;

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

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.xml.sax.SAXException;
import org.xmlpull.v1.XmlPullParserException;

public class OceanTilesCreator {
    public static final byte TILE_ZOOMLEVEL = BasemapProcessor.TILE_ZOOMLEVEL;
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
     * @throws XmlPullParserException 
     */
    public static void main(String[] args) throws Exception{
//    	System.out.println("Tiles generated ");
//    	createTilesFile("/Users/victorshcherb/osmand/maps/coastline.osm.bz2",
//    			"/Users/victorshcherb/osmand/maps/oceantiles_"+TILE_ZOOMLEVEL+".dat");
    	createJOSMFile(new String[] {
    			"/Users/victorshcherb/osmand/maps/oceantiles.osm",
    			null
//    			"/Users/victorshcherb/osmand/maps/oceantiles_"+TILE_ZOOMLEVEL+".dat"
    	});
//        if(args.length > 0 && args[0].equals("generate")) {
//        	createTilesFile(args[1], args[2]);
//        }
    }

    public static void checkOceanTile(String[] args) {
    	double[] lat = new double[] { Double.parseDouble(args[0])};
    	double[] lon = new double[] { Double.parseDouble(args[1])};
    	int zoom = 11;
    	if(args.length > 2) {
    		zoom = Integer.parseInt(args[2]);
    	}
    	BasemapProcessor bmp = new BasemapProcessor();
        bmp.constructBitSetInfo(null);
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


    public static void createTilesFile(String coastlinesInput, String result) throws IOException, XmlPullParserException {
    	if(result == null ) {
    		result = "oceantiles_12.dat";
    	}
        File readFile = new File(coastlinesInput);
        InputStream stream = new BufferedInputStream(new FileInputStream(readFile), 8192 * 4);
        InputStream streamFile = stream;
        long st = System.currentTimeMillis();
		if (readFile.getName().endsWith(".bz2")) { //$NON-NLS-1$
			stream = new BZip2CompressorInputStream(stream);
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
							if (intersect2Segments(tx, ty, px, py, x, y + 0.5d, x + 1, y + 0.5d)) {
								getOrCreate(map, key).linesIntersectMedian++;
								getOrCreate(map, key).type = OceanTileInfo.MIXED;
							} else if (intersect2Segments(tx, ty, px, py, x, y, x + 1, y)) {
								getOrCreate(map, key).type = OceanTileInfo.MIXED;
							} else if (intersect2Segments(tx, ty, px, py, x, y + 1, x + 1, y + 1)) {
								getOrCreate(map, key).type = OceanTileInfo.MIXED;
							} else if (intersect2Segments(tx, ty, px, py, x, y, x, y + 1)) {
								getOrCreate(map, key).type = OceanTileInfo.MIXED;
							} else if (intersect2Segments(tx, ty, px, py, x + 1, y, x + 1, y + 1)) {
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

    public static long getNodeId(int x, int y, int z) {
    	return (((long) x) << 25) | ((long)y << 5) | z;
    }

    public static Node getNode(long id) {
    	int x = (int) (id >> 25);
    	int y = (int) ((id - getNodeId(x, 0, 0)) >> 5);
    	int z = (int) (id - getNodeId(x, y, 0));
    	Node nod = new Node(
                MapUtils.getLatitudeFromTile(z, y), MapUtils.getLongitudeFromTile(z, x),
                id);
    	return nod;
    }

    public static void createJOSMFile(String[] args) throws XMLStreamException, IOException {
        String fileLocation = args.length == 0 ? "oceanTiles.osm" : args[0];
        int z = TILE_ZOOMLEVEL;
        BasemapProcessor bmp = new BasemapProcessor();
        bmp.constructBitSetInfo(args.length > 1 ? args[1] : null);

        OsmBaseStorage st = new OsmBaseStorage();
        Set<Entity.EntityId> s = new LinkedHashSet();
        TLongHashSet nodeIds = new TLongHashSet();

        int minzoom = 4;
        BasemapProcessor.SimplisticQuadTree quadTree = bmp.constructTilesQuadTree(z);
        for (int zm = minzoom; zm <= z; zm++) {
			int pz = 1 << zm;
			for (int x = 0; x < pz; x++) {
				for (int y = 0; y < pz; y++) {
					// if((quadTree.getOrCreateSubTree(i, j, z).seaCharacteristic < 0.9 && !bmp.isWaterTile(i, j, z))||
					// bmp.isLandTile(i, j, z) ) {
					boolean parentWater = bmp.isWaterTile(x >> 1, y >> 1, zm - 1);
					boolean parentLand = bmp.isLandTile(x >> 1, y >> 1, zm - 1);
					if(zm > minzoom && (parentLand || parentWater)) {
						continue;
					}
					boolean landTile = bmp.isLandTile(x, y, zm);
					boolean waterTile = bmp.isWaterTile(x, y, zm);
					if (waterTile || landTile) {
						Way w = new Way(-getNodeId(x, y, zm));
						addNode(w, nodeIds, x, y, zm);
						addNode(w, nodeIds, x, y + 1, zm);
						addNode(w, nodeIds, x + 1, y + 1, zm);
						addNode(w, nodeIds, x + 1, y, zm);
						addNode(w, nodeIds, x, y, zm);

						if(waterTile) {
							w.putTag("natural", "water");
						} else if(landTile){
							w.putTag("landuse", "grass");
						}
						w.putTag("name", x + " " + y + " " + zm + " " +
								(waterTile  ? 1 : 0));
						s.addAll(w.getEntityIds());

						s.add(Entity.EntityId.valueOf(w));
						st.registerEntity(w, null);
					}
				}
			}
		}
        for(long l : nodeIds.toArray()) {
            st.registerEntity(getNode(l), null);
        }

        new OsmStorageWriter().saveStorage(new FileOutputStream(fileLocation),
                st, s, true);
    }


    private static void addNode(Way w, TLongHashSet nodeIds, int x, int y, int zm) {
    	long nodeId = getNodeId(x, y, zm);
    	w.addNode(nodeId);
    	nodeIds.add(nodeId);
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
