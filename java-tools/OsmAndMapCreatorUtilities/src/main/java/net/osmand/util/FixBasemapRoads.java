package net.osmand.util;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.zip.GZIPInputStream;

import javax.xml.stream.XMLStreamException;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.logging.Log;
import org.xmlpull.v1.XmlPullParserException;

import gnu.trove.iterator.TLongIterator;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.set.hash.TLongHashSet;
import net.osmand.PlatformUtil;
import net.osmand.binary.BinaryMapDataObject;
import net.osmand.data.LatLon;
import net.osmand.data.QuadRect;
import net.osmand.impl.ConsoleProgressImplementation;
import net.osmand.map.OsmandRegions;
import net.osmand.obf.preparation.TagsTransformer;
import net.osmand.osm.MapRenderingTypesEncoder;
import net.osmand.osm.MapRenderingTypesEncoder.EntityConvertApplyType;
import net.osmand.osm.edit.Entity;
import net.osmand.osm.edit.Entity.EntityId;
import net.osmand.osm.edit.Entity.EntityType;
import net.osmand.osm.edit.EntityInfo;
import net.osmand.osm.edit.Node;
import net.osmand.osm.edit.OSMSettings.OSMTagKey;
import net.osmand.osm.edit.OsmMapUtils;
import net.osmand.osm.edit.Relation;
import net.osmand.osm.edit.Way;
import net.osmand.osm.io.IOsmStorageFilter;
import net.osmand.osm.io.OsmBaseStorage;
import net.osmand.osm.io.OsmStorageWriter;

public class FixBasemapRoads {
	private final static Log LOG = PlatformUtil.getLog(FixBasemapRoads.class);
    
	private static int PREFERRED_DISTANCE = 50000; // -> 1500? primary
	private static int MINIMAL_DISTANCE = 2000; // -> 1500? primary
	
	// In case road is shorter than min distance after stage 1, it is considered as link / roundabout
	private static final double MINIMUM_DISTANCE_LINK = 150;
	
	// distance to measure direction of the road (150 m)
	private static final double DIST_DIRECTION = 150;
	
	// straight angle
	private static final double ANGLE_TO_ALLOW_DIRECTION = 2 * Math.PI / 3;
	
	// metrics penalty = connection distance + shortLengthPenalty + (90 - angle diff) * anglePenalty
	private static final double METRIC_SHORT_LENGTH_THRESHOLD = 100;
	private static final double METRIC_SHORT_LENGTH_PENALTY = 100;
	// useful for railways
	private static final double METRIC_LONG_LENGTH_THRESHOLD = 5000;
	private static final double METRIC_LONG_LENGTH_BONUS = -100;
	private static final double METRIC_ANGLE_THRESHOLD = Math.PI / 6;
	private static final double METRIC_ANGLE_PENALTY = 50; 
	
	// making it less < 31 joins unnecessary roads make merge process more complicated 
	private static final int APPROXIMATE_POINT_ZOOM = 31;

	private static final int ADMIN_LEVEL_ZOOM_SPLIT = 4;
	private static final int RAILWAY_ZOOM_SPLIT = 4;
	private static final int REF_EMPTY_ZOOM_SPLIT = 6;
	
		
    private static boolean FILTER_BBOX = false;  
    private static double LEFT_LON = 0.0;
    private static double RIGHT_LON = 12.25;
    private static double TOP_LAT = 57.25;
    private static double BOTTOM_LAT = 45.0;

	public static void main(String[] args) throws Exception {
		if(args != null && args.length == 1 && args[1].equals("test")) {
			args = new String[] {
					"/home/denisxs/osmand-maps/proc/" + "line_motorway_trunk_primary_c.osm",
					"/home/denisxs/osmand-maps/raw/line_mtp_cut.osm",
					//"/home/denisxs/osmand-maps/raw/route_road.osm.gz"
			};
		}
		String fileToWrite =  args[0];
		List<File> relationFiles = new ArrayList<>();
		List<File> filesToRead = new ArrayList<>();
		

		for(int i = 0; i < args.length; i++) {
			if(args[i].equals("--route-relations")) {
				i++;
				relationFiles.add(new File(args[i]));
			} else if(args[i].equals("--min-dist")) {
				i++;
				MINIMAL_DISTANCE = Integer.parseInt(args[i]);
			} else if(args[i].equals("--pref-dist")) {
				i++;
				PREFERRED_DISTANCE = Integer.parseInt(args[i]);
			} else {
				filesToRead.add(new File(args[i]));
			}
		}
		LOG.info(String.format("Preffered road distance: %d, minimal road distance: %d", PREFERRED_DISTANCE, MINIMAL_DISTANCE));

		File write = new File(fileToWrite);
		write.createNewFile();
        new FixBasemapRoads().process(write, filesToRead, relationFiles);
	}

	private void process(File write, List<File> filesToRead, List<File> relationFiles) throws  IOException, XMLStreamException, XmlPullParserException, SQLException {
		MapRenderingTypesEncoder renderingTypes = new MapRenderingTypesEncoder("basemap");
		OsmandRegions or = prepareRegions();
		TagsTransformer transformer = new TagsTransformer();
		for(File relFile : relationFiles) {
			LOG.info("Parse relations file " + relFile.getName());
			OsmBaseStorage storage = parseOsmFile(relFile);
			int total = 0;
			for(EntityId e : storage.getRegisteredEntities().keySet()){
				if(e.getType() == EntityType.RELATION){
					total++;
					if(total % 1000 == 0) {
						LOG.info("Processed " + total + " relations");
					}
					Relation es = (Relation) storage.getRegisteredEntities().get(e);
					transformer.handleRelationPropogatedTags(es, renderingTypes, null, EntityConvertApplyType.MAP);
				}
			}
		}
        
		Map<EntityId, Entity> mainEntities = new HashMap<>();
		Map<EntityId, EntityInfo> mainEntityInfo = new HashMap<>();
		int total = 0;
		long nid = -10000000000l;
		for (File read : filesToRead) {
			LOG.info("Parse file " + read.getName());
			OsmBaseStorage storage = parseOsmFile(read);
			Map<EntityId, Entity> entities = storage.getRegisteredEntities();
			Map<EntityId, EntityInfo> infos = storage.getRegisteredEntityInfo();
			for (Entity e : entities.values()) {
				if (e instanceof Way) {
					Way w = (Way) e;
					boolean missingNode = false;
					EntityId eid = EntityId.valueOf(e);
					EntityInfo info = infos.get(EntityId.valueOf(e));
					Way way;
					if(mainEntities.containsKey(eid)) {
						way = new Way(nid++);
					} else {
						way = new Way(w.getId());
					}
					way.copyTags(w);
					for (Node n : w.getNodes()) {
						if (n == null) {
							missingNode = true;
							break;
						}
						Node nnode = new Node(n, nid++);
						way.addNode(nnode);
						mainEntities.put(EntityId.valueOf(nnode), nnode);
					}
					mainEntities.put(EntityId.valueOf(way), way);
					mainEntityInfo.put(EntityId.valueOf(way), info);
					if (missingNode) {
						LOG.info(String.format("Missing node for way %d", w.getId()));
						continue;
					}
					total++;
					if (total % 1000 == 0) {
						LOG.info("Processed " + total + " ways");
					}
					addRegionTag(or, way);
					transformer.addPropogatedTags(way);
					Map<String, String> ntags = renderingTypes.transformTags(way.getModifiableTags(), EntityType.WAY,
							EntityConvertApplyType.MAP);
					if (way.getModifiableTags() != ntags) {
						way.getModifiableTags().putAll(ntags);
					}
					processWay(way);
				}
			}
		}
		
		
        List<EntityId> toWrite = new ArrayList<EntityId>();
		processRegion(toWrite);
		OsmStorageWriter writer = new OsmStorageWriter();
		LOG.info("Writing file... ");
		writer.saveStorage(new FileOutputStream(write), mainEntities, mainEntityInfo, toWrite, true);
		LOG.info("DONE");
	}

	private OsmandRegions prepareRegions() throws IOException {
		OsmandRegions or = new OsmandRegions();
		or.prepareFile();
		or.cacheAllCountries();
		return or;
	}
	
	private OsmBaseStorage parseOsmFile(File read) throws FileNotFoundException, IOException, XmlPullParserException {
		OsmBaseStorage storage = new OsmBaseStorage();
        InputStream stream = new BufferedInputStream(new FileInputStream(read), 8192 * 4);
		InputStream streamFile = stream;
        if (read.getName().endsWith(".bz2")) { //$NON-NLS-1$
        	stream = new BZip2CompressorInputStream(stream);
        } else if (read.getName().endsWith(".gz")) { //$NON-NLS-1$
        	stream = new GZIPInputStream(stream);
        }
        
        
        storage.getFilters().add(new IOsmStorageFilter() {
			boolean nodeAccepted = true;
			@Override
			public boolean acceptEntityToLoad(OsmBaseStorage storage, EntityId entityId, Entity entity) {
				if(!FILTER_BBOX) {
					return true;
				}
				if(entity instanceof Node) {
					double lat = ((Node) entity).getLatitude();
					double lon = ((Node) entity).getLongitude();
					if(lat > TOP_LAT || lat < BOTTOM_LAT) {
						nodeAccepted = false;
						return false;
					}
					if(lon > RIGHT_LON || lon < LEFT_LON) {
						nodeAccepted = false;
						return false;
					}
					return true;
				} else {
					if(nodeAccepted) {
						return true;
					} else {
						nodeAccepted = true;
						return false;
					}
				}
			}
		});
		storage.parseOSM(stream, new ConsoleProgressImplementation(), streamFile, true);
		return storage;
	}

    public static long convertLatLon(LatLon l) {
        long lx = (long) MapUtils.get31TileNumberY(l.getLatitude()) >> (31 - APPROXIMATE_POINT_ZOOM);
        lx = (lx << 31) | (int) MapUtils.get31TileNumberX(l.getLongitude()) >> (31 - APPROXIMATE_POINT_ZOOM);
        return lx;
    }

    static class RoadLine {
        ArrayList<Way> combinedWays = new ArrayList<Way>();
        Node last;
        long beginPoint;
        Node first;
        long endPoint;
        String highway;
        String ref;
        double distance = 0;
        boolean deleted = false;
        boolean isLink = false;
		

        RoadLine(Way w, String ref, String hwType) {
            combinedWays.add(w);
            this.ref = ref;
            this.highway = hwType;
			updateData();
        }
        
        public String getHighway() {
			return highway;
		}
        
        public String getSimpleRef() {
        	if(ref != null) {
        		return ref.replace("-", "").replace(" ", "");
        	}
			return ref;
		}
        
		private void updateData() {
			distance = 0;
			first = null;
			last = null;
			for (int j = 0; j < combinedWays.size(); j++) {
				Way w = combinedWays.get(j);
				List<Node> nodes = w.getNodes();
				for (int i = 1; i < nodes.size(); i++) {
					if (first == null) {
						first = nodes.get(i - 1);
					}
					last = nodes.get(i);
					if (nodes.get(i - 1) != null && nodes.get(i) != null) {
						distance += OsmMapUtils.getDistance(nodes.get(i - 1), nodes.get(i));
					}
				}
			}
			// beginPoint = combinedWays.get(0).getFirstNodeId();
			// endPoint = combinedWays.get(combinedWays.size() - 1).getLastNodeId();
			if(first != null) {
				beginPoint = convertLatLon(first.getLatLon());
			}
			if(last != null) {
				endPoint = convertLatLon(last.getLatLon());
			}
		}

        EntityId getFirstWayId() {
            return EntityId.valueOf(combinedWays.get(0));
        }

        Way getFirstWay(){
            return combinedWays.get(0);
        }

        Way getLastWay(){
            return combinedWays.get(combinedWays.size() - 1);
        }
        
        void delete() {
        	deleted = true;
        }
        
        boolean isDeleted() {
        	return deleted;
        }

        List<Node> getFirstPoints(double distance) {
            ArrayList<Node> ns = new ArrayList<Node>();
            Node prev = null;
            double ds = 0;
            loop : for(int j = 0 ; j < combinedWays.size() ; j++) {
                List<Node> nps = combinedWays.get(j).getNodes();
                for(int i = 0 ; i < nps.size() - 1 ||
                        (i < nps.size()  && j == combinedWays.size() - 1); i++) {
                    Node np = nps.get(i);
                    if(np != null) {
                        ns.add(np);
                        if(prev != null) {
                            ds += OsmMapUtils.getDistance(np, prev);
                            if(ds > distance) {
                                break loop;
                            }
                        }
                        prev = np;
                    }
                }
            }
            return ns;
        }

        List<Node> getLastPoints(double distance) {
            ArrayList<Node> ns = new ArrayList<Node>();
            Node prev = null;
            double ds = 0;
            loop : for(int j = combinedWays.size() - 1 ; j >= 0; j--) {
                List<Node> nps = combinedWays.get(j).getNodes();
                for(int i = nps.size() - 1 ; i > 0 || (i == 0 && j == 0); i--) {
                    Node np = nps.get(i);
                    if(np != null) {
                        ns.add(np);
                        if(prev != null) {
                            ds += OsmMapUtils.getDistance(np, prev);
                            if(ds > distance) {
                                break loop;
                            }
                        }
                        prev = np;
                    }
                }
            }
            Collections.reverse(ns);
            return ns;
        }

        public void insertInBeginning(RoadLine toMerge) {
            combinedWays.addAll(0, toMerge.combinedWays);
            first = toMerge.first;
            distance += toMerge.distance;
            this.beginPoint = toMerge.beginPoint;
            isLink = toMerge.isLink && isLink;
        }
        
        public void insertInToEnd(RoadLine toMerge) {
            combinedWays.addAll(toMerge.combinedWays);
            last = toMerge.last;
            distance += toMerge.distance;
            this.endPoint = toMerge.endPoint;
            isLink = toMerge.isLink && isLink;
        }

        public void combineWaysIntoOneWay() {
            Way first = combinedWays.get(0);
            for(int i = 1; i < combinedWays.size(); i++) {
                boolean f = true;
                for(Node n : combinedWays.get(i).getNodes()) {
                    if(n != null && !f){
                        first.addNode(n);
                    }
                    f = false;
                }
            }
	        // don't keep names
	        first.removeTag("name");
	        first.removeTag("junction");
	        if(highway == null) {
	        	first.removeTag("highway");
	        } else {
	        	first.putTag("highway", highway);
	        }
	        if(ref == null) {
	        	first.removeTag("ref");
	        } else {
	        	first.putTag("ref", ref);
	        }
        }

        public void reverse() {
            Collections.reverse(combinedWays);
            for(Way w : combinedWays) {
                w.reverseNodes();
            }
            Node n = first;
            first = last;
            last = n;
            long np = beginPoint;
            beginPoint = endPoint;
            endPoint = np;
        }
        
        private LatLon getEdgePoint( boolean start) {
			if(start) {
				return getFirstWay().getFirstNode().getLatLon();
			}
			return getLastWay().getLastNode().getLatLon();
		}
    }
    
    public enum RoadInfoRefType {
    	// proceed empty ref first (so we can create "roundabouts", "links" from too short roads)
    	EMPTY_REF,
    	REF,
    	INT_REF,
    	ADMIN_LEVEL
    }
    
    public enum RoadSimplifyStages {
    	MERGE_UNIQUE,
    	MERGE_BEST,
    	MERGE_WITH_OTHER_REF,
    	WRITE
    }
    
    public enum RoadType {
    	MOTORWAY ("motorway", 3.0),
    	TRUNK ("trunk", 2.0),
    	PRIMARY ("primary", 1.5);
    	
    	private String highwayVal;
    	private double weight;
    	
    	private RoadType (String highwayVal, double weight) {
    		this.highwayVal = highwayVal;
    		this.weight = weight;
    	};
    	
    	public double getWeight() {
    		return this.weight;
    	}
    	
    	public String getTag() {
    		return this.highwayVal;
    	}
    	
    }

    class RoadInfo {
    	
		final String ref;
	    final RoadInfoRefType type;
        List<RoadLine> roadLines = new ArrayList<RoadLine>();
        TLongObjectHashMap<List<RoadLine>> startPoints = new TLongObjectHashMap<List<RoadLine>>();
        TLongObjectHashMap<List<RoadLine>> endPoints = new TLongObjectHashMap<List<RoadLine>>();
		
		public RoadInfo(String rf, RoadInfoRefType type) {
			this.ref = rf;
			this.type = type;
		}
		
		public int countRoadLines() {
			int i = 0; 
			for(RoadLine r : roadLines) {
				if(!r.isDeleted()) {
					i++;
				}
			}
			return i;
		}
		
        public void compactDeleted() {
        	Iterator<RoadLine> it = roadLines.iterator();
        	while(it.hasNext()) {
        		if(it.next().isDeleted()) {
        			it.remove();
        		}
        	}
        }
        
        private void registerRoadLine(RoadLine r){
        	if(r.first != null && r.last != null) {
        		roadLines.add(r);
        		registerPoints(r);
        	}
        }
        
        private void registerPoints(RoadLine r) {
            if(!startPoints.containsKey(r.beginPoint)){
                startPoints.put(r.beginPoint, new ArrayList<RoadLine>());
            }
            startPoints.get(r.beginPoint).add(r);
            if(!endPoints.containsKey(r.endPoint)){
                endPoints.put(r.endPoint, new ArrayList<RoadLine>());
            }
            endPoints.get(r.endPoint).add(r);
        }

        public void mergeRoadInto(RoadLine toMerge, RoadLine toKeep, boolean mergeToEnd) {
        	if(!Algorithms.objectEquals(toMerge.ref, toKeep.ref)) {
        		if(toKeep.distance > MINIMAL_DISTANCE || toMerge.distance > MINIMAL_DISTANCE) {
        			toKeep.ref = null;
        		} else if(toMerge.distance > toKeep.distance) {
            		toKeep.ref = toMerge.ref;
            	}	
        	}
        	if(toMerge.distance > toKeep.distance) {
        		toKeep.highway = toMerge.highway;
        	}
        	if(mergeToEnd) {
        		long op = toKeep.endPoint;
        		toKeep.insertInToEnd(toMerge);
        		endPoints.get(op).remove(toKeep);
        		endPoints.get(toKeep.endPoint).add(toKeep);
        	} else {
        		long op = toKeep.beginPoint;
                toKeep.insertInBeginning(toMerge);
                startPoints.get(op).remove(toKeep);
                startPoints.get(toKeep.beginPoint).add(toKeep);
        	}
        	startPoints.get(toMerge.beginPoint).remove(toMerge);
            endPoints.get(toMerge.endPoint).remove(toMerge);
            toMerge.delete();
        }

		public List<RoadLine> getConnectedLinesStart(long point) {
			TLongHashSet rs = roundabouts.get(point);
			if (rs == null) {
				List<RoadLine> res = startPoints.get(point);
				if (res != null) {
					return res;
				}
				return Collections.emptyList();
			}
			List<RoadLine> newL = new ArrayList<RoadLine>();
			TLongIterator it = rs.iterator();
			while (it.hasNext()) {
				List<RoadLine> lt = startPoints.get(it.next());
				if (lt != null) {
					newL.addAll(lt);
				}
			}
			return newL;
		}

		public List<RoadLine> getConnectedLinesEnd(long point) {
			TLongHashSet rs = roundabouts.get(point);
			if (rs == null) {
				List<RoadLine> res = endPoints.get(point);
				if (res != null) {
					return res;
				}
				return Collections.emptyList();
			}
			List<RoadLine> newL = new ArrayList<RoadLine>();
			TLongIterator it = rs.iterator();
			while (it.hasNext()) {
				List<RoadLine> lt = endPoints.get(it.next());
				if (lt != null) {
					newL.addAll(lt);
				}
			}
			return newL;
		}

        public void reverseRoad(RoadLine roadLine) {
            startPoints.get(roadLine.beginPoint).remove(roadLine);
            endPoints.get(roadLine.endPoint).remove(roadLine);
            roadLine.reverse();
            registerPoints(roadLine);
        }

        public double getTotalDistance() {
        	double d = 0;
        	for(RoadLine rl : roadLines) {
        		if(!rl.isDeleted()) {
        			d += rl.distance;
        		}
        	}
			return d;
		}
        

		public String toString(String msg) {
			return String.format("Road %s - %s %d - segments, %.2f dist", 
					ref, msg, countRoadLines(), getTotalDistance());
		}

		
    }

    private Map<String, RoadInfo> roadInfoMap = new LinkedHashMap<String, RoadInfo>();
    private TLongObjectHashMap<TLongHashSet> roundabouts = new TLongObjectHashMap<TLongHashSet>();

	private void processRegion(List<EntityId> toWrite) throws IOException {
		
		// process in certain order by ref type
		EnumSet<RoadSimplifyStages> stages = EnumSet.of(RoadSimplifyStages.MERGE_UNIQUE, RoadSimplifyStages.MERGE_BEST);
		Collection<RoadInfo> infos = roadInfoMap.values();
		processRoadInfoByStages(infos, stages);
		RoadInfo global = new RoadInfo("", RoadInfoRefType.REF);
		for(RoadInfo l : roadInfoMap.values()) {
			for(RoadLine rl : l.roadLines) {
				global.registerRoadLine(rl);
			}
		}
		processRoadInfoByStages(Collections.singleton(global), stages);
		writeWays(toWrite, global);
	}

	private void processRoadInfoByStages(Collection<RoadInfo> infos, EnumSet<RoadSimplifyStages> stages) {
		for (RoadSimplifyStages stage : stages) {
			for (RoadInfoRefType tp : RoadInfoRefType.values()) {
				for (RoadInfo ri : infos) {
					if (ri.type == tp) {
						processRoadsByRef(stage, ri);
					}
				}
			}
		}
	}

	private void processRoadsByRef(RoadSimplifyStages stage, RoadInfo ri) {
		boolean verbose = ri.getTotalDistance() > 40000;
		if (RoadSimplifyStages.MERGE_UNIQUE == stage) {
			if (verbose) {
				System.out.println(ri.toString("Initial:"));
			}
			// combine unique roads
			int merged = combineRoadsWithLongestRoad(ri, true);
			if (verbose) {
				System.out.println(String.format("After combine unique merged (%d): ", merged));
			}
			for (RoadLine r : ri.roadLines) {
				if (r.distance < MINIMUM_DISTANCE_LINK && !r.isDeleted()) {
					for (Way way : r.combinedWays) {
						addRoadToRoundabouts(way);
					}
					r.delete();
				}
			}
		} else if (RoadSimplifyStages.MERGE_BEST == stage) {
			// not needed more than 1
			int RUNS = 1;
			for (int run = 0; run < RUNS; run++) {
				int merged = combineRoadsWithLongestRoad(ri, false);
				if (verbose)
					System.out.println(
							ri.toString(String.format("After combine %d best merged (%d): ", run + 1, merged)));
				if (merged == 0) {
					break;
				}
			}
			
		}
		
	}

	private void writeWays(List<EntityId> toWrite, RoadInfo ri) {
		ri.compactDeleted();
		for (RoadLine ls : ri.roadLines) {
			if (ls.distance > MINIMAL_DISTANCE ) {
				ls.combineWaysIntoOneWay();
				Way w = ls.getFirstWay();
				String hw = w.getTag(OSMTagKey.HIGHWAY);
				if (hw != null && hw.endsWith("_link")) {
					w.putTag("highway", hw.substring(0, hw.length() - "_link".length()));
				}
				toWrite.add(ls.getFirstWayId());
			}
		}
	}

	private void addRegionTag(OsmandRegions or, Way firstWay) throws IOException {
		QuadRect qr = firstWay.getLatLonBBox();
		int lx = MapUtils.get31TileNumberX(qr.left);
		int rx = MapUtils.get31TileNumberX(qr.right);
		int by = MapUtils.get31TileNumberY(qr.bottom);
		int ty = MapUtils.get31TileNumberY(qr.top);
		List<BinaryMapDataObject> bbox = or.query(lx, rx, ty, by);
		TreeSet<String> lst = new TreeSet<String>();
		for (BinaryMapDataObject bo : bbox) {
			String dw = or.getDownloadName(bo);
			if (!Algorithms.isEmpty(dw) && or.isDownloadOfType(bo, OsmandRegions.MAP_TYPE)) {
				lst.add(dw);
			}
		}
		firstWay.putTag(MapRenderingTypesEncoder.OSMAND_REGION_NAME_TAG, serialize(lst));
	}
	
	private static String serialize(TreeSet<String> lst) {
		StringBuilder bld = new StringBuilder();
		Iterator<String> it = lst.iterator();
		while(it.hasNext()) {
			String next = it.next();
			if(bld.length() > 0) {
				bld.append(",");
			}
			bld.append(next);
		}
		return bld.toString();
	}

    
    
    private double directionRoute(List<Node> ns) {
        double x = MapUtils.get31TileNumberX(ns.get(0).getLongitude());
        double y = MapUtils.get31TileNumberY(ns.get(0).getLatitude());
        double px = MapUtils.get31TileNumberX(ns.get(ns.size() - 1).getLongitude());
        double py = MapUtils.get31TileNumberY(ns.get(ns.size() - 1).getLatitude());
        if(x == px && y == py) {
            return 0;
        }
        return -Math.atan2( x - px, y - py );
    }

	private int combineRoadsWithLongestRoad(RoadInfo ri, boolean combineOnlyUnique) {
		boolean merged = true;
		Collections.sort(ri.roadLines, new Comparator<RoadLine>() {

			@Override
			public int compare(RoadLine o1, RoadLine o2) {
				return -Double.compare(o1.distance, o2.distance);
			}
		});
		int mergedCnt = 0;
		while (merged) {
			merged = false;
			int inc = 1;
			for (int i = 0; i < ri.roadLines.size(); i += inc) {
				RoadLine longRoadToKeep = ri.roadLines.get(i);
				inc = 1;
				if (longRoadToKeep.isDeleted()) {
					// line already merged and deleted
					continue;
				} else {
					boolean attachedToEnd = findRoadToCombine(ri, longRoadToKeep, combineOnlyUnique, true);
					boolean attachedToStart = findRoadToCombine(ri, longRoadToKeep, combineOnlyUnique, false);
					if (attachedToEnd || attachedToStart) {
						merged = true;
						mergedCnt++;
						inc = 0;
					}
				}
			}
		}
		return mergedCnt;
	}

	private class RoadLineConnection {
		RoadLine rl;
		boolean reverse;
		double direction;
		boolean startPoints;
		
		public RoadLineConnection(RoadLine rl, boolean startPoints, boolean reverse) {
			this.startPoints = startPoints;
			this.reverse = reverse;
			this.rl = rl;
			direction = directionRoute(startPoints ? 
					rl.getFirstPoints(DIST_DIRECTION) : rl.getLastPoints(DIST_DIRECTION));
			if(reverse) {
				direction = direction - Math.PI;
			}
		}

		public LatLon getStartPoint() {
			return rl.getEdgePoint(startPoints);
		}

	}
	
	private static final int CONNECT_NOT_ALLOWED = 0;
	private static final int CONNECT_S1 = 1;
	private static final int CONNECT_S2 = 2;
	private static final int CONNECT_S3 = 3;
	private static final int CONNECT_S4= 4;
	
	private int getConnectionType(RoadLine longRoadToKeep, RoadLine candidate) {
		if (!Algorithms.stringsEqual(longRoadToKeep.getHighway(), candidate.getHighway())) {
			if (candidate.distance < MINIMAL_DISTANCE || longRoadToKeep.distance < MINIMAL_DISTANCE ) {
				// connect anyway
				return CONNECT_S1;
			}
			if (candidate.distance < PREFERRED_DISTANCE || longRoadToKeep.distance < PREFERRED_DISTANCE ) {
				// connect anyway
				return CONNECT_S2;
			}
			return CONNECT_NOT_ALLOWED;
		}
		if (!Algorithms.stringsEqual(longRoadToKeep.getSimpleRef(), candidate.getSimpleRef())) {
			return CONNECT_S3;	
		} else {
			return CONNECT_S4;	
		}
		
	}
	
	private boolean findRoadToCombine(RoadInfo ri, RoadLine longRoadToKeep, 
			boolean onlyUnique, boolean attachToEnd) {
		long point = attachToEnd ? longRoadToKeep.endPoint : longRoadToKeep.beginPoint;
		double direction = directionRoute(attachToEnd ? longRoadToKeep.getLastPoints(DIST_DIRECTION)
				: longRoadToKeep.getFirstPoints(DIST_DIRECTION));
		// 1. find all connected candidates
		List<RoadLineConnection> candidates = new ArrayList<RoadLineConnection>();
		for (RoadLine roadToAttach : ri.getConnectedLinesStart(point)) {
			if (!roadToAttach.isDeleted() && roadToAttach != longRoadToKeep) {
				candidates.add(new RoadLineConnection(roadToAttach, true, !attachToEnd));
			}
		}
		for (RoadLine roadToAttach : ri.getConnectedLinesEnd(point)) {
			if (!roadToAttach.isDeleted() && roadToAttach != longRoadToKeep) {
				candidates.add(new RoadLineConnection(roadToAttach, false, attachToEnd));
			}
		}
		RoadLineConnection merge = null;
		double bestContinuationMetric = 0;
		// 2. find best candidate to merge with
		for (RoadLineConnection r : candidates) {
			// use angle difference as metric for merging
			double angle = Math.abs(MapUtils.alignAngleDifference(direction - r.direction));
			boolean straight = angle < ANGLE_TO_ALLOW_DIRECTION;
			int connectionType = getConnectionType(longRoadToKeep, r.rl);
			if (!straight || connectionType == CONNECT_NOT_ALLOWED) {
				continue;
			}
			double dist = MapUtils.getDistance(longRoadToKeep.getEdgePoint(!attachToEnd), r.getStartPoint());
			double continuationMetric = metric(connectionType, angle,  dist, longRoadToKeep.distance, r.rl.distance);
			if (merge == null) {
				merge = r;
				bestContinuationMetric = continuationMetric;
			} else {
				if (onlyUnique) {
					// not unique
					merge = null;
					break;
				} else {
					if (bestContinuationMetric > continuationMetric) {
						merge = r;
						bestContinuationMetric = continuationMetric;
					}
				}
			}
		}
		
		// 3. check there is no better candidate to be merged with
		if (merge != null) {
			for (RoadLineConnection r : candidates) {
				double angle = Math.abs(MapUtils.alignAngleDifference(merge.direction - r.direction - Math.PI));
				boolean straight = angle < ANGLE_TO_ALLOW_DIRECTION;
				int connectionType = getConnectionType(merge.rl, r.rl);
				if (!straight || connectionType == CONNECT_NOT_ALLOWED) {
					continue;
				}
				double dist = MapUtils.getDistance(merge.getStartPoint(), r.getStartPoint());
				double continuationMetric = metric(connectionType, angle, dist, merge.rl.distance, r.rl.distance);
				if (onlyUnique) {
					// don't merge there is another candidate for that road
					merge = null;
					break;
				} else {
					if (bestContinuationMetric > continuationMetric) {
						// don't merge there is better candidate for that road
						merge = null;
						break;
					}
				}
			}
		}

		if (merge != null) {
			if (merge.reverse) {
				ri.reverseRoad(merge.rl);
			}
			ri.mergeRoadInto(merge.rl, longRoadToKeep, attachToEnd);
			return true;
		}
		return false;
	}

	


	private double metric(int connType, double angleDiff, double distBetween, double l1, double l2) {
		double metrics = distBetween;
		// give X meters penalty in case road is too short
		metrics += penaltyDist(l1);
		metrics += penaltyDist(l2);
		// 90 degrees, penalty - 50 m
		if(angleDiff > METRIC_ANGLE_THRESHOLD) {
			metrics += angleDiff / (Math.PI / 2) * METRIC_ANGLE_PENALTY;
		}
		return metrics + connType * 10000;
	}

	private double penaltyDist(double l1) {
		double metrics;
		if(l1 < METRIC_SHORT_LENGTH_THRESHOLD) {
			metrics = METRIC_SHORT_LENGTH_PENALTY;
		} else if(l1 > METRIC_LONG_LENGTH_THRESHOLD) {
			metrics = METRIC_LONG_LENGTH_BONUS;
		} else {
			metrics = METRIC_SHORT_LENGTH_PENALTY +  (l1 - METRIC_SHORT_LENGTH_THRESHOLD) / 
					(METRIC_LONG_LENGTH_THRESHOLD - METRIC_SHORT_LENGTH_THRESHOLD) * (METRIC_LONG_LENGTH_BONUS - METRIC_SHORT_LENGTH_PENALTY);
		}	
		return metrics;
	}

	private void processWay(Way way) {
		String ref = way.getTag("ref");
		String hw = way.getTag("highway");
        boolean isLink = hw != null && hw.endsWith("_link");
        if(isLink) {
        	hw = hw.substring(0, hw.length() - "_link".length());
        }
        // boolean shortDist = MapUtils.getDistance(way.getLastNode().getLatLon(), way.getFirstNode().getLatLon()) < MINIMUM_DISTANCE_LINK;
		if (convertLatLon(way.getFirstNode().getLatLon()) == convertLatLon(way.getLastNode().getLatLon()) || 
				"roundabout".equals(way.getTag("junction")) || isLink) {
			addRoadToRoundabouts(way);
			return;
		}
		RoadInfoRefType type = RoadInfoRefType.REF;
		
		if (ref == null || ref.isEmpty()) {
			type = RoadInfoRefType.INT_REF;
			ref = way.getTag("int_ref");
		}
		
		if (ref == null || ref.isEmpty()) {
			// too many breaks in between names
//			ref = way.getTag("name");
			type = RoadInfoRefType.EMPTY_REF;
		} else {
			if (ref.indexOf(';') != -1) {
				ref = ref.substring(0, ref.indexOf(';'));
			}
		}
		String originalRef = ref;
		if (hw != null) {
			ref += "_" + hw;
		}
		if(ref != null) {
			// fix road inconsistency
			ref = ref.replace("-", "").replace(" ", "");
		}

		if(ref == null || ref.isEmpty() || way.getTag("railway") != null) {
			LatLon lt = way.getLatLon();
			if(way.getTag("admin_level") != null) {
				type = RoadInfoRefType.ADMIN_LEVEL;
				ref = way.getTag("admin_level");
				ref += ((int) MapUtils.getTileNumberY(ADMIN_LEVEL_ZOOM_SPLIT, lt.getLatitude())) + " "
					+ ((int) MapUtils.getTileNumberX(ADMIN_LEVEL_ZOOM_SPLIT, lt.getLongitude()));
			} else if(way.getTag("railway") != null) {
				ref = ((int) MapUtils.getTileNumberY(RAILWAY_ZOOM_SPLIT, lt.getLatitude())) + " "
						+ ((int) MapUtils.getTileNumberX(RAILWAY_ZOOM_SPLIT, lt.getLongitude()));
			} else {
				ref = ((int) MapUtils.getTileNumberY(REF_EMPTY_ZOOM_SPLIT, lt.getLatitude())) + " "
						+ ((int) MapUtils.getTileNumberX(REF_EMPTY_ZOOM_SPLIT, lt.getLongitude()));	
			}
		}
		
		RoadInfo ri = roadInfoMap.get(ref);
		if (ri == null) {
			ri = new RoadInfo(ref, type);
			roadInfoMap.put(ref, ri);
		}
		ri.registerRoadLine(new RoadLine(way, originalRef, hw));
	}

	private void addRoadToRoundabouts(Way way) {
		List<Node> wn = way.getNodes();
		TLongHashSet allPointSet = new TLongHashSet();
		for (Node n : wn) {
			if (n != null) {
				long pnt = convertLatLon(n.getLatLon());
				allPointSet.add(pnt);
				TLongHashSet prev = roundabouts.put(pnt, allPointSet);
				if (prev != null && prev != allPointSet) {
					allPointSet.addAll(prev);
					TLongIterator it = prev.iterator();
					while (it.hasNext()) {
						long pntP = it.next();
						TLongHashSet prevPnts = roundabouts.put(pntP, allPointSet);
						if (!allPointSet.containsAll(prevPnts)) {
							throw new IllegalStateException("Error in algorithm");
						}
					}
				}
			}
		}
	}

}
