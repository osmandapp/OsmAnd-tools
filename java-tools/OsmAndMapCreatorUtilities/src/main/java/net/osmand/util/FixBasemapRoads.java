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
import java.util.Collections;
import java.util.Comparator;
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
    
	private static int MINIMAL_DISTANCE = 5000; // -> 1500? primary
	
	// consider road as link if distance is < 20m
	private static final double MINIMUM_DISTANCE_LINK = 20;
	
	// distance to measure direction of the road (150 m)
	private static final double DIST_DIRECTION = 150;
	
	// straight angle
	private static final double STRAIGHT_ANGLE = Math.PI / 2;
	
	// metrics penalty = connection distance + shortLengthPenalty + (90 - angle diff) * anglePenalty
	private static final double METRIC_LENGTH_THRESHOLD = 100;
	private static final double METRIC_SHORT_LENGTH_PENALTY = 100;
	private static final double METRIC_ANGLE_PENALTY = 50;
	
	// making it less < 31 joins unnecessary roads make merge process more complicated 
	private static final int APPROXIMATE_POINT_ZOOM = 31;


	
	
    private static boolean FILTER_BBOX = false;  
    private static double LEFT_LON = 4.12;
    private static double RIGHT_LON = 4.43;
    private static double TOP_LAT = 52.15;
    private static double BOTTOM_LAT = 51.94;

	public static void main(String[] args) throws Exception {
		if(args == null || args.length == 0) {
//			String line = "motorway_n";
//			String line = "trunk_n";
			String line = "primary_n";
//			line = "secondary";
//			line = "tertiary";
//			line = "nlprimary";
//			line = "nlsecondary";
			args = new String[] {
					System.getProperty("maps.dir") + "basemap/line_" + line + ".osm.gz",
					System.getProperty("maps.dir") + "basemap/proc_" + MINIMAL_DISTANCE + "_line_" + line + ".osm",
					//System.getProperty("maps.dir") + "route_road.osm.gz"
					
			};
		}
		String fileToRead = args[0] ;
		File read = new File(fileToRead);
		String fileToWrite =  args[1];
		List<File> relationFiles = new ArrayList<>();
		if(args.length > 2) {
			for(int i = 2; i < args.length; i++) {
				relationFiles.add(new File(args[i]));
			}
		}
		File write = new File(fileToWrite);
		write.createNewFile();
        new FixBasemapRoads().process(read, write, relationFiles);
	}

	private void process(File read, File write, List<File> relationFiles) throws  IOException, XMLStreamException, XmlPullParserException, SQLException {
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
        
        
		LOG.info("Parse main file " + read.getName());
		OsmBaseStorage storage = parseOsmFile(read);
		Map<EntityId, Entity> entities = new HashMap<EntityId, Entity>( storage.getRegisteredEntities());
		int total = 0;
		for(EntityId e : entities.keySet()){
			if(e.getType() == EntityType.WAY){
				Way es = (Way) storage.getRegisteredEntities().get(e);
				boolean missingNode = false;
				for(Node n : es.getNodes()) {
					if(n == null) {
						missingNode = true;
						break;
					}
				}
				if(missingNode) {
					continue;
				}
				total++;
				if(total % 1000 == 0) {
					LOG.info("Processed " + total + " ways");
				}
				
				
				addRegionTag(or, es);
				transformer.addPropogatedTags(es);
				Map<String, String> ntags = renderingTypes.transformTags(es.getModifiableTags(), EntityType.WAY, EntityConvertApplyType.MAP);
				if(es.getModifiableTags() != ntags) {
					es.getModifiableTags().putAll(ntags);
				}
				processWay(es);
			}
		}
        List<EntityId> toWrite = new ArrayList<EntityId>();

		processRegion(toWrite);
		OsmStorageWriter writer = new OsmStorageWriter();
		LOG.info("Writing file... ");
		writer.saveStorage(new FileOutputStream(write), storage, toWrite, true);
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
        double distance = 0;
        boolean deleted = false;
        boolean isLink = false;

        RoadLine(Way w) {
            combinedWays.add(w);
            String hw = w.getTag("highway");
            isLink = hw != null && hw.endsWith("_link");
			updateData();
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
    	REF,
    	INT_REF,
    	EMPTY_REF,
    	ADMIN_LEVEL
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
		for (RoadInfoRefType tp : RoadInfoRefType.values()) {
			for (String ref : roadInfoMap.keySet()) {
				RoadInfo ri = roadInfoMap.get(ref);
				if (ri.type == tp) {
					processRoadsByRef(toWrite, ri, null);
				}
			}
		}
	}

	private void processRoadsByRef(List<EntityId> toWrite, RoadInfo ri, RoadInfo emptyRoads) {
		boolean verbose = ri.getTotalDistance() > 40000;
		if(verbose) { 
			System.out.println(ri.toString("Initial:"));
		}
		// combine unique roads
		int merged = combineRoadsWithLongestRoad(ri, true, emptyRoads);
		if(verbose) {
			System.out.println(String.format("After combine unique merged (%d): ", merged));
		}
		
		// not needed more than 1
		for (int i = 0; i < 1; i++) {
			merged = combineRoadsWithLongestRoad(ri, false, emptyRoads);
			 if (verbose) System.out.println(ri.toString(String.format("After combine %d best merged (%d): ", i + 1, merged)));
			if (merged == 0) {
				break;
			}
		}
			
		
		
		ri.compactDeleted();
		for (RoadLine ls : ri.roadLines) {
			if (ls.distance > MINIMAL_DISTANCE && !ls.isDeleted()) {
				ls.combineWaysIntoOneWay();
				Way w = ls.getFirstWay();
				String hw = w.getTag(OSMTagKey.HIGHWAY);
				if(hw != null && hw.endsWith("_link")) {
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
		List<BinaryMapDataObject> bbox = or.queryBbox(lx, rx, ty, by);
		TreeSet<String> lst = new TreeSet<String>();
		for (BinaryMapDataObject bo : bbox) {
//						if (!or.intersect(bo, lx, ty, rx, by)) {
//							continue;
//						}
			if(or.contain(bo, lx/2+rx/2, by/2 + ty/2)) {
				String dw = or.getDownloadName(bo);
				if (!Algorithms.isEmpty(dw) && or.isDownloadOfType(bo, OsmandRegions.MAP_TYPE)) {
					lst.add(dw);
				}
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

	private int combineRoadsWithLongestRoad(RoadInfo ri, boolean combineOnlyUnique, RoadInfo emptyRoads) {
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
					boolean attachedToEnd = findRoadToCombine(ri, longRoadToKeep, emptyRoads, combineOnlyUnique, true);
					boolean attachedToStart = findRoadToCombine(ri, longRoadToKeep, emptyRoads, combineOnlyUnique, false);
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
	
	
	private boolean findRoadToCombine(RoadInfo ri, RoadLine longRoadToKeep, RoadInfo emptyRoads,
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
			boolean straight = angle < STRAIGHT_ANGLE;
			if (!straight) {
				continue;
			}
			double dist = MapUtils.getDistance(longRoadToKeep.getEdgePoint(!attachToEnd), r.getStartPoint());
			double continuationMetric = metric(angle,  dist, longRoadToKeep.distance, r.rl.distance);
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
				boolean straight = angle < STRAIGHT_ANGLE;
				if (!straight) {
					continue;
				}
				double dist = MapUtils.getDistance(merge.getStartPoint(), r.getStartPoint());
				double continuationMetric = metric(angle, dist, merge.rl.distance, r.rl.distance);
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

	


	private double metric(double angleDiff, double distBetween, double l1, double l2) {
		double metrics = distBetween;
		// give X meters penalty in case road is too short
		if(l1 < METRIC_LENGTH_THRESHOLD) {
			metrics += METRIC_SHORT_LENGTH_PENALTY;
		}
		if(l2 < METRIC_LENGTH_THRESHOLD) {
			metrics += METRIC_SHORT_LENGTH_PENALTY;
		}
		// 90 degrees, penalty - 50 m
		metrics += angleDiff / (Math.PI / 2) * METRIC_ANGLE_PENALTY;
		return metrics;
	}

	private void processWay(Way way) {
		String ref = way.getTag("ref");
		String hw = way.getTag("highway");
        boolean isLink = hw != null && hw.endsWith("_link");
        boolean shortDist = MapUtils.getDistance(way.getLastNode().getLatLon(), way.getFirstNode().getLatLon()) < MINIMUM_DISTANCE_LINK;
		if (way.getFirstNodeId() == way.getLastNodeId() || 
				"roundabout".equals(way.getTag("junction")) || isLink || shortDist) {
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
			// fix road inconsistency
			ref = ref.replace("-", "").replace(" ", "");
			if (ref.indexOf(';') != -1) {
				ref = ref.substring(0, ref.indexOf(';'));
			}
		}
		if((ref == null || ref.isEmpty()) && way.getTag("admin_level") != null) {
			type = RoadInfoRefType.ADMIN_LEVEL;
			ref = way.getTag("admin_level");
			LatLon lt = way.getLatLon();
			ref += ((int) MapUtils.getTileNumberY(4, lt.getLatitude())) + " "
					+ ((int) MapUtils.getTileNumberX(4, lt.getLongitude()));
		}
		
		if (ref == null || ref.isEmpty()) {
			LatLon lt = way.getLatLon();
			ref = ((int) MapUtils.getTileNumberY(6, lt.getLatitude())) + " "
					+ ((int) MapUtils.getTileNumberX(6, lt.getLongitude()));
		}

		RoadInfo ri = roadInfoMap.get(ref);
		if (ri == null) {
			ri = new RoadInfo(ref, type);
			roadInfoMap.put(ref, ri);
		}
		ri.registerRoadLine(new RoadLine(way));
	}

}
