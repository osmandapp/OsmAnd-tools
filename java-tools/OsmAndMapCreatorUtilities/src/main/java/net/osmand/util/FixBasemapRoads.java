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
    private static int MINIMAL_DISTANCE = 2000; // -> 1500? primary
    private final static Log LOG = PlatformUtil.getLog(FixBasemapRoads.class);
    
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
        
        // ref = way.getTag("name"); compare name ?
        public boolean isRoute1HigherPriority(RoadLine r1, RoadLine r2) {
        	if(r1.isLink == r2.isLink) {
        		if(r1.distance > r2.distance) {
        			return true;
        		}
        		return false;
			} else {
				if (r1.isLink) {
					return false;
				} else {
					return true;
				}
			}
        }

		public boolean isRoadTheBestToBeConnected(boolean attachToEnd, RoadLine roadLine, double resDirection) {
            boolean best = true;
            for (RoadLine rt : getConnectedLinesEnd(attachToEnd ? roadLine.endPoint : roadLine.beginPoint)) {
                if (!rt.isDeleted() && rt != roadLine && isRoute1HigherPriority(rt, roadLine)) {
                	double cd = directionRoute(getPoints(!attachToEnd, rt, false));
                	if(!oppositeDirectionAngle(cd, resDirection)) {
                		best = false;
                		break;
                	}
                }
            }
            for (RoadLine rt : getConnectedLinesStart(attachToEnd ? roadLine.endPoint : roadLine.beginPoint)) {
                if (!rt.isDeleted() && rt != roadLine && isRoute1HigherPriority(rt, roadLine)) {
                	double cd = directionRoute(getPoints(!attachToEnd, rt, true));
                	if(!oppositeDirectionAngle(cd, resDirection)) {
                		best = false;
                		break;
                	}
                }
            }
			return best;
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
		if(verbose) System.out.println(ri.toString("Initial:"));
		// combine unique roads
		combineRoadsWithLongestRoad(ri, true, emptyRoads);
		if(verbose) System.out.println(ri.toString("After combine unique: "));
		
		combineRoadsWithLongestRoad(ri, false, emptyRoads);
		if(verbose) System.out.println(ri.toString("After combine best: "));
		
		
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

    
    private boolean inSameDirectionLastPoints(RoadLine r2, RoadLine r1) {
        double last1 = directionRoute(r2.getLastPoints(150));
        double last2 = directionRoute(r1.getLastPoints(150));
        return sameDirectionAngle(last1, last2);
    }
    
    private boolean inSameDirectionFirstPoints(RoadLine r1, RoadLine r2) {
        double last1 = directionRoute(r1.getFirstPoints(150));
        double last2 = directionRoute(r2.getFirstPoints(150));
        return sameDirectionAngle(last1, last2);
    }

    private boolean inOppositeDirection(RoadLine end, RoadLine begin) {
        double last = directionRoute(end.getLastPoints(150));
        double first = directionRoute(begin.getFirstPoints(150));
        return oppositeDirectionAngle(last, first);
    }

    private boolean sameDirectionAngle(double last1, double last2) {
		double diff = MapUtils.alignAngleDifference(last2 - last1);
	    return Math.abs(diff) < Math.PI / 4;
	}

	private boolean oppositeDirectionAngle(double last, double first) {
		double diff = MapUtils.alignAngleDifference(first - last - Math.PI);
	    return Math.abs(diff) < Math.PI / 4;
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

	private void combineRoadsWithLongestRoad(RoadInfo ri, boolean combineOnlyUnique, RoadInfo emptyRoads) {
		boolean merged = true;
		Collections.sort(ri.roadLines, new Comparator<RoadLine>() {

			@Override
			public int compare(RoadLine o1, RoadLine o2) {
				return -Double.compare(o1.distance, o2.distance);
			}
		});
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
						inc = 0;
					}
				}
			}
		}
	}

	private boolean findRoadToCombine(RoadInfo ri, RoadLine longRoadToKeep, RoadInfo emptyRoads,
			boolean onlyUnique, boolean attachToEnd) {
		boolean merged = false;
		List<RoadLine> list = attachToEnd ? ri.getConnectedLinesStart(longRoadToKeep.endPoint)
				: ri.getConnectedLinesEnd(longRoadToKeep.beginPoint);
		RoadLine resToAttach = null;
		boolean readyToMerge = true;
		boolean resToAttachReverse = false;
		for (RoadLine roadToAttach : list) {
			boolean attach = checkRoadToAttach(longRoadToKeep, onlyUnique, attachToEnd, resToAttach, resToAttachReverse, roadToAttach, false);
			if(onlyUnique && attach && resToAttach != null) {
				readyToMerge = false;
				break;
			}
			if(attach) {
				resToAttach = roadToAttach;
				resToAttachReverse = false;
			}
		}
		// check reverse connected road (primary / trunks)
		List<RoadLine> rlist = attachToEnd ? ri.getConnectedLinesEnd(longRoadToKeep.endPoint)
				: ri.getConnectedLinesStart(longRoadToKeep.beginPoint);
		for (RoadLine roadToAttach : rlist) {
			boolean attach = checkRoadToAttach(longRoadToKeep, onlyUnique, attachToEnd, resToAttach, resToAttachReverse, roadToAttach, 
					true);
			if(onlyUnique && attach && resToAttach != null) {
				readyToMerge = false;
				break;
			}
			if(attach) {
				resToAttach = roadToAttach;
				resToAttachReverse = true;
			}
		}
		if (resToAttach != null && readyToMerge) {
			double resDirection = directionRoute(getPoints(attachToEnd, resToAttach, resToAttachReverse));
			readyToMerge = ri.isRoadTheBestToBeConnected(attachToEnd, longRoadToKeep, resDirection);
			if (readyToMerge) {
				merged = true;
				if (resToAttachReverse) {
					ri.reverseRoad(resToAttach);
				}
				ri.mergeRoadInto(resToAttach, longRoadToKeep, attachToEnd);
			}
		}
		return merged;
	}

	private boolean checkRoadToAttach(RoadLine longRoadToKeep, boolean onlyUnique, boolean attachToEnd,
			RoadLine resToAttach, boolean resToAttachReverse, RoadLine roadToAttach, boolean reverse) {
		if (roadToAttach.isDeleted() || roadToAttach == longRoadToKeep) {
			return false;
		}
		if (attachToEnd) {
			if(reverse && inSameDirectionLastPoints(longRoadToKeep, roadToAttach)) {
				return false;
			} else if (!reverse && inOppositeDirection(longRoadToKeep, roadToAttach)) {
				return false;
			}
		} else {
			if(reverse && inSameDirectionFirstPoints(roadToAttach, longRoadToKeep)) {
				return false;
			} else if (!reverse && inOppositeDirection(roadToAttach, longRoadToKeep)) {
				return false;
			}
		}
		if (resToAttach == null || onlyUnique) {
			return true;
		} else {
			double resDirection = directionRoute(getPoints(attachToEnd, resToAttach, resToAttachReverse));
			double roadDirection = directionRoute(getPoints(attachToEnd, roadToAttach, reverse));
			
			double roadItself = directionRoute(getPoints(!attachToEnd, longRoadToKeep, false));
			double resDiff = Math.abs(MapUtils.alignAngleDifference(resDirection - roadItself));
			double roadDiff = Math.abs(MapUtils.alignAngleDifference(roadDirection - roadItself));
			return isFirstBetterRoadToAttach(roadDiff, roadToAttach.distance, resDiff, resToAttach.distance); 
		}
	}

	private boolean isFirstBetterRoadToAttach(double contAngle1, double dist1, double contAngle2, double dist2) {
		if(contAngle1 < Math.PI / 6 && contAngle2 < Math.PI / 6) {
			return dist1 > dist2;
		}
		return contAngle1 < contAngle2;
	}

	private List<Node> getPoints(boolean startAngleOrEnd, RoadLine road, boolean reverseWay) {
		List<Node> resToAttachPoints;
		if(startAngleOrEnd) {
			if(reverseWay) {
				resToAttachPoints = road.getLastPoints(150);
				Collections.reverse(resToAttachPoints);
			} else {
				resToAttachPoints = road.getFirstPoints(150);
			}
		} else {
			if(reverseWay) {
				resToAttachPoints = road.getFirstPoints(150);
				Collections.reverse(resToAttachPoints);
			} else {
				resToAttachPoints = road.getLastPoints(150);
			}
		}
		return resToAttachPoints;
	}


	private void processWay(Way way) {
		String ref = way.getTag("ref");
		String hw = way.getTag("highway");
        boolean isLink = hw != null && hw.endsWith("_link");
		if (way.getFirstNodeId() == way.getLastNodeId() || 
				"roundabout".equals(way.getTag("junction")) || isLink) {
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
