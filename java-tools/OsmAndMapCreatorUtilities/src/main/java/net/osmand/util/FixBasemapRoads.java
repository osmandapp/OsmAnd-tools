package net.osmand.util;

import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TLongObjectHashMap;

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
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.zip.GZIPInputStream;

import javax.xml.stream.XMLStreamException;

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

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.logging.Log;
import org.xmlpull.v1.XmlPullParserException;

public class FixBasemapRoads {
    private static float MINIMAL_DISTANCE = 100; // -> 1500? primary
    private static float MAXIMAL_DISTANCE_CUT = 400;
    private final static Log LOG = PlatformUtil.getLog(FixBasemapRoads.class);
    
    private static boolean FILTER_BBOX = true;  
	private static double LEFT_LON = -3;
	private static double RIGHT_LON = -1;
	private static double TOP_LAT = 53;
	private static double BOTTOM_LAT = 50;

	public static void main(String[] args) throws Exception {
		if(args == null || args.length == 0) {
			String line = "motorway";
			line = "trunk";
//			line = "primary";
//			line = "secondary";
//			line = "tertiary";
//			line = "nlprimary";
//			line = "nlsecondary";
			args = new String[] {
					System.getProperty("maps.dir") + "basemap/line_" + line + ".osm.gz",
					System.getProperty("maps.dir") + "basemap/proc/proc_line_" + line + ".osm",
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
        long lx = MapUtils.get31TileNumberY(l.getLatitude());
        lx = (lx << 31) | MapUtils.get31TileNumberX(l.getLongitude());
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

    class RoadInfo {
	    String ref;
        List<RoadLine> roadLines = new ArrayList<RoadLine>();
        TLongObjectHashMap<List<RoadLine>> startPoints = new TLongObjectHashMap<List<RoadLine>>();
        TLongObjectHashMap<List<RoadLine>> endPoints = new TLongObjectHashMap<List<RoadLine>>();
		boolean adminLevel;
		
		public RoadInfo(String rf) {
			this.ref = rf;
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

        public void mergeRoadInto(RoadLine toMerge, RoadLine toKeep) {
        	long op = toKeep.beginPoint;
            toKeep.insertInBeginning(toMerge);
            startPoints.get(op).remove(toKeep);
            startPoints.get(toKeep.beginPoint).add(toKeep);
            
            startPoints.get(toMerge.beginPoint).remove(toMerge);
            endPoints.get(toMerge.endPoint).remove(toMerge);
            toMerge.delete();
        }

        public List<RoadLine> getConnectedLinesStart(long point) {
            TLongArrayList rs = roundabouts.get(point);
            if(rs == null) {
                List<RoadLine> res = startPoints.get(point);
                if(res != null) {
                	return res;
                }
                return Collections.emptyList();
            }
            List<RoadLine> newL = new ArrayList<RoadLine>();
            for(int i = 0; i < rs.size(); i++){
                List<RoadLine> lt = startPoints.get(rs.get(i));
                if(lt != null){
                    newL.addAll(lt);
                }
            }
            return newL;
        }

        public List<RoadLine> getConnectedLinesEnd(long point) {
            TLongArrayList rs = roundabouts.get(point);
            if(rs == null) {
            	List<RoadLine> res = endPoints.get(point);
                if(res != null) {
                	return res;
                }
                return Collections.emptyList();
            }
            List<RoadLine> newL = new ArrayList<RoadLine>();
            for(int i = 0; i < rs.size(); i++){
                List<RoadLine> lt = endPoints.get(rs.get(i));
                if(lt != null){
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

		public boolean isMaxRouteInTheEnd(RoadLine roadLine) {
            boolean maxRoute = true;
            for (RoadLine rt : getConnectedLinesEnd(roadLine.endPoint)) {
                if (!rt.isDeleted() && rt != roadLine && isRoute1HigherPriority(rt, roadLine)
                		&& !inOppositeDirection(roadLine, rt)) {
                    maxRoute = false;
                    break;
                }

            }
			return maxRoute;
		}

		public String toString(String msg) {
			return String.format("Road %s - %s %d - segments, %.2f dist", 
					ref, msg, countRoadLines(), getTotalDistance());
		}

		
    }


    private Map<String, RoadInfo> roadInfoMap = new LinkedHashMap<String, RoadInfo>();
    private TLongObjectHashMap<TLongArrayList> roundabouts = new TLongObjectHashMap<TLongArrayList>();



	private void processRegion(List<EntityId> toWrite) throws IOException {
		for (String ref : roadInfoMap.keySet()) {
			RoadInfo ri = roadInfoMap.get(ref);
			boolean verbose = ri.getTotalDistance() > 40000;
			if(verbose) System.out.println(ri.toString("Initial:"));
			// combine unique roads
			combineUniqueIdentifyRoads(ri);
			if(verbose) System.out.println(ri.toString("After combine unique: "));
			reverseWrongPositionedRoads(ri);
			combineUniqueIdentifyRoads(ri);
			
			if(verbose) System.out.println(ri.toString("After combine reverse unique: "));
			// last step not definite
			combineIntoLongestRoad(ri);
			if(verbose) System.out.println(ri.toString("After combine longest: "));
			combineRoadsWithCut(ri);
			
//			ri.compactDeleted();
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

    private void reverseWrongPositionedRoads(RoadInfo ri) {
        for (RoadLine roadLine : ri.roadLines) {
        	if(roadLine.isDeleted()) {
        		continue;
        	}
            List<RoadLine> ps = ri.getConnectedLinesEnd(roadLine.beginPoint);
            List<RoadLine> ws = ri.getConnectedLinesStart(roadLine.endPoint);
			if (ps.size() == 0 && ws.size() == 0) {
                ri.reverseRoad(roadLine);
            }
        }
    }

    private boolean inOppositeDirection(RoadLine end, RoadLine begin) {
        double last = directionRoute(end.getLastPoints(150), false);
        double first = directionRoute(begin.getFirstPoints(150), true);
        double diff = MapUtils.alignAngleDifference(first - last);
	    return Math.abs(diff) < Math.PI / 4;
    }

    private boolean continuation(RoadLine end, RoadLine begin, boolean reverse) {
        List<Node> lv = end.getLastPoints(150);
        double last = directionRoute(lv, false);
        List<Node> ltv = new ArrayList<Node>();
        ltv.add(end.last);
        if(reverse) {
        	ltv.add(begin.last);
        } else {
        	ltv.add(begin.first);
        }
        double cut = directionRoute(ltv, true);
        
        double first;
        if(reverse){
        	List<Node> rv = begin.getLastPoints(150);
        	Collections.reverse(rv);
        	first = directionRoute(rv, true);
        } else {
        	first = directionRoute(begin.getFirstPoints(150), true);
        }
        double diff = MapUtils.alignAngleDifference(first - last - Math.PI);
        double diff2 = MapUtils.alignAngleDifference(cut - last - Math.PI);
        return Math.abs(diff) < Math.PI / 4 && Math.abs(diff2) < Math.PI / 4;
    }

    private double directionRoute(List<Node> ns, boolean dir) {
        double x = MapUtils.get31TileNumberX(ns.get(0).getLongitude());
        double y = MapUtils.get31TileNumberY(ns.get(0).getLatitude());
        double px = MapUtils.get31TileNumberX(ns.get(ns.size() - 1).getLongitude());
        double py = MapUtils.get31TileNumberY(ns.get(ns.size() - 1).getLatitude());
        if(x == px && y == py) {
            return 0;
        }
        if(dir) {
            return -Math.atan2( x - px, y - py );
        } else {
            return -Math.atan2( -x + px, - y + py );
        }
    }

    private void combineIntoLongestRoad(RoadInfo ri) {
        boolean merged = true;
		while (merged) {
			merged = false;
			for (RoadLine start : ri.roadLines) {
				if (start.isDeleted()) {
					continue;
				}
				List<RoadLine> list = ri.getConnectedLinesStart(start.endPoint);
				double maxDist = 0;
				RoadLine longest = null;
				for (RoadLine end : list) {
					if(end.isDeleted() || start == end || inOppositeDirection(start, end)) {
						continue;
					}
		            if(!ri.isMaxRouteInTheEnd(start)) {
		            	continue;
		            }
					if (end.distance > maxDist) {
						maxDist = end.distance;
						longest = end;
					}
				}
				if (longest != null) {
					merged = true;
					ri.mergeRoadInto(start, longest);
				}
			}
		}
    }

    private void combineUniqueIdentifyRoads(RoadInfo ri) {
        boolean merged = true;
		while (merged) {
			merged = false;
			for (RoadLine start : ri.roadLines) {
				if (start.isDeleted()) {
					// line already merged and deleted
					continue;
				}
				List<RoadLine> list = ri.getConnectedLinesStart(start.endPoint);
				if(ri.isMaxRouteInTheEnd(start)) {
					RoadLine uniqueToCombine = null; 
					for(RoadLine end : list) {
						if(end.isDeleted() || end == start) {
							continue;
						}
						if(inOppositeDirection(start, end)) {
							continue;
						}
						if(uniqueToCombine == null) {
							uniqueToCombine = end;
						} else {
							// not unique!
							uniqueToCombine = null;
							break;
						}
					}
					if(uniqueToCombine != null) {
						merged = true;
						ri.mergeRoadInto(start, uniqueToCombine);
					}
				}
			}
		}
    }

    private void combineRoadsWithCut(RoadInfo ri) {
        for (RoadLine roadLine : ri.roadLines) {
            if (roadLine.isDeleted()) {
                continue;
            }
            if(!ri.isMaxRouteInTheEnd(roadLine)) {
            	continue;
            }
            if (roadLine.distance > MAXIMAL_DISTANCE_CUT / 2) {
                for (RoadLine rl : ri.roadLines) {
                    if (!rl.isDeleted() && roadLine != rl 
                    		&& rl.distance > MAXIMAL_DISTANCE_CUT / 2){
                         if(OsmMapUtils.getDistance(roadLine.last, rl.first) < MAXIMAL_DISTANCE_CUT 
								&& continuation(roadLine, rl, false)) {
							roadLine.getLastWay().addNode(rl.first);
							ri.mergeRoadInto(roadLine, rl);
							break;
						} else if(OsmMapUtils.getDistance(roadLine.last, rl.last) < MAXIMAL_DISTANCE_CUT 
								&& continuation(roadLine, rl, true)) {
							ri.reverseRoad(rl);
							roadLine.getLastWay().addNode(rl.first);
							ri.mergeRoadInto(roadLine, rl);
							break;
						}
                    }
                }

            }
        }
    }


	private void processWay(Way way) {
		String ref = way.getTag("ref");
		if (way.getFirstNodeId() == way.getLastNodeId()) {
			TLongArrayList connectionIds = new TLongArrayList(way.getNodeIds());
			for (int i = 0; i < connectionIds.size(); i++) {
				roundabouts.put(connectionIds.get(i), connectionIds);
			}
			return;
		}
		
		if (ref == null || ref.isEmpty()) {
			ref = way.getTag("int_ref");
		}
		if (ref == null || ref.isEmpty()) {
			// too many breaks in between names
//			ref = way.getTag("name");
		} else {
			// fix road inconsistency
			ref = ref.replace('-', ' ');
			if (ref.indexOf(';') != -1) {
				ref = ref.substring(0, ref.indexOf(';'));
			}
		}
		boolean adminLevel = false;
		if((ref == null || ref.isEmpty()) && way.getTag("admin_level") != null) {
			ref = way.getTag("admin_level");
			LatLon lt = way.getLatLon();
			ref += ((int) MapUtils.getTileNumberY(4, lt.getLatitude())) + " "
					+ ((int) MapUtils.getTileNumberX(4, lt.getLongitude()));
			adminLevel = true;
		}
		if (ref == null || ref.isEmpty()) {
			LatLon lt = way.getLatLon();
			ref = ((int) MapUtils.getTileNumberY(6, lt.getLatitude())) + " "
					+ ((int) MapUtils.getTileNumberX(6, lt.getLongitude()));
		}

		if (!roadInfoMap.containsKey(ref)) {
			roadInfoMap.put(ref, new RoadInfo(ref));
		}
		RoadInfo ri = roadInfoMap.get(ref);
		ri.adminLevel = adminLevel;
		ri.registerRoadLine(new RoadLine(way));
	}

}
