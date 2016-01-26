package net.osmand.osm.util;

import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TLongObjectHashMap;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import javax.xml.stream.XMLStreamException;

import net.osmand.data.LatLon;
import net.osmand.impl.ConsoleProgressImplementation;
import net.osmand.osm.edit.Entity;
import net.osmand.osm.edit.Entity.EntityId;
import net.osmand.osm.edit.Entity.EntityType;
import net.osmand.osm.edit.Node;
import net.osmand.osm.edit.OsmMapUtils;
import net.osmand.osm.edit.Way;
import net.osmand.osm.io.OsmBaseStorage;
import net.osmand.osm.io.OsmStorageWriter;
import net.osmand.util.MapUtils;

import org.apache.tools.bzip2.CBZip2InputStream;
import org.xmlpull.v1.XmlPullParserException;

public class FixBasemapRoads {
    private static float MINIMAL_DISTANCE= 500;
    private static float MAXIMAL_DISTANCE_CUT = 3000;
	
	public static void main(String[] args) throws Exception {
		String fileToRead = args != null && args.length > 0 ? args[0] : null; 
		if(fileToRead == null) {
			fileToRead = "/Users/victorshcherb/temp/test_fixbasemaproads.osm";
		}
		File read = new File(fileToRead);
		File write ;
		String fileToWrite =  args != null && args.length > 1 ? args[1] : null;
		if(fileToWrite != null){
			write = new File(fileToWrite);
			 
		} else {
			String fileName = read.getName();
			int i = fileName.indexOf('.');
			fileName = fileName.substring(0, i) + "_out.osm";
			write = new File(read.getParentFile(), fileName);
		}
		
		write.createNewFile();

        new FixBasemapRoads().process(read, write);
	}
	
	private void process(File read, File write) throws  IOException, XMLStreamException, XmlPullParserException {
		OsmBaseStorage storage = new OsmBaseStorage();
        InputStream stream = new BufferedInputStream(new FileInputStream(read), 8192 * 4);
        InputStream streamFile = stream;
        long st = System.currentTimeMillis();
        if (read.getName().endsWith(".bz2")) { //$NON-NLS-1$
            if (stream.read() != 'B' || stream.read() != 'Z') {
//				throw new RuntimeException("The source stream must start with the characters BZ if it is to be read as a BZip2 stream."); //$NON-NLS-1$
            } else {
                stream = new CBZip2InputStream(stream);
            }
        }
		storage.parseOSM(stream, new ConsoleProgressImplementation(), streamFile, true);
		
		Map<EntityId, Entity> entities = new HashMap<EntityId, Entity>( storage.getRegisteredEntities());
		for(EntityId e : entities.keySet()){
			Entity es = storage.getRegisteredEntities().get(e);
			if(e.getType() == EntityType.WAY){
				processWay((Way) es);
			}
		}
        List<EntityId> toWrite = new ArrayList<EntityId>();
		processRegion(toWrite);
		OsmStorageWriter writer = new OsmStorageWriter();
		writer.saveStorage(new FileOutputStream(write), storage, toWrite, true);
	}

    public static long convertLatLon(LatLon l) {
        long lx = MapUtils.get31TileNumberY(l.getLatitude());
        lx = (lx << 31) | MapUtils.get31TileNumberX(l.getLongitude());
        return lx;
    }

    static class RoadLine {
        ArrayList<Way> combinedWays = new ArrayList<Way>();
        long beginPoint;
        Node last;
        Node first;
        long endPoint;
        double distance = 0;

        public void updateData() {
            distance = 0;
            first = null;
            last = null;
            for(int j = 0; j < combinedWays.size(); j++) {
                Way w = combinedWays.get(j);
                List<Node> nodes = w.getNodes();
                for(int i = 1; i < nodes.size(); i++) {
                    if(first == null )  {
                        first = nodes.get(i - 1);
                    }
                    last = nodes.get(i);
                    if(nodes.get(i - 1) != null && nodes.get(i) != null) {
                        distance += OsmMapUtils.getDistance(nodes.get(i - 1), nodes.get(i));
                    }
                }
            }
//            beginPoint = combinedWays.get(0).getFirstNodeId();
//            endPoint = combinedWays.get(combinedWays.size() - 1).getLastNodeId();
	        beginPoint =  convertLatLon(combinedWays.get(0).getFirstNode().getLatLon());
	        endPoint =  convertLatLon( combinedWays.get(combinedWays.size() - 1).getLastNode().getLatLon());
        }

        RoadLine(Way w) {
            combinedWays.add(w);
            updateData( );
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


        void insertInBeginning(RoadLine toMerge) {
            combinedWays.addAll(0, toMerge.combinedWays);
            updateData();
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
            updateData();
        }
    }

    class RoadInfo {
	    String ref;
        Collection<RoadLine> roadLines = new LinkedHashSet<RoadLine>();
        TLongObjectHashMap<List<RoadLine>> startPoints = new TLongObjectHashMap<List<RoadLine>>();
        TLongObjectHashMap<List<RoadLine>> endPoints = new TLongObjectHashMap<List<RoadLine>>();

        void registerRoadLine(RoadLine r){
            roadLines.add(r);
            registerPoints(r);

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
            deleteRoadLine(toMerge);
            deleteRoadLine(toKeep);
            toKeep.insertInBeginning(toMerge);
            registerRoadLine(toKeep);
        }

        private void deleteRoadLine(RoadLine roadLine) {
            roadLines.remove(roadLine);
            deletePoints(roadLine);
        }

        private void deletePoints(RoadLine roadLine) {
            startPoints.get(roadLine.beginPoint).remove(roadLine);
            endPoints.get(roadLine.endPoint).remove(roadLine);
        }

        public List<RoadLine> getConnectedLinesStart(long point) {
            TLongArrayList rs = roundabouts.get(point);
            if(rs == null) {
                return startPoints.get(point);
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
                return endPoints.get(point);
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
            deletePoints(roadLine);
            roadLine.reverse();
            registerPoints(roadLine);
        }
    }


    private Map<String, RoadInfo> roadInfoMap = new LinkedHashMap<String, RoadInfo>();
    private TLongObjectHashMap<TLongArrayList> roundabouts = new TLongObjectHashMap<TLongArrayList>();



    // TODO try reverse?
    private void processRegion(List<EntityId> toWrite) {
        for(String ref : roadInfoMap.keySet()){
            RoadInfo ri = roadInfoMap.get(ref);
            // combine unique roads
            combineUniqueIdentifyRoads(ri);
            reverseWrongPositionedRoads(ri);
            combineUniqueIdentifyRoads(ri);
            // last step not definite
            combineIntoLongestRoad(ri);
            combineRoadsWithCut(ri);
            for(RoadLine ls :  ri.roadLines) {
                if(ls.distance > MINIMAL_DISTANCE ){
                    ls.combineWaysIntoOneWay();
                    toWrite.add(ls.getFirstWayId());
                }
            }
        }
    }

    private void reverseWrongPositionedRoads(RoadInfo ri) {
        for (RoadLine roadLine : ri.roadLines) {
            List<RoadLine> ps = ri.getConnectedLinesEnd(roadLine.beginPoint);
            List<RoadLine> ws = ri.getConnectedLinesStart(roadLine.endPoint);
            if((ps == null || ps.size() == 0)  && (ws == null || ws.size() == 0) ) {
                ri.reverseRoad(roadLine);
            }
        }
    }

    private boolean inOppositeDirection(RoadLine end, RoadLine begin) {
        double last = directionRoute(end.getLastPoints(150), false);
        double first = directionRoute(begin.getFirstPoints(150), true);
        double diff = MapUtils.alignAngleDifference(first - last);
	    return Math.abs(diff) < 3 * Math.PI / 8;
    }

    private boolean continuation(RoadLine end, RoadLine begin) {
        List<Node> lv = end.getLastPoints(150);
        double last = directionRoute(lv, false);
        List<Node> ltv = new ArrayList<Node>();
        ltv.add(end.last);
        ltv.add(begin.first);
        double cut = directionRoute(ltv, true);
        double first = directionRoute(begin.getFirstPoints(150), true);
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
        while(merged) {
            merged = false;
            for (RoadLine roadLine : new ArrayList<RoadLine>(ri.roadLines)) {
                if (!ri.roadLines.contains(roadLine)) {
                    continue;
                }

                List<RoadLine> list = ri.getConnectedLinesStart(roadLine.endPoint);
                double maxDist = 0;
                if (list != null && list.size() > 0) {
                    RoadLine longest = null;
                    for (RoadLine rs : list) {
                        if (inOppositeDirection(roadLine, rs)) {
                            continue;
                        }
                        boolean maxRoute = true;
                        for (RoadLine rt : ri.getConnectedLinesEnd(roadLine.endPoint)) {
                            if (rt.distance > roadLine.distance && !inOppositeDirection(roadLine, rt)) {
                                maxRoute = false;
                                break;
                            }

                        }
                        if (!maxRoute) {
                            continue;
                        }
                        if (rs.distance > maxDist) {
                            maxDist = rs.distance;
                            longest = rs;
                        }
                    }
                    if (longest != roadLine && longest != null) {
                        merged = true;
                        ri.mergeRoadInto(roadLine, longest);
                    }
                }
            }
        }
    }

    private void combineUniqueIdentifyRoads(RoadInfo ri) {
        boolean merged = true;
        while(merged) {
            merged = false;
            for (RoadLine ls : new ArrayList<RoadLine>(ri.roadLines)) {
                if(!ri.roadLines.contains(ls)){
                    continue;
                }
                List<RoadLine> endedInSamePoint = ri.getConnectedLinesEnd(ls.endPoint);
                List<RoadLine> list = ri.getConnectedLinesStart(ls.endPoint);
                if (list != null && list.size() == 1 && endedInSamePoint.size() == 1) {
                    if (list.get(0) != ls && !inOppositeDirection(ls, list.get(0))) {
                        merged = true;
                        ri.mergeRoadInto(ls, list.get(0));
                    }
                }
            }
        }
    }

    private void combineRoadsWithCut(RoadInfo ri) {
        for (RoadLine roadLine : new ArrayList<RoadLine>(ri.roadLines)) {
            if (!ri.roadLines.contains(roadLine)) {
                continue;
            }
            boolean maxRoute = true;
            for (RoadLine rt : ri.getConnectedLinesEnd(roadLine.endPoint)) {
                if (rt.distance > roadLine.distance && !inOppositeDirection(roadLine, rt)) {
                    maxRoute = false;
                    break;
                }

            }
            if (maxRoute && roadLine.last != null && roadLine.distance > MAXIMAL_DISTANCE_CUT / 2) {
                List<Node> ps = roadLine.getLastPoints(50);
                Node last = ps.get(ps.size() - 1);
                for (RoadLine rl : ri.roadLines) {
                    if (roadLine != rl && rl.distance > MAXIMAL_DISTANCE_CUT / 2
                            &&
                            OsmMapUtils.getDistance(roadLine.last, rl.first) < MAXIMAL_DISTANCE_CUT &&
                            continuation(roadLine, rl)) {
                        roadLine.getLastWay().addNode(rl.first);
                        ri.mergeRoadInto(roadLine, rl);
                        break;
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
			ref = way.getTag("name");
		} else {
			// fix road inconsistency
			ref = ref.replace('-', ' ');
			if (ref.indexOf(';') != -1) {
				ref = ref.substring(0, ref.indexOf(';'));
			}
		}
		if (ref == null || ref.isEmpty()) {
			LatLon lt = way.getLatLon();
			ref = ((int) MapUtils.getTileNumberY(4, lt.getLatitude())) + " "
					+ ((int) MapUtils.getTileNumberX(4, lt.getLongitude()));
		}

		if (!roadInfoMap.containsKey(ref)) {
			roadInfoMap.put(ref, new RoadInfo());
		}
		RoadInfo ri = roadInfoMap.get(ref);
		ri.registerRoadLine(new RoadLine(way));
	}

}
