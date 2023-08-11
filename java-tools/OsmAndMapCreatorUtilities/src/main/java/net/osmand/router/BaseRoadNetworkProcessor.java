package net.osmand.router;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

import javax.xml.stream.XMLStreamException;

import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.set.TLongSet;
import gnu.trove.set.hash.TLongHashSet;
import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.binary.BinaryMapRouteReaderAdapter.RouteTypeRule;
import net.osmand.binary.RouteDataObject;
import net.osmand.data.LatLon;
import net.osmand.osm.edit.Entity;
import net.osmand.osm.edit.Node;
import net.osmand.osm.edit.Way;
import net.osmand.osm.io.OsmBaseStorage;
import net.osmand.osm.io.OsmStorageWriter;
import net.osmand.router.BinaryRoutePlanner.RouteSegment;
import net.osmand.router.BinaryRoutePlanner.RouteSegmentPoint;
import net.osmand.router.RoutePlannerFrontEnd.RouteCalculationMode;
import net.osmand.router.RoutingConfiguration.Builder;
import net.osmand.router.RoutingConfiguration.RoutingMemoryLimits;
import net.osmand.util.MapUtils;

public class BaseRoadNetworkProcessor {

	private static long ID = -1;
	private static final int ROUTE_POINTS = 11;
	protected static LatLon EX1 = new LatLon(52.3201813,4.7644685);
	protected static LatLon EX2 = new LatLon(52.33265, 4.77738);
	protected static LatLon EX4 = new LatLon(52.2728791, 4.8064803);
	protected static LatLon EX3 = new LatLon(52.27757, 4.85731);
	
	
	private static String ROUTING_PROFILE = "car";
	private static int INITIAL_LOOKUP = 7;
	private static int MAX_BLOCKERS = 100;
	
	
	private RoutingContext ctx;
	
	
	class BaseCluster {
		PriorityQueue<RouteSegment> graphSegments = new PriorityQueue<>(new Comparator<RouteSegment>() {

			@Override
			public int compare(RouteSegment o1, RouteSegment o2) {
				return ctx.roadPriorityComparator(o1.distanceFromStart, o1.distanceToEnd, o2.distanceFromStart,
						o2.distanceToEnd);
			}
		});
		TLongObjectHashMap<RouteSegment> visitedSegments = new TLongObjectHashMap<RouteSegment>();
		TLongObjectHashMap<RouteSegment> toVisit = new TLongObjectHashMap<RouteSegment>();
	}
	
	
	public static void main(String[] args) throws IOException, XMLStreamException {
		File fl = new File(System.getProperty("maps.dir") + "/Netherlands_noord-holland_europe_2.road.obf");
		RandomAccessFile raf = new RandomAccessFile(fl, "r"); //$NON-NLS-1$ //$NON-NLS-2$
		List<Entity> objects = new BaseRoadNetworkProcessor().collectDisconnectedRoads(new BinaryMapIndexReader(raf, fl));

		OsmBaseStorage st = new OsmBaseStorage();
		for (Entity e : objects) {
			if (e instanceof Way) {
				for (Node n : ((Way) e).getNodes()) {
					st.registerEntity(n, null);
				}
			}
			st.registerEntity(e, null);
		}
		OsmStorageWriter w = new OsmStorageWriter();
		FileOutputStream fous = new FileOutputStream(new File(System.getProperty("maps.dir") + "/Netherlands_noord-holland.road.osm"));
		w.saveStorage(fous, st, null, true);
		fous.close();
	}

	private List<Entity> collectDisconnectedRoads(BinaryMapIndexReader reader) throws IOException {
		RoutePlannerFrontEnd router = new RoutePlannerFrontEnd();
		Builder builder = RoutingConfiguration.getDefault();
		RoutingMemoryLimits memoryLimit = new RoutingMemoryLimits(RoutingConfiguration.DEFAULT_MEMORY_LIMIT * 3,
				RoutingConfiguration.DEFAULT_NATIVE_MEMORY_LIMIT);
		RoutingConfiguration config = builder.build(ROUTING_PROFILE, memoryLimit);
		ctx = router.buildRoutingContext(config, null, new BinaryMapIndexReader[] { reader },RouteCalculationMode.NORMAL);
//		if (reader.getRoutingIndexes().size() != 1) {
//			throw new UnsupportedOperationException();
//		}
//		RouteRegion reg = reader.getRoutingIndexes().get(0);
//		List<RouteSubregion> baseSubregions = reg.getBaseSubregions();
//		List<RoutingSubregionTile> tiles = new ArrayList<>();
//		for (RouteSubregion s : baseSubregions) {
//			List<RoutingSubregionTile> loadTiles = ctx.loadAllSubregionTiles(reader, s);
//			tiles.addAll(loadTiles);
//		}
		RouteSegmentPoint pnt = router.findRouteSegment(EX3.getLatitude(), EX3.getLongitude(), ctx, null);
		
		BaseCluster cluster = new BaseCluster();
		cluster.graphSegments.add(pnt);
		buildRoadNetworks(cluster, INITIAL_LOOKUP);
		return visualizeWays(cluster.toVisit.valueCollection(), cluster.visitedSegments);

	}

	private void buildRoadNetworks(BaseCluster c, int init) {
		while (c.graphSegments.size() < init && !c.graphSegments.isEmpty()) {
			RouteSegment p = c.graphSegments.poll();
			proceed(c, p);
			System.out.println(c.visitedSegments.size() + " -> " + c.graphSegments.size());
		}
		mergeStraights(c);
		if (init > 2) {
			mergeConnected(c, init);
		}
		mergeStraights(c);
 		System.out.println(c.toVisit.size());
	}

	private void mergeConnected(BaseCluster c, int init) {
		c.graphSegments.clear();
		c.graphSegments.addAll(c.toVisit.valueCollection());
		while (!c.graphSegments.isEmpty()) {
			RouteSegment t = c.graphSegments.poll();
			BaseCluster bc = new BaseCluster();
			bc.graphSegments.add(t);
			buildRoadNetworks(bc, init - 1);
			int cnt = bc.toVisit.size();
			for (long k : bc.toVisit.keys()) {
				if (!c.visitedSegments.containsKey(k)) {
					cnt--;
				}
			}
			if (cnt < 2) {
				// merge
				c.visitedSegments.putAll(bc.visitedSegments);
				for (long l : bc.visitedSegments.keys()) {
					c.toVisit.remove(l);
				}
				for (long k : bc.toVisit.keys()) {
					if (!c.visitedSegments.containsKey(k)) {
						c.toVisit.put(k, bc.toVisit.get(k));
					}
				}
				System.out.println("M " + cnt + " " + c.visitedSegments.size() + " -> " + c.toVisit.size());
			}
		}
	}

	private void mergeStraights(BaseCluster c) {
		boolean foundStraights = true;
		while (foundStraights && !c.toVisit.isEmpty() && c.toVisit.size() < MAX_BLOCKERS) {
			foundStraights = false;
			for(RouteSegment segment : c.toVisit.valueCollection()) {
				if (goodCandidate(c, segment)) {
					foundStraights = true;
					c.toVisit.remove(calculateRoutePointId(segment));
					proceed(c,segment);
					break;
				}
			}
			System.out.println(c.visitedSegments.size() + " -> " + c.toVisit.size());
		}
	}

	private boolean goodCandidate(BaseCluster c, RouteSegment segment) {
		int cnt = countNonVisited(c, segment, segment.getSegmentEnd());
		cnt += countNonVisited(c, segment, segment.getSegmentStart());
		if (cnt < 2) {
			return true;
		}
		
//		// include whole segment
//		RouteSegment opp = segment.initRouteSegment(!segment.isPositive());
//		if (opp != null && visitedSegments.containsKey(calculateRoutePointId(opp)) && 
//				Math.min(opp.getSegmentStart(), opp.getSegmentEnd()) > 0 &&
//				Math.max(opp.getSegmentStart(), opp.getSegmentEnd()) < opp.getRoad().getPointsLength() - 1) {
//			return true;
//		}
		return false;
	}

	private int countNonVisited(BaseCluster c, RouteSegment segment, int ind) {
		int x = segment.getRoad().getPoint31XTile(ind);
		int y = segment.getRoad().getPoint31YTile(ind);
		RouteSegment next = ctx.loadRouteSegment(x, y, 0);
		int cnt = 0;
		while (next != null) {
			if (!c.toVisit.containsKey(calculateRoutePointId(next))  && !c.visitedSegments.containsKey(calculateRoutePointId(next))) {
				cnt++;
			}
			next = next.getNext();
		}
		return cnt;
	}

	private List<Entity> visualizeWays(Collection<RouteSegment> toVisit, TLongObjectHashMap<RouteSegment> visitedSegments) {
		TLongSet viewed = new TLongHashSet();
		List<Entity> objs = new ArrayList<>();
		TLongObjectIterator<RouteSegment> it = visitedSegments.iterator();
		while (it.hasNext()) {
			it.advance();
			long pntKey = it.key();
			if (!viewed.contains(pntKey)) {
				RouteSegment s = it.value();
				int d = (s.getSegmentStart() < s.getSegmentEnd() ? 1 : -1);
				int segmentEnd = s.getSegmentEnd();
				viewed.add(pntKey);
				while ((segmentEnd + d) >= 0 && (segmentEnd + d) < s.getRoad().getPointsLength()) {
					RouteSegment nxt = new RouteSegment(s.getRoad(), segmentEnd, segmentEnd + d);
					pntKey = calculateRoutePointId(nxt);
					if (!visitedSegments.containsKey(pntKey) || viewed.contains(pntKey)) {
						break;
					}
					viewed.add(pntKey);
					segmentEnd +=d;
				}
				Way w = convertRoad(s.getRoad(), s.getSegmentStart(), segmentEnd);
				objs.add(w);
			}
			it.remove();
		}
		for (RouteSegment r : toVisit) {
			int x1 = r.getRoad().getPoint31XTile(r.getSegmentStart());
			int x2 = r.getRoad().getPoint31XTile(r.getSegmentEnd());
			int y1 = r.getRoad().getPoint31YTile(r.getSegmentStart());
			int y2 = r.getRoad().getPoint31YTile(r.getSegmentEnd());
			double lon = MapUtils.get31LongitudeX(x1 / 2 + x2 / 2);
			double lat = MapUtils.get31LatitudeY(y1 / 2 + y2 / 2);
			Node n = new Node(lat, lon, ID--);
			n.putTag("highway", "stop");
			n.putTag("name", r.getRoad().getId() / 64 + " " + r.getSegmentStart() + " " + r.getSegmentEnd());
			objs.add(n);
		}
		return objs;
	}

	private Way convertRoad(RouteDataObject road, int st, int end) {
		Way w = new Way(ID--);
		int[] tps = road.getTypes();
		for (int i = 0; i < tps.length; i++) {
			RouteTypeRule rtr = road.region.quickGetEncodingRule(tps[i]);
			w.putTag(rtr.getTag(), rtr.getValue());
		}
		w.putTag("oid", (road.getId() / 64) + "");
		while (true) {
			double lon = MapUtils.get31LongitudeX(road.getPoint31XTile(st));
			double lat = MapUtils.get31LatitudeY(road.getPoint31YTile(st));
			w.addNode(new Node(lat, lon, ID--));
			if (st == end) {
				break;
			} else if (st < end) {
				st++;
			} else {
				st--;
			}
		}
		return w;
	}

	private void proceed(BaseCluster c, RouteSegment segment) {
		long pntId = calculateRoutePointId(segment);
		c.toVisit.remove(pntId);
		if (c.visitedSegments.containsKey(pntId)) {
			return;
		}
		c.visitedSegments.put(calculateRoutePointId(segment), segment);
		int prevX = segment.getRoad().getPoint31XTile(segment.getSegmentStart());
		int prevY = segment.getRoad().getPoint31YTile(segment.getSegmentStart());
		int x = segment.getRoad().getPoint31XTile(segment.getSegmentEnd());
		int y = segment.getRoad().getPoint31YTile(segment.getSegmentEnd());

		float distFromStart = segment.distanceFromStart + (float) MapUtils.squareRootDist31(x, y, prevX, prevY);
		addSegment(c, distFromStart, segment, true);
		addSegment(c, distFromStart, segment, false);
	}
	
	
	private void addSegment(BaseCluster c, float distFromStart, RouteSegment segment, boolean end) {
		int segmentInd = end ? segment.getSegmentEnd() : segment.getSegmentStart();
		int x = segment.getRoad().getPoint31XTile(segmentInd);
		int y = segment.getRoad().getPoint31YTile(segmentInd);
		RouteSegment next = ctx.loadRouteSegment(x, y, 0);
		while (next != null) {
			// next.distanceFromStart == 0 
			if (!c.toVisit.containsKey(calculateRoutePointId(next)) && !c.visitedSegments.containsKey(calculateRoutePointId(next))) {

				next.distanceFromStart = distFromStart;
				c.graphSegments.add(next);
				c.toVisit.put(calculateRoutePointId(next), next);
			}
			RouteSegment opp = next.initRouteSegment(!next.isPositive());
			if (opp != null && !c.toVisit.containsKey(calculateRoutePointId(opp)) && !c.visitedSegments.containsKey(calculateRoutePointId(opp))) {
				opp.distanceFromStart = distFromStart;
				c.graphSegments.add(opp);
				c.toVisit.put(calculateRoutePointId(opp), opp);
			}
			next = next.getNext();
		}
	}


	private long calculateRoutePointInternalId(final RouteDataObject road, int pntId, int nextPntId) {
		int positive = nextPntId - pntId;
		int pntLen = road.getPointsLength();
		if (pntId < 0 || nextPntId < 0 || pntId >= pntLen || nextPntId >= pntLen || (positive != -1 && positive != 1)) {
			// should be assert
			throw new IllegalStateException("Assert failed");
		}
		return (road.getId() << ROUTE_POINTS) + (pntId << 1) + (positive > 0 ? 1 : 0);
	}

	private long calculateRoutePointId(RouteSegment segm) {
		if (segm.getSegmentStart() < segm.getSegmentEnd()) {
			return calculateRoutePointInternalId(segm.getRoad(), segm.getSegmentStart(), segm.getSegmentEnd());
		} else {
			return calculateRoutePointInternalId(segm.getRoad(), segm.getSegmentEnd(), segm.getSegmentStart());
		}
	}

}
