package net.osmand.router;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Comparator;
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
import net.osmand.osm.edit.Node;
import net.osmand.osm.edit.Way;
import net.osmand.osm.io.OsmBaseStorage;
import net.osmand.osm.io.OsmStorageWriter;
import net.osmand.router.BinaryRoutePlanner.RouteSegment;
import net.osmand.router.BinaryRoutePlanner.RouteSegmentPoint;
import net.osmand.router.RoutePlannerFrontEnd;
import net.osmand.router.RoutePlannerFrontEnd.RouteCalculationMode;
import net.osmand.router.RoutingConfiguration;
import net.osmand.router.RoutingConfiguration.Builder;
import net.osmand.router.RoutingConfiguration.RoutingMemoryLimits;
import net.osmand.router.RoutingContext;
import net.osmand.util.MapUtils;

public class BaseRoadNetworkProcessor {

	private static long ID = -1;
	private static final int ROUTE_POINTS = 11;
	
	public static void main(String[] args) throws IOException, XMLStreamException {
		File fl = new File(System.getProperty("maps.dir") + "/Netherlands_noord-holland_europe_2.road.obf");
		RandomAccessFile raf = new RandomAccessFile(fl, "r"); //$NON-NLS-1$ //$NON-NLS-2$
		List<Way> ways = new BaseRoadNetworkProcessor().collectDisconnectedRoads(new BinaryMapIndexReader(raf, fl));

		OsmBaseStorage st = new OsmBaseStorage();
		for (Way w : ways) {
			for (Node n : w.getNodes()) {
				st.registerEntity(n, null);
			}
			st.registerEntity(w, null);
		}

		OsmStorageWriter w = new OsmStorageWriter();
		FileOutputStream fous = new FileOutputStream(
				new File(System.getProperty("maps.dir") + "/Netherlands_noord-holland.road.osm"));
		w.saveStorage(fous, st, null, true);
		fous.close();
	}

	private List<Way> collectDisconnectedRoads(BinaryMapIndexReader reader) throws IOException {
		RoutePlannerFrontEnd router = new RoutePlannerFrontEnd();
		Builder builder = RoutingConfiguration.getDefault();
		RoutingMemoryLimits memoryLimit = new RoutingMemoryLimits(RoutingConfiguration.DEFAULT_MEMORY_LIMIT * 3,
				RoutingConfiguration.DEFAULT_NATIVE_MEMORY_LIMIT);
		RoutingConfiguration config = builder.build("car", memoryLimit);
		RoutingContext ctx = router.buildRoutingContext(config, null, new BinaryMapIndexReader[] { reader },
				RouteCalculationMode.NORMAL);
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
		RouteSegmentPoint pnt = router.findRouteSegment(52.3201813, 4.7644685, ctx, null);

		return buildRoadNetworks(pnt, ctx);

	}

	private List<Way> buildRoadNetworks(RouteSegmentPoint pnt, RoutingContext ctx) {
		List<Way> ways = new ArrayList<>();
		PriorityQueue<RouteSegment> graphSegments = new PriorityQueue<>(new Comparator<RouteSegment>() {

			@Override
			public int compare(RouteSegment o1, RouteSegment o2) {
				return ctx.roadPriorityComparator(o1.distanceFromStart, o1.distanceToEnd, o2.distanceFromStart, o2.distanceToEnd);
			}
		});
		graphSegments.add(pnt);
		TLongObjectHashMap<RouteSegment> visitedSegments = new TLongObjectHashMap<RouteSegment>();
		while (visitedSegments.size() < 500 && !graphSegments.isEmpty()) {
			proceed(ctx, graphSegments, visitedSegments);
			System.out.println(graphSegments.size() + " " + visitedSegments.size());
		}
		System.out.println(graphSegments.size());
		
		TLongSet viewed = new TLongHashSet();
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
				convertRoad(s.getRoad(), s.getSegmentStart(), segmentEnd, ways);
			}
			it.remove();
		}
		return ways;
	}

	private void convertRoad(RouteDataObject road, int st, int end, List<Way> ways) {
		Way w = new Way(ID--);
		ways.add(w);
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
	}

	private void proceed(RoutingContext ctx, PriorityQueue<RouteSegment> graphSegments, TLongObjectHashMap<RouteSegment> visitedSegments) {
		RouteSegment segment = graphSegments.poll();
		long pntId = calculateRoutePointId(segment);
		if (visitedSegments.containsKey(pntId)) {
			return;
		}
		boolean directionAllowed = true;
		boolean direction = segment.isPositive();
		visitedSegments.put(calculateRoutePointId(segment), segment);
		int segmentEnd = segment.getSegmentStart();
		int prevX = segment.getRoad().getPoint31XTile(segmentEnd);
		int prevY = segment.getRoad().getPoint31YTile(segmentEnd);
		float distOnRoadToPass = segment.distanceFromStart;
		while (directionAllowed) {
			segmentEnd += (direction ? 1 : -1);
			if (segmentEnd < 0 || segmentEnd >= segment.getRoad().getPointsLength()) {
				directionAllowed = false;
				continue;
			}

			int x = segment.getRoad().getPoint31XTile(segmentEnd);
			int y = segment.getRoad().getPoint31YTile(segmentEnd);
			
			distOnRoadToPass += (float) MapUtils.squareRootDist31(x, y, prevX, prevY);
			RouteSegment next = ctx.loadRouteSegment(x, y, 0);
			while (next != null) {
				if (next.distanceFromStart == 0) {
					next.distanceFromStart = distOnRoadToPass;
					graphSegments.add(next);
				}
				next = next.getNext();
			}
			prevX = x;
			prevY = y;
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
		return calculateRoutePointInternalId(segm.getRoad(), segm.getSegmentStart(), 
				segm.isPositive() ? segm.getSegmentStart() + 1 : segm.getSegmentStart() - 1);
	}

}
