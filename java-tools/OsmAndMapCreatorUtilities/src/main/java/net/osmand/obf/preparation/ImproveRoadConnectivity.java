package net.osmand.obf.preparation;

import gnu.trove.iterator.TLongIterator;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.set.hash.TLongHashSet;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;

import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.binary.BinaryMapRouteReaderAdapter.RouteRegion;
import net.osmand.binary.BinaryMapRouteReaderAdapter.RouteSubregion;
import net.osmand.binary.RouteDataObject;
import net.osmand.router.BinaryRoutePlanner.RouteSegment;
import net.osmand.router.RoutePlannerFrontEnd;
import net.osmand.router.RoutePlannerFrontEnd.RouteCalculationMode;
import net.osmand.router.RoutingConfiguration;
import net.osmand.router.RoutingConfiguration.Builder;
import net.osmand.router.RoutingContext;
import net.osmand.router.RoutingContext.RoutingSubregionTile;
import net.osmand.router.VehicleRouter;
import net.osmand.util.MapUtils;

public class ImproveRoadConnectivity {
	public static boolean TRACE = false;

	public static void main(String[] args) throws IOException {

		ImproveRoadConnectivity crc = new ImproveRoadConnectivity();
		File fl = new File("/home/victor/projects/osmand/osm-gen/Brazil_southamerica_2.obf");
		RandomAccessFile raf = new RandomAccessFile(fl, "r"); //$NON-NLS-1$ //$NON-NLS-2$

		crc.collectDisconnectedRoads(new BinaryMapIndexReader(raf, fl));
//		ClusteringContext ctx = new ClusteringContext();
//		crc.clustering(ctx, new BinaryMapIndexReader(raf));
	}

	private static class ClusteringContext {
	}


	public void clustering(final ClusteringContext clusterCtx, BinaryMapIndexReader reader)
			throws IOException {
		RoutePlannerFrontEnd router = new RoutePlannerFrontEnd();
		Builder builder = RoutingConfiguration.getDefault();
		RoutingConfiguration config = builder.build("car", RoutingConfiguration.DEFAULT_MEMORY_LIMIT * 3);
		RoutingContext ctx = router.buildRoutingContext(config, null, new BinaryMapIndexReader[] { reader },
				RouteCalculationMode.BASE);
		if (reader.getRoutingIndexes().size() != 1) {
			throw new UnsupportedOperationException();
		}
		RouteRegion reg = reader.getRoutingIndexes().get(0);
		List<RouteSubregion> baseSubregions = reg.getBaseSubregions();
		List<RoutingSubregionTile> tiles = new ArrayList<RoutingContext.RoutingSubregionTile>();
		for (RouteSubregion s : baseSubregions) {
			List<RoutingSubregionTile> loadTiles = ctx.loadAllSubregionTiles(reader, s);
			tiles.addAll(loadTiles);
		}

		List<Cluster> allClusters = new ArrayList<ImproveRoadConnectivity.Cluster>();
		for(RoutingSubregionTile tile : tiles) {
			List<Cluster> clusters = processDataObjects(ctx, tile);
			allClusters.addAll(clusters);
		}
		combineClusters(allClusters, tiles);
	}

	public TLongObjectHashMap<RouteDataObject> collectDisconnectedRoads(BinaryMapIndexReader reader) throws IOException {
		TLongObjectHashMap<List<RouteDataObject>> all = new TLongObjectHashMap<List<RouteDataObject>>();
		TLongObjectHashMap<List<RouteDataObject>> onlyRoads = new TLongObjectHashMap<List<RouteDataObject>>();
		TLongHashSet registeredRoadIds = new TLongHashSet();
		findAllBaseRoadIntersections(reader, all, onlyRoads, registeredRoadIds);
		return calculateDisconnectedRoadsToAddAndDelete(onlyRoads, all, reader, null, registeredRoadIds);
	}

	private void findAllBaseRoadIntersections(BinaryMapIndexReader reader,
			TLongObjectHashMap<List<RouteDataObject>> all, TLongObjectHashMap<List<RouteDataObject>> onlyRoads,
			TLongHashSet registeredRoadIds)
			throws IOException {
		RoutePlannerFrontEnd router = new RoutePlannerFrontEnd();
		Builder builder = RoutingConfiguration.getDefault();
		RoutingConfiguration config = builder.build("car", RoutingConfiguration.DEFAULT_MEMORY_LIMIT * 3);
		RoutingContext ctx = router.buildRoutingContext(config, null, new BinaryMapIndexReader[] { reader },
				RouteCalculationMode.BASE);
		if (reader.getRoutingIndexes().size() != 1) {
			throw new UnsupportedOperationException();
		}
		RouteRegion reg = reader.getRoutingIndexes().get(0);
		List<RouteSubregion> baseSubregions = reg.getBaseSubregions();
		List<RoutingSubregionTile> tiles = new ArrayList<RoutingContext.RoutingSubregionTile>();
		for (RouteSubregion s : baseSubregions) {
			List<RoutingSubregionTile> loadTiles = ctx.loadAllSubregionTiles(reader, s);
			tiles.addAll(loadTiles);
		}


		for(RoutingSubregionTile tile : tiles) {
			ArrayList<RouteDataObject> dataObjects = new ArrayList<RouteDataObject>();
			ctx.loadSubregionTile(tile, false, dataObjects, null);
			for(RouteDataObject o : dataObjects) {
				registeredRoadIds.add(o.getId());
				int len = o.getPointsLength() - 1;
				double dist = MapUtils.squareRootDist31(o.getPoint31XTile(0), o.getPoint31YTile(0),
						o.getPoint31XTile(len), o.getPoint31YTile(len));
				boolean shortFerry = "ferry".equals(o.getRoute()) && dist < 1000;
				if(shortFerry) {
					continue;
				}
				boolean link = o.getHighway() != null && (o.getHighway().endsWith("link") /*||
						o.getHighway().equals("tertiary")*/);
				long b = calcPointId(o, 0);
				long e = calcPointId(o, len);
				if(!link) {
					addPoint(onlyRoads, o, b);
					addPoint(onlyRoads, o, e);
				}
				for(int i = 0; i < o.getPointsLength(); i++) {
					addPoint(all, o, calcPointId(o, i));
				}
			}
		}
	}

	private void addPoint(TLongObjectHashMap<List<RouteDataObject>> map, RouteDataObject o, long b) {
		if(!map.containsKey(b)) {
			map.put(b, new ArrayList<RouteDataObject>());
		}
		map.get(b).add(o);
	}

	private TLongObjectHashMap<RouteDataObject> calculateDisconnectedRoadsToAddAndDelete(TLongObjectHashMap<List<RouteDataObject>> mapOfObjectToCheck,
			TLongObjectHashMap<List<RouteDataObject>> all, BinaryMapIndexReader reader, TLongHashSet setToRemove, TLongHashSet registeredIds) {
		RoutePlannerFrontEnd frontEnd = new RoutePlannerFrontEnd();
		RoutingConfiguration config = RoutingConfiguration.getDefault().build("car", 1000);

		long[] keys = mapOfObjectToCheck.keys();
		TLongObjectHashMap<RouteDataObject> toAdd = new TLongObjectHashMap<RouteDataObject>();
		TLongHashSet beginIsolated = new TLongHashSet();
		TLongHashSet endIsolated = new TLongHashSet();
		for(int k = 0; k < keys.length; k++) {
			long point = keys[k];
			if(all.get(point).size() == 1) {
				RouteDataObject rdo = all.get(keys[k]).get(0);
				boolean begin = calcPointId(rdo, 0) == point;
				RoutingContext ctx = frontEnd.buildRoutingContext(config, null, new BinaryMapIndexReader[] {reader}, RouteCalculationMode.NORMAL);
				List<RouteDataObject> result = findConnectedRoads(ctx, rdo, begin, all);
				if(result == null) {
					if(begin) {
						beginIsolated.add(rdo.getId());
					} else {
						endIsolated.add(rdo.getId());
					}
				} else {
					for(RouteDataObject obj : result) {
						if(!registeredIds.contains(obj.id)) {
							toAdd.put(obj.id, obj);
						}
					}
				}
			}
		}
		int begSize = beginIsolated.size();
		int endSize = endIsolated.size();
		beginIsolated.retainAll(endIsolated);
		int intersectionSize = beginIsolated.size();
		if(setToRemove != null) {
			setToRemove.addAll(beginIsolated);
		}
		System.out.println("All objects in base file " + mapOfObjectToCheck.size() + " to keep isolated " + (begSize + endSize - 2 * intersectionSize) +
				" to add " + toAdd.size() + " to remove " + beginIsolated.size());
		return toAdd;
	}

	private List<RouteDataObject> findConnectedRoads(RoutingContext ctx, RouteDataObject initial, boolean begin,
			TLongObjectHashMap<List<RouteDataObject>> all) {
		PriorityQueue<RouteSegment> queue = new PriorityQueue<RouteSegment>(10, new Comparator<RouteSegment>() {

			@Override
			public int compare(RouteSegment o1, RouteSegment o2) {
				return Double.compare(o1.getDistanceFromStart(), o2.getDistanceFromStart());
			}
		});
		VehicleRouter router = ctx.getRouter();
		ArrayList<RouteDataObject> next = new ArrayList<RouteDataObject>();
		ctx.loadTileData(initial.getPoint31XTile(0), initial.getPoint31YTile(0), 17, next);
		for(RouteDataObject n : next) {
			if(n.id == initial.id) {
				initial = n;
				break;
			}
		}
		queue.add(new RouteSegment(initial, begin ? 1 : initial.getPointsLength() - 2));
		TLongHashSet visited = new TLongHashSet();
		RouteSegment finalSegment = null;
		while(!queue.isEmpty() && finalSegment == null){
			RouteSegment segment = queue.poll();
			int oneWay = router.isOneWay(segment.getRoad());
			boolean start = initial.id == segment.getRoad().id;
			if(start) {
				oneWay = begin ? -1 : 1;
			}
			if(oneWay >= 0) {
				finalSegment = processSegment(ctx, segment, queue, visited, all, true, start);
			}
			if(oneWay <= 0) {
				finalSegment = processSegment(ctx, segment, queue, visited, all, false, start);
			}
		}
		if(finalSegment == null) {
			if(TRACE) {
				System.out.println("Isolated " + initial.id);
			}
		} else {
			StringBuilder b = new StringBuilder("Route for " + initial.id + " : ");
			RouteSegment s = finalSegment;
			List<RouteDataObject> rdoToAdd = new ArrayList<RouteDataObject>();
			while(s != null) {
				if(s.getRoad().id != initial.id) {
					b.append(s.getRoad().id).append(", ");
					rdoToAdd.add(s.getRoad());
				}
				s = s.getParentRoute();
			}
			if(TRACE) {
				System.out.println(b);
			}
			return rdoToAdd;
		}
		return null;
	}

	private RouteSegment processSegment(RoutingContext ctx, RouteSegment segment, PriorityQueue<RouteSegment> queue, TLongHashSet visited,
			TLongObjectHashMap<List<RouteDataObject>> all, boolean direction, boolean start) {
		int ind = segment.getSegmentStart();
		RouteDataObject road = segment.getRoad();
		final long pid = calcPointIdUnique(segment.getRoad(), ind);
		if(visited.contains(pid)) {
			return null;
		}
		visited.add(pid);
		double distFromStart = segment.getDistanceFromStart();
		while(true) {
			int py = road.getPoint31YTile(ind);
			int px = road.getPoint31XTile(ind);
			if(direction) {
				ind ++;
			} else{
				ind --;
			}
			if(ind < 0 || ind >= segment.getRoad().getPointsLength()) {
				break;
			}
			if(all.contains(calcPointId(segment.getRoad(), ind)) && !start) {
				return segment;
			}
			visited.add(calcPointIdUnique(segment.getRoad(), ind));
			int x = road.getPoint31XTile(ind);
			int y = road.getPoint31YTile(ind);
			float spd = ctx.getRouter().defineRoutingSpeed(road) * ctx.getRouter().defineSpeedPriority(road);
			if(spd > ctx.getRouter().getMaxSpeed()) {
				spd = ctx.getRouter().getMaxSpeed();
			}
			distFromStart += MapUtils.squareDist31TileMetric(px, py, x, y) / spd;
			RouteSegment rs = ctx.loadRouteSegment(x, y, 0);
			while(rs != null) {
				if(!visited.contains(calcPointIdUnique(rs.getRoad(), rs.getSegmentStart()))) {
					if(!queue.contains(rs) || rs.getDistanceFromStart() > distFromStart) {
						rs.setDistanceFromStart((float) distFromStart);
						rs.setParentSegmentEnd(ind);
						rs.setParentRoute(segment);
						queue.remove(rs);
						queue.add(rs);
					}
				}
				rs = rs.getNext();
			}
		}
		return null;
	}

	private void combineClusters(List<Cluster> allClusters, List<RoutingSubregionTile> tiles) {
		boolean change = true;
		int cnt = 0;
		while (change) {
			change = false;
			for(int i = 0; i < allClusters.size(); i++) {
				Cluster c = allClusters.get(i);
				for(int j = i + 1; j < allClusters.size(); ) {
					if((cnt ++) % 1000000 == 0) {
						System.out.println("Cluster to process left  " + allClusters.size() +" ...");
					}
					Cluster toMerge = allClusters.get(j);
					if (c != toMerge && mergeable(c, toMerge)) {
						change = true;
						c.merge(toMerge);
						allClusters.remove(j);
					} else {
						j++;
					}
				}
			}
		}

		System.out.println("Tiles " + tiles.size() + " clusters " +allClusters.size());
		for(Cluster c : allClusters) {
			if(c.roadsIncluded > 100) {
				System.out.println("Main Cluster roads = " + c.roadsIncluded + " id = "+ c.initalRoadId);
			} else {
				System.out.println("Cluster roads = " + c.roadsIncluded + " id = "+ c.initalRoadId + " " + c.highways + " " + c.roadIds);
			}
		}

	}

	private boolean mergeable(Cluster a, Cluster b) {
		boolean order = a.points.size() < b.points.size();
		Cluster as = order ? a : b;
		Cluster bs = order ? b : a;
		TLongIterator iterator = as.points.iterator();
		while(iterator.hasNext()) {
			if(bs.points.contains(iterator.next())) {
				return true;
			}
		}
		return false;
	}

	private class Cluster {
		long initalRoadId;
		int roadsIncluded;
		TLongHashSet points = new TLongHashSet();
		TLongHashSet roadIds = new TLongHashSet();
		Set<String> highways = new HashSet<String>();
		public void merge(Cluster toMerge) {
			roadsIncluded += toMerge.roadsIncluded;
			points.addAll(toMerge.points);
			roadIds.addAll(toMerge.roadIds);
			highways.addAll(toMerge.highways);

		}
	}

	private List<Cluster> processDataObjects(RoutingContext ctx, RoutingSubregionTile tl) {
		ArrayList<RouteDataObject> dataObjects = new ArrayList<RouteDataObject>();
		ctx.loadSubregionTile(tl, false, dataObjects, null);
		TLongObjectHashMap<List<RouteDataObject>> clusterPoints = new TLongObjectHashMap<List<RouteDataObject>>();
		for(RouteDataObject rdo : dataObjects) {
			for(int i = 0 ; i < rdo.getPointsLength(); i++) {
				long pointId = calcPointId(rdo, i);
				List<RouteDataObject> list = clusterPoints.get(pointId);
				if(list == null) {
					list = new LinkedList<RouteDataObject>();
					clusterPoints.put(pointId, list);
				}
				list.add(rdo);
			}
		}

		HashSet<RouteDataObject> toVisit = new HashSet<RouteDataObject>(dataObjects);
		List<Cluster> clusters = new ArrayList<Cluster>();
		while(!toVisit.isEmpty()) {
			RouteDataObject firstObject = toVisit.iterator().next();
			LinkedList<RouteDataObject> queue = new LinkedList<RouteDataObject>();
			queue.add(firstObject);
			Cluster c = new Cluster();
			c.initalRoadId = firstObject.id;
			clusters.add(c);
			while (!queue.isEmpty()) {
				RouteDataObject rdo = queue.poll();
				if(!toVisit.contains(rdo)) {
					continue;
				}
				c.roadsIncluded++;
				c.roadIds.add(rdo.id);
				String hw = rdo.getHighway();
				if(hw == null) {
					hw = rdo.getRoute();
				}
				c.highways.add(hw);
				toVisit.remove(rdo);
				for (int i = 0; i < rdo.getPointsLength(); i++) {
					long pointId = calcPointId(rdo, i);
					c.points.add(pointId);
					List<RouteDataObject> list = clusterPoints.get(pointId);
					for(RouteDataObject r : list) {
						if(r.id != rdo.id && toVisit.contains(r)) {
							queue.add(r);
						}
					}
				}
			}
		}
		return clusters;
	}

	private long calcPointId(RouteDataObject rdo, int i) {
		return ((long)rdo.getPoint31XTile(i) << 31l) + (long)rdo.getPoint31YTile(i);
	}

	private long calcPointIdUnique(RouteDataObject rdo, int i) {
		return ((long)rdo.getId() << 20l) + i;
	}

}

