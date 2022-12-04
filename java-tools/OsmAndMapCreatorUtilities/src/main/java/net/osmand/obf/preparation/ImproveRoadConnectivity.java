package net.osmand.obf.preparation;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import org.apache.commons.logging.Log;

import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.set.hash.TLongHashSet;
import net.osmand.PlatformUtil;
import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.binary.BinaryMapRouteReaderAdapter.RouteRegion;
import net.osmand.binary.BinaryMapRouteReaderAdapter.RouteSubregion;
import net.osmand.binary.RouteDataObject;
import net.osmand.impl.ConsoleProgressImplementation;
import net.osmand.obf.BinaryInspector;
import net.osmand.router.BinaryRoutePlanner.RouteSegment;
import net.osmand.router.RoutePlannerFrontEnd;
import net.osmand.router.RoutePlannerFrontEnd.RouteCalculationMode;
import net.osmand.router.RoutingConfiguration;
import net.osmand.router.RoutingConfiguration.Builder;
import net.osmand.router.RoutingConfiguration.RoutingMemoryLimits;
import net.osmand.router.RoutingContext;
import net.osmand.router.RoutingContext.RoutingSubregionTile;
import net.osmand.router.VehicleRouter;
import net.osmand.util.MapUtils;


// This map generation step adds roads to the Base routing to improve roads connectivity

public class ImproveRoadConnectivity {

	private final Log log = PlatformUtil.getLog(ImproveRoadConnectivity.class);
	
	
	private static final String ROUTING_PROFILE = "car";
	private static final int DEFAULT_MEMORY_LIMIT = 4000;
	
	private static final boolean TRACE = false;
	private static final boolean TRACE_IMPROVE = true;
	
	/**
	 * TODO
	 * Algorithms are not allowed to delete roads from the graph: though it will be correct and once implemented this ref could be used.
	 * Deleting couldbe implemented as adding duplicate object using no:access tag/value ?
	 */
	private static final boolean ROAD_SHOULD_BE_DELETED = false;
	
	
	/**
	 * Algorithm 0: finds simple connection between isolated points (road cuts) and adds missing road using normal routing
	 */
	private static final int ALG_0_NAME__CONNECT_ENDPOINTS_ISOLATED_ROADS = 0;
	private static final double ALG_0_SHORT_FERRY_DISTANCE = 1000;
	
	/**
	 * Algorithm 1
	 * - Searches dangling points with distance <{@value ALG_1_DISTANCE_TO_SEARCH_ROUTE_FROM_DANGLING_POINT}.
	 * - Loads / groups tiles {@value ALG_1_ZOOM_SEARCH_ROUTES_DANGLING_ROADS} so it should be low number to cover distance,
	 * - then build all routes from a point with radius {@value ALG_1_ROUTING_RAIDUS_FROM_DANGLING_POINT} MultiDijkstra
	 * - if points are not reached then normal routing is built and necessary missing roads are added
	 */
	private static final int ALG_1_NAME__CONNECT_ISOLATED_POINTS_MULTIDIJKSTRA = 1;
	private static final int ALG_1_ZOOM_SEARCH_ROUTES_DANGLING_ROADS = 13;
	private static final double ALG_1_DISTANCE_TO_SEARCH_ROUTE_FROM_DANGLING_POINT = 500;
	private static final double ALG_1_ROUTING_RADIUS_FROM_DANGLING_POINT = 25000;
	
	/**
	 * Algorithm 2: guarantees to find disconnected (doesn't check oneway roads) within 
	 * {@value #ALG_2_ZOOM_SCAN_DANGLING_ROADS} 5km (13th zoom) tile that couldn't be connected using extra
	 * {@value #ALG_2_SEARCH_ROUTES_DANGLING_ROADS} 60 km radius of roads (3 * 2 tiles)
	 */
	private static final int ALG_2_NAME__FIND_DISCONNECTED_ROADS = 2;
	private static final int ALG_2_ZOOM_SCAN_DANGLING_ROADS = 13;
	private static final int ALG_2_SEARCH_ROUTES_DANGLING_ROADS = 3; // should be at least >= 2
	private static final int ALG_2_IGNORE_SHORT_FULL_ISOLATED_ROADS = 1500; // shouldn't exceed 10% of the tile

	private static int IMPROVE_ROUTING_ALGORITHM_VERSION = ALG_2_NAME__FIND_DISCONNECTED_ROADS;

	public static void main(String[] args) throws IOException {
		ConsoleProgressImplementation.deltaTimeToPrintMax = 2000;
		ImproveRoadConnectivity crc = new ImproveRoadConnectivity();
		File fl = new File(System.getProperty("maps.dir") + "China_henan_asia.obf");
//		File fl = new File(System.getProperty("maps.dir") + "Denmark_central-region_europe.obf");
		
		RandomAccessFile raf = new RandomAccessFile(fl, "r"); //$NON-NLS-1$ //$NON-NLS-2$
		TLongObjectHashMap<RouteDataObject> map = crc.findJointsForDisconnectedRoads(new BinaryMapIndexReader(raf, fl));
		createOSMFile(System.getProperty("maps.dir") + "/test_" + IMPROVE_ROUTING_ALGORITHM_VERSION + ".osm",
				map.valueCollection());
		
		System.out.println("Found roads: " + map.size());
	}

	private static void createOSMFile(String fous, Collection<RouteDataObject> values) throws IOException {
		StringBuilder b = new StringBuilder();
		FileOutputStream osmOut = new FileOutputStream(fous);
		osmOut.write(("<?xml version='1.0' encoding='UTF-8'?>\n" + "\n<osm version='0.6'>\n").getBytes());
		for (RouteDataObject obj : values) {
			b.setLength(0);
			BinaryInspector.printOsmRouteDetails(obj, b);
			osmOut.write(b.toString().getBytes());
		}
		osmOut.write("\n</osm>".getBytes());
		osmOut.close();
	}

	public TLongObjectHashMap<RouteDataObject> findJointsForDisconnectedRoads(BinaryMapIndexReader reader) throws IOException {
		RoutePlannerFrontEnd router = new RoutePlannerFrontEnd();
		Builder builder = RoutingConfiguration.getDefault();
		RoutingMemoryLimits memoryLimit = new RoutingMemoryLimits(DEFAULT_MEMORY_LIMIT, RoutingConfiguration.DEFAULT_NATIVE_MEMORY_LIMIT);
		RoutingConfiguration config = builder.build(ROUTING_PROFILE, memoryLimit, new LinkedHashMap<String, String>());
		RoutingContext baseCtx = router.buildRoutingContext(config, null, new BinaryMapIndexReader[] { reader }, RouteCalculationMode.BASE);
		RoutingContext normalCtx = router.buildRoutingContext(config, null, new BinaryMapIndexReader[] { reader }, RouteCalculationMode.NORMAL);

		TLongObjectHashMap<List<RouteDataObject>> all = new TLongObjectHashMap<>();
		TLongObjectHashMap<List<RouteDataObject>> onlyRoads = new TLongObjectHashMap<>();
		TLongHashSet registeredRoadIds = new TLongHashSet();
		findAllBaseRoadIntersections(config, baseCtx, all, onlyRoads, registeredRoadIds);
		if (IMPROVE_ROUTING_ALGORITHM_VERSION == ALG_1_NAME__CONNECT_ISOLATED_POINTS_MULTIDIJKSTRA || IMPROVE_ROUTING_ALGORITHM_VERSION == ALG_0_NAME__CONNECT_ENDPOINTS_ISOLATED_ROADS) {
			TLongObjectHashMap<RouteDataObject> pointsToCheck = getIsolatedPoints(onlyRoads, all);
			return connectMissingBaseRoads(pointsToCheck, onlyRoads, all, baseCtx, normalCtx, registeredRoadIds);
		} else if (IMPROVE_ROUTING_ALGORITHM_VERSION == ALG_2_NAME__FIND_DISCONNECTED_ROADS) {
			List<RouteDataObject> result = makeClustersAndFindIsolatedRoads(baseCtx, normalCtx, all);
			TLongObjectHashMap<RouteDataObject> toAdd = new TLongObjectHashMap<>();
			for (RouteDataObject obj : result) {
//				if (!registeredIds.contains(obj.id)) {
					toAdd.put(obj.id, obj);
//				}
			}
			return toAdd;
		}
		return new TLongObjectHashMap<>();
		
	}

	private void findAllBaseRoadIntersections(RoutingConfiguration config, RoutingContext baseCtx,
			TLongObjectHashMap<List<RouteDataObject>> all, TLongObjectHashMap<List<RouteDataObject>> onlyRoads,
			TLongHashSet registeredRoadIds)
			throws IOException {
		if (baseCtx.getMaps().length != 1) {
			throw new UnsupportedOperationException();
		}
		BinaryMapIndexReader reader = baseCtx.getMaps()[0];
		RouteRegion reg = reader.getRoutingIndexes().get(0);
		List<RouteSubregion> baseSubregions = reg.getBaseSubregions();
		List<RoutingSubregionTile> tiles = new ArrayList<>();
		for (RouteSubregion s : baseSubregions) {
			List<RoutingSubregionTile> loadTiles = baseCtx.loadAllSubregionTiles(reader, s);
			tiles.addAll(loadTiles);
		}
		
		int pnts = 0;
		int roads = 0;
		for (RoutingSubregionTile tile : tiles) {
			List<RouteDataObject> dataObjects = new ArrayList<>();
			baseCtx.loadSubregionTile(tile, false, dataObjects, null);
			for (RouteDataObject o : dataObjects) {
				boolean added = registeredRoadIds.add(o.getId());
				if (!added) {
					continue;
				}
				if (!config.router.acceptLine(o)) {
					deleteRoadFromOriginalGraph(o);
					continue;
				}
				boolean shortFerry = "ferry".equals(o.getRoute()) && getRoadDist(o) < ALG_0_SHORT_FERRY_DISTANCE;
				if (shortFerry && IMPROVE_ROUTING_ALGORITHM_VERSION == ALG_0_NAME__CONNECT_ENDPOINTS_ISOLATED_ROADS) {
					deleteRoadFromOriginalGraph(o);
					continue;
				}
				roads++;
				boolean link = o.getHighway() != null && (o.getHighway().endsWith("link"));
				long b = calcPointId(o, 0);
				long e = calcPointId(o, o.getPointsLength() - 1);
				if (!link) {
					addPoint(onlyRoads, o, b);
					addPoint(onlyRoads, o, e);
				}
				for (int i = 0; i < o.getPointsLength(); i++) {
					pnts++;
					addPoint(all, o, calcPointId(o, i));
				}
			}
		}
		log.info(String.format("In total %d base points, %d base segments", pnts, roads));
	}

	

	private double getRoadDist(RouteDataObject o) {
		double dist = 0;
		for (int i = 1; i < o.getPointsLength(); i++) {
			dist += MapUtils.squareRootDist31(o.getPoint31XTile(i - 1), o.getPoint31YTile(i - 1), o.getPoint31XTile(i),
					o.getPoint31YTile(i));
		}
		return dist;
	}

	private void addPoint(TLongObjectHashMap<List<RouteDataObject>> map, RouteDataObject routeDataObject, long pointId) {
		if (!map.containsKey(pointId)) {
			map.put(pointId, new ArrayList<>());
		}
		if (!map.get(pointId).contains(routeDataObject)) {
			map.get(pointId).add(routeDataObject);
		}
	}
	
	private TLongObjectHashMap<RouteDataObject> getIsolatedPoints(TLongObjectHashMap<List<RouteDataObject>> mapOfObjectToCheck,
			TLongObjectHashMap<List<RouteDataObject>> all) {
		TLongObjectHashMap<RouteDataObject> pointsToCheck = new TLongObjectHashMap<>();
		TLongObjectIterator<List<RouteDataObject>> it = mapOfObjectToCheck.iterator();
		while (it.hasNext()) {
			it.advance();
			long point = it.key();
			RouteDataObject rdo = it.value().get(0);
			if (all.get(point).size() == 1) {
				pointsToCheck.put(point, rdo);
			}
		}
		return pointsToCheck;
	}

	private TLongObjectHashMap<RouteDataObject> connectMissingBaseRoads(
			TLongObjectHashMap<RouteDataObject> pointsToCheck,
			TLongObjectHashMap<List<RouteDataObject>> onlyRoads,
			TLongObjectHashMap<List<RouteDataObject>> all,
			RoutingContext baseCtx, RoutingContext normalCtx,  TLongHashSet registeredIds) {
		ImproveRoadsStatsAlg1 ctx = new ImproveRoadsStatsAlg1();
		TLongObjectHashMap<RouteDataObject> toAdd = new TLongObjectHashMap<>();
		TLongHashSet beginIsolated = new TLongHashSet();
		TLongHashSet endIsolated = new TLongHashSet();
		ConsoleProgressImplementation cpi = new ConsoleProgressImplementation();
		
		
		cpi.startTask("Start searching roads in Normal routing graph for adding to Base routing", pointsToCheck.size());
		TLongObjectIterator<RouteDataObject> itn = pointsToCheck.iterator();
		TLongObjectHashMap<RoadClusterTile> allTiles = groupByTiles(all, ALG_1_ZOOM_SEARCH_ROUTES_DANGLING_ROADS);
		TLongObjectHashMap<RoadClusterTile> checkTiles = groupByTiles(onlyRoads, ALG_1_ZOOM_SEARCH_ROUTES_DANGLING_ROADS);
		
		while (itn.hasNext()) {
			if (cpi.progressAndPrint(1)) {
				log.info(String.format("Isolated points %d (scan base %d segs) -> to check %d -> %d (scan normal %d segs) -> found %d (added %d segs)", 
						ctx.isolatedBasePoints, ctx.visitedSegmentsBase, ctx.pointsToCheckShortRoutes,
						ctx.pointsToReachShortRoutes, ctx.visitedSegmentsNormal, ctx.pointsReached,
						ctx.shorterRoutesFound));
			}
			ctx.isolatedBasePoints++;
			itn.advance();
			long point = itn.key();
			RouteDataObject rdo = itn.value();
			int startInd = calcPointId(rdo, 0) == point ? 0 : rdo.getPointsLength() - 1;
			if (IMPROVE_ROUTING_ALGORITHM_VERSION == ALG_1_NAME__CONNECT_ISOLATED_POINTS_MULTIDIJKSTRA) {
				// 1. get points to be reached by routing
				TLongObjectHashMap<List<RouteDataObject>> surroundPointsToReach = getSurroundPoints(rdo, startInd, checkTiles);
				// 2. test base routing to reach points
				MultiDijsktraContext bres = multiTargetDijsktraRouting(baseCtx, new RouteSegment(rdo, startInd), surroundPointsToReach, allTiles, ALG_1_ROUTING_RADIUS_FROM_DANGLING_POINT);;
				ctx.visitedSegmentsBase += bres.visitedSegments;
				if (!surroundPointsToReach.isEmpty()) {
					int toReach = surroundPointsToReach.size();
					ctx.pointsToCheckShortRoutes++;
					ctx.pointsToReachShortRoutes += toReach;
					// 3. test normal routing to reach points (unreached by base)
					MultiDijsktraContext nres = multiTargetDijsktraRouting(normalCtx, new RouteSegment(rdo, startInd), surroundPointsToReach, null, ALG_1_ROUTING_RADIUS_FROM_DANGLING_POINT);;
					ctx.pointsReached += (toReach - surroundPointsToReach.size());
					ctx.visitedSegmentsNormal += nres.visitedSegments;
					// 4. build up missing segments to add from result of normal routing to reach points 
					// TODO here we need to build and find missing routes
					List<RouteDataObject> result = new ArrayList<>();
					for (RouteDataObject obj : result) {
						if (!registeredIds.contains(obj.id)) {
							toAdd.put(obj.id, obj);
							if (TRACE_IMPROVE) {
								log.info("Attach road " + obj.toString());
							}
							ctx.shorterRoutesFound++;
						}
					}
				}
			} else if (IMPROVE_ROUTING_ALGORITHM_VERSION == ALG_0_NAME__CONNECT_ENDPOINTS_ISOLATED_ROADS) {
				List<RouteDataObject> result = findConnectedRoadsOld(normalCtx, rdo, startInd == 0, all);
				if (result.isEmpty()) {
					if (startInd == 0) {
						beginIsolated.add(rdo.getId());
					} else {
						endIsolated.add(rdo.getId());
					}
				} else {
					for (RouteDataObject obj : result) {
						if (!registeredIds.contains(obj.id)) {
							toAdd.put(obj.id, obj);
						}
					}
				}
			}
		}
		
		if (IMPROVE_ROUTING_ALGORITHM_VERSION == ALG_0_NAME__CONNECT_ENDPOINTS_ISOLATED_ROADS) {
			int begSize = beginIsolated.size();
			int endSize = endIsolated.size();
			beginIsolated.retainAll(endIsolated);
			int intersectionSize = beginIsolated.size();
			log.info("All points in base file " + all.size() + " to keep isolated " + (begSize + endSize - 2 * intersectionSize) +
					" to add " + toAdd.size() + " to remove " + beginIsolated.size());
		}
		
		return toAdd;
	}


	/*
	 * Returns road from isolated cluster (they needs to be checked)
	 */
	private List<RouteDataObject> makeClustersAndFindIsolatedRoads(RoutingContext baseCtx, RoutingContext normalCtx, TLongObjectHashMap<List<RouteDataObject>> all) {
		log.info(String.format("Get all road from %d zoom and combine roads to clusters", ALG_2_ZOOM_SCAN_DANGLING_ROADS));
		

		TLongObjectHashMap<RoadClusterTile> clusterTiles = groupByTiles(all, ALG_2_ZOOM_SCAN_DANGLING_ROADS + 1);
		for (RoadClusterTile tile : clusterTiles.valueCollection()) {
			for (RouteDataObject o : tile.ownedRoads.valueCollection()) {
				tile.addToCluster(o, all);
			}
		}

		log.info("Add neighboor tiles to improve connectivity of clusters");
		final int jointNB = ALG_2_SEARCH_ROUTES_DANGLING_ROADS * 2;
		for (RoadClusterTile tile1 : clusterTiles.valueCollection()) {
			for (RoadClusterTile tile2 : clusterTiles.valueCollection()) {
				if (tile1 != tile2 
						&& (tile1.tileX - tile2.tileX <= jointNB && tile2.tileX - tile1.tileX <= jointNB + 1)
						&& (tile1.tileY - tile2.tileY <= jointNB && tile2.tileY - tile1.tileY <= jointNB + 1)) {
					// Group by 4 tiles of lower zoom to avoid problem that disconnected segment between tiles
					if ((tile2.tileX - tile1.tileX == 1 || tile2.tileX == tile1.tileX ) &&
							(tile2.tileY - tile1.tileY == 1 || tile2.tileY == tile1.tileY)) {
						tile1.neighboorTile.add(tile2);
					}
					for (RouteDataObject o : tile2.ownedRoads.valueCollection()) {
						tile1.addToCluster(o, all);
					}
				}
			}
		}
		
		log.info("Clean up clusters from neighboor tile roads");
		for (RoadClusterTile tile : clusterTiles.valueCollection()) {
			// collect own roads of supertile (group by 4)
			TLongObjectHashMap<RouteDataObject> ownedRoads = new TLongObjectHashMap<>(tile.ownedRoads);
			for (RoadClusterTile n : tile.neighboorTile) {
				ownedRoads.putAll(n.ownedRoads);
			}
			Iterator<RoadCluster> it = tile.clusters.iterator();
			while (it.hasNext()) {
				RoadCluster cluster = it.next();
				cluster.removeExcept(ownedRoads);
				if (cluster.roads.size() == 0) {
					it.remove();
				}
			}
		}
		
		log.info("Find disconnected clusters");
		List<RouteDataObject> testResults = new ArrayList<>();
		for (RoadClusterTile tile : clusterTiles.valueCollection()) {
			if (tile.clusters.size() <= 1) {
				// all roads connected
				continue;
			}
			// sort clusters by size desc
			Collections.sort(tile.clusters, new Comparator<RoadCluster>() {
				@Override
				public int compare(RoadCluster o1, RoadCluster o2) {
					return -Integer.compare(o1.roads.size(), o2.roads.size());
				}
			});
			RoadCluster mainCluster = tile.clusters.get(0);
			for (int i = 1; i < tile.clusters.size(); i++) {
				RoadCluster c = tile.clusters.get(i);
				System.out.println(tile.tileId + " " + c.roads.size() + ": " + c.roads + " vs " + mainCluster.roads);
//				testResults.addAll(c.roads);
				RouteDataObject anyRoad = c.roads.get(0);
				if (c.roads.size() == 1 && getRoadDist(anyRoad) < ALG_2_IGNORE_SHORT_FULL_ISOLATED_ROADS) {
					// ROAD SHOULD BE DELETED
					deleteRoadFromOriginalGraph(anyRoad);
					continue;
				}
				testResults.add(anyRoad);
				// testResults.add(addStraightLine(anyRoad, mainCluster.roads));
			}
		}
		return testResults;
		
    }

	private TLongObjectHashMap<RoadClusterTile> groupByTiles(TLongObjectHashMap<List<RouteDataObject>> all,
			 int zoom) {
		TLongObjectIterator<List<RouteDataObject>> pointsIterator = all.iterator();
		TLongObjectHashMap<RoadClusterTile> clusterTiles = new TLongObjectHashMap<>();
		RoadClusterTile clusterTile = new RoadClusterTile();
		while (pointsIterator.hasNext()) {
			pointsIterator.advance();
			long pnt = pointsIterator.key();
			int tileX = (int) (MapUtils.deinterleaveX(pnt) >> (31 - zoom));
			int tileY = (int) (MapUtils.deinterleaveY(pnt) >> (31 - zoom));
			long tileId = MapUtils.interleaveBits(tileX, tileY);
			if (clusterTile.tileId != tileId) {
				clusterTile = clusterTiles.get(tileId);
				if (clusterTile == null) {
					clusterTile = new RoadClusterTile();
					clusterTile.tileId = tileId;
					clusterTile.tileX = tileX;
					clusterTile.tileY = tileY;
					clusterTile.zoom = zoom;
					clusterTiles.put(tileId, clusterTile);
				}
			}
			
			for (RouteDataObject o : pointsIterator.value()) {
				clusterTile.ownedRoads.put(o.getId(), o);
			}
		}
		return clusterTiles;
	}
	
	private static long ID = 1000;
	protected RouteDataObject addStraightLine(RouteDataObject a, List<RouteDataObject> roads) {
		RouteRegion reg = new RouteRegion();
		reg.initRouteEncodingRule(0, "highway", "service");
		RouteDataObject targetRoad = null;
		RouteDataObject rdo = new RouteDataObject(reg);
		int mini = 0, minj = 0; 
		double minDist = Double.POSITIVE_INFINITY;
		for (RouteDataObject road : roads) {
			if (targetRoad == null) {
				targetRoad = road;
			}
			for (int i = 0; i < a.pointsX.length; i++) {
				for (int j = 0; j < road.pointsX.length; j++) {
					double dist = MapUtils.squareRootDist31(a.pointsX[i], a.pointsY[i], road.pointsX[j],
							road.pointsY[j]);
					if(minDist > dist) {
						minDist = dist;
						targetRoad = road;
						mini = i;
						minj = j;
					}
				}
			}
		}

		rdo.pointsX = new int[] { a.pointsX[mini], targetRoad.pointsX[minj] };
		rdo.pointsY = new int[] { a.pointsY[mini], targetRoad.pointsY[minj] };
		rdo.types = new int[] { 0 };
		rdo.id = ID++;
		return rdo;
	}

    private TLongObjectHashMap<List<RouteDataObject>> getSurroundPoints(RouteDataObject startRdo, int ind, 
			TLongObjectHashMap<RoadClusterTile> tiles) {
		TLongObjectHashMap<List<RouteDataObject>> neighboringPoints = new TLongObjectHashMap<>();
		int tileX = startRdo.getPoint31XTile(ind) >> (31 - ALG_1_ZOOM_SEARCH_ROUTES_DANGLING_ROADS);
		int tileY = startRdo.getPoint31YTile(ind) >> (31 - ALG_1_ZOOM_SEARCH_ROUTES_DANGLING_ROADS);
		int nb = (int) Math.ceil(ALG_1_DISTANCE_TO_SEARCH_ROUTE_FROM_DANGLING_POINT / MapUtils.getTileDistanceWidth(
				MapUtils.get31LatitudeY(startRdo.getPoint31YTile(ind)), ALG_1_ZOOM_SEARCH_ROUTES_DANGLING_ROADS));
		for (RoadClusterTile tile : tiles.valueCollection()) {
			if ((Math.abs(tile.tileY - tileY) <= nb && Math.abs(tile.tileX - tileX) <= nb)) {
				for (RouteDataObject rdo : tile.ownedRoads.valueCollection()) {
					double dist = MapUtils.squareRootDist31(rdo.pointsX[ind], rdo.pointsY[ind], startRdo.pointsX[ind],
							startRdo.pointsY[ind]);
					if (dist < ALG_1_DISTANCE_TO_SEARCH_ROUTE_FROM_DANGLING_POINT) {
						addPoint(neighboringPoints, rdo, calcPointId(rdo, 0));
					}
					dist = MapUtils.squareRootDist31(rdo.pointsX[rdo.getPointsLength() - 1],
							rdo.pointsY[rdo.getPointsLength() - 1], startRdo.pointsX[ind], startRdo.pointsY[ind]);
					if (dist < ALG_1_DISTANCE_TO_SEARCH_ROUTE_FROM_DANGLING_POINT) {
						addPoint(neighboringPoints, rdo, calcPointId(rdo, rdo.getPointsLength() - 1));
					}
				}
			}
		}
		return neighboringPoints;
	}

	
	private MultiDijsktraContext multiTargetDijsktraRouting(RoutingContext rctx, RouteSegment routeSegment,
			TLongObjectHashMap<List<RouteDataObject>> surroundPointsToReach,
			TLongObjectHashMap<RoadClusterTile> allTiles, double ROUTING_RADIUS) {
		MultiDijsktraContext ctx = new MultiDijsktraContext();
		
		PriorityQueue<RouteSegment> queue = new PriorityQueue<>(10, Comparator.comparingDouble(RouteSegment::getDistanceFromStart));
//	    List<RouteDataObject> rdoToAdd = new ArrayList<>();
//		VehicleRouter router = rctx.getRouter();
//		ArrayList<RouteDataObject> next = new ArrayList<>();
//		ctx.loadTileData(initial.getPoint31XTile(0), initial.getPoint31YTile(0), 15, next);
//		for (RouteDataObject n : next) {
//			if (n.id == initial.id) {
//				initial = n;
//				break;
//			}
//		}
//		queue.add(new RouteSegment(initial, begin ? 1 : initial.getPointsLength() - 2));
//		TLongHashSet visited = new TLongHashSet();
//		while (!queue.isEmpty()) {
//			RouteSegment segment = queue.poll();
//			int oneWay = router.isOneWay(segment.getRoad());
//			boolean startRoad = initial.id == segment.getRoad().id;
//			if (startRoad) {
//				oneWay = begin ? -1 : 1;
//			}
//			if (oneWay >= 0) {
//				processSegment(ctx, segment, queue, visited, disconnectedPoints, true, startRoad, initial, rdoToAdd);
//			}
//			if (oneWay <= 0) {
//				processSegment(ctx, segment, queue, visited, disconnectedPoints, false, startRoad, initial, rdoToAdd);
//			}
//		}
//	    return rdoToAdd;
		// TODO Auto-generated method stub
		return ctx;
	}
    
	private List<RouteDataObject> findConnectedRoadsOld(RoutingContext ctx, RouteDataObject initial, boolean begin,
	                                                 TLongObjectHashMap<List<RouteDataObject>> all) {
		PriorityQueue<RouteSegment> queue = new PriorityQueue<>(10, Comparator.comparingDouble(RouteSegment::getDistanceFromStart));
		List<RouteDataObject> rdoToAdd = new ArrayList<>();
		VehicleRouter router = ctx.getRouter();
		ArrayList<RouteDataObject> next = new ArrayList<>();
		ctx.loadTileData(initial.getPoint31XTile(0), initial.getPoint31YTile(0), 17, next);
		for (RouteDataObject n : next) {
			if (n.id == initial.id) {
				initial = n;
				break;
			}
		}
		queue.add(new RouteSegment(initial, begin ? 1 : initial.getPointsLength() - 2));
		TLongHashSet visited = new TLongHashSet();
		RouteSegment finalSegment = null;
		while (!queue.isEmpty() && finalSegment == null) {
			RouteSegment segment = queue.poll();
			int oneWay = router.isOneWay(segment.getRoad());
			boolean startRoad = initial.id == segment.getRoad().id;
			if (startRoad) {
				oneWay = begin ? -1 : 1;
			}
			if (oneWay >= 0) {
				finalSegment = processSegment(ctx, segment, queue, visited, all, true, startRoad, initial, rdoToAdd);
			}
			if (oneWay <= 0 && finalSegment != null) {
				finalSegment = processSegment(ctx, segment, queue, visited, all, false, startRoad, initial, rdoToAdd);
			}
		}
		if (finalSegment == null) {
			if (TRACE) {
				System.out.println("Isolated " + initial.id);
			}
		} else {
			StringBuilder b = new StringBuilder("Route for " + initial.id + " : ");
			RouteSegment s = finalSegment;
			while (s != null) {
				if (s.getRoad().id != initial.id) {
					b.append(s.getRoad().id).append(", ");
					rdoToAdd.add(s.getRoad());
				}
				s = s.getParentRoute();
			}
			if (TRACE) {
				System.out.println(b);
			}
			return rdoToAdd;
		}
		return Collections.emptyList();
	}
	
	

	private RouteSegment processSegment(RoutingContext ctx, RouteSegment segment, PriorityQueue<RouteSegment> queue,
	                                    TLongHashSet visited, TLongObjectHashMap<List<RouteDataObject>> basePoints,
			boolean direction, boolean start, RouteDataObject initial, List<RouteDataObject> rdoToAdd) {
		int ind = segment.getSegmentStart();
		RouteDataObject road = segment.getRoad();
		final long pid = calcPointIdUnique(segment.getRoad(), ind);
		if (visited.contains(pid)) {
			return null;
		}
		visited.add(pid);
		double distFromStart = segment.getDistanceFromStart();
		while (true) {
			int py = road.getPoint31YTile(ind);
			int px = road.getPoint31XTile(ind);
			if (direction) {
				ind++;
			} else {
				ind--;
			}
			if (ind < 0 || ind >= segment.getRoad().getPointsLength()) {
				break;
			}

			if (IMPROVE_ROUTING_ALGORITHM_VERSION == ALG_0_NAME__CONNECT_ENDPOINTS_ISOLATED_ROADS) {
				if (basePoints.contains(calcPointId(segment.getRoad(), ind)) && !start) {
					return segment;
				}
			}

			visited.add(calcPointIdUnique(segment.getRoad(), ind));
			int x = road.getPoint31XTile(ind);
			int y = road.getPoint31YTile(ind);
			float spd = ctx.getRouter().defineRoutingSpeed(road) * ctx.getRouter().defineSpeedPriority(road);
			if (spd > ctx.getRouter().getMaxSpeed()) {
				spd = ctx.getRouter().getMaxSpeed();
			}
			distFromStart += MapUtils.squareDist31TileMetric(px, py, x, y) / spd;
			RouteSegment rs = ctx.loadRouteSegment(x, y, 0);
			while (rs != null) {
				if (!visited.contains(calcPointIdUnique(rs.getRoad(), rs.getSegmentStart()))) {
					if (!queue.contains(rs) || rs.getDistanceFromStart() > distFromStart) {
						rs.setDistanceFromStart((float) distFromStart);
						rs.setParentRoute(segment);
						queue.remove(rs);
						queue.add(rs);
					}
				}
				rs = rs.getNext();
			}

			if (IMPROVE_ROUTING_ALGORITHM_VERSION == ALG_1_NAME__CONNECT_ISOLATED_POINTS_MULTIDIJKSTRA) {
				if (basePoints.contains(calcPointId(segment.getRoad(), ind)) && !start) {
					while (segment != null) {
						if (segment.getRoad().id != initial.id) {
							if (!rdoToAdd.contains(segment.getRoad())) {
								rdoToAdd.add(segment.getRoad());
							}
						}
						segment = segment.getParentRoute();
					}
					segment = new RouteSegment(road, ind);
				}
			}
		}
		return null;
	}

	private static long calcPointId(RouteDataObject rdo, int i) {
		return MapUtils.interleaveBits(rdo.getPoint31XTile(i), rdo.getPoint31YTile(i));
	}

	private long calcPointIdUnique(RouteDataObject rdo, int i) {
		return (rdo.getId() << 20L) + i;
	}
	
	private static class ImproveRoadsStatsAlg1 {

		public int isolatedBasePoints;
		public int visitedSegmentsBase;
		public int pointsToCheckShortRoutes;
		public int pointsToReachShortRoutes;
		public int visitedSegmentsNormal;
		public int pointsReached;
		public int shorterRoutesFound;
		
	}
	
	protected static class MultiDijsktraContext {
		public int visitedSegments;
	}

	protected static class RoadClusterTile {
		int zoom;
		int tileY;
		int tileX;
		long tileId;
		List<RoadCluster> clusters = new ArrayList<>();
		List<RoadClusterTile> neighboorTile = new ArrayList<>();
		TLongObjectHashMap<RouteDataObject> ownedRoads = new TLongObjectHashMap<>();
		
		private void addToCluster(RouteDataObject o, TLongObjectHashMap<List<RouteDataObject>> all) {
			RoadCluster toJoin = joinCluster(null, o);
			for (int i = 0; i < o.getPointsLength(); i++) {
				List<RouteDataObject> others = all.get(calcPointId(o, i));
				for (RouteDataObject connectedRoad : others) {
					if (connectedRoad.getId() == o.getId()) {
						continue;
					}
					toJoin = joinCluster(toJoin, connectedRoad);
				}
			}
			if (toJoin == null) {
				toJoin = new RoadCluster(o);
				clusters.add(toJoin);
			} else {
				toJoin.add(o);
			}
		}
		
		private RoadCluster joinCluster(RoadCluster toJoin, RouteDataObject connectedRoad) {
			// could be speed up
			for (int ic = 0; ic < clusters.size(); ic++) {
				RoadCluster check = clusters.get(ic);
				if (check.byId.contains(connectedRoad.getId())) {
					if (toJoin == null || toJoin == check) {
						toJoin = check;
					} else {
						clusters.remove(ic);
						toJoin.merge(check);
					}
					break;
				}
			}
			return toJoin;
		}
	}
	
	private static class RoadCluster {
		
		List<RouteDataObject> roads = new ArrayList<>();
		TLongObjectHashMap<RouteDataObject> byId = new TLongObjectHashMap<RouteDataObject>();
		
		public RoadCluster(RouteDataObject o) {
			add(o);
		}
		
		private void removeExcept(TLongObjectHashMap<RouteDataObject> toKeep) {
			for (RouteDataObject r : new ArrayList<>(roads)) {
				if (!toKeep.containsKey(r.getId())) {
					byId.remove(r.getId());
					roads.remove(r);
				}
			}
		}

		private void add(RouteDataObject o) {
			if (!byId.containsKey(o.getId())) {
				byId.put(o.getId(), o);
				roads.add(o);
			}
		}

		public void merge(RoadCluster r) {
			for (RouteDataObject o : r.roads) {
				add(o);
			}
		}
	}
	
	private void deleteRoadFromOriginalGraph(RouteDataObject o) {
		if (ROAD_SHOULD_BE_DELETED) {
			// Should be implemented later
		}
	}	
}

