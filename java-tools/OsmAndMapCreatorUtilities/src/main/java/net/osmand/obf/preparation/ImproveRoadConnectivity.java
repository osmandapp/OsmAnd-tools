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
	 * Algorithm 0: finds simple connection between isolated points (road cuts) and adds missing road using normal routing
	 */
	private static final int ALG_0_NAME__CONNECT_ENDPOINTS_ISOLATED_ROADS = 0;
	private static final double ALG_01_SHORT_FERRY_DISTANCE = 1000;
	
	/**
	 * Algorithm 1: TODO add description
	 */
	private static final int ALG_1_NAME__CONNECT_ISOLATED_POINTS_MULTIDIJKSTRA = 1;
	private static final double ALG_1_DISTANCE_TO_SEARCH_ROUTE_FROM_DANGLING_POINT = 500;
	private static final int ALG_1_ZOOM_SEARCH_ROUTES_DANGLING_ROADS = 13;
	
	/**
	 * Algorithm 2: guarantees to find disconnected (doesn't check oneway roads) within 
	 * {@value #ALG_2_ZOOM_SCAN_DANGLING_ROADS} 5km (13th zoom) tile that couldn't be connected using extra
	 * {@value #ALG_2_SEARCH_ROUTES_DANGLING_ROADS} 60 km radius of roads (3 * 2 tiles)
	 */
	private static final int ALG_2_NAME__FIND_DISCONNECTED_ROADS = 2;
	private static final int ALG_2_ZOOM_SCAN_DANGLING_ROADS = 13;
	private static final int ALG_2_SEARCH_ROUTES_DANGLING_ROADS = 3;
	private static final int ALG_2_IGNORE_SHORT_FULL_ISOLATED_ROADS = 300;

	private static int IMPROVE_ROUTING_ALGORITHM_VERSION = ALG_2_NAME__FIND_DISCONNECTED_ROADS;

	public static void main(String[] args) throws IOException {
		ConsoleProgressImplementation.deltaTimeToPrintMax = 2000;
		ImproveRoadConnectivity crc = new ImproveRoadConnectivity();
//		File fl = new File(System.getProperty("maps.dir") + "China_henan_asia.obf");
		File fl = new File(System.getProperty("maps.dir") + "Denmark_central-region_europe.obf");
		
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
		RoutingConfiguration config = builder.build(ROUTING_PROFILE, memoryLimit);
		RoutingContext baseCtx = router.buildRoutingContext(config, null, new BinaryMapIndexReader[] { reader }, RouteCalculationMode.BASE);
		RoutingContext normalCtx = router.buildRoutingContext(config, null, new BinaryMapIndexReader[] { reader }, RouteCalculationMode.NORMAL);

		TLongObjectHashMap<List<RouteDataObject>> all = new TLongObjectHashMap<>();
		TLongObjectHashMap<List<RouteDataObject>> onlyRoads = new TLongObjectHashMap<>();
		TLongHashSet registeredRoadIds = new TLongHashSet();
		findAllBaseRoadIntersections(config, baseCtx, all, onlyRoads, registeredRoadIds);
		if (IMPROVE_ROUTING_ALGORITHM_VERSION == ALG_1_NAME__CONNECT_ISOLATED_POINTS_MULTIDIJKSTRA || IMPROVE_ROUTING_ALGORITHM_VERSION == ALG_0_NAME__CONNECT_ENDPOINTS_ISOLATED_ROADS) {
			return connectMissingBaseRoads(onlyRoads, all, baseCtx, normalCtx, registeredRoadIds);
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
					continue;
				}
				double dist = getRoadDist(o);
				boolean shortFerry = "ferry".equals(o.getRoute()) && getRoadDist(o) < ALG_01_SHORT_FERRY_DISTANCE;
				if (shortFerry && IMPROVE_ROUTING_ALGORITHM_VERSION != ALG_2_NAME__FIND_DISCONNECTED_ROADS) {
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

	private TLongObjectHashMap<RouteDataObject> connectMissingBaseRoads(
			TLongObjectHashMap<List<RouteDataObject>> mapOfObjectToCheck,
			TLongObjectHashMap<List<RouteDataObject>> all,
			RoutingContext baseCtx, RoutingContext normalCtx,  TLongHashSet registeredIds) {
		ImproveRoadsContext ctx = new ImproveRoadsContext();
		ctx.baseCtx = baseCtx;
		ctx.normalCtx = normalCtx;
		TLongObjectHashMap<RouteDataObject> toAdd = new TLongObjectHashMap<>();
		TLongHashSet beginIsolated = new TLongHashSet();
		TLongHashSet endIsolated = new TLongHashSet();
		ConsoleProgressImplementation cpi = new ConsoleProgressImplementation();
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
		
		cpi.startTask("Start found roads in Normal routing for added to Base routing", pointsToCheck.size());
		TLongObjectIterator<RouteDataObject> itn = pointsToCheck.iterator();
		
		while (itn.hasNext()) {
			if (cpi.progressAndPrint(1)) {
				log.info(String.format("Isolated points %d -> nearby points %d (scan %d segments) -> points no base routes %d -> found routes %d", 
						ctx.isolatedPoints, ctx.pointsNearbyIsolatedPoints, ctx.visitedPoints, ctx.pointsToCheckShortRoutes,
						ctx.shorterRoutesFound));
			}
			ctx.isolatedPoints++;
			itn.advance();
			long point = itn.key();
			RouteDataObject rdo = itn.value();
			boolean isBeginPoint = calcPointId(rdo, 0) == point;
			if (IMPROVE_ROUTING_ALGORITHM_VERSION == ALG_1_NAME__CONNECT_ISOLATED_POINTS_MULTIDIJKSTRA) {
				TLongObjectHashMap<List<RouteDataObject>> neighboringPoints = getPointsForFindDisconnectedRoads(rdo, ctx.baseCtx);
				if (!neighboringPoints.isEmpty()) {
					ctx.pointsNearbyIsolatedPoints += neighboringPoints.size();
					
					TLongObjectHashMap<List<RouteDataObject>> disconnectedPoints = findDisconnectedBasePoints(ctx, rdo, neighboringPoints);
					if (disconnectedPoints != null && !disconnectedPoints.isEmpty()) {
						if (TRACE_IMPROVE) {
							log.info("Start road= " + rdo);
							log.info("End roads= " + disconnectedPoints.valueCollection());
						}
						ctx.pointsToCheckShortRoutes += disconnectedPoints.size();
						List<RouteDataObject> result = findConnectedRoads(ctx.normalCtx, rdo, isBeginPoint, disconnectedPoints);
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
				}
			} else if (IMPROVE_ROUTING_ALGORITHM_VERSION == ALG_0_NAME__CONNECT_ENDPOINTS_ISOLATED_ROADS) {
				List<RouteDataObject> result = findConnectedRoadsOld(ctx.normalCtx, rdo, isBeginPoint, all);
				if (result.isEmpty()) {
					if (isBeginPoint) {
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
			log.info("All objects in base file " + mapOfObjectToCheck.size() + " to keep isolated " + (begSize + endSize - 2 * intersectionSize) +
					" to add " + toAdd.size() + " to remove " + beginIsolated.size());
		}
		
		return toAdd;
	}
	
	
	
	
	private List<RouteDataObject> makeClustersAndFindIsolatedRoads(RoutingContext baseCtx, RoutingContext normalCtx, TLongObjectHashMap<List<RouteDataObject>> all) {
		log.info(String.format("Get all road from %d zoom and combine roads to clusters", ALG_2_ZOOM_SCAN_DANGLING_ROADS));
		TLongObjectIterator<List<RouteDataObject>> pointsIterator = all.iterator();
		TLongObjectHashMap<RoadClusterTile> clusterTiles = new TLongObjectHashMap<>();
		RoadClusterTile clusterTile = new RoadClusterTile();
		while (pointsIterator.hasNext()) {
			pointsIterator.advance();
			long pnt = pointsIterator.key();
			int tileX = (int) (MapUtils.deinterleaveX(pnt) >> (31 - ALG_2_ZOOM_SCAN_DANGLING_ROADS));
			int tileY = (int) (MapUtils.deinterleaveY(pnt) >> (31 - ALG_2_ZOOM_SCAN_DANGLING_ROADS));
			long tileId = MapUtils.interleaveBits(tileX, tileY);
			if (clusterTile.tileId != tileId) {
				clusterTile = clusterTiles.get(tileId);
				if (clusterTile == null) {
					clusterTile = new RoadClusterTile();
					clusterTile.tileId = tileId;
					clusterTile.tileX = tileX;
					clusterTile.tileY = tileY;
					clusterTiles.put(tileId, clusterTile);
				}
			}
			
			for (RouteDataObject o : pointsIterator.value()) {
				clusterTile.addToCluster(o, all);
				clusterTile.ownedRoads.put(o.getId(), o);
			}
		}

		log.info("Add neighboor tiles to improve connectivity of clusters");
		for (RoadClusterTile tile1 : clusterTiles.valueCollection()) {
			for (RoadClusterTile tile2 : clusterTiles.valueCollection()) {
				if (tile1 != tile2 && Math.abs(tile1.tileX - tile2.tileX) <= ALG_2_SEARCH_ROUTES_DANGLING_ROADS
						&& Math.abs(tile1.tileY - tile2.tileY) <= ALG_2_SEARCH_ROUTES_DANGLING_ROADS) {
					for (RouteDataObject o : tile2.ownedRoads.valueCollection()) {
						tile1.addToCluster(o, all);
					}
				}
			}
		}
		
		log.info("Clean up clusters from neighboor tile roads");
		for (RoadClusterTile tile : clusterTiles.valueCollection()) {
			Iterator<RoadCluster> it = tile.clusters.iterator();
			while (it.hasNext()) {
				RoadCluster cluster = it.next();
				cluster.removeExcept(tile.ownedRoads);
				if (cluster.roads.size() == 0) {
					it.remove();
				}
			}
		}
		
		log.info("Find disconnected clusters");
		List<RouteDataObject> testResults = new ArrayList<>();
		for (RoadClusterTile tile : clusterTiles.valueCollection()) {
			if(tile.clusters.size() <= 1) {
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
				System.out.println(tile.tileId + " " + c.roads.size() + ": " + c.roads);
//				testResults.addAll(c.roads);
				RouteDataObject anyRoad = c.roads.get(0);
				if(c.roads.size() == 1 &&  getRoadDist(anyRoad) < ALG_2_IGNORE_SHORT_FULL_ISOLATED_ROADS) {
					continue;
				}
				testResults.add(anyRoad);
				testResults.add(addStraightLine(anyRoad, mainCluster.roads));
			}
		}
		return testResults;
		
//        return Collections.emptyList();
    }
	
	private static long ID = 1000;

	private RouteDataObject addStraightLine(RouteDataObject a, List<RouteDataObject> roads) {
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

    private TLongObjectHashMap<List<RouteDataObject>> getPointsForFindDisconnectedRoads(RouteDataObject startRdo, RoutingContext baseCtx) {
        TLongObjectHashMap<List<RouteDataObject>> neighboringPoints = new TLongObjectHashMap<>();
		
		//get all road from tile 14 zoom (tileDistanceWidth = 2000m)
        List<RouteDataObject> roadsForCheck = loadTile(baseCtx, 
        		startRdo.getPoint31XTile(0), startRdo.getPoint31YTile(0), ALG_1_ZOOM_SEARCH_ROUTES_DANGLING_ROADS);
        
        for (RouteDataObject rdo : roadsForCheck) {
        	double distStart = MapUtils.squareRootDist31(rdo.pointsX[0], rdo.pointsY[0], startRdo.pointsX[0], startRdo.pointsY[0]);
        	double distEnd = MapUtils.squareRootDist31(rdo.pointsX[rdo.getPointsLength() - 1], rdo.pointsY[rdo.getPointsLength() - 1], 
        			startRdo.pointsX[startRdo.getPointsLength() - 1], startRdo.pointsY[startRdo.getPointsLength() - 1]);
	        if (distStart < ALG_1_DISTANCE_TO_SEARCH_ROUTE_FROM_DANGLING_POINT || distEnd < ALG_1_DISTANCE_TO_SEARCH_ROUTE_FROM_DANGLING_POINT) {
		        addPoint(neighboringPoints, rdo, calcPointId(rdo, 0));
		        addPoint(neighboringPoints, rdo, calcPointId(rdo, rdo.getPointsLength() - 1));
	        }
        }
        return neighboringPoints;
    }

	private List<RouteDataObject> loadTile(RoutingContext ctx, int x31, int y31, int zoom) {
        List<RouteDataObject> roadsForCheck = new ArrayList<>();
        // tile width of zoom on 31 scale
		int tileWidth31 = 1 << (31 - zoom);
        // tile of previous zoom
        int tilex = x31 >> (31 - (zoom + 1));
        int tiley = y31 >> (31 - (zoom + 1));
        int nbx = x31 + (tilex % 2 == 1 ? tileWidth31 : -tileWidth31);
        int nby = y31 + (tiley % 2 == 1 ? tileWidth31 : -tileWidth31);
	    ctx.loadTileData(x31, y31, zoom, roadsForCheck);
	    ctx.loadTileData(nbx, y31, zoom, roadsForCheck);
	    ctx.loadTileData(x31, nby, zoom, roadsForCheck);
	    ctx.loadTileData(nbx, nby, zoom, roadsForCheck);
	    return roadsForCheck;
	}
    
    private TLongObjectHashMap<List<RouteDataObject>> findDisconnectedBasePoints(ImproveRoadsContext ctx,
    		RouteDataObject startRdo, TLongObjectHashMap<List<RouteDataObject>> neighboringPoints) {
        TLongObjectHashMap<List<RouteDataObject>> result = new TLongObjectHashMap<>();
        VehicleRouter router = ctx.baseCtx.getRouter();
        Map<Long, Double> distFromStarts = new HashMap<>();
	
		if (TRACE_IMPROVE) {
			if (startRdo.id / 64 == 696268599) {
				System.out.println(startRdo);
			}
		}
	    List<RouteDataObject> roadsForCheck = loadTile(ctx.baseCtx, startRdo.getPoint31XTile(0), startRdo.getPoint31YTile(0),
	    		ALG_1_ZOOM_SEARCH_ROUTES_DANGLING_ROADS);
	    TLongObjectHashMap<List<RouteDataObject>> allPoints = new TLongObjectHashMap<>();

		for (RouteDataObject rdo : roadsForCheck) {
			for(int i = 0; i < rdo.getPointsLength(); i++) {
				addPoint(allPoints, rdo, calcPointId(rdo, i));
			}
	    }
	
	    long pointId = calcPointId(startRdo, 0);
	    for (long id : allPoints.keys()) {
		    if (id == pointId) {
			    distFromStarts.put(id, 0.0);
		    } else {
			    distFromStarts.put(id, Double.MAX_VALUE);
		    }
	    }
		
        PriorityQueue<Point> queue = new PriorityQueue<>(allPoints.keys().length, new Point());
        
        queue.add(new Point(pointId, 0));
        TLongHashSet visited = new TLongHashSet();
	    TLongHashSet visitedNeighboringPoints = new TLongHashSet();
		
        while (visited.size() != allPoints.keys().length - 1) {
            if (queue.isEmpty() || visitedNeighboringPoints.size() == neighboringPoints.size()) {
				if (neighboringPoints != null) {
					for (long id : neighboringPoints.keys()) {
						if (!distFromStarts.isEmpty() && distFromStarts.containsKey(id) && distFromStarts.get(id) == Double.MAX_VALUE) {
							result.put(id, neighboringPoints.get(id));
						}
					}
				}
                return result;
            }
			
            Point point = queue.poll();
            visited.add(point.pointId);
            ctx.visitedPoints++;
			if (neighboringPoints.contains(point.pointId)) {
				visitedNeighboringPoints.add(point.pointId);
			}
            processNeighboringPoints(point, allPoints, router, distFromStarts, queue, visited);
        }
        return null;
    }
	
	private void processNeighboringPoints(Point point, TLongObjectHashMap<List<RouteDataObject>> points, VehicleRouter router,
	                                      Map<Long, Double> distFromStarts, PriorityQueue<Point> queue, TLongHashSet visited) {
		List<RouteDataObject> rdos = points.get(point.pointId);
		if (rdos != null) {
			for (RouteDataObject rdo : rdos) {
				if (TRACE_IMPROVE) {
					if (rdo.id / 64 == 724261312) {
						System.out.println(rdo);
					}
				}
				int oneWay = router.isOneWay(rdo);
				int ind = -1;
				for (int i = 0; i < rdo.getPointsLength(); i++) {
					if (calcPointId(rdo, i) == point.pointId) {
						ind = i;
						break;
					}
				}
				
				if (ind != -1) {
					boolean isBegin = calcPointId(rdo, 0) == point.pointId;
					RouteSegment segment = new RouteSegment(rdo, isBegin ? 0 : ind);
					if (oneWay >= 0) {
						findNextPoint(true, segment, points, distFromStarts, queue, visited);
					}
					if (oneWay <= 0) {
						findNextPoint(false, segment, points, distFromStarts, queue, visited);
					}
				}
			}
		}
	}
	
	private void findNextPoint(boolean direction, RouteSegment segment, TLongObjectHashMap<List<RouteDataObject>> points, Map<Long, Double> distFromStarts,
	                           PriorityQueue<Point> queue, TLongHashSet visited) {
		int ind = segment.getSegmentStart();
		long startPointId = calcPointId(segment.getRoad(), ind);
		int startLat = segment.getRoad().getPoint31XTile(segment.getSegmentStart());
		int startLon = segment.getRoad().getPoint31YTile(segment.getSegmentStart());
		
		while (true) {
			if (direction) {
				ind++;
			} else {
				ind--;
			}
			if (ind < 0 || ind >= segment.getRoad().getPointsLength()) {
				break;
			}
			
			long foundPointId = calcPointId(segment.getRoad(), ind);
			if (points.contains(foundPointId) && !visited.contains(foundPointId)) {
				if (distFromStarts != null) {
					double resDist;
					int endLat = segment.getRoad().getPoint31XTile(ind);
					int endLon = segment.getRoad().getPoint31YTile(ind);
					double dist = MapUtils.squareRootDist31(endLat, endLon,
							startLat, startLon) + distFromStarts.get(startPointId);
					
					boolean hasAlready = false;
					for (Point p : queue) {
						if (p.pointId == foundPointId) {
							hasAlready = true;
							if (p.distFromStart > dist) {
								distFromStarts.replace(p.pointId, dist);
								queue.remove(p);
								queue.add(new Point(foundPointId, distFromStarts.get(foundPointId)));
								break;
							}
						}
					}
					
					
					if (dist < distFromStarts.get(foundPointId)) {
						resDist = dist;
						distFromStarts.replace(calcPointId(segment.getRoad(), ind), dist);
					} else {
						resDist = distFromStarts.get(foundPointId);
					}
					if (resDist < 20000 && !hasAlready) {
						queue.add(new Point(foundPointId, distFromStarts.get(foundPointId)));
					}
				} else {
					queue.add(new Point(foundPointId, 0));
				}
				break;
			}
		}
	}
	
	static class Point implements Comparator<Point> {
		public long pointId;
		public double distFromStart;
		
		public Point() {
		}
		
		public Point(long pointId, double distFromStart) {
			this.pointId = pointId;
			this.distFromStart = distFromStart;
		}
		
		@Override
		public int compare(Point point1, Point point2) {
			return Double.compare(point1.distFromStart, point2.distFromStart);
		}
	}
    
    
    private List<RouteDataObject> findConnectedRoads(RoutingContext ctx, RouteDataObject initial, boolean begin,
	                                                 TLongObjectHashMap<List<RouteDataObject>> disconnectedPoints) {
		PriorityQueue<RouteSegment> queue = new PriorityQueue<>(10, Comparator.comparingDouble(RouteSegment::getDistanceFromStart));
	    List<RouteDataObject> rdoToAdd = new ArrayList<>();
		VehicleRouter router = ctx.getRouter();
		ArrayList<RouteDataObject> next = new ArrayList<>();
		ctx.loadTileData(initial.getPoint31XTile(0), initial.getPoint31YTile(0), 15, next);
		for (RouteDataObject n : next) {
			if (n.id == initial.id) {
				initial = n;
				break;
			}
		}
		queue.add(new RouteSegment(initial, begin ? 1 : initial.getPointsLength() - 2));
		TLongHashSet visited = new TLongHashSet();
		while (!queue.isEmpty()) {
			RouteSegment segment = queue.poll();
			int oneWay = router.isOneWay(segment.getRoad());
			boolean startRoad = initial.id == segment.getRoad().id;
			if (startRoad) {
				oneWay = begin ? -1 : 1;
			}
			if (oneWay >= 0) {
				processSegment(ctx, segment, queue, visited, disconnectedPoints, true, startRoad, initial, rdoToAdd);
			}
			if (oneWay <= 0) {
				processSegment(ctx, segment, queue, visited, disconnectedPoints, false, startRoad, initial, rdoToAdd);
			}
		}
	    return rdoToAdd;
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
		if(visited.contains(pid)) {
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
	
	private static class ImproveRoadsContext {

		public int visitedPoints;
		public int isolatedPoints = 0;
		public int pointsNearbyIsolatedPoints = 0;
		public int pointsToCheckShortRoutes = 0;
		public int shorterRoutesFound = 0;
		public RoutingContext normalCtx;
		public RoutingContext baseCtx;
		
	}

	private static long calcPointId(RouteDataObject rdo, int i) {
		return MapUtils.interleaveBits(rdo.getPoint31XTile(i), rdo.getPoint31YTile(i));
	}

	private long calcPointIdUnique(RouteDataObject rdo, int i) {
		return (rdo.getId() << 20L) + i;
	}
	
	

	private static class RoadClusterTile {
		int tileY;
		int tileX;
		long tileId;
		List<RoadCluster> clusters = new ArrayList<RoadCluster>();
		TLongObjectHashMap<RouteDataObject> ownedRoads = new TLongObjectHashMap<RouteDataObject>();
		
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
	
}

