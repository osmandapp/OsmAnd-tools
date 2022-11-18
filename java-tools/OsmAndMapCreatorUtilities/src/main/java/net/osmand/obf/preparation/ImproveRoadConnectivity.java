package net.osmand.obf.preparation;

import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.set.hash.TLongHashSet;

import java.io.*;
import java.util.*;

import net.osmand.PlatformUtil;
import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.binary.BinaryMapRouteReaderAdapter.RouteRegion;
import net.osmand.binary.BinaryMapRouteReaderAdapter.RouteSubregion;
import net.osmand.binary.RouteDataObject;
import net.osmand.impl.ConsoleProgressImplementation;
import net.osmand.router.*;
import net.osmand.router.BinaryRoutePlanner.RouteSegment;
import net.osmand.router.RoutePlannerFrontEnd.RouteCalculationMode;
import net.osmand.router.RoutingConfiguration.Builder;
import net.osmand.router.RoutingConfiguration.RoutingMemoryLimits;
import net.osmand.router.RoutingContext.RoutingSubregionTile;
import net.osmand.util.MapUtils;
import org.apache.commons.logging.Log;


//This map generation step adds roads to the Base routing to improve it.
// NEW_IMPROVE_BASE_ROUTING_ALGORITHM:
//1. Using the findAllBaseRoadIntersections() we get a map of base points.
//2. After using the connectMissingBaseRoads() we find all the roads from Normal routing to add to Base routing.
// 2.1 In this method there is a loop over the base points.
// 2.2 For each point, the method getPointsForFindDisconnectedRoads() return neighboring points from tile 14 zoom.
// 2.3 After using method findDisconnectedBasePoints() and using Dijkstra's algorithm, we find all points to which a route hasn't been built.
// 2.4 For each disconnected point using method findConnectedRoads() (with Dijkstra's algorithm) we find roads from Normal routing to which connected start point with disconnected point.
// 2.5 If roads were found, we remove base roads duplicates from result and return.

public class ImproveRoadConnectivity {

	private final Log log = PlatformUtil.getLog(ImproveRoadConnectivity.class);
	
	private static final boolean USE_NEW_IMPROVE_BASE_ROUTING_ALGORITHM = true;
	private static final String ROUTING_PROFILE = "car";
	private static final int DEFAULT_MEMORY_LIMIT = 4000;
	
	private static final boolean TRACE = false;
	private static final boolean TRACE_IMPROVE = false;
	private static final boolean TRACE_TILES = false;
	
	
	private static final int ZOOM_SCAN_DANGLING_ROADS = 16;
	private static final double DISTANCE_TO_SEARCH_ROUTE_FROM_DANGLING_POINT = 500;
	private static final int ZOOM_SEARCH_ROUTES_DANGLING_ROADS = 13;
	

	public static void main(String[] args) throws IOException {
		ConsoleProgressImplementation.deltaTimeToPrintMax = 2000;
		ImproveRoadConnectivity crc = new ImproveRoadConnectivity();
		//File fl = new File("/Users/plotva/work/osmand/maps/Denmark_central-region_europe_2.obf");
//		File fl = new File("/Users/plotva/osmand/China_henan_asia.obf");
		File fl = new File("/Users/victorshcherb/Desktop/China_henan_asia_2.obf");
//		File fl = new File("/Users/victorshcherb/Desktop/Denmark_central-region_europe_2.obf");
		
		RandomAccessFile raf = new RandomAccessFile(fl, "r"); //$NON-NLS-1$ //$NON-NLS-2$
		TLongObjectHashMap<RouteDataObject> map = crc.collectDisconnectedRoads(new BinaryMapIndexReader(raf, fl));
		System.out.println("Found roads: " + map.size());
	}

	public TLongObjectHashMap<RouteDataObject> collectDisconnectedRoads(BinaryMapIndexReader reader) throws IOException {
		TLongObjectHashMap<List<RouteDataObject>> all = new TLongObjectHashMap<>();
		TLongObjectHashMap<List<RouteDataObject>> onlyRoads = new TLongObjectHashMap<>();
		TLongHashSet registeredRoadIds = new TLongHashSet();
		findAllBaseRoadIntersections(reader, all, onlyRoads, registeredRoadIds);
		return connectMissingBaseRoads(onlyRoads, all, reader, null, registeredRoadIds);
	}

	private void findAllBaseRoadIntersections(BinaryMapIndexReader reader,
			TLongObjectHashMap<List<RouteDataObject>> all, TLongObjectHashMap<List<RouteDataObject>> onlyRoads,
			TLongHashSet registeredRoadIds)
			throws IOException {
		RoutePlannerFrontEnd router = new RoutePlannerFrontEnd();
		Builder builder = RoutingConfiguration.getDefault();
		RoutingMemoryLimits memoryLimit = new RoutingMemoryLimits(DEFAULT_MEMORY_LIMIT,
				RoutingConfiguration.DEFAULT_NATIVE_MEMORY_LIMIT);
		RoutingConfiguration config = builder.build(ROUTING_PROFILE, memoryLimit);
		RoutingContext ctx = router.buildRoutingContext(config, null, new BinaryMapIndexReader[] { reader },
				RouteCalculationMode.BASE);
		if (reader.getRoutingIndexes().size() != 1) {
			throw new UnsupportedOperationException();
		}
		RouteRegion reg = reader.getRoutingIndexes().get(0);
		List<RouteSubregion> baseSubregions = reg.getBaseSubregions();
		List<RoutingSubregionTile> tiles = new ArrayList<>();
		for (RouteSubregion s : baseSubregions) {
			List<RoutingSubregionTile> loadTiles = ctx.loadAllSubregionTiles(reader, s);
			tiles.addAll(loadTiles);
		}
		
		for (RoutingSubregionTile tile : tiles) {
			List<RouteDataObject> dataObjects = new ArrayList<>();
			ctx.loadSubregionTile(tile, false, dataObjects, null);
			for (RouteDataObject o : dataObjects) {
				registeredRoadIds.add(o.getId());
				int len = o.getPointsLength() - 1;
				double dist = MapUtils.squareRootDist31(o.getPoint31XTile(0), o.getPoint31YTile(0),
						o.getPoint31XTile(len), o.getPoint31YTile(len));
				boolean shortFerry = "ferry".equals(o.getRoute()) && dist < 1000;
				if (shortFerry) {
					continue;
				}
				boolean link = o.getHighway() != null && (o.getHighway().endsWith("link"));
				long b = calcPointId(o, 0);
				long e = calcPointId(o, len);
				if (!link) {
					addPoint(onlyRoads, o, b);
					addPoint(onlyRoads, o, e);
				}
				for (int i = 0; i < o.getPointsLength(); i++) {
					addPoint(all, o, calcPointId(o, i));
				}
			}
		}
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
			BinaryMapIndexReader reader, TLongHashSet setToRemove, TLongHashSet registeredIds) {
		RoutePlannerFrontEnd frontEnd = new RoutePlannerFrontEnd();
		RoutingMemoryLimits memoryLimit = new RoutingMemoryLimits(DEFAULT_MEMORY_LIMIT,
				RoutingConfiguration.DEFAULT_NATIVE_MEMORY_LIMIT);
		RoutingConfiguration config = RoutingConfiguration.getDefault().build(ROUTING_PROFILE, memoryLimit);
		ImproveRoadsContext ctx = new ImproveRoadsContext();
		ctx.baseCtx = frontEnd.buildRoutingContext(config, null, new BinaryMapIndexReader[] { reader },
				RouteCalculationMode.BASE);
		ctx.normalCtx = frontEnd.buildRoutingContext(config, null, new BinaryMapIndexReader[] { reader },
				RouteCalculationMode.NORMAL);

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
		
		//create map with areas connectivity of a graph
		//Map<Long,TLongHashSet> mapWithAreasConnectivity = getAreasConnectivity(mapOfObjectToCheck, pointsToCheck, ctx);
		//log.info("mapWithAreasConnectivity size = " + mapWithAreasConnectivity.size());
		
		cpi.startTask("Start found roads in Normal routing for added to Base routing: ", pointsToCheck.size());
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
			
			if (USE_NEW_IMPROVE_BASE_ROUTING_ALGORITHM) {
				TLongObjectHashMap<List<RouteDataObject>> neighboringPoints = getPointsForFindDisconnectedRoads(rdo, ctx.baseCtx);
				if (!neighboringPoints.isEmpty()) {
					ctx.pointsNearbyIsolatedPoints += neighboringPoints.size();
					
					TLongObjectHashMap<List<RouteDataObject>> disconnectedPoints = findDisconnectedBasePoints(ctx, rdo, neighboringPoints);
					if (disconnectedPoints != null && !disconnectedPoints.isEmpty()) {
						if (TRACE_IMPROVE) {
							System.out.println("Start road= " + rdo);
							System.out.println("End road= " + disconnectedPoints.values()[0]);
						}
						ctx.pointsToCheckShortRoutes += disconnectedPoints.size();
						List<RouteDataObject> result = findConnectedRoads(ctx.normalCtx, rdo, isBeginPoint, disconnectedPoints);
						for (RouteDataObject obj : result) {
							if (!registeredIds.contains(obj.id)) {
								toAdd.put(obj.id, obj);
								log.debug("Attach road " + obj.toString());
								ctx.shorterRoutesFound++;
							}
						}
					}
				}
			} else {
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
		
		if (!USE_NEW_IMPROVE_BASE_ROUTING_ALGORITHM) {
			int begSize = beginIsolated.size();
			int endSize = endIsolated.size();
			beginIsolated.retainAll(endIsolated);
			int intersectionSize = beginIsolated.size();
			if(setToRemove != null) {
				setToRemove.addAll(beginIsolated);
			}
			log.info("All objects in base file " + mapOfObjectToCheck.size() + " to keep isolated " + (begSize + endSize - 2 * intersectionSize) +
					" to add " + toAdd.size() + " to remove " + beginIsolated.size());
		}
		
		return toAdd;
	}
	
	protected Map<Long, TLongHashSet> getAreasConnectivity(TLongObjectHashMap<List<RouteDataObject>> mapOfObjectToCheck,
	                                                     TLongObjectHashMap<RouteDataObject> pointsToCheck,
	                                                     ImproveRoadsContext ctx) {
		Map<Long, TLongHashSet> resMap = new HashMap<>();
		PriorityQueue<Point> queue = new PriorityQueue<>(mapOfObjectToCheck.keys().length, new Point());
		TLongObjectHashMap<List<RouteDataObject>> allPoints = new TLongObjectHashMap<>();
		
		for (List<RouteDataObject> rdos : mapOfObjectToCheck.valueCollection()) {
			for (RouteDataObject rdo : rdos) {
				for (int i = 0; i < rdo.getPointsLength(); i++) {
					addPoint(allPoints, rdo, calcPointId(rdo, i));
				}
			}
		}
		
		for (long pointId : pointsToCheck.keys()) {
			Map<Long, Double> distFromStarts = new HashMap<>();
			for (long id : allPoints.keys()) {
				if (id == pointId) {
					distFromStarts.put(id, 0.0);
				} else {
					distFromStarts.put(id, Double.MAX_VALUE);
				}
			}
			if (!resMap.containsKey(pointId) || !hasPoint(resMap, pointId)) {
				queue.add(new Point(pointId, 0));
				TLongHashSet visited = new TLongHashSet();
				while (visited.size() != allPoints.keys().length - 1) {
					if (queue.isEmpty()) {
						resMap.put(pointId, visited);
						allPoints.keySet().removeAll(visited);
						break;
					}
					Point point = queue.poll();
					visited.add(point.pointId);
					processNeighboringPoints(point, allPoints, ctx.baseCtx.getRouter(), distFromStarts, queue, visited);
				}
			}
		}
		return resMap;
	}
	
	private boolean hasPoint(Map<Long,TLongHashSet> map, long pointId) {
		for (Map.Entry<Long,TLongHashSet> entry : map.entrySet()) {
			TLongHashSet visited = entry.getValue();
			if (visited.contains(pointId)) {
				return true;
			}
		}
		return false;
	}
    
    private TLongObjectHashMap<List<RouteDataObject>> getPointsForFindDisconnectedRoads(RouteDataObject startRdo, RoutingContext baseCtx) {
        TLongObjectHashMap<List<RouteDataObject>> neighboringPoints = new TLongObjectHashMap<>();
		
		//get all road from tile 14 zoom (tileDistanceWidth = 2000m)
        List<RouteDataObject> roadsForCheck = loadTile(baseCtx, 
        		startRdo.getPoint31XTile(0), startRdo.getPoint31YTile(0), ZOOM_SCAN_DANGLING_ROADS);
        
        for (RouteDataObject rdo : roadsForCheck) {
        	double distStart = MapUtils.squareRootDist31(rdo.pointsX[0], rdo.pointsY[0], startRdo.pointsX[0], startRdo.pointsY[0]);
        	double distEnd = MapUtils.squareRootDist31(rdo.pointsX[rdo.getPointsLength() - 1], rdo.pointsY[rdo.getPointsLength() - 1], 
        			startRdo.pointsX[startRdo.getPointsLength() - 1], startRdo.pointsY[startRdo.getPointsLength() - 1]);
	        if (distStart < DISTANCE_TO_SEARCH_ROUTE_FROM_DANGLING_POINT || distEnd < DISTANCE_TO_SEARCH_ROUTE_FROM_DANGLING_POINT) {
		        addPoint(neighboringPoints, rdo, calcPointId(rdo, 0));
		        addPoint(neighboringPoints, rdo, calcPointId(rdo, rdo.getPointsLength() - 1));
	        }
        }
        return neighboringPoints;
    }

	private List<RouteDataObject> loadTile(RoutingContext ctx, int x, int y, int zoom) {
        List<RouteDataObject> roadsForCheck = new ArrayList<>();
        // tile width of zoom on 31 scale
		int tile = 1 << (31 - zoom);
        // tile of previous zoom
        int tilex = x >> (31 - zoom - 1);
        int tiley = y >> (31 - zoom - 1);
        int nbx = x + (tilex % 2 == 1 ? tile : -tile);
        int nby = y + (tiley % 2 == 1 ? tile : -tile);
	    ctx.loadTileData(x, y, zoom, roadsForCheck);
	    ctx.loadTileData(nbx, y, zoom, roadsForCheck);
	    ctx.loadTileData(x, nby, zoom, roadsForCheck);
	    ctx.loadTileData(nbx, nby, zoom, roadsForCheck);
		
		if (TRACE_TILES) {
			int opx = x - (tilex % 2 == 1 ? tile : -tile);
			int opy = y - (tiley % 2 == 1 ? tile : -tile);
			System.out.printf("X: %d, tile %d, neighbor  tile %d, opposite tile %d%n", x, x >> (31 - zoom),
					nbx >> (31 - zoom), opx >> (31 - zoom));
			System.out.printf("Y: %d, tile %d, neighbor  tile %d, opposite tile %d%n", y, y >> (31 - zoom),
					nby >> (31 - zoom), opy >> (31 - zoom));
			System.out.printf("Distance %.2f < %.2f!%n",
					MapUtils.squareRootDist31(x, y,
							((nbx >> (31 - zoom)) * 2 + 1) << (30 - zoom),
							((nby >> (31 - zoom)) * 2 + 1) << (30 - zoom)),
					MapUtils.squareRootDist31(x, y,
							((opx >> (31 - zoom)) * 2 + 1) << (30 - zoom),
							((opy >> (31 - zoom)) * 2 + 1) << (30 - zoom)));
		}
	    return roadsForCheck;
	}
    
    private TLongObjectHashMap<List<RouteDataObject>> findDisconnectedBasePoints(ImproveRoadsContext ctx,
    		RouteDataObject startRdo, TLongObjectHashMap<List<RouteDataObject>> neighboringPoints) {
        TLongObjectHashMap<List<RouteDataObject>> result = new TLongObjectHashMap<>();
        VehicleRouter router = ctx.baseCtx.getRouter();
        Map<Long, Double> distFromStarts = new HashMap<>();
	
	    if (TRACE_IMPROVE) {
		    if (startRdo.id/64 == 696268599) {
			    System.out.println(startRdo);
		    }
	    }
	    List<RouteDataObject> roadsForCheck = loadTile(ctx.baseCtx, startRdo.getPoint31XTile(0), startRdo.getPoint31YTile(0),
	    		ZOOM_SEARCH_ROUTES_DANGLING_ROADS);
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
			
			if (!USE_NEW_IMPROVE_BASE_ROUTING_ALGORITHM) {
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
			
			if (USE_NEW_IMPROVE_BASE_ROUTING_ALGORITHM) {
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

	private long calcPointId(RouteDataObject rdo, int i) {
		return ((long)rdo.getPoint31XTile(i) << 31L) + rdo.getPoint31YTile(i);
	}

	private long calcPointIdUnique(RouteDataObject rdo, int i) {
		return (rdo.getId() << 20L) + i;
	}
}

