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
//2. After using the getRoadsForImproveBaseRouting() we find all the roads from Normal routing to add to Base routing.
// 2.1 In this method there is a loop over the base points.
// 2.2 For each point, the method getPointsForFindDisconnectedRoads() return neighboring points from tile 14 zoom.
// 2.3 After using method findDisconnectedBasePoints() and using Dijkstra's algorithm, we find all points to which a route hasn't been built.
// 2.4 For each disconnected point using method findConnectedRoads() (with Dijkstra's algorithm) we find roads from Normal routing to which connected start point with disconnected point.
// 2.5 If roads were found, we remove base roads duplicates from result and return.

public class ImproveRoadConnectivity {
	private static final boolean TRACE = false;
	private static final boolean USE_NEW_IMPROVE_BASE_ROUTING_ALGORITHM = true;
	private final Log log = PlatformUtil.getLog(ImproveRoadConnectivity.class);
	
	private final static int ZOOM_SCAN_DANGLING_ROADS = 15;
	private final static double DISTANCE_TO_SEARCH_ROUTE_FROM_DANGLING_POINT = 500;
	private final static int ZOOM_SEARCH_ROUTES_DANGLING_ROADS = 14;

	public static void main(String[] args) throws IOException {
		ConsoleProgressImplementation.deltaTimeToPrintMax = 10000;
		ImproveRoadConnectivity crc = new ImproveRoadConnectivity();
		File fl = new File("/Users/plotva/work/osmand/maps/Denmark_central-region_europe_2.obf");
		//File fl = new File("/Users/plotva/work/osmand/maps/China_henan_asia_2.obf");
		//File fl = new File("/Users/plotva/work/osmand/maps/Newtestv3.obf");
		RandomAccessFile raf = new RandomAccessFile(fl, "r"); //$NON-NLS-1$ //$NON-NLS-2$
		TLongObjectHashMap<RouteDataObject> map = crc.collectDisconnectedRoads(new BinaryMapIndexReader(raf, fl));
		System.out.println("Found roads: " + map.size());
	}

	public TLongObjectHashMap<RouteDataObject> collectDisconnectedRoads(BinaryMapIndexReader reader) throws IOException {
		TLongObjectHashMap<List<RouteDataObject>> all = new TLongObjectHashMap<>();
		TLongObjectHashMap<List<RouteDataObject>> onlyRoads = new TLongObjectHashMap<>();
		TLongHashSet registeredRoadIds = new TLongHashSet();
		findAllBaseRoadIntersections(reader, all, onlyRoads, registeredRoadIds);
		
		return getRoadsForImproveBaseRouting(onlyRoads, all, reader, null, registeredRoadIds);
	}

	private void findAllBaseRoadIntersections(BinaryMapIndexReader reader,
			TLongObjectHashMap<List<RouteDataObject>> all, TLongObjectHashMap<List<RouteDataObject>> onlyRoads,
			TLongHashSet registeredRoadIds)
			throws IOException {
		RoutePlannerFrontEnd router = new RoutePlannerFrontEnd();
		Builder builder = RoutingConfiguration.getDefault();
		RoutingMemoryLimits memoryLimit = new RoutingMemoryLimits(
				RoutingConfiguration.DEFAULT_MEMORY_LIMIT * 3,
				RoutingConfiguration.DEFAULT_NATIVE_MEMORY_LIMIT);
		RoutingConfiguration config = builder.build("car", memoryLimit);
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
			ArrayList<RouteDataObject> dataObjects = new ArrayList<>();
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
		map.get(pointId).add(routeDataObject);
	}

	private TLongObjectHashMap<RouteDataObject> getRoadsForImproveBaseRouting(
			TLongObjectHashMap<List<RouteDataObject>> mapOfObjectToCheck,
			TLongObjectHashMap<List<RouteDataObject>> all,
			BinaryMapIndexReader reader, TLongHashSet setToRemove, TLongHashSet registeredIds) {
		RoutePlannerFrontEnd frontEnd = new RoutePlannerFrontEnd();
		RoutingMemoryLimits memoryLimit = new RoutingMemoryLimits(4000, RoutingConfiguration.DEFAULT_NATIVE_MEMORY_LIMIT * 10);
		RoutingConfiguration config = RoutingConfiguration.getDefault().build("car", memoryLimit);

		TLongObjectHashMap<RouteDataObject> toAdd = new TLongObjectHashMap<>();
		TLongHashSet beginIsolated = new TLongHashSet();
		TLongHashSet endIsolated = new TLongHashSet();
		ConsoleProgressImplementation cpi = new ConsoleProgressImplementation();
		TLongObjectHashMap<RouteDataObject> pointsToCheck = new TLongObjectHashMap<>();
		TLongObjectIterator<List<RouteDataObject>> it = mapOfObjectToCheck.iterator();
		while (it.hasNext()) {
			it.advance();
			if (it.value().size() == 1) {
				pointsToCheck.put(it.key(), it.value().get(0));
			}

		}
		cpi.startTask("Start found roads in Normal routing for added to Base routing: ", pointsToCheck.size());
		TLongObjectIterator<RouteDataObject> itn = pointsToCheck.iterator();
		int isolatedPoints = 0;
		int pointsNearbyIsolatedPoints = 0;
		int pointsToCheckShortRoutes = 0;
		int shorterRoutesFound = 0;
		while (itn.hasNext()) {
			if (cpi.progressAndPrint(1)) {
				log.info(String.format("Isolated points %d -> nearby points %d -> search shorter routes %d -> found routes %d", isolatedPoints, pointsNearbyIsolatedPoints,
						pointsToCheckShortRoutes, shorterRoutesFound));
			}
			isolatedPoints++;
			itn.advance();
			long point = itn.key();
			RouteDataObject rdo = itn.value();
			boolean isBeginPoint = calcPointId(rdo, 0) == point;
			RoutingContext baseCtx = frontEnd.buildRoutingContext(config, null, new BinaryMapIndexReader[] { reader },
					RouteCalculationMode.BASE);
			RoutingContext ctx = frontEnd.buildRoutingContext(config, null, new BinaryMapIndexReader[] { reader },
					RouteCalculationMode.NORMAL);

			if (USE_NEW_IMPROVE_BASE_ROUTING_ALGORITHM) {
				TLongObjectHashMap<List<RouteDataObject>> neighboringPoints = getPointsForFindDisconnectedRoads(rdo,
						baseCtx);
				if (!neighboringPoints.isEmpty()) {
					pointsNearbyIsolatedPoints += neighboringPoints.size();
					TLongObjectHashMap<List<RouteDataObject>> disconnectedPoints = findDisconnectedBasePoints(rdo, baseCtx, neighboringPoints);
					if (disconnectedPoints != null && !disconnectedPoints.isEmpty()) {
						pointsToCheckShortRoutes += disconnectedPoints.size();
						List<RouteDataObject> result = findConnectedRoads(ctx, rdo, isBeginPoint, disconnectedPoints);
						if (!result.isEmpty()) {
							shorterRoutesFound += result.size();
							for (RouteDataObject obj : result) {
								if (!toAdd.contains(obj.id)) {
									toAdd.put(obj.id, obj);
								}
							}
						}
					}
				}
			} else {
				List<RouteDataObject> result = findConnectedRoadsOld(ctx, rdo, isBeginPoint, all);
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
		
		if (USE_NEW_IMPROVE_BASE_ROUTING_ALGORITHM) {
			//remove base roads duplicates from result
			if (!toAdd.isEmpty()) {
				List<RouteDataObject> rdos = new ArrayList<>();
				for (long point : all.keys()) {
					rdos.addAll(all.get(point));
				}
				for (RouteDataObject rdo : rdos) {
					if (toAdd.contains(rdo.id)) {
						toAdd.remove(rdo.id);
					}
				}
			}
		} else {
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
        int tiley = x >> (31 - zoom - 1);
        int nbx = x + (tilex % 2 == 1 ? tile : -tile);
        int nby = y + (tiley % 2 == 1 ? tile : -tile);
	    ctx.loadTileData(x, y, zoom, roadsForCheck);
	    ctx.loadTileData(nbx, y, zoom, roadsForCheck);
	    ctx.loadTileData(x, nby, zoom, roadsForCheck);
	    ctx.loadTileData(nbx, y, zoom, roadsForCheck);
	    
	    // TO CHECK
	    int opx = x - (tilex % 2 == 1 ? tile : -tile);
        int opy = y - (tiley % 2 == 1 ? tile : -tile);
		System.out.println(String.format("X: %d, tile %d, neighbor  tile %d, opposite tile %d", x, x >> (31 - zoom),
				nbx >> (31 - zoom), opx >> (31 - zoom)));
		System.out.println(String.format("Y: %d, tile %d, neighbor  tile %d, opposite tile %d", y, y >> (31 - zoom),
				nby >> (31 - zoom), opy >> (31 - zoom)));
		System.out.println(String.format("Distance %.2f < %.2f!", MapUtils.squareRootDist31(x, y, nbx, nby),
				MapUtils.squareRootDist31(x, y, opx, opy)));
	    return roadsForCheck;
	}
    
    private TLongObjectHashMap<List<RouteDataObject>> findDisconnectedBasePoints(RouteDataObject startRdo, RoutingContext baseCtx, TLongObjectHashMap<List<RouteDataObject>> neighboringPoints) {
        TLongObjectHashMap<List<RouteDataObject>> result = new TLongObjectHashMap<>();
        VehicleRouter router = baseCtx.getRouter();
        Map<Long, Double> distFromStarts = new HashMap<>();
		
	    List<RouteDataObject> roadsForCheck = loadTile(baseCtx, startRdo.getPoint31XTile(0), startRdo.getPoint31YTile(0), ZOOM_SEARCH_ROUTES_DANGLING_ROADS);
	    TLongObjectHashMap<List<RouteDataObject>> allPoints = new TLongObjectHashMap<>();
	    for (RouteDataObject rdo : roadsForCheck) {
		    addPoint(allPoints, rdo, calcPointId(rdo, 0));
		    addPoint(allPoints, rdo, calcPointId(rdo, rdo.getPointsLength() - 1));
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
        while (visited.size() != allPoints.keys().length - 1) {
            if (queue.isEmpty()) {
                for (long id : neighboringPoints.keys()) {
                    if (distFromStarts.get(id) == Double.MAX_VALUE) {
                        result.put(id, neighboringPoints.get(id));
                    }
                }
                return result;
            }
            Point point = queue.poll();
            visited.add(point.pointId);
            processNeighboringPoints(point, allPoints, router, distFromStarts, queue, visited, pointId);
        }
        return null;
    }
    
    private void processNeighboringPoints(Point point, TLongObjectHashMap<List<RouteDataObject>> points, VehicleRouter router,
                                          Map<Long, Double> distFromStarts, PriorityQueue<Point> queue, TLongHashSet visited, long pointId) {
        List<RouteDataObject> rdos = points.get(point.pointId);
		if (rdos != null ) {
			for (RouteDataObject rdo : rdos) {
				int oneWay = router.isOneWay(rdo);
				boolean isBegin = calcPointId(rdo, 0) == point.pointId;
				RouteSegment segment = new RouteSegment(rdo, isBegin ? 0 : rdo.getPointsLength() - 1);
				boolean startRoad = points.get(pointId).get(0).id == segment.getRoad().id;
				if (startRoad) {
					oneWay = isBegin ? 1 : -1;
				}
				if (oneWay >= 0) {
					findNextPoint(true, segment, points, distFromStarts, queue, point, visited);
				}
				if (oneWay <= 0) {
					findNextPoint(false, segment, points, distFromStarts, queue, point, visited);
				}
			}
		}
    }
	
	private void findNextPoint(boolean direction, RouteSegment segment, TLongObjectHashMap<List<RouteDataObject>> points, Map<Long, Double> distFromStarts,
	                           PriorityQueue<Point> queue, Point point, TLongHashSet visited) {
		int ind = segment.getSegmentStart();
		double startLat = (float) MapUtils.get31LongitudeX(segment.getRoad().getPoint31XTile(ind));
		double startLon = (float) MapUtils.get31LatitudeY(segment.getRoad().getPoint31YTile(ind));
		
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
				double endLat = (float) MapUtils.get31LongitudeX(segment.getRoad().getPoint31XTile(ind));
				double endLon = (float) MapUtils.get31LatitudeY(segment.getRoad().getPoint31YTile(ind));
				double dist = MapUtils.getDistance(startLon, startLat, endLon, endLat) + point.distFromStart;
				if (dist < distFromStarts.get(foundPointId)) {
					distFromStarts.replace(calcPointId(segment.getRoad(), ind), dist);
				}
				queue.add(new Point(foundPointId, distFromStarts.get(foundPointId)));
				
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

	private long calcPointId(RouteDataObject rdo, int i) {
		return ((long)rdo.getPoint31XTile(i) << 31L) + rdo.getPoint31YTile(i);
	}

	private long calcPointIdUnique(RouteDataObject rdo, int i) {
		return (rdo.getId() << 20L) + i;
	}
}

