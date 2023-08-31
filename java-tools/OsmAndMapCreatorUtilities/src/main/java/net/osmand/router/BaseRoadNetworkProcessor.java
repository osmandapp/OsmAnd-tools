package net.osmand.router;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.TreeMap;

import javax.xml.stream.XMLStreamException;

import org.apache.commons.logging.Log;


import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.set.TLongSet;
import gnu.trove.set.hash.TLongHashSet;
import net.osmand.PlatformUtil;
import net.osmand.ResultMatcher;
import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.binary.BinaryMapRouteReaderAdapter.RouteRegion;
import net.osmand.binary.BinaryMapRouteReaderAdapter.RouteSubregion;
import net.osmand.binary.BinaryMapRouteReaderAdapter.RouteTypeRule;
import net.osmand.binary.RouteDataObject;
import net.osmand.data.LatLon;
import net.osmand.osm.edit.Entity;
import net.osmand.osm.edit.Node;
import net.osmand.osm.edit.Way;
import net.osmand.osm.io.OsmBaseStorage;
import net.osmand.osm.io.OsmStorageWriter;
import net.osmand.router.BinaryRoutePlanner.MultiFinalRouteSegment;
import net.osmand.router.BinaryRoutePlanner.RouteSegment;
import net.osmand.router.BinaryRoutePlanner.RouteSegmentPoint;
import net.osmand.router.NetworkDB.NetworkDBPoint;
import net.osmand.router.NetworkDB.NetworkDBSegment;
import net.osmand.router.RoutePlannerFrontEnd.RouteCalculationMode;
import net.osmand.router.RoutingConfiguration.Builder;
import net.osmand.router.RoutingConfiguration.RoutingMemoryLimits;
import net.osmand.util.MapUtils;


// TODO 
// 1st phase - investigation
// 1.1 TODO think that island is not possible shortest way to reach boundaries
// 1.2 TODO check toVisitVertices including depth
// 1.3 TODO for long distance causes bugs if (pnt.index != 2005) { 2005-> 1861 } - 3372.75 vs 2598
// 1.4 BinaryRoutePlanner TODO routing 1/-1/0 FIX routing time 7288 / 7088 / 7188 (43.15274, 19.55169 -> 42.955495, 19.0972263)
// 1.5 BinaryRoutePlanner TODO double checkfix correct at all?  https://github.com/osmandapp/OsmAnd/issues/14148
// 1.6 BinaryRoutePlanner TODO ?? we don't stop here in order to allow improve found *potential* final segment - test case on short route
// 1.7 BinaryRoutePlanner TODO test that routing time is different with on & off!

// 2nd phase - improvements
// 2.1 Merge islands that are isolated
// 2.2 Better points distribution
public class BaseRoadNetworkProcessor {

	private static long ID = -1;
	
	final static Log LOG = PlatformUtil.getLog(BaseRoadNetworkProcessor.class);
	final static int BUILD_NETWORK_POINTS = 1;
	final static int BUILD_NETWORK_SEGMENTS = 2;
	final static int RUN_ROUTING = 3;
	
	private static final boolean ALL_VERTICES = false;
	protected static int LIMIT_START = 0;//100
	protected static int LIMIT = -1;
	protected static int PROCESS = RUN_ROUTING;
	
	protected static LatLon EX1 = new LatLon(52.3201813,4.7644685); // 337 - 4
	protected static LatLon EX2 = new LatLon(52.33265, 4.77738); // 301 - 12
	protected static LatLon EX3 = new LatLon(52.2728791, 4.8064803); // 632 - 14
	protected static LatLon EX4 = new LatLon(52.27757, 4.85731); // 218 - 7
	protected static LatLon EX5 = new LatLon(42.78725, 18.95036); // 391 - 8
	
	protected static LatLon EX = null; // for all - null; otherwise specific point 
	protected int VERBOSE_LEVEL = 1;
	
	private static String ROUTING_PROFILE = "car";
	private static int[] MAX_VERT_DEPTH_LOOKUP = new int[] {7,7,7,7} ; //new int[] { 7, 5,3 };
	private static int MAX_BLOCKERS = 50;
	private static float MAX_DIST = 50000;
	
	private RoutingContext ctx;
	private BinaryMapIndexReader reader;
	
	
	class NetworkIsland {
		final NetworkIsland parent;
		final RouteSegment start;
		int index;
		PriorityQueue<RouteSegment> queue =  new PriorityQueue<>(new Comparator<RouteSegment>() {

			@Override
			public int compare(RouteSegment o1, RouteSegment o2) {
				return ctx.roadPriorityComparator(o1.distanceFromStart, o1.distanceToEnd, o2.distanceFromStart,
						o2.distanceToEnd);
			}
		});
		TLongObjectHashMap<RouteSegment> visitedVertices = new TLongObjectHashMap<RouteSegment>();
		TLongObjectHashMap<RouteSegment> toVisitVertices = new TLongObjectHashMap<RouteSegment>();
		
		NetworkIsland(NetworkIsland parent, RouteSegment start) {
			this.parent = parent;
			this.start = start;
			if (start != null) {
				addSegmentToQueue(start);
			}
		}

		private NetworkIsland addSegmentToQueue(RouteSegment s) {
			queue.add(s);
			toVisitVertices.put(calculateRoutePointInternalId(s), s);
			return this;
		}
		
		
		public boolean testIfNetworkPoint(long pntId) {
			if (parent != null) {
				return parent.testIfNetworkPoint(pntId);
			}
			return false;
		}
		
		public boolean testIfVisited(long pntId, boolean checkToVisit) {
			if (visitedVertices.containsKey(pntId)) {
				return true;
			}
			if (checkToVisit && toVisitVertices.containsKey(pntId)) {
				return true;
			}
			if (parent != null) {
				return parent.testIfVisited(pntId, checkToVisit);
			}
			return false;
		}

		public int visitedVerticesSize() {
			return visitedVertices.size() + ( parent == null ? 0 : parent.visitedVerticesSize());
		}

		
		public int depth() {
			return 1 + (parent == null ? 0 : parent.depth());
		}

		public void printCurentState(String string, int lvl) {
			printCurentState(string, lvl, "");
		}
		
		public void printCurentState(String string, int lvl, String extra) {
			if (VERBOSE_LEVEL >= (parent == null ? lvl - 1 : lvl)) {
				StringBuilder tabs = new StringBuilder();
				for(int k = 0 ; k < depth(); k++) {
					tabs.append("   ");
				}
				System.out.println(String.format(" %s %-8s %d -> %d %s", tabs.toString(), string, visitedVerticesSize(), toVisitVertices.size(), extra));
			}
		}
	}
	
	class FullNetwork extends NetworkIsland {

		List<NetworkIsland> clusters = new ArrayList<NetworkIsland>();
		
		TLongObjectHashMap<List<NetworkIsland>> networkPointsCluster = new TLongObjectHashMap<>();
		TLongObjectHashMap<NetworkIsland> visitedPointsCluster = new TLongObjectHashMap<>();
		
		FullNetwork() {
			super(null, null);
		}
		
		public boolean testIfNetworkPoint(long pntId) {
			if (networkPointsCluster.contains(pntId)) {
				return true;
			}
			return super.testIfNetworkPoint(pntId);
		}
		
		public void addCluster(NetworkIsland cluster, RouteSegmentPoint centerPoint) {
			toVisitVertices.putAll(cluster.toVisitVertices);
			for (long key : cluster.toVisitVertices.keys()) {
				List<NetworkIsland> lst = networkPointsCluster.get(key);
				if (lst == null) {
					lst = new ArrayList<NetworkIsland>();
					networkPointsCluster.put(key, lst);
				}
				lst.add(cluster);
			}
			for (long key : cluster.visitedVertices.keys()) {
				if (visitedPointsCluster.containsKey(key)) {
					throw new IllegalStateException();
				}
				visitedPointsCluster.put(key, cluster);
				visitedVertices.put(key, cluster.visitedVertices.get(key)); // reduce memory usage
//				cluster.visitedVertices.put(key, null);// reduce memory usage
			}
			cluster.index = clusters.size();
			clusters.add(cluster);
		}
		
		@Override
		public int depth() {
			return 0;
		}

		public TLongObjectHashMap<List<RouteSegment>> visualConnections() {
			TLongObjectHashMap<List<RouteSegment>> conn = new TLongObjectHashMap<>();
			for (NetworkIsland island : clusters) {
				List<RouteSegment> lst = new ArrayList<>();
				conn.put(calculateRoutePointInternalId(island.start), lst);
				for (RouteSegment border : island.toVisitVertices.valueCollection()) {
					lst.add(border);
				}
			}
			return conn;
		}
		
		public TLongObjectHashMap<RouteSegment> visualPoints() {
			TLongObjectHashMap<RouteSegment> visualPoints = new TLongObjectHashMap<>();
			visualPoints.putAll(toVisitVertices);
			for (NetworkIsland island : clusters) {
				visualPoints.put(calculateRoutePointInternalId(island.start), island.start);
			}
			return visualPoints;
		}
		
	}
	
	
	public static void main(String[] args) throws Exception {
		String name = "Montenegro_europe_2.road.obf";
//		name = "Netherlands_noord-holland_europe_2.road.obf";
//		name = "Ukraine_europe_2.road.obf";
		File obfFile = new File(System.getProperty("maps.dir"), name);
		
		RandomAccessFile raf = new RandomAccessFile(obfFile, "r"); 
		BaseRoadNetworkProcessor proc = new BaseRoadNetworkProcessor();
		
		 
		NetworkDB networkDB = new NetworkDB(new File(obfFile.getParentFile(), name + ".db"),
				PROCESS == BUILD_NETWORK_POINTS ? NetworkDB.FULL_RECREATE
						: PROCESS == BUILD_NETWORK_SEGMENTS ? NetworkDB.RECREATE_SEGMENTS : NetworkDB.READ);
		proc.prepareContext(new BinaryMapIndexReader(raf, obfFile));
		if (PROCESS == BUILD_NETWORK_POINTS) {
			FullNetwork network = proc.new FullNetwork();
			proc.collectNetworkPoints(network);
			networkDB.insertPoints(network);
			List<Entity> objects = proc.visualizeWays(network.visualPoints(), network.visualConnections(), 
					network.visitedVertices);
			saveOsmFile(objects, new File(System.getProperty("maps.dir"), name + ".osm"));
		} else if (PROCESS == BUILD_NETWORK_SEGMENTS) {
			TLongObjectHashMap<NetworkDBPoint> pnts = networkDB.getNetworkPoints(true);
			Collection<Entity> objects = proc.buildNetworkShortcuts(pnts, networkDB);
			saveOsmFile(objects, new File(System.getProperty("maps.dir"), name + "-hh.osm"));
		} else if (PROCESS == RUN_ROUTING) {
			long startTime = System.nanoTime();
			TLongObjectHashMap<NetworkDBPoint> pnts = networkDB.getNetworkPoints(false);
			networkDB.loadNetworkSegments(pnts, false);
			NetworkDBPoint start = pnts.get(1263);// 43.15274, 19.55169
//			NetworkDBPoint start = pnts.get(1659);// 42.4542877, 18.5585636
//			NetworkDBPoint end = pnts.get(253); // 43.16624, 19.55463
//			NetworkDBPoint end = pnts.get(1861); // 43.11556 19.45290 
//			 NetworkDBPoint end = pnts.get(1733); // 42.955495, 19.0972263
			NetworkDBPoint end = pnts.get(1143); // 42.45166, 18.54425
			long loadTime = System.nanoTime() - startTime;
			// Routing
			Collection<Entity> objects = proc.runDijkstraNetworkRouting(networkDB, pnts, start, end);
			long routingTime = System.nanoTime() - startTime - loadTime;
			saveOsmFile(objects, new File(System.getProperty("maps.dir"), name + "-rt.osm"));
			System.out.printf("Routing finished %.2f ms, load data %.2f ms\n", routingTime / 1e6, loadTime / 1e6);
			
		}
		networkDB.close();
	}
	
	





	//////////////////////// UTILITIES /////////////////

	private void prepareContext(BinaryMapIndexReader reader) {
		this.reader = reader;
		RoutePlannerFrontEnd router = new RoutePlannerFrontEnd();
		Builder builder = RoutingConfiguration.getDefault();
		RoutingMemoryLimits memoryLimit = new RoutingMemoryLimits(RoutingConfiguration.DEFAULT_MEMORY_LIMIT * 3,
				RoutingConfiguration.DEFAULT_NATIVE_MEMORY_LIMIT);
		Map<String, String> map = new TreeMap<String, String>();
		map.put("avoid_ferries", "true");
		RoutingConfiguration config = builder.build(ROUTING_PROFILE, memoryLimit, map);
		config.planRoadDirection = 1;
		config.heuristicCoefficient = 0; // dijkstra
		ctx = router.buildRoutingContext(config, null, new BinaryMapIndexReader[] { reader },RouteCalculationMode.NORMAL);
	}

	private List<Entity> visualizeWays(TLongObjectHashMap<RouteSegment> networkPoints, 
			TLongObjectHashMap<List<RouteSegment>> networkPointsConnections , TLongObjectHashMap<RouteSegment> visitedSegments) {
		List<Entity> objs = new ArrayList<>();
		int nodes = 0;
		int ways1 = 0;
		int ways2 = 0;
		if (visitedSegments != null) {
			TLongSet viewed = new TLongHashSet();
			TLongObjectIterator<RouteSegment> it = visitedSegments.iterator();
			while (it.hasNext()) {
				it.advance();
				long pntKey = it.key();
				if (!viewed.contains(pntKey)) {
					RouteSegment s = it.value();
					if (s == null) {
						continue;
					}
					int d = (s.getSegmentStart() < s.getSegmentEnd() ? 1 : -1);
					int segmentEnd = s.getSegmentEnd();
					viewed.add(pntKey);
					while ((segmentEnd + d) >= 0 && (segmentEnd + d) < s.getRoad().getPointsLength()) {
						RouteSegment nxt = new RouteSegment(s.getRoad(), segmentEnd, segmentEnd + d);
						pntKey = calculateRoutePointInternalId(nxt);
						if (!visitedSegments.containsKey(pntKey) || viewed.contains(pntKey)) {
							break;
						}
						viewed.add(pntKey);
						segmentEnd += d;
					}
					Way w = convertRoad(s.getRoad(), s.getSegmentStart(), segmentEnd);
					objs.add(w);
					ways1++;
				}
			}
		}
		if (networkPoints != null) {
			for (long key : networkPoints.keys()) {
				RouteSegment r  = networkPoints.get(key);
				LatLon l = getPoint(r); 
				Node n = new Node(l.getLatitude(), l.getLongitude(), ID--);
				n.putTag("highway", "stop");
				n.putTag("name", r.getRoad().getId() / 64 + " " + r.getSegmentStart() + " " + r.getSegmentEnd());
				objs.add(n);
				nodes++;
				if (networkPointsConnections != null && networkPointsConnections.containsKey(key)) {
					for (RouteSegment conn : networkPointsConnections.get(key)) {
						LatLon ln = getPoint(conn);
						Node n2 = new Node(ln.getLatitude(), ln.getLongitude(), ID--);
						objs.add(n2);
						Way w = new Way(ID--, Arrays.asList(n, n2));
						w.putTag("highway", "motorway");
						objs.add(w);
						ways2++;
					}
				}
			}
		}
		
		System.out.printf("Total visited roads %d, total vertices %d, total stop %d connections - %.1f %% \n",
				ways1, nodes, ways2, (ways2 / 2.0d) / (nodes * (nodes - 1) / 2.0) * 100.0 );
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

	private static void saveOsmFile(Collection<Entity> objects, File file)
			throws FileNotFoundException, XMLStreamException, IOException {
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
		FileOutputStream fous = new FileOutputStream(file);
		w.saveStorage(fous, st, null, true);
		fous.close();
	}

	//////////////////////////// BUILD NETWORK ////////////////////////
	private void collectNetworkPoints(final FullNetwork network) throws IOException {
		if (EX != null) {
			RoutePlannerFrontEnd router = new RoutePlannerFrontEnd();
			RouteSegmentPoint pnt = router.findRouteSegment(EX.getLatitude(), EX.getLongitude(), ctx, null);
			NetworkIsland cluster = new NetworkIsland(network, pnt);
			buildRoadNetworkIsland(cluster);
			network.addCluster(cluster, pnt);
			return;
		}
		double lattop = 85, latbottom = -85, lonleft = -179.9, lonright = 179.9;	
		for (RouteRegion routeRegion : reader.getRoutingIndexes()) {
			List<RouteSubregion> regions = reader
					.searchRouteIndexTree(BinaryMapIndexReader.buildSearchRequest(MapUtils.get31TileNumberX(lonleft),
							MapUtils.get31TileNumberX(lonright), MapUtils.get31TileNumberY(lattop),
							MapUtils.get31TileNumberY(latbottom), 16, null), routeRegion.getSubregions());
			int[] cnt = new int[1];

			reader.loadRouteIndexData(regions, new ResultMatcher<RouteDataObject>() {

				@Override
				public boolean publish(RouteDataObject object) {
					if (!ctx.getRouter().acceptLine(object)) {
						return false;
					}

					cnt[0]++;
					if (cnt[0] < LIMIT_START || isCancelled()) {
						System.out.println("SKIP PROCESS " + cnt[0]);
					} else {
						RouteSegmentPoint pntAround = new RouteSegmentPoint(object, 0, 0);
						long mainPoint = calculateRoutePointInternalId(pntAround);
						if (network.visitedPointsCluster.containsKey(mainPoint)
								|| network.networkPointsCluster.containsKey(mainPoint)) {
							// already existing cluster
							return false;
						}
						NetworkIsland cluster = new NetworkIsland(network, pntAround);
						buildRoadNetworkIsland(cluster);
						int nwPoints = cluster.toVisitVertices.size();
						System.out.println(String.format("CLUSTER: %2d border <- %4d points (%d segments) - %s",
								cluster.toVisitVertices.size(), cluster.visitedVertices.size(),
								nwPoints * (nwPoints - 1) / 2, pntAround));
						network.addCluster(cluster, pntAround);
						System.out.println(String.format("%d %.2f%%: %d points -> %d border points, %d clusters",
								cnt[0], cnt[0] / 1000.0f, network.visitedPointsCluster.size(),
								network.networkPointsCluster.size(), network.clusters.size()));
					}
					return false;
				}

				@Override
				public boolean isCancelled() {
					return LIMIT != -1 && cnt[0] >= LIMIT;
				}

			});
		}
		int total = 0, maxBorder = 0, minBorder = 10000, maxPnts = 0, minPnts= 10000, iso = 0, shortcuts = 0; 
		for (NetworkIsland c : network.clusters) {
			int borderPoints = c.toVisitVertices.size();
			maxBorder = Math.max(maxBorder, borderPoints);
			if (borderPoints == 0) {
				iso++;
			} else {
				minBorder = Math.min(minBorder, borderPoints);
			}
			total += borderPoints;
			shortcuts += borderPoints * (borderPoints - 1) / 2;
			maxPnts = Math.max(maxPnts, c.visitedVertices.size());
			minPnts = Math.min(minPnts, c.visitedVertices.size());
		}
		for(List<NetworkIsland> cl: network.networkPointsCluster.valueCollection()) { 
			total += cl.size();
		};
		System.out.println(String.format(
				"RESULT %d points -> %d border points, %d clusters (%d isolated), %d shortcuts",
				network.visitedPointsCluster.size(), network.networkPointsCluster.size(), network.clusters.size(), iso, shortcuts
				));
		
		System.out.println(String.format(
				"       %.1f avg / %d min / %d max border points per cluster, %.1f avg / %d min / %d max points in cluster", 
				total * 1.0 / network.clusters.size(), minBorder, maxBorder, 
				network.visitedPointsCluster.size() * 1.0 / network.clusters.size(), minPnts, maxPnts ));
		
	}


	private void buildRoadNetworkIsland(NetworkIsland c) {
		c.printCurentState("START", 2);
		while (c.toVisitVertices.size() < MAX_VERT_DEPTH_LOOKUP[c.depth() - 1] && !c.queue.isEmpty()) {
			if (!proceed(c, c.queue.poll(), c.queue)) {
				break;
			}
			// mergeStraights(c, queue); // potential improvement
			c.printCurentState("VISIT", 3);
		}
		c.printCurentState("INITIAL", 2);
		mergeStraights(c, null);
		if (c.depth() < MAX_VERT_DEPTH_LOOKUP.length) {
			mergeConnected(c);
			mergeStraights(c, null);
		}
		c.printCurentState("END", 2);
	}

	private void mergeConnected(NetworkIsland c) {
		List<RouteSegment> potentialNetworkPoints = new ArrayList<>(c.toVisitVertices.valueCollection());
		c.printCurentState("MERGE", 2);
		for (RouteSegment t : potentialNetworkPoints) {
			// 1.3 TODO check toVisitVertices including depth
			if (c.toVisitVertices.size() >= MAX_BLOCKERS) {
				break;
			}
			if (!c.toVisitVertices.contains(calculateRoutePointInternalId(t))) {
				continue;
			}
			int parentToVisit = c.toVisitVertices.size();
			// long id = t.getRoad().getId() / 64;
			NetworkIsland bc = new NetworkIsland(c,t );
			buildRoadNetworkIsland(bc);
			int incPointsAfterMerge = 0;
			for (long l : bc.visitedVertices.keys()) {
				if (c.toVisitVertices.containsKey(l)) {
					incPointsAfterMerge--;
				}
			}
			for (long l : bc.toVisitVertices.keys()) {
				if (!c.toVisitVertices.containsKey(l)) {
					incPointsAfterMerge++;
				}
			}
			// it could only increase by 1 (1->2)
			if ((incPointsAfterMerge <= 2 || coeffToMinimize(c.visitedVerticesSize(),
					parentToVisit) > coeffToMinimize(bc.visitedVerticesSize(), parentToVisit + incPointsAfterMerge) )) {
				int sz = c.visitedVerticesSize();
				c.visitedVertices.putAll(bc.visitedVertices);
				for (long l : bc.visitedVertices.keys()) {
					c.toVisitVertices.remove(l);
				}
				for (long k : bc.toVisitVertices.keys()) {
					if (!c.visitedVertices.containsKey(k)) {
						c.toVisitVertices.put(k, bc.toVisitVertices.get(k));
					}
				}
				c.printCurentState(incPointsAfterMerge == 0 ? "IGNORE": " MERGED", 2, String.format(" - before %d -> %d", sz, parentToVisit));
			} else {
				c.printCurentState(" NOMERGE", 3, String.format(" - %d -> %d", bc.visitedVerticesSize(),
						parentToVisit + incPointsAfterMerge));
			}
		}
	}

	private double coeffToMinimize(int internalSegments, int boundaryPoints) {
		return boundaryPoints * (boundaryPoints - 1) / 2.0 / internalSegments;
	}

	private void mergeStraights(NetworkIsland c, PriorityQueue<RouteSegment> queue) {
		boolean foundStraights = true;
		while (foundStraights && !c.toVisitVertices.isEmpty() && c.toVisitVertices.size() < MAX_BLOCKERS) {
			foundStraights = false;
			for (RouteSegment segment : c.toVisitVertices.valueCollection()) {
				if (!c.testIfNetworkPoint(calculateRoutePointInternalId(segment)) && nonVisitedSegmentsLess(c, segment, 1)) {
					if (proceed(c, segment, queue)) {
						foundStraights = true;
						break;
					}
				}
			}
		}
		c.printCurentState("CONNECT", 2);
	}

	private boolean nonVisitedSegmentsLess(NetworkIsland c, RouteSegment segment, int max) {
		int cnt = countNonVisited(c, segment, segment.getSegmentEnd());
		cnt += countNonVisited(c, segment, segment.getSegmentStart());
		if (cnt <= max) {
			return true;
		}
		return false;
	}

	private int countNonVisited(NetworkIsland c, RouteSegment segment, int ind) {
		int x = segment.getRoad().getPoint31XTile(ind);
		int y = segment.getRoad().getPoint31YTile(ind);
		RouteSegment next = ctx.loadRouteSegment(x, y, 0);
		int cnt = 0;
		while (next != null) {
			if (!c.testIfVisited(calculateRoutePointInternalId(next), true)) {
				cnt++;
			}
			next = next.getNext();
		}
		return cnt;
	}
	
	private boolean proceed(NetworkIsland c, RouteSegment segment, PriorityQueue<RouteSegment> queue) {
		if (segment.distanceFromStart > MAX_DIST) {
			return false;
		}
		long pntId = calculateRoutePointInternalId(segment);
//		System.out.println(" > " + segment);
		if (c.testIfNetworkPoint(pntId)) {
			return true;
		}
		c.toVisitVertices.remove(pntId);
		if (c.testIfVisited(pntId, false)) {
			throw new IllegalStateException();
		}
		c.visitedVertices.put(pntId,  ALL_VERTICES ? segment : null);
		int prevX = segment.getRoad().getPoint31XTile(segment.getSegmentStart());
		int prevY = segment.getRoad().getPoint31YTile(segment.getSegmentStart());
		int x = segment.getRoad().getPoint31XTile(segment.getSegmentEnd());
		int y = segment.getRoad().getPoint31YTile(segment.getSegmentEnd());

		float distFromStart = segment.distanceFromStart + (float) MapUtils.squareRootDist31(x, y, prevX, prevY);
		addSegment(c, distFromStart, segment, true, queue);
		addSegment(c, distFromStart, segment, false, queue);
		return true;
	}
	
	
	private void addSegment(NetworkIsland c, float distFromStart, RouteSegment segment, boolean end, PriorityQueue<RouteSegment> queue) {
		int segmentInd = end ? segment.getSegmentEnd() : segment.getSegmentStart();
		int x = segment.getRoad().getPoint31XTile(segmentInd);
		int y = segment.getRoad().getPoint31YTile(segmentInd);
		RouteSegment next = ctx.loadRouteSegment(x, y, 0);
		while (next != null) {
			// next.distanceFromStart == 0
			RouteSegment test = makePositiveDir(next); 
			long nextPnt = calculateRoutePointInternalId(test);
			
			if (!c.testIfVisited(nextPnt, false) && !c.toVisitVertices.containsKey(nextPnt)) {
//				System.out.println(" + " + test);
				test.distanceFromStart = distFromStart;
				if (queue != null) {
					queue.add(test);
				}
				c.toVisitVertices.put(nextPnt, test);
			}
			RouteSegment opp = makePositiveDir(next.initRouteSegment(!next.isPositive()));
			long oppPnt = opp == null ? 0 : calculateRoutePointInternalId(opp);
			if (opp != null && !c.testIfVisited(oppPnt, false) && !c.toVisitVertices.containsKey(oppPnt)) {
//				System.out.println(" + " + opp);
				opp.distanceFromStart = distFromStart;
				if (queue != null) {
					queue.add(opp);
				}
				c.toVisitVertices.put(oppPnt, opp);
			}
			next = next.getNext();
		}
	}


	/////////////////////////// STATIC ////////////////////////
	private static final int ROUTE_POINTS = 11;
	
	static LatLon getPoint(RouteSegment r) {
		double lon = MapUtils.get31LongitudeX(r.getRoad().getPoint31XTile(r.getSegmentStart(), r.getSegmentEnd()));
		double lat = MapUtils.get31LatitudeY(r.getRoad().getPoint31YTile(r.getSegmentStart(), r.getSegmentEnd()));
		return new LatLon(lat, lon);
	}

	
	static RouteSegment makePositiveDir(RouteSegment next) {
		return next == null ? null : next.isPositive() ? next : 
			new RouteSegment(next.getRoad(), next.getSegmentEnd(), next.getSegmentStart());
	}


	private static long calculateRoutePointInternalId(final RouteDataObject road, int pntId, int nextPntId) {
		int positive = nextPntId - pntId;
		int pntLen = road.getPointsLength();
		if (positive < 0) {
			throw new IllegalStateException("Check only positive segments are in calculation");
		}
		if (pntId < 0 || nextPntId < 0 || pntId >= pntLen || nextPntId >= pntLen || (positive != -1 && positive != 1) ||
				pntLen > (1 << ROUTE_POINTS)) {
			// should be assert
			throw new IllegalStateException("Assert failed");
		}
		return (road.getId() << ROUTE_POINTS) + (pntId << 1) + (positive > 0 ? 1 : 0);
	}
	
	private static long calculateRoutePointInternalId(long id, int pntId, int nextPntId) {
		int positive = nextPntId - pntId;
		return (id << ROUTE_POINTS) + (pntId << 1) + (positive > 0 ? 1 : 0);
	}

	private static long calculateRoutePointInternalId(RouteSegment segm) {
		if (segm.getSegmentStart() < segm.getSegmentEnd()) {
			return calculateRoutePointInternalId(segm.getRoad(), segm.getSegmentStart(), segm.getSegmentEnd());
		} else {
			return calculateRoutePointInternalId(segm.getRoad(), segm.getSegmentEnd(), segm.getSegmentStart());
		}
	}
	
	
	///////////////////////////////////////////// BUILD SHORTCUTS //////////////////////////////////////////
	
	private Collection<Entity> buildNetworkShortcuts(TLongObjectHashMap<NetworkDBPoint> networkPoints, NetworkDB networkDB) throws InterruptedException, IOException, SQLException {
		TLongObjectHashMap<Entity> osmObjects = new TLongObjectHashMap<>();
		double sz = networkPoints.size() / 100.0;
		int ind = 0;
		long tm = System.currentTimeMillis();
		BinaryRoutePlanner brp = new BinaryRoutePlanner();
		// 1.3 TODO for long distance causes bugs if (pnt.index != 2005) { 2005-> 1861 } - 3372.75 vs 2598
		BinaryRoutePlanner.PRECISE_DIST_MEASUREMENT = true;
		TLongObjectHashMap<RouteSegment> segments = new  TLongObjectHashMap<>(); 
		for (NetworkDBPoint pnt : networkPoints.valueCollection()) {
			segments.put(calculateRoutePointInternalId(pnt.roadId, pnt.start, pnt.end), new RouteSegment(null, pnt.start, pnt.end));
			segments.put(calculateRoutePointInternalId(pnt.roadId, pnt.end, pnt.start), new RouteSegment(null, pnt.end, pnt.start));
			addNode(osmObjects, pnt, null, "highway", "stop");
		}
		int maxDirectedPointsGraph = 0;
		int maxFinalSegmentsFound = 0;
		int totalFinalSegmentsFound = 0;
		int totalVisitedDirectSegments = 0;
		for (NetworkDBPoint pnt : networkPoints.valueCollection()) {
//			if (pnt.index != 2005)   { 
//				continue;
//			}
			long nt = System.nanoTime();
			RouteSegment s = ctx.loadRouteSegment(pnt.startX, pnt.startY, ctx.config.memoryLimitation);
			while (s != null && (s.getRoad().getId() != pnt.roadId || s.getSegmentStart() != pnt.start
					|| s.getSegmentEnd() != pnt.end)) {
				s = s.getNext();
			}
			if (s == null) {
				throw new IllegalStateException("Error on segment " + pnt.roadId / 64);
			}
			
			addNode(osmObjects, pnt, getPoint(s), "place", "city");
			List<RouteSegment> result = runDijsktra(brp, s, segments);
			for (RouteSegment t : result) {
				NetworkDBSegment segment = new NetworkDBSegment();
				segment.start = pnt;
				segment.dist = t.getDistanceFromStart();
				segment.end = networkPoints.get(calculateRoutePointInternalId(t.getRoad().getId(),
						Math.min(t.getSegmentStart(), t.getSegmentEnd()),
						Math.max(t.getSegmentStart(), t.getSegmentEnd())));
				pnt.connected.add(segment);
				while (t != null) {
					segment.geometry.add(getPoint(t));
					t = t.getParentRoute();
				}
				Collections.reverse(segment.geometry);
				if (ALL_VERTICES) {
					addWay(osmObjects, segment, "highway", "secondary");
				}
//				System.out.println(segment + " " + segment.dist);
				if (segment.dist < 0) {
					throw new IllegalStateException(segment + " dist < " + segment.dist);
				}
			}
			networkDB.insertSegments(pnt.connected);
			
			
			maxDirectedPointsGraph = Math.max(maxDirectedPointsGraph, ctx.calculationProgress.visitedDirectSegments);
			totalVisitedDirectSegments += ctx.calculationProgress.visitedDirectSegments;
			maxFinalSegmentsFound = Math.max(maxFinalSegmentsFound, ctx.calculationProgress.finalSegmentsFound);
			totalFinalSegmentsFound += ctx.calculationProgress.finalSegmentsFound;
			
			double timeLeft = (System.currentTimeMillis() - tm) / 1000.0 * (networkPoints.size() / (ind + 1) - 1);
			System.out.println(String.format("%.2f%% Process %d (%d shortcuts) - %.1f ms left %.1f sec",
							ind++ / sz, s.getRoad().getId() / 64, result.size(), (System.nanoTime() - nt) / 1.0e6, timeLeft));
			if (ind > LIMIT && LIMIT != -1) {
				break;
			}
		}
		System.out.println(String.format("Total segments %d: %d total shorcuts, per border point max %d, avergage %d shortcuts (routing sub graph max %d, avg %d segments)", 
				segments.size(), totalFinalSegmentsFound, maxFinalSegmentsFound, totalFinalSegmentsFound / ind,
				maxDirectedPointsGraph, totalVisitedDirectSegments  / ind));
		return osmObjects.valueCollection();
	}



	private void addNode(TLongObjectHashMap<Entity> osmObjects, NetworkDBPoint pnt, LatLon l, String tag, String val) {
		if (l == null) {
			l = getPoint(pnt);
		}
		Node n = new Node(l.getLatitude(), l.getLongitude(), ID--);
		n.putTag(tag, val);
		n.putTag("name", pnt.index + " " + pnt.roadId / 64 + " " + pnt.start + " " + pnt.end);
		osmObjects.put(pnt.id, n);
	}




	private LatLon getPoint(NetworkDBPoint pnt) {
		return new LatLon(MapUtils.get31LatitudeY(pnt.startY / 2 + pnt.endY / 2),
				MapUtils.get31LongitudeX(pnt.startX / 2 + pnt.endX / 2));
	}
	
	void addWay(TLongObjectHashMap<Entity> osmObjects, RouteSegment s) {
		Way w = new Way(ID--);
		int[] tps = s.getRoad().getTypes();
		for (int i = 0; i < tps.length; i++) {
			RouteTypeRule rtr = s.getRoad().region.quickGetEncodingRule(tps[i]);
			w.putTag(rtr.getTag(), rtr.getValue());
		}
		int xt = s.getRoad().getPoint31XTile(s.getSegmentStart());
		int yt = s.getRoad().getPoint31YTile(s.getSegmentStart());
		w.addNode(new Node(MapUtils.get31LatitudeY(yt), MapUtils.get31LongitudeX(xt), ID--));
		int xs = s.getRoad().getPoint31XTile(s.getSegmentEnd());
		int ys = s.getRoad().getPoint31YTile(s.getSegmentEnd());
		w.addNode(new Node(MapUtils.get31LatitudeY(ys), MapUtils.get31LongitudeX(xs), ID--));
		w.putTag("oid", (s.getRoad().getId() / 64) + "");
		osmObjects.put(calculateRoutePointInternalId(s), w);
	}
	
	void addWay(TLongObjectHashMap<Entity> osmObjects, NetworkDBSegment segment, String tag, String value) {
		Way w = new Way(ID--);
		w.putTag("name", String.format("%d -> %d %.1f", segment.start.index, segment.end.index, segment.dist));
		for (LatLon l : segment.geometry) {
			w.addNode(new Node(l.getLatitude(), l.getLongitude(), ID--));
		}
		osmObjects.put(w.getId(), w);
	}

	private List<RouteSegment> runDijsktra(BinaryRoutePlanner brp, RouteSegment s, TLongObjectHashMap<RouteSegment> segments) throws InterruptedException, IOException {
		
		long pnt1 = calculateRoutePointInternalId(s.getRoad().getId(), s.getSegmentStart(), s.getSegmentEnd());
		long pnt2 = calculateRoutePointInternalId(s.getRoad().getId(), s.getSegmentEnd(), s.getSegmentStart());
		RouteSegment rm1 = segments.remove(pnt1);
		RouteSegment rm2 = segments.remove(pnt2);
		
		List<RouteSegment> res = new ArrayList<>();
		ctx.unloadAllData(); // needed for proper multidijsktra work
		ctx.calculationProgress = new RouteCalculationProgress();
		ctx.startX = s.getRoad().getPoint31XTile(s.getSegmentStart(), s.getSegmentEnd()) ;
		ctx.startY = s.getRoad().getPoint31YTile(s.getSegmentStart(), s.getSegmentEnd()) ;
		RouteSegmentPoint pnt = new RouteSegmentPoint(s.getRoad(), s.getSegmentStart(), s.getSegmentEnd(), 0);
		MultiFinalRouteSegment frs = (MultiFinalRouteSegment) brp.searchRouteInternal(ctx, pnt, null, null, segments);
		
		if (frs != null) {
			TLongSet set = new TLongHashSet();
			for (RouteSegment o : frs.all) {
				// duplicates are possible as alternative routes
				long pntId = calculateRoutePointInternalId(o);
				if (set.add(pntId)) {
					res.add(o);
				}
			}
		}
		
		System.out.println(ctx.calculationProgress.getInfo(null));
		if (rm1 != null) {
			segments.put(pnt1, rm1);
		}
		if (rm2 != null) {
			segments.put(pnt2, rm2);
		}
		return res;
	}




	
	////////////////////////////////////// ROUTING ////////////////////////////////////////////////	

	private Collection<Entity> runDijkstraNetworkRouting(NetworkDB networkDB, TLongObjectHashMap<NetworkDBPoint> pnts, NetworkDBPoint start,
			NetworkDBPoint end) throws SQLException {
		TLongObjectHashMap<Entity> entities = new TLongObjectHashMap<>();
		int visited = 0;
		PriorityQueue<NetworkDBSegment> queue = new PriorityQueue<>(new Comparator<NetworkDBSegment>() {

			@Override
			public int compare(NetworkDBSegment o1, NetworkDBSegment o2) {
//				return Double.compare(o1DistanceFromStart + heuristicCoefficient * o1DistanceToEnd, 
//						o2DistanceFromStart + heuristicCoefficient *  o2DistanceToEnd);
				return Double.compare(o1.start.rtDistanceFromStart + o1.dist, o2.start.rtDistanceFromStart + o2.dist);
			}
		});
		start.rtDistanceFromStart = 0.1;
		queue.addAll(start.connected);
		while (!queue.isEmpty()) {
			NetworkDBSegment segment = queue.poll();
			// already visited
			if (segment.end.rtDistanceFromStart > 0) { // segment.end.rtRouteToPoint != null
				continue;
			}
			visited++;
			System.out.printf("Visit Point %d from %d (%.1f m from start) %.5f/%.5f - %d\n" , segment.end.index, segment.start.index, 
					(segment.start.rtDistanceFromStart + segment.dist), 
					MapUtils.get31LatitudeY(segment.end.startY), MapUtils.get31LongitudeX(segment.end.startX), segment.end.roadId / 64);
			segment.end.rtRouteToPoint = segment;
			segment.end.rtDistanceFromStart = segment.start.rtDistanceFromStart + segment.dist;
			if (segment.end.index == end.index) {
				break;
			}
			queue.addAll(segment.end.connected);
		}
		System.out.println("-----");
		if (end.rtRouteToPoint != null) {
			NetworkDBSegment parent = end.rtRouteToPoint;
			LinkedList<NetworkDBSegment> segments = new LinkedList<>();
			while (parent != null) {
				networkDB.loadGeometry(parent);
				segments.addFirst(parent);
				addWay(entities, parent, "highway", "secondary");
				parent = parent.start.rtRouteToPoint;
			}
			double sumDist = 0;
			for (NetworkDBSegment s : segments) {
				sumDist += s.dist;
				System.out.printf("Route %d -> %d ( %.5f/%.5f - %d - %.2f s) \n", s.start.index, s.end.index,
						MapUtils.get31LatitudeY(s.end.startY), MapUtils.get31LongitudeX(s.end.startX), s.end.roadId / 64, sumDist);
			}
			
		}
		System.out.println(String.format("Found final route - cost %.2f, %d depth, visited %d vertices", end.rtDistanceFromStart, entities.size(), visited));
		return entities.valueCollection();
	}
}
