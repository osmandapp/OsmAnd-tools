package net.osmand.router;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
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
import net.osmand.obf.preparation.DBDialect;
import net.osmand.osm.edit.Entity;
import net.osmand.osm.edit.Node;
import net.osmand.osm.edit.Way;
import net.osmand.osm.io.OsmBaseStorage;
import net.osmand.osm.io.OsmStorageWriter;
import net.osmand.router.BinaryRoutePlanner.MultiFinalRouteSegment;
import net.osmand.router.BinaryRoutePlanner.RouteSegment;
import net.osmand.router.BinaryRoutePlanner.RouteSegmentPoint;
import net.osmand.router.RoutePlannerFrontEnd.RouteCalculationMode;
import net.osmand.router.RoutingConfiguration.Builder;
import net.osmand.router.RoutingConfiguration.RoutingMemoryLimits;
import net.osmand.util.MapUtils;

public class BaseRoadNetworkProcessor {

	private static long ID = -1;
	private static final int ROUTE_POINTS = 11;
	private static final Log LOG = PlatformUtil.getLog(BaseRoadNetworkProcessor.class);
	private static final boolean ALL_VERTICIES = true;
	protected static int LIMIT_START = 100;//100
	protected static int LIMIT = 150;
	
	protected static LatLon EX1 = new LatLon(52.3201813,4.7644685);
	protected static LatLon EX2 = new LatLon(52.33265, 4.77738);
	protected static LatLon EX3 = new LatLon(52.2728791, 4.8064803);
	protected static LatLon EX4 = new LatLon(52.27757, 4.85731);
	protected static LatLon EX5 = new LatLon(42.78725, 18.95036);
	
	protected static LatLon EX = null; // for all - null; otherwise specific point 
	protected int VERBOSE_LEVEL = 1;
	
	private static String ROUTING_PROFILE = "car";
	private static int[] INITIAL_LOOKUP = new int[] {7,6,5,4} ; //new int[] { 7, 5,3 };
	private static int MAX_BLOCKERS = 100;
	private static float MAX_DIST = 5000;
	
	double lattop = 85;
	double latbottom = -85;
	double lonleft = -179.9;
	double lonright = 179.9;	
	
	private RoutingContext ctx;
	private BinaryMapIndexReader reader;
	
	
	class BaseCluster {
		final BaseCluster parent;
		PriorityQueue<RouteSegment> queue =  new PriorityQueue<>(new Comparator<RouteSegment>() {

			@Override
			public int compare(RouteSegment o1, RouteSegment o2) {
				return ctx.roadPriorityComparator(o1.distanceFromStart, o1.distanceToEnd, o2.distanceFromStart,
						o2.distanceToEnd);
			}
		});
		TLongObjectHashMap<RouteSegment> visitedVertices = new TLongObjectHashMap<RouteSegment>();
		TLongObjectHashMap<RouteSegment> toVisitVertices = new TLongObjectHashMap<RouteSegment>();
		
		// to trace which were reviewed
		TLongObjectHashMap<RouteSegment> existingNetworkPoints = new TLongObjectHashMap<RouteSegment>();
		
		BaseCluster(BaseCluster parent) {
			this.parent = parent;
		}


		private BaseCluster addSegmentToQueue(RouteSegment s) {
			queue.add(s);
			toVisitVertices.put(calculateRoutePointId(s), s);
			return this;
		}
		
				
		public RouteSegment testIfNetworkPoint(long pntId) {
			if (existingNetworkPoints.contains(pntId)) {
				return existingNetworkPoints.get(pntId);
			}
			if (parent != null) {
				return parent.testIfNetworkPoint(pntId);
			}
			return null;
		}
		
		public boolean testIfVisited(long pntId, boolean checkToVisit) {
			if (markAsReached) {
				RouteSegment segment = testIfNetworkPoint(pntId);
				if (segment != null) {
					reachedNetworkPoints.put(pntId, segment);
				}
			}
			if (visitedVertices.containsKey(pntId)) {
				return true;
			}
			if (checkToVisit && toVisitVertices.containsKey(pntId)) {
				return true;
			}
			if (parent != null) {
				return parent.testIfVisited(pntId, checkToVisit, false);
			}
			return false;
		}

		public int visitedVerticesSize() {
			return visitedVertices.size() + ( parent == null ? 0 : parent.visitedVerticesSize());
		}

		public void fixToVisitVerticesAsNetworkPoints() {
			existingNetworkPoints.putAll(toVisitVertices);
			visitedVertices.putAll(toVisitVertices);
			toVisitVertices.clear();
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
	
	
	public static void main(String[] args) throws Exception {
		String name = "Montenegro_europe_2.road.obf";
//		name = "Netherlands_noord-holland_europe_2.road.obf";
		File obfFile = new File(System.getProperty("maps.dir"), name);
		
		RandomAccessFile raf = new RandomAccessFile(obfFile, "r"); 
		BaseRoadNetworkProcessor proc = new BaseRoadNetworkProcessor();
		
		boolean collectPoints = true; 
		NetworkDB networkDB = new NetworkDB(new File(obfFile.getParentFile(), name + ".db"), collectPoints);
		proc.prepareContext(new BinaryMapIndexReader(raf, obfFile));
		if (collectPoints) {
			BaseCluster cluster = proc.new BaseCluster(null);
			// TODO move to cluster
			TLongObjectHashMap<RouteSegment> networkPoints = new TLongObjectHashMap<>();
			TLongObjectHashMap<List<RouteSegment>> networkPointsConnections = new TLongObjectHashMap<>();
			proc.collectNetworkPoints(cluster, networkPoints, networkPointsConnections);
			networkDB.insertPoints(networkPoints);
			List<Entity> objects = proc.visualizeWays(networkPoints, networkPointsConnections, 
					cluster.visitedVertices);
			saveOsmFile(objects, new File(System.getProperty("maps.dir"), name + ".osm"));
		} else {
			TLongObjectHashMap<NetworkPoint> pnts = networkDB.getNetworkPoints();
			List<Entity> objects = proc.buildNetworkSegments(pnts);
			saveOsmFile(objects, new File(System.getProperty("maps.dir"), name + "-hh.osm"));
		}
		networkDB.close();
	}
	
	

	private static void saveOsmFile(List<Entity> objects, File file)
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

	private TLongObjectHashMap<RouteSegment> collectNetworkPoints(final BaseCluster cluster, 
			TLongObjectHashMap<RouteSegment> networkPoints, TLongObjectHashMap<List<RouteSegment>> networkPointsConnections) throws IOException {
		if (reader.getRoutingIndexes().size() != 1) {
			throw new UnsupportedOperationException();
		}		
		if (EX != null) {
			RoutePlannerFrontEnd router = new RoutePlannerFrontEnd();
			RouteSegmentPoint pnt = router.findRouteSegment(EX.getLatitude(), EX.getLongitude(), ctx, null);
			buildRoadNetworkIsland(cluster.addSegmentToQueue(pnt), 0);
			for (long pntKey : cluster.toVisitVertices.keys()) {
				networkPoints.put(pntKey, cluster.toVisitVertices.get(pntKey));
			}
			return networkPoints;
		}
		RouteRegion routeRegion = reader.getRoutingIndexes().get(0);
		List<RouteSubregion> regions = reader.searchRouteIndexTree(
				BinaryMapIndexReader.buildSearchRequest(MapUtils.get31TileNumberX(lonleft),
						MapUtils.get31TileNumberX(lonright), MapUtils.get31TileNumberY(lattop),
						MapUtils.get31TileNumberY(latbottom), 16, null),routeRegion.getSubregions());
		int[] cnt = new int[1];
		
		reader.loadRouteIndexData(regions, new ResultMatcher<RouteDataObject>() {

			@Override
			public boolean publish(RouteDataObject object) {
				if (!ctx.getRouter().acceptLine(object)) {
					return false;
				}
				
				cnt[0]++;
				if(cnt[0] < LIMIT_START || isCancelled()) {
					System.out.println("SKIP PROCESS " + cnt[0]);
				} else {
					cluster.reachedNetworkPoints.clear();
					RouteSegmentPoint pntAround = new RouteSegmentPoint(object, 0, 0);
					buildRoadNetworkIsland(cluster.addSegmentToQueue(pntAround), 0);
					for (long pntKey : cluster.toVisitVertices.keys()) {
						networkPoints.put(pntKey, cluster.toVisitVertices.get(pntKey));
						for (long pntKeyConn : cluster.toVisitVertices.keys()) {
							if (pntKeyConn != pntKey)
								addNetworkPointConnection(networkPointsConnections, pntKey,
										cluster.toVisitVertices.get(pntKeyConn));
						}
						for (long pntKeyConn : cluster.reachedNetworkPoints.keys()) {
							if (pntKeyConn != pntKey)
								addNetworkPointConnection(networkPointsConnections, pntKey,
										cluster.reachedNetworkPoints.get(pntKeyConn));
						}
						
					}
					cluster.fixToVisitVerticesAsNetworkPoints();
					
					System.out.println(String.format("%d %.2f%%: %d -> %d - %d", cnt[0], cnt[0] / 1000.0f,
							cluster.visitedVerticesSize(), networkPoints.size(), object.getId() / 64));
				}
				return false;
			}

			private void addNetworkPointConnection(TLongObjectHashMap<List<RouteSegment>> networkPointsConnections,
					long pntKey, RouteSegment routeSegment) {
				List<RouteSegment> connections = networkPointsConnections.get(pntKey);
				if (connections == null) {
					connections = new ArrayList<BinaryRoutePlanner.RouteSegment>();
					networkPointsConnections.put(pntKey, connections);

				}
				if (!connections.contains(routeSegment)) {
					connections.add(routeSegment);
				}
			}

			@Override
			public boolean isCancelled() {
				return LIMIT != -1 && cnt[0] >= LIMIT;
			}
			
		});
		
		return networkPoints;

	}





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

	
	private void buildRoadNetworkIsland(BaseCluster c, int depth) {
		c.printCurentState("START", 2);
		while (c.queue.size() < INITIAL_LOOKUP[depth] && !c.queue.isEmpty()) {
			if (!proceed(c, c.queue.poll(), c.queue)) {
				break;
			}
			// mergeStraights(c, queue); // potential improvement
			c.printCurentState("VISIT", 3);
		}
		c.printCurentState("INITIAL", 2);
		mergeStraights(c, null);
		if (depth + 1 < INITIAL_LOOKUP.length) {
			mergeConnected(c, depth + 1);
			mergeStraights(c, null);
		}
		c.printCurentState("END", 2);
	}

	private void mergeConnected(BaseCluster c, int depth) {
		List<RouteSegment> potentialNetworkPoints = new ArrayList<>(c.toVisitVertices.valueCollection());
		c.printCurentState("MERGE", 2);
		for (RouteSegment t : potentialNetworkPoints) {
			int parentToVisit = c.toVisitVertices.size();
			// long id = t.getRoad().getId() / 64;
			BaseCluster bc = new BaseCluster(c).addSegmentToQueue(t);
			buildRoadNetworkIsland(bc, depth);
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
					parentToVisit) > coeffToMinimize(bc.visitedVerticesSize(), parentToVisit + incPointsAfterMerge))) {
				int sz = c.visitedVerticesSize();
				c.visitedVertices.putAll(bc.visitedVertices);
				c.reachedNetworkPoints.putAll(bc.reachedNetworkPoints);
				for (long l : bc.visitedVertices.keys()) {
					c.toVisitVertices.remove(l);
				}
				for (long k : bc.toVisitVertices.keys()) {
					if (!c.visitedVertices.containsKey(k)) {
						c.toVisitVertices.put(k, bc.toVisitVertices.get(k));
					}
				}
				c.printCurentState(incPointsAfterMerge == 0 ? " IGNORE": " MERGED", 2, String.format(" - before %d -> %d", sz, parentToVisit));
			} else {
				c.printCurentState(" NOMERGE", 3, String.format(" - %d -> %d", bc.visitedVerticesSize(),
						parentToVisit + incPointsAfterMerge));
			}
		}
	}

	private double coeffToMinimize(int internalSegments, int boundaryPoints) {
		return boundaryPoints * (boundaryPoints - 1) / 2.0 / internalSegments;
	}

	private void mergeStraights(BaseCluster c, PriorityQueue<RouteSegment> queue) {
		boolean foundStraights = true;
		while (foundStraights && !c.toVisitVertices.isEmpty() && c.toVisitVertices.size() < MAX_BLOCKERS) {
			foundStraights = false;
			for (RouteSegment segment : c.toVisitVertices.valueCollection()) {
				if (nonVisitedSegmentsLess(c, segment, 1)) {
					if (proceed(c, segment, queue)) {
						foundStraights = true;
						break;
					}
				}
			}
		}
		c.printCurentState("CONNECT", 2);
	}

	private boolean nonVisitedSegmentsLess(BaseCluster c, RouteSegment segment, int max) {
		int cnt = countNonVisited(c, segment, segment.getSegmentEnd());
		cnt += countNonVisited(c, segment, segment.getSegmentStart());
		if (cnt <= max) {
			return true;
		}
		return false;
	}

	private int countNonVisited(BaseCluster c, RouteSegment segment, int ind) {
		int x = segment.getRoad().getPoint31XTile(ind);
		int y = segment.getRoad().getPoint31YTile(ind);
		RouteSegment next = ctx.loadRouteSegment(x, y, 0);
		int cnt = 0;
		while (next != null) {
			if (!c.testIfVisited(calculateRoutePointId(next), true, false)) {
				cnt++;
			}
			next = next.getNext();
		}
		return cnt;
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
						pntKey = calculateRoutePointId(nxt);
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
				if(networkPointsConnections != null && networkPointsConnections.containsKey(key)) {
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
		
		System.out.println(String.format("Total visited roads %d, total vertices %d, total stop %d connections - %.1f %% ",
				ways1, nodes, ways2, (ways2 / 2.0d) / (nodes * (nodes - 1) / 2.0) * 100.0 )) ;
		return objs;
	}





	private static LatLon getPoint(RouteSegment r) {
		int x1 = r.getRoad().getPoint31XTile(r.getSegmentStart());
		int x2 = r.getRoad().getPoint31XTile(r.getSegmentEnd());
		int y1 = r.getRoad().getPoint31YTile(r.getSegmentStart());
		int y2 = r.getRoad().getPoint31YTile(r.getSegmentEnd());
		double lon = MapUtils.get31LongitudeX(x1 / 2 + x2 / 2);
		double lat = MapUtils.get31LatitudeY(y1 / 2 + y2 / 2);
		LatLon l = new LatLon(lat, lon);
		return l;
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

	private boolean proceed(BaseCluster c, RouteSegment segment, PriorityQueue<RouteSegment> queue) {
		if (segment.distanceFromStart > MAX_DIST) {
			return false;
		}
		long pntId = calculateRoutePointId(segment);
		if (c.existingNetworkPoints.containsKey(pntId)) {
			return true;
		}
		c.toVisitVertices.remove(pntId);
		if (c.testIfVisited(pntId, false, true)) {
			throw new IllegalStateException();
//			return true;
		}
		c.visitedVertices.put(calculateRoutePointId(segment),  ALL_VERTICIES ? segment : null);
		int prevX = segment.getRoad().getPoint31XTile(segment.getSegmentStart());
		int prevY = segment.getRoad().getPoint31YTile(segment.getSegmentStart());
		int x = segment.getRoad().getPoint31XTile(segment.getSegmentEnd());
		int y = segment.getRoad().getPoint31YTile(segment.getSegmentEnd());

		float distFromStart = segment.distanceFromStart + (float) MapUtils.squareRootDist31(x, y, prevX, prevY);
		addSegment(c, distFromStart, segment, true, queue);
		addSegment(c, distFromStart, segment, false, queue);
		return true;
	}
	
	
	private void addSegment(BaseCluster c, float distFromStart, RouteSegment segment, boolean end, PriorityQueue<RouteSegment> queue) {
		int segmentInd = end ? segment.getSegmentEnd() : segment.getSegmentStart();
		int x = segment.getRoad().getPoint31XTile(segmentInd);
		int y = segment.getRoad().getPoint31YTile(segmentInd);
		RouteSegment next = ctx.loadRouteSegment(x, y, 0);
		while (next != null) {
			// next.distanceFromStart == 0
			if (!c.testIfVisited(calculateRoutePointId(next), true, true)) {
				next.distanceFromStart = distFromStart;
				if (queue != null) {
					queue.add(next);
				}
				c.toVisitVertices.put(calculateRoutePointId(next), next);
			}
			RouteSegment opp = next.initRouteSegment(!next.isPositive());
			if (opp != null && !c.testIfVisited(calculateRoutePointId(opp), true, true)) {
				opp.distanceFromStart = distFromStart;
				if (queue != null) {
					queue.add(opp);
				}
				c.toVisitVertices.put(calculateRoutePointId(opp), opp);
			}
			next = next.getNext();
		}
	}


	private static long calculateRoutePointInternalId(final RouteDataObject road, int pntId, int nextPntId) {
		int positive = nextPntId - pntId;
		int pntLen = road.getPointsLength();
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

	private static long calculateRoutePointId(RouteSegment segm) {
		if (segm.getSegmentStart() < segm.getSegmentEnd()) {
			return calculateRoutePointInternalId(segm.getRoad(), segm.getSegmentStart(), segm.getSegmentEnd());
		} else {
			return calculateRoutePointInternalId(segm.getRoad(), segm.getSegmentEnd(), segm.getSegmentStart());
		}
	}
	
	
	/////////////////////////////////////////////////////////////////////////////////////////////
	
	private static class NetworkPoint {
		long id;
		public long roadId;
		public int start;
		public int end;
		public int startX;
		public int startY;
	}
	private static class NetworkDB {

		private Connection conn;
		private static int BATCH = 1000;

		public NetworkDB(File file, boolean recreate) throws SQLException {
			if (file.exists() && recreate) {
				file.delete();
			}
			this.conn = DBDialect.SQLITE.getDatabaseConnection(file.getAbsolutePath(), LOG);
			if (recreate) {
				Statement st = conn.createStatement();
				st.execute("CREATE TABLE points(idPoint, roadId, start, end, x31, y31, lat, lon)");
				st.close();
			}
		}
		
		public TLongObjectHashMap<NetworkPoint> getNetworkPoints() throws SQLException {
			Statement st = conn.createStatement();
			ResultSet rs = st.executeQuery("SELECT idPoint, roadId, start, end, x31, y31 from points");
			TLongObjectHashMap<NetworkPoint> mp = new TLongObjectHashMap<>();
			while (rs.next()) {
				NetworkPoint pnt = new NetworkPoint();
				pnt.id = rs.getLong(1);
				pnt.roadId = rs.getLong(2);
				pnt.start = rs.getInt(3);
				pnt.end = rs.getInt(4);
				pnt.startX = rs.getInt(5);
				pnt.startY = rs.getInt(6);
				mp.put(pnt.id, pnt);
			}
			rs.close();
			st.close();
			return mp;
		}

		public void insertPoints(TLongObjectHashMap<RouteSegment> networkPoints) throws SQLException {
			PreparedStatement s = conn.prepareStatement("INSERT INTO points(idPoint, roadId, start, end, x31, y31, lat, lon) "
					+ " VALUES(?, ?, ?, ?, ?, ?, ?, ?)");
			int ind = 0;
			TLongObjectIterator<RouteSegment> it = networkPoints.iterator();
			while (it.hasNext()) {
				it.advance();
				int p= 1;
				s.setLong(p++, it.key());
				RouteSegment obj = it.value();
				LatLon pnt = getPoint(obj);
				s.setLong(p++, obj.getRoad().getId());
				s.setLong(p++, obj.getSegmentStart());
				s.setLong(p++, obj.getSegmentEnd());
				s.setInt(p++, obj.getRoad().getPoint31XTile(obj.getSegmentStart()));
				s.setInt(p++, obj.getRoad().getPoint31YTile(obj.getSegmentStart()));
				s.setDouble(p++, pnt.getLatitude());
				s.setDouble(p++, pnt.getLongitude());
				s.addBatch();
				if (ind++ > BATCH) {
					s.executeBatch();
					ind = 0;
				}
			}
			s.executeBatch();
			s.close();
		}

		public void close() throws SQLException {
			conn.close();
		}

	}
	
	private List<Entity> buildNetworkSegments(TLongObjectHashMap<NetworkPoint> pnts) throws InterruptedException, IOException {
		double sz = pnts.size() / 100.0;
		int ind = 0;
		long tm = System.currentTimeMillis();
		TLongObjectHashMap<RouteSegment> segments = new  TLongObjectHashMap<>(); 
		for (NetworkPoint pnt : pnts.valueCollection()) {
			segments.put(pnt.id, new RouteSegment(null, pnt.start, pnt.end));
			segments.put(calculateRoutePointInternalId(pnt.roadId, pnt.end, pnt.start), new RouteSegment(null, pnt.end, pnt.start));
		}
		List<Entity> osmObjects = new ArrayList<Entity>();
		int maxDirectedPointsGraph = 0;
		int maxFinalSegmentsFound = 0;
		int totalFinalSegmentsFound = 0;
		int totalVisitedDirectSegments = 0;
		for (NetworkPoint pnt : pnts.valueCollection()) {
			long nt = System.nanoTime();
			RouteSegment s = ctx.loadRouteSegment(pnt.startX, pnt.startY, ctx.config.memoryLimitation);
			while (s != null && s.getRoad().getId() != pnt.roadId && s.getSegmentStart() != pnt.start
					&& s.getSegmentEnd() != pnt.end) {
				s = s.getNext();
			}
			if (s == null) {
				System.err.println("Error on segment " + pnt.roadId / 64);
				continue;
			}
			long pnt1 = calculateRoutePointInternalId(pnt.roadId, pnt.end, pnt.start);
			long pnt2 = calculateRoutePointInternalId(pnt.roadId, pnt.start, pnt.end);
			RouteSegment rm1 = segments.remove(pnt1);
			RouteSegment rm2 = segments.remove(pnt2);
			runDijsktra(osmObjects, s, segments);
			if (rm1 != null) {
				segments.put(pnt1, rm1);
			}
			if (rm2 != null) {
				segments.put(pnt2, rm2);
			}
			maxDirectedPointsGraph = Math.max(maxDirectedPointsGraph, ctx.calculationProgress.visitedDirectSegments);
			totalVisitedDirectSegments += ctx.calculationProgress.visitedDirectSegments;
			maxFinalSegmentsFound = Math.max(maxFinalSegmentsFound, ctx.calculationProgress.finalSegmentsFound);
			totalFinalSegmentsFound += ctx.calculationProgress.finalSegmentsFound;
			
			double timeLeft = (System.currentTimeMillis() - tm) / 1000.0 * (pnts.size() / (ind + 1) - 1);
			System.out.println(String.format("%.2f%% Process %d  - %.1f ms left %.1f sec",
							ind++ / sz, s.getRoad().getId() / 64, (System.nanoTime() - nt) / 1.0e6, timeLeft));
			if (ind > LIMIT && LIMIT != -1) {
				break;
			}
		}
		System.out.println(String.format("Total segments %d: max sub graph %d, avg sub graph %d, max shortcuts %d, average shortcuts %d", 
				segments.size(), maxDirectedPointsGraph, totalVisitedDirectSegments  / ind, maxFinalSegmentsFound, totalFinalSegmentsFound / ind));
		return osmObjects;
	}



	private void runDijsktra(List<Entity> osmObjects, RouteSegment s, TLongObjectHashMap<RouteSegment> segments) throws InterruptedException, IOException {
		BinaryRoutePlanner brp = new BinaryRoutePlanner();
		ctx.unloadAllData();
		ctx.calculationProgress = new RouteCalculationProgress();
		
		LatLon l = getPoint(s); 
		Node n = new Node(l.getLatitude(), l.getLongitude(), ID--);
		n.putTag("highway", "stop");
		n.putTag("name", s.getRoad().getId() / 64 + " " + s.getSegmentStart() + " " + s.getSegmentEnd());
		osmObjects.add(n);
		ctx.startX = s.getRoad().getPoint31XTile(s.getSegmentStart());
		ctx.startY = s.getRoad().getPoint31YTile(s.getSegmentStart());
		MultiFinalRouteSegment frs = (MultiFinalRouteSegment) brp.searchRouteInternal(ctx, new RouteSegmentPoint(s.getRoad(), s.getSegmentStart(), 0), null, null, 
				segments);
		if (frs != null) {
			System.out.println(frs.others.size() + 1 + " --- ");
			addW(osmObjects, frs, l);
			for (RouteSegment o : frs.others) {
				addW(osmObjects, o, l);
			}
		}
		System.out.println(ctx.calculationProgress.getInfo(null));
		
	}



	private void addW(List<Entity> osmObjects, RouteSegment s, LatLon l) {
		Way w = new Way(ID--);
		w.putTag("highway", "secondary");
		w.addNode(new Node(l.getLatitude(), l.getLongitude(), ID--));
		LatLon m = getPoint(s);
		w.addNode(new Node(m.getLatitude(), m.getLongitude(), ID--));
		w.putTag("oid", (s.getRoad().getId() / 64) + "");
		osmObjects.add(w);
	}	
	
	
	

}
