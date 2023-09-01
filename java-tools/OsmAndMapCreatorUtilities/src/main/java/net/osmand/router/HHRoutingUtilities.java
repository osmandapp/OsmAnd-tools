package net.osmand.router;

import static net.osmand.router.HHRoutingUtilities.DEBUG_OSM_ID;
import static net.osmand.router.HHRoutingUtilities.calculateRoutePointInternalId;
import static net.osmand.router.HHRoutingUtilities.getPoint;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import javax.xml.stream.XMLStreamException;

import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.set.TLongSet;
import gnu.trove.set.hash.TLongHashSet;
import net.osmand.binary.RouteDataObject;
import net.osmand.binary.BinaryMapRouteReaderAdapter.RouteTypeRule;
import net.osmand.data.LatLon;
import net.osmand.osm.edit.Entity;
import net.osmand.osm.edit.Node;
import net.osmand.osm.edit.Way;
import net.osmand.osm.io.OsmBaseStorage;
import net.osmand.osm.io.OsmStorageWriter;
import net.osmand.router.BinaryRoutePlanner.RouteSegment;
import net.osmand.router.HHRoutingPreparationDB.NetworkDBPoint;
import net.osmand.router.HHRoutingPreparationDB.NetworkDBSegment;
import net.osmand.util.MapUtils;

public class HHRoutingUtilities {
	static long DEBUG_OSM_ID = -1;
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


	static long calculateRoutePointInternalId(final RouteDataObject road, int pntId, int nextPntId) {
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
	
	static long calculateRoutePointInternalId(long id, int pntId, int nextPntId) {
		int positive = nextPntId - pntId;
		return (id << ROUTE_POINTS) + (pntId << 1) + (positive > 0 ? 1 : 0);
	}

	static long calculateRoutePointInternalId(RouteSegment segm) {
		if (segm.getSegmentStart() < segm.getSegmentEnd()) {
			return calculateRoutePointInternalId(segm.getRoad(), segm.getSegmentStart(), segm.getSegmentEnd());
		} else {
			return calculateRoutePointInternalId(segm.getRoad(), segm.getSegmentEnd(), segm.getSegmentStart());
		}
	}
	

	static void addWay(TLongObjectHashMap<Entity> osmObjects, RouteSegment s) {
		Way w = new Way(DEBUG_OSM_ID--);
		int[] tps = s.getRoad().getTypes();
		for (int i = 0; i < tps.length; i++) {
			RouteTypeRule rtr = s.getRoad().region.quickGetEncodingRule(tps[i]);
			w.putTag(rtr.getTag(), rtr.getValue());
		}
		int xt = s.getRoad().getPoint31XTile(s.getSegmentStart());
		int yt = s.getRoad().getPoint31YTile(s.getSegmentStart());
		w.addNode(new Node(MapUtils.get31LatitudeY(yt), MapUtils.get31LongitudeX(xt), DEBUG_OSM_ID--));
		int xs = s.getRoad().getPoint31XTile(s.getSegmentEnd());
		int ys = s.getRoad().getPoint31YTile(s.getSegmentEnd());
		w.addNode(new Node(MapUtils.get31LatitudeY(ys), MapUtils.get31LongitudeX(xs), DEBUG_OSM_ID--));
		w.putTag("oid", (s.getRoad().getId() / 64) + "");
		osmObjects.put(calculateRoutePointInternalId(s), w);
	}
	
	static void addWay(TLongObjectHashMap<Entity> osmObjects, NetworkDBSegment segment, String tag, String value) {
		Way w = new Way(DEBUG_OSM_ID--);
		w.putTag("name", String.format("%d -> %d %.1f", segment.start.index, segment.end.index, segment.dist));
		for (LatLon l : segment.geometry) {
			w.addNode(new Node(l.getLatitude(), l.getLongitude(), DEBUG_OSM_ID--));
		}
		osmObjects.put(w.getId(), w);
	}

	static void addNode(TLongObjectHashMap<Entity> osmObjects, NetworkDBPoint pnt, LatLon l, String tag, String val) {
		if (l == null) {
			l = pnt.getPoint();
		}
		Node n = new Node(l.getLatitude(), l.getLongitude(), DEBUG_OSM_ID--);
		n.putTag(tag, val);
		n.putTag("name", pnt.index + " " + pnt.roadId / 64 + " " + pnt.start + " " + pnt.end);
		osmObjects.put(pnt.id, n);
	}


	public  static List<Entity> visualizeWays(TLongObjectHashMap<RouteSegment> networkPoints, 
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
				Node n = new Node(l.getLatitude(), l.getLongitude(), DEBUG_OSM_ID--);
				n.putTag("highway", "stop");
				n.putTag("name", r.getRoad().getId() / 64 + " " + r.getSegmentStart() + " " + r.getSegmentEnd());
				objs.add(n);
				nodes++;
				if (networkPointsConnections != null && networkPointsConnections.containsKey(key)) {
					for (RouteSegment conn : networkPointsConnections.get(key)) {
						LatLon ln = getPoint(conn);
						Node n2 = new Node(ln.getLatitude(), ln.getLongitude(), DEBUG_OSM_ID--);
						objs.add(n2);
						Way w = new Way(DEBUG_OSM_ID--, Arrays.asList(n, n2));
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


	private static Way convertRoad(RouteDataObject road, int st, int end) {
		Way w = new Way(DEBUG_OSM_ID--);
		int[] tps = road.getTypes();
		for (int i = 0; i < tps.length; i++) {
			RouteTypeRule rtr = road.region.quickGetEncodingRule(tps[i]);
			w.putTag(rtr.getTag(), rtr.getValue());
		}
		w.putTag("oid", (road.getId() / 64) + "");
		while (true) {
			double lon = MapUtils.get31LongitudeX(road.getPoint31XTile(st));
			double lat = MapUtils.get31LatitudeY(road.getPoint31YTile(st));
			w.addNode(new Node(lat, lon, DEBUG_OSM_ID--));
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

	
	public static void saveOsmFile(Collection<Entity> objects, File file)
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
}
