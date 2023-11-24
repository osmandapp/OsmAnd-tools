package net.osmand.router;

import static net.osmand.router.HHRoutePlanner.calcUniDirRoutePointInternalId;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import javax.xml.stream.XMLStreamException;

import gnu.trove.iterator.TIntIntIterator;
import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.set.TLongSet;
import gnu.trove.set.hash.TLongHashSet;
import net.osmand.binary.BinaryMapRouteReaderAdapter.RouteTypeRule;
import net.osmand.binary.RouteDataObject;
import net.osmand.data.LatLon;
import net.osmand.data.QuadRect;
import net.osmand.osm.edit.Entity;
import net.osmand.osm.edit.Node;
import net.osmand.osm.edit.Way;
import net.osmand.osm.io.OsmBaseStorage;
import net.osmand.osm.io.OsmStorageWriter;
import net.osmand.router.BinaryRoutePlanner.RouteSegment;
import net.osmand.router.HHRoutingDB.NetworkDBPoint;
import net.osmand.router.HHRoutingDB.NetworkDBSegment;
import net.osmand.router.HHRoutingSubGraphCreator.NetworkIsland;
import net.osmand.router.HHRoutingSubGraphCreator.RouteSegmentBorderPoint;
import net.osmand.router.HHRoutingSubGraphCreator.RouteSegmentVertex;
import net.osmand.util.MapUtils;

public class HHRoutingUtilities {
	static long DEBUG_OSM_ID = -1;
	static long DEBUG_START_TIME = 0;

	public static void logf(String string, Object... a) {
		if (DEBUG_START_TIME == 0) {
			DEBUG_START_TIME = System.currentTimeMillis();
		}
		String ms = String.format("%3.1fs ", (System.currentTimeMillis() - DEBUG_START_TIME) / 1000.f);
		System.out.printf(ms + string + "\n", a);
	}
	
	static LatLon getPoint(RouteSegment r) {
		double lon = MapUtils.get31LongitudeX(r.getRoad().getPoint31XTile(r.getSegmentStart(), r.getSegmentEnd()));
		double lat = MapUtils.get31LatitudeY(r.getRoad().getPoint31YTile(r.getSegmentStart(), r.getSegmentEnd()));
		return new LatLon(lat, lon);
	}
	
	static RouteSegment makePositiveDir(RouteSegment next) {
		return next == null ? null : next.isPositive() ? next : 
			new RouteSegment(next.getRoad(), next.getSegmentEnd(), next.getSegmentStart());
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
		osmObjects.put(calcUniDirRoutePointInternalId(s), w);
	}
	
	public static void addWay(TLongObjectHashMap<Entity> osmObjects, NetworkDBSegment segment, String tag, String value) {
		if (segment.geom == null || segment.geom.isEmpty()) {
			return;
		}
		List<LatLon> geometry = segment.getGeometry();
		Way w = new Way(DEBUG_OSM_ID--);
		w.putTag("name", String.format("%d -> %d %.1f", segment.start.index, segment.end.index, segment.dist));
		for (LatLon l : geometry) {
			w.addNode(new Node(l.getLatitude(), l.getLongitude(), DEBUG_OSM_ID--));
		}
		osmObjects.put(w.getId(), w);
	}
	
	public static void addWay(TLongObjectHashMap<Entity> osmObjects, RouteSegmentResult segment, String tag, String value) {
		Way w = new Way(DEBUG_OSM_ID--);
		// w.putTag("name", String.format("%d -> %d %.1f", segment.getSegmentStart(), segment.getSegmentEnd(), segment.getDistanceFromStart()));
		int i = segment.getStartPointIndex();
		boolean pos = segment.getStartPointIndex() < segment.getEndPointIndex();
		while (true) {
			int x = segment.getObject().getPoint31XTile(i);
			int y = segment.getObject().getPoint31YTile(i);
			w.addNode(new Node(MapUtils.get31LatitudeY(y), MapUtils.get31LongitudeX(x), DEBUG_OSM_ID--));
			if (i == segment.getEndPointIndex()) {
				break;
			}
			i += pos ? 1 : -1;
		}
		osmObjects.put(w.getId(), w);
	}
	
	public static void addWay(TLongObjectHashMap<Entity> osmObjects, RouteSegment segment, String tag, String value) {
		Way w = new Way(DEBUG_OSM_ID--);
		// w.putTag("name", String.format("%d -> %d %.1f", segment.getSegmentStart(), segment.getSegmentEnd(), segment.getDistanceFromStart()));
		int i = segment.getSegmentStart();
		boolean pos = segment.getSegmentStart() < segment.getSegmentEnd();
		while (true) {
			int x = segment.getRoad().getPoint31XTile(i);
			int y = segment.getRoad().getPoint31YTile(i);
			w.addNode(new Node(MapUtils.get31LatitudeY(y), MapUtils.get31LongitudeX(x), DEBUG_OSM_ID--));
			if (i == segment.getSegmentEnd()) {
				break;
			}
			i += pos ? 1 : -1;
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
		n.putTag("clusterId", pnt.clusterId+"");
		n.putTag("index", pnt.index+"");
		if(pnt.dualPoint != null) {
			n.putTag("dualClusterId", pnt.dualPoint.clusterId+"");
			n.putTag("dualPoint", pnt.dualPoint.index +"");
		}
		osmObjects.put(pnt.getGeoPntId(), n);
	}


	public static List<Entity> visualizeClusters(List<NetworkIsland> visualClusters) {
		List<Entity> objs = new ArrayList<>();
		int clusters = 0;
		int allCenterRoads = 0, allShortcuts = 0;
		if (visualClusters == null) {
			return objs;
		}
		int allRoads = visualizeAllNodes(visualClusters, objs);

		TLongObjectHashMap<Node> pnts = new TLongObjectHashMap<>();
		for (NetworkIsland island : visualClusters) {
			LatLon l = island.startLatLon;
			Node startNode = new Node(l.getLatitude(), l.getLongitude(), DEBUG_OSM_ID--);
			startNode.putTag("highway", "traffic_signals");
			startNode.putTag("name", island.startToString);
			startNode.putTag("clusterId", island.dbIndex+"");
			objs.add(startNode);
			startNode.putTag("borderPoints", island.borderVertices.size() + "");
			
			clusters++;
			allShortcuts += island.borderVertices.size() * (island.borderVertices.size() - 1);
			allCenterRoads += island.borderVertices.size();
			for (RouteSegmentBorderPoint bp : island.borderVertices) {
				double lon = MapUtils.get31LongitudeX(bp.sx / 2 + bp.ex / 2);
				double lat = MapUtils.get31LatitudeY(bp.sy / 2 + bp.ey / 2);
				Node bNode = new Node(lat, lon, DEBUG_OSM_ID--);
				bNode.putTag("highway", "stop");
				bNode.putTag("dir", bp.isPositive() +"");
				bNode.putTag("unidir", bp.unidirId+"");
				bNode.putTag("name", bp.toString());
				bNode.putTag("clusterId", island.dbIndex+"");
				pnts.put(bp.uniqueId, bNode);
				
				if (island.visualBorders == null) {
					Way w = new Way(DEBUG_OSM_ID--, Arrays.asList(startNode, bNode));
					w.putTag("highway", "track");
					objs.add(w);
				}
			}
			if (island.visualBorders == null) {
				continue;
			}
			TLongObjectIterator<List<LatLon>> it = island.visualBorders.iterator();
			while (it.hasNext()) {
				it.advance();
				long pid = it.key();
				List<LatLon> lls = it.value();
				if (lls.size() > 0) {
					List<Node> nodesList = new ArrayList<Node>();
					nodesList.add(startNode);
					for (int i = lls.size() - 2; i >= 0; i--) {
						LatLon ln = lls.get(i);
						Node n2 = new Node(ln.getLatitude(), ln.getLongitude(), DEBUG_OSM_ID--);
						objs.add(n2);
						nodesList.add(n2);
					}
					if (!pnts.containsKey(pid)) {
						LatLon ln = lls.get(0);
						Node n2 = new Node(ln.getLatitude(), ln.getLongitude(), DEBUG_OSM_ID--);
						objs.add(n2);
						pnts.put(pid, n2);
					}
					if (lls.size() > 1) {
						nodesList.add(pnts.get(pid));
						Way w = new Way(DEBUG_OSM_ID--, nodesList);
						w.putTag("highway", "track");
						objs.add(w);
					}
					nodesList.clear();
					nodesList.add(startNode);
					nodesList.add(pnts.get(pid));
					Way w = new Way(DEBUG_OSM_ID--, nodesList);
					w.putTag("highway", "motorway");
					objs.add(w);
				}
			}
		}

		System.out.printf("Total visited roads %d, total clusters %d, total segments %d (from centers) - 2x border points, total segments %d between border points \n", 
				allRoads,clusters, allCenterRoads, allShortcuts);
		return objs;
	}

	private static int visualizeAllNodes(List<NetworkIsland> visualClusters, List<Entity> objs) {
		boolean merge = false;
		int ways1 = 0;
		for (NetworkIsland ni : visualClusters) {
			TLongSet viewed = new TLongHashSet();
			if (ni.visitedVertices == null) {
				continue;
			}
			TLongObjectIterator<RouteSegmentVertex> it = ni.visitedVertices.iterator();
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
					if (merge) {
						while ((segmentEnd + d) >= 0 && (segmentEnd + d) < s.getRoad().getPointsLength()) {
							RouteSegment nxt = new RouteSegment(s.getRoad(), segmentEnd, segmentEnd + d);
							pntKey = calcUniDirRoutePointInternalId(nxt);
							if (!ni.visitedVertices.containsKey(pntKey) || viewed.contains(pntKey)) {
								break;
							}
							viewed.add(pntKey);
							segmentEnd += d;
						}
					}
					Way w = convertRoad(s.getRoad(), s.getSegmentStart(), segmentEnd);
					if (s.distanceToEnd > 0) {
						w.putTag("dist", "" + (int) s.distanceToEnd);
					}
					objs.add(w);
					ways1++;
				}
			}
		}
		return ways1;
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
	
	

	public static int distrSum(TIntIntHashMap mp, int maxEl) {
		int k = 0;
		TIntIntIterator it = mp.iterator();
		while (it.hasNext()) {
			it.advance();
			if (it.key() < maxEl || maxEl < 0) {
				k += it.value();
			}
		}
		return k;
	}
	
	public static int distrCumKey(TIntIntHashMap mp, int maxEl) {
		int sum = 0;
		int[] keys = mp.keys();
		Arrays.sort(keys);
		for (int k : keys) {
			sum += mp.get(k);
			if (sum > maxEl) {
				return k;
			}
		}
		return -1;
	}
	
	public static String distrString(TIntIntHashMap distr, String suf) {
		return distrString(distr, suf, false, false, 3);
	}
	
	public static String distrString(TIntIntHashMap distr, String suf,  
			boolean cum, boolean valPrint, double percentMin) {
		int[] keys = distr.keys();
		Arrays.sort(keys);
		StringBuilder b = new StringBuilder();
		int sum = 0;
		for (int k = 0; k < keys.length; k++) {
			sum += distr.get(keys[k]);
		}
		b.append(String.format("%,d", sum));
		int k = 0, val = 0, prevVal = 0;
		double percent = 0;
		int kMin = -1;
		for (; k < keys.length; k++) {
			if (kMin < 0) {
				kMin = keys[k];
			}
			val += distr.get(keys[k]);
			percent = val * 100.0 / sum;
			if ((percent - prevVal * 100.0 / sum) >= percentMin) {
				String d = !valPrint ? (int)percent + "%" : val + "";
				String range = keys[k] + "";
				if (!cum && kMin != keys[k]) {
					range = kMin + "-" + range;
				}
				b.append(", ").append(range).append(suf).append(" - " + d);
				if (!cum) {
					val = 0;
				} else {
					prevVal = val;
				}
				kMin = -1;
			}
		}
		if (percent < percentMin && keys.length > 0) {
			b.append("... " + keys[keys.length - 1]);
		}
		return b.toString();
	}

	public static QuadRect expandLatLonRect(QuadRect qr, double left, double top, double right, double bottom) {
		if (qr == null) {
			qr = new QuadRect(left, top, right, bottom);
		}
		if (left < qr.left) {
			qr.left = left;
		}
		if (right > qr.right) {
			qr.right = right;
		}
		if (top > qr.top) {
			qr.top = top;
		}
		if (bottom < qr.bottom) {
			qr.bottom = bottom;
		}
		return qr;
	}



	static double testGetDist(RouteSegment t, boolean precise) {
		int px = 0, py = 0;
		double d = 0;
		while (t != null) {
			int sx = t.getRoad().getPoint31XTile(t.getSegmentStart(), t.getSegmentEnd());
			int sy = t.getRoad().getPoint31YTile(t.getSegmentStart(), t.getSegmentEnd());
			t = t.getParentRoute();
			if (px != 0) {
				if (precise) {
					d += MapUtils.measuredDist31(px, py, sx, sy);
				} else {
					d += MapUtils.squareRootDist31(px, py, sx, sy);
				}
			}
			px = sx;
			py = sy;
		}
		return d;
	}
	
	static List<LatLon> testGeometry(RouteSegment t) {
		List<LatLon> l = new ArrayList<LatLon>();
		while (t != null) {
			LatLon p = getPoint(t);
			l.add(p);
			t = t.getParentRoute();
		}
		return l;
	}

	static String testGetGeometry(List<LatLon> ls) {
		StringBuilder b = new StringBuilder();
		b.append("<?xml version='1.0' encoding='UTF-8' standalone='yes' ?>");
		b.append("<gpx version=\"1.1\" ><trk><trkseg>");
		for (LatLon p : ls) {
			b.append(String.format("<trkpt lat=\"%.6f\" lon=\"%.6f\"/> ", p.getLatitude(), p.getLongitude()));
		}
		b.append("</trkseg></trk></gpx>");
		return b.toString();
	}

	
}
