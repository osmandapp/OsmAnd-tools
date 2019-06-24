package net.osmand.util;

import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.set.hash.TLongHashSet;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.binary.BinaryMapRouteReaderAdapter.RouteRegion;
import net.osmand.binary.BinaryMapRouteReaderAdapter.RouteSubregion;
import net.osmand.binary.RouteDataObject;
import net.osmand.router.BinaryRoutePlanner;
import net.osmand.router.BinaryRoutePlanner.RouteSegment;
import net.osmand.router.GeneralRouter;
import net.osmand.router.RoutePlannerFrontEnd;
import net.osmand.router.RoutePlannerFrontEnd.RouteCalculationMode;
import net.osmand.router.RoutingConfiguration;
import net.osmand.router.RoutingContext;
import net.osmand.router.RoutingContext.RoutingSubregionTile;

public class ManyToOneRoadCalculation {

	private static final int THRESHOLD_DISCONNECTED = 200;


	public class ManyToManySegment {
		public RouteDataObject road;
		public int segmentIndex;

		public double distanceFromStart = Double.POSITIVE_INFINITY;

		public ManyToManySegment next;
		public ManyToManySegment parentSegment;
		public int parentEndIndex;

		public double estimateDistanceEnd(GeneralRouter router, int sbottom) {
			return squareRootDist(0, road.getPoint31YTile(segmentIndex), 0, sbottom) / router.getMaxSpeed();
		}
	}

	public static void main(String[] args) throws IOException, InterruptedException {
		File fl = new File("/home/victor/projects/osmand/osm-gen/Netherlands_europe_2.obf");
		RandomAccessFile raf = new RandomAccessFile(fl, "r"); //$NON-NLS-1$ //$NON-NLS-2$
		BinaryMapIndexReader reader = new BinaryMapIndexReader(raf, fl);
		int zoom = 9;
		double top = 53.2949;
		double bottom = MapUtils.getLatitudeFromTile(zoom, (MapUtils.getTileNumberY(zoom, top) + 1));
		System.out.println(top +" - " + bottom);
		new ManyToOneRoadCalculation().manyToManyCalculation(reader, top, bottom/*, 51.48*/);

	}

	private void manyToManyCalculation(BinaryMapIndexReader reader, double top, double bottom) throws IOException {
		RoutePlannerFrontEnd frontEnd = new RoutePlannerFrontEnd();
		RoutingConfiguration config = RoutingConfiguration.getDefault().build("car", 1000);
		RouteCalculationMode mode = RouteCalculationMode.BASE;
		RoutingContext ctx = frontEnd.buildRoutingContext(config, null, new BinaryMapIndexReader[] {reader}, mode);

		RouteRegion reg = reader.getRoutingIndexes().get(0);
		List<RouteSubregion> baseSubregions = reg.getBaseSubregions();
		List<RoutingSubregionTile> tiles = new ArrayList<RoutingContext.RoutingSubregionTile>();
		for (RouteSubregion s : baseSubregions) {
			List<RoutingSubregionTile> loadTiles = ctx.loadAllSubregionTiles(reader, s);
			tiles.addAll(loadTiles);
		}
		int st = MapUtils.get31TileNumberY(top);
		int sb = MapUtils.get31TileNumberY(bottom);

		List<ManyToManySegment> topIntersects = new ArrayList<ManyToManySegment>();
		List<ManyToManySegment> bottomIntersects = new ArrayList<ManyToManySegment>();
		TLongObjectHashMap<ManyToManySegment> allSegments = initSegments(st, sb, ctx, tiles, topIntersects, bottomIntersects);
		filterDisconnected(ctx, allSegments, topIntersects);
		filterDisconnected(ctx, allSegments, topIntersects);
		System.out.println("TOP " + topIntersects.size());
		System.out.println("BOTTOM " + bottomIntersects.size());

		calculateManyToMany(ctx, allSegments, topIntersects, bottomIntersects, st, sb);

	}

	private void filterDisconnected(RoutingContext ctx, TLongObjectHashMap<ManyToManySegment> allSegments,
			List<ManyToManySegment> initialSegments) {
		Iterator<ManyToManySegment> it = initialSegments.iterator();
		while(it.hasNext()) {
			ManyToManySegment init = it.next();
			int iterations = 0;
			int threshold = THRESHOLD_DISCONNECTED;
			LinkedList<ManyToManySegment> mms = new LinkedList<ManyToManySegment>();
			TLongHashSet visited = new TLongHashSet();
			mms.push(init);
			while(iterations < threshold && !mms.isEmpty()) {
				ManyToManySegment o = mms.poll();
				if(!visited.add(o.road.id)) {
					continue;
				}
				int ow = ctx.config.router.isOneWay(o.road);
				int start = ow > 0 ? o.segmentIndex : 0;
				int end = ow < 0 ? o.segmentIndex : o.road.getPointsLength();
				for(int i = start; i < end; i++) {
					long calcLong = calcLong(o.road.getPoint31XTile(i), o.road.getPoint31YTile(i));
					ManyToManySegment ind = allSegments.get(calcLong);
					while(ind != null) {
						if(!visited.contains(ind.road.id)) {
							mms.push(ind);
						}
						ind = ind.next;
					}
				}
				iterations ++;
			}
			if(iterations < threshold) {
				it.remove();
			}
		}
	}

	public static double squareRootDist(int x1, int y1, int x2, int y2) {
		return MapUtils.squareRootDist31(x1, y1, x2, y2);
	}

	private void calculateManyToMany(RoutingContext ctx, TLongObjectHashMap<ManyToManySegment> allSegments,
			List<ManyToManySegment> topIntersects, List<ManyToManySegment> bottomIntersects, int stop, final int sbottom) {
		final GeneralRouter router = ctx.config.router;
		float DISTANCE_THRESHOLD = 50000;
		// TODO depth search from one top intersect
		List<TLongArrayList> sets = new ArrayList<TLongArrayList>();
		for (int i = 0; i < topIntersects.size(); i++) {
			ManyToManySegment oneTop = topIntersects.get(i);
			clearAllSegments(allSegments);
			List<ManyToManySegment> finalSegmentResult = calculateOneToMany(allSegments, bottomIntersects, sbottom, router,
					oneTop);
			for (ManyToManySegment fnsResult : finalSegmentResult) {
				TLongArrayList set = convertToRoadIds(fnsResult, DISTANCE_THRESHOLD/router.getMaxSpeed());
				combineWithLocal(sets, set);
			}
			System.out.println(oneTop.road.getHighway() + " " + oneTop.road.id + " " + oneTop.segmentIndex + " common ways="+sets.size());

		}


		System.out.println(sets.size());
		for(TLongArrayList s : sets) {
			System.out.println(s);
		}

	}

	private void combineWithLocal(List<TLongArrayList> sets, TLongArrayList source) {
		boolean found = false;
		TLongHashSet set = new TLongHashSet(source);
		for (TLongArrayList oneList : sets) {
			int k = 0;
			for(; k < oneList.size(); k++) {
				if(set.contains(oneList.get(k))){
					break;
				}
			}
			if(k < oneList.size()) {
				oneList.remove(0, k);
				for(k = 0; k < oneList.size(); k++) {
					if(!set.contains(oneList.get(k))){
						break;
					}
				}
				if(k < oneList.size()) {
					oneList.remove(k, oneList.size() - k);
				}
				found = true;
			}
			if(found) {
				break;
			}
		}
		if (!found) {
			sets.add(source);
		}
	}

	private void clearAllSegments(TLongObjectHashMap<ManyToManySegment> allSegments) {
		for(ManyToManySegment m : allSegments.valueCollection()) {
			ManyToManySegment mp = m;
			while(mp != null) {
				mp.parentEndIndex = 0;
				mp.parentSegment = null;
				mp.distanceFromStart = Double.POSITIVE_INFINITY;
				mp = mp.next;
			}
		}

	}


	private List<ManyToManySegment> calculateOneToMany(TLongObjectHashMap<ManyToManySegment> allSegments,
			List<ManyToManySegment> bottomIntersects, final int sbottom, final GeneralRouter router,
			ManyToManySegment oneTop) {
		if(router.isOneWay(oneTop.road) > 0 && oneTop.segmentIndex == oneTop.road.getPointsLength() - 1) {
			int s = oneTop.segmentIndex - 1;
			long key = calcLong(oneTop.road.getPoint31XTile(s), oneTop.road.getPoint31YTile(s));
			oneTop = find(allSegments.get(key), oneTop.road.id);
		} else if(router.isOneWay(oneTop.road) < 0 && oneTop.segmentIndex == 0) {
			int s = oneTop.segmentIndex + 1;
			long key = calcLong(oneTop.road.getPoint31XTile(s), oneTop.road.getPoint31YTile(s));
			oneTop = find(allSegments.get(key), oneTop.road.id);
		}
		PriorityQueue<ManyToManySegment> queue = new PriorityQueue<ManyToManySegment>(100, new Comparator<ManyToManySegment>() {

			@Override
			public int compare(ManyToManySegment arg0, ManyToManySegment arg1) {
//				return Double.compare(arg0.distanceFromStart , arg1.distanceFromStart );
//				return Double.compare(arg0.distanceFromStart + arg0.estimateDistanceEnd(router, sbottom),
//						arg1.distanceFromStart + arg1.estimateDistanceEnd(router, sbottom));
				return Double.compare(arg0.distanceFromStart, arg1.distanceFromStart);
			}
		});
		TLongHashSet finalSegments = new TLongHashSet();
		for(ManyToManySegment fs : bottomIntersects) {
			finalSegments.add(fs.road.id);
		}
		oneTop.distanceFromStart = 0;
		//oneTop.estimateDistanceToEnd = squareRootDist(0, stop, 0, sbottom) / router.getMaxSpeed();
		queue.add(oneTop);
		TLongHashSet visitedSegments = new TLongHashSet();
		List<ManyToManySegment> finalSegmentResult = new ArrayList<ManyToOneRoadCalculation.ManyToManySegment>();
		while(!queue.isEmpty()) {
			ManyToManySegment seg = queue.poll();
			if(finalSegments.contains(seg.road.id)) {
				finalSegmentResult.add(seg);
				finalSegments.remove(seg.road.id);
				if(finalSegments.size() == 0) {
					break;
				} else {
					continue;
				}
			}
			int oneWay = router.isOneWay(seg.road);
			if(oneWay >= 0) {
				processRoadSegment(queue, router, sbottom, seg, true, allSegments, visitedSegments);
			}
			if(oneWay <= 0) {
				processRoadSegment(queue, router, sbottom, seg, false, allSegments, visitedSegments);
			}
			//visitedRoads++;
		}

		return finalSegmentResult;
	}


	private ManyToManySegment find(ManyToManySegment ms, long id) {
		while(ms != null && ms.road.id != id) {
			ms = ms.next;
		}
		return ms;
	}

	private TLongArrayList convertToRoadIds(ManyToManySegment fnsResult, float distanceFromStart) {
		TLongArrayList set = new TLongArrayList();
		ManyToManySegment ms = fnsResult;
		while(ms != null){
			set.add(ms.road.id);
			ms = ms.parentSegment;
			if(ms.distanceFromStart < distanceFromStart) {
				break;
			}
		}

		return set;
	}


	private void processRoadSegment(PriorityQueue<ManyToManySegment> queue, GeneralRouter router, int sbottom,
			ManyToManySegment seg, boolean direction, TLongObjectHashMap<ManyToManySegment> allSegments, TLongHashSet visitedSegments) {
		int p = seg.segmentIndex;
		double dist = 0;
		double speed = router.defineRoutingSpeed(seg.road);
		boolean continueMovement = true;
		while(continueMovement) {
			int pp = p;
			visitedSegments.add(calcSegmentId(seg));
			p = direction ? p + 1 : p - 1;
			if(p >= seg.road.getPointsLength() || p < 0) {
				break;
			}
			//	System.out.println("Visit " + seg.road.id + " " + p + " (" + seg.road.getPointsLength()+") ");
			int px = seg.road.getPoint31XTile(p);
			int py = seg.road.getPoint31YTile(p);
			dist += squareRootDist(seg.road.getPoint31XTile(pp), seg.road.getPoint31YTile(pp), px, py);
			long key = calcLong(px, py);
			ManyToManySegment sgs = allSegments.get(key);
			double distFromStart = seg.distanceFromStart + dist / speed;
			while(sgs != null) {
				// System.out.println("Connected to " + sgs.road.id + " " + sgs.segmentIndex + " " + sgs.road.getHighway());
				boolean visited = visitedSegments.contains(calcSegmentId(sgs));
				if(sgs.road != seg.road){
					boolean viewed = !Double.isInfinite(sgs.distanceFromStart);
					if((!viewed || sgs.distanceFromStart > distFromStart)) {
						if (visited) {
							if (sgs.distanceFromStart > distFromStart * 1.1) {
								System.err.println("Prev " + sgs.distanceFromStart + " ? current " + distFromStart
										+ " " + seg.distanceFromStart + " " + sgs.road.id + " : prev "
										+ sgs.parentSegment.road.id + " current " + seg.road.id);
							}
						} else {
							if (viewed) {
								queue.remove(sgs);
							}
							// sgs.estimateDistanceToEnd = estEnd;
							if(sgs.distanceFromStart < distFromStart) {
								throw new IllegalArgumentException();
							}
							sgs.distanceFromStart = distFromStart;
							sgs.parentEndIndex = pp;
							sgs.parentSegment = seg;
							queue.add(sgs);
						}
					}
				} else {
					// same road id
					if (sgs.distanceFromStart > distFromStart) {
						if (visited) {
							System.err.println("!Prev " + sgs.distanceFromStart + " ? current " + distFromStart
									+ " " + seg.distanceFromStart + " " + sgs.road.id + " : prev "
									+ sgs.parentSegment.road.id + " current " + seg.road.id);
						} else {
							sgs.parentSegment = seg.parentSegment;
							sgs.distanceFromStart = distFromStart;
						}
					} else {
						continueMovement = false;
					}
				}
				sgs = sgs.next;
			}
		}
	}

	private long calcSegmentId(ManyToManySegment seg) {
		return (seg.road.id << 10) + seg.segmentIndex;
	}

	private TLongObjectHashMap<ManyToManySegment> initSegments(int stop, int sbottom, RoutingContext ctx, List<RoutingSubregionTile> tiles,
			List<ManyToManySegment> topIntersects, List<ManyToManySegment> bottomIntersects) {
		TLongObjectHashMap<ManyToManySegment> res = new TLongObjectHashMap<ManyToManySegment>();
		List<RouteDataObject> startObjects = new ArrayList<RouteDataObject>();
		for (RoutingSubregionTile st : tiles) {
			if (st.subregion.top <= sbottom && st.subregion.bottom >= stop) {
				ctx.loadSubregionTile(st, false, startObjects, null);
			}
		}
		System.out.println("Roads in layer " + startObjects.size());
		for(RouteDataObject ro : startObjects ){
			boolean topCheck = false, bottomCheck = false;
			for(int i = 0; i < ro.getPointsLength(); i++) {
				ManyToManySegment sg = new ManyToManySegment();
				sg.road = ro;
				sg.segmentIndex = i;
				int px = ro.getPoint31XTile(i);
				int py = ro.getPoint31YTile(i);
				if (i > 0) {
					int prevX = ro.getPoint31XTile(i - 1);
					int prevY = ro.getPoint31YTile(i - 1);
					if (checkIntersection(prevX, prevY, px, py, 0, Integer.MAX_VALUE, stop, stop) && !topCheck) {
						topIntersects.add(sg);
						topCheck = true;
					}
					if (checkIntersection(prevX, prevY, px, py, 0, Integer.MAX_VALUE, sbottom, sbottom) && !bottomCheck) {
						bottomIntersects.add(sg);
						bottomCheck = true;
					}
				}
				long key = calcLong(px, py);
				ManyToManySegment sm = res.get(key);
				if(sm != null) {
					while(sm.next != null) {
						sm = sm.next;
					}
					sm.next = sg;
				} else {
					res.put(key, sg);
				}

			}
		}
		return res;
	}

	private boolean checkIntersection(int prevx, int prevy, int px, int py, int l, int r, int t, int b) {
		int pxin = prevx <= l ? -1 : (prevx >= r ? 1 : 0);
		int pyin = prevy <= t ? -1 : (prevy >= b ? 1 : 0);
		int xin = px <= l ? -1 : (px >= r ? 1 : 0);
		int yin = py <= t ? -1 : (py >= b ? 1 : 0);
		// this check could give wrong intersections
		// when the segment is out of the box imagenary crosses diagonal
		boolean intersectX = xin != pxin && (yin != pyin || yin == 0);
		boolean intersectY = yin != pyin && (xin != pxin || xin == 0);
		if (intersectY || intersectX) {
			return true;
		}
		return false;
	}

	private long calcLong(int x31, int y31) {
		return (((long) x31) << 31) + (long) y31;
	}


	private void cut(BinaryMapIndexReader reader) throws IOException {
		RoutePlannerFrontEnd frontEnd = new RoutePlannerFrontEnd();
		RoutingConfiguration config = RoutingConfiguration.getDefault().build("car", 1000);
		RouteCalculationMode mode = RouteCalculationMode.BASE;
		RoutingContext ctx = frontEnd.buildRoutingContext(config, null, new BinaryMapIndexReader[] {reader}, mode);

		RouteRegion reg = reader.getRoutingIndexes().get(0);
		List<RouteSubregion> baseSubregions = reg.getBaseSubregions();
		List<RoutingSubregionTile> tiles = new ArrayList<RoutingContext.RoutingSubregionTile>();
		for (RouteSubregion s : baseSubregions) {
			List<RoutingSubregionTile> loadTiles = ctx.loadAllSubregionTiles(reader, s);
			tiles.addAll(loadTiles);
		}
		int zoom = 9;
		int ty = (int) MapUtils.getTileNumberY(zoom, reg.getTopLatitude());
		int by = (int) MapUtils.getTileNumberY(zoom, reg.getBottomLatitude()) + 1;
		int lx = (int) MapUtils.getTileNumberX(zoom, reg.getLeftLongitude());
		int rx = (int) MapUtils.getTileNumberX(zoom, reg.getRightLongitude()) + 1;
		for(int ky = ty + 1; ky < by; ky++) {
			for(int kx = lx + 1 ; kx < rx; kx++) {
				cutByQuadrant((kx - 1) << (31 - zoom),
						(ky - 1) << (31 - zoom), kx << (31 - zoom) , ky << (31 - zoom) , ctx, tiles);
			}
		}
	}



	private void cutByQuadrant(int px, int py, int sx, int sy, RoutingContext ctx, List<RoutingSubregionTile> tiles) {
		int bc = 0;
		int rc = 0;
		Map<String, Integer> counts = new LinkedHashMap<String, Integer>();
		for(RoutingSubregionTile st : tiles) {
			if(st.subregion.left <= sx && st.subregion.right >= px &&
					st.subregion.top <= sy && st.subregion.bottom >= py){
				List<RouteDataObject> startObjects = new ArrayList<RouteDataObject>();
				ctx.loadSubregionTile(st, false, startObjects, null);

				List<RouteSegment> res = filterIntersections(px, sx, sy, sy, startObjects);
				bc += res.size();
				updateCounts(counts, res);
				res = filterIntersections(sx, sx, py, sy, startObjects);
				rc += res.size();
				updateCounts(counts, res);
			}
		}

		if (bc + rc > 0) {
			System.out.println("Q "
					+ ((float) MapUtils.get31LatitudeY(sy) + " " + (float) MapUtils.get31LongitudeX(sx)) + " \t  B="
					+ bc + " R=" + rc + " " + counts);
		}
	}

	private void updateCounts(Map<String, Integer> counts, List<RouteSegment> res) {
		for(RouteSegment r : res) {
			String key = r.getRoad().getHighway();
			if(key == null) {
				key = r.getRoad().getRoute();
			}
			Integer c = counts.get(key);
			if(c == null) {
				c = 0;
			}
			counts.put(key, (Integer)(++c));
		}
	}



	private List<RouteSegment> filterIntersections(int l, int r, int t, int b, List<RouteDataObject> startObjects) {
		List<RouteSegment> intersections = new ArrayList<BinaryRoutePlanner.RouteSegment>();
		for(RouteDataObject rdo : startObjects) {
			if(rdo == null) {
				continue;
			}
			int pxin = 0, pyin = 0;
			object : for(int i = 0; i < rdo.getPointsLength(); i++) {
				int x = rdo.getPoint31XTile(i);
				int y = rdo.getPoint31YTile(i);
				int xin = x <= l ? -1 : (x >= r ? 1 : 0);
				int yin = y <= t ? -1 : (y >= b ? 1 : 0);
				if(i > 0) {
					// this check could give wrong intersections
					// when the segment is out of the box imagenary crosses diagonal
					boolean intersectX = xin != pxin && (yin != pyin || yin == 0 );
					boolean intersectY = yin != pyin && (xin != pxin || xin == 0 );
					if(intersectY || intersectX) {
						intersections.add(new RouteSegment(rdo, rdo.getOneway() >= 0 ? i - 1 : i));
						break object;
					}
				}
				pxin = xin;
				pyin = yin;
			}
		}
		return intersections;
	}


	private List<ManyToManySegment> filterIntersectionSegments(int l, int r, int t, int b, List<RouteDataObject> startObjects) {
		List<ManyToManySegment> intersections = new ArrayList<ManyToManySegment>();
		for(RouteDataObject rdo : startObjects) {
			if(rdo == null) {
				continue;
			}
			int pxin = 0, pyin = 0;
			object : for(int i = 0; i < rdo.getPointsLength(); i++) {
				int x = rdo.getPoint31XTile(i);
				int y = rdo.getPoint31YTile(i);
				int xin = x <= l ? -1 : (x >= r ? 1 : 0);
				int yin = y <= t ? -1 : (y >= b ? 1 : 0);
				if(i > 0) {
					// this check could give wrong intersections
					// when the segment is out of the box imagenary crosses diagonal
					boolean intersectX = xin != pxin && (yin != pyin || yin == 0 );
					boolean intersectY = yin != pyin && (xin != pxin || xin == 0 );
					if(intersectY || intersectX) {
						ManyToManySegment segment = new ManyToManySegment();
						segment.segmentIndex = rdo.getOneway() >= 0 ? i - 1 : i;
						segment.road = rdo;
						intersections.add(segment);
						break object;
					}
				}
				pxin = xin;
				pyin = yin;
			}
		}
		return intersections;
	}



}
