package net.osmand.router.tester;

import net.osmand.data.LatLon;
import net.osmand.router.RouteSegmentResult;
import net.osmand.router.RoutingContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class RandomRouteEntry {
	LatLon start;
	LatLon finish;
	List<LatLon> via = new ArrayList<>(); // inter points

	String profile = "car";
	List<String> params = new ArrayList<>();

	List<RandomRouteResult> results = new ArrayList<>();

	Map<String, String> mapParams() {
		Map<String, String> map = new HashMap<>();
		params.forEach(p -> {
			String[] kv = p.split("[:=]"); // height_obstacles (true) or height:5 or height=5
			if (kv.length > 1) {
				map.put(kv[0], kv[1]); // key && value
			} else {
				map.put(kv[0], "true"); // key only (= "true")
			}
		});
		return map;
	}

	public String toString() {
		return toURL("osrm");
	}

	String toURL(String type) {
		String START = String.format("%f,%f", start.getLatitude(), start.getLongitude());
		String FINISH = String.format("%f,%f", finish.getLatitude(), finish.getLongitude());
		String TYPE = type == null ? "osmand" : type;
		String PROFILE = profile;
		String GO = String.format(
				"10/%f/%f",
				(start.getLatitude() + finish.getLatitude()) / 2,
				(start.getLongitude() + finish.getLongitude()) / 2
		);

		String hasVia = via.size() > 0 ? "&via=" : "";

		List<String> viaList = new ArrayList<>();
		via.forEach(ll -> viaList.add(String.format("%f,%f", ll.getLatitude(), ll.getLongitude())));
		String VIA = String.join(";", viaList);

		String hasParams = params.size() > 0 ? "&params=" : "";
		String PARAMS = String.join(",", params); // site will fix it to "profile,params"

		return String.format(
				"https://test.osmand.net/map/?start=%s&finish=%s%s%s&type=%s&profile=%s%s%s#%s",
				START, FINISH, hasVia, VIA, TYPE, PROFILE, hasParams, PARAMS, GO
		);
	}
}

class RandomRouteResult {
	String type;
	double cost;
	long runTime; // ms
	int visitedSegments;
	double distance; // meters
	RandomRouteEntry entry; // ref to the parent: start, finish, etc

	public RandomRouteResult(String type, RandomRouteEntry entry, long runTime,
	                         RoutingContext ctx, List<RouteSegmentResult> segments) {
		this.type = type;
		this.entry = entry;
		this.runTime = runTime;

		this.distance = 0;
		this.cost = ctx.routingTime;
		this.visitedSegments = ctx.calculationProgress.visitedSegments;

		if (segments != null) {
			for (RouteSegmentResult r : segments) {
				this.distance += r.getDistance();
			}
		}

		System.err.printf("%s (%d) cost=%f dist=%f\n", type, runTime, cost, distance);
	}

	public String toString() {
		return entry.toURL(type);
	}
}
