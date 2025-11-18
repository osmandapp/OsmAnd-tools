package net.osmand.router.tester;

import net.osmand.binary.RouteDataObject;
import net.osmand.data.LatLon;
import net.osmand.router.RouteSegmentResult;
import net.osmand.router.RoutingContext;
import net.osmand.router.TransportRoutePlanner;
import net.osmand.router.TransportRouteResult;
import net.osmand.util.MapUtils;

import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

class RandomRouteEntry {
	LatLon start;
	LatLon finish;
	List<LatLon> via = new ArrayList<>(); // inter points

	String profile = "car";
	List<String> params = new ArrayList<>();

	List<RandomRouteResult> routeResults = new ArrayList<>();
	List<RandomRouteResult> transportResults = new ArrayList<>();

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

	public boolean isPublicTransport() {
		return RandomRouteTester.PUBLIC_TRANSPORT_PROFILE.equals(profile);
	}

	public String toString() {
		return toURL("osmand");
	}

	String toURL(String type) {
		return toURL(type, "test.osmand.net", false);
	}

	String toURL(String type, String domain, boolean car2phase) {
		String START = String.format("%f,%f", start.getLatitude(), start.getLongitude());
		String FINISH = String.format("%f,%f", finish.getLatitude(), finish.getLongitude());

		String TYPE = type == null ? "osmand" : type;

		String PROFILE = profile;
		String GO = String.format(
				"10/%f/%f",
				(start.getLatitude() + finish.getLatitude()) / 2,
				(start.getLongitude() + finish.getLongitude()) / 2
		);

		String hasVia = !via.isEmpty() ? "&via=" : "";

		List<String> viaList = new ArrayList<>();
		via.forEach(ll -> viaList.add(String.format("%f,%f", ll.getLatitude(), ll.getLongitude())));
		String VIA = String.join(";", viaList);

		// convert type->params to make navigation href
		List<String> typeParams = new ArrayList<>(params);
		if ("brp-java".equals(TYPE)) {
			typeParams.add("hhoff:true");
		}
		if ("brp-cpp".equals(TYPE)) {
			typeParams.add("hhoff:true,nativerouting:true");
		}
		if ("hh-java".equals(TYPE)) {
			typeParams.add("hhonly:true");
		}
		if ("hh-cpp".equals(TYPE)) {
			typeParams.add("hhonly:true,nativerouting:true");
		}
		if (TYPE.startsWith("brp") && "car".equals(PROFILE)) {
			typeParams.add("calcmode:" + (car2phase ? "COMPLEX" : "NORMAL"));
		}

		String hasParams = !typeParams.isEmpty() ? "&params=" : "";
		String PARAMS = String.join(",", typeParams); // site will fix it to "profile,params"

		// detect proto: 1) default "https" 2) "http" for localhost 3) might be already included in domain
		String protoDomain = domain.contains("://") ? domain : (
				(domain.contains("localhost") ? "http://" : "https://") + domain);

		return String.format(
				"%s/map/navigate/?start=%s&finish=%s%s%s&profile=%s%s%s#%s",
				protoDomain, START, FINISH, hasVia, VIA, PROFILE, hasParams, PARAMS, GO
		);
	}
}

class RandomRouteResult {
	String type;
	double cost;
	long runTime; // ms
	int visitedSegments;
	float distance; // meters
	RandomRouteEntry entry; // ref to the parent: start, finish, etc

	// Public Transport Result
	RandomRouteResult(String type, RandomRouteEntry entry, long runTime, List<TransportRouteResult> results) {
		this.type = type;
		this.entry = entry;
		this.runTime = runTime;

		visitedSegments = results.size();
		cost = calcTransportRouteAvgCost(results);
		distance = calcTransportRouteAvgDistance(results);
	}

	// BRP/HH Routing Result
	RandomRouteResult(String type, RandomRouteEntry entry, long runTime,
	                  RoutingContext ctx, List<RouteSegmentResult> segments) {
		this.type = type;
		this.entry = entry;
		this.runTime = runTime;

		this.distance = 0;
		this.cost = ctx.routingTime;
		this.visitedSegments = ctx.calculationProgress.visitedSegments;

		if (segments != null) {
			float untrustedDistance = 0;
			for (RouteSegmentResult r : segments) {
				untrustedDistance += r.getDistance();
				this.distance += calcSegmentDistance(r);
			}
			if (Float.compare(this.distance, untrustedDistance) != 0) {
				System.err.printf("WARN: %s got different distance (%f != %f)\n", type, this.distance, untrustedDistance);
			}
		}
	}

	private double calcTransportRouteAvgCost(List<TransportRouteResult> results) {
		double cost = 0;
		if (!results.isEmpty()) {
			for (TransportRouteResult r : results) {
				cost += r.getRouteTime();
			}
			cost /= results.size();
		}
		return cost;
	}

	private float calcTransportRouteAvgDistance(List<TransportRouteResult> results) {
		double dist = 0;
		if (!results.isEmpty()) {
			for (TransportRouteResult r : results) {
				for (TransportRoutePlanner.TransportRouteResultSegment segment : r.getSegments()) {
					dist += segment.travelDistApproximate;
					dist += segment.walkDist;
				}
				dist += r.getFinishWalkDist();
			}
			dist /= results.size();
		}
		return (float)dist;
	}

	private float calcSegmentDistance(RouteSegmentResult rr) {
		int next;
		double distance = 0;
		RouteDataObject road = rr.getObject();
		boolean plus = rr.getStartPointIndex() < rr.getEndPointIndex();
		for (int j = rr.getStartPointIndex(); j != rr.getEndPointIndex(); j = next) {
			next = plus ? j + 1 : j - 1;
			double d = measuredDist(road.getPoint31XTile(j), road.getPoint31YTile(j),
					road.getPoint31XTile(next), road.getPoint31YTile(next));
			distance += d;
		}
		return (float) distance;
	}

	private double measuredDist(int x1, int y1, int x2, int y2) {
		return MapUtils.getDistance(MapUtils.get31LatitudeY(y1), MapUtils.get31LongitudeX(x1),
				MapUtils.get31LatitudeY(y2), MapUtils.get31LongitudeX(x2));
	}

	public String toString() {
		return entry.toURL(type);
	}

	public String toURL(String domain, boolean car2phase) {
		return entry.toURL(type, domain, car2phase);
	}
}

class RandomRouteReport {
	private String text;
	private String html;
	private final String htmlDomain;
	private final boolean car2phase;
	private final double deviationRed;
	private final double deviationYellow;
	private int failedResults, deviatedResults;

	RandomRouteReport(long runTime, int nObf, int nRoutes, double red, double yellow, String htmlDomain, boolean car2phase) {
		this.deviationRed = red;
		this.deviationYellow = yellow;
		this.htmlDomain = htmlDomain;
		this.car2phase = car2phase;

		String dt = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").format(Calendar.getInstance().getTime());

		this.text = String.format("%s Random Route Tester (%d obf files, %d routes, %d seconds) %s\n\n",
				dt, nObf, nRoutes, runTime / 1000, car2phase ? "[car_2phase_mode]" : "");

		this.html = "<html><head><style>" + "table, th, td { border: 1px solid silver; border-collapse: collapse; }" +
				"</style></head><body>\n" + this.text + "<br><table border=1>\n";
	}

	static String getMapCreatorProfileParams(RandomRouteEntry entry) {
		return (entry.profile + "," + entry.params.toString())
				.replaceAll("[\\[ \\]]", "") // remove array specific chars
				.replaceAll(":", "=") // replace key:value to key=value
				.replaceAll(",$", ""); // drop tailing comma
	}

	static String resultPrimaryText(int n, RandomRouteResult primary) {
		String mapCreatorProfileParams = getMapCreatorProfileParams(primary.entry);
		return String.format("%d:%s cost=%.2f dist=%.2f segments=%d seconds=%.1f via=%d profile=%s",
				n,
				primary.type,
				primary.cost,
				primary.distance,
				primary.visitedSegments,
				primary.runTime / 1000F,
				primary.entry.via.size(),
				mapCreatorProfileParams
		);
	}

	String resultPrimaryHtml(RandomRouteResult primary) {
		String mapCreatorProfileParams = getMapCreatorProfileParams(primary.entry);

		String start = String.format("%f,%f", primary.entry.start.getLatitude(), primary.entry.start.getLongitude());
		String finish = String.format("%f,%f", primary.entry.finish.getLatitude(), primary.entry.finish.getLongitude());
		String url = primary.toURL(htmlDomain, car2phase);

		String sCost = primary.cost > 0 ? String.format("%.2f", primary.cost) : "zero";
		String colorCost = costDistHtmlColor(primary.cost);

		String sDistance = primary.distance > 0 ? String.format("%.2f", primary.distance) : "zero";
		String colorDistance = costDistHtmlColor(primary.distance);

		return "<tr align=center>" +
				String.format("<td><a href=\"%s\" target=_blank>%s</a></td>", url, primary.type) + // 1
				String.format("<td><font color=%s>%s</font></td>", colorCost, sCost) +             // 2
				String.format("<td><font color=%s>%s</font></td>", colorDistance, sDistance) +     // 3
				String.format("<td>%d</td>", primary.visitedSegments) +                            // 4
				String.format("<td>%.1f</td>", primary.runTime / 1000F) +                          // 5
				String.format("<td>%s</td>", start) +                                              // 6
				String.format("<td>%s</td>", finish) +                                             // 7
				String.format("<td>%d</td>", primary.entry.via.size()) +                           // 8
				String.format("<td>%s</td>", mapCreatorProfileParams) +                            // 9
				"</tr>\n";
	}


	void resultPrimary(int n, RandomRouteResult primary) {
		text += resultPrimaryText(n, primary) + "\n";
		html += resultPrimaryHtml(primary);
	}

	void resultCompare(int n, RandomRouteResult result, RandomRouteResult primary) {
		String url = result.toURL(htmlDomain, car2phase);

		double dCost = primary.cost > 0 ? (result.cost / primary.cost - 1) * 100 : 0;
		String sCost = Math.abs(dCost) < deviationYellow && result.cost > 0 ? "ok"
				: (result.cost > 0 ? String.format("%s%.2f%%", dCost > 0 ? "+" : "", dCost) : "zero");
		String colorCost = deviationHtmlColor(dCost, result.cost);

		double dDistance = primary.distance > 0 ? (result.distance / primary.distance - 1) * 100 : 0;
		String sDistance = Math.abs(dDistance) < deviationYellow && result.distance > 0 ? "ok"
				: (result.distance > 0 ? String.format("%s%.2f%%", dDistance > 0 ? "+" : "", dDistance) : "zero");
		String colorDistance = deviationHtmlColor(dDistance, result.distance);

		html += "<tr align=center>" +
				String.format("<td><a href=\"%s\" target=_blank>%s</a></td>", url, result.type) + // 1
				String.format("<td><font color=%s>%s</font></td>", colorCost, sCost) +            // 2
				String.format("<td><font color=%s>%s</font></td>", colorDistance, sDistance) +    // 3
				String.format("<td>%d</td>", result.visitedSegments) +                            // 4
				String.format("<td>%.1f</td>", result.runTime / 1000F) +                          // 5
				"<td colspan=4>&nbsp;</td>" +                                                     // 6-9
				"</tr>\n";

		text += String.format("%d:%s cost %s dist %s segments=%d seconds=%.1f\n",
				n,
				result.type,
				sCost,
				sDistance,
				result.visitedSegments,
				result.runTime / 1000F
		);
	}

	void entryOpen(int n) {
		html += "<tr>" +
				String.format("<th>%d</th>", n) + // 1
				"<th>cost</th>" +                 // 2
				"<th>dist</th>" +                 // 3
				"<th>vis</th>" +                  // 4
				"<th>s</th>" +                    // 5
				"<th>start</th>" +                // 6
				"<th>finish</th>" +               // 7
				"<th>via</th>" +                  // 8
				"<th>profile</th>" +              // 9
				"</tr>\n";
	}

	void entryClose() {
		text += "\n";
		html += "<tr><td colspan=10>&nbsp;</td></tr>\n";
	}

	private String costDistHtmlColor(double n) {
		if (n > 0) {
			return "green";
		} else {
			failedResults++;
			return "red";
		}
	}

	private String deviationHtmlColor(double percent, double value) {
		if (value <= 0) {
			failedResults++;
			return "red";
		}
		if (Math.abs(percent) >= deviationRed) {
			deviatedResults++;
			return "red";
		} else if (Math.abs(percent) >= deviationYellow) {
			return "orange";
		}
		return "green";
	}

	void flush(String htmlFileName) throws IOException {
		FileWriter writer = new FileWriter(htmlFileName);
		html += """
				</table><br>
				cost - cost of all segments (seconds) | avg cost among all PT (public transport) routes<br>
				dist - distance of geometry (meters) | avg distance among all PT routes<br>
				vis - count of visited segments | number of found PT routes<br>
				s - route calc time (seconds)<br>
				via - number of inter points<br>
				</body></html>
				""";
		System.err.printf("\n%s", text);
		writer.write(html);
		writer.close();
	}

	public boolean isFailed() {
		return failedResults > 0;
	}

	public boolean isDeviated() {
		return deviatedResults > 0;
	}
}
