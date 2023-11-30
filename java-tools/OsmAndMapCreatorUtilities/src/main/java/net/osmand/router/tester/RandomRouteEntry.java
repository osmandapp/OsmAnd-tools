package net.osmand.router.tester;

import net.osmand.data.LatLon;
import net.osmand.router.RouteSegmentResult;
import net.osmand.router.RoutingContext;

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
		return toURL("osrm"); // osrm is used for quick overview
	}

	String toURL(String type) {
		String START = String.format("%f,%f", start.getLatitude(), start.getLongitude());
		String FINISH = String.format("%f,%f", finish.getLatitude(), finish.getLongitude());

		String TYPE = type == null ? "osmand" : type; // TODO process type with conversion to web-style params

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

	RandomRouteResult(String type, RandomRouteEntry entry, long runTime,
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

		System.err.printf("\n\nRandomRouteResult %s (%d) cost=%f dist=%f\n\n", type, runTime, cost, distance);
	}

	public String toString() {
		return entry.toURL(type);
	}
}

class RandomRouteReport {
	private String text;
	private String html;
	private double deviationRed;
	private double deviationYellow;

	RandomRouteReport(long started, int nObf, int nRoutes, double red, double yellow) {
		this.deviationRed = red;
		this.deviationYellow = red;

		long runTime = System.currentTimeMillis() - started;
		String dt = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").format(Calendar.getInstance().getTime());

		this.text = String.format("%s Random Route Tester (%d obf files, %d routes, %d seconds)\n\n",
				dt, nObf, nRoutes, runTime / 1000);

		this.html = "<html><head><style>" + "table, th, td { border: 1px solid silver; border-collapse: collapse; }" +
				"</style></head><body>\n" + this.text + "<br><table border=1>\n";
	}

	void resultIdeal(int n, RandomRouteResult ideal) {
		String mapCreatorProfileParams = (ideal.entry.profile + "," + ideal.entry.params.toString())
				.replaceAll("[\\[ \\]]", "") // remove array specific chars
				.replaceAll(":", "=") // replace key:value to key=value
				.replaceAll(",$", ""); // drop tailing comma
		String start = String.format("%f,%f", ideal.entry.start.getLatitude(), ideal.entry.start.getLongitude());
		String finish = String.format("%f,%f", ideal.entry.finish.getLatitude(), ideal.entry.finish.getLongitude());
		String url = ideal.toString();
		String geoOk = "ok";

		this.html += "<tr align=center>" +
				String.format("<td><a href=\"%s\" target=_blank>%s</a></td>", url, ideal.type) + // 1
				String.format("<td>%.2f</td>", ideal.cost) +                                     // 2
				String.format("<td>%.2f</td>", ideal.distance) +                                 // 3
				String.format("<td>%s</td>", geoOk) +                                            // 4
				String.format("<td>%d</td>", ideal.visitedSegments) +                            // 5
				String.format("<td>%.1f</td>", ideal.runTime / 1000F) +                          // 6
				String.format("<td>%s</td>", start) +                                            // 7
				String.format("<td>%s</td>", finish) +                                           // 8
				String.format("<td>%d</td>", ideal.entry.via.size()) +                           // 9
				String.format("<td>%s</td>", mapCreatorProfileParams) +                          // 10
				"</tr>\n";

		this.text += String.format("%d:%s cost=%.2f dist=%.2f segments=%d seconds=%.1f via=%d profile=%s\n",
				n,
				ideal.type,
				ideal.cost,
				ideal.distance,
				ideal.visitedSegments,
				ideal.runTime / 1000F,
				ideal.entry.via.size(),
				mapCreatorProfileParams
		);
	}

	void resultCompare(int n, RandomRouteResult result, RandomRouteResult ideal) {
		String mapCreatorProfileParams = (result.entry.profile + "," + result.entry.params.toString())
				.replaceAll("[\\[ \\]]", "") // remove array specific chars
				.replaceAll(":", "=") // replace key:value to key=value
				.replaceAll(",$", ""); // drop tailing comma
		String start = String.format("%f,%f", result.entry.start.getLatitude(), result.entry.start.getLongitude());
		String finish = String.format("%f,%f", result.entry.finish.getLatitude(), result.entry.finish.getLongitude());
		String url = result.toString();
		String geoOk = "?"; // TODO

		double dCost = ideal.cost > 0 ? (result.cost / ideal.cost - 1) * 100 : 0;
		String sCost = Math.abs(dCost) < deviationYellow ? "ok"
				: String.format("%s%.2f%%", dCost > 0 ? "+" : "", dCost);
		String colorCost = deviationHtmlColor(dCost);

		double dDistance = ideal.distance > 0 ? (result.distance / ideal.distance - 1) * 100 : 0;
		String sDistance = Math.abs(dDistance) < deviationYellow ? "ok"
				: String.format("%s%.2f%%", dDistance > 0 ? "+" : "", dDistance);
		String colorDistance = deviationHtmlColor(dDistance);

		this.html += "<tr align=center>" +
				String.format("<td><a href=\"%s\" target=_blank>%s</a></td>", url, result.type) + // 1
				String.format("<td><font color=%s>%s</font></td>", colorCost, sCost) +            // 2
				String.format("<td><font color=%s>%s</font></td>", colorDistance, sDistance) +    // 2
				String.format("<td>%s</td>", geoOk) +                                             // 4
				String.format("<td>%d</td>", result.visitedSegments) +                            // 5
				String.format("<td>%.1f</td>", result.runTime / 1000F) +                          // 6
				String.format("<td>%s</td>", start) +                                             // 7
				String.format("<td>%s</td>", finish) +                                            // 8
				String.format("<td>%d</td>", result.entry.via.size()) +                           // 9
				String.format("<td>%s</td>", mapCreatorProfileParams) +                           // A
				"</tr>\n";

		this.text += String.format("%d:%s cost %s dist %s segments=%d seconds=%.1f\n",
				n,
				result.type,
				sCost,
				sDistance,
				result.visitedSegments,
				result.runTime / 1000F
		);
	}

	void entryOpen(int n) {
		this.html += "<tr>" +
				String.format("<th>%d</th>", n) + // 1
				"<th>cost</th>" +                 // 2
				"<th>dist</th>" +                 // 3
				"<th>geo</th>" +                  // 4
				"<th>vis</th>" +                  // 5
				"<th>s</th>" +                    // 6
				"<th>start</th>" +                // 7
				"<th>finish</th>" +               // 8
				"<th>via</th>" +                  // 9
				"<th>profile</th>" +              // A
				"</tr>\n";
	}

	void entryClose() {
		this.text += "\n";
		this.html += "<tr><td colspan=10>&nbsp;</td></tr>\n";
	}

	private String deviationHtmlColor(double percent) {
		if (Math.abs(percent) >= deviationRed) {
			return "red";
		} else if (Math.abs(percent) >= deviationYellow) {
			return "orange";
		}
		return "green";
	}

	void flush(FileWriter writer) throws IOException {
		this.html += "</table><br>\n" +
				"cost - cost of all segments (seconds)<br>\n" +
				"dist - distance of geometry (meters)<br>\n" +
				"vis - count of visited segments<br>\n" +
				"s - route calc time (seconds)<br>\n" +
				"via - number of inter points<br>\n" +
				"</body></html>\n";
		System.err.printf("\n%s", text);
		writer.write(html);
		writer.close();
	}
}
