
package net.osmand.server.controllers.pub;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import javax.servlet.http.HttpSession;
import javax.validation.Valid;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import net.osmand.binary.GeocodingUtilities.GeocodingResult;
import net.osmand.data.LatLon;
import net.osmand.data.LatLonEle;
import net.osmand.gpx.GPXFile;
import net.osmand.gpx.GPXUtilities;
import net.osmand.router.GeneralRouter;
import net.osmand.router.GeneralRouter.RoutingParameterType;
import net.osmand.router.RouteCalculationProgress;
import net.osmand.router.RoutePlannerFrontEnd.RouteCalculationMode;
import net.osmand.router.RouteSegmentResult;
import net.osmand.router.RoutingConfiguration;
import net.osmand.server.api.services.OsmAndMapsService;
import net.osmand.server.api.services.OsmAndMapsService.RoutingServerConfigEntry;
import net.osmand.server.api.services.RoutingService;
import net.osmand.server.utils.WebGpxParser;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;

@Controller
@RequestMapping("/routing")
public class RoutingController {
	public static final String MSG_LONG_DIST = "Sorry, in our beta mode max routing distance is limited to ";
	protected static final Log LOGGER = LogFactory.getLog(RoutingController.class);
	
	@Autowired
	OsmAndMapsService osmAndMapsService;
	
	@Autowired
	RoutingService routingService;
	
	@Autowired
	UserSessionResources session;
	
	Gson gson = new Gson();
	
	Gson gsonWithNans = new GsonBuilder().serializeSpecialFloatingPointValues().create();

	public static class FeatureCollection {
		public String type = "FeatureCollection";
		public List<Feature> features = new ArrayList<>();
		
		public FeatureCollection(Feature... features) {
			this.features.addAll(Arrays.asList(features));
		}
	}
	
	public static class Feature {
		public Map<String, Object> properties = new LinkedHashMap<>();
		public String type = "Feature";
		public final Geometry geometry;
		
		public Feature(Geometry geometry) {
			this.geometry = geometry;
		}
		
		public Feature prop(String key, Object vl) {
			properties.put(key, vl);
			return this;
		}
	}
	
	public static class Geometry {
		public final String type;
		public Object coordinates;
		
		public Geometry(String type) {
			this.type = type;
		}

		public static Geometry lineString(List<LatLon> lst) {
			Geometry gm = new Geometry("LineString");
			float[][] coordinates = new float[lst.size()][];
			for (int i = 0; i < lst.size(); i++) {
				coordinates[i] = new float[]{(float) lst.get(i).getLongitude(), (float) lst.get(i).getLatitude()};
			}
			gm.coordinates = coordinates;
			return gm;
		}

		public static Geometry lineStringElevation(List<LatLonEle> lst) {
			Geometry gm = new Geometry("LineString");
			float[][] coordinates = new float[lst.size()][];
			for (int i = 0; i < lst.size(); i++) {
				float lat = (float) lst.get(i).getLatitude();
				float lon = (float) lst.get(i).getLongitude();
				float ele = (float) lst.get(i).getElevation();
				if (Float.isNaN(ele)) {
					coordinates[i] = new float[]{lon, lat}; // GeoJSON [] longitude first, then latitude
				} else {
					coordinates[i] = new float[]{lon, lat, ele}; // https://www.rfc-editor.org/rfc/rfc7946 3.1.1
				}
			}
			gm.coordinates = coordinates;
			return gm;
		}

		public static Geometry point(LatLon pnt) {
			Geometry gm = new Geometry("Point");
			gm.coordinates = new float[]{(float) pnt.getLongitude(), (float) pnt.getLatitude()};
			return gm;
		}

		public static Geometry pointElevation(LatLonEle pnt) {
			Geometry gm = new Geometry("Point");
			float lat = (float) pnt.getLatitude();
			float lon = (float) pnt.getLongitude();
			float ele = (float) pnt.getElevation();
			if (Double.isNaN(ele)) {
				gm.coordinates = new float[]{lon, lat};
			} else {
				gm.coordinates = new float[]{lon, lat, ele};
			}
			return gm;
		}
	}
	
	public static class RoutingMode {
		public String key;
		public String name;
		public Map<String, RoutingParameter> params = new LinkedHashMap<>();

		public RoutingMode(String key) {
			this.key = key;
			name = Algorithms.capitalizeFirstLetter(key).replace('_', ' ');
		}
	}
	
	public static class RoutingParameter {
		public String key;
		public String label;
		public String description;
		public String type;
		public String section;
		public String group;
		public Object value;
		public String[] valueDescriptions;
		public Object[] values;
		
		public RoutingParameter(String key, String section, String name, boolean defValue) {
			this.key = key;
			this.label = name;
			this.description = name;
			this.type = "boolean";
			this.section = section;
			this.value = defValue;
		}
		
		public RoutingParameter(String key,  String name, String description, String group, String type) {
			this.key = key;
			this.label = name;
			this.description = description;
			this.type = type;
			this.group = group;
		}
		
	}
	
	@RequestMapping(path = "/routing-modes", produces = {MediaType.APPLICATION_JSON_VALUE})
	public ResponseEntity<?> routingParams() {
		Map<String, RoutingMode> routers = new LinkedHashMap<>();
//		RoutingParameter applyApproximation = new RoutingParameter("applyapproximation", "",
//				"Attach to roads (OsmAnd)", true);
		RoutingParameter hhRouting = new RoutingParameter("hhoff", "Development",
				"[Dev] Disable HH routing", false);
		RoutingParameter nativeRouting = new RoutingParameter("nativerouting", "Development",
				"[Dev] Use C++ for routing", false);
		RoutingParameter nativeTrack = new RoutingParameter("nativeapproximation", "Development", 
				"[Dev] Use C++ approximation", false);
		RoutingParameter sepMaps = new RoutingParameter("noglobalfile", "Development", 
				"[Dev] Use separate maps", false);
		
		RoutingParameter calcMode = new RoutingParameter("calcmode", "Mode (old)",
				"Algorithm to calculate route", null, RoutingParameterType.SYMBOLIC.name().toLowerCase());
		calcMode.section = "Development";
		calcMode.value = "";
		calcMode.valueDescriptions = new String[] {"Optimal", "Basic", "Slow"};
		calcMode.values = new String[] { RouteCalculationMode.COMPLEX.name(), RouteCalculationMode.BASE.name(),
				RouteCalculationMode.NORMAL.name()
		};
		RoutingParameter shortWay = new RoutingParameter("short_way", null, "Short way", false);
		// internal profiles (build-in routers)
		for (Map.Entry<String, GeneralRouter> e : RoutingConfiguration.getDefault().getAllRouters().entrySet()) {
			if (!e.getKey().equals("geocoding") && !e.getKey().equals("public_transport")) {
				RoutingMode rm;
				String derivedProfiles = e.getValue().getAttribute("derivedProfiles");
				if (derivedProfiles != null) {
					String[] derivedProfilesList = derivedProfiles.split(",");
					for (String profile : derivedProfilesList) {
						rm = new RoutingMode("default".equals(profile) ? e.getKey() : profile);
						routers.put(rm.key, rm);
						routingService.fillRoutingModeParams(
								Arrays.asList(hhRouting, nativeRouting, nativeTrack, sepMaps, calcMode), shortWay, e, rm);
					}
				} else {
					rm = new RoutingMode(e.getKey());
					routers.put(rm.key, rm);
					routingService.fillRoutingModeParams(
							Arrays.asList(hhRouting, nativeRouting, nativeTrack, sepMaps, calcMode), shortWay, e, rm);
				}
			}
		}
		// external profiles (see osmand-server-boot.conf RUN_ARGS --osmand.routing.config)
		for (RoutingServerConfigEntry rs : osmAndMapsService.getRoutingConfig().config.values()) {
			RoutingMode rm = new RoutingMode(rs.name);

			// reuse previously filled params using profile as key
			if (rs.profile != null && routers.get(rs.profile) != null) {
				routers.get(rs.profile).params.forEach((key, val) -> rm.params.put(key, val));
			}

			routers.put(rm.key, rm);
		}
		return ResponseEntity.ok(gson.toJson(routers));
	}

	@PostMapping(path = {"/gpx-approximate"}, produces = "application/json")
	public ResponseEntity<String> uploadGpx(@RequestPart(name = "file") @Valid @NotNull @NotEmpty MultipartFile file,
	                                        @RequestParam(defaultValue = "car") String routeMode) throws IOException {
		InputStream is = file.getInputStream();
		GPXFile gpxFile = GPXUtilities.loadGPXFile(is);
		is.close();
		if (gpxFile.error != null) {
			return ResponseEntity.badRequest().body("Error reading gpx!");
		} else {
			gpxFile.path = file.getOriginalFilename();
			List<LatLon> resList = new ArrayList<>();
			List<Feature> features = new ArrayList<>();
			Map<String, Object> props = new TreeMap<>();
			try {
				List<RouteSegmentResult> res = osmAndMapsService.gpxApproximation(routeMode, props, gpxFile);
				routingService.convertResults(resList, features, res);
			} catch (IOException | InterruptedException | RuntimeException e) {
				LOGGER.error(e.getMessage(), e);
			}
			Feature route = new Feature(Geometry.lineString(resList));
			features.add(0, route);
			route.properties = props;
			return ResponseEntity.ok(gson.toJson(new FeatureCollection(features.toArray(new Feature[features.size()]))));
		}
	}
	
	@RequestMapping(path = "/geocoding", produces = {MediaType.APPLICATION_JSON_VALUE})
	public ResponseEntity<String> geocoding(@RequestParam double lat, @RequestParam double lon) throws IOException, InterruptedException {
		if (!osmAndMapsService.validateAndInitConfig()) {
			return osmAndMapsService.errorConfig();
		}
		try {
			List<GeocodingResult> lst = osmAndMapsService.geocoding(lat, lon);
			List<Feature> features = new ArrayList<>();
			int i = 0;
			for (GeocodingResult rr : lst) {
				i++;
				features.add(new Feature(Geometry.point(rr.getLocation())).prop("index", i).prop("description",
						i + ". " + rr.toString()));
			}
			return ResponseEntity.ok(gson.toJson(new FeatureCollection(features.toArray(new Feature[features.size()]))));
		} catch (IOException | InterruptedException | RuntimeException e) {
			LOGGER.error(e.getMessage(), e);
			throw e;
		}
	}
	
	@RequestMapping(path = "/route", produces = {MediaType.APPLICATION_JSON_VALUE})
	public ResponseEntity<String> routing(HttpSession session,
			@RequestParam String[] points, @RequestParam(defaultValue = "car") String routeMode,
	                                 @RequestParam(required = false) String[] avoidRoads,
	                                 @RequestParam(defaultValue = "production") String limits) throws IOException {
		RouteCalculationProgress progress = this.session.getRoutingProgress(session);
		final int hhOnlyLimit = osmAndMapsService.getRoutingConfig().hhOnlyLimit;
		List<LatLon> list = new ArrayList<>();
		double lat = 0;
		int k = 0;
		boolean disableOldRouting = false;
		LatLon prev = null;
		for (String point : points) {
			String[] sl = point.split(",");
			for (String p : sl) {
				double vl = Double.parseDouble(p);
				if (k++ % 2 == 0) {
					lat = vl;
				} else {
					LatLon pnt = new LatLon(lat, vl);
					if (!list.isEmpty()) {
						disableOldRouting = disableOldRouting || MapUtils.getDistance(prev, pnt) > hhOnlyLimit * 1000;
					}
					list.add(pnt);
					prev = pnt;
				}
			}
		}
		List<LatLonEle> resListElevation = new ArrayList<>();
		List<Feature> features = new ArrayList<>();
		Map<String, Object> props = new TreeMap<>();
		if (list.size() >= 2) {
			try {
				List<RouteSegmentResult> res =
						osmAndMapsService.routing(disableOldRouting, routeMode, props, list.get(0),
								list.get(list.size() - 1), list.subList(1, list.size() - 1),
								avoidRoads == null ? Collections.emptyList() : Arrays.asList(avoidRoads), progress);
				if (res != null) {
					routingService.convertResultsWithElevation(resListElevation, features, res);
				}
			} catch (IOException | InterruptedException | RuntimeException e) {
				LOGGER.error(e.getMessage(), e);
			}
		}

		float dist = 0;
		boolean reportLimitError = false;
		List<LatLon> resListFallbackLine = new ArrayList<>();
		if (resListElevation.isEmpty()) {
			reportLimitError = true;
			resListFallbackLine = new ArrayList<>(list);
			routingService.calculateStraightLine(resListFallbackLine);
			for (int i = 1; i < resListFallbackLine.size(); i++) {
				dist += MapUtils.getDistance(resListFallbackLine.get(i - 1), resListFallbackLine.get(i));
			}
			props.put("distance", dist);
		}

		Feature route = resListElevation.isEmpty() ?
				new Feature(Geometry.lineString(resListFallbackLine)) :
				new Feature(Geometry.lineStringElevation(resListElevation));
		route.properties = props;
		features.add(0, route);

		if (reportLimitError && dist >= hhOnlyLimit * 1000) {
			return ResponseEntity.ok(gson.toJson(Map.of("features", new FeatureCollection(features.toArray(new Feature[features.size()])), "msg",
					MSG_LONG_DIST + hhOnlyLimit + " km.")));
		} else {
			return ResponseEntity.ok(gson.toJson(new FeatureCollection(features.toArray(new Feature[features.size()]))));
		}
	}


	@PostMapping(path = {"/update-route-between-points"}, produces = "application/json")
	@ResponseBody
	public ResponseEntity<String> updateRouteBetweenPoints(HttpSession session, @RequestParam String start,
			@RequestParam String end, @RequestParam String routeMode, @RequestParam boolean hasRouting,
			@RequestParam(defaultValue = "production") String limits) throws IOException, InterruptedException {

		final int hhOnlyLimit = osmAndMapsService.getRoutingConfig().hhOnlyLimit;
		LatLon startPoint = gson.fromJson(start, LatLon.class);
		LatLon endPoint = gson.fromJson(end, LatLon.class);
		boolean disableOldRouting = MapUtils.getDistance(startPoint, endPoint) > hhOnlyLimit * 1000;
		RouteCalculationProgress progress = this.session.getRoutingProgress(session);
		List<WebGpxParser.Point> trackPointsRes =
				routingService.updateRouteBetweenPoints(startPoint, endPoint, routeMode, hasRouting, disableOldRouting, progress);
		if (trackPointsRes.size() <= 2 && disableOldRouting) { // report limit error
			return ResponseEntity.ok(gsonWithNans.toJson(Map.of("points", trackPointsRes, "msg",
					MSG_LONG_DIST + hhOnlyLimit + " km.")));
		} else {
			return ResponseEntity.ok(gsonWithNans.toJson(Map.of("points", trackPointsRes)));
		}
	}
	
	@PostMapping(path = {"/get-route"}, produces = "application/json")
	@ResponseBody
	public ResponseEntity<String> getRoute(@RequestBody List<WebGpxParser.Point> points) throws IOException, InterruptedException {
		List<WebGpxParser.Point> res = routingService.getRoute(points);
		return ResponseEntity.ok(gsonWithNans.toJson(Map.of("points", res)));
	}
	
	@PostMapping(path = {"/approximate"}, produces = "application/json")
	@ResponseBody
	public ResponseEntity<String> approximateRoute(@RequestBody List<WebGpxParser.Point> points, @RequestParam String routeMode) throws IOException, InterruptedException {
		List<WebGpxParser.Point> res = routingService.approximateRoute(points, routeMode);
		return ResponseEntity.ok(gsonWithNans.toJson(Map.of("points", res)));
	}
}
