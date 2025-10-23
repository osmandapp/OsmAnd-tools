
package net.osmand.server.controllers.pub;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import jakarta.servlet.http.HttpSession;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.bind.annotation.ResponseBody;
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
import net.osmand.router.RouteSegmentResult;
import net.osmand.router.RoutingConfiguration;
import net.osmand.server.api.services.OsmAndMapsService;
import net.osmand.server.api.services.OsmAndMapsService.RoutingServerConfigEntry;
import net.osmand.server.api.services.RoutingService;
import net.osmand.server.controllers.pub.GeojsonClasses.Feature;
import net.osmand.server.controllers.pub.GeojsonClasses.FeatureCollection;
import net.osmand.server.controllers.pub.GeojsonClasses.Geometry;
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

		public void fillSelectList(String section, Map<String, String> list) {
			this.section = section;
			this.values = new String[list.size()];
			this.valueDescriptions = new String[list.size()];
			int i = 0;
			for (String key : list.keySet()) {
				if (i == 0) {
					this.value = key; // default
				}
				this.values[i] = key;
				this.valueDescriptions[i] = list.get(key);
				i++;
			}
		}
	}

	@RequestMapping(path = "/routing-modes", produces = {MediaType.APPLICATION_JSON_VALUE})
	public ResponseEntity<?> routingParams() {
		Map<String, RoutingMode> routers = new LinkedHashMap<>();

		final String routingSection = "Routing (devel)";
		final String approximationSection = "Approximation (devel)";

		RoutingParameter noConditionals = new RoutingParameter("noconditionals", routingSection, "Ignore *:conditional tags", false);
		RoutingParameter sepMaps = new RoutingParameter("noglobalfile", routingSection, "Use separate OBF maps", false);

		RoutingParameter selectRoutingTypeCar = new RoutingParameter("routing", "Routing type",
				"Algorithm and library for routing", null, RoutingParameterType.SYMBOLIC.name().toLowerCase());
		RoutingParameter selectRoutingTypeAll = new RoutingParameter("routing", "Routing type",
				"Algorithm and library for routing", null, RoutingParameterType.SYMBOLIC.name().toLowerCase());
		selectRoutingTypeCar.fillSelectList(routingSection, OsmAndMapsService.ServerRoutingTypes.getSelectList(true));
		selectRoutingTypeAll.fillSelectList(routingSection, OsmAndMapsService.ServerRoutingTypes.getSelectList(false));

		RoutingParameter selectApproximationType = new RoutingParameter("approximation", "GPX approximation type",
				"Algorithm and library for approximation", null, RoutingParameterType.SYMBOLIC.name().toLowerCase());
		selectApproximationType.fillSelectList(approximationSection, OsmAndMapsService.ServerApproximationTypes.getSelectList());

		RoutingParameter gpxTimestampsDisabled = new RoutingParameter("gpxtimestamps",
				approximationSection, "Use GPX timestamps", false);
		RoutingParameter gpxTimestampsEnabled = new RoutingParameter("gpxtimestamps",
				approximationSection, "Use external timestamps", true); // rescuetrack only

		RoutingParameter minPointApproximation = new RoutingParameter("minPointApproximation", "minPointApproximation (m)",
				"ctx.config.minPointApproximation", null, RoutingParameterType.SYMBOLIC.name().toLowerCase());
		List <String> values = new ArrayList<>();
		for (int n = 0; n <= 100; n += 5) {
			values.add(String.valueOf(n));
		}
		minPointApproximation.section = approximationSection;
		minPointApproximation.values = values.toArray(new String[0]);
		minPointApproximation.valueDescriptions = values.toArray(new String[0]);
		int defaultMinPointApproximation = (int) new RoutingConfiguration().minPointApproximation;
		minPointApproximation.value = String.valueOf(defaultMinPointApproximation); // do not consider xml for the web

		RoutingParameter shortWay = new RoutingParameter("short_way", null, "Short way", false);
		// internal profiles (build-in routers)
		for (Map.Entry<String, GeneralRouter> e : RoutingConfiguration.getDefault().getAllRouters().entrySet()) {
			if (!e.getKey().equals("geocoding") && !e.getKey().equals("public_transport")) {
				RoutingMode rm;
				String derivedProfiles = e.getValue().getAttribute("derivedProfiles");
				RoutingParameter routingTypes = derivedProfiles != null && "car".equals(e.getKey())
						? selectRoutingTypeCar : selectRoutingTypeAll;
				List<RoutingController.RoutingParameter> passParams = new ArrayList<>(Arrays.asList(routingTypes,
						noConditionals, sepMaps, selectApproximationType, minPointApproximation, gpxTimestampsDisabled));
				if (derivedProfiles != null) {
					String[] derivedProfilesList = derivedProfiles.split(",");
					for (String profile : derivedProfilesList) {
						rm = new RoutingMode("default".equals(profile) ? e.getKey() : profile);
						routers.put(rm.key, rm);
						routingService.fillRoutingModeParams(passParams, shortWay, e, rm);
					}
				} else {
					rm = new RoutingMode(e.getKey());
					routers.put(rm.key, rm);
					routingService.fillRoutingModeParams(passParams, shortWay, e, rm);
				}
			}
		}
		// external profiles (see osmand-server-boot.conf RUN_ARGS --osmand.routing.config)
		for (RoutingServerConfigEntry rs : osmAndMapsService.getRoutingConfig().config.values()) {
			RoutingMode rm = new RoutingMode(rs.name);

			// reuse previously filled params using profile as key
			if (rs.profile != null && routers.get(rs.profile) != null) {
				routers.get(rs.profile).params.forEach((key, val) -> {
							if ("gpxtimestamps".equals(key) && rs.name.startsWith("rescuetrack")) {
								rm.params.put(key, gpxTimestampsEnabled);
							} else {
								rm.params.put(key, val);
							}
						}
				);
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
					resListElevation = routingService.getElevationsBySegments(resListElevation, features, res);
					routingService.interpolateEmptyElevationSegments(resListElevation);
					List<Double> eleDiff = routingService.calculateElevationDiffs(resListElevation);
					if (!eleDiff.isEmpty() && !Double.isNaN(eleDiff.get(0)) && !Double.isNaN(eleDiff.get(1))) {
						props.put("diffElevationUp", eleDiff.get(0));
						props.put("diffElevationDown", eleDiff.get(1));
					}
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
		RoutingService.RouteResult routeResult =
				routingService.updateRouteBetweenPoints(startPoint, endPoint, routeMode, hasRouting, disableOldRouting, progress);
		if (routeResult.points.size() <= 2 && disableOldRouting) { // report limit error
			return ResponseEntity.ok(gsonWithNans.toJson(Map.of("points", routeResult.points, "routeTypes", routeResult.routeTypes, "msg",
					MSG_LONG_DIST + hhOnlyLimit + " km.")));
		} else {
			return ResponseEntity.ok(gsonWithNans.toJson(Map.of("points", routeResult.points, "routeTypes", routeResult.routeTypes)));
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
