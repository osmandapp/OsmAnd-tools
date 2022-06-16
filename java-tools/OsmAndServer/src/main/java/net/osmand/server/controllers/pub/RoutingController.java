
package net.osmand.server.controllers.pub;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import javax.validation.Valid;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.multipart.MultipartFile;

import com.google.gson.Gson;

import net.osmand.GPXUtilities;
import net.osmand.GPXUtilities.GPXFile;
import net.osmand.binary.GeocodingUtilities.GeocodingResult;
import net.osmand.data.Amenity;
import net.osmand.data.LatLon;
import net.osmand.data.Street;
import net.osmand.router.GeneralRouter;
import net.osmand.router.GeneralRouter.RoutingParameterType;
import net.osmand.router.RoutePlannerFrontEnd.RouteCalculationMode;
import net.osmand.router.RouteSegmentResult;
import net.osmand.router.RoutingConfiguration;
import net.osmand.search.core.ObjectType;
import net.osmand.search.core.SearchResult;
import net.osmand.server.api.services.OsmAndMapsService;
import net.osmand.server.api.services.OsmAndMapsService.RoutingServerConfigEntry;
import net.osmand.server.api.services.OsmAndMapsService.VectorTileServerConfig;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;

@Controller
@RequestMapping("/routing")
public class RoutingController {
	
	private static final int DISTANCE_MID_POINT = 25000;
	private static final int MAX_DISTANCE = 1000000;

	protected static final Log LOGGER = LogFactory.getLog(RoutingController.class);

	@Autowired
	OsmAndMapsService osmAndMapsService;
	
	Gson gson = new Gson();

	private ResponseEntity<?> errorConfig() {
		VectorTileServerConfig config = osmAndMapsService.getConfig();
		return ResponseEntity.badRequest()
				.body("Tile service is not initialized: " + (config == null ? "" : config.initErrorMessage));
	}
	
	
	public static class FeatureCollection {
		public String type = "FeatureCollection";
		public List<Feature> features = new ArrayList<RoutingController.Feature>();
		
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
		public Map<String, Object> properties = new LinkedHashMap<>();
		public Object coordinates;
		
		public Geometry(String type) {
			this.type = type;
		}
		
		public static Geometry lineString(List<LatLon> lst) {
			Geometry gm = new Geometry("LineString");
			double[][] coordnates =  new double[lst.size()][];
			for(int i = 0; i < lst.size() ; i++) {
				coordnates[i] = new double[] {lst.get(i).getLongitude(), lst.get(i).getLatitude() };
			}
			gm.coordinates = coordnates;
			return gm;
		}
		
		public static Geometry point(LatLon pnt) {
			Geometry gm = new Geometry("Point");
			gm.coordinates = new double[] {pnt.getLongitude(), pnt.getLatitude() };
			return gm;
		}
	}
	
	protected static class RoutingMode {
		public String key;
		public String name;
		public Map<String, RoutingParameter> params = new LinkedHashMap<String, RoutingParameter>();

		public RoutingMode(String key) {
			this.key = key;
			name = Algorithms.capitalizeFirstLetter(key).replace('_', ' ');
		}
	}
	
	protected static class RoutingParameter {
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
		Map<String, RoutingMode> routers = new LinkedHashMap<String, RoutingMode>();
		RoutingParameter nativeRouting = new RoutingParameter("nativerouting", "Development", 
				"[Dev] C++ routing", true);
		RoutingParameter nativeTrack = new RoutingParameter("nativeapproximation", "Development", 
				"[Dev] C++ route track", true);
		RoutingParameter calcMode = new RoutingParameter("calcmode", "[Dev] Route mode", 
				"Algorithm to calculate route", null, RoutingParameterType.SYMBOLIC.name().toLowerCase());
		calcMode.section = "Development";
		calcMode.value = "";
		calcMode.valueDescriptions = new String[] {"Optimal", "Basic", "Slow"};
		calcMode.values = new String[] {RouteCalculationMode.COMPLEX.name(),
				RouteCalculationMode.BASE.name(),
				RouteCalculationMode.NORMAL.name()
		};
		RoutingParameter shortWay = new RoutingParameter("short_way", null, "Short way", false); 
		for (Map.Entry<String, GeneralRouter> e : RoutingConfiguration.getDefault().getAllRouters().entrySet()) {
			if (!e.getKey().equals("geocoding") && !e.getKey().equals("public_transport")) {
				RoutingMode rm;
				String derivedProfiles = e.getValue().getAttribute("derivedProfiles");
				if (derivedProfiles != null) {
					String[] derivedProfilesList = derivedProfiles.split(",");
					for (String profile : derivedProfilesList) {
						rm = new RoutingMode("default".equals(profile) ? e.getKey() : profile);
						routers.put(rm.key, rm);
						fillRoutingModeParams(nativeRouting, nativeTrack, calcMode, shortWay, e, rm);
					}
				} else {
					rm = new RoutingMode(e.getKey());
					routers.put(rm.key, rm);
					fillRoutingModeParams(nativeRouting, nativeTrack, calcMode, shortWay, e, rm);
				}
			}
		}
		for (RoutingServerConfigEntry rs : osmAndMapsService.getRoutingConfig().config.values()) {
			RoutingMode rm = new RoutingMode(rs.name);
			routers.put(rm.key, rm);
			rm.params.put(nativeRouting.key, nativeRouting);
			rm.params.put(nativeTrack.key, nativeTrack);
		}
		return ResponseEntity.ok(gson.toJson(routers));
	}

	private void fillRoutingModeParams(RoutingParameter nativeRouting, RoutingParameter nativeTrack, RoutingParameter calcMode,
	                                   RoutingParameter shortWay, Entry<String, GeneralRouter> e, RoutingMode rm) {
		List<RoutingParameter> rps = new ArrayList<>();
		rps.add(shortWay);
		for (Entry<String, GeneralRouter.RoutingParameter> epm : e.getValue().getParameters().entrySet()) {
			GeneralRouter.RoutingParameter pm = epm.getValue();
			String[] profiles = pm.getProfiles();
			if (profiles != null) {
				boolean accept = false;
				for (String profile : profiles) {
					if ("default".equals(profile) && rm.key.equals(e.getKey()) || rm.key.equals(profile)) {
						accept = true;
						break;
					}
				}
				if (!accept) {
					continue;
				}
			}
			RoutingParameter rp = new RoutingParameter(pm.getId(), pm.getName(), pm.getDescription(),
					pm.getGroup(), pm.getType().name().toLowerCase());
			if (pm.getId().equals("short_way")) {
				continue;
			}
			if (pm.getId().startsWith("avoid")) {
				rp.section = "Avoid";
			} else if (pm.getId().startsWith("allow") || pm.getId().startsWith("prefer")) {
				rp.section = "Allow";
			} else if (pm.getGroup() != null) {
				rp.section = Algorithms.capitalizeFirstLetter(pm.getGroup().replace('_', ' '));
			}
			if (pm.getType() == RoutingParameterType.BOOLEAN) {
				rp.value = pm.getDefaultBoolean();
			} else {
				if (pm.getType() == RoutingParameterType.NUMERIC) {
					rp.value = 0;
				} else {
					rp.value = "";
				}
				rp.valueDescriptions = pm.getPossibleValueDescriptions();
				rp.values = pm.getPossibleValues();
			}
			int lastIndex = -1;
			for (int i = 0; i < rps.size(); i++) {
				if (Algorithms.objectEquals(rp.section, rps.get(i).section)) {
					lastIndex = i;
				}
			}
			if (lastIndex != -1 && !Algorithms.isEmpty(rp.section)) {
				rps.add(lastIndex + 1, rp);
			} else {
				rps.add(rp);
			}
		}
		for (RoutingParameter rp : rps) {
			rm.params.put(rp.key, rp);
		}
		rm.params.put(nativeRouting.key, nativeRouting);
		rm.params.put(nativeTrack.key, nativeTrack);
		rm.params.put(calcMode.key, calcMode);
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
			List<LatLon> resList = new ArrayList<LatLon>();
			List<Feature> features = new ArrayList<Feature>();
			Map<String, Object> props = new TreeMap<>();
			try {
				List<RouteSegmentResult> res = osmAndMapsService.gpxApproximation(routeMode, props, gpxFile);
				convertResults(resList, features, res);
			} catch (IOException e) {
				LOGGER.error(e.getMessage(), e);
			} catch (InterruptedException e) {
				LOGGER.error(e.getMessage(), e);
			} catch (RuntimeException e) {
				LOGGER.error(e.getMessage(), e);
			}
			Feature route = new Feature(Geometry.lineString(resList));
			features.add(0, route);
			route.properties = props;
			return ResponseEntity.ok(gson.toJson(new FeatureCollection(features.toArray(new Feature[features.size()]))));
		}
	}
	
	@RequestMapping(path = "/geocoding", produces = {MediaType.APPLICATION_JSON_VALUE})
	public ResponseEntity<?> geocoding(@RequestParam double lat, @RequestParam double lon) throws IOException, InterruptedException {
		if (!osmAndMapsService.validateAndInitConfig()) {
			return errorConfig();
		}
		try {
			List<GeocodingResult> lst = osmAndMapsService.geocoding(lat, lon);
			List<Feature> features = new ArrayList<Feature>();
			int i = 0;
			for (GeocodingResult rr : lst) {
				i++;
				features.add(new Feature(Geometry.point(rr.getLocation())).prop("index", i).prop("description",
						i + ". " + rr.toString()));
			}
			return ResponseEntity.ok(gson.toJson(new FeatureCollection(features.toArray(new Feature[features.size()]))));
		} catch (IOException e) {
			LOGGER.error(e.getMessage(), e);
			throw e;
		} catch (InterruptedException e) {
			LOGGER.error(e.getMessage(), e);
			throw e;
		} catch (RuntimeException e) {
			LOGGER.error(e.getMessage(), e);
			throw e;
		}
	}
	
	
	@RequestMapping(path = "/search", produces = {MediaType.APPLICATION_JSON_VALUE})
	public ResponseEntity<?> search(@RequestParam double lat, @RequestParam double lon, @RequestParam String search) throws IOException, InterruptedException {
		if (!osmAndMapsService.validateAndInitConfig()) {
			return errorConfig();
		}
		try {
			List<SearchResult> res = osmAndMapsService.search(lat, lon, search);
			List<Feature> features = new ArrayList<Feature>();
			int pos = 0;
			int posLoc = 0;
			for (SearchResult sr : res) {
				pos++;
				if (sr.location != null) {
					posLoc++;
					double loc = MapUtils.getDistance(sr.location, lat, lon) / 1000.0;
					String typeString = "";
					if (!Algorithms.isEmpty(sr.localeRelatedObjectName)) {
						typeString += " " + sr.localeRelatedObjectName;
						if (sr.distRelatedObjectName != 0) {
							typeString += " " + (int) (sr.distRelatedObjectName / 1000.f) + " km";
						}
					}
					if (sr.objectType == ObjectType.HOUSE) {
						if (sr.relatedObject instanceof Street) {
							typeString += " " + ((Street) sr.relatedObject).getCity().getName();
						}
					}
					if (sr.objectType == ObjectType.LOCATION) {
						typeString += " " + osmAndMapsService.getOsmandRegions().getCountryName(sr.location);
					}
					if (sr.object instanceof Amenity) {
						typeString += " " + ((Amenity) sr.object).getSubType();
						if (((Amenity) sr.object).isClosed()) {
							typeString += " (CLOSED)";
						}
					}
					String r = String.format("%d. %s %s [%.2f km, %d, %s, %.2f] ", pos, sr.localeName, typeString, loc,
							sr.getFoundWordCount(), sr.objectType, sr.getUnknownPhraseMatchWeight());
					features.add(new Feature(Geometry.point(sr.location)).prop("description", r).
							prop("index", pos).prop("locindex", posLoc).prop("distance", loc).prop("type", sr.objectType));
				}
			}
			return ResponseEntity.ok(gson.toJson(new FeatureCollection(features.toArray(new Feature[features.size()]))));
		} catch (IOException e) {
			LOGGER.error(e.getMessage(), e);
			throw e;
		} catch (InterruptedException e) {
			LOGGER.error(e.getMessage(), e);
			throw e;
		} catch (RuntimeException e) {
			LOGGER.error(e.getMessage(), e);
			throw e;
		}
	}
	
	@RequestMapping(path = "/route", produces = {MediaType.APPLICATION_JSON_VALUE})
	public ResponseEntity<?> routing(@RequestParam String[] points, @RequestParam(defaultValue = "car") String routeMode)
			throws IOException, InterruptedException {
		if (!osmAndMapsService.validateAndInitConfig()) {
			return errorConfig();
		}
		List<LatLon> list = new ArrayList<LatLon>();
		double lat = 0;
		int k = 0;
		boolean tooLong = false;
		LatLon prev = null;
		for (String point : points) {
			String[] sl = point.split(",");
			for (String p : sl) {
				double vl = Double.parseDouble(p);
				if (k++ % 2 == 0) {
					lat = vl;
				} else {
					LatLon pnt = new LatLon(lat, vl);
					if (list.size() > 0) {
						tooLong = tooLong || MapUtils.getDistance(prev, pnt) > MAX_DISTANCE;
					}
					list.add(pnt);
					prev = pnt;
				}
			}
		}
		List<LatLon> resList = new ArrayList<LatLon>();
		List<Feature> features = new ArrayList<Feature>();
		Map<String, Object> props = new TreeMap<>();
		if (list.size() >= 2 && !tooLong) {
			try {
				List<RouteSegmentResult> res = osmAndMapsService.routing(routeMode, props, list.get(0), list.get(list.size() - 1),
						list.subList(1, list.size() - 1));
				if (res != null) {
					convertResults(resList, features, res);
				}
			} catch (IOException e) {
				LOGGER.error(e.getMessage(), e);
			} catch (InterruptedException e) {
				LOGGER.error(e.getMessage(), e);
			} catch (RuntimeException e) {
				LOGGER.error(e.getMessage(), e);
			}
		}
		if (resList.size() == 0) {
			resList = new ArrayList<LatLon>(list);
			calculateStraightLine(resList);
			float dist = 0;
			for (int i = 1; i < resList.size(); i++) {
				dist += MapUtils.getDistance(resList.get(i - 1), resList.get(i));
			}
			props.put("distance", dist);
		}
		Feature route = new Feature(Geometry.lineString(resList));
		route.properties = props;
		features.add(0, route);

		return ResponseEntity.ok(gson.toJson(new FeatureCollection(features.toArray(new Feature[features.size()]))));
	}

	private void convertResults(List<LatLon> resList, List<Feature> features, List<RouteSegmentResult> res) {
		LatLon last = null;
		for (RouteSegmentResult r : res) {
			int i;
			int dir = r.isForwardDirection() ? 1 : -1;
			if (r.getDescription() != null && r.getDescription().length() > 0) {
				features.add(new Feature(Geometry.point(r.getStartPoint())).prop("description", r.getDescription()));
			}
			for (i = r.getStartPointIndex(); ; i += dir) {
				if(i != r.getEndPointIndex()) {
					resList.add(r.getPoint(i));
				} else {
					last = r.getPoint(i);
					break;
				}
			}
		}
		if (last != null) {
			resList.add(last);
		}
	}

	private void calculateStraightLine(List<LatLon> list) {
		for (int i = 1; i < list.size();) {
			if (MapUtils.getDistance(list.get(i - 1), list.get(i)) > DISTANCE_MID_POINT) {
				LatLon midPoint = MapUtils.calculateMidPoint(list.get(i - 1), list.get(i));
				list.add(i, midPoint);
			} else {
				i++;
			}
		}
	}


}
