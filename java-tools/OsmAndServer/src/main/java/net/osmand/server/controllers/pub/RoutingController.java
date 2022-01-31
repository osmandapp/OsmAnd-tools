
package net.osmand.server.controllers.pub;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import com.google.gson.Gson;

import net.osmand.data.LatLon;
import net.osmand.router.RouteSegmentResult;
import net.osmand.server.api.services.OsmAndMapsService;
import net.osmand.server.api.services.OsmAndMapsService.VectorTileServerConfig;
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
		if (list.size() >= 2) {
			LatLon last = null;
			try {
				List<RouteSegmentResult> res = osmAndMapsService.routing(routeMode, list.get(0), list.get(list.size() - 1),
						list.subList(1, list.size() - 1));
				for (RouteSegmentResult r : res) {
					int i;
					int dir = r.isForwardDirection() ? 1 : -1;
					for (i = r.getStartPointIndex(); i != r.getEndPointIndex(); i += dir) {
						resList.add(r.getPoint(i));
					}
					last = r.getPoint(i);
				}
				if (last != null) {
					resList.add(last);
				}
			} catch (IOException e) {
				LOGGER.error(e.getMessage(), e);
			} catch (InterruptedException e) {
				LOGGER.error(e.getMessage(), e);
			} catch (RuntimeException e) {
				LOGGER.error(e.getMessage());
			}
		}
		if (resList.size() == 0) {
			resList = new ArrayList<LatLon>(list);
			calculateStraightLine(resList);
		}

		Feature feature = new Feature(Geometry.lineString(resList));
		return ResponseEntity.ok(gson.toJson(new FeatureCollection(feature)));
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
