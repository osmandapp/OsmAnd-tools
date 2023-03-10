package net.osmand.server.api.services;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import net.osmand.Location;
import net.osmand.binary.BinaryMapRouteReaderAdapter;
import net.osmand.binary.RouteDataBundle;
import net.osmand.binary.StringBundle;
import net.osmand.data.LatLon;
import net.osmand.gpx.GPXUtilities;
import net.osmand.router.GeneralRouter;
import net.osmand.router.RouteDataResources;
import net.osmand.router.RouteSegmentResult;
import net.osmand.server.controllers.pub.RoutingController;
import net.osmand.server.utils.WebGpxParser;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;

import static net.osmand.gpx.GPXUtilities.GAP_PROFILE_TYPE;
import static net.osmand.server.utils.WebGpxParser.LINE_PROFILE_TYPE;

@Service
public class RoutingService {
    
    private static final int DISTANCE_MID_POINT = 25000;
    @Autowired
    OsmAndMapsService osmAndMapsService;
    
    @Autowired
    WebGpxParser webGpxParser;
    
    Gson gsonWithNans = new GsonBuilder().serializeSpecialFloatingPointValues().create();
    
    public List<WebGpxParser.Point> updateRouteBetweenPoints(LatLon startLatLon, LatLon endLatLon, String routeMode, boolean hasRouting, boolean isLongDist) throws IOException, InterruptedException {
        Map<String, Object> props = new TreeMap<>();
        List<Location> locations = new ArrayList<>();
        List<WebGpxParser.Point> pointsRes;
        List<RouteSegmentResult> routeSegmentResults = new ArrayList<>();
        if (routeMode.equals(LINE_PROFILE_TYPE) || isLongDist) {
            pointsRes = getStraightLine(startLatLon.getLatitude(), startLatLon.getLongitude(), endLatLon.getLatitude(), endLatLon.getLongitude());
        } else {
            routeSegmentResults = osmAndMapsService.routing(routeMode, props, startLatLon,
                    endLatLon, Collections.emptyList(), Collections.emptyList());
    
            pointsRes = getPoints(routeSegmentResults, locations);
        }
        
        if (!pointsRes.isEmpty()) {
            GPXUtilities.TrkSegment seg = generateRouteSegments(routeSegmentResults, locations);
            if (hasRouting) {
                webGpxParser.addRouteSegmentsToPoints(seg, pointsRes);
            }
            addDistance(pointsRes);
        }
        return pointsRes;
    }
    
    public List<WebGpxParser.Point> getRoute(List<WebGpxParser.Point> points) throws IOException, InterruptedException {
        List<WebGpxParser.Point> res = new ArrayList<>();
        res.add(points.get(0));
        for (int i = 1; i < points.size(); i++) {
            WebGpxParser.Point prevPoint = points.get(i - 1);
            WebGpxParser.Point currentPoint = points.get(i);
            LatLon prevCoord = new LatLon(prevPoint.lat, prevPoint.lng);
            LatLon currentCoord = new LatLon(currentPoint.lat, currentPoint.lng);
            if (prevPoint.profile.equals(LINE_PROFILE_TYPE)) {
                currentPoint.geometry = getStraightLine(prevPoint.lat, prevPoint.lng, currentPoint.lat, currentPoint.lng);
            } else if (!prevPoint.profile.equals(GAP_PROFILE_TYPE)) {
                currentPoint.geometry = updateRouteBetweenPoints(prevCoord, currentCoord, prevPoint.profile, true, false);
            }
            res.add(currentPoint);
        }
        return res;
    }
    
    private List<WebGpxParser.Point> getStraightLine(double lat1, double lng1, double lat2, double lng2) {
        List<WebGpxParser.Point> line = new ArrayList<>();
        WebGpxParser.Point firstP = new WebGpxParser.Point();
        firstP.lat = lat1;
        firstP.lng = lng1;
        WebGpxParser.Point lastP = new WebGpxParser.Point();
        lastP.lat = lat2;
        lastP.lng = lng2;
        line.add(firstP);
        line.add(lastP);
        
        return line;
    }
    
    public void fillRoutingModeParams(RoutingController.RoutingParameter nativeRouting, RoutingController.RoutingParameter nativeTrack, RoutingController.RoutingParameter calcMode,
                                       RoutingController.RoutingParameter shortWay, Map.Entry<String, GeneralRouter> e, RoutingController.RoutingMode rm) {
        List<RoutingController.RoutingParameter> rps = new ArrayList<>();
        rps.add(shortWay);
        for (Map.Entry<String, GeneralRouter.RoutingParameter> epm : e.getValue().getParameters().entrySet()) {
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
            RoutingController.RoutingParameter rp = new RoutingController.RoutingParameter(pm.getId(), pm.getName(), pm.getDescription(),
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
            if (pm.getType() == GeneralRouter.RoutingParameterType.BOOLEAN) {
                rp.value = pm.getDefaultBoolean();
            } else {
                if (pm.getType() == GeneralRouter.RoutingParameterType.NUMERIC) {
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
        for (RoutingController.RoutingParameter rp : rps) {
            rm.params.put(rp.key, rp);
        }
        rm.params.put(nativeRouting.key, nativeRouting);
        rm.params.put(nativeTrack.key, nativeTrack);
        rm.params.put(calcMode.key, calcMode);
    }
    
    public void calculateStraightLine(List<LatLon> list) {
        for (int i = 1; i < list.size();) {
            if (MapUtils.getDistance(list.get(i - 1), list.get(i)) > DISTANCE_MID_POINT) {
                LatLon midPoint = MapUtils.calculateMidPoint(list.get(i - 1), list.get(i));
                list.add(i, midPoint);
            } else {
                i++;
            }
        }
    }
    
    public void convertResults(List<LatLon> resList, List<RoutingController.Feature> features, List<RouteSegmentResult> res) {
        LatLon last = null;
        for (RouteSegmentResult r : res) {
            int i;
            int dir = r.isForwardDirection() ? 1 : -1;
            if (r.getDescription() != null && r.getDescription().length() > 0) {
                RoutingController.Feature f = new RoutingController.Feature(RoutingController.Geometry.point(r.getStartPoint()));
                f.prop("description", r.getDescription()).prop("routingTime", r.getRoutingTime())
                        .prop("segmentTime", r.getRoutingTime()).prop("segmentSpeed", r.getRoutingTime())
                        .prop("roadId", r.getObject().getId());
                features.add(f);
                
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
    
    private GPXUtilities.TrkSegment generateRouteSegments(List<RouteSegmentResult> route, List<Location> locations) {
        GPXUtilities.TrkSegment trkSegment = new GPXUtilities.TrkSegment();
        RouteDataResources resources = new RouteDataResources(locations, Collections.emptyList());
        List<StringBundle> routeItems = new ArrayList<>();
        if (!Algorithms.isEmpty(route)) {
            for (RouteSegmentResult sr : route) {
                sr.collectTypes(resources);
            }
            for (RouteSegmentResult sr : route) {
                sr.collectNames(resources);
            }
            
            for (RouteSegmentResult sr : route) {
                RouteDataBundle itemBundle = new RouteDataBundle(resources);
                sr.writeToBundle(itemBundle);
                routeItems.add(itemBundle);
            }
        }
        List<StringBundle> typeList = new ArrayList<>();
        Map<BinaryMapRouteReaderAdapter.RouteTypeRule, Integer> rules = resources.getRules();
        for (BinaryMapRouteReaderAdapter.RouteTypeRule rule : rules.keySet()) {
            RouteDataBundle typeBundle = new RouteDataBundle(resources);
            rule.writeToBundle(typeBundle);
            typeList.add(typeBundle);
        }
        
        if (locations.isEmpty()) {
            return trkSegment;
        }
        
        List<GPXUtilities.RouteSegment> routeSegments = new ArrayList<>();
        for (StringBundle item : routeItems) {
            routeSegments.add(GPXUtilities.RouteSegment.fromStringBundle(item));
        }
        trkSegment.routeSegments = routeSegments;
        
        List<GPXUtilities.RouteType> routeTypes = new ArrayList<>();
        for (StringBundle item : typeList) {
            routeTypes.add(GPXUtilities.RouteType.fromStringBundle(item));
        }
        trkSegment.routeTypes = routeTypes;
        
        return trkSegment;
    }
    
    private void addDistance(List<WebGpxParser.Point> pointsRes) {
        for (int i = 1; i < pointsRes.size(); i++) {
            WebGpxParser.Point curr = pointsRes.get(i);
            WebGpxParser.Point prev = pointsRes.get(i - 1);
            pointsRes.get(i).distance = (float) MapUtils.getDistance(prev.lat, prev.lng, curr.lat, curr.lng);
        }
    }
    
    private List<WebGpxParser.Point> getPoints(List<RouteSegmentResult> routeSegmentResults, List<Location> locations) {
        if (routeSegmentResults != null) {
            List<WebGpxParser.Point> pointsRes = new ArrayList<>();
            for (RouteSegmentResult r : routeSegmentResults) {
                float[] heightArray = r.getObject().calculateHeightArray();
                int stInd = r.getStartPointIndex();
                int endInd = r.getEndPointIndex();
                while (stInd != endInd) {
                    getPoint(stInd, r, locations, heightArray, pointsRes);
                    stInd += ((stInd < endInd) ? 1 : -1);
                }
                if (routeSegmentResults.indexOf(r) == routeSegmentResults.size() - 1) {
                    getPoint(endInd, r, locations, heightArray, pointsRes);
                }
            }
            return pointsRes;
        }
        return Collections.emptyList();
    }
    
    private void getPoint(int ind, RouteSegmentResult r, List<Location> locations, float[] heightArray, List<WebGpxParser.Point> pointsRes) {
        LatLon point = r.getPoint(ind);
        locations.add(new Location("", point.getLatitude(), point.getLongitude()));
        GPXUtilities.WptPt pt = new GPXUtilities.WptPt();
        if (heightArray != null && heightArray.length > ind * 2 + 1) {
            pt.ele = heightArray[ind * 2 + 1];
        }
        pt.lat = point.getLatitude();
        pt.lon = point.getLongitude();
        pointsRes.add(new WebGpxParser.Point(pt));
    }
}
