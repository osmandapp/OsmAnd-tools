package net.osmand.server.api.services;

import net.osmand.GPXUtilities;
import net.osmand.Location;
import net.osmand.binary.BinaryMapRouteReaderAdapter;
import net.osmand.binary.RouteDataBundle;
import net.osmand.binary.StringBundle;
import net.osmand.data.LatLon;
import net.osmand.router.GeneralRouter;
import net.osmand.router.RouteDataResources;
import net.osmand.router.RouteSegmentResult;
import net.osmand.server.controllers.pub.RoutingController;
import net.osmand.server.utils.WebGpxParser;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;

@Service
public class RoutingService {
    
    private static final int DISTANCE_MID_POINT = 25000;
    @Autowired
    OsmAndMapsService osmAndMapsService;
    
    @Autowired
    WebGpxParser webGpxParser;
    
    public List<WebGpxParser.Point> updateRouteBetweenPoints(LatLon startLatLon, LatLon endLatLon,
                                                             String routeMode, boolean hasSpeed, boolean hasRouting) throws IOException, InterruptedException {
        Map<String, Object> props = new TreeMap<>();
        List<Location> locations = new ArrayList<>();
        List<RouteSegmentResult> routeSegmentResults = osmAndMapsService.routing(routeMode, props, startLatLon,
                endLatLon, Collections.emptyList(), Collections.emptyList());
        
        List<WebGpxParser.Point> pointsRes = getPoints(routeSegmentResults, locations);
        
        if (!pointsRes.isEmpty()) {
            GPXUtilities.TrkSegment seg = generateRouteSegments(routeSegmentResults, locations);
            if (hasRouting) {
                webGpxParser.addRouteSegmentsToPoints(seg, pointsRes);
            }
            if (hasSpeed) {
                addSpeed(seg, pointsRes);
            }
            addDistance(pointsRes);
        }
        return pointsRes;
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
        RouteDataResources resources = new RouteDataResources(locations);
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
    
    private void addSpeed(GPXUtilities.TrkSegment seg, List<WebGpxParser.Point> pointsRes) {
        int startInd = 0;
        if (!seg.routeSegments.isEmpty()) {
            for (GPXUtilities.RouteSegment rs : seg.routeSegments) {
                int length = Integer.parseInt(rs.length);
                for (int i = startInd; i < startInd + (length - 1); i++) {
                    pointsRes.get(i).ext.speed = Double.parseDouble(rs.speed);
                }
                startInd = startInd + (length - 1);
            }
        }
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
                    LatLon point = r.getPoint(stInd);
                    locations.add(new Location("", point.getLatitude(), point.getLongitude()));
                    GPXUtilities.WptPt pt = new GPXUtilities.WptPt();
                    if (heightArray != null && heightArray.length > stInd * 2 + 1) {
                        pt.ele = heightArray[stInd * 2 + 1];
                    }
                    pt.lat = point.getLatitude();
                    pt.lon = point.getLongitude();
                    pointsRes.add(new WebGpxParser.Point(pt));
                    stInd += ((stInd < endInd) ? 1 : -1);
                }
            }
            return pointsRes;
        }
        return Collections.emptyList();
    }
}
