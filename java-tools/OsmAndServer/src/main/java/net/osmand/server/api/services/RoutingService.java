package net.osmand.server.api.services;

import net.osmand.GPXUtilities;
import net.osmand.Location;
import net.osmand.binary.BinaryMapRouteReaderAdapter;
import net.osmand.binary.RouteDataBundle;
import net.osmand.binary.StringBundle;
import net.osmand.data.LatLon;
import net.osmand.router.RouteDataResources;
import net.osmand.router.RouteSegmentResult;
import net.osmand.server.utils.WebGpxParser;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;

@Service
public class RoutingService {
    
    @Autowired
    OsmAndMapsService osmAndMapsService;
    
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
                addRouteSegmentsToPoints(seg, pointsRes);
            }
            if (hasSpeed) {
                addSpeed(seg, pointsRes);
            }
            addDistance(pointsRes);
        }
        return pointsRes;
    }
    
    public void addRouteSegmentsToPoints(GPXUtilities.TrkSegment seg, List<WebGpxParser.Point> points) {
        int startInd = 0;
        if (!seg.routeSegments.isEmpty()) {
            for (GPXUtilities.RouteSegment rs : seg.routeSegments) {
                WebGpxParser.RouteSegment segment = new WebGpxParser.RouteSegment();
                segment.ext = rs;
                segment.routeTypes = seg.routeTypes;
                int length = Integer.parseInt(rs.length);
                points.get(startInd).segment = segment;
                startInd = startInd + (length - 1);
            }
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
