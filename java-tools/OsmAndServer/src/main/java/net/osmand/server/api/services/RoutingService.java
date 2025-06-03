package net.osmand.server.api.services;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import net.osmand.data.LatLonEle;
import net.osmand.shared.gpx.ElevationApproximator;
import net.osmand.router.*;
import net.osmand.shared.gpx.ElevationDiffsCalculator;
import net.osmand.shared.gpx.GpxUtilities;
import net.osmand.shared.gpx.primitives.TrkSegment;
import net.osmand.shared.gpx.primitives.WptPt;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import net.osmand.Location;
import net.osmand.binary.BinaryMapRouteReaderAdapter;
import net.osmand.binary.RouteDataBundle;
import net.osmand.shared.util.StringBundle;
import net.osmand.data.LatLon;
import net.osmand.server.controllers.pub.RoutingController;
import net.osmand.server.utils.WebGpxParser;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;

import static net.osmand.gpx.GPXUtilities.GAP_PROFILE_TYPE;
import static net.osmand.server.utils.WebGpxParser.LINE_PROFILE_TYPE;
import static net.osmand.server.controllers.pub.GeojsonClasses.*;
@Service
public class RoutingService {

    private static final int DISTANCE_MID_POINT = 25000;
    @Autowired
    OsmAndMapsService osmAndMapsService;

    @Autowired
    WebGpxParser webGpxParser;

	public List<WebGpxParser.Point> updateRouteBetweenPoints(LatLon startLatLon, LatLon endLatLon, String routeMode,
			boolean hasRouting, boolean disableOldRouting, RouteCalculationProgress progress)
			throws IOException, InterruptedException {
        Map<String, Object> props = new TreeMap<>();
        List<Location> locations = new ArrayList<>();
        List<WebGpxParser.Point> pointsRes;
        List<RouteSegmentResult> routeSegmentResults;
        List<WebGpxParser.Point> lineRes = getStraightLine(startLatLon.getLatitude(), startLatLon.getLongitude(),
                endLatLon.getLatitude(), endLatLon.getLongitude());
        if (routeMode.equals(LINE_PROFILE_TYPE)) {
            return lineRes;
        } else {
            routeSegmentResults = osmAndMapsService.routing(disableOldRouting, routeMode, props, startLatLon, endLatLon,
                    Collections.emptyList(), Collections.emptyList(), progress);
            pointsRes = getPoints(routeSegmentResults, locations);
        }

        if (pointsRes.isEmpty()) {
            return lineRes;
        }

        TrkSegment seg = generateRouteSegments(routeSegmentResults, locations);
        if (hasRouting) {
            webGpxParser.addRouteSegmentsToPoints(seg, pointsRes, true, null);
        }
        addDistance(pointsRes);
        return pointsRes;
    }

    public synchronized List<WebGpxParser.Point> approximateRoute(List<WebGpxParser.Point> points, String routeMode) throws IOException, InterruptedException {
        List<Location> locations = new ArrayList<>();
        List<RouteSegmentResult> approximateResult = osmAndMapsService.approximateRoute(points, routeMode);
        List<WebGpxParser.Point> gpxPoints = getPoints(approximateResult, locations);
        if (!gpxPoints.isEmpty()) {
            TrkSegment seg = generateRouteSegments(approximateResult, locations);
            webGpxParser.addRouteSegmentsToPoints(seg, gpxPoints, true, null);
            addDistance(gpxPoints);
        }
        return gpxPoints;
    }

    public List<WebGpxParser.Point> getRoute(List<WebGpxParser.Point> points) throws IOException, InterruptedException {
        List<WebGpxParser.Point> res = new ArrayList<>();
        res.add(points.get(0));
        for (int i = 1; i < points.size(); i++) {
            WebGpxParser.Point prevPoint = points.get(i - 1);
            WebGpxParser.Point currentPoint = points.get(i);
            LatLon prevCoord = new LatLon(prevPoint.lat, prevPoint.lng);
            LatLon currentCoord = new LatLon(currentPoint.lat, currentPoint.lng);
            if (prevPoint.profile == null || prevPoint.profile.equals(LINE_PROFILE_TYPE)) {
                currentPoint.geometry = (getStraightLine(prevPoint.lat, prevPoint.lng, currentPoint.lat, currentPoint.lng));
            } else if (!prevPoint.profile.equals(GAP_PROFILE_TYPE)) {
                currentPoint.geometry = (updateRouteBetweenPoints(prevCoord, currentCoord, prevPoint.profile, true, false, null));
            }
            res.add(currentPoint);
        }
        return res;
    }

    private List<WebGpxParser.Point> getStraightLine(double lat1, double lng1, double lat2, double lng2) {
        List<WebGpxParser.Point> line = new ArrayList<>();
        WebGpxParser.Point firstP = new WebGpxParser.Point();
        firstP.lat = (lat1);
        firstP.lng = (lng1);
        WebGpxParser.Point lastP = new WebGpxParser.Point();
        lastP.lat = (lat2);
        lastP.lng = (lng2);
        line.add(firstP);
        line.add(lastP);

        return line;
    }

    public void fillRoutingModeParams(List<RoutingController.RoutingParameter> passParams,
                                      RoutingController.RoutingParameter shortWay,
                                      Map.Entry<String, GeneralRouter> e, RoutingController.RoutingMode rm) {

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
        passParams.forEach(p -> rm.params.put(p.key, p));
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

    public void interpolateEmptyElevationSegments(List<LatLonEle> points) {
        List <WptPt> waypoints = new ArrayList<>();
        for (LatLonEle point : points) {
            WptPt waypoint = new WptPt(point.getLatitude(), point.getLongitude());
            waypoint.setEle(point.getElevation());
            waypoints.add(waypoint);
        }
        GpxUtilities.INSTANCE.interpolateEmptyElevationWpts(waypoints);
        for (int i = 0; i < waypoints.size(); i++) {
            points.get(i).setElevation((float)waypoints.get(i).getEle());
        }
    }

    public List<LatLonEle> getElevationsBySegments(List<LatLonEle> resListEle,
                                            List<Feature> features, List<RouteSegmentResult> res) {
        for (int i = 0; i < res.size(); i++) {
            RouteSegmentResult r = res.get(i);

            final int dir = r.isForwardDirection() ? 1 : -1;
            final int start = r.getStartPointIndex();
            final int end = r.getEndPointIndex();

            // calculate and validate heights
            final float[] heightArray = r.getObject().calculateHeightArray();
            final boolean isHeightsValid = heightArray.length / 2 == r.getObject().getPointsLength();

            // add points: very last segment should include very last point (end+dir)
            final int lastPointIndex = (i == res.size() - 1) ? (end + dir) : end;
            for (int j = start; j != lastPointIndex; j += dir) {
                if (r.getPoint(j) != null) {
                    double lat = r.getPoint(j).getLatitude();
                    double lon = r.getPoint(j).getLongitude();
                    if (isHeightsValid) {
                        float ele = heightArray[j * 2 + 1];
                        resListEle.add(new LatLonEle(lat, lon, ele));
                    } else {
                        resListEle.add(new LatLonEle(lat, lon)); // NaN elevation will be excluded from results
                    }
                }
            }

            // process (segment/turn) description
            String description = r.getDescription(true);
            if (description != null && description.length() > 0) {
                Geometry point;

                if (isHeightsValid) {
                    float ele = heightArray[start * 2 + 1];
                    double lat = r.getStartPoint().getLatitude();
                    double lon = r.getStartPoint().getLongitude();
                    point = Geometry.pointElevation(new LatLonEle(lat, lon, ele));
                } else {
                    point = Geometry.point(r.getStartPoint());
                }

                Feature f = new Feature(point);
                f.prop("description", description).prop("routingTime", r.getRoutingTime())
                        .prop("segmentTime", r.getRoutingTime()).prop("segmentSpeed", r.getRoutingTime())
                        .prop("roadId", r.getObject().getId());
                features.add(f);
            }
        }
        return resListEle;
    }
    
    public List<Double> calculateElevationDiffs(List<LatLonEle> points) {
        ElevationApproximator approximator = getElevationApproximator(points);
        approximator.approximate();
        final double[] distances = approximator.getDistances();
        final double[] elevations = approximator.getElevations();
        final int[] indexes = approximator.getSurvivedIndexes();
        
        if (distances != null && elevations != null && indexes != null) {
            ElevationDiffsCalculator elevationDiffsCalc = getElevationDiffsCalculator(distances, elevations, indexes);
            elevationDiffsCalc.calculateElevationDiffs();
            double diffElevationUp = elevationDiffsCalc.getDiffElevationUp();
            double diffElevationDown = elevationDiffsCalc.getDiffElevationDown();
            return List.of(diffElevationUp, diffElevationDown);
        }
        return Collections.emptyList();
    }
    
    private ElevationDiffsCalculator getElevationDiffsCalculator(final double[] distances, final double[] elevations, final int[] indexes) {
        return new ElevationDiffsCalculator() {
            @Override
            public double getPointDistance(int index) {
                return distances[index];
            }
            
            @Override
            public double getPointElevation(int index) {
                return elevations[index];
            }
            
            @Override
            public int getPointIndex(int index) {
                return indexes[index];
            }

            @Override
            public int getPointsCount() {
                return distances.length;
            }
        };
    }
    
    private ElevationApproximator getElevationApproximator(List<LatLonEle> points) {
        return new ElevationApproximator() {
            @Override
            public double getPointLatitude(int index) {
                return points.get(index).getLatitude();
            }
            
            @Override
            public double getPointLongitude(int index) {
                return points.get(index).getLongitude();
            }
            
            @Override
            public double getPointElevation(int index) {
                return points.get(index).getElevation();
            }
            
            @Override
            public int getPointsCount() {
                return points.size();
            }
        };
    }
    
    public void convertResults(List<LatLon> resList, List<Feature> features, List<RouteSegmentResult> res) {
        LatLon last = null;
        for (RouteSegmentResult r : res) {
            int i;
            int dir = r.isForwardDirection() ? 1 : -1;
            String description = r.getDescription(true);
            if (!Algorithms.isEmpty(description)) {
                Feature f = new Feature(Geometry.point(r.getStartPoint()));
                f.prop("description", description).prop("routingTime", r.getRoutingTime())
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

    private TrkSegment generateRouteSegments(List<RouteSegmentResult> route, List<Location> locations) {
        TrkSegment trkSegment = new TrkSegment();
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
                routeItems.add(toKotlinStringBundle(itemBundle));
            }
        }
        List<StringBundle> typeList = new ArrayList<>();
        Map<BinaryMapRouteReaderAdapter.RouteTypeRule, Integer> rules = resources.getRules();
        for (BinaryMapRouteReaderAdapter.RouteTypeRule rule : rules.keySet()) {
            RouteDataBundle typeBundle = new RouteDataBundle(resources);
            rule.writeToBundle(typeBundle);
            typeList.add(toKotlinStringBundle(typeBundle));
        }

        if (locations.isEmpty()) {
            return trkSegment;
        }

        List<GpxUtilities.RouteSegment> routeSegments = new ArrayList<>();
        for (StringBundle item : routeItems) {
            routeSegments.add(GpxUtilities.RouteSegment.Companion.fromStringBundle(item));
        }
        trkSegment.setRouteSegments(routeSegments);

        List<GpxUtilities.RouteType> routeTypes = new ArrayList<>();
        for (StringBundle item : typeList) {
            routeTypes.add(GpxUtilities.RouteType.Companion.fromStringBundle(item));
        }
        trkSegment.setRouteTypes(routeTypes);

        return trkSegment;
    }

    public StringBundle toKotlinStringBundle(RouteDataBundle routeDataBundle) {
        StringBundle kotlinBundle = new StringBundle();
        for (Map.Entry<String, net.osmand.binary.StringBundle.Item<?>> entry : routeDataBundle.getMap().entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue().getValue() != null ? entry.getValue().getValue().toString() : null;
            if (value != null) {
                kotlinBundle.putString(key, value);
            }
        }
        return kotlinBundle;
    }

    private void addDistance(List<WebGpxParser.Point> pointsRes) {
        for (int i = 1; i < pointsRes.size(); i++) {
            WebGpxParser.Point curr = pointsRes.get(i);
            WebGpxParser.Point prev = pointsRes.get(i - 1);
            pointsRes.get(i).distance = ((float) MapUtils.getDistance(prev.lat, prev.lng, curr.lat, curr.lng));
        }
    }

   public List<WebGpxParser.Point> getPoints(List<RouteSegmentResult> routeSegmentResults, List<Location> locations) {
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

            List <WptPt> waypoints = new ArrayList<>();
            pointsRes.forEach(p -> {
                    WptPt waypoint = new WptPt(p.lat, p.lng);
                    waypoint.setEle(p.ele);
                    waypoints.add(waypoint);
            });
            GpxUtilities.INSTANCE.interpolateEmptyElevationWpts(waypoints);
            for (int i = 0; i < waypoints.size(); i++) {
                pointsRes.get(i).ele = (waypoints.get(i).getEle());
            }
            return pointsRes;
        }
        return Collections.emptyList();
    }

    private void getPoint(int ind, RouteSegmentResult r, List<Location> locations, float[] heightArray, List<WebGpxParser.Point> pointsRes) {
        LatLon point = r.getPoint(ind);
        locations.add(new Location("", point.getLatitude(), point.getLongitude()));
        WptPt pt = new WptPt();
        if (heightArray != null && heightArray.length > ind * 2 + 1) {
            pt.setEle(heightArray[ind * 2 + 1]);
        }
        pt.setLat(point.getLatitude());
        pt.setLon(point.getLongitude());
        pointsRes.add(new WebGpxParser.Point(pt));
    }
}
