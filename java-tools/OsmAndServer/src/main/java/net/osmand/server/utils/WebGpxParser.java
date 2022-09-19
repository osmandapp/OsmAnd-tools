package net.osmand.server.utils;

import net.osmand.GPXUtilities;
import net.osmand.Location;
import net.osmand.binary.BinaryMapRouteReaderAdapter;
import net.osmand.binary.RouteDataBundle;
import net.osmand.binary.StringBundle;
import net.osmand.data.LatLon;
import net.osmand.osm.edit.Entity;
import net.osmand.osm.edit.Way;
import net.osmand.router.RouteDataResources;
import net.osmand.router.RouteSegmentResult;
import net.osmand.server.api.services.OsmAndMapsService;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static net.osmand.GPXUtilities.*;
import static net.osmand.router.RouteExporter.OSMAND_ROUTER_V2;

@Component
public class WebGpxParser {
    
    @Autowired
    OsmAndMapsService osmAndMapsService;
    
    public static class TrackData {
        public MetaData metaData;
        public List<Wpt> wpts;
        public List<Track> tracks;
        public Map<String, Object> analysis;
        public Map<String, String> ext;
    }
    
    public static class MetaData {
        public String name;
        public String desc;
        public GPXUtilities.Metadata ext;
        
        public MetaData(GPXUtilities.Metadata data) {
            if (data != null) {
                if (data.desc != null) {
                    desc = data.desc;
                    data.desc = null;
                }
                if (data.name != null) {
                    name = data.name;
                    data.name = null;
                }
                ext = data;
            }
        }
    }
    
    public static class Wpt {
        public GPXUtilities.WptPt ext;
        
        public Wpt(GPXUtilities.WptPt point) {
            if (point != null)
                ext = point;
        }
    }
    
    public class Track {
        public List<Point> points;
        public GPXUtilities.Track ext;
        
        public Track(GPXUtilities.Track track) {
            points = new ArrayList<>();
            track.segments.forEach(seg -> {
                List<Point> pointsSeg = new ArrayList<>();
                seg.points.forEach(point -> {
                    int index = seg.points.indexOf(point);
                    Point p = new Point(point);
                    if (track.segments.size() > 1 && index == seg.points.size() - 1 && track.segments.indexOf(seg) != track.segments.size() - 1) {
                        p.profile = GAP_PROFILE_TYPE;
                    }
                    pointsSeg.add(p);
                });
                addPointRouteSegment(seg, pointsSeg);
                points.addAll(pointsSeg);
            });
            
            if (!points.isEmpty()) {
                track.segments = null;
            }
            ext = track;
        }
    }
    
    public static class Point {
        
        public double lat;
        public double lng;
        public double ele = Double.NaN;
        public double srtmEle = Double.NaN;
        public double distance;
        public String profile;
        public List<Point> geometry;
        public transient int geometrySize;
        public RouteSegment segment; // on each turn point
        public GPXUtilities.WptPt ext;
        
        public Point(GPXUtilities.WptPt point) {
            if (!Double.isNaN(point.lat)) {
                lat = point.lat;
                point.lat = Double.NaN;
            }
            
            if (!Double.isNaN(point.lon)) {
                lng = point.lon;
                point.lon = Double.NaN;
            }
            
            if (!Double.isNaN(point.ele)) {
                ele = point.ele;
                point.ele = Double.NaN;
            }
            
            Iterator<Map.Entry<String, String>> it = point.getExtensionsToWrite().entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<String, String> e = it.next();
                if (e.getKey().equals(TRKPT_INDEX_EXTENSION)) {
                    geometry = new ArrayList<>();
                    geometrySize = Integer.parseInt(e.getValue());
                    it.remove();
                } else {
                    geometrySize = -1;
                }
                if (e.getKey().equals(PROFILE_TYPE_EXTENSION)) {
                    profile = e.getValue();
                    it.remove();
                }
            }
            ext = point;
        }
        
    }
    
    public static class RouteSegment {
        public GPXUtilities.RouteSegment ext;
        public List<GPXUtilities.RouteType> routeTypes;
    }
    
    public void addPointRouteSegment(TrkSegment seg, List<Point> points) {
        int startInd = 0;
        if (!seg.routeSegments.isEmpty()) {
            for (GPXUtilities.RouteSegment rs : seg.routeSegments) {
                RouteSegment segment = new RouteSegment();
                segment.ext = rs;
                segment.routeTypes = seg.routeTypes;
                int length = Integer.parseInt(rs.length);
                points.get(startInd).segment = segment;
                for (int i = startInd; i < startInd + (length - 1); i++ ) {
                    points.get(i).ext.speed = Double.parseDouble(rs.speed);
                }
                startInd = startInd + (length - 1);
            }
        }
    }
    
    public void addRoutePoints(GPXUtilities.GPXFile gpxFile, TrackData gpxData) {
        gpxFile.routes.forEach(route -> {
            List<Point> routePoints = new ArrayList<>();
            List<Point> trackPoints = gpxData.tracks.get(gpxFile.routes.indexOf(route)).points;
            int prevTrkPointInd = -1;
            for (GPXUtilities.WptPt p : route.points) {
                Point routePoint = new Point(p);
                int currTrkPointInd;
                if (routePoint.geometrySize == -1) {
                    currTrkPointInd = findNearestPoint(trackPoints, routePoint);
                } else {
                    currTrkPointInd = routePoint.geometrySize;
                }
                
                prevTrkPointInd = addTrkptToRoutePoint(currTrkPointInd, prevTrkPointInd, routePoint, trackPoints, routePoints);
            }
            gpxData.tracks.get(gpxFile.routes.indexOf(route)).points = routePoints;
        });
    }
    
    public int findNearestPoint(List<Point> trackPoints, Point routePoint) {
        double minDist = -1;
        int res = -1;
        for (Point tp : trackPoints) {
            double currentDist = MapUtils.getDistance(routePoint.lat, routePoint.lng, tp.lat, tp.lng);
            if (minDist == -1) {
                minDist = currentDist;
            } else if (currentDist < minDist) {
                minDist = currentDist;
                res = trackPoints.indexOf(tp);
            }
        }
        return res;
    }
    
    public int addTrkptToRoutePoint(int currTrkPointInd, int prevTrkPointInd, Point routePoint, List<Point> trackPoints, List<Point> routePoints) {
        if (currTrkPointInd != 0) {
            for (Point pt : trackPoints) {
                int pointInd = trackPoints.indexOf(pt);
                if (pointInd >= prevTrkPointInd && pointInd <= currTrkPointInd) {
                    if (routePoint.geometry == null) {
                        routePoint.geometry = new ArrayList<>();
                    }
                    routePoint.geometry.add(pt);
                }
            }
            prevTrkPointInd = currTrkPointInd;
            routePoints.add(routePoint);
        } else {
            routePoints.add(routePoint);
        }
        return prevTrkPointInd;
    }
    
    public void addSrtmEle(List<Track> tracks, GPXUtilities.GPXTrackAnalysis srtmAnalysis) {
        if (srtmAnalysis != null) {
            tracks.forEach(track -> track.points.forEach(point -> {
                if (point.geometry != null) {
                    point.geometry.forEach(p -> p.srtmEle = srtmAnalysis.elevationData.get(point.geometry.indexOf(p)).elevation);
                } else {
                    track.points.forEach(p -> p.srtmEle = srtmAnalysis.elevationData.get(track.points.indexOf(p)).elevation);
                }
            }));
        }
    }
    
    public void addDistance(List<Track> tracks, GPXUtilities.GPXTrackAnalysis analysis) {
        if (analysis != null && !analysis.elevationData.isEmpty()) {
            tracks.forEach(track -> track.points.forEach(point -> {
                if (point.geometry != null) {
                    point.geometry.forEach(p -> p.distance = analysis.elevationData.get(point.geometry.indexOf(p)).distance);
                } else {
                    track.points.forEach(p -> p.distance = analysis.elevationData.get(track.points.indexOf(p)).distance);
                }
            }));
        }
    }
    
    public Map<String, Object> getTrackAnalysis(GPXUtilities.GPXTrackAnalysis analysis, GPXUtilities.GPXTrackAnalysis srtmAnalysis) {
        if (analysis != null) {
            Map<String, Object> res = new HashMap<>();
            res.put("totalDistance", analysis.totalDistance);
            res.put("startTime", analysis.startTime);
            res.put("endTime", analysis.endTime);
            res.put("timeMoving", analysis.timeMoving);
            res.put("hasElevationData", analysis.hasElevationData);
            res.put("diffElevationUp", analysis.diffElevationUp);
            res.put("diffElevationDown", analysis.diffElevationDown);
            res.put("minElevation", analysis.minElevation);
            res.put("avgElevation", analysis.avgElevation);
            res.put("maxElevation", analysis.maxElevation);
            res.put("hasSpeedData", analysis.hasSpeedData);
            res.put("minSpeed", analysis.minSpeed);
            res.put("avgSpeed", analysis.avgSpeed);
            res.put("maxSpeed", analysis.maxSpeed);
            
            if (srtmAnalysis != null) {
                res.put("srtmAnalysis", true);
                res.put("minElevationSrtm", srtmAnalysis.minElevation);
                res.put("avgElevationSrtm", srtmAnalysis.avgElevation);
                res.put("maxElevationSrtm", srtmAnalysis.maxElevation);
            }
            return res;
        }
        return Collections.emptyMap();
    }
    
    public List<Wpt> getWpts(GPXUtilities.GPXFile gpxFile) {
        List<GPXUtilities.WptPt> points = gpxFile.getPoints();
        if (points != null) {
            List<Wpt> res = new ArrayList<>();
            points.forEach(wpt -> res.add(new Wpt(wpt)));
            return res;
        }
        return Collections.emptyList();
    }
    
    public List<Track> getTracks(GPXUtilities.GPXFile gpxFile) {
        if (!gpxFile.tracks.isEmpty()) {
            List<Track> res = new ArrayList<>();
            List<GPXUtilities.Track> tracks = gpxFile.tracks.stream().filter(t -> !t.generalTrack).collect(Collectors.toList());
            if (!gpxFile.routes.isEmpty() && tracks.size() != gpxFile.routes.size()) {
                return Collections.emptyList();
            }
            tracks.forEach(track -> {
                Track t = new Track(track);
                res.add(t);
            });
            return res;
        }
        return Collections.emptyList();
    }
    
    public GPXUtilities.GPXFile createGpxFileFromTrackData(WebGpxParser.TrackData trackData) {
        GPXUtilities.GPXFile gpxFile = new GPXUtilities.GPXFile(OSMAND_ROUTER_V2);
        if (trackData.metaData != null) {
            gpxFile.metadata = trackData.metaData.ext;
            gpxFile.metadata.name = trackData.metaData.name;
            gpxFile.metadata.desc = trackData.metaData.desc;
        }
        if (trackData.wpts != null) {
            for (Wpt wpt : trackData.wpts) {
                gpxFile.addPoint(wpt.ext);
            }
        }
        
        if (trackData.tracks != null) {
            trackData.tracks.forEach(t -> {
                GPXUtilities.Track track = t.ext;
                List<GPXUtilities.TrkSegment> segments = new ArrayList<>();
                if (t.points.get(0).geometry != null) {
                    GPXUtilities.Route route = new GPXUtilities.Route();
                    List<Point> trkPoints = new ArrayList<>();
                    for (int i = 0; i < t.points.size(); i++) {
                        Point point = t.points.get(i);
                        GPXUtilities.WptPt routePoint = point.ext;
                        routePoint.lat = point.lat;
                        routePoint.lon = point.lng;
                        routePoint.extensions.put(PROFILE_TYPE_EXTENSION, String.valueOf(point.profile));
                        int index = point.geometry.isEmpty() ? 0 : point.geometry.size() - 1;
                        routePoint.extensions.put(TRKPT_INDEX_EXTENSION, String.valueOf(index));
                        route.points.add(routePoint);
                        trkPoints.addAll(point.geometry);
                    }
                    gpxFile.routes.add(route);
                    addSegmentsToTrack(trkPoints, segments);
                } else {
                    addSegmentsToTrack(t.points, segments);
                }
                track.segments = segments;
                gpxFile.tracks.add(track);
            });
        }
        
        if (trackData.ext != null) {
            gpxFile.extensions = trackData.ext;
        }
        
        return gpxFile;
    }
    
    private void addSegmentsToTrack(List<Point> points, List<GPXUtilities.TrkSegment> segments) {
        GPXUtilities.TrkSegment segment = new GPXUtilities.TrkSegment();
        boolean isNanEle = isNanEle(points);
        for (Point point : points) {
            GPXUtilities.WptPt filePoint = point.ext;
            if (filePoint.hdop == -1) {
                filePoint.hdop = Double.NaN;
            }
            if (filePoint.heading == 0) {
                filePoint.heading = Float.NaN;
            }
            filePoint.lat = point.lat;
            filePoint.lon = point.lng;
            if (!isNanEle) {
                filePoint.ele = point.ele;
            }
            if (point.profile != null && point.profile.equals(GAP_PROFILE_TYPE)) {
                filePoint.extensions.put(PROFILE_TYPE_EXTENSION, GAP_PROFILE_TYPE);
                segment.points.add(filePoint);
                segments.add(segment);
                segment = new GPXUtilities.TrkSegment();
            } else {
                segment.points.add(filePoint);
            }
            
            if (point.segment != null) {
                segment.routeSegments.add(point.segment.ext);
                if (segment.routeTypes.isEmpty()) {
                    segment.routeTypes.addAll(point.segment.routeTypes);
                }
            }
        }
        segments.add(segment);
    }
    
    private boolean isNanEle(List<Point> points) {
        return points.get(0).ele == 99999;
    }
    
    public TrkSegment generateRouteSegments(List<RouteSegmentResult> route) {
        TrkSegment trkSegment = new TrkSegment();
        List<Entity> es = new ArrayList<>();
        osmAndMapsService.calculateResult(es, route);
        List<Location> locations = new ArrayList<>();
        for (Entity ent : es) {
            if (ent instanceof Way) {
                for (net.osmand.osm.edit.Node node : ((Way) ent).getNodes()) {
                    locations.add(new Location("", node.getLatitude(), node.getLongitude()));
                }
            }
        }
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
        
        List<RouteType> routeTypes = new ArrayList<>();
        for (StringBundle item : typeList) {
            routeTypes.add(RouteType.fromStringBundle(item));
        }
        trkSegment.routeTypes = routeTypes;
        
        return trkSegment;
    }
    
    public List<WebGpxParser.Point> getNewGeometry(WebGpxParser.Point start, WebGpxParser.Point end) throws IOException, InterruptedException {
        
        LatLon startLatLon = new LatLon(start.lat, start.lng);
        LatLon endLatLon = new LatLon(end.lat, end.lng);
        
        Map<String, Object> props = new TreeMap<>();
        List<RouteSegmentResult> routeSegmentResults = osmAndMapsService.routing(start.profile, props, startLatLon,
                endLatLon, Collections.emptyList(), Collections.emptyList());
        
        List<WebGpxParser.Point> pointsRes = new ArrayList<>();
        if (routeSegmentResults != null) {
            for (RouteSegmentResult r : routeSegmentResults) {
                float[] heightArray = r.getObject().calculateHeightArray();
                int stInd = r.getStartPointIndex();
                int endInd = r.getEndPointIndex();
                while (stInd != endInd) {
                    LatLon point = r.getPoint(stInd);
                    WptPt pt = new WptPt();
                    if (heightArray != null && heightArray.length > stInd * 2 + 1) {
                        pt.ele = heightArray[stInd * 2 + 1];
                    }
                    pt.lat = point.getLatitude();
                    pt.lon = point.getLongitude();
                    pointsRes.add(new WebGpxParser.Point(pt));
                    stInd += ((stInd < endInd) ? 1 : -1);
                }
            }
        }
        
        if (!pointsRes.isEmpty() && (start.segment != null || end.segment != null)) {
            addPointRouteSegment(generateRouteSegments(routeSegmentResults), pointsRes);
        }
    
        for (int i = 1; i < pointsRes.size(); i++) {
            Point curr = pointsRes.get(i);
            Point prev = pointsRes.get(i - 1);
            pointsRes.get(i).distance = (float) MapUtils.getDistance(prev.lat, prev.lng, curr.lat, curr.lng);
        }
        
        return pointsRes;
    }
}
