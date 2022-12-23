package net.osmand.server.utils;

import net.osmand.GPXUtilities;
import net.osmand.util.MapUtils;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;

import static net.osmand.GPXUtilities.*;
import static net.osmand.router.RouteExporter.OSMAND_ROUTER_V2;
import static net.osmand.util.Algorithms.colorToString;

@Component
public class WebGpxParser {
    
    private static final String COLOR_EXTENSION = "color";
    
    public static class TrackData {
        public MetaData metaData;
        public List<Wpt> wpts;
        public List<Track> tracks;
        public Map<String, PointsGroup> pointsGroups;
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
        
        public String name;
        public String desc;
        public String address;
        public String color;
        public String background;
        public String icon;
        public double lat;
        public double lon;
        public String category;
        public GPXUtilities.WptPt ext;
        
        public Wpt(GPXUtilities.WptPt point) {
            if (point != null) {
                if (point.name != null) {
                    name = point.name;
                    point.name = null;
                }
                if (point.desc != null) {
                    desc = point.desc;
                    point.desc = null;
                }
                if (point.lat != 0) {
                    lat = point.lat;
                    point.lat = 0;
                }
                if (point.lon != 0) {
                    lon = point.lon;
                    point.lon = 0;
                }
                if (point.category != null) {
                    category = point.category;
                    point.category = null;
                }
                
                Iterator<Map.Entry<String, String>> it = point.getExtensionsToWrite().entrySet().iterator();
                
                while (it.hasNext()) {
                    Map.Entry<String, String> e = it.next();
                    if (e.getKey().equals(ADDRESS_EXTENSION)) {
                        address = e.getValue();
                        it.remove();
                    }
                    if (e.getKey().equals(COLOR_EXTENSION)) {
                        color = e.getValue();
                        it.remove();
                    }
                    if (e.getKey().equals(BACKGROUND_TYPE_EXTENSION)) {
                        background = e.getValue();
                        it.remove();
                    }
                    if (e.getKey().equals(ICON_NAME_EXTENSION)) {
                        icon = e.getValue();
                        it.remove();
                    }
                }
            }
            ext = point;
        }
    }
    
    public class PointsGroup {
        public String color;
        public final List<Wpt> points = new ArrayList<>();
        public GPXUtilities.PointsGroup ext;
        public PointsGroup(GPXUtilities.PointsGroup group) {
            if (group != null) {
                if (group.color != 0) {
                    color = colorToString(group.color);
                    group.color = 0;
                }
                if (!group.points.isEmpty()) {
                    List<Wpt> res = new ArrayList<>();
                    group.points.forEach(wpt -> res.add(new Wpt(wpt)));
                    points.addAll(res);
                    group.points = Collections.emptyList();
                }
            }
            ext = group;
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
                addRouteSegmentsToPoints(seg, pointsSeg);
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
    
    public void addRoutePoints(GPXUtilities.GPXFile gpxFile, TrackData gpxData) {
        gpxFile.routes.forEach(route -> {
            List<Point> routePoints = new ArrayList<>();
            List<Point> trackPoints = gpxData.tracks.get(gpxFile.routes.indexOf(route)).points;
            int prevTrkPointInd = 0;
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
    
    public Map<String, PointsGroup> getPointsGroups(GPXUtilities.GPXFile gpxFile) {
        Map<String, GPXUtilities.PointsGroup> pointsGroups = gpxFile.getPointsGroups();
        Map<String, PointsGroup> res = new LinkedHashMap<>();
        if (!pointsGroups.isEmpty()) {
            for (String key : pointsGroups.keySet()) {
                res.put(key, new PointsGroup(pointsGroups.get(key)));
            }
            return res;
        }
        
        return Collections.emptyMap();
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
                gpxFile.addPoint(updateWpt(wpt));
            }
        }
    
        if (trackData.pointsGroups != null) {
            Map<String, GPXUtilities.PointsGroup> res = new LinkedHashMap<>();
            for (String key : trackData.pointsGroups.keySet()) {
                PointsGroup dataGroup = trackData.pointsGroups.get(key);
                GPXUtilities.PointsGroup group = dataGroup.ext;
                group.color = parseColor(dataGroup.color, 0);
                List<Wpt> wptsData = dataGroup.points;
                for (Wpt wpt : wptsData) {
                    group.points.add(updateWpt(wpt));
                }
                res.put(key, group);
            }
            gpxFile.setPointsGroups(res);
        }
        
        if (trackData.tracks != null) {
            trackData.tracks.forEach(t -> {
                GPXUtilities.Track track = t.ext;
                List<GPXUtilities.TrkSegment> segments = new ArrayList<>();
                if (t.points.get(0).geometry != null) {
                    GPXUtilities.Route route = new GPXUtilities.Route();
                    List<Point> trkPoints = new ArrayList<>();
                    int allPoints = 0;
                    for (int i = 0; i < t.points.size(); i++) {
                        Point point = t.points.get(i);
                        GPXUtilities.WptPt routePoint = point.ext;
                        routePoint.lat = point.lat;
                        routePoint.lon = point.lng;
                        if (point.ele == 99999) {
                            routePoint.ele = Double.NaN;
                        }
                        routePoint.extensions.put(PROFILE_TYPE_EXTENSION, String.valueOf(point.profile));
                        allPoints += point.geometry.isEmpty() ? 0 : point.geometry.size() - 1;
                        routePoint.extensions.put(TRKPT_INDEX_EXTENSION, String.valueOf(allPoints));
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
    
    private WptPt updateWpt(Wpt wpt) {
        WptPt point = wpt.ext != null ? wpt.ext : new WptPt();
        point.name = wpt.name;
        point.desc = wpt.desc;
        point.lat = wpt.lat;
        point.lon = wpt.lon;
        point.category = wpt.category;
        if (point.extensions == null) {
            point.extensions = new LinkedHashMap<>();
        }
        point.extensions.put(COLOR_EXTENSION, String.valueOf(wpt.color));
        point.extensions.put(ADDRESS_EXTENSION, String.valueOf(wpt.address));
        point.extensions.put(BACKGROUND_TYPE_EXTENSION, String.valueOf(wpt.background));
        point.extensions.put(ICON_NAME_EXTENSION, String.valueOf(wpt.icon));
        return point;
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
            filePoint.ele = !isNanEle ? point.ele : Double.NaN;

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
}
