package net.osmand.server.controllers.pub;

import net.osmand.GPXUtilities;
import net.osmand.util.MapUtils;

import java.util.*;

import static net.osmand.GPXUtilities.GAP_PROFILE_TYPE;
import static net.osmand.GPXUtilities.PROFILE_TYPE_EXTENSION;

public class WebGpxData {
    
    WebGpxData() {
    }
    
    static class TrackData {
        List<Track> tracks;
        List<Wpt> wpts;
        MetaData metaData;
        
        GPXUtilities.GPXTrackAnalysis analysis;
        GPXUtilities.GPXTrackAnalysis srtmAnalysis;
    }
    
    static class Track {
        List<Point> points;
        List<Segment> segments;
        GPXUtilities.Track ext;
        
        public Track(GPXUtilities.Track track) {
            points = new ArrayList<>();
            
            track.segments.forEach(seg -> {
                if (!seg.routeSegments.isEmpty()) {
                    segments = new ArrayList<>();
                    int startInd = 0;
                    int endInd = 0;
                    for (GPXUtilities.RouteSegment rs : seg.routeSegments) {
                        Segment segment = new Segment();
                        if (segment.info == null) {
                            segment.info = new HashMap<>();
                        }
                        if (segment.points == null) {
                            segment.points = new ArrayList<>();
                        }
                        segment.info.put("id", rs.id);
                        int length = Integer.parseInt(rs.length);
                        endInd = endInd + (length - 1);
                        for (int i = startInd; i <= endInd; i++) {
                            segment.points.add(new Point(seg.points.get(i)));
                        }
                        segments.add(segment);
                        startInd = startInd + (length - 1);
                    }
                } else {
                    seg.points.forEach(point -> {
                        int index = seg.points.indexOf(point);
                        Point p = new Point(point);
                        if (track.segments.size() > 1 && index == seg.points.size() - 1 && track.segments.indexOf(seg) != track.segments.size() - 1) {
                            p.info.put(PROFILE_TYPE_EXTENSION, GAP_PROFILE_TYPE);
                        }
                        points.add(p);
                    });
                }
            });
            
            ext = track;
        }
    }
    
    static class Wpt {
        Map<String, String> info;
        GPXUtilities.WptPt ext;
        
        public Wpt(GPXUtilities.WptPt point) {
            if (point.lat != 0 && point.lon != 0) {
                info = new HashMap<>();
                info.put("lat", String.valueOf(point.lat));
                info.put("lng", String.valueOf(point.lon));
                
                if (point.name != null) {
                    info.put("name", point.name);
                }
                if (point.comment != null) {
                    info.put("cmt", point.comment);
                }
                if (point.category != null) {
                    info.put("type", point.category);
                }
                if (!point.getExtensionsToRead().isEmpty()) {
                    info.putAll(point.getExtensionsToRead());
                }
                ext = point;
            }
        }
    }
    
    static class MetaData {
        public String desc;
        public GPXUtilities.Metadata ext;
        
        public MetaData(GPXUtilities.Metadata data) {
            if (data != null) {
                if (data.desc != null) {
                    desc = data.desc;
                }
                ext = data;
            }
        }
    }
    
    static class Point {
        int id;
        Map<String, String> info;
        Geometry geometry;
        GPXUtilities.WptPt ext;
        
        public Point(GPXUtilities.WptPt point) {
            id = Integer.parseInt(createID());
            info = new HashMap<>();
            info.put("lat", String.valueOf(point.lat));
            info.put("lng", String.valueOf(point.lon));
            if (!Float.isNaN((float) point.ele) && Math.abs(((float) point.ele + 99999.0) - point.ele + 99999.0) < 0.00001) {
                info.put("ele", String.valueOf(point.ele));
            }
            if (!point.getExtensionsToRead().isEmpty()) {
                point.getExtensionsToRead().forEach((k, v) -> {
                    if (k.equals("trkpt_idx")) {
                        geometry = new Geometry();
                    }
                    if (k.equals("profile")) {
                        info.put(k, v);
                    }
                });
            }
            ext = point;
        }
        
    }
    
    private static long idCounter = 0;
    
    public static synchronized String createID()
    {
        return String.valueOf(idCounter++);
    }
    
    static class Geometry {
        List<Point> points;
        List<Segment> segments;
    }
    
    static class Segment {
        Map<String, String> info;
        List<Point> points;
    }
    
    static void addRoutePoints(GPXUtilities.GPXFile gpxFile, WebGpxData.TrackData gpxData) {
        gpxFile.routes.forEach(route -> {
            List<WebGpxData.Point> routePoints = new ArrayList<>();
            List<WebGpxData.Point> trackPoints = new ArrayList<>();
            List<WebGpxData.Segment> trackSegments = new ArrayList<>();
            if (gpxData.tracks.get(gpxFile.routes.indexOf(route)).segments != null) {
                trackSegments = gpxData.tracks.get(gpxFile.routes.indexOf(route)).segments;
            } else {
                trackPoints = gpxData.tracks.get(gpxFile.routes.indexOf(route)).points;
            }
            int prevTrkPointInd = -1;
            for (GPXUtilities.WptPt p : route.points) {
                WebGpxData.Point routePoint = new WebGpxData.Point(p);
                int currTrkPointInd;
                if (routePoint.geometry != null) {
                    currTrkPointInd = Integer.parseInt(routePoint.ext.getExtensionsToRead().get("trkpt_idx"));
                } else {
                    routePoint.geometry = new Geometry();
                    currTrkPointInd = findNearestPoint(trackPoints, routePoint);
                }
                prevTrkPointInd = addTrkptToRoutePoint(currTrkPointInd, prevTrkPointInd, routePoint, trackPoints, routePoints, trackSegments);
            }
            gpxData.tracks.get(gpxFile.routes.indexOf(route)).points = routePoints;
            gpxData.tracks.get(gpxFile.routes.indexOf(route)).segments = null;
        });
    }
    
    static int findNearestPoint(List<WebGpxData.Point> trackPoints, WebGpxData.Point routePoint) {
        double minDist = -1;
        int res = -1;
        
        for (Point tp : trackPoints) {
            double currentDist = MapUtils.getDistance(Double.parseDouble(routePoint.info.get("lat")), Double.parseDouble(routePoint.info.get("lng")),
                    Double.parseDouble(tp.info.get("lat")), Double.parseDouble(tp.info.get("lng")));
            if (minDist == -1) {
                minDist = currentDist;
            } else if (currentDist < minDist) {
                minDist = currentDist;
                res = trackPoints.indexOf(tp);
            }
        }
        return res;
    }
    
    static int addTrkptToRoutePoint(int currTrkPointInd, int prevTrkPointInd, WebGpxData.Point routePoint, List<WebGpxData.Point> trackPoints, List<WebGpxData.Point> routePoints, List<WebGpxData.Segment> trackSegments) {
        if (currTrkPointInd != 0) {
            if (!trackSegments.isEmpty()) {
                List<Segment> segs = new ArrayList<>();
                int pointsLength = 0;
                for (WebGpxData.Segment s : trackSegments) {
                    if (pointsLength < currTrkPointInd - 1) {
                        pointsLength += s.points.size() - 1;
                        segs.add(s);
                    } else {
                        routePoint.geometry.segments = segs;
                        routePoints.add(routePoint);
                        break;
                    }
                }
            } else {
                for (WebGpxData.Point pt : trackPoints) {
                    int pointInd = trackPoints.indexOf(pt);
                    if (pointInd >= prevTrkPointInd && pointInd <= currTrkPointInd) {
                        if (routePoint.geometry.points == null) {
                            routePoint.geometry.points = new ArrayList<>();
                        }
                        routePoint.geometry.points.add(pt);
                    }
                }
                prevTrkPointInd = currTrkPointInd;
                routePoints.add(routePoint);
            }
        } else {
            routePoints.add(routePoint);
        }
        return prevTrkPointInd;
    }
}
