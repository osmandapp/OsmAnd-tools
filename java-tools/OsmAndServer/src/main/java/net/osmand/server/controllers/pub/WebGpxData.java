package net.osmand.server.controllers.pub;

import net.osmand.GPXUtilities;

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
                            int indexSeg = track.segments.indexOf(seg);
                            segment.points.add(new Point(seg.points.get(i), i, indexSeg));
                        }
                        segments.add(segment);
                        startInd = startInd + (length - 1);
                    }
                } else {
                    seg.points.forEach(point -> {
                        int index = seg.points.indexOf(point);
                        Point p = new Point(point, index, track.segments.indexOf(seg));
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
        
        public Point(GPXUtilities.WptPt point, int index, int indexSeg) {
            id = index * (indexSeg + 1);
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
            int prevInd = -1;
            for (GPXUtilities.WptPt p : route.points) {
                WebGpxData.Point point = new WebGpxData.Point(p, route.points.indexOf(p), gpxFile.routes.indexOf(route));
                if (point.geometry != null) {
                    int ind = Integer.parseInt(point.ext.getExtensionsToRead().get("trkpt_idx"));
                    if (ind != 0) {
                        if (!trackSegments.isEmpty()) {
                            List<Segment> segs = new ArrayList<>();
                            int pointsLength = 0;
                            for (WebGpxData.Segment s : trackSegments) {
                                if (pointsLength < ind - 1) {
                                    pointsLength += s.points.size() - 1;
                                    segs.add(s);
                                } else {
                                    point.geometry.segments = segs;
                                    routePoints.add(point);
                                    break;
                                }
                            }
                        } else {
                            for (WebGpxData.Point pt : trackPoints) {
                                int pointInd = trackPoints.indexOf(pt);
                                if (pointInd >= prevInd && pointInd <= ind) {
                                    if (point.geometry.points == null) {
                                        point.geometry.points = new ArrayList<>();
                                    }
                                    point.geometry.points.add(pt);
                                }
                            }
                            prevInd = ind;
                            routePoints.add(point);
                        }
                    } else {
                        routePoints.add(point);
                    }
                }
            }
            gpxData.tracks.get(gpxFile.routes.indexOf(route)).points = routePoints;
            gpxData.tracks.get(gpxFile.routes.indexOf(route)).segments = null;
        });
    }
}
