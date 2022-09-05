package net.osmand.server.controllers.pub;

import net.osmand.GPXUtilities;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

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
        GPXUtilities.Track ext;
        
        public Track(GPXUtilities.Track track, GPXUtilities.GPXTrackAnalysis analysis) {
            points = new ArrayList<>();
            if (track.segments.size() > 1) {
                track.segments.forEach(seg -> seg.points.forEach(point -> {
                    int index = seg.points.indexOf(point);
                    Point p = new Point(point, index, analysis);
                    if (index == seg.points.size() - 1) {
                        p.info.put(PROFILE_TYPE_EXTENSION, GAP_PROFILE_TYPE);
                    }
                    points.add(p);
                }));
            } else {
                track.segments.get(0).points.forEach(point -> {
                    int index = track.segments.get(0).points.indexOf(point);
                    Point p = new Point(point, index, analysis);
                    points.add(p);
                });
            }
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
        Map<String, String> info;
        Geometry geometry;
        GPXUtilities.WptPt ext;
        
        public Point(GPXUtilities.WptPt point, int index, GPXUtilities.GPXTrackAnalysis analysis) {
            info = new HashMap<>();
            info.put("lat", String.valueOf(point.lat));
            info.put("lng", String.valueOf(point.lon));
            if (!Float.isNaN((float) point.ele) && Math.abs(((float) point.ele + 99999.0) - point.ele + 99999.0) < 0.00001) {
                info.put("ele", String.valueOf(point.ele));
            }
            if (analysis != null && index != -1) {
                info.put("dist", String.valueOf(analysis.speedData.get(index).distance));
            }
            if (!point.getExtensionsToRead().isEmpty()) {
                point.getExtensionsToRead().forEach((k, v) -> {
                    if (k.equals("trkpt_idx")) {
                        geometry = new Geometry();
                    }
                    info.put(k, v);
                });
            }
            ext = point;
        }
        
    }
    
    static class Geometry {
        List<Point> points;
    }
    
    static void addRoutePoints(GPXUtilities.GPXFile gpxFile, WebGpxData.TrackData gpxData) {
        gpxFile.routes.forEach(route -> {
            List<WebGpxData.Point> routePoints = new ArrayList<>();
            List<WebGpxData.Point> trackPoints = gpxData.tracks.get(gpxFile.routes.indexOf(route)).points;
            AtomicInteger prevInd = new AtomicInteger();
            route.points.forEach(p -> {
                WebGpxData.Point point = new WebGpxData.Point(p, -1, null);
                if (point.geometry != null) {
                    int ind = Integer.parseInt(point.info.get("trkpt_idx"));
                    if (prevInd.get() == 0 && prevInd.get() == ind) {
                        prevInd.set(ind);
                    } else {
                        AtomicReference<Float> dist = new AtomicReference<>((float) 0);
                        trackPoints.forEach(pt -> {
                            int pointInd = trackPoints.indexOf(pt);
                            if (pointInd >= prevInd.get() && pointInd <= ind) {
                                if (point.geometry.points == null) {
                                    point.geometry.points = new ArrayList<>();
                                }
                                point.geometry.points.add(pt);
                                dist.set(Float.parseFloat(pt.info.get("dist")));
                            }
                        });
                        point.info.put("dist", String.valueOf(dist.get()));
                        prevInd.set(ind);
                    }
                }
                routePoints.add(point);
            });
            gpxData.tracks.get(gpxFile.routes.indexOf(route)).points = routePoints;
        });
    }
}
