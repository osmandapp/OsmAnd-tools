package net.osmand.server.webgpx;

import net.osmand.GPXUtilities;

import java.util.*;

import static net.osmand.GPXUtilities.*;

public class WebGpxData {
    
    WebGpxData() {
    }
    
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
    
    public static class Track {
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
                int startInd = 0;
                if (!seg.routeSegments.isEmpty()) {
                    for (GPXUtilities.RouteSegment rs : seg.routeSegments) {
                        RouteSegment segment = new RouteSegment();
                        segment.ext = rs;
                        segment.routeTypes = seg.routeTypes;
                        int length = Integer.parseInt(rs.length);
                        pointsSeg.get(startInd).segment = segment;
                        startInd = startInd + (length - 1);
                    }
                }
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
}
