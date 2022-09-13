package net.osmand.server.webgpx;

import net.osmand.GPXUtilities;
import net.osmand.util.MapUtils;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;

import static net.osmand.GPXUtilities.*;
import static net.osmand.router.RouteExporter.OSMAND_ROUTER_V2;

@Component
public class WebGpxParser {
    public void addRoutePoints(GPXUtilities.GPXFile gpxFile, WebGpxData.TrackData gpxData) {
        gpxFile.routes.forEach(route -> {
            List<WebGpxData.Point> routePoints = new ArrayList<>();
            List<WebGpxData.Point> trackPoints = gpxData.tracks.get(gpxFile.routes.indexOf(route)).points;
            int prevTrkPointInd = -1;
            for (GPXUtilities.WptPt p : route.points) {
                WebGpxData.Point routePoint = new WebGpxData.Point(p);
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
    
    public int findNearestPoint(List<WebGpxData.Point> trackPoints, WebGpxData.Point routePoint) {
        double minDist = -1;
        int res = -1;
        for (WebGpxData.Point tp : trackPoints) {
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
    
    public int addTrkptToRoutePoint(int currTrkPointInd, int prevTrkPointInd, WebGpxData.Point routePoint, List<WebGpxData.Point> trackPoints, List<WebGpxData.Point> routePoints) {
        if (currTrkPointInd != 0) {
            for (WebGpxData.Point pt : trackPoints) {
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
    
    public void addSrtmEle(List<WebGpxData.Track> tracks, GPXUtilities.GPXTrackAnalysis srtmAnalysis) {
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
    
    public void addDistance(List<WebGpxData.Track> tracks, GPXUtilities.GPXTrackAnalysis analysis) {
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
            res.put("minSpeed ", analysis.minSpeed);
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
    
    public List<WebGpxData.Wpt> getWpts(GPXUtilities.GPXFile gpxFile) {
        List<GPXUtilities.WptPt> points = gpxFile.getPoints();
        if (points != null) {
            List<WebGpxData.Wpt> res = new ArrayList<>();
            points.forEach(wpt -> res.add(new WebGpxData.Wpt(wpt)));
            return res;
        }
        return Collections.emptyList();
    }
    
    public List<WebGpxData.Track> getTracks(GPXUtilities.GPXFile gpxFile) {
        if (!gpxFile.tracks.isEmpty()) {
            List<WebGpxData.Track> res = new ArrayList<>();
            List<GPXUtilities.Track> tracks = gpxFile.tracks.stream().filter(t -> !t.generalTrack).collect(Collectors.toList());
            if (!gpxFile.routes.isEmpty() && tracks.size() != gpxFile.routes.size()) {
                return Collections.emptyList();
            }
            tracks.forEach(track -> {
                WebGpxData.Track t = new WebGpxData.Track(track);
                res.add(t);
            });
            return res;
        }
        return Collections.emptyList();
    }
    
    public GPXUtilities.GPXFile createGpxFileFromTrackData(WebGpxData.TrackData trackData) {
        GPXUtilities.GPXFile gpxFile = new GPXUtilities.GPXFile(OSMAND_ROUTER_V2);
        if (trackData.metaData != null) {
            gpxFile.metadata = trackData.metaData.ext;
            gpxFile.metadata.name = trackData.metaData.name;
            gpxFile.metadata.desc = trackData.metaData.desc;
        }
        if (trackData.wpts != null) {
            for (WebGpxData.Wpt wpt : trackData.wpts) {
                gpxFile.addPoint(wpt.ext);
            }
        }
        
        if (trackData.tracks != null) {
            trackData.tracks.forEach(t -> {
                GPXUtilities.Track track = t.ext;
                List<GPXUtilities.TrkSegment> segments = new ArrayList<>();
                if (t.points.get(0).geometry != null) {
                    GPXUtilities.Route route = new GPXUtilities.Route();
                    List<WebGpxData.Point> trkPoints = new ArrayList<>();
                    for (int i = 0; i < t.points.size(); i++) {
                        WebGpxData.Point point = t.points.get(i);
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
    
    private void addSegmentsToTrack(List<WebGpxData.Point> points, List<GPXUtilities.TrkSegment> segments) {
        GPXUtilities.TrkSegment segment = new GPXUtilities.TrkSegment();
        boolean isNanEle = isNanEle(points);
        for (WebGpxData.Point point : points) {
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
    
    private boolean isNanEle(List<WebGpxData.Point> points) {
        return points.get(0).ele == 99999;
    }
}
