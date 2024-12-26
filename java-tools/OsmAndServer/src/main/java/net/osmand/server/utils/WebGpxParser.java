package net.osmand.server.utils;

import static net.osmand.gpx.GPXUtilities.parseColor;
import static net.osmand.shared.gpx.GpxUtilities.ADDRESS_EXTENSION;
import static net.osmand.shared.gpx.GpxUtilities.BACKGROUND_TYPE_EXTENSION;
import static net.osmand.shared.gpx.GpxUtilities.GAP_PROFILE_TYPE;
import static net.osmand.shared.gpx.GpxUtilities.ICON_NAME_EXTENSION;
import static net.osmand.shared.gpx.GpxUtilities.PROFILE_TYPE_EXTENSION;
import static net.osmand.shared.gpx.GpxUtilities.TRKPT_INDEX_EXTENSION;
import static net.osmand.shared.gpx.GpxUtilities.HIDDEN_EXTENSION;
import static net.osmand.router.RouteExporter.OSMAND_ROUTER_V2;
import static net.osmand.util.Algorithms.colorToString;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import net.osmand.shared.gpx.GpxFile;
import net.osmand.shared.gpx.GpxTrackAnalysis;
import net.osmand.shared.gpx.GpxUtilities;
import net.osmand.shared.gpx.primitives.*;
import org.springframework.stereotype.Component;

import net.osmand.util.MapUtils;

@Component
public class WebGpxParser {
    
    private static final String COLOR_EXTENSION = "color";
    private static final String DESC_EXTENSION = "desc";
    
    public static final String LINE_PROFILE_TYPE = "line";
    public static final int NAN_MARKER = 99999;
    
    public static class TrackData {
        public WebMetaData metaData;
        public List<Wpt> wpts;
        public List<WebTrack> tracks;
        public Map<String, WebPointsGroup> pointsGroups;
        public Map<String, Object> analysis;
        public Map<String, String> ext;
    }
    
    public static class WebMetaData {
        public String name;
        public String desc;
        public Link link;
        public Metadata ext;
    
        public WebMetaData(Metadata data) {
            if (data != null) {

                if (data.getName() != null) {
                    name = data.getName();
                    data.setName(null);
                }
                if (data.getDescription() != null) {
                    desc = data.getDesc();
                    data.setDesc(null);
                }
                if (data.getLink() != null) {
                    link = data.getLink();
                    data.setLink(null);
                }
            
                Iterator<Map.Entry<String, String>> it = data.getExtensionsToWrite().entrySet().iterator();
            
                while (it.hasNext()) {
                    Map.Entry<String, String> e = it.next();
                    if (e.getKey().equals(DESC_EXTENSION)) {
                        desc = e.getValue();
                        it.remove();
                    }
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
        public String hidden;
        public String icon;
        public double lat;
        public double lon;
        public String category;
        public WptPt ext;
        
        public Wpt(WptPt point) {
            if (point != null) {
                if (point.getName() != null) {
                    name = point.getName();
                    point.setName(null);
                }
                if (point.getDesc() != null) {
                    desc = point.getDesc();
                    point.setDesc(null);
                }
                if (point.getLat() != 0) {
                    lat = point.getLat();
                    point.setLat(0);
                }
                if (point.getLon() != 0) {
                    lon = point.getLon();
                    point.setLon(0);
                }
                if (point.getCategory() != null) {
                    category = point.getCategory();
                    point.setCategory(null);
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
                    if (e.getKey().equals(HIDDEN_EXTENSION)) {
                        hidden = e.getValue();
                        it.remove();
                    }
                }
            }
            ext = point;
        }
    }
    
    public class WebPointsGroup {
        public String color;
        public String name;
        public String iconName;
        public String backgroundType;
        public final List<Wpt> points = new ArrayList<>();
        public GpxUtilities.PointsGroup ext;
        public WebPointsGroup(GpxUtilities.PointsGroup group) {
            if (group != null) {
                if (group.getColor() != 0) {
                    color = colorToString(group.getColor());
                    group.setColor(0);
                }
                if (group.getName() != null) {
                    name = group.getName();
                }
                if (group.getIconName() != null) {
                    iconName = group.getIconName();
                    group.setIconName(null);
                }
                if (group.getBackgroundType() != null) {
                    backgroundType = group.getBackgroundType();
                    group.setBackgroundType(null);
                }
                if (!group.getPoints().isEmpty()) {
                    List<Wpt> res = new ArrayList<>();
                    group.getPoints().forEach(wpt -> res.add(new Wpt(wpt)));
                    points.addAll(res);
                    group.setPoints(Collections.emptyList());
                }
            }
            ext = group;
        }
    }
    
    public class WebTrack {
        public List<Point> points;
    
        public List<List<Point>> segments = new ArrayList<>();
        public Track ext;
        
        public WebTrack(Track track) {
            points = new ArrayList<>();
            track.getSegments().forEach(seg -> {
                List<Point> pointsSeg = new ArrayList<>();
                seg.getPoints().forEach(point -> {
                    int index = seg.getPoints().indexOf(point);
                    Point p = new Point(point);
                    if (track.getSegments().size() > 1 && index == seg.getPoints().size() - 1 && track.getSegments().indexOf(seg) != track.getSegments().size() - 1) {
                        p.profile = GAP_PROFILE_TYPE;
                    }
                    pointsSeg.add(p);
                });
                addRouteSegmentsToPoints(seg, pointsSeg);
                segments.add(pointsSeg);
                points.addAll(pointsSeg);
            });
            
            if (!points.isEmpty()) {
                track.setSegments(Collections.emptyList());
            }
            ext = track;
        }
    }
    
    public static class Point {
        
        public double lat;
        public double lng;
        public double ele = Double.NaN;
        public double srtmEle = Double.NaN;
        public double speed;
        public double distance;
        public String profile;
        public List<Point> geometry;
        public transient int geometrySize;
        public RouteSegment segment; // on each turn point
        public WptPt ext;
    
        public Point(){}
        
        public Point(WptPt point) {
            if (!Double.isNaN(point.getLat())) {
                lat = point.getLat();
                point.setLat(Double.NaN);
            }
            
            if (!Double.isNaN(point.getLon())) {
                lng = point.getLon();
                point.setLon(Double.NaN);
            }
            
            if (!Double.isNaN(point.getEle()) || point.getEle() != NAN_MARKER) {
                ele = point.getEle();
                point.setEle(Double.NaN);
            }
    
            if (!Double.isNaN(point.getSpeed())) {
                speed = point.getSpeed();
                point.setSpeed(Double.NaN);
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
            if (geometry != null && profile == null) {
                profile = LINE_PROFILE_TYPE;
            }
            ext = point;
        }
        
    }
    
    public static class RouteSegment {
        public GpxUtilities.RouteSegment ext;
        public List<GpxUtilities.RouteType> routeTypes;
    }
    
    public void addRoutePoints(GpxFile gpxFile, TrackData gpxData) {
        Map<Integer, List<Point>> trackPointsMap = new HashMap<>();
        AtomicBoolean skip = new AtomicBoolean(false);
        gpxFile.getRoutes().forEach(route -> {
            List<Point> routePoints = new ArrayList<>();
            int index = gpxFile.getRoutes().indexOf(route);
            List<Point> trackPoints = getPointsFromSegmentIndex(gpxData, index);
            if (trackPoints.isEmpty()) {
                //case with only one route points
                if (route.getPoints().size() == 1) {
                    routePoints.add(new Point(route.getPoints().get(0)));
                    trackPointsMap.put(0, routePoints);
                }
            } else {
                int prevTrkPointInd = -1;
                for (WptPt p : route.getPoints()) {
                    boolean isLastPoint = route.getPoints().indexOf(p) == route.getPoints().size() - 1;
                    Point routePoint = new Point(p);
                    int currTrkPointInd;
                    if (routePoint.geometry == null && routePoint.geometrySize == 0) {
                        skip.set(true);
                        return;
                    }
                    if (routePoint.geometrySize == -1) {
                        currTrkPointInd = findNearestPoint(trackPoints, routePoint);
                    } else {
                        currTrkPointInd = routePoint.geometrySize - 1;
                    }
                    //add last trkpt to last rtept geo
                    if (isLastPoint) {
                        currTrkPointInd += 1;
                    }
                    prevTrkPointInd = addTrkptToRoutePoint(currTrkPointInd, prevTrkPointInd, routePoint, trackPoints, routePoints);
                }
                if (skip.get()) {
                    return;
                }
                int indTrack = getTrackBySegmentIndex(gpxData, index);
                List<Point> routeP = trackPointsMap.get(indTrack);
                if (routeP != null) {
                    routeP.addAll(routePoints);
                } else {
                    trackPointsMap.put(indTrack, routePoints);
                }
            }
        });
        if (skip.get()) {
            return;
        }
        gpxData.tracks.forEach(track -> track.points = trackPointsMap.get(gpxData.tracks.indexOf(track)));
    }
    
    public List<Point> getPointsFromSegmentIndex(TrackData gpxData, int index) {
        List<List<Point>> segments = new ArrayList<>();
        gpxData.tracks.forEach(track -> segments.addAll(track.segments));
        
        return segments.isEmpty() ? Collections.emptyList() : segments.get(index);
    }
    
    public int getTrackBySegmentIndex(TrackData gpxData, int index) {
        int size = 0;
        for(WebTrack track : gpxData.tracks) {
            size += track.segments.size() - 1;
            if (size >= index) {
                return gpxData.tracks.indexOf(track);
            } else {
                size += index;
            }
        }
        return -1;
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
                if (pointInd > prevTrkPointInd && pointInd <= currTrkPointInd) {
                    if (routePoint.geometry == null) {
                        routePoint.geometry = new ArrayList<>();
                    }
                    if (Objects.equals(pt.profile, GAP_PROFILE_TYPE)) {
                        routePoint.profile = GAP_PROFILE_TYPE;
                    }
                    routePoint.geometry.add(pt);
                }
            }
        }
        routePoints.add(routePoint);
        return currTrkPointInd;
    }
    
    public void addSrtmEle(List<WebTrack> tracks, GpxTrackAnalysis srtmAnalysis) {
        if (srtmAnalysis != null && tracks != null) {
            for (WebTrack track : tracks) {
                int pointsSize = 0;
                for (Point point : track.points) {
                    if (point.geometry != null) {
                        for (Point p : point.geometry) {
                            p.srtmEle = srtmAnalysis.getPointAttributes().get(point.geometry.indexOf(p) + pointsSize).getElevation();
                        }
                        pointsSize += point.geometry.size();
                    } else {
                        track.points.forEach(p -> p.srtmEle = srtmAnalysis.getPointAttributes().get(track.points.indexOf(p)).getElevation());
                    }
                }
            }
        }
    }
    
    public void addAdditionalInfo(List<WebTrack> tracks, GpxTrackAnalysis analysis, boolean addSpeed) {
        tracks.forEach(track -> {
            int pointsSize = 0;
            for (Point point : track.points) {
                if (point.geometry != null) {
                    for (Point p : point.geometry) {
                        int ind = point.geometry.indexOf(p);
                        if (ind + pointsSize < analysis.getPointAttributes().size()) {
                            p.distance = analysis.getPointAttributes().get(ind + pointsSize).getDistance();
                            if (addSpeed) {
                                p.speed = analysis.getPointAttributes().get(ind + pointsSize).getSpeed();
                            }
                        }
                    }
                } else {
                    int ind = track.points.indexOf(point);
                    if (ind < analysis.getPointAttributes().size()) {
                        point.distance = analysis.getPointAttributes().get(ind).getDistance();
                        if (addSpeed) {
                            point.speed = analysis.getPointAttributes().get(ind).getSpeed();
                        }
                    }
                }
                if (point.geometry != null) {
                    pointsSize += point.geometry.size();
                }
            }
        });
    }
    
    public Map<String, Object> getTrackAnalysis(GpxTrackAnalysis analysis, GpxTrackAnalysis srtmAnalysis) {
        if (analysis != null) {
            Map<String, Object> res = new HashMap<>();
            res.put("totalDistance", analysis.getTotalDistance());
            res.put("startTime", analysis.getStartTime());
            res.put("endTime", analysis.getEndTime());
            res.put("timeMoving", analysis.getTimeMoving());
            res.put("hasElevationData", analysis.hasElevationData());
            res.put("diffElevationUp", analysis.getDiffElevationUp());
            res.put("diffElevationDown", analysis.getDiffElevationDown());
            res.put("minElevation", analysis.getMinElevation());
            res.put("avgElevation", analysis.getAvgElevation());
            res.put("maxElevation", analysis.getMaxElevation());
            res.put("hasSpeedData", analysis.hasSpeedData());
            res.put("minSpeed", analysis.getMinSpeed());
            res.put("avgSpeed", analysis.getAvgSpeed());
            res.put("maxSpeed", analysis.getMaxSpeed());
            if (srtmAnalysis != null) {
                res.put("srtmAnalysis", true);
                res.put("minElevationSrtm", srtmAnalysis.getMinElevation());
                res.put("avgElevationSrtm", srtmAnalysis.getAvgElevation());
                res.put("maxElevationSrtm", srtmAnalysis.getMaxElevation());
            }
            return res;
        }
        return Collections.emptyMap();
    }
    
    public List<Wpt> getWpts(GpxFile gpxFile) {
        List<WptPt> points = gpxFile.getPointsList();
        if (!points.isEmpty()) {
            List<Wpt> res = new ArrayList<>();
            points.forEach(wpt -> res.add(new Wpt(wpt)));
            return res;
        }
        return Collections.emptyList();
    }
    
    public Map<String, WebPointsGroup> getPointsGroups(GpxFile gpxFile) {
        Map<String, GpxUtilities.PointsGroup> pointsGroups = gpxFile.getPointsGroups();
        Map<String, WebPointsGroup> res = new LinkedHashMap<>();
        if (!pointsGroups.isEmpty()) {
            for (String key : pointsGroups.keySet()) {
                res.put(key, new WebPointsGroup(pointsGroups.get(key)));
            }
            return res;
        }
        
        return Collections.emptyMap();
    }
    
    public List<WebTrack> getTracks(GpxFile gpxFile) {
        if (!gpxFile.getTracks().isEmpty()) {
            List<WebTrack> res = new ArrayList<>();
            List<Track> tracks = gpxFile.getTracks().stream().filter(t -> !t.getGeneralTrack()).toList();
            tracks.forEach(track -> {
                WebTrack t = new WebTrack(track);
                res.add(t);
            });
            return res;
        }
        return Collections.emptyList();
    }
    
    public GpxFile createGpxFileFromTrackData(WebGpxParser.TrackData trackData) {
        GpxFile gpxFile = new GpxFile(OSMAND_ROUTER_V2);
        if (trackData.metaData != null) {
            if (trackData.metaData.ext != null) {
                gpxFile.setMetadata(trackData.metaData.ext);
            }
            Metadata metadata = gpxFile.getMetadata();
            metadata.setName(trackData.metaData.name);
            metadata.setDesc(trackData.metaData.desc);
            metadata.setLink(trackData.metaData.link);
        }
        if (trackData.wpts != null) {
            for (Wpt wpt : trackData.wpts) {
                gpxFile.addPoint(convertToWptPt(wpt));
            }
        }
    
        if (trackData.pointsGroups != null) {
            Map<String, GpxUtilities.PointsGroup> res = new LinkedHashMap<>();
            for (String key : trackData.pointsGroups.keySet()) {
                WebPointsGroup dataGroup = trackData.pointsGroups.get(key);
                GpxUtilities.PointsGroup group;
                if (dataGroup.ext != null) {
                    group = dataGroup.ext;
                    group.setColor(parseColor(dataGroup.color, 0));
                    List<Wpt> wptsData = dataGroup.points;
                    for (Wpt wpt : wptsData) {
                        group.getPoints().add(convertToWptPt(wpt));
                    }
                } else {
                    group = new GpxUtilities.PointsGroup(dataGroup.name, dataGroup.iconName, dataGroup.backgroundType, parseColor(dataGroup.color, 0));
                }
                res.put(key, group);
            }
            gpxFile.setPointsGroups(res);
        }
        
        if (trackData.tracks != null) {
            trackData.tracks.forEach(t -> {
                Track track = t.ext;
                if (track == null) {
                    track = new Track();
                }
                if (!t.points.isEmpty()) {
                    List<TrkSegment> segments = new ArrayList<>();
                    if (t.points.get(0).geometry != null) {
                        Route route = new Route();
                        List<Point> trkPoints = new ArrayList<>();
                        int allPoints = 0;
                        for (int i = 0; i < t.points.size(); i++) {
                            Point point = t.points.get(i);
                            List<WebGpxParser.Point> geo = point.geometry;
                            if (geo.isEmpty()) {
                                if (!route.getPoints().isEmpty()) {
                                    gpxFile.getRoutes().add(route);
                                }
                                route = new Route();
                                allPoints = 0;
                            }
                            WptPt routePoint = point.ext;
                            if (routePoint == null) {
                                routePoint = new WptPt();
                            }
                            routePoint.setLat(point.lat);
                            routePoint.setLon(point.lng);
                            if (point.ele == NAN_MARKER) {
                                routePoint.setEle(Double.NaN);
                            }
                            if (!point.profile.equals(LINE_PROFILE_TYPE)) {
                                routePoint.getExtensionsToWrite().put(PROFILE_TYPE_EXTENSION, String.valueOf(point.profile));
                            }
                            allPoints += geo.isEmpty() ? 0 : geo.size();
                            boolean isLast = i == t.points.size() - 1;
                            //for last rtept trkpt_idx = last index of trkpt points
                            int ind = isLast ? allPoints - 1 : allPoints;
                            routePoint.getExtensionsToWrite().put(TRKPT_INDEX_EXTENSION, String.valueOf(allPoints == 0 ? 0 : ind));
                            route.getPoints().add(routePoint);
                            trkPoints.addAll(geo);
                        }
                        gpxFile.getRoutes().add(route);
                        if (!trkPoints.isEmpty()) {
                            addSegmentsToTrack(trkPoints, segments);
                        }
                    } else {
                        addSegmentsToTrack(t.points, segments);
                    }
                    track.setSegments(segments);
                    gpxFile.getTracks().add(track);
                }
            });
        }
        
        if (trackData.ext != null) {
            gpxFile.setExtensions(trackData.ext);
        }
        
        return gpxFile;
    }
    
    public WptPt convertToWptPt(Wpt wpt) {
        WptPt point = wpt.ext != null ? wpt.ext : new WptPt();
        point.setName(wpt.name);
        if (wpt.desc != null) {
            point.setDesc(wpt.desc);
        }
        point.setLat(wpt.lat);
        point.setLon(wpt.lon);
        point.setCategory(wpt.category);
        point.getExtensionsToWrite();
        if (wpt.color != null) {
            point.getExtensionsToWrite().put(COLOR_EXTENSION, wpt.color);
        }
        if (wpt.address != null) {
            point.getExtensionsToWrite().put(ADDRESS_EXTENSION, wpt.address);
        }
        if (wpt.background != null) {
            point.getExtensionsToWrite().put(BACKGROUND_TYPE_EXTENSION, wpt.background);
        }
        if (wpt.icon != null) {
            point.getExtensionsToWrite().put(ICON_NAME_EXTENSION, wpt.icon);
        }
        if (wpt.hidden != null) {
            point.getExtensionsToWrite().put(HIDDEN_EXTENSION, wpt.hidden);
        }
        return point;
    }
    
    private void addSegmentsToTrack(List<Point> points, List<TrkSegment> segments) {
        TrkSegment segment = new TrkSegment();
        boolean isNanEle = isNanEle(points);
        int lastStartTrkptIdx = 0;
        int prevTypesSize = 0;
        Point lastPointWithSeg = null;
        for (Point point : points) {
            WptPt filePoint = point.ext;
            if (filePoint == null) {
                filePoint = new WptPt();
            }
            if (filePoint.getHdop() == -1) {
                filePoint.setHdop(Double.NaN);
            }
            if (filePoint.getHeading() == 0) {
                filePoint.setHeading(Float.NaN);
            }
            filePoint.setLat(point.lat);
            filePoint.setLon(point.lng);
            filePoint.setSpeed(point.speed);
            filePoint.setEle((!isNanEle && point.ele != NAN_MARKER) ? point.ele : Double.NaN);

            if (point.profile != null && point.profile.equals(GAP_PROFILE_TYPE)) {
                filePoint.getExtensionsToWrite().put(PROFILE_TYPE_EXTENSION, GAP_PROFILE_TYPE);
                segment.getPoints().add(filePoint);
                segments.add(segment);
                segment = new TrkSegment();
            } else {
                segment.getPoints().add(filePoint);
            }
            
            if (point.segment != null) {
                GpxUtilities.RouteSegment seg = point.segment.ext;
                int segStartInd = seg.getStartTrackPointIndex() != null ? Integer.parseInt(seg.getStartTrackPointIndex()) : points.indexOf(point);
                if (segStartInd == 0 && points.indexOf(point) > 0 && lastPointWithSeg != null) {
                    int currentLength = Integer.parseInt(lastPointWithSeg.segment.ext.getLength());
                    //fix approximate results
                    int segmentLength = currentLength == 1 ? 1 : currentLength - 1;
                    int lastPointWithSegStartInd = lastPointWithSeg.segment.ext.getStartTrackPointIndex() != null ? Integer.parseInt(lastPointWithSeg.segment.ext.getStartTrackPointIndex()) : points.indexOf(lastPointWithSeg);
                    lastStartTrkptIdx = segmentLength + lastPointWithSegStartInd;
                    prevTypesSize += lastPointWithSeg.segment.routeTypes.size();
                    segment.getRouteTypes().addAll(point.segment.routeTypes);
                }
                seg.setStartTrackPointIndex(Integer.toString(segStartInd + lastStartTrkptIdx));
                
                seg.setTypes(prepareTypes(seg.getTypes(), prevTypesSize));
                seg.setPointTypes(prepareTypes(seg.getPointTypes(), prevTypesSize));
                seg.setNames(prepareTypes(seg.getNames(), prevTypesSize));
                
                segment.getRouteSegments().add(seg);
                
                if (segment.getRouteTypes().isEmpty()) {
                    segment.getRouteTypes().addAll(point.segment.routeTypes);
                }
                lastPointWithSeg = point;
            }
        }
        segments.add(segment);
    }
    
    private String prepareTypes(String stringTypes, int prevTypesSize) {
        if (stringTypes != null) {
            List<Character> characterList = stringTypes.chars()
                    .mapToObj(e->((char)e)).
                    collect(Collectors.toList());
            StringBuilder sbNumber = null;
            StringBuilder sb = new StringBuilder();
            for (char current : characterList) {
                if (Character.isDigit(current)) {
                    if (sbNumber == null) {
                        sbNumber = new StringBuilder();
                    }
                    sbNumber.append(current);
                } else {
                    if (sbNumber != null) {
                        sb.append(Integer.parseInt(String.valueOf(sbNumber)) + prevTypesSize);
                        sbNumber = null;
                    }
                    sb.append(current);
                }
            }
            if (sbNumber != null) {
                sb.append(Integer.parseInt(String.valueOf(sbNumber)) + prevTypesSize);
            }
            
            return sb.toString();
        } else {
            return null;
        }
    }
    
    private boolean isNanEle(List<Point> points) {
        return points.get(0).ele == NAN_MARKER;
    }
    
    public void addRouteSegmentsToPoints(TrkSegment seg, List<WebGpxParser.Point> points) {
        int startInd = 0;
        if (!seg.getRouteSegments().isEmpty()) {
            for (GpxUtilities.RouteSegment rs : seg.getRouteSegments()) {
                WebGpxParser.RouteSegment segment = new WebGpxParser.RouteSegment();
                segment.ext = rs;
                segment.routeTypes = seg.getRouteTypes();
                int length = Integer.parseInt(rs.getLength());
                points.get(startInd).segment = segment;
                startInd = startInd + (length - 1);
            }
        }
    }
}
