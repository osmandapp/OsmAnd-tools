package net.osmand.server.utils;

import static net.osmand.router.RouteExporter.OSMAND_ROUTER_V2;
import static net.osmand.shared.gpx.GpxUtilities.*;
import static net.osmand.util.Algorithms.colorToString;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import kotlin.Pair;
import net.osmand.shared.gpx.GpxFile;
import net.osmand.shared.gpx.GpxTrackAnalysis;
import net.osmand.shared.gpx.GpxUtilities;
import net.osmand.shared.gpx.primitives.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import net.osmand.util.MapUtils;

@Component
public class WebGpxParser {
    
    private static final String COLOR_EXTENSION = "color";
    private static final String DESC_EXTENSION = "desc";
    
    public static final String LINE_PROFILE_TYPE = "line";
    public static final int NAN_MARKER = 99999;

    // Track Appearance
    public static final String GPX_EXT_SHOW_ARROWS = "show_arrows";
    public static final String GPX_EXT_SHOW_START_FINISH = "show_start_finish";
    public static final String GPX_EXT_COLOR = "color";
    public static final String GPX_EXT_WIDTH = "width";
    private static final Log log = LogFactory.getLog((WebGpxParser.class));

    public static class TrackData {
    	public WebMetaData metaData;
    	public List<Wpt> wpts;
    	public List<WebTrack> tracks;
    	public Map<String, WebPointsGroup> pointsGroups;
    	public Map<String, Object> analysis;
    	public WebTrackAppearance trackAppearance;
    	public Map<String, String> ext;
    	public List<GpxUtilities.RouteType> routeTypes;
        
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

    public static class WebPointsGroup {
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
        private List<Point> points;

        private List<List<Point>> segments = new ArrayList<>();
        private Track ext;
        
        public WebTrack(Track track, List<GpxUtilities.RouteType> routeTypes) {
            points = new ArrayList<>();
            Map<Integer, Integer> updatedSegmentRouteTypes = new HashMap<>();
	        for (TrkSegment seg : track.getSegments()) {
		        List<Point> pointsSeg = new ArrayList<>();
                // update route types indexes
		        List<GpxUtilities.RouteType> types = seg.getRouteTypes();
                for (int oldIndex = 0; oldIndex < types.size(); oldIndex++) {
                    GpxUtilities.RouteType type = types.get(oldIndex);
                    int newIndex = getOrCreateRouteTypeIndex(type, routeTypes);
                    updatedSegmentRouteTypes.put(oldIndex, newIndex);
                }

		        seg.getPoints().forEach(point -> {
			        int index = seg.getPoints().indexOf(point);
			        Point p = new Point(point);
			        if (track.getSegments().size() > 1 && index == seg.getPoints().size() - 1 && track.getSegments().indexOf(seg) != track.getSegments().size() - 1) {
				        p.profile = GAP_PROFILE_TYPE;
			        }
			        pointsSeg.add(p);
		        });
		        addRouteSegmentsToPoints(seg, pointsSeg, false, updatedSegmentRouteTypes);
		        segments.add(pointsSeg);
		        points.addAll(pointsSeg);
	        }

	        if (!points.isEmpty()) {
                track.setSegments(Collections.emptyList());
            }
            ext = track;
        }

        private int getOrCreateRouteTypeIndex(GpxUtilities.RouteType type, List<GpxUtilities.RouteType> routeTypes) {
            for (int i = 0; i < routeTypes.size(); i++) {
                if (Objects.equals(routeTypes.get(i).getTag(), type.getTag()) && Objects.equals(routeTypes.get(i).getValue(), type.getValue())) {
                    return i;
                }
            }
            routeTypes.add(type);
            return routeTypes.size() - 1;
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

    public static class WebTrackAppearance {
    	public Boolean showArrows;
        public Boolean showStartFinish;
        public String color;
        public String width;

        public WebTrackAppearance(Map<String, String> gpxExtensions) {
            if (gpxExtensions == null || gpxExtensions.isEmpty()) {
                return;
            }
            for (Map.Entry<String, String> entry : gpxExtensions.entrySet()) {
                if (entry.getKey().equals(GPX_EXT_SHOW_ARROWS)) {
                    showArrows = Boolean.parseBoolean(entry.getValue());
                }
                if (entry.getKey().equals(GPX_EXT_SHOW_START_FINISH)) {
                    showStartFinish = Boolean.parseBoolean(entry.getValue());
                }
                if (entry.getKey().equals(GPX_EXT_COLOR)) {
                    color = entry.getValue();
                }
                if (entry.getKey().equals(GPX_EXT_WIDTH)) {
                    width = entry.getValue();
                }
            }
        }

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

        return (segments.isEmpty() || index > segments.size() - 1) ? Collections.emptyList() : segments.get(index);
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
            int globalIndex = 0;
            for (WebTrack track : tracks) {
                for (Point point : track.points) {
                    if (point.geometry != null) {
                        for (Point p : point.geometry) {
                            if (globalIndex < srtmAnalysis.getPointAttributes().size()) {
                                p.srtmEle = srtmAnalysis.getPointAttributes().get(globalIndex).getElevation();
                            }
                            globalIndex++;
                        }
                    } else {
                        if (globalIndex < srtmAnalysis.getPointAttributes().size()) {
                            point.srtmEle = srtmAnalysis.getPointAttributes().get(globalIndex).getElevation();
                        }
                        globalIndex++;
                    }
                }
            }
        }
    }
    
    public void addAdditionalInfo(List<WebTrack> tracks, GpxTrackAnalysis analysis, boolean addSpeed) {
        tracks.forEach(track -> {
            int pointsSize = 0;
            if (track.points == null || track.points.isEmpty() || analysis == null || analysis.getPointAttributes().isEmpty()) {
                return;
            }
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
    
    public Pair<List<WebTrack>, List<GpxUtilities.RouteType>> getTracks(GpxFile gpxFile) {
        if (!gpxFile.getTracks().isEmpty()) {
            List<WebTrack> res = new ArrayList<>();
            List<GpxUtilities.RouteType> routeTypes = new ArrayList<>();
            List<Track> tracks = gpxFile.getTracks().stream().filter(t -> !t.getGeneralTrack()).toList();
            tracks.forEach(track -> {
                WebTrack t = new WebTrack(track, routeTypes);
                res.add(t);
            });
            return new Pair<>(res, routeTypes);
        }
        return new Pair<>(Collections.emptyList(), Collections.emptyList());
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
                    List<Wpt> wptsData = dataGroup.points;
                    for (Wpt wpt : wptsData) {
                        group.getPoints().add(convertToWptPt(wpt));
                    }
                } else {
                    group = new GpxUtilities.PointsGroup(dataGroup.name, dataGroup.iconName, dataGroup.backgroundType, 0);
                }
                Integer parsedColor = GpxUtilities.INSTANCE.parseColor(dataGroup.color, 0);
                if (parsedColor != null) {
                    group.setColor(parsedColor);
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
                            addSegmentsToTrack(trkPoints, segments, trackData.routeTypes);
                        }
                    } else {
                        addSegmentsToTrack(t.points, segments, trackData.routeTypes);
                    }
                    track.setSegments(segments);
                    gpxFile.getTracks().add(track);
                }
            });
        }

        Map<String, String> trackExt = parseTrackExt(trackData);
        if (trackExt != null) {
            gpxFile.setExtensions(trackExt);
        }
        
        return gpxFile;
    }

    private Map<String, String> parseTrackExt(WebGpxParser.TrackData trackData) {
        Map<String, String> trackExt = trackData.ext;
        WebTrackAppearance trackAppearance = trackData.trackAppearance;
        if (trackAppearance != null) {
            if (trackExt == null) {
                trackExt = new LinkedHashMap<>();
            }
            trackExt.put(GPX_EXT_SHOW_ARROWS, String.valueOf(trackAppearance.showArrows));
            trackExt.put(GPX_EXT_SHOW_START_FINISH, String.valueOf(trackAppearance.showStartFinish));
            trackExt.put(GPX_EXT_COLOR, trackAppearance.color);
            trackExt.put(GPX_EXT_WIDTH, trackAppearance.width);
        }
        return trackExt;
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
            boolean isHidden = Boolean.parseBoolean(wpt.hidden);
            if (isHidden) {
                point.getExtensionsToWrite().put(HIDDEN_EXTENSION, wpt.hidden);
            } else {
                point.getExtensionsToWrite().remove(HIDDEN_EXTENSION);
            }
        }
        return point;
    }
    
    private void addSegmentsToTrack(List<Point> points, List<TrkSegment> segments, List<GpxUtilities.RouteType> routeTypes) {
        TrkSegment segment = new TrkSegment();
        boolean isNanEle = isNanEle(points);
        int lastStartTrkptIdx = 0;
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
                segment.getRouteTypes().addAll(routeTypes);
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
                }
                seg.setStartTrackPointIndex(Integer.toString(segStartInd + lastStartTrkptIdx));
                
                segment.getRouteSegments().add(seg);

                lastPointWithSeg = point;
            }
        }
        if (routeTypes != null) {
            segment.getRouteTypes().addAll(routeTypes);
        }
        segments.add(segment);
    }
    
    private boolean isNanEle(List<Point> points) {
        return points.get(0).ele == NAN_MARKER;
    }

    public void addRouteSegmentsToPoints(TrkSegment seg, List<Point> points, boolean addRouteTypes, Map<Integer, Integer> updatedSegmentRouteTypes) {
        int startInd = 0;
        if (!seg.getRouteSegments().isEmpty()) {
            for (GpxUtilities.RouteSegment rs : seg.getRouteSegments()) {
                // update route types indexes
                if (updatedSegmentRouteTypes != null) {
                    if (rs.getTypes() != null) {
                        rs.setTypes(updateIndexes(rs.getTypes(), updatedSegmentRouteTypes));
                    }
                    if (rs.getPointTypes() != null) {
                        rs.setPointTypes(updateIndexes(rs.getPointTypes(), updatedSegmentRouteTypes));
                    }
                    if (rs.getNames() != null) {
                        rs.setNames(updateIndexes(rs.getNames(), updatedSegmentRouteTypes));
                    }
                }

                RouteSegment segment = new RouteSegment();
                segment.ext = rs;
                if (addRouteTypes) {
                    segment.routeTypes = seg.getRouteTypes();
                }
                int length = Integer.parseInt(rs.getLength());
                if (startInd < points.size()) {
                    points.get(startInd).segment = segment;
                } else {
                    log.warn(String.format("Route segment index out of bounds: startInd=%d, length=%d, points.size()=%d, segmentCount=%d",
                            startInd, length, points.size(), seg.getRouteSegments().size()));
                    break;
                }
                startInd += length - 1;
            }
        }
    }


    public String updateIndexes(String value, Map<Integer, Integer> updatedSegmentRouteTypes) {
        Pattern pattern = Pattern.compile("\\d+"); // Match all numbers in the string
        Matcher matcher = pattern.matcher(value);
        StringBuilder result = new StringBuilder();

        while (matcher.find()) {
            int oldIndex = Integer.parseInt(matcher.group()); // Extract the number
            int newIndex = updatedSegmentRouteTypes.getOrDefault(oldIndex, oldIndex); // Replace if found in the map
            matcher.appendReplacement(result, String.valueOf(newIndex)); // Append the new index to the result
        }
        matcher.appendTail(result); // Append the remaining part of the string

        return result.toString();
    }
}
