package net.osmand.swing;

import net.osmand.ResultMatcher;
import net.osmand.binary.BinaryMapDataObject;
import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.binary.BinaryMapIndexReader.SearchRequest;
import net.osmand.data.Amenity;
import net.osmand.data.LatLon;
import net.osmand.data.QuadRect;
import net.osmand.obf.BinaryInspector;
import net.osmand.util.MapUtils;

import org.apache.commons.logging.Log;

import java.awt.event.MouseEvent;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MapDataSearcher {
    private static int NUM = 1;

    public static void searchAndPrintObjects(MouseEvent e, MapPanel panel, Log log) {
        double dx = e.getX() - panel.getCenterPointX();
        double dy = e.getY() - panel.getCenterPointY();
        double tileOffsetX = dx / panel.getTileSize();
        double tileOffsetY = dy / panel.getTileSize();
        double longitude = MapUtils.getLongitudeFromTile(panel.getZoom(), panel.getXTile() + tileOffsetX);
        double latitude = MapUtils.getLatitudeFromTile(panel.getZoom(), panel.getYTile() + tileOffsetY);
        final LatLon center = new LatLon(latitude, longitude);

        double tileWidthInMeters = MapUtils.getTileDistanceWidth(latitude, panel.getZoom());
        double metersPerPixel = tileWidthInMeters / panel.getTileSize();
        int radius = (int) ((panel.getWidth() / 100.0) * metersPerPixel);
        final QuadRect bbox = MapUtils.calculateLatLonBbox(latitude, longitude, radius);
        final int left = MapUtils.get31TileNumberX(bbox.left);
        final int right = MapUtils.get31TileNumberX(bbox.right);
        final int top = MapUtils.get31TileNumberY(bbox.top);
        final int bottom = MapUtils.get31TileNumberY(bbox.bottom);
        try {
            BinaryMapIndexReader[] readers = DataExtractionSettings.getSettings().getObfReaders();

            List<Object> objects = new ArrayList<>();
            List<Double> distances = new ArrayList<>();
            for (BinaryMapIndexReader reader : readers) {
                SearchRequest<BinaryMapDataObject> req = BinaryMapIndexReader.buildSearchRequest(left, right, top, bottom, panel.getZoom(), null, new ResultMatcher<>() {
                    @Override
                    public boolean publish(BinaryMapDataObject object) {
                        double distance = getMinDistance(object, center, radius);
                        if (distance <= radius) {
                            objects.add(object);
                            distances.add(distance);
                        }
                        return false;
                    }

                    @Override
                    public boolean isCancelled() {
                        return false;
                    }
                });
                reader.searchMapIndex(req);

                SearchRequest<Amenity> reqAmenity = BinaryMapIndexReader.buildSearchPoiRequest(left, right, top, bottom, panel.getZoom(), null, new ResultMatcher<>() {
                    @Override
                    public boolean publish(Amenity object) {
                        objects.add(object);
                        double distance = MapUtils.getDistance(object.getLocation(), center);
                        distances.add(distance);
                        return false;
                    }

                    @Override
                    public boolean isCancelled() {
                        return false;
                    }
                });
                reader.searchPoi(reqAmenity);
            }

            System.out.printf("%d. Found %d objects in box %s around center (%s) within %d meters.\n", NUM++, objects.size(), bbox, center, radius);
            for (int i = 0; i < objects.size(); i++) {
                Object o = objects.get(i);
                if (o instanceof BinaryMapDataObject) {
                    print((BinaryMapDataObject) o, (Double) distances.get(i));
                } else if (o instanceof Amenity) {
                    print((Amenity) o, (Double) distances.get(i));
                }
            }
        } catch (IOException ex) {
            log.error("Error searching for map objects", ex);
        }
    }

    private static void print(Amenity amenity, Double distance) {
        StringBuilder s = new StringBuilder(String.valueOf(amenity.printNamesAndAdditional()));
        long id = (amenity.getId());
        if (id > 0) {
            id = id >> 1;
        }
        Map<Integer, List<BinaryMapIndexReader.TagValuePair>> tagGroups = amenity.getTagGroups();
        if (tagGroups != null) {
            s.append(" cities:");
            for (Map.Entry<Integer, List<BinaryMapIndexReader.TagValuePair>> entry : tagGroups.entrySet()) {
                s.append("[");
                for (BinaryMapIndexReader.TagValuePair p : entry.getValue()) {
                    s.append(p.tag).append("=").append(p.value).append(" ");
                }
                s.append("]");
            }
        }
        System.out.println(amenity.getType().getKeyName() + ": " + amenity.getSubType() + " " + amenity.getName() +
                " " + amenity.getLocation() + " osmid=" + id + " " + s + " distance=" + String.format("%.2f", distance));
    }

    private static void print(BinaryMapDataObject object, Double distance) {
        StringBuilder s = new StringBuilder();
        BinaryInspector.printMapDetails(object, s, false);
        s.append(" distance=").append(String.format("%.2f", distance));
        System.out.println(s);
    }

    /**
     * Calculate min distance from any point of the given {@link BinaryMapDataObject} lies within the specified radius (in meters)
     * from the provided {@link LatLon} center.
     */
    private static double getMinDistance(BinaryMapDataObject object, LatLon center, int radiusMeters) {
        int pointsCount = object.getPointsLength();
        double minDistance = Double.MAX_VALUE;
        if (pointsCount == 0) {
            return minDistance;
        }

        for (int i = 0; i < pointsCount; i++) {
            double plat = MapUtils.get31LatitudeY(object.getPoint31YTile(i));
            double plon = MapUtils.get31LongitudeX(object.getPoint31XTile(i));
            double distance = MapUtils.getDistance(center.getLatitude(), center.getLongitude(), plat, plon);
            if (distance <= radiusMeters) {
                return distance;
            }
            if (minDistance > distance) {
                minDistance = distance;
            }
        }
        return minDistance;
    }
}
