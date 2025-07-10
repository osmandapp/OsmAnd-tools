package net.osmand.swing;

import net.osmand.ResultMatcher;
import net.osmand.binary.BinaryMapDataObject;
import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.binary.BinaryMapIndexReader.SearchRequest;
import net.osmand.data.Amenity;
import net.osmand.data.DataTileManager;
import net.osmand.data.LatLon;
import net.osmand.data.QuadRect;
import net.osmand.obf.BinaryInspector;
import net.osmand.osm.edit.Entity;
import net.osmand.osm.edit.Node;
import net.osmand.osm.edit.OSMSettings;
import net.osmand.util.MapUtils;

import org.apache.commons.logging.Log;

import java.awt.Point;
import java.awt.event.MouseEvent;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MapDataPrinter {
    private static int NUM = 1;
    private final MapPanel panel;
    private final Log log;
    private List<Amenity> amenities = new ArrayList<>();
    private int zoom;

    public MapDataPrinter(MapPanel panel, Log log) {
        this.panel = panel;
        this.log = log;
        this.zoom = panel.getZoom();
    }

    private int getRadius(double latitude, int partSize) {
        double tileWidthInMeters = MapUtils.getTileDistanceWidth(latitude, panel.getZoom());
        double metersPerPixel = tileWidthInMeters / panel.getTileSize();
        return (int) ((double) panel.getWidth() / partSize * metersPerPixel);
    }
    public void searchAndPrintObjects(MouseEvent e) {
        double dx = e.getX() - panel.getCenterPointX();
        double dy = e.getY() - panel.getCenterPointY();
        double tileOffsetX = dx / panel.getTileSize();
        double tileOffsetY = dy / panel.getTileSize();
        double longitude = MapUtils.getLongitudeFromTile(panel.getZoom(), panel.getXTile() + tileOffsetX);
        double latitude = MapUtils.getLatitudeFromTile(panel.getZoom(), panel.getYTile() + tileOffsetY);
        final LatLon center = new LatLon(latitude, longitude);

        int radius = getRadius(latitude, 100);
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
                        double distance = MapUtils.getDistance(center, object.getLocation());
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
                reader.searchPoi(reqAmenity);
            }

            System.out.printf("%d. Found %d objects in box %s around center (%s) within %d meters.\n", NUM++, objects.size(), bbox, center, radius);
            for (int i = 0; i < objects.size(); i++) {
                Object o = objects.get(i);
                if (o instanceof BinaryMapDataObject) {
                    print((BinaryMapDataObject) o, distances.get(i));
                } else if (o instanceof Amenity) {
                    print((Amenity) o, distances.get(i));
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

    public void searchPOIs(boolean refresh) {
        if (refresh) {
            Point popupMenuPoint = panel.getPopupMenuPoint();
            double fy = (popupMenuPoint.y - panel.getCenterPointY()) / panel.getTileSize();
            double fx = (popupMenuPoint.x - panel.getCenterPointX()) / panel.getTileSize();
            final double latitude = MapUtils.getLatitudeFromTile(panel.getZoom(), panel.getYTile() + fy);
            final double longitude = MapUtils.getLongitudeFromTile(panel.getZoom(), panel.getXTile() + fx);

            final LatLon center = new LatLon(latitude, longitude);
            int radius = Math.min(getRadius(latitude, 2), 10000);
            final QuadRect bbox = MapUtils.calculateLatLonBbox(latitude, longitude, radius);
            final int left = MapUtils.get31TileNumberX(bbox.left);
            final int right = MapUtils.get31TileNumberX(bbox.right);
            final int top = MapUtils.get31TileNumberY(bbox.top);
            final int bottom = MapUtils.get31TileNumberY(bbox.bottom);

            List<Amenity> objects = new ArrayList<>();
            try {
                BinaryMapIndexReader[] readers = DataExtractionSettings.getSettings().getObfReaders();
                for (BinaryMapIndexReader reader : readers) {
                    SearchRequest<Amenity> reqAmenity = BinaryMapIndexReader.buildSearchPoiRequest(left, right, top, bottom, panel.getZoom(), null, new ResultMatcher<>() {
                        @Override
                        public boolean publish(Amenity object) {
                            double distance = MapUtils.getDistance(center, object.getLocation());
                            if (distance <= radius) {
                                objects.add(object);
                            }
                            return false;
                        }

                        @Override
                        public boolean isCancelled() {
                            return false;
                        }
                    });
                    reader.searchPoi(reqAmenity);
                }
            } catch (IOException ex) {
                log.error("Error searching for POI objects", ex);
            }

            System.out.printf("%d. Found %d POIs in box %s around center (%s) within %d meters.\n", NUM++, objects.size(), bbox, center, radius);
            for (Amenity object : objects) {
                print(object);
            }
            amenities = objects;
        }

        if (refresh || zoom != panel.getZoom()) {
            DataTileManager<Entity> points = new DataTileManager<>(15);
            for (Amenity poi : amenities) {
                LatLon loc = poi.getLocation();
                Node n = new Node(loc.getLatitude(), loc.getLongitude(), poi.getId());
                n.putTag(OSMSettings.OSMTagKey.NAME.getValue(), panel.getZoom() <= 16 ? "" : toString(poi));

                LatLon ll = n.getLatLon();
                points.registerObject(ll.getLatitude(), ll.getLongitude(), n);
            }

            zoom = panel.getZoom();
            panel.setPoints(points);
            panel.repaint();
        }
    }

    public static String toString(Amenity amenity) {
        long id = (amenity.getId());
        if(id > 0) {
            id = id >> 1;
        }
        return amenity.getSubType() + ":" + (amenity.getName() != null && !amenity.getName().trim().isEmpty() ? "\n" + amenity.getName() : "") + "\nosmid=" + id;
    }

    public static void print(Amenity amenity) {
        String s = amenity.printNamesAndAdditional().toString();
        long id = (amenity.getId());
        if(id > 0) {
            id = id >> 1;
        }

        System.out.println(amenity.getType().getKeyName() + ": " + amenity.getSubType() + " " + amenity.getName() +
                " " + amenity.getLocation() + " osmid=" + id + " " + s);
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
