package net.osmand.swing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;

import net.osmand.ResultMatcher;
import net.osmand.binary.BinaryMapDataObject;
import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.binary.BinaryMapIndexReader.SearchRequest;
import net.osmand.data.Amenity;
import net.osmand.data.LatLon;
import net.osmand.data.QuadRect;
import net.osmand.osm.edit.OsmMapUtils;
import net.osmand.util.MapUtils;

import gnu.trove.iterator.TLongIterator;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TIntObjectHashMap;

public class MapDataSearcher {
    private static final int EARTH_RADIUS_A = 6378137;
    public static int getMapZoomForRadius(double lat, int radius) {
        if (radius <= 0) {
            return 18; // Default zoom for point objects
        }
        double zoom = Math.log(2 * Math.PI * EARTH_RADIUS_A * Math.cos(Math.toRadians(lat)) / (radius * 2)) / Math.log(2);
        int intZoom = (int) Math.round(zoom);
        if (intZoom > 22) { // Max practical zoom
            return 22;
        }
        if (intZoom < 1) {
            return 1;
        }
        return intZoom;
    }
    public static void searchAndPrintObjects(double lat, double lon, int radius, BinaryMapIndexReader[] readers, Log log) throws IOException {
        try {
            final LatLon center = new LatLon(lat, lon);
            final QuadRect bbox = MapUtils.calculateLatLonBbox(lat, lon, radius);

            final int left = MapUtils.get31TileNumberX(bbox.left);
            final int right = MapUtils.get31TileNumberX(bbox.right);
            final int top = MapUtils.get31TileNumberY(bbox.top);
            final int bottom = MapUtils.get31TileNumberY(bbox.bottom);
            int zoom = getMapZoomForRadius(lat, radius);

            final List<Object> objects = new ArrayList<>();
            final List<LatLon> points = new ArrayList<>();
            BinaryMapIndexReader.SearchFilter filter = (types, index) -> {
                return true;
            };

            for (BinaryMapIndexReader reader : readers) {
                SearchRequest<BinaryMapDataObject> req = BinaryMapIndexReader.buildSearchRequest(left, right, top, bottom, zoom, filter, new ResultMatcher<BinaryMapDataObject>() {
                    @Override
                    public boolean publish(BinaryMapDataObject object) {
                        LatLon point = getObjectCenter(object);
                        if (point == null) {
                            return false;
                        }
                        double distance = MapUtils.getDistance(center, point);
                        if (distance <= radius) {
                            objects.add(object);
                            points.add(point);
                        }
                        System.out.printf("Object %s at %s is %d meters.\n", object.getName(), point, (int)distance);
                        return false;
                    }

                    @Override
                    public boolean isCancelled() {
                        return false;
                    }
                });
                reader.searchMapIndex(req);

                SearchRequest<Amenity> reqAmenity = BinaryMapIndexReader.buildSearchPoiRequest(left, right, top, bottom, zoom, null, new ResultMatcher<Amenity>() {
                    @Override
                    public boolean publish(Amenity object) {
                        LatLon point = object.getLocation();
                        double distance = MapUtils.getDistance(center, point);
                        if (distance <= radius) {
                            objects.add(object);
                            points.add(point);
                        }
                        System.out.printf("POI %s at %s is %d meters.\n", object.getName(), point, (int)distance);
                        return false;
                    }

                    @Override
                    public boolean isCancelled() {
                        return false;
                    }
                });
                reader.searchPoi(reqAmenity);
            }

            System.out.printf("%s is box around center (%s) for searching objects within %d meters. Found %d objects.\n", bbox, center, radius, objects.size());
            for (int i = 0; i < objects.size(); i++) {
                System.out.println((i+1) + ". " + objects.get(i) + " at " + points.get(i));
            }
        } catch (IOException e) {
            log.error("Error searching for map objects", e);
        }
    }

    public static LatLon getObjectCenter(BinaryMapDataObject obj) {
        if (obj.isArea()) {
            return getPolygonCenter(obj);
        } else if (obj.getPointsLength() > 0) {
            List<LatLon> points = new ArrayList<>();
            for (int i = 0; i < obj.getPointsLength(); i++) {
                double lat = MapUtils.get31LatitudeY(obj.getPoint31YTile(i));
                double lon = MapUtils.get31LongitudeX(obj.getPoint31XTile(i));
                points.add(new LatLon(lat, lon));
            }
            return OsmMapUtils.getWeightCenter(points);
        }
        return null;
    }

    public static LatLon getPolygonCenter(BinaryMapDataObject obj) {
        if (obj == null || !obj.isArea() || obj.getPointsLength() == 0) {
            return null;
        }

        List<List<LatLon>> rings = new ArrayList<>();

        // Process outer ring
        List<LatLon> outerRing = new ArrayList<>();
        for (int i = 0; i < obj.getPointsLength(); i++) {
            double lat = MapUtils.get31LatitudeY(obj.getPoint31YTile(i));
            double lon = MapUtils.get31LongitudeX(obj.getPoint31XTile(i));
            outerRing.add(new LatLon(lat, lon));
        }
        rings.add(outerRing);

        // Process inner rings
        if (obj.getPolygonInnerCoordinates() != null) {
            for (int[] innerCoords : obj.getPolygonInnerCoordinates()) {
                List<LatLon> innerRing = new ArrayList<>();
                for (int i = 0; i < innerCoords.length; i += 2) {
                    double lon = MapUtils.get31LongitudeX(innerCoords[i]);
                    double lat = MapUtils.get31LatitudeY(innerCoords[i + 1]);
                    innerRing.add(new LatLon(lat, lon));
                }
                if (!innerRing.isEmpty()) {
                    rings.add(innerRing);
                }
            }
        }

        return OsmMapUtils.getPolylabelPoint(rings);
    }
}
