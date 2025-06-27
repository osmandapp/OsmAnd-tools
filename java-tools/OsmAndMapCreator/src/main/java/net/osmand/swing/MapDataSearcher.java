package net.osmand.swing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;

import net.osmand.ResultMatcher;
import net.osmand.binary.BinaryMapDataObject;
import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.binary.BinaryMapIndexReader.SearchRequest;
import net.osmand.data.LatLon;
import net.osmand.data.MapObject;
import net.osmand.data.QuadRect;
import net.osmand.util.MapUtils;

public class MapDataSearcher {

    private static QuadRect calculateBoundingBox(LatLon center, int radiusInMeters) {
        double lat = center.getLatitude();
        double lon = center.getLongitude();
        double latRad = Math.toRadians(lat);

        // Rough conversion from meters to degrees
        double metersPerDegreeLat = 111320.0;
        double metersPerDegreeLon = metersPerDegreeLat * Math.cos(latRad);

        double deltaLat = radiusInMeters / metersPerDegreeLat;
        double deltaLon = radiusInMeters / metersPerDegreeLon;

        double top = lat + deltaLat;
        double bottom = lat - deltaLat;
        double left = lon - deltaLon;
        double right = lon + deltaLon;

        return new QuadRect(left, top, right, bottom);
    }

    public static void searchAndPrintObjects(double lat, double lon, int radius, BinaryMapIndexReader[] readers, Log log) throws IOException {
        try {
            final LatLon center = new LatLon(lat, lon);
            final QuadRect bbox = calculateBoundingBox(center, radius);

            final int left = MapUtils.get31TileNumberX(bbox.left);
            final int right = MapUtils.get31TileNumberX(bbox.right);
            final int top = MapUtils.get31TileNumberY(bbox.top);
            final int bottom = MapUtils.get31TileNumberY(bbox.bottom);

            final List<BinaryMapDataObject> objects = new ArrayList<BinaryMapDataObject>();
            ResultMatcher<BinaryMapDataObject> matcher = new ResultMatcher<BinaryMapDataObject>() {

                @Override
                public boolean publish(BinaryMapDataObject object) {
                    if (MapUtils.getDistance(new LatLon(MapUtils.getLatitudeFromTile(31, object.getLabelY()), MapUtils.getLongitudeFromTile(31, object.getLabelX())), center) <= radius) {
                        objects.add(object);
                    }
                    return false;
                }

                @Override
                public boolean isCancelled() {
                    return false;
                }
            };

            System.out.println(String.format("Searching objects at lat=%f, lon=%f within %d meters", lat, lon, radius));

            for (BinaryMapIndexReader reader : readers) {
                SearchRequest<BinaryMapDataObject> req = BinaryMapIndexReader.buildSearchRequest(left, right, top, bottom, 18, null, matcher);
                reader.searchMapIndex(req);
            }

            System.out.println("Found " + objects.size() + " objects:");
            for (BinaryMapDataObject o : objects) {
                System.out.println("  - " + o + " at " + new LatLon(MapUtils.getLatitudeFromTile(31, o.getLabelY()), MapUtils.getLongitudeFromTile(31, o.getLabelX())));
            }

        } catch (IOException e) {
            log.error("Error searching for map objects", e);
        }
    }
}
