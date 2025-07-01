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

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class MapDataSearcher {
    private static int NUM = 1;

    public static void searchAndPrintObjects(double lat, double lon, MapPanel panel) throws IOException {
        double tileWidthInMeters = MapUtils.getTileDistanceWidth(lat, panel.getZoom());
        double metersPerPixel = tileWidthInMeters / panel.getTileSize();
        int radius = (int) ((panel.getWidth() / 100.0) * metersPerPixel);

        BinaryMapIndexReader[] readers = DataExtractionSettings.getSettings().getObfReaders();
        final QuadRect bbox = MapUtils.calculateLatLonBbox(lat, lon, radius);

        final int left = MapUtils.get31TileNumberX(bbox.left);
        final int right = MapUtils.get31TileNumberX(bbox.right);
        final int top = MapUtils.get31TileNumberY(bbox.top);
        final int bottom = MapUtils.get31TileNumberY(bbox.bottom);

        System.out.printf("%d. Searching for objects in box %s around center (%s) within %d meters...\n", NUM++, bbox, new LatLon(lat, lon), radius);

        for (BinaryMapIndexReader reader : readers) {
            SearchRequest<BinaryMapDataObject> req = BinaryMapIndexReader.buildSearchRequest(left, right, top, bottom, panel.getZoom(), null, new ResultMatcher<>() {
                @Override
                public boolean publish(BinaryMapDataObject object) {
                    print(object);
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
                    print(object);
                    return false;
                }

                @Override
                public boolean isCancelled() {
                    return false;
                }
            });
            reader.searchPoi(reqAmenity);
        }
    }

    private static void print(Amenity amenity) {
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
                " " + amenity.getLocation() + " osmid=" + id + " " + s);
    }

    private static void print(BinaryMapDataObject object) {
        StringBuilder s = new StringBuilder();
        BinaryInspector.printMapDetails(object, s, false);
        System.out.println(s);
    }
}
