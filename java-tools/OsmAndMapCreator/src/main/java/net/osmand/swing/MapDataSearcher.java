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
                        LatLon point = new LatLon(MapUtils.get31LatitudeY(object.getLabelY()), MapUtils.get31LongitudeX(object.getLabelX()));
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

    public static final int SHIFT_ID = 6;
    private static int OSM_ID = 1;

    private static String printOsmMapDetails(BinaryMapDataObject obj) {
        StringBuilder b = new StringBuilder();
        boolean multipolygon = obj.getPolygonInnerCoordinates() != null && obj.getPolygonInnerCoordinates().length > 0;
        boolean point = obj.getPointsLength() == 1;
        StringBuilder tags = new StringBuilder();
        int[] types = obj.getTypes();
        for (int j = 0; j < types.length; j++) {
            BinaryMapIndexReader.TagValuePair pair = obj.getMapIndex().decodeType(types[j]);
            if (pair == null) {
                throw new NullPointerException("Type " + types[j] + "was not found");
            }
            tags.append("\t<tag k='").append(pair.tag).append("' v='").append(quoteName(pair.value)).append("' />\n");
        }

        if (obj.getAdditionalTypes() != null && obj.getAdditionalTypes().length > 0) {
            for (int j = 0; j < obj.getAdditionalTypes().length; j++) {
                int addtype = obj.getAdditionalTypes()[j];
                BinaryMapIndexReader.TagValuePair pair = obj.getMapIndex().decodeType(addtype);
                if (pair == null) {
                    throw new NullPointerException("Type " + obj.getAdditionalTypes()[j] + "was not found");
                }
                tags.append("\t<tag k='").append(pair.tag).append("' v='").append(quoteName(pair.value)).append("' />\n");
            }
        }
        TIntObjectHashMap<String> names = obj.getObjectNames();
        if (names != null && !names.isEmpty()) {
            int[] keys = names.keys();
            for (int j = 0; j < keys.length; j++) {
                BinaryMapIndexReader.TagValuePair pair = obj.getMapIndex().decodeType(keys[j]);
                if (pair == null) {
                    throw new NullPointerException("Type " + keys[j] + "was not found");
                }
                String name = names.get(keys[j]);
                name = quoteName(name);
                tags.append("\t<tag k='").append(pair.tag).append("' v='").append(name).append("' />\n");
            }
        }

        tags.append("\t<tag k=\'").append("original_id").append("' v='").append(obj.getId() >> (SHIFT_ID + 1)).append("'/>\n");
        tags.append("\t<tag k=\'").append("osmand_id").append("' v='").append(obj.getId()).append("'/>\n");

        if(point) {
            float lon= (float) MapUtils.get31LongitudeX(obj.getPoint31XTile(0));
            float lat = (float) MapUtils.get31LatitudeY(obj.getPoint31YTile(0));
            b.append("<node id = '" + OSM_ID++ + "' version='1' lat='" + lat + "' lon='" + lon + "' >\n");
            b.append(tags);
            b.append("</node>\n");
        } else {
            TLongArrayList innerIds = new TLongArrayList();
            TLongArrayList ids = new TLongArrayList();
            for (int i = 0; i < obj.getPointsLength(); i++) {
                float lon = (float) MapUtils.get31LongitudeX(obj.getPoint31XTile(i));
                float lat = (float) MapUtils.get31LatitudeY(obj.getPoint31YTile(i));
                int id = OSM_ID++;
                b.append("\t<node id = '" + id + "' version='1' lat='" + lat + "' lon='" + lon + "' />\n");
                ids.add(id);
            }

            long outerId = printWay(ids, b, multipolygon ? null : tags);
            if (multipolygon) {
                int[][] polygonInnerCoordinates = obj.getPolygonInnerCoordinates();
                for (int j = 0; j < polygonInnerCoordinates.length; j++) {
                    ids.clear();
                    for (int i = 0; i < polygonInnerCoordinates[j].length; i += 2) {
                        float lon = (float) MapUtils.get31LongitudeX(polygonInnerCoordinates[j][i]);
                        float lat = (float) MapUtils.get31LatitudeY(polygonInnerCoordinates[j][i + 1]);
                        int id = OSM_ID++;
                        b.append("<node id = '" + id + "' version='1' lat='" + lat + "' lon='" + lon + "' />\n");
                        ids.add(id);
                    }
                    innerIds.add(printWay(ids, b, null));
                }
                int id = OSM_ID++;
                b.append("<relation id = '" + id + "' version='1'>\n");
                b.append(tags);
                b.append("\t<member type='way' role='outer' ref= '" + outerId + "'/>\n");
                TLongIterator it = innerIds.iterator();
                while (it.hasNext()) {
                    long ref = it.next();
                    b.append("<member type='way' role='inner' ref= '" + ref + "'/>\n");
                }
                b.append("</relation>\n");
            }
        }

        return b.toString();
    }

    private static String quoteName(String name) {
        if(name == null || name.length() == 0) {
            return "EMPTY";
        }
        name = name.replace("'", "&apos;");
        name = name.replace("<", "&lt;");
        name = name.replace(">", "&gt;");
        name = name.replace("&", "&amp;");
        return name;
    }

    private static long printWay(TLongArrayList ids, StringBuilder b, StringBuilder tags) {
        int id = OSM_ID++;
        b.append("<way id = '" + id + "' version='1'>\n");
        if (tags != null) {
            b.append(tags);
        }
        TLongIterator it = ids.iterator();
        while (it.hasNext()) {
            long ref = it.next();
            b.append("\t<nd ref = '" + ref + "'/>\n");
        }
        b.append("</way>\n");
        return id;
    }
}
