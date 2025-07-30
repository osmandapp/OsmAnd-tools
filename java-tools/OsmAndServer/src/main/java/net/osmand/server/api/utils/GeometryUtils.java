package net.osmand.server.api.utils;

import net.osmand.data.LatLon;
import net.osmand.server.controllers.pub.GeojsonClasses;

public class GeometryUtils {
	private GeometryUtils() {
		// Private constructor to prevent instantiation
	}

	public static LatLon getLatLon(GeojsonClasses.Feature feature) {
		float[] point = "Point".equals(feature.geometry.type) ? (float[])feature.geometry.coordinates : ((float[][])feature.geometry.coordinates)[0];
		return new LatLon(point[1], point[0]);
	}

	public static LatLon parseLatLon(String lat, String lon) {
		if (lat == null || lon == null) {
			return null;
		}
		try {
			return new LatLon(Double.parseDouble(lat), Double.parseDouble(lon));
		} catch (Exception e) {
			return null;
		}
	}
	public static LatLon parsePoint(String wkt) {
		if (wkt == null || !wkt.toUpperCase().startsWith("POINT")) {
			return null;
		}
		try {
			String[] parts = wkt.substring(wkt.indexOf('(') + 1, wkt.indexOf(')')).trim().split("\\s+");
			double lat = Double.parseDouble(parts[0]);
			double lon = Double.parseDouble(parts[1]);
			return new LatLon(lat, lon);
		} catch (Exception e) {
			return null;
		}
	}
	public static GeojsonClasses.Geometry getGeometry(String[] headers) {
		int latIndex = -1;
		int lonIndex = -1;
        for (int i = 0; i < headers.length; i++) {
			if ("lat".equalsIgnoreCase(headers[i])) {
				latIndex = i;
			} else if ("lon".equalsIgnoreCase(headers[i])) {
				lonIndex = i;
			}
		}
		GeojsonClasses.Geometry  geometry = new GeojsonClasses.Geometry("Point");
		geometry.coordinates = new int[] {latIndex, lonIndex};
		return geometry;
	}

	public static String geometryToString(GeojsonClasses.Geometry geometry, String[] values) {
		int[] latLon = (int[]) geometry.coordinates;
		return String.format("POINT(%s %s)", values[latLon[0]], values[latLon[1]]);
	}
}
