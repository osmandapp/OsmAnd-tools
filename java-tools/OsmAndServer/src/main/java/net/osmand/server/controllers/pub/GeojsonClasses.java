package net.osmand.server.controllers.pub;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import net.osmand.data.LatLon;
import net.osmand.data.LatLonEle;

public class GeojsonClasses {

	

	public static class FeatureCollection {
		public String type = "FeatureCollection";
		public List<Feature> features = new ArrayList<>();

		public FeatureCollection(Feature... features) {
			this.features.addAll(Arrays.asList(features));
		}
	}

	public static class Feature {
		public Map<String, Object> properties = new LinkedHashMap<>();
		public String type = "Feature";
		public final Geometry geometry;

		public Feature(Geometry geometry) {
			this.geometry = geometry;
		}

		public Feature prop(String key, Object vl) {
			properties.put(key, vl);
			return this;
		}
	}

	public static class Geometry {
		public final String type;
		public Object coordinates;

		public Geometry(String type) {
			this.type = type;
		}

		public static Geometry lineString(List<LatLon> lst) {
			Geometry gm = new Geometry("LineString");
			float[][] coordinates = new float[lst.size()][];
			for (int i = 0; i < lst.size(); i++) {
				coordinates[i] = new float[]{(float) lst.get(i).getLongitude(), (float) lst.get(i).getLatitude()};
			}
			gm.coordinates = coordinates;
			return gm;
		}

		public static Geometry lineStringElevation(List<LatLonEle> lst) {
			Geometry gm = new Geometry("LineString");
			float[][] coordinates = new float[lst.size()][];
			for (int i = 0; i < lst.size(); i++) {
				float lat = (float) lst.get(i).getLatitude();
				float lon = (float) lst.get(i).getLongitude();
				float ele = (float) lst.get(i).getElevation();
				if (Float.isNaN(ele)) {
					coordinates[i] = new float[]{lon, lat}; // GeoJSON [] longitude first, then latitude
				} else {
					coordinates[i] = new float[]{lon, lat, ele}; // https://www.rfc-editor.org/rfc/rfc7946 3.1.1
				}
			}
			gm.coordinates = coordinates;
			return gm;
		}

		public static Geometry point(LatLon pnt) {
			Geometry gm = new Geometry("Point");
			gm.coordinates = new float[]{(float) pnt.getLongitude(), (float) pnt.getLatitude()};
			return gm;
		}

		public static Geometry pointElevation(LatLonEle pnt) {
			Geometry gm = new Geometry("Point");
			float lat = (float) pnt.getLatitude();
			float lon = (float) pnt.getLongitude();
			float ele = (float) pnt.getElevation();
			if (Double.isNaN(ele)) {
				gm.coordinates = new float[]{lon, lat};
			} else {
				gm.coordinates = new float[]{lon, lat, ele};
			}
			return gm;
		}
	}
}
