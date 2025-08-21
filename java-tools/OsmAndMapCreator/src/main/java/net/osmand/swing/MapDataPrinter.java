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
	private static final int SHIFT_CONSTANT = 1;
	private int numSearchAttempts = 1;
	private final MapPanel panel;
	private final Log log;
	private List<Node> nodes = new ArrayList<>();
	private int zoom;

	public MapDataPrinter(MapPanel panel, Log log) {
		this.panel = panel;
		this.log = log;
		this.zoom = panel.getZoom();
	}

	public void searchAndPrintObjects(MouseEvent e) {
		if (panel.getZoom() < 14)
			return;

		double dx = e.getX() - panel.getCenterPointX();
		double dy = e.getY() - panel.getCenterPointY();
		double tileOffsetX = dx / panel.getTileSize();
		double tileOffsetY = dy / panel.getTileSize();
		double longitude = MapUtils.getLongitudeFromTile(panel.getZoom(), panel.getXTile() + tileOffsetX);
		double latitude = MapUtils.getLatitudeFromTile(panel.getZoom(), panel.getYTile() + tileOffsetY);
		LatLon center = new LatLon(latitude, longitude);

		int radius = getRadius(latitude, 100);
		QuadRect bbox = MapUtils.calculateLatLonBbox(latitude, longitude, radius);
		int left = MapUtils.get31TileNumberX(bbox.left);
		int right = MapUtils.get31TileNumberX(bbox.right);
		int top = MapUtils.get31TileNumberY(bbox.top);
		int bottom = MapUtils.get31TileNumberY(bbox.bottom);

		List<Object> objects = new ArrayList<>();
		List<Double> distances = new ArrayList<>();
		ResultMatcher<BinaryMapDataObject> rmForObject = new ResultMatcher<>() {
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
		};
		ResultMatcher<Amenity> rmForAmenity = new ResultMatcher<>() {
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
		};
		try {
			BinaryMapIndexReader[] readers = DataExtractionSettings.getSettings().getObfReaders();
			for (BinaryMapIndexReader reader : readers) {
				SearchRequest<BinaryMapDataObject> req = BinaryMapIndexReader.buildSearchRequest(left, right, top,
						bottom, panel.getZoom(), null, rmForObject);
				reader.searchMapIndex(req);

				SearchRequest<Amenity> reqAmenity = BinaryMapIndexReader.buildSearchPoiRequest(left, right, top,
						bottom, panel.getZoom(), null, rmForAmenity);
				reader.searchPoi(reqAmenity);
			}

			System.out.printf("%d. Found %d objects in %s around center (%s) within %d meters.\n", numSearchAttempts++
					, objects.size(), bbox, center, radius);
			for (int i = 0; i < objects.size(); i++) {
				Object o = objects.get(i);
				if (o instanceof BinaryMapDataObject) {
					printDataObject((BinaryMapDataObject) o, distances.get(i));
				} else if (o instanceof Amenity) {
					printAmenity((Amenity) o, distances.get(i));
				}
			}
		} catch (IOException ex) {
			log.error("Error searching for map objects", ex);
		}
	}

	public void searchPOIs(boolean refresh) {
		DataTileManager<Entity> points = panel.getPoints();
		if (points == null)
			points = new DataTileManager<>(15);

		if (refresh) {
			Point popupMenuPoint = panel.getPopupMenuPoint();
			double fy = (popupMenuPoint.y - panel.getCenterPointY()) / panel.getTileSize();
			double fx = (popupMenuPoint.x - panel.getCenterPointX()) / panel.getTileSize();
			double latitude = MapUtils.getLatitudeFromTile(panel.getZoom(), panel.getYTile() + fy);
			double longitude = MapUtils.getLongitudeFromTile(panel.getZoom(), panel.getXTile() + fx);

			LatLon center = new LatLon(latitude, longitude);
			int radius = Math.min(getRadius(latitude, 2), 10000);
			QuadRect bbox = MapUtils.calculateLatLonBbox(latitude, longitude, radius);
			int left = MapUtils.get31TileNumberX(bbox.left);
			int right = MapUtils.get31TileNumberX(bbox.right);
			int top = MapUtils.get31TileNumberY(bbox.top);
			int bottom = MapUtils.get31TileNumberY(bbox.bottom);

			List<Amenity> objects = new ArrayList<>();
			ResultMatcher<Amenity> rm = new ResultMatcher<>() {
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
			};
			try {
				BinaryMapIndexReader[] readers = DataExtractionSettings.getSettings().getObfReaders();
				for (BinaryMapIndexReader reader : readers) {
					SearchRequest<Amenity> reqAmenity = BinaryMapIndexReader.buildSearchPoiRequest(left, right, top,
							bottom, panel.getZoom(), null, rm);
					reader.searchPoi(reqAmenity);
				}
			} catch (IOException ex) {
				log.error("Error searching for POI objects", ex);
			}

			System.out.printf("%d. Found %d POIs in %s around center (%s) within %d meters.\n", numSearchAttempts++,
					objects.size(), bbox, center, radius);
			for (Amenity object : objects) {
				printAmenity(object);
			}

			List<Node> poiNodes = new ArrayList<>();
			for (Amenity poi : objects) {
				LatLon loc = poi.getLocation();
				Node n = new Node(loc.getLatitude(), loc.getLongitude(), poi.getId());
				n.putTag(OSMSettings.OSMTagKey.NAME.getValue(), panel.getZoom() <= 16 ? "" : displayString(poi));
				poiNodes.add(n);
			}

			for (Node n : nodes) {
				LatLon ll = n.getLatLon();
				points.unregisterObject(ll.getLatitude(), ll.getLongitude(), n);
			}
			for (Node n : poiNodes) {
				LatLon ll = n.getLatLon();
				points.registerObject(ll.getLatitude(), ll.getLongitude(), n);
			}
			nodes = poiNodes;
		}

		if (refresh || zoom != panel.getZoom()) {
			zoom = panel.getZoom();
			panel.setPoints(points);
			panel.repaint();
		}
	}

	public void clearPOIs() {
		DataTileManager<Entity> points = panel.getPoints();
		if (points != null) {
			for (Node n : nodes) {
				LatLon ll = n.getLatLon();
				points.unregisterObject(ll.getLatitude(), ll.getLongitude(), n);
			}
		}
		nodes.clear();

		zoom = panel.getZoom();
		if (points != null) {
			panel.setPoints(points);
			panel.repaint();
		}
	}

	private static String displayString(Amenity object) {
		long id = (object.getId());
		if (id > 0) {
			id = id >> 1;
		}
		return object.getSubType() + ":" + (object.getName() != null && !object.getName().trim().isEmpty() ?
				"\n" + object.getName() : "") + "\nosmid=" + id;
	}

	private static void printAmenity(Amenity object) {
		String s = object.printNamesAndAdditional().toString();
		long id = (object.getId());
		if (id > 0) {
			id = id >> SHIFT_CONSTANT;
		}

		System.out.println(object.getType().getKeyName() + ": " + object.getSubType() + " " + object.getName() + " " + object.getLocation() + " osmid=" + id + " " + s);
	}

	private int getRadius(double latitude, int partSize) {
		double tileWidthInMeters = MapUtils.getTileDistanceWidth(latitude, panel.getZoom());
		double metersPerPixel = tileWidthInMeters / panel.getTileSize();
		return (int) ((double) panel.getWidth() / partSize * metersPerPixel);
	}

	private static void printAmenity(Amenity amenity, Double distance) {
		StringBuilder s = new StringBuilder(String.valueOf(amenity.printNamesAndAdditional()));
		long id = amenity.getId();
		if (id > 0) {
			id = id >> SHIFT_CONSTANT;
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
		System.out.println(amenity.getType().getKeyName() + ": " + amenity.getSubType() + " " + amenity.getName() + " "
				+ amenity.getLocation() + " osmid=" + id + " " + s + " distance=" + String.format("%.2f", distance));
	}

	private static void printDataObject(BinaryMapDataObject object, Double distance) {
		StringBuilder s = new StringBuilder();
		BinaryInspector.printMapDetails(object, s, false);
		s.append(" distance=").append(String.format("%.2f", distance));
		System.out.println(s);
	}

	/**
	 * Calculate min distance from any point of the given {@link BinaryMapDataObject} lies within the specified radius
	 * (in meters)
	 * from the provided {@link LatLon} center.
	 */
	private static double getMinDistance(BinaryMapDataObject object, LatLon center, int radiusMeters) {
		int pointsCount = object.getPointsLength();
		double minDistance = Double.MAX_VALUE;
		for (int i = 0; i < pointsCount; i++) {
			double lat = MapUtils.get31LatitudeY(object.getPoint31YTile(i));
			double lon = MapUtils.get31LongitudeX(object.getPoint31XTile(i));
			double distance = MapUtils.getDistance(center.getLatitude(), center.getLongitude(), lat, lon);
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
