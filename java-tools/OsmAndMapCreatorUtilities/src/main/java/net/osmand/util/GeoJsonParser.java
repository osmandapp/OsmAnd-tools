package net.osmand.util;

import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.binary.RouteDataObject;
import net.osmand.data.*;
import net.osmand.osm.edit.Entity;
import net.osmand.router.RoutePlannerFrontEnd;
import net.osmand.router.RoutingConfiguration;
import net.osmand.router.RoutingContext;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;


public class GeoJsonParser {

	public List<LatLon> pseudoGeoJsonCoordinates;
	double northLatitude = 52.5483;
	double westLongitude = 13.4021;
	double southLatitude = 52.514;
	double eastLongitude = 13.4778;
	RoutingContext ctx;

	private QuadTree<LatLon> geoJsonQuadTree = new QuadTree<LatLon>(new QuadRect(0, 0, Integer.MAX_VALUE, Integer.MAX_VALUE),
            8, 0.55f);

	public GeoJsonParser(RoutingContext ctx) {
		init(ctx);
	}

	public GeoJsonParser() {
		try {
			ctx = initRoutingContext();
			init(ctx);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void init(RoutingContext ctx) {
		this.ctx = ctx;
		generatePseudoJsonCoordinates();
		setGeoJsonQuadTree(ctx);
	}

	public static void main(String[] args) {

		GeoJsonParser geoJsonParser = new GeoJsonParser();
		geoJsonParser.generatePseudoJsonCoordinates();
		RoutingContext ctx = null;
		try {
			ctx = geoJsonParser.initRoutingContext();
			if (ctx != null) {
				geoJsonParser.setGeoJsonQuadTree(ctx);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		int x = 1153625984;
		int y = 704371968;
		List<LatLon> res = new ArrayList<>();
//		geoJsonParser.geoJsonQuadTree.queryInBox(new QuadRect(x-1,y-1,x+1,y+1 ),res);
		geoJsonParser.geoJsonQuadTree.queryInBox(new QuadRect(10, 1, 100, 400), res);
		System.out.println("size " + res.size());
	}


	public void setGeoJsonQuadTree(RoutingContext ctx) {
		for (LatLon latLon : pseudoGeoJsonCoordinates) {
			double[] result = new double[2];
			if (findRouteSegment(latLon, ctx, 100, result)) {
				double y = result[1];
				double x = result[0];
				LatLon savedLatLon = new LatLon(MapUtils.get31LatitudeY((int) y), MapUtils.get31LongitudeX((int) x));
				System.out.println(MapUtils.get31LatitudeY((int) y) + " " + MapUtils.get31LongitudeX((int) x));
				QuadRect qr = new QuadRect(x, y, x, y);
				System.out.println((int) x + " " + (int) y);
				geoJsonQuadTree.insert(savedLatLon, qr);
			}
		}
	}

	/**
	 * data as LatLon
	 * QuadRect bbox in tile31 format
	 *
	 * @return
	 */
	public QuadTree<LatLon> getGeoJsonQuadTree() {
		return geoJsonQuadTree;
	}

	private boolean findRouteSegment(LatLon latLon, RoutingContext ctx, double maxDist, double[] result) {
		int px = MapUtils.get31TileNumberX(latLon.getLongitude());
		int py = MapUtils.get31TileNumberY(latLon.getLatitude());
		ArrayList<RouteDataObject> dataObjects = new ArrayList<RouteDataObject>();
		ctx.loadTileData(px, py, 17, dataObjects, true);
		if (dataObjects.isEmpty()) {
			ctx.loadTileData(px, py, 15, dataObjects, true);
		}
		if (dataObjects.isEmpty()) {
			ctx.loadTileData(px, py, 14, dataObjects, true);
		}
		for (RouteDataObject r : dataObjects) {
			if (r.getPointsLength() > 1) {
				for (int j = 1; j < r.getPointsLength(); j++) {
					QuadPoint pr = MapUtils.getProjectionPoint31(px, py, r.getPoint31XTile(j - 1),
							r.getPoint31YTile(j - 1), r.getPoint31XTile(j), r.getPoint31YTile(j));
					double currentsDistSquare = squareDist((int) pr.x, (int) pr.y, px, py);
					if (currentsDistSquare <= maxDist) {
						result[0] = pr.x;
						result[1] = pr.y;
						return true;
					}
				}
			}
		}
		return false;
	}

	private static double squareDist(int x1, int y1, int x2, int y2) {
		// translate into meters
		double dy = MapUtils.convert31YToMeters(y1, y2, x1);
		double dx = MapUtils.convert31XToMeters(x1, x2, y1);
		return dx * dx + dy * dy;
	}

	public void generatePseudoJsonCoordinates() {
		pseudoGeoJsonCoordinates = new ArrayList<>();
//        int i = 0;
//        double diffLat = northLatitude - southLatitude;
//        double diffLon = eastLongitude - westLongitude;
//        while (i < 10) {
//            double rand1 = Math.random();
//            double rand2 = Math.random();
//            double lat = southLatitude + rand1 * diffLat;
//            double lon = westLongitude + rand2 * diffLon;
//            pseudoGeoJsonCoordinates.add(new LatLon(lat, lon));
//            i++;
//        }
		pseudoGeoJsonCoordinates.add(new LatLon(52.5104520, 13.3916206));

	}

	private RoutingContext initRoutingContext() throws IOException {
		List<Entity> results = new ArrayList<Entity>();
		int x = MapUtils.get31TileNumberX(westLongitude);
		int y = MapUtils.get31TileNumberY(northLatitude);
		List<BinaryMapIndexReader> list = new ArrayList<BinaryMapIndexReader>();
//        for (File f : new File(DataExtractionSettings.getSettings().getBinaryFilesDir()).listFiles()) {
//            if (f.getName().endsWith(".obf")) {
		File f = new File("/home/user/osmand/origin/maps/Germany_berlin_europe.obf");
		RandomAccessFile raf = new RandomAccessFile(f, "r"); //$NON-NLS-1$ //$NON-NLS-2$
		BinaryMapIndexReader rd = new BinaryMapIndexReader(raf, f);
		if (rd.containsAddressData() && rd.containsRouteData(x, y, x, y, 15)) {
			list.add(rd);
		} else {
			rd.close();
			raf.close();
		}
//            }
//        }
		RoutingConfiguration.Builder builder = RoutingConfiguration.getDefault();
		RoutingConfiguration config = builder.build("geocoding", RoutingConfiguration.DEFAULT_MEMORY_LIMIT * 3);
		return new RoutePlannerFrontEnd().buildRoutingContext(config, null, list.toArray(new BinaryMapIndexReader[list.size()]));
	}

}
