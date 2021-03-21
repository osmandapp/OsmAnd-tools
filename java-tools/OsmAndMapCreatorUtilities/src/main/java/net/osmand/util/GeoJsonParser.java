package net.osmand.util;

import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.binary.RouteDataObject;
import net.osmand.data.*;
import net.osmand.router.RoutePlannerFrontEnd;
import net.osmand.router.RoutingConfiguration;
import net.osmand.router.RoutingContext;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

public class GeoJsonParser {

	public static final double TEST_LAT = 52.51045;
	public static final double TEST_LON = 13.39151;
	public static final int MAX_DIST = 10;
	public List<LatLon> pseudoGeoJsonCoordinates;
	double northLatitude = 52.5483;
	double westLongitude = 13.4021;
//	double southLatitude = 52.514;
//	double eastLongitude = 13.4778;
	RoutingContext ctx;

	private final QuadTree<double[]> geoJsonQuadTree = new QuadTree<>(new QuadRect(0, 0, Integer.MAX_VALUE,
			Integer.MAX_VALUE),
			8, 0.55f);

	public GeoJsonParser(RoutingContext ctx) {
		init(ctx);
	}

	public GeoJsonParser(File mapFile) {
		ctx = initRoutingContext(mapFile);
		init(ctx);
	}

	private void init(RoutingContext ctx) {
		this.ctx = ctx;
		generatePseudoJsonCoordinates();
		setGeoJsonQuadTree(ctx);
	}

	public static void main(String[] args) {
		String obfFileName = args.length > 0 ? args[0] : "";
		if (obfFileName.endsWith(".obf")) {
			File mapFile = new File(obfFileName);
			if (mapFile.exists()) {
				GeoJsonParser geoJsonParser = new GeoJsonParser(mapFile);
				LatLon testPoint = new LatLon(TEST_LAT, TEST_LON);
				int x = MapUtils.get31TileNumberX(testPoint.getLongitude());
				int y = MapUtils.get31TileNumberY(testPoint.getLatitude());
				List<double[]> res = new ArrayList<>();
				geoJsonParser.geoJsonQuadTree.queryInBox(new QuadRect(x - 10, y - 10, x + 10, y + 10), res);
//				geoJsonParser.geoJsonQuadTree.queryInBox(new QuadRect(10, 1, 100, 400), res);
				System.out.println("size " + res.size());
			} else {
				System.out.println("File " + obfFileName + " not exist");
			}
		} else {
			System.out.println("First argument is not obf file name");
		}
	}

	public void setGeoJsonQuadTree(RoutingContext ctx) {
		for (LatLon latLon : pseudoGeoJsonCoordinates) {
			double[] result = new double[4];
			if (findRouteSegment(latLon, ctx, MAX_DIST, result)) {
				QuadRect qr = new QuadRect(result[0], result[1], result[2], result[3]);
				geoJsonQuadTree.insert(result, qr);
			}
		}
	}

	/**
	 * data as double[4] x1,y1,x2,y2 coordinates of start and end points
	 * QuadRect bbox in tile31 format
	 *
	 * @return QuadTree<double[]>
	 */
	public QuadTree<double[]> getGeoJsonQuadTree() {
		return geoJsonQuadTree;
	}

	private boolean findRouteSegment(LatLon latLon, RoutingContext ctx, double maxDist, double[] result) {
		int px = MapUtils.get31TileNumberX(latLon.getLongitude());
		int py = MapUtils.get31TileNumberY(latLon.getLatitude());
		ArrayList<RouteDataObject> dataObjects = new ArrayList<>();
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
					if (currentsDistSquare <= maxDist * maxDist) {
						result[0] = r.getPoint31XTile(j - 1);
						result[1] = r.getPoint31YTile(j - 1);
						result[2] = r.getPoint31XTile(j);
						result[3] = r.getPoint31YTile(j);
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
		pseudoGeoJsonCoordinates.add(new LatLon(TEST_LAT, TEST_LON));

	}

	private RoutingContext initRoutingContext(File f) {
		int x = MapUtils.get31TileNumberX(westLongitude);
		int y = MapUtils.get31TileNumberY(northLatitude);
		List<BinaryMapIndexReader> list = new ArrayList<>();
		RandomAccessFile raf;
		try {
			raf = new RandomAccessFile(f, "r");
			BinaryMapIndexReader rd = new BinaryMapIndexReader(raf, f);
			if (rd.containsAddressData() && rd.containsRouteData(x, y, x, y, 15)) {
				list.add(rd);
			} else {
				rd.close();
				raf.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		RoutingConfiguration.Builder builder = RoutingConfiguration.getDefault();
		RoutingConfiguration config = builder.build("geocoding", RoutingConfiguration.DEFAULT_MEMORY_LIMIT * 3);
		return new RoutePlannerFrontEnd().buildRoutingContext(config, null, list.toArray(new BinaryMapIndexReader[0]));
	}

}
