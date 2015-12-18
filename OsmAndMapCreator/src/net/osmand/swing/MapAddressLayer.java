package net.osmand.swing;

import java.awt.Graphics2D;
import java.awt.Point;
import java.awt.event.ActionEvent;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.swing.AbstractAction;
import javax.swing.Action;
import javax.swing.JPopupMenu;

import net.osmand.binary.BinaryMapAddressReaderAdapter;
import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.binary.BinaryMapRouteReaderAdapter.RouteRegion;
import net.osmand.binary.GeocodingUtilities;
import net.osmand.binary.GeocodingUtilities.GeocodingResult;
import net.osmand.data.City;
import net.osmand.data.DataTileManager;
import net.osmand.data.LatLon;
import net.osmand.data.Street;
import net.osmand.osm.edit.Entity;
import net.osmand.osm.edit.Node;
import net.osmand.osm.edit.OSMSettings.OSMTagKey;
import net.osmand.router.RoutePlannerFrontEnd;
import net.osmand.router.RoutingConfiguration;
import net.osmand.router.RoutingContext;
import net.osmand.util.MapUtils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class MapAddressLayer implements MapPanelLayer {

	private Log log = LogFactory.getLog(MapAddressLayer.class);
	
	private MapPanel map;
	private static int distance = 15000;
	
	@Override
	public void destroyLayer() {
		
	}

	@Override
	public void initLayer(MapPanel map) {
		this.map = map;
		fillPopupMenuWithActions(map.getPopupMenu());
	}

	public void fillPopupMenuWithActions(JPopupMenu menu) {
		Action where= new AbstractAction("Where am I?") {
			private static final long serialVersionUID = 7477484340246483239L;

			@Override
			public void actionPerformed(ActionEvent e) {
				whereAmI();
			}

			
		};
		menu.add(where);
		Action add= new AbstractAction("Show address") {
			private static final long serialVersionUID = 7477484340246483239L;

			@Override
			public void actionPerformed(ActionEvent e) {
				showCurrentCityActions();
			}

			
		};
		menu.add(add);
	}
	
	
	private void whereAmI() {
		Point popupMenuPoint = map.getPopupMenuPoint();
		double fy = (popupMenuPoint.y - map.getCenterPointY()) / map.getTileSize();
		double fx = (popupMenuPoint.x - map.getCenterPointX()) / map.getTileSize();
		final double latitude = MapUtils.getLatitudeFromTile(map.getZoom(), map.getYTile() + fy);
		final double longitude = MapUtils.getLongitudeFromTile(map.getZoom(), map.getXTile() + fx);
		new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					DataTileManager<Entity> points = new DataTileManager<Entity>(15);
					List<Entity> os = whereAmI(latitude, longitude, points);
					for (Entity w : os) {
						LatLon n = w.getLatLon();
						points.registerObject(n.getLatitude(), n.getLongitude(), w);
					}
					map.setPoints(points);
					map.repaint();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

		}).start();
	}
	
	private void showCurrentCityActions() {
		Point popupMenuPoint = map.getPopupMenuPoint();
		double fy = (popupMenuPoint.y - map.getCenterPointY()) / map.getTileSize();
		double fx = (popupMenuPoint.x - map.getCenterPointX()) / map.getTileSize();
		final double latitude = MapUtils.getLatitudeFromTile(map.getZoom(), map.getYTile() + fy);
		final double longitude = MapUtils.getLongitudeFromTile(map.getZoom(), map.getXTile() + fx);
		new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					DataTileManager<Entity> points = new DataTileManager<Entity>(15);
					List<Entity> os = searchAddress(latitude, longitude, points);
					for (Entity w : os) {
						LatLon n = w.getLatLon();
						points.registerObject(n.getLatitude(), n.getLongitude(), w);
					}
					map.setPoints(points);
					map.repaint();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

		}).start();
	}
	
	private List<Entity> searchAddress(double lat, double lon,
			final DataTileManager<Entity> points ) throws IOException{
		List<Entity> results = new ArrayList<Entity>(); 
		for (File f : new File(DataExtractionSettings.getSettings().getBinaryFilesDir()).listFiles()) {
			if (f.getName().endsWith(".obf")) {
				RandomAccessFile raf = new RandomAccessFile(f, "r"); //$NON-NLS-1$ //$NON-NLS-2$
				BinaryMapIndexReader rd = new BinaryMapIndexReader(raf, f);
				if(rd.containsAddressData() && (!rd.containsPoiData() || rd.containsPoiData(lat, lon))){
					searchAddressDetailedInfo(rd, lat, lon, results);
				}
				rd.close();
				raf.close();
			}
		}
		return results;
	}
	
	private List<Entity> whereAmI(double lat, double lon,
			final DataTileManager<Entity> points ) throws IOException{
		List<Entity> results = new ArrayList<Entity>(); 
		int x = MapUtils.get31TileNumberX(lon);
		int y = MapUtils.get31TileNumberY(lat);
		List<BinaryMapIndexReader> list = new ArrayList<BinaryMapIndexReader>();
		for (File f : new File(DataExtractionSettings.getSettings().getBinaryFilesDir()).listFiles()) {
			if (f.getName().endsWith(".obf")) {
				RandomAccessFile raf = new RandomAccessFile(f, "r"); //$NON-NLS-1$ //$NON-NLS-2$
				BinaryMapIndexReader rd = new BinaryMapIndexReader(raf, f);
				if(rd.containsAddressData() && rd.containsRouteData(x, y, x, y, 15)){
					list.add(rd);
				} else {
					rd.close();
					raf.close();
				}
			}
		}
		RoutingConfiguration cfg = RoutingConfiguration.getDefault().build("car", 100, 
				new HashMap<String, String>());
		RoutingContext ctx = new RoutePlannerFrontEnd(false).buildRoutingContext(cfg, null, list.toArray(new BinaryMapIndexReader[list.size()]));
		GeocodingUtilities su = new GeocodingUtilities();
		double minBuildingDistance = 0;
		List<GeocodingResult> complete = new ArrayList<GeocodingUtilities.GeocodingResult>();
		List<GeocodingResult> res = su.reverseGeocodingSearch(ctx, lat, lon);
		minBuildingDistance = justifyResults(list, su, complete, res);
//		complete.addAll(res);
		Collections.sort(complete, GeocodingUtilities.DISTANCE_COMPARATOR);
		long lid = -1;
		for(GeocodingResult r : complete) {
			if(r.building != null && 
					r.getDistance() > minBuildingDistance * GeocodingUtilities.THRESHOLD_MULTIPLIER_SKIP_BUILDINGS_AFTER) {
				continue;
			}
			Node n = new Node(r.getLocation().getLatitude(), r.getLocation().getLongitude(), lid--);
			n.putTag(OSMTagKey.NAME.getValue(), r.toString());
			results.add(n);
		}
		for(BinaryMapIndexReader l : list) {
			l.close();
		}
		return results;
	}

	private double justifyResults(List<BinaryMapIndexReader> list, GeocodingUtilities su,
			List<GeocodingResult> complete, List<GeocodingResult> res) throws IOException {
		double minBuildingDistance = 0;
		for (GeocodingResult r : res) {
			BinaryMapIndexReader reader = null;
			for (BinaryMapIndexReader b : list) {
				for (RouteRegion rb : b.getRoutingIndexes()) {
					if (r.regionFP == rb.getFilePointer() && r.regionLen == rb.getLength()) {
						reader = b;
						break;

					}
				}
				if (reader != null) {
					break;
				}
			}
			if (reader != null) {
				List<GeocodingResult> justified = su.justifyReverseGeocodingSearch(r, reader, minBuildingDistance, null);
				if(!justified.isEmpty()) {
					double md = justified.get(0).getDistance();
					if(minBuildingDistance == 0){
						minBuildingDistance = md;
					} else {
						minBuildingDistance = Math.min(md, minBuildingDistance);
					}
					complete.addAll(justified);
				}
			} else {
				complete.add(r);
			}
		}
		return minBuildingDistance;
	}
	
	private void searchAddressDetailedInfo(BinaryMapIndexReader index, double lat, double lon, List<Entity> results) throws IOException {
		Map<String, List<Street>> streets = new LinkedHashMap<String, List<Street>>();
			log.info("Searching region ");
			int[] cityType = new int[] {BinaryMapAddressReaderAdapter.CITY_TOWN_TYPE,
					BinaryMapAddressReaderAdapter.POSTCODES_TYPE, 
					BinaryMapAddressReaderAdapter.VILLAGES_TYPE};
			for (int j = 0; j < cityType.length; j++) {
				int type = cityType[j];
				for (City c : index.getCities(null, type)) {
					if (MapUtils.getDistance(c.getLocation(), lat, lon) < distance) {
						log.info("Searching city " + c.getName());
						index.preloadStreets(c, null);
						for (Street t : c.getStreets()) {
							Long id = t.getId();
							if(!streets.containsKey(t.getName())) {
								streets.put(t.getName(), new ArrayList<Street>());
							}
							streets.get(t.getName()).add(t);
//							index.preloadBuildings(t, null);
//							List<Street> streets = t.getIntersectedStreets();
//							if (streets != null && !streets.isEmpty()) {
//								for (Street s : streets) {
//									// TODO
//								}
//							}
						}
					}
				}
			}
		
		for(List<Street> l : streets.values()) {
			while(l.size() > 0){
				Street s = l.remove(l.size()-1);
				String cityName = s.getCity().getName();
				LatLon loc = s.getLocation();
				Node n = new Node(loc.getLatitude(), loc.getLongitude(), -1);
				for(int k = 0; k< l.size(); ){
					if(MapUtils.getDistance(l.get(k).getLocation(), loc) < 50) {
						Street ks = l.remove(k);
						cityName += ";"+ks.getCity().getName(); 
					} else {
						k++;
					}
				}
				n.putTag(OSMTagKey.NAME.getValue(), s.getName()+"\n"+cityName);
				results.add(n);
			}

		}
	}
	
	
	
	
	@Override
	public void prepareToDraw() {
	}

	@Override
	public void paintLayer(Graphics2D g) {
	}

}
