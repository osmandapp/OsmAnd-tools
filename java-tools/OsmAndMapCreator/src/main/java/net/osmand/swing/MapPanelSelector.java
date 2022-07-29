package net.osmand.swing;

import java.awt.Rectangle;
import java.awt.event.ActionEvent;
import java.awt.event.MouseEvent;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.swing.AbstractAction;
import javax.swing.JLabel;
import javax.swing.JPopupMenu;

import org.apache.commons.logging.Log;

import gnu.trove.list.array.TIntArrayList;
import net.osmand.GPXUtilities.GPXFile;
import net.osmand.GPXUtilities.Track;
import net.osmand.GPXUtilities.TrkSegment;
import net.osmand.GPXUtilities.WptPt;
import net.osmand.NativeJavaRendering.RenderingImageContext;
import net.osmand.NativeLibrary.RenderedObject;
import net.osmand.PlatformUtil;
import net.osmand.data.DataTileManager;
import net.osmand.data.LatLon;
import net.osmand.osm.edit.Entity;
import net.osmand.osm.edit.Node;
import net.osmand.osm.edit.Way;
import net.osmand.router.network.NetworkRouteContext.NetworkRouteContextStats;
import net.osmand.router.network.NetworkRouteSelector;
import net.osmand.router.network.NetworkRouteSelector.NetworkRouteSelectorFilter;
import net.osmand.router.network.NetworkRouteSelector.RouteKey;
import net.osmand.router.network.NetworkRouteSelector.RouteType;
import net.osmand.swing.MapPanel.NativeRendererRunnable;
import net.osmand.util.MapUtils;

public class MapPanelSelector {
	protected static final Log log = PlatformUtil.getLog(MapPanelSelector.class);
	private static final boolean LOAD_ROUTE_BBOX = false;
	private final MapPanel panel;
	private MapSelectionArea mapSelectionArea;
	private ThreadPoolExecutor threadPool;

	public MapPanelSelector(MapPanel panel) {
		this.panel = panel;
		mapSelectionArea = new MapSelectionArea();
		threadPool = new ThreadPoolExecutor(0, 1, 5, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(5));
	}
	
	public MapSelectionArea getSelectionArea() {
		return mapSelectionArea;
	}

	public void select(RenderingImageContext ctx, MouseEvent e) {
		if (panel.getNativeLibrary() == null) {
			return;
		}
		NativeRendererRunnable rr = panel.new NativeRendererRunnable(getWidth(), getHeight());
		int x = (int) ((rr.oleft - ctx.sleft) / MapUtils.getPowZoom(31 - getZoom()) * getTileSize() + e.getPoint().x);
		int y = (int) ((rr.otop - ctx.stop) / MapUtils.getPowZoom(31 - getZoom()) * getTileSize() + e.getPoint().y);
		System.out.println("Search objects at " + x + " " + y);
		RenderedObject[] ls = panel.getNativeLibrary().searchRenderedObjectsFromContext(ctx.context, x, y, true);
		DataTileManager<Entity> points = new DataTileManager<Entity>(6);
		if (ls != null && ls.length > 0) {
			for (RenderedObject o : ls) {
				System.out.println((o.isText() ? o.getName() : "Icon") + " " + o.getId() + " " + o.getTags() + " ("
						+ o.getBbox() + ") " + " order = " + o.getOrder() + " visible = " + o.isVisible());
				if (!o.isVisible()) {
					continue;
				}
				if (o.getX().size() > 1) {
					List<RouteKey> keys = RouteType.getRouteKeys(o);
					if (keys.size() > 0) {
						try {
							createMenu(o).show(panel, e.getX(), e.getY());
						} catch (IOException e1) {
							log.error(e1.getMessage(), e1);
						}
					}
					Way way = new Way(-1);
					TIntArrayList x1 = o.getX();
					TIntArrayList y1 = o.getY();
					for (int i = 0; i < x1.size(); i++) {
						double lat = MapUtils.get31LatitudeY(y1.get(i));
						double lon = MapUtils.get31LongitudeX(x1.get(i));
						Node n = new Node(lat, lon, -1);
						way.addNode(n);
					}
					LatLon wayCenter = way.getLatLon();
					points.registerObject(wayCenter.getLatitude(), wayCenter.getLongitude(), way);
				} else {
					LatLon n = o.getLabelLatLon();
					if (n == null) {
						n = o.getLocation();
					}
					if (n != null) {
						Node nt = new Node(n.getLatitude(), n.getLongitude(), -1);
						points.registerObject(n.getLatitude(), n.getLongitude(), nt);
					}
				}
			}
		}
		panel.setPoints(points);
		panel.repaint();
	}
	
	private JPopupMenu createMenu(RenderedObject renderedObject) throws IOException {
		JPopupMenu menu = new JPopupMenu();
		menu.add(new JLabel("Select route"));
		Map<RouteKey, GPXFile> routeMap ;
		NetworkRouteSelectorFilter f = new NetworkRouteSelectorFilter();
		NetworkRouteSelector routeSelector = new NetworkRouteSelector(DataExtractionSettings.getSettings().getObfReaders(), f, null, false);
		Collection<RouteKey> routeKeys;
		if (LOAD_ROUTE_BBOX) {
			routeMap = routeSelector.getRoutes(panel.getLatLonBBox(), true, null);
			routeKeys = routeMap.keySet();
		} else {
			routeMap = null;
			routeKeys = RouteType.getRouteKeys(renderedObject);
		}
		for (RouteKey routeKey : routeKeys) {
			menu.add(new AbstractAction(routeKey.set.toString()) {
				private static final long serialVersionUID = 8970133073749840163L;

				@Override
				public void actionPerformed(ActionEvent actionEvent) {
					threadPool.submit(() -> {
						try {
							Map<RouteKey, GPXFile> routes;
							long tms = System.currentTimeMillis();
							if (routeMap == null || routeMap.get(routeKey) == null) {
								System.out.println("Start loading " + routeKey.set);
								if (LOAD_ROUTE_BBOX) {
									Map<RouteKey, GPXFile> res = routeSelector.getRoutes(panel.getLatLonBBox(), true, routeKey);
									routes = Collections.singletonMap(routeKey, res.get(routeKey));
								} else {
									f.keyFilter = Collections.singleton(routeKey);
									routes = routeSelector.getRoutes(renderedObject); 	
								}
							} else {
//								routes = routeMap;
								routes = Collections.singletonMap(routeKey, routeMap.get(routeKey));
							}
							NetworkRouteContextStats stats = routeSelector.getNetworkRouteContext().getStats();
							String msg = String.format("Load route after %.1f s: loaded %d tiles, %d routes, time %.1f s",
									(System.currentTimeMillis() - tms) / 1.0e3, stats.loadedTiles,
									stats.loadedRoutes, stats.loadTimeNs / 1.0e9);
							System.out.println(msg);
							drawGpxFiles(routes.values());
						} catch (Exception e) {
							log.error(e.getMessage(), e);
						}
					});
				}
			});
		}
		return menu;
	}

	private void drawGpxFiles(Collection<GPXFile> files) {
		DataTileManager<Entity> points = new DataTileManager<>(4);
		List<Way> ways = new ArrayList<>();
		for (GPXFile file : files) {
			for (Track track : file.tracks) {
				for (TrkSegment segment : track.segments) {
					Way w = new Way(-1);
					Node last = null, first = null;
					for (WptPt point : segment.points) {
						last = new Node(point.lat, point.lon, -1);
						if (first == null) {
							first = last;
						}
						w.addNode(last);
					}
					if (first != null) {
						ways.add(w);
						LatLon n = w.getLatLon();
						points.registerObject(n.getLatitude(), n.getLongitude(), w);
						first = new Node(first.getLatitude() + Math.random() * 0.00005, first.getLongitude(), -1);
						last = new Node(last.getLatitude() + Math.random() * 0.00005, last.getLongitude(), -1);
						first.putTag("colour", "green");
						points.registerObject(first.getLatitude(), first.getLongitude(), first);
						last.putTag("colour", "red");
						points.registerObject(last.getLatitude(), last.getLongitude(), last);
					}
				}
			}
			panel.setPoints(points);
		}
	}
	
	private int getHeight() {
		return panel.getHeight();
	}
	
	public int getWidth() {
		return panel.getWidth();
	}


	public double getTileSize() {
		return panel.getTileSize();
	}

	public double getYTile() {
		return panel.getYTile();
	}

	public double getXTile() {
		return panel.getXTile();
	}

	public int getZoom() {
		return panel.getZoom();
	}
	

	public double getLongitude() {
		return panel.getLongitude();
	}


	public double getLatitude() {
		return panel.getLatitude();
	}
	
	public double getMapDensity() {
		return panel.getMapDensity();
	}

	
	public class MapSelectionArea {
		private double lat1;
		private double lon1;
		private double lat2;
		private double lon2;

		public double getLat1() {
			return lat1;
		}

		public double getLat2() {
			return lat2;
		}

		public double getLon1() {
			return lon1;
		}

		public double getLon2() {
			return lon2;
		}

		public Rectangle getSelectedArea(){
			Rectangle r = new Rectangle();
			int zoom = getZoom();
			r.x = getWidth() / 2 + MapUtils.getPixelShiftX((float) (zoom + Math.log(getMapDensity()) / Math.log(2)), lon1, getLongitude(), getTileSize());
			r.y = getHeight() / 2 + MapUtils.getPixelShiftY((float) (zoom + Math.log(getMapDensity()) / Math.log(2)), lat1, getLatitude(), getTileSize());
			r.width = getWidth() / 2 + MapUtils.getPixelShiftX((float) (zoom + Math.log(getMapDensity()) / Math.log(2)), lon2, getLongitude(), getTileSize()) - r.x;
			r.height = getHeight() / 2 + MapUtils.getPixelShiftY((float) (zoom + Math.log(getMapDensity()) / Math.log(2)), lat2, getLatitude(), getTileSize()) - r.y;
			return r;
		}


		public boolean isVisible(){
			if(lat1 == lat2 || lon1 == lon2){
				return false;
			}
			Rectangle area = getSelectedArea();
			return area.width > 4 && area.height > 4;
		}
		

		public void setSelectedArea(int x1, int y1, int x2, int y2){
			int rx1 = Math.min(x1, x2);
			int rx2 = Math.max(x1, x2);
			int ry1 = Math.min(y1, y2);
			int ry2 = Math.max(y1, y2);
			int zoom = getZoom();
			double xTile = getXTile();
			double yTile = getYTile();
			int wid = getWidth();
			int h = getHeight();
			double tileSize = getTileSize();

			double xTile1 = xTile - (wid / 2 - rx1) / (tileSize);
			double yTile1 = yTile - (h / 2 - ry1) / (tileSize);
			double xTile2 = xTile - (wid / 2 - rx2) / (tileSize);
			double yTile2 = yTile - (h / 2 - ry2) / (tileSize);
			lat1 = MapUtils.getLatitudeFromTile(zoom, yTile1);
			lat2 = MapUtils.getLatitudeFromTile(zoom, yTile2);
			lon1 = MapUtils.getLongitudeFromTile(zoom, xTile1);
			lon2 = MapUtils.getLongitudeFromTile(zoom, xTile2);
			panel.getLayer(MapInformationLayer.class).setAreaButtonVisible(isVisible());
		}

	}

	
}
