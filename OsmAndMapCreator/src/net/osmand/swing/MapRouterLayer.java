package net.osmand.swing;

import java.awt.Color;
import java.awt.Component;
import java.awt.Graphics2D;
import java.awt.Point;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.io.StringReader;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.List;

import javax.swing.AbstractAction;
import javax.swing.Action;
import javax.swing.JButton;
import javax.swing.JFileChooser;
import javax.swing.JOptionPane;
import javax.swing.JPopupMenu;
import javax.swing.SwingUtilities;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import net.osmand.PlatformUtil;
import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.data.DataTileManager;
import net.osmand.data.LatLon;
import net.osmand.osm.edit.Entity;
import net.osmand.osm.edit.OSMSettings.OSMTagKey;
import net.osmand.osm.edit.OsmMapUtils;
import net.osmand.osm.edit.Way;
import net.osmand.router.BinaryRoutePlanner.RouteSegment;
import net.osmand.router.BinaryRoutePlanner.RouteSegmentVisitor;
import net.osmand.router.RoutePlannerFrontEnd;
import net.osmand.router.RouteSegmentResult;
import net.osmand.router.RoutingConfiguration;
import net.osmand.router.RoutingConfiguration.Builder;
import net.osmand.router.RoutingContext;
import net.osmand.util.MapUtils;

import org.apache.commons.logging.Log;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xmlpull.v1.XmlPullParserException;



public class MapRouterLayer implements MapPanelLayer {
	
	private final static Log log = PlatformUtil.getLog(MapRouterLayer.class);
	private boolean USE_OLD_ROUTING = false;
	private boolean USE_NATIVE_ROUTING = false;

	private MapPanel map;
	private LatLon startRoute ;
	private LatLon endRoute ;
	private List<LatLon> intermediates = new ArrayList<LatLon>();
	private boolean nextAvailable = true;
	private boolean pause = true;
	private boolean stop = false;
	private int steps = 1;
	private JButton nextTurn;
	private JButton playPauseButton;
	private JButton stopButton;

	private List<RouteSegmentResult> previousRoute;
	
	
	@Override
	public void destroyLayer() {
		
	}

	@Override
	public void initLayer(MapPanel map) {
		this.map = map;
		fillPopupMenuWithActions(map.getPopupMenu());
		startRoute =  DataExtractionSettings.getSettings().getStartLocation();
		endRoute =  DataExtractionSettings.getSettings().getEndLocation();
		
		nextTurn = new JButton(">>"); //$NON-NLS-1$
		nextTurn.addActionListener(new ActionListener(){
			@Override
			public void actionPerformed(ActionEvent e) {
				nextAvailable = true;
				synchronized (MapRouterLayer.this) {
					MapRouterLayer.this.notify();
				}
			}
		});
		nextTurn.setVisible(false);
		nextTurn.setAlignmentY(Component.TOP_ALIGNMENT);
		map.add(nextTurn, 0);
		playPauseButton = new JButton("Play"); //$NON-NLS-1$
		playPauseButton.addActionListener(new ActionListener(){
			@Override
			public void actionPerformed(ActionEvent e) {
				pause = !pause; 
				playPauseButton.setText(pause ? "Play" : "Pause");
				nextAvailable = true;
				synchronized (MapRouterLayer.this) {
					MapRouterLayer.this.notify();
				}
			}
		});
		playPauseButton.setVisible(false);
		playPauseButton.setAlignmentY(Component.TOP_ALIGNMENT);
		map.add(playPauseButton, 0);
		stopButton = new JButton("Stop"); //$NON-NLS-1$
		stopButton.addActionListener(new ActionListener(){
			@Override
			public void actionPerformed(ActionEvent e) {
				stop = true;
				nextAvailable = true;
				synchronized (MapRouterLayer.this) {
					MapRouterLayer.this.notify();
				}
			}
		});
		stopButton.setVisible(false);
		stopButton.setAlignmentY(Component.TOP_ALIGNMENT);
		map.add(stopButton);
	}

	public void fillPopupMenuWithActions(JPopupMenu menu) {
		Action start = new AbstractAction("Mark start point") {
			private static final long serialVersionUID = 507156107455281238L;

			@Override
			public void actionPerformed(ActionEvent e) {
				Point popupMenuPoint = map.getPopupMenuPoint();
				double fy = (popupMenuPoint.y - map.getCenterPointY()) / map.getTileSize();
				double fx = (popupMenuPoint.x - map.getCenterPointX()) / map.getTileSize();
				double latitude = MapUtils.getLatitudeFromTile(map.getZoom(), map.getYTile() + fy);
				double longitude = MapUtils.getLongitudeFromTile(map.getZoom(), map.getXTile() + fx);
				startRoute = new LatLon(latitude, longitude);
				DataExtractionSettings.getSettings().saveStartLocation(latitude, longitude);
				map.repaint();
			}
		};
		menu.add(start);
		Action end= new AbstractAction("Mark end point") {
			private static final long serialVersionUID = 4446789424902471319L;

			@Override
			public void actionPerformed(ActionEvent e) {
				Point popupMenuPoint = map.getPopupMenuPoint();
				double fy = (popupMenuPoint.y - map.getCenterPointY()) / map.getTileSize();
				double fx = (popupMenuPoint.x - map.getCenterPointX()) / map.getTileSize();
				double latitude = MapUtils.getLatitudeFromTile(map.getZoom(), map.getYTile() + fy);
				double longitude = MapUtils.getLongitudeFromTile(map.getZoom(), map.getXTile() + fx);
				endRoute = new LatLon(latitude, longitude);
				DataExtractionSettings.getSettings().saveEndLocation(latitude, longitude);
				map.repaint();
			}
		};
		menu.add(end);
		Action selfRoute = new AbstractAction("Calculate OsmAnd route") {
			private static final long serialVersionUID = 507156107455281238L;

			@Override
			public void actionPerformed(ActionEvent e) {
				previousRoute = null;
				calcRoute(false);
			}
		};
		menu.add(selfRoute);
		
		Action selfBaseRoute = new AbstractAction("Calculate OsmAnd base route") {
			private static final long serialVersionUID = 8049785829806139142L;

			@Override
			public void actionPerformed(ActionEvent e) {
				previousRoute = null;
				calcRoute(true);
			}
		};
		menu.add(selfBaseRoute);
		
		Action recalculate = new AbstractAction("Recalculate OsmAnd route") {
			private static final long serialVersionUID = 507156107455281238L;
			
			@Override
			public boolean isEnabled() {
//				return previousRoute != null;
				return true;
			}

			@Override
			public void actionPerformed(ActionEvent e) {
				calcRoute(false);
			}
		};
		
		
		menu.add(recalculate);
		Action route_YOURS = new AbstractAction("Calculate YOURS route") {
			private static final long serialVersionUID = 507156107455281238L;

			@Override
			public void actionPerformed(ActionEvent e) {
				new Thread(){
					@Override
					public void run() {
						List<Way> ways = route_YOURS(startRoute, endRoute);
						DataTileManager<Way> points = new DataTileManager<Way>(11);
						for(Way w : ways){
							LatLon n = w.getLatLon();
							points.registerObject(n.getLatitude(), n.getLongitude(), w);
						}
						map.setPoints(points);
					}
				}.start();
			}
		};
		menu.add(route_YOURS);
		Action loadGPXFile = new AbstractAction("Load GPX file...") {
			private static final long serialVersionUID = 507156107455281238L;

			@Override
			public void actionPerformed(ActionEvent e) {
				new Thread(){
					@Override
					public void run() {
						JFileChooser fileChooser = new JFileChooser();
						if (fileChooser.showOpenDialog(map) == JFileChooser.APPROVE_OPTION) {
							File file = fileChooser.getSelectedFile();
							DataTileManager<Way> points = new DataTileManager<Way>(11);
							List<Way> ways = parseGPX(file);
							for (Way w : ways) {
								LatLon n = w.getLatLon();
								points.registerObject(n.getLatitude(), n.getLongitude(), w);
							}
							// load from file
							map.setPoints(points);
						}
					}
				}.start();
			}
		};
		menu.add(loadGPXFile);
		Action route_CloudMate = new AbstractAction("Calculate CloudMade route") {
			private static final long serialVersionUID = 507156107455281238L;

			@Override
			public void actionPerformed(ActionEvent e) {
				new Thread() {
					@Override
					public void run() {
						List<Way> ways = route_CloudMate(startRoute, endRoute);
						DataTileManager<Way> points = new DataTileManager<Way>(11);
						for (Way w : ways) {
							LatLon n = w.getLatLon();
							points.registerObject(n.getLatitude(), n.getLongitude(), w);
						}
						map.setPoints(points);
					}
				}.start();
			}
		};
		menu.add(route_CloudMate);
		Action swapLocations = new AbstractAction("Swap locations") {
			private static final long serialVersionUID = 507156107455281238L;

			@Override
			public void actionPerformed(ActionEvent e) {
				LatLon l = endRoute;
				endRoute = startRoute;
				startRoute = l;
				map.repaint();
			}
		};
		menu.add(swapLocations);
		Action addIntermediate = new AbstractAction("Add transit point") {

			@Override
			public void actionPerformed(ActionEvent e) {
				Point popupMenuPoint = map.getPopupMenuPoint();
				double fy = (popupMenuPoint.y - map.getCenterPointY()) / map.getTileSize();
				double fx = (popupMenuPoint.x - map.getCenterPointX()) / map.getTileSize();
				double latitude = MapUtils.getLatitudeFromTile(map.getZoom(), map.getYTile() + fy);
				double longitude = MapUtils.getLongitudeFromTile(map.getZoom(), map.getXTile() + fx);
				intermediates.add(new LatLon(latitude, longitude));
				map.repaint();
			}
		};
		menu.add(addIntermediate);
		
		Action remove = new AbstractAction("Remove transit point") {

			@Override
			public void actionPerformed(ActionEvent e) {
				if(intermediates.size() > 0){
					intermediates.remove(0);
				}
				map.repaint();
			}
		};
		menu.add(remove);

	}
	
	
	private void calcRoute(final boolean useBasemap) {
		new Thread() {
			@Override
			public void run() {
				List<Way> ways = selfRoute(startRoute, endRoute, intermediates, previousRoute, useBasemap);
				if (ways != null) {
					DataTileManager<Way> points = new DataTileManager<Way>(11);
					for (Way w : ways) {
						LatLon n = w.getLatLon();
						points.registerObject(n.getLatitude(), n.getLongitude(), w);
					}
					map.setPoints(points);
				}
			}
		}.start();
	}
	
	public static List<Way> route_YOURS(LatLon start, LatLon end){
		List<Way> res = new ArrayList<Way>();
		long time = System.currentTimeMillis();
		System.out.println("Route from " + start + " to " + end);
		if (start != null && end != null) {
			try {
				StringBuilder uri = new StringBuilder();
				uri.append("http://www.yournavigation.org/api/1.0/gosmore.php?format=kml");
				uri.append("&flat=").append(start.getLatitude());
				uri.append("&flon=").append(start.getLongitude());
				uri.append("&tlat=").append(end.getLatitude());
				uri.append("&tlon=").append(end.getLongitude());
				uri.append("&v=motorcar").append("&fast=1").append("&layer=mapnik");

				URL url = new URL(uri.toString());
				URLConnection connection = url.openConnection();
				StringBuilder content = new StringBuilder();
				BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
				{
					String s = null;
					boolean fist = true;
					while ((s = reader.readLine()) != null) {
						if (fist) {
							fist = false;
						}
						content.append(s).append("\n");
					}
					System.out.println(content);
				}
				DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
				DocumentBuilder dom = factory.newDocumentBuilder();
				Document doc = dom.parse(new InputSource(new StringReader(content.toString())));
				NodeList list = doc.getElementsByTagName("coordinates");
				for(int i=0; i<list.getLength(); i++){
					Node item = list.item(i);
					String str = item.getTextContent();
					int st = 0;
					int next = 0;
					Way w = new Way(-1);
					while((next = str.indexOf('\n', st)) != -1){
						String coordinate = str.substring(st, next + 1);
						int s = coordinate.indexOf(',');
						if (s != -1) {
							try {
								double lon = Double.parseDouble(coordinate.substring(0, s));
								double lat = Double.parseDouble(coordinate.substring(s + 1));
								w.addNode(new net.osmand.osm.edit.Node(lat, lon, -1));
							} catch (NumberFormatException e) {
							}
						}
						st = next + 1;
					}
					if(!w.getNodes().isEmpty()){
						res.add(w);
					}
					
				}
			} catch (IOException e) {
				ExceptionHandler.handle(e);
			} catch (ParserConfigurationException e) {
				ExceptionHandler.handle(e);
			} catch (SAXException e) {
				ExceptionHandler.handle(e);
			}
			System.out.println("Finding routes " + res.size() + " " + (System.currentTimeMillis() - time) + " ms");
		}
		return res;
	}
	
	public List<Way> parseGPX(File f) {
		List<Way> res = new ArrayList<Way>();
		try {
			StringBuilder content = new StringBuilder();
			BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(f)));
			{
				String s = null;
				boolean fist = true;
				while ((s = reader.readLine()) != null) {
					if (fist) {
						fist = false;
					}
					content.append(s).append("\n");
				}
			}
			DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
			DocumentBuilder dom = factory.newDocumentBuilder();
			Document doc = dom.parse(new InputSource(new StringReader(content.toString())));
			NodeList list = doc.getElementsByTagName("trkpt");
			Way w = new Way(-1);
			for (int i = 0; i < list.getLength(); i++) {
				Element item = (Element) list.item(i);
				try {
					double lon = Double.parseDouble(item.getAttribute("lon"));
					double lat = Double.parseDouble(item.getAttribute("lat"));
					w.addNode(new net.osmand.osm.edit.Node(lat, lon, -1));
				} catch (NumberFormatException e) {
				}
			}
			if (!w.getNodes().isEmpty()) {
				res.add(w);
			}
		} catch (IOException e) {
			ExceptionHandler.handle(e);
		} catch (ParserConfigurationException e) {
			ExceptionHandler.handle(e);
		} catch (SAXException e) {
			ExceptionHandler.handle(e);
		}
		return res;
	}
	

		
	public List<Way> route_CloudMate(LatLon start, LatLon end) {
		List<Way> res = new ArrayList<Way>();
		long time = System.currentTimeMillis();
		System.out.println("Cloud made route from " + start + " to " + end);
		if (start != null && end != null) {
			try {
				StringBuilder uri = new StringBuilder();
				// possibly hide that API key because it is privacy of osmand
				uri.append("http://routes.cloudmade.com/A6421860EBB04234AB5EF2D049F2CD8F/api/0.3/");
				 
				uri.append(String.valueOf(start.getLatitude())).append(",");
				uri.append(String.valueOf(start.getLongitude())).append(",");
				uri.append(String.valueOf(end.getLatitude())).append(",");
				uri.append(String.valueOf(end.getLongitude())).append("/");
				uri.append("car.gpx").append("?lang=ru");

				URL url = new URL(uri.toString());
				URLConnection connection = url.openConnection();	
				StringBuilder content = new StringBuilder();
				BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
				{
					String s = null;
					boolean fist = true;
					while ((s = reader.readLine()) != null) {
						if (fist) {
							fist = false;
						}
						content.append(s).append("\n");
					}
					System.out.println(content);
				}
				DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
				DocumentBuilder dom = factory.newDocumentBuilder();
				Document doc = dom.parse(new InputSource(new StringReader(content.toString())));
				NodeList list = doc.getElementsByTagName("wpt");
				Way w = new Way(-1);
				for (int i = 0; i < list.getLength(); i++) {
					Element item = (Element) list.item(i);
					try {
						double lon = Double.parseDouble(item.getAttribute("lon"));
						double lat = Double.parseDouble(item.getAttribute("lat"));
						w.addNode(new net.osmand.osm.edit.Node(lat, lon, -1));
					} catch (NumberFormatException e) {
					}
				}
				list = doc.getElementsByTagName("rtept");
				for (int i = 0; i < list.getLength(); i++) {
					Element item = (Element) list.item(i);
					try {
						double lon = Double.parseDouble(item.getAttribute("lon"));
						double lat = Double.parseDouble(item.getAttribute("lat"));
						System.out.println("Lat " + lat + " lon " + lon);
						System.out.println("Distance : " + item.getElementsByTagName("distance").item(0).getTextContent());
						System.out.println("Time : " + item.getElementsByTagName("time").item(0).getTextContent());
						System.out.println("Offset : " + item.getElementsByTagName("offset").item(0).getTextContent());
						System.out.println("Direction : " + item.getElementsByTagName("direction").item(0).getTextContent());
					} catch (NumberFormatException e) {
					}
				}
				
				if (!w.getNodes().isEmpty()) {
					res.add(w);
				}
			} catch (IOException e) {
				ExceptionHandler.handle(e);
			} catch (ParserConfigurationException e) {
				ExceptionHandler.handle(e);
			} catch (SAXException e) {
				ExceptionHandler.handle(e);
			}
			System.out.println("Finding cloudmade routes " + res.size() + " " + (System.currentTimeMillis() - time) + " ms");
		}
		return res;
	}
	
	private static Double[] decodeGooglePolylinesFlow(String encodedData) {
        final List<Double> decodedValues = new ArrayList<Double>(); 
        int rawDecodedValue = 0;
        int carriage = 0;
        for (int x = 0, xx = encodedData.length(); x < xx; ++x) {
            int i = encodedData.charAt(x);
            i -= 63;
            int _5_bits = i << (32 - 5) >>> (32 - 5);
            rawDecodedValue |= _5_bits << carriage;
            carriage += 5;
            boolean isLast = (i & (1 << 5)) == 0;
            if (isLast) {
                boolean isNegative = (rawDecodedValue & 1) == 1;
                rawDecodedValue >>>= 1;
                if (isNegative) {
                	rawDecodedValue = ~rawDecodedValue;
                }
                decodedValues.add(((double)rawDecodedValue) / 1e5);
                carriage = 0;
                rawDecodedValue = 0;
            }
        }
        return decodedValues.toArray(new Double[decodedValues.size()]);
    }
	public static List<Way> route_OSRM(LatLon start, LatLon end){
		List<Way> res = new ArrayList<Way>();
		long time = System.currentTimeMillis();
		System.out.println("Route from " + start + " to " + end);
		if (start != null && end != null) {
			try {
				StringBuilder uri = new StringBuilder();
				uri.append(DataExtractionSettings.getSettings().getOsrmServerAddress());
				uri.append("/viaroute?");
				uri.append("&loc=").append(start.getLatitude()).append(",").append(start.getLongitude());
				uri.append("&loc=").append(end.getLatitude()).append(",").append(end.getLongitude());
				uri.append("&output=json");
				uri.append("&instructions=false");
				uri.append("&geomformat=cmp");

				URL url = new URL(uri.toString());
				URLConnection connection = url.openConnection();
				StringBuilder content = new StringBuilder();
				BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
				{
					String s = null;
					boolean fist = true;
					while ((s = reader.readLine()) != null) {
						if (fist) {
							fist = false;
						}
						content.append(s).append("\n");
					}
					System.out.println(content);
				}
				
				final JSONObject jsonContent = (JSONObject)new JSONTokener(content.toString()).nextValue();
				
				// Encoded as https://developers.google.com/maps/documentation/utilities/polylinealgorithm
				final String routeGeometry = jsonContent.getString("route_geometry");
				final Double[] route = decodeGooglePolylinesFlow(routeGeometry);
				double latitude = 0.0;
				double longitude = 0.0;
				Way w = new Way(-1);
				for(int routePointIdx = 0; routePointIdx < route.length / 2; routePointIdx++) {
					latitude += route[routePointIdx * 2 + 0];
					longitude += route[routePointIdx * 2 + 1];
					
					w.addNode(new net.osmand.osm.edit.Node(latitude, longitude, -1));
				}
				
				if (!w.getNodes().isEmpty()) {
					res.add(w);
				}
			} catch (IOException e) {
				ExceptionHandler.handle(e);
			} catch (JSONException e) {
				ExceptionHandler.handle(e);
			}
			System.out.println("Finding routes " + res.size() + " " + (System.currentTimeMillis() - time) + " ms");
		}
		return res;
	}
	
	public List<Way> selfRoute(LatLon start, LatLon end, List<LatLon> intermediates, List<RouteSegmentResult> previousRoute, boolean useBasemap) {
		List<Way> res = new ArrayList<Way>();
		long time = System.currentTimeMillis();
		List<File> files = new ArrayList<File>();
		for(File f :new File(DataExtractionSettings.getSettings().getBinaryFilesDir()).listFiles()){
			if(f.getName().endsWith(".obf")){
				files.add(f);
			}
		}
		String xmlPath = DataExtractionSettings.getSettings().getRoutingXmlPath();
		Builder builder;
		if(xmlPath.equals("routing.xml")){
			builder = RoutingConfiguration.getDefault() ;
		} else{
			try {
				builder = RoutingConfiguration.parseFromInputStream(new FileInputStream(xmlPath));
			} catch (IOException e) {
				throw new IllegalArgumentException("Error parsing routing.xml file",e);
			} catch (XmlPullParserException e) {
				throw new IllegalArgumentException("Error parsing routing.xml file",e);
			}
		}
		final boolean animateRoutingCalculation = DataExtractionSettings.getSettings().isAnimateRouting();
		if(animateRoutingCalculation) {
			nextTurn.setVisible(true);
			playPauseButton.setVisible(true);
			stopButton.setVisible(true);
			pause = true;
			playPauseButton.setText("Play");
		}
		stop = false;
		if(files.isEmpty()){
			JOptionPane.showMessageDialog(OsmExtractionUI.MAIN_APP.getFrame(), "Please specify obf file in settings", "Obf file not found", 
					JOptionPane.ERROR_MESSAGE);
			return null;
		}
		System.out.println("Self made route from " + start + " to " + end);
		if (start != null && end != null) {
			try {
				BinaryMapIndexReader[] rs = new BinaryMapIndexReader[files.size()];
				int it = 0;
				for (File f : files) {
					RandomAccessFile raf = new RandomAccessFile(f, "r"); //$NON-NLS-1$ //$NON-NLS-2$
					rs[it++] = new BinaryMapIndexReader(raf);
				}
				String m = DataExtractionSettings.getSettings().getRouteMode();
				String[] props = m.split("\\,");
				RoutePlannerFrontEnd router = new RoutePlannerFrontEnd(USE_OLD_ROUTING);
				RoutingConfiguration config = builder.build(props[0], /*RoutingConfiguration.DEFAULT_MEMORY_LIMIT*/ 500, props);
//				config.initialDirection = 90d / 180d * Math.PI; // EAST
//				config.initialDirection = 180d / 180d * Math.PI; // SOUTH
//				config.initialDirection = -90d / 180d * Math.PI; // WEST
//				config.initialDirection = 0 / 180d * Math.PI; // NORTH
				// config.NUMBER_OF_DESIRABLE_TILES_IN_MEMORY = 300;
				// config.ZOOM_TO_LOAD_TILES = 14;
				final RoutingContext ctx = new RoutingContext(config, 
						USE_NATIVE_ROUTING ? NativeSwingRendering.getDefaultFromSettings() :
						null
						, rs, useBasemap);
				ctx.previouslyCalculatedRoute = previousRoute;
				log.info("Use " + config.routerName + "mode for routing");
				
				
				final DataTileManager<Entity> points = new DataTileManager<Entity>(11);
				map.setPoints(points);
				ctx.setVisitor(createSegmentVisitor(animateRoutingCalculation, points));
				// Choose native or not native
				long nt = System.nanoTime();
				new Thread(){
					@Override
					public void run() {
						while(ctx.calculationProgress != null && !ctx.calculationProgress.isCancelled) {
							float p = ctx.calculationProgress.distanceFromBegin + ctx.calculationProgress.distanceFromEnd;
							float all = ctx.calculationProgress.totalEstimatedDistance;
//							while (p > all * 0.9) {
//								all *= 1.2;
//							}
							if(all > 0 ) {
								int  t = (int) (p*p/(all*all)*100f);  
//								int  t = (int) (p/all*100f);
								System.out.println("Progress " + t + " % " + 
								ctx.calculationProgress.distanceFromBegin + " " + ctx.calculationProgress.distanceFromEnd+" " + all);
							}
							try {
								sleep(100);
							} catch (InterruptedException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						}
					};
				}.start();
				try {
					List<RouteSegmentResult> searchRoute = router.searchRoute(ctx, start, end, intermediates, false);
					if (searchRoute == null) {
						String reason = "unknown";
						if (ctx.calculationProgress.segmentNotFound >= 0) {
							if (ctx.calculationProgress.segmentNotFound == 0) {
								reason = " start point is too far from road";
							} else {
								reason = " target point " + ctx.calculationProgress.segmentNotFound + " is too far from road";
							}
						} else if (ctx.calculationProgress.directSegmentQueueSize == 0) {
							reason = " route can not be found from start point (" + ctx.calculationProgress.distanceFromBegin / 1000f
									+ " km)";
						} else if (ctx.calculationProgress.reverseSegmentQueueSize == 0) {
							reason = " route can not be found from end point (" + ctx.calculationProgress.distanceFromEnd / 1000f + " km)";
						}
						throw new RuntimeException("Route not found : " + reason);
					}

					System.out.println("External native time " + (System.nanoTime() - nt) / 1e9f);
					if (animateRoutingCalculation) {
						playPauseButton.setVisible(false);
						nextTurn.setText("FINISH");
						waitNextPress();
						nextTurn.setText(">>");
					}
					calculateResult(res, searchRoute);
				} finally {
					ctx.calculationProgress.isCancelled = true;
				}
			} catch (Exception e) {
				ExceptionHandler.handle(e);
			} finally {
				playPauseButton.setVisible(false);
				nextTurn.setVisible(false);
				stopButton.setVisible(false);
				if(map.getPoints() != null) {
					map.getPoints().clear();
				}
			}
			System.out.println("Finding self routes " + res.size() + " " + (System.currentTimeMillis() - time) + " ms");
		}
		return res;
	}

	private void calculateResult(List<Way> res, List<RouteSegmentResult> searchRoute) {
		net.osmand.osm.edit.Node prevWayNode = null;
		for (RouteSegmentResult s : searchRoute) {
			// double dist = MapUtils.getDistance(s.startPoint, s.endPoint);
			Way way = new Way(-1);
//					String name = String.format("time %.2f ", s.getSegmentTime());
			String name = s.getDescription();
			if(s.getTurnType() != null) {
				name += " (TA " + s.getTurnType().getTurnAngle() + ") ";
			}
//					String name = String.format("beg %.2f end %.2f ", s.getBearingBegin(), s.getBearingEnd());
			way.putTag(OSMTagKey.NAME.getValue(),name);
			boolean plus = s.getStartPointIndex() < s.getEndPointIndex();
			int i = s.getStartPointIndex();
			while (true) {
				LatLon l = s.getPoint(i);
				net.osmand.osm.edit.Node n = new net.osmand.osm.edit.Node(l.getLatitude(), l.getLongitude(), -1);
				if (prevWayNode != null) {
					if (OsmMapUtils.getDistance(prevWayNode, n) > 0) {
						System.out.println("Warning not connected road " + " " + s.getObject().id + " dist "
								+ OsmMapUtils.getDistance(prevWayNode, n));
					}
					prevWayNode = null;
				}
				way.addNode(n);
				if (i == s.getEndPointIndex()) {
					break;
				}
				if (plus) {
					i++;
				} else {
					i--;
				}
			}
			if (way.getNodes().size() > 0) {
				prevWayNode = way.getNodes().get(way.getNodes().size() - 1);
			}
			res.add(way);
		
		}
	}

	private RouteSegmentVisitor createSegmentVisitor(final boolean animateRoutingCalculation, final DataTileManager<Entity> points) {
		return new RouteSegmentVisitor() {
			
			private List<RouteSegment> cache = new ArrayList<RouteSegment>();
			private List<RouteSegment> pollCache = new ArrayList<RouteSegment>();
			private List<Integer> cacheInt = new ArrayList<Integer>();
			
			@Override
			public void visitSegment(RouteSegment s, int  endSegment, boolean poll) {
				if(stop) {
					throw new RuntimeException("Interrupted");
				}
				if (!animateRoutingCalculation) {
					return;
				}
				if (!poll && pause) {
					pollCache.add(s);
					return;
				}

				cache.add(s);
				cacheInt.add(endSegment);
				if (cache.size() < steps) {
					return;
				}
				if(pause) {
					registerObjects(points, poll, pollCache, null);
					pollCache.clear();
				}
				registerObjects(points, !poll, cache, cacheInt);
				cache.clear();
				cacheInt.clear();
				redraw();
				if (pause) {
					waitNextPress();
				}
			}

			private void registerObjects(final DataTileManager<Entity> points, boolean white, List<RouteSegment> registerCache,
					List<Integer> cacheInt) {
				for (int l = 0; l < registerCache.size(); l++) {
					RouteSegment segment = registerCache.get(l);
					Way way = new Way(-1);
					way.putTag(OSMTagKey.NAME.getValue(), segment.getTestName());
					if (white) {
						way.putTag("color", "white");
					}
					int from = cacheInt != null ? segment.getSegmentStart() : segment.getSegmentStart() - 2;
					int to = cacheInt != null ? cacheInt.get(l) : segment.getSegmentStart() + 2;
					if(from > to) {
						int x = from;
						from = to;
						to = x;
					}
					for (int i = from; i <= to; i++) {
						if (i >= 0 && i < segment.getRoad().getPointsLength()) {
							net.osmand.osm.edit.Node n = createNode(segment, i);
							way.addNode(n);
						}
					}
					LatLon n = way.getLatLon();
					points.registerObject(n.getLatitude(), n.getLongitude(), way);
				}
			}

		};
	}
	
	private net.osmand.osm.edit.Node createNode(RouteSegment segment, int i) {
		net.osmand.osm.edit.Node n = new net.osmand.osm.edit.Node(MapUtils.get31LatitudeY(segment.getRoad().getPoint31YTile(i)),
				MapUtils.get31LongitudeX(segment.getRoad().getPoint31XTile(i)), -1);
		return n;
	}
	
	private void redraw() {
		try {
			SwingUtilities.invokeAndWait(new Runnable() {
				@Override
				public void run() {
					map.prepareImage();
				}
			});
		} catch (InterruptedException e1) {
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		}
	}
	
	private void waitNextPress() {
		nextTurn.setVisible(true);
		while (!nextAvailable) {
			try {
				synchronized (MapRouterLayer.this) {
					MapRouterLayer.this.wait();
				}
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}
		nextTurn.setVisible(false);
		nextAvailable = false;
	}
	
	@Override
	public void prepareToDraw() {
	}

	
	@Override
	public void paintLayer(Graphics2D g) {
		g.setColor(Color.green);
		if(startRoute != null){
			int x = map.getMapXForPoint(startRoute.getLongitude());
			int y = map.getMapYForPoint(startRoute.getLatitude());
			g.drawOval(x, y, 12, 12);
			g.fillOval(x, y, 12, 12);
		}
		g.setColor(Color.red);
		if(endRoute != null){
			int x = map.getMapXForPoint(endRoute.getLongitude());
			int y = map.getMapYForPoint(endRoute.getLatitude());
			g.drawOval(x, y, 12, 12);
			g.fillOval(x, y, 12, 12);
		}
		g.setColor(Color.yellow);
		for(LatLon i : intermediates) {
			int x = map.getMapXForPoint(i.getLongitude());
			int y = map.getMapYForPoint(i.getLatitude());
			g.drawOval(x, y, 12, 12);
			g.fillOval(x, y, 12, 12);
		}
	}

}
