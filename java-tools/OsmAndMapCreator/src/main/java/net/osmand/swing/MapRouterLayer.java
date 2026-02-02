package net.osmand.swing;



import static net.osmand.router.RoutingConfiguration.DEFAULT_NATIVE_MEMORY_LIMIT;

import java.awt.Color;
import java.awt.Component;
import java.awt.Graphics2D;
import java.awt.Point;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.image.BufferedImage;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.Writer;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.net.URLConnection;
import java.sql.Connection;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TimeZone;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import javax.swing.AbstractAction;
import javax.swing.Action;
import javax.swing.JButton;
import javax.swing.JFileChooser;
import javax.swing.JMenu;
import javax.swing.JOptionPane;
import javax.swing.JPopupMenu;
import javax.swing.SwingUtilities;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.imageio.ImageIO;

import net.osmand.router.*;
import net.osmand.shared.gpx.GpxFile;
import net.osmand.shared.gpx.GpxTrackAnalysis;
import net.osmand.shared.gpx.GpxUtilities;
import net.osmand.shared.gpx.primitives.Track;
import net.osmand.shared.gpx.primitives.TrkSegment;
import net.osmand.shared.gpx.primitives.WptPt;
import net.osmand.shared.io.KFile;
import net.osmand.shared.routing.RouteColorize.ColorizationType;
import org.apache.commons.logging.Log;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.stream.JsonReader;

import gnu.trove.map.hash.TLongObjectHashMap;
import net.osmand.Location;
import net.osmand.LocationsHolder;
import net.osmand.PlatformUtil;
import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.binary.BinaryMapRouteReaderAdapter.RouteTypeRule;
import net.osmand.data.DataTileManager;
import net.osmand.data.LatLon;
import net.osmand.data.QuadRect;
import net.osmand.data.QuadTree;
import net.osmand.obf.preparation.DBDialect;
import net.osmand.obf.preparation.IndexHeightData;
import net.osmand.osm.edit.Entity;
import net.osmand.osm.edit.OSMSettings.OSMTagKey;
import net.osmand.osm.edit.Way;
import net.osmand.router.BinaryRoutePlanner.RouteSegment;
import net.osmand.router.BinaryRoutePlanner.RouteSegmentVisitor;
import net.osmand.router.HHRouteDataStructure.HHNetworkRouteRes;
import net.osmand.router.HHRouteDataStructure.HHNetworkSegmentRes;
import net.osmand.router.HHRouteDataStructure.HHRoutingConfig;
import net.osmand.router.HHRouteDataStructure.HHRoutingContext;
import net.osmand.router.HHRouteDataStructure.NetworkDBPoint;
import net.osmand.router.RoutePlannerFrontEnd.GpxPoint;
import net.osmand.router.RoutePlannerFrontEnd.RouteCalculationMode;
import net.osmand.router.RouteResultPreparation.RouteCalcResult;
import net.osmand.router.RoutingConfiguration.RoutingMemoryLimits;
import net.osmand.router.tester.RandomRouteEntry;
import net.osmand.router.tester.RandomRouteGenerator;
import net.osmand.router.tester.RandomRouteTester;
import net.osmand.util.MapUtils;


public class MapRouterLayer implements MapPanelLayer {

	private final static Log log = PlatformUtil.getLog(MapRouterLayer.class);

	private static final double MIN_STRAIGHT_DIST = 50000;
	private static final double ANGLE_TO_DECLINE = 15;
	private static final String RANDOM_ROUTES_PROFILE = "car";
	private static final String RANDOM_ROUTES_FILE_NAME = "random_routes.json";
	private static final String ROUTE_DIR_PREFIX = "route_";
	private static final int ROUTE_RENDER_DELAY_MS = 3000;
	private static final int RANDOM_ROUTE_MIN_KM = 2;
	private static final int RANDOM_ROUTE_MAX_KM = 3;
	private static final int RANDOM_ROUTE_CENTER_RADIUS_KM = 5;
	private static final int ROUTE_SCREEN_ZOOM = 16;

	private static boolean USE_CACHE_CONTEXT = false;
	private HHRoutingContext<NetworkDBPoint> cacheHHCtx;
	private RoutingContext cacheRctx;
	private String cacheRouteParams;

	private MapPanel map;
	private LatLon startRoute;
	private LatLon endRoute;
	private List<LatLon> intermediates = new ArrayList<LatLon>();
	private boolean nextAvailable = true;
	private boolean pause = true;
	private boolean stop = false;
	private int steps = 1;
	private JButton nextTurn;
	private JButton playPauseButton;
	private JButton stopButton;
	private GpxFile selectedGPXFile;
	private QuadTree<net.osmand.osm.edit.Node> directionPointsFile;

	private static class RouteSeed {
		private final int index;
		private final LatLon start;
		private final LatLon end;

		private RouteSeed(int index, LatLon start, LatLon end) {
			this.index = index;
			this.start = start;
			this.end = end;
		}
	}


	private List<RouteSegmentResult> previousRoute;
	public ActionListener setStartActionListener = new ActionListener(){
		@Override
		public void actionPerformed(ActionEvent e) {
			setStart(new LatLon(map.getLatitude(), map.getLongitude()));
		}
	};

	public ActionListener setEndActionListener = new ActionListener() {
		@Override
		public void actionPerformed(ActionEvent e) {
			setEnd(new LatLon(map.getLatitude(), map.getLongitude()));
		}
	};
	private boolean gpx = false;



	private File getHHFile(String profile) {
		return new File(DataExtractionSettings.getSettings().getBinaryFilesDir(), "Maps_" + profile + ".hhdb");
	}


	@Override
	public void destroyLayer() {

	}

	public void setStart(LatLon start) {
		startRoute = start;
		DataExtractionSettings.getSettings().saveStartLocation(start.getLatitude(), start.getLongitude());
		map.repaint();
	}

	public void setEnd(LatLon end) {
		endRoute = end;
		DataExtractionSettings.getSettings().saveEndLocation(end.getLatitude(), end.getLongitude());
		map.repaint();
	}

	@Override
	public void initLayer(MapPanel map) {
		this.map = map;
		startRoute = DataExtractionSettings.getSettings().getStartLocation();
		endRoute = DataExtractionSettings.getSettings().getEndLocation();

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

	private LatLon getPointFromMenu() {
		Point popupMenuPoint = map.getPopupMenuPoint();
		double fy = (popupMenuPoint.y - map.getCenterPointY()) / map.getTileSize();
		double fx = (popupMenuPoint.x - map.getCenterPointX()) / map.getTileSize();
		double latitude = MapUtils.checkLatitude(
				MapUtils.getLatitudeFromTile(map.getZoom(), map.getYTile() + fy));
		double longitude = MapUtils.checkLongitude(
				MapUtils.getLongitudeFromTile(map.getZoom(), map.getXTile() + fx));
		LatLon l = new LatLon(latitude, longitude);
		return l;
	}

	public void fillPopupMenuWithActions(JPopupMenu menu ) {
		Action start = new AbstractAction("Mark start point") {
			private static final long serialVersionUID = 507156107455281238L;

			@Override
			public void actionPerformed(ActionEvent e) {
				setStart(getPointFromMenu());
			}


		};
		menu.add(start);
		Action end= new AbstractAction("Mark end point") {
			private static final long serialVersionUID = 4446789424902471319L;

			@Override
			public void actionPerformed(ActionEvent e) {
				setEnd(getPointFromMenu());
			}
		};
		menu.add(end);
		final JMenu points = new JMenu("Transit Points"); //$NON-NLS-1$
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
		points.add(swapLocations);
		Action addIntermediate = new AbstractAction("Add transit point") {

			private static final long serialVersionUID = 1021949691943312782L;

			@Override
			public void actionPerformed(ActionEvent e) {
				intermediates.add(getPointFromMenu());
				map.repaint();
			}
		};
		points.add(addIntermediate);

		Action remove = new AbstractAction("Remove transit point") {

			private static final long serialVersionUID = 1L;

			@Override
			public void actionPerformed(ActionEvent e) {
				if(intermediates.size() > 0){
					intermediates.remove(0);
				}
				map.repaint();
			}
		};
		points.add(remove);
		menu.add(points);
		final JMenu directions = new JMenu("Directions"); //$NON-NLS-1$
		menu.add(directions);
		Action complexRoute = new AbstractAction("Build route (OsmAnd 2-phase|COMPLEX)") {
			private static final long serialVersionUID = 8049785829806139142L;

			@Override
			public void actionPerformed(ActionEvent e) {
				previousRoute = null;
				calcRoute(RouteCalculationMode.COMPLEX, false);
			}
		};
		directions.add(complexRoute);


		Action selfRoute = new AbstractAction("Build route (OsmAnd slow+precise|NORMAL)") {
			private static final long serialVersionUID = 507156107455281238L;

			@Override
			public void actionPerformed(ActionEvent e) {
				previousRoute = null;
				calcRoute(RouteCalculationMode.NORMAL, false);
			}
		};
		directions.add(selfRoute);

		Action selfBaseRoute = new AbstractAction("Build route (OsmAnd fast+long|BASE)") {
			private static final long serialVersionUID = 8049785829806139142L;

			@Override
			public void actionPerformed(ActionEvent e) {
				previousRoute = null;
				calcRoute(RouteCalculationMode.BASE, false);
			}
		};
		directions.add(selfBaseRoute);

		Action selfHHRoute = new AbstractAction("Build HH route (only)") {
			private static final long serialVersionUID = 8049785829806139142L;

			@Override
			public void actionPerformed(ActionEvent e) {
				previousRoute = null;
				calcRoute(RouteCalculationMode.NORMAL, true);
			}
		};
		directions.add(selfHHRoute);

		if (selectedGPXFile != null) {
			Action approximateByRouting = new AbstractAction("Approximate GPX (by routing)") {
				private static final long serialVersionUID = 507156107455281238L;

				@Override
				public void actionPerformed(ActionEvent e) {
					calcRouteGpx(selectedGPXFileToPolyline(), false);
				}
			};
			directions.add(approximateByRouting);

			Action approximateByGeometry = new AbstractAction("Approximate GPX (by geometry)") {
				private static final long serialVersionUID = 507156107455281238L;

				@Override
				public void actionPerformed(ActionEvent e) {
					calcRouteGpx(selectedGPXFileToPolyline(), true);
				}
			};
			directions.add(approximateByGeometry);
		}

		if (previousRoute != null) {
			Action recalculate = new AbstractAction("Rebuild route (OsmAnd)") {
				private static final long serialVersionUID = 507156107455281238L;

				@Override
				public void actionPerformed(ActionEvent e) {
					calcRoute(RouteCalculationMode.NORMAL, false);
				}
			};
			directions.add(recalculate);
		}

		Action straightRoute = new AbstractAction("Build straight route ") {
			private static final long serialVersionUID = 8049785829806139142L;

			@Override
			public void actionPerformed(ActionEvent e) {
				previousRoute = null;
				calcStraightRoute(getPointFromMenu());
				map.fillPopupActions();
			}
		};
		directions.add(straightRoute);

		Action generateRandomRoutes = new AbstractAction("Generate random routes") {
			private static final long serialVersionUID = 347156107455281238L;

			@Override
			public void actionPerformed(ActionEvent e) {
				generateRandomRoutes();
			}
		};
		directions.add(generateRandomRoutes);

		Action loadRandomRoutes = new AbstractAction("Load JSON and regenerate routes") {
			private static final long serialVersionUID = 347156107455281239L;

			@Override
			public void actionPerformed(ActionEvent e) {
				loadRandomRoutes();
			}
		};
		directions.add(loadRandomRoutes);

		if (directionPointsFile == null) {
			Action loadGeoJSON = new AbstractAction("Load Direction Points (GeoJSON)...") {
				private static final long serialVersionUID = 507356107455281238L;

				@Override
				public void actionPerformed(ActionEvent e) {
					// new Thread() {
					// @Override
					// public void run() {
					JFileChooser fileChooser = new JFileChooser(
							DataExtractionSettings.getSettings().getDefaultWorkingDir());
					if (fileChooser.showOpenDialog(map) == JFileChooser.APPROVE_OPTION) {
						File file = fileChooser.getSelectedFile();
						Gson gson = new Gson();
						directionPointsFile = new QuadTree<net.osmand.osm.edit.Node>(new QuadRect(0, 0, Integer.MAX_VALUE, Integer.MAX_VALUE), 15, 0.5f);
						try {
							com.google.gson.JsonObject mp = gson.fromJson(new JsonReader(new FileReader(file)), com.google.gson.JsonObject.class);
							JsonElement features = mp.get("features");
							if(features == null) {
								return;
							}
							Iterator<JsonElement> jsonE = features.getAsJsonArray().iterator();
							while(jsonE.hasNext()) {
								JsonObject obj = jsonE.next().getAsJsonObject();
								JsonArray ar = obj.get("geometry").getAsJsonObject().get("coordinates").getAsJsonArray();
								double lon = ar.get(0).getAsDouble();
								double lat = ar.get(1).getAsDouble();
								JsonObject props = obj.get("properties").getAsJsonObject();
								net.osmand.osm.edit.Node pt = new net.osmand.osm.edit.Node(lat, lon, -1);
								int x = MapUtils.get31TileNumberX(lon);
								int y = MapUtils.get31TileNumberY(lat);
								Iterator<Entry<String, JsonElement>> keyIt = props.entrySet().iterator();
								while (keyIt.hasNext()) {
									Entry<String, JsonElement> el = keyIt.next();
									pt.putTag(el.getKey(), el.getValue().getAsString());
								}
								directionPointsFile.insert(pt, new QuadRect(x, y, x, y));
							}
						} catch (Exception e1) {
							log.info("Error loading directions point (geojson): " + e1.getMessage(), e1);
						}
						displayGpxFiles();
						map.fillPopupActions();
					}
					// }
					// }.start();
				}

			};
			directions.add(loadGeoJSON);
		} else {
			Action loadGeoJSON = new AbstractAction("Unload Direction Points") {
				private static final long serialVersionUID = 507356107455281238L;

				@Override
				public void actionPerformed(ActionEvent e) {
					directionPointsFile = null;
					displayGpxFiles();
					map.fillPopupActions();
				}
			};
			directions.add(loadGeoJSON);

		}


		if (previousRoute != null) {
			Action saveGPX = new AbstractAction("Save GPX...") {
				private static final long serialVersionUID = 5757334824774850326L;

				@Override
				public void actionPerformed(ActionEvent e) {
					// new Thread() {
					// @Override
					// public void run() {
					List<Entity> es = new ArrayList<>();
					calculateResult(es, previousRoute);
					List<Location> locations = new ArrayList<>();
					for (Entity ent : es) {
						if (ent instanceof Way) {
							for (net.osmand.osm.edit.Node node : ((Way) ent).getNodes()) {
								locations.add(new Location("", node.getLatitude(), node.getLongitude()));
							}
						}
					}
					String name = new SimpleDateFormat("yyyy-MM-dd_HH-mm_EEE", Locale.US).format(new Date());
					RouteExporter exporter = new RouteExporter(name, previousRoute, locations, null, null);
					GpxFile gpxFile = exporter.exportRoute();
					JFileChooser fileChooser = new JFileChooser(
							DataExtractionSettings.getSettings().getDefaultWorkingDir());
					if (fileChooser.showSaveDialog(map) == JFileChooser.APPROVE_OPTION) {
						KFile file = new KFile(fileChooser.getSelectedFile().getAbsolutePath());
						GpxUtilities.INSTANCE.writeGpxFile(file, gpxFile);
					}
					// }
					// }.start();
				}
			};
			directions.add(saveGPX);
		}


		if (selectedGPXFile == null) {
			Action loadGPXFile = new AbstractAction("Load GPX file...") {
				private static final long serialVersionUID = 507156107455281238L;

				@Override
				public void actionPerformed(ActionEvent e) {
					// new Thread() {
					// @Override
					// public void run() {
					JFileChooser fileChooser = new JFileChooser(
							DataExtractionSettings.getSettings().getLastUsedDir());
					if (fileChooser.showOpenDialog(map) == JFileChooser.APPROVE_OPTION) {
						File file = fileChooser.getSelectedFile();
						KFile kFile = new KFile(file.getAbsolutePath());
//						GpxUtilities gpxUtilities = GpxUtilities.INSTANCE;
//						GpxFile gfile = gpxUtilities.loadGpxFile(new KFile(file.getAbsolutePath()));
//						System.out.println(gfile.getTracksCount() + " " + gfile.getAllPoints().size());
						DataExtractionSettings.getSettings().setLastUsedDir(file.getParent());
						selectedGPXFile = GpxUtilities.INSTANCE.loadGpxFile(kFile);
						displayGpxFiles();
						map.fillPopupActions();
					}
					// }
					// }.start();
				}

			};
			menu.add(loadGPXFile);
		} else {
			AbstractAction unselectGPXFile = new AbstractAction("Unselect GPX file") {
				private static final long serialVersionUID = 507156107455281238L;

				@Override
				public void actionPerformed(ActionEvent e) {
					DataTileManager<Entity> points = new DataTileManager<Entity>(11);
					map.setPoints(points);
					selectedGPXFile = null;
					displayGpxFiles();
					map.setColorizationType(selectedGPXFile, ColorizationType.NONE, true);
					map.fillPopupActions();
				}
			};
			menu.add(unselectGPXFile);

			AbstractAction calcAltitude = new AbstractAction("Recalculate altitude ") {
				private static final long serialVersionUID = 507156107454181238L;

				@Override
				public void actionPerformed(ActionEvent e) {
					File[] missingFile = new File[1];
					displayTrackInfo(selectedGPXFile, "Unprocessed track info");
					GpxFile res = calculateAltitude(selectedGPXFile, missingFile);
					if (res == null || missingFile[0] != null) {
						String msg = missingFile[0] != null ? "Missing in 'srtm' folder: " + missingFile[0].getName()
								: ("Missing 'srtm' folder: " + DataExtractionSettings.getSettings().getBinaryFilesDir() + "/srtm");
						JOptionPane.showMessageDialog(OsmExtractionUI.MAIN_APP.getFrame(), msg, "Missing srtm data",
								JOptionPane.INFORMATION_MESSAGE);
					} else {
						selectedGPXFile = res;
						displayTrackInfo(selectedGPXFile, "Processed track info");
						displayGpxFiles();
						map.fillPopupActions();
					}
				}
			};
			menu.add(calcAltitude);

			final JMenu colorize = new JMenu("Colorize GPX file");
			Action altitude = new AbstractAction("Altitude") {
				private static final long serialVersionUID = 507156107355281238L;

				@Override
				public void actionPerformed(ActionEvent e) {
					int result = showOptionColorSchemeDialog(colorize);
					map.setColorizationType(selectedGPXFile, ColorizationType.ELEVATION, result == JOptionPane.YES_OPTION);
					map.fillPopupActions();
				}
			};
			colorize.add(altitude);
			Action speed = new AbstractAction("Speed") {
				private static final long serialVersionUID = 507156107455281238L;

				@Override
				public void actionPerformed(ActionEvent e) {
					int result = showOptionColorSchemeDialog(colorize);
					map.setColorizationType(selectedGPXFile, ColorizationType.SPEED, result == JOptionPane.YES_OPTION);
					map.fillPopupActions();
				}
			};
			colorize.add(speed);
			Action slope = new AbstractAction("Slope") {
				private static final long serialVersionUID = 50715610765281238L;

				@Override
				public void actionPerformed(ActionEvent e) {
					int result = showOptionColorSchemeDialog(colorize);
					map.setColorizationType(selectedGPXFile, ColorizationType.SLOPE, result == JOptionPane.YES_OPTION);
					map.fillPopupActions();
				}
			};
			colorize.add(slope);
			menu.add(colorize);

		}

	}


	protected void displayTrackInfo(GpxFile gpxFile, String header) {
		GpxTrackAnalysis analysis = selectedGPXFile.getAnalysis(gpxFile.getModifiedTime());
		StringBuilder msg = new StringBuilder();

		msg.append(String.format("Track: distance %.1f, distance no gaps %.1f, tracks %d, points %d\n", analysis.getTotalDistance(),
				analysis.getTotalDistanceWithoutGaps(), analysis.getTotalTracks(), analysis.getWptPoints()));

		if (analysis.hasElevationData()) {
			msg.append(String.format("Ele: min - %.1f, max - %.1f, avg - %.1f, uphill - %.1f, downhill - %.1f\n",
					analysis.getMinElevation(), analysis.getMaxElevation(), analysis.getAvgElevation(), analysis.getDiffElevationUp(),
					analysis.getDiffElevationDown()));
		}
		if (analysis.hasSpeedData()) {
			msg.append(String.format("Speed: min - %.1f, max - %.1f, avg - %.1f, dist+speed - %.1f, dist+speed no gaps - %.1f\n",
					analysis.getMinSpeed(), analysis.getMaxSpeed(), analysis.getAvgSpeed(),
					analysis.getTotalDistanceMoving(), analysis.getTotalDistanceMovingWithoutGaps()));
		}
		if (analysis.getStartTime() != analysis.getEndTime()) {
			msg.append(String.format("Time: start - %s, end - %s, span - %.1f min, span no gaps - %.1f min\n",
					new Date(analysis.getStartTime()), new Date(analysis.getEndTime()), analysis.getTimeSpan() / 60000.0,
					analysis.getTimeSpanWithoutGaps() / 60000.0));
		}
		log.info(header + " " + msg);
		JOptionPane.showMessageDialog(OsmExtractionUI.MAIN_APP.getFrame(), msg, header,
				JOptionPane.INFORMATION_MESSAGE);
	}


	private int showOptionColorSchemeDialog(JMenu frame) {
		String[] options = new String[2];
		options[0] = "Grey";
		options[1] = "Red, yellow, green";
		return JOptionPane.showOptionDialog(frame, "What color scheme to use?", "Color scheme", 0, JOptionPane.INFORMATION_MESSAGE, null, options, null);
	}


	protected GpxFile calculateAltitude(GpxFile gpxFile, File[] missingFile) {
		File srtmFolder = new File(DataExtractionSettings.getSettings().getBinaryFilesDir(), "srtm");
		if (!srtmFolder.exists()) {
			return null;
		}
		IndexHeightData hd = new IndexHeightData();
		hd.setSrtmData(srtmFolder.getAbsolutePath(), srtmFolder);
		for (Track tr : gpxFile.getTracks()) {
			for (TrkSegment s : tr.getSegments()) {
				for (int i = 0; i < s.getPoints().size(); i++) {
					WptPt wpt = s.getPoints().get(i);
					double h = hd.getPointHeight(wpt.getLat(), wpt.getLon(), missingFile);
					if (h != IndexHeightData.INEXISTENT_HEIGHT) {
						wpt.setEle(h);
					} else if (i == 0) {
						return null;
					}

				}
			}
		}
		return gpxFile;
	}

	private void displayGpxFiles() {
		DataTileManager<Entity> points = new DataTileManager<Entity>(9);
		if (selectedGPXFile != null) {
			for (Track t : selectedGPXFile.getTracks()) {
				for (TrkSegment ts : t.getSegments()) {
					Way w = new Way(-1);
					int id = 0;
					for (WptPt p : ts.getPoints()) {
						net.osmand.osm.edit.Node n = new net.osmand.osm.edit.Node(p.getLat(), p.getLon(), -1);
						w.addNode(n);
						n.putTag(OSMTagKey.NAME.getValue(), String.valueOf(id));
						n.putTag("gpx", "yes");
						n.putTag("colour", "blue");
						points.registerObject(n.getLatitude(), n.getLongitude(), n);
						id++;
					}
					w.putTag("gpx", "yes");
					w.putTag("colour", "green");
					LatLon n = w.getLatLon();
					points.registerObject(n.getLatitude(), n.getLongitude(), w);
				}
			}
		}
		if (directionPointsFile != null) {
			List<net.osmand.osm.edit.Node> pnts =
					directionPointsFile.queryInBox(new QuadRect(0, 0, Integer.MAX_VALUE, Integer.MAX_VALUE), new ArrayList<net.osmand.osm.edit.Node>());
			for (net.osmand.osm.edit.Node n : pnts) {
				points.registerObject(n.getLatitude(), n.getLongitude(), n);
			}
		}
		// load from file
		map.setPoints(points);
	}


	private void calcStraightRoute(LatLon currentLatLon) {
		System.out.println("Distance: " + MapUtils.getDistance(startRoute, endRoute));
		List<Way> ways = new ArrayList<>();
		{
			Way w = new Way(1);
			w.addNode(new net.osmand.osm.edit.Node(startRoute.getLatitude(), startRoute.getLongitude(), -1));
			addStraightLine(w, startRoute, endRoute);
			Location c = new Location("");
			c.setLatitude(currentLatLon.getLatitude());
			c.setLongitude(currentLatLon.getLongitude());
			double minDist = w.getFirstNode().getLocation().distanceTo(c);
			int minInd = 0;
			LatLon prev = null;
			// search closest point and save prev
			for (int ind = 1; ind < w.getNodes().size(); ind++) {
				float dt = w.getNodes().get(ind).getLocation().distanceTo(c);
				if(dt < minDist) {
					minDist = dt;
					minInd = ind;
					prev = w.getNodes().get(ind - 1).getLatLon();
				}
			}
			while(minInd > 0) {
				w.removeNodeByIndex(0);
				minInd --;
			}
			// proceed to the next point with min acceptable bearing
			for (; w.getNodes().size() > 1; ) {
				net.osmand.osm.edit.Node s = w.getFirstNode();
				net.osmand.osm.edit.Node n = w.getNodes().get(1);
				float bearingTo = c.bearingTo(s.getLocation());
				float bearingTo2 = c.bearingTo(n.getLocation());
				if(Math.abs(MapUtils.degreesDiff(bearingTo2, bearingTo)) > ANGLE_TO_DECLINE) {
					w.removeNodeByIndex(0);
					prev = s.getLatLon();
				} else {
					break;
				}
			}

			if(prev != null) {
				LatLon f = w.getFirstNode().getLatLon();
				float bearingTo = c.bearingTo(w.getFirstNode().getLocation());
				LatLon mp = MapUtils.calculateMidPoint(prev, f);
				while (MapUtils.getDistance(mp, f) > 100) {
					Location l = new Location("");
					l.setLatitude(mp.getLatitude());
					l.setLongitude(mp.getLongitude());
					float bearingMid = c.bearingTo(l);
					if (Math.abs(MapUtils.degreesDiff(bearingMid, bearingTo)) < ANGLE_TO_DECLINE) {
						prev = mp;
						w.addNode(new net.osmand.osm.edit.Node(mp.getLatitude(), mp.getLongitude(), -1l), 0);
						break;
					}
					mp = MapUtils.calculateMidPoint(mp, f);
				}
			}

			Way wr = new Way(2);
			wr.addNode(new net.osmand.osm.edit.Node(currentLatLon.getLatitude(),currentLatLon.getLongitude(), -1l), 0);
			addStraightLine(wr, currentLatLon, w.getNodes().get(0).getLatLon());
			for(int i = 1; i < w.getNodes().size(); i++) {
				wr.addNode(w.getNodes().get(i));
			}
			ways.add(wr);
		}
		DataTileManager<Entity> points = new DataTileManager<Entity>(11);
		for (Way w : ways) {
			LatLon n = w.getLatLon();
			points.registerObject(n.getLatitude(), n.getLongitude(), w);
		}
		map.setPoints(points);
	}

	private void addStraightLine(Way w, LatLon s1, LatLon s2) {
		if(MapUtils.getDistance(s1, s2) > MIN_STRAIGHT_DIST) {
			LatLon m = MapUtils.calculateMidPoint(s1, s2);
			addStraightLine(w, s1, m);
			addStraightLine(w, m, s2);
		} else {
			w.addNode(new net.osmand.osm.edit.Node(s2.getLatitude(), s2.getLongitude(), -1));
		}
	}

	private LatLon toLatLon(WptPt wptPt) {
		return new LatLon(wptPt.getLat(), wptPt.getLon());
	}

	private void calcRouteGpx(List<LatLon> polyline, boolean useGeometryBased) {
		new Thread() {
			@Override
			public void run() {
				RoutePlannerFrontEnd frontEnd = new RoutePlannerFrontEnd();
				frontEnd.setUseGeometryBasedApproximation(useGeometryBased);
				List<Entity> entities = selfRoute(startRoute, endRoute, polyline, true, null,
						frontEnd, RouteCalculationMode.NORMAL);
				if (entities != null) {
					DataTileManager<Entity> points = new DataTileManager<Entity>(11);
					for (Entity w : entities) {
						LatLon n = w.getLatLon();
						points.registerObject(n.getLatitude(), n.getLongitude(), w);
					}
					final List<RouteSegmentResult> routeSegmentsToExport = previousRoute;
					SwingUtilities.invokeLater(new Runnable() {
						@Override
						public void run() {
							map.setPoints(points);
							map.fillPopupActions();
							exportScreenshotAndRouteJson(routeSegmentsToExport);
						}
					});
				}

				if (selectedGPXFile != null) {
					JOptionPane.showMessageDialog(OsmExtractionUI.MAIN_APP.getFrame(), "Check validity of GPX File",
							"GPX route correction", JOptionPane.INFORMATION_MESSAGE);
				}
			}
		}.start();
	}


	private void calcRoute(final RouteCalculationMode m, boolean hh) {
		new Thread() {


			@Override
			public void run() {
				map.setPoints(new DataTileManager<Entity>(11));
				Collection<Entity> res;
				RoutePlannerFrontEnd router = new RoutePlannerFrontEnd();
				if (hh) {
//					res = hhRoute(startRoute, endRoute); // to test DB
					if (DataExtractionSettings.getSettings().useNativeRouting()) {
						router.setHHRouteCpp(true);
					}
					HHRoutingConfig hhConfig = HHRoutePlanner.prepareDefaultRoutingConfig(null).cacheContext(cacheHHCtx);
					router.setUseOnlyHHRouting(true).setHHRoutingConfig(hhConfig);
					res = selfRoute(startRoute, endRoute, intermediates, false, previousRoute, router,  m);
					if (USE_CACHE_CONTEXT) {
						cacheHHCtx = hhConfig.cacheCtx;
					}
				} else {
					router.setUseOnlyHHRouting(false).setHHRoutingConfig(null);
					res = selfRoute(startRoute, endRoute, intermediates, false, previousRoute, router, m);
				}
				if (res != null) {
					DataTileManager<Entity> points = new DataTileManager<Entity>(11);
					for (Entity w : res) {
						LatLon n = w.getLatLon();
						points.registerObject(n.getLatitude(), n.getLongitude(), w);
					}
					final List<RouteSegmentResult> routeSegmentsToExport = previousRoute;
					SwingUtilities.invokeLater(new Runnable() {
						@Override
						public void run() {
							map.setPoints(points);
							map.fillPopupActions();
							exportScreenshotAndRouteJson(routeSegmentsToExport);
						}
					});
				}
			}

		}.start();
	}

	private void exportScreenshotAndRouteJson(List<RouteSegmentResult> routeSegments) {
		if (routeSegments == null || routeSegments.isEmpty()) {
			return;
		}
		File[] savedPng = new File[1];
		Runnable captureScreenshotRunnable = new Runnable() {
			@Override
			public void run() {
				try {
					map.prepareImage();
					map.paintImmediately(0, 0, map.getWidth(), map.getHeight());
					savedPng[0] = map.saveScreenshotToEnvDir();
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}
		};
		try {
			if (SwingUtilities.isEventDispatchThread()) {
				captureScreenshotRunnable.run();
			} else {
				SwingUtilities.invokeAndWait(captureScreenshotRunnable);
			}
		} catch (InvocationTargetException e) {
			Throwable cause = e.getCause();
			ExceptionHandler.handle(cause == null ? e : cause);
			return;
		} catch (InterruptedException e) {
			ExceptionHandler.handle(e);
			return;
		} catch (RuntimeException e) {
			Throwable cause = e.getCause();
			if (cause instanceof IOException) {
				ExceptionHandler.handle("Failed to save screenshot", cause);
			} else {
				ExceptionHandler.handle("Failed to save screenshot", e);
			}
			return;
		}
		if (savedPng[0] == null) {
			return;
		}

		String pngName = savedPng[0].getName();
		String jsonName = pngName.endsWith(".png") ? pngName.substring(0, pngName.length() - 4) + ".json" : pngName + ".json";
		File jsonFile = new File(savedPng[0].getParentFile(), jsonName);
		writeRouteJson(jsonFile, startRoute, endRoute, routeSegments);
	}

	private void exportScreenshotAndRouteJson(List<RouteSegmentResult> routeSegments, File outputDir, int routeIndex,
			LatLon start, LatLon end) {
		if (routeSegments == null || routeSegments.isEmpty()) {
			return;
		}
		File savedPng = saveScreenshotToFile(outputDir, routeIndex);
		if (savedPng == null) {
			return;
		}
		File jsonFile = new File(outputDir, routeIndex + ".json");
		writeRouteJson(jsonFile, start, end, routeSegments);
	}

	private File saveScreenshotToFile(File outputDir, int routeIndex) {
		File[] savedPng = new File[1];
		Runnable captureScreenshotRunnable = new Runnable() {
			@Override
			public void run() {
				try {
					if (!outputDir.exists() && !outputDir.mkdirs()) {
						throw new IOException("Failed to create output directory: " + outputDir.getAbsolutePath());
					}
					if (!outputDir.isDirectory()) {
						throw new IOException("Output path is not a directory: " + outputDir.getAbsolutePath());
					}
					map.prepareImage();
					map.paintImmediately(0, 0, map.getWidth(), map.getHeight());
					BufferedImage image = map.captureScreenshot();
					File targetFile = new File(outputDir, routeIndex + ".png");
					ImageIO.write(image, "png", targetFile);
					savedPng[0] = targetFile;
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}
		};
		try {
			if (SwingUtilities.isEventDispatchThread()) {
				captureScreenshotRunnable.run();
			} else {
				SwingUtilities.invokeAndWait(captureScreenshotRunnable);
			}
		} catch (InvocationTargetException e) {
			Throwable cause = e.getCause();
			ExceptionHandler.handle(cause == null ? e : cause);
			return null;
		} catch (InterruptedException e) {
			ExceptionHandler.handle(e);
			return null;
		} catch (RuntimeException e) {
			Throwable cause = e.getCause();
			if (cause instanceof IOException) {
				ExceptionHandler.handle("Failed to save screenshot", cause);
			} else {
				ExceptionHandler.handle("Failed to save screenshot", e);
			}
			return null;
		}
		return savedPng[0];
	}

	private void writeRouteJson(File jsonFile, LatLon start, LatLon end, List<RouteSegmentResult> routeSegments) {
		try (Writer writer = Files.newBufferedWriter(jsonFile.toPath(), StandardCharsets.UTF_8)) {
			Gson gson = new GsonBuilder().setPrettyPrinting().create();
			JsonObject root = new JsonObject();
			if (start != null) {
				JsonObject startPoint = new JsonObject();
				startPoint.addProperty("lat", start.getLatitude());
				startPoint.addProperty("lon", start.getLongitude());
				root.add("start_point", startPoint);
			}
			if (end != null) {
				JsonObject endPoint = new JsonObject();
				endPoint.addProperty("lat", end.getLatitude());
				endPoint.addProperty("lon", end.getLongitude());
				root.add("end_point", endPoint);
			}

			JsonArray segments = new JsonArray();
			int indVisual = 0;
			int prevSegment = -1;
			double dist = 0;
			for (int i = 0; i <= routeSegments.size(); i++) {
				if (i == routeSegments.size() || routeSegments.get(i).getTurnType() != null) {
					if (prevSegment >= 0) {
						RouteSegmentResult segment = routeSegments.get(prevSegment);
						TurnType tt = segment.getTurnType();
						if (tt != null) {
							JsonObject seg = new JsonObject();
							seg.addProperty("point", ++indVisual);
							seg.addProperty("turn_type", tt.toXmlString());
							if (tt.getLanes() != null) {
								String turnLanesValue = getTurnLanesString(routeSegments.get(prevSegment - 1));
								String encoded = TurnType.lanesToString(tt.getLanes());
								//seg.addProperty("lanes_encoded", encoded);
								seg.add("lanes", gson.toJsonTree(buildLaneGuidanceJson(tt, turnLanesValue)));
//								if (turnLanesValue != null) {
//									seg.addProperty("turn_lanes_tag", turnLanesValue);
//								}
							}
							seg.addProperty("distance_to_next_point_m", Math.round(dist * 100.0) / 100.0);

							segments.add(seg);
						}
					}
					prevSegment = i;
					dist = 0;
				}
				if (i < routeSegments.size()) {
					dist += routeSegments.get(i).getDistance();
				}
			}
			root.add("segments", segments);

			gson.toJson(root, writer);
		} catch (IOException e) {
			ExceptionHandler.handle("Failed to write route json: " + jsonFile.getAbsolutePath(), e);
		}
	}

	private void generateRandomRoutes() {
		BinaryMapIndexReader[] readers;
		try {
			readers = DataExtractionSettings.getSettings().getObfReaders();
		} catch (IOException e) {
			ExceptionHandler.handle(e);
			return;
		}
		if (readers.length == 0) {
			JOptionPane.showMessageDialog(OsmExtractionUI.MAIN_APP.getFrame(),
					"Please specify obf file in settings", "Obf file not found", JOptionPane.ERROR_MESSAGE);
			return;
		}
		File outputDir = createNextRouteOutputDir();
		if (outputDir == null) {
			return;
		}
		RandomRouteTester.GeneratorConfig config = new RandomRouteTester.GeneratorConfig();
		config.MIN_DISTANCE_KM = RANDOM_ROUTE_MIN_KM;
		config.MAX_DISTANCE_KM = RANDOM_ROUTE_MAX_KM;
		config.CENTER_POINT = new LatLon(map.getLatitude(), map.getLongitude());
		config.CENTER_RADIUS_KM = RANDOM_ROUTE_CENTER_RADIUS_KM;
		config.RANDOM_PROFILES = new String[] { RANDOM_ROUTES_PROFILE };
		RandomRouteGenerator generator = new RandomRouteGenerator(config);
		List<RandomRouteEntry> entries = generator.generateTestList(List.of(readers));
		List<RouteSeed> routes = new ArrayList<>();
		int index = 1;
		LatLon mapCenter = config.CENTER_POINT;
		for (RandomRouteEntry entry : entries) {
			LatLon start = entry.getStart();
			LatLon end = entry.getFinish();
			if (start != null && end != null
					&& isRouteNearCenter(mapCenter, start, end)
					&& isRouteInSingleScreen(start, end)) {
				routes.add(new RouteSeed(index++, start, end));
			}
		}
		if (routes.isEmpty()) {
			JOptionPane.showMessageDialog(OsmExtractionUI.MAIN_APP.getFrame(),
					"No routes were generated", "Random routes", JOptionPane.WARNING_MESSAGE);
			return;
		}
		File obfFile = readers[0].getFile();
		writeRandomRoutesJson(new File(outputDir, RANDOM_ROUTES_FILE_NAME), obfFile.getName(), routes);
		runBatchRoutes(routes, outputDir);
	}

	private void loadRandomRoutes() {
		JFileChooser fileChooser = new JFileChooser(DataExtractionSettings.getSettings().getDefaultWorkingDir());
		fileChooser.setDialogTitle("Load random_routes.json");
		if (fileChooser.showOpenDialog(map) != JFileChooser.APPROVE_OPTION) {
			return;
		}
		File jsonFile = fileChooser.getSelectedFile();
		if (jsonFile == null || !jsonFile.isFile()) {
			return;
		}
		BinaryMapIndexReader[] readers;
		try {
			readers = DataExtractionSettings.getSettings().getObfReaders();
		} catch (IOException e) {
			ExceptionHandler.handle(e);
			return;
		}
		if (readers.length == 0) {
			JOptionPane.showMessageDialog(OsmExtractionUI.MAIN_APP.getFrame(),
					"Please specify obf file in settings", "Obf file not found", JOptionPane.ERROR_MESSAGE);
			return;
		}
		if (readers.length > 1) {
			JOptionPane.showMessageDialog(OsmExtractionUI.MAIN_APP.getFrame(),
					"Please select a single obf file in settings", "Multiple obf files", JOptionPane.ERROR_MESSAGE);
			return;
		}
		File outputDir = createNextRouteOutputDir();
		if (outputDir == null) {
			return;
		}
		File obfFile = readers[0].getFile();
		List<RouteSeed> routes = readRandomRoutesJson(jsonFile, obfFile.getName());
		if (routes.isEmpty()) {
			JOptionPane.showMessageDialog(OsmExtractionUI.MAIN_APP.getFrame(),
					"No routes were loaded", "Random routes", JOptionPane.WARNING_MESSAGE);
			return;
		}
		writeRandomRoutesJson(new File(outputDir, RANDOM_ROUTES_FILE_NAME), obfFile.getName(), routes);
		runBatchRoutes(routes, outputDir);
	}

	private void runBatchRoutes(List<RouteSeed> routes, File outputDir) {
		new Thread(() -> {
			String previousRouteMode = DataExtractionSettings.getSettings().getRouteMode();
			boolean previousAnimate = DataExtractionSettings.getSettings().isAnimateRouting();
			try {
				DataExtractionSettings.getSettings().setRouteMode(RANDOM_ROUTES_PROFILE);
				DataExtractionSettings.getSettings().setAnimateRouting(false);
				intermediates.clear();
				for (RouteSeed route : routes) {
					previousRoute = null;
					setStart(route.start);
					setEnd(route.end);
					RoutePlannerFrontEnd router = new RoutePlannerFrontEnd();
					router.setUseOnlyHHRouting(false).setHHRoutingConfig(null);
					List<Entity> entities = selfRoute(route.start, route.end, intermediates, false, previousRoute,
							router, RouteCalculationMode.NORMAL);
					if (entities == null || previousRoute == null || previousRoute.isEmpty()) {
						continue;
					}
					DataTileManager<Entity> points = new DataTileManager<Entity>(11);
					for (Entity w : entities) {
						LatLon n = w.getLatLon();
						points.registerObject(n.getLatitude(), n.getLongitude(), w);
					}
					List<RouteSegmentResult> routeSegmentsToExport = new ArrayList<>(previousRoute);
					SwingUtilities.invokeAndWait(() -> {
						map.setPoints(points);
						LatLon center = MapUtils.calculateMidPoint(route.start, route.end);
						map.setLatLon(center.getLatitude(), center.getLongitude());
						map.setZoom(ROUTE_SCREEN_ZOOM);
						map.refresh();
						map.fillPopupActions();
					});
					try {
						Thread.sleep(ROUTE_RENDER_DELAY_MS);
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
					}
					SwingUtilities.invokeAndWait(() ->
						exportScreenshotAndRouteJson(routeSegmentsToExport, outputDir, route.index, route.start, route.end));
				}
			} catch (Exception e) {
				ExceptionHandler.handle(e);
			} finally {
				DataExtractionSettings.getSettings().setRouteMode(previousRouteMode);
				DataExtractionSettings.getSettings().setAnimateRouting(previousAnimate);
			}
		}).start();
	}

	private File createNextRouteOutputDir() {
		File baseDir = DataExtractionSettings.getSettings().getDefaultWorkingDir();
		if (!baseDir.exists() && !baseDir.mkdirs()) {
			ExceptionHandler.handle(new IOException("Failed to create working directory: " + baseDir.getAbsolutePath()));
			return null;
		}
		File[] existingDirs = baseDir.listFiles(File::isDirectory);
		int maxIndex = 0;
		if (existingDirs != null) {
			for (File dir : existingDirs) {
				String name = dir.getName();
				if (!name.startsWith(ROUTE_DIR_PREFIX)) {
					continue;
				}
				String numberPart = name.substring(ROUTE_DIR_PREFIX.length());
				try {
					int value = Integer.parseInt(numberPart);
					if (value > maxIndex) {
						maxIndex = value;
					}
				} catch (NumberFormatException ex) {
					// ignore non-numeric names
				}
			}
		}
		int nextIndex = maxIndex + 1;
		String dirName = String.format(Locale.US, "%s%04d", ROUTE_DIR_PREFIX, nextIndex);
		File outputDir = new File(baseDir, dirName);
		if (!outputDir.exists() && !outputDir.mkdirs()) {
			ExceptionHandler.handle(new IOException("Failed to create output directory: " + outputDir.getAbsolutePath()));
			return null;
		}
		return outputDir;
	}

	private void writeRandomRoutesJson(File jsonFile, String obfFileName, List<RouteSeed> routes) {
		try (Writer writer = Files.newBufferedWriter(jsonFile.toPath(), StandardCharsets.UTF_8)) {
			Gson gson = new GsonBuilder().setPrettyPrinting().create();
			JsonObject root = new JsonObject();
			root.addProperty("schema_version", 1);
			root.addProperty("created_at", formatUtcDate(new Date()));
			root.addProperty("profile", RANDOM_ROUTES_PROFILE);
			root.addProperty("obf_file_name", obfFileName);
			root.addProperty("route_count", routes.size());
			JsonArray routesJson = new JsonArray();
			for (RouteSeed route : routes) {
				JsonObject entry = new JsonObject();
				entry.addProperty("index", route.index);
				entry.addProperty("start", formatLatLon(route.start));
				entry.addProperty("end", formatLatLon(route.end));
				routesJson.add(entry);
			}
			root.add("routes", routesJson);
			gson.toJson(root, writer);
		} catch (IOException e) {
			ExceptionHandler.handle("Failed to write random routes json: " + jsonFile.getAbsolutePath(), e);
		}
	}

	private List<RouteSeed> readRandomRoutesJson(File jsonFile, String expectedObfFileName) {
		List<RouteSeed> routes = new ArrayList<>();
		try (Reader reader = Files.newBufferedReader(jsonFile.toPath(), StandardCharsets.UTF_8)) {
			Gson gson = new Gson();
			JsonObject root = gson.fromJson(reader, JsonObject.class);
			if (root == null) {
				return routes;
			}
			int schemaVersion = root.has("schema_version") ? root.get("schema_version").getAsInt() : 0;
			if (schemaVersion != 1) {
				JOptionPane.showMessageDialog(OsmExtractionUI.MAIN_APP.getFrame(),
						"Unsupported schema version: " + schemaVersion, "Random routes", JOptionPane.ERROR_MESSAGE);
				return routes;
			}
			String obfFileName = root.has("obf_file_name") ? root.get("obf_file_name").getAsString() : null;
			if (obfFileName == null || !obfFileName.equals(expectedObfFileName)) {
				JOptionPane.showMessageDialog(OsmExtractionUI.MAIN_APP.getFrame(),
						"OBF mismatch: expected " + expectedObfFileName + " but found " + obfFileName,
						"Random routes", JOptionPane.ERROR_MESSAGE);
				return routes;
			}
			JsonArray routesJson = root.has("routes") ? root.getAsJsonArray("routes") : null;
			if (routesJson == null) {
				return routes;
			}
			for (JsonElement element : routesJson) {
				JsonObject entry = element.getAsJsonObject();
				int index = entry.has("index") ? entry.get("index").getAsInt() : routes.size() + 1;
				LatLon start = parseLatLon(entry.get("start").getAsString());
				LatLon end = parseLatLon(entry.get("end").getAsString());
				if (start != null && end != null) {
					routes.add(new RouteSeed(index, start, end));
				}
			}
		} catch (Exception e) {
			ExceptionHandler.handle("Failed to read random routes json: " + jsonFile.getAbsolutePath(), e);
		}
		return routes;
	}

	private String formatLatLon(LatLon latLon) {
		return String.format(Locale.US, "%.6f, %.6f", latLon.getLatitude(), latLon.getLongitude());
	}

	private LatLon parseLatLon(String value) {
		if (value == null) {
			return null;
		}
		String[] parts = value.split(",");
		if (parts.length < 2) {
			return null;
		}
		try {
			double lat = Double.parseDouble(parts[0].trim());
			double lon = Double.parseDouble(parts[1].trim());
			return new LatLon(lat, lon);
		} catch (NumberFormatException e) {
			return null;
		}
	}

	private String formatUtcDate(Date date) {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.US);
		format.setTimeZone(TimeZone.getTimeZone("UTC"));
		return format.format(date);
	}

	private boolean isRouteNearCenter(LatLon mapCenter, LatLon start, LatLon end) {
		if (mapCenter == null || start == null || end == null) {
			return false;
		}
		double radiusMeters = RANDOM_ROUTE_CENTER_RADIUS_KM * 1000.0;
		return MapUtils.getDistance(mapCenter, start) <= radiusMeters
				&& MapUtils.getDistance(mapCenter, end) <= radiusMeters;
	}

	private boolean isRouteInSingleScreen(LatLon start, LatLon end) {
		double centerLat = (start.getLatitude() + end.getLatitude()) / 2.0;
		double centerLon = (start.getLongitude() + end.getLongitude()) / 2.0;
		double centerTileX = MapUtils.getTileNumberX(ROUTE_SCREEN_ZOOM, centerLon);
		double centerTileY = MapUtils.getTileNumberY(ROUTE_SCREEN_ZOOM, centerLat);
		double halfWidthTiles = (map.getWidth() / map.getTileSize()) / 2.0;
		double halfHeightTiles = (map.getHeight() / map.getTileSize()) / 2.0;
		double startTileX = MapUtils.getTileNumberX(ROUTE_SCREEN_ZOOM, start.getLongitude());
		double startTileY = MapUtils.getTileNumberY(ROUTE_SCREEN_ZOOM, start.getLatitude());
		double endTileX = MapUtils.getTileNumberX(ROUTE_SCREEN_ZOOM, end.getLongitude());
		double endTileY = MapUtils.getTileNumberY(ROUTE_SCREEN_ZOOM, end.getLatitude());
		return Math.abs(startTileX - centerTileX) <= halfWidthTiles
				&& Math.abs(endTileX - centerTileX) <= halfWidthTiles
				&& Math.abs(startTileY - centerTileY) <= halfHeightTiles
				&& Math.abs(endTileY - centerTileY) <= halfHeightTiles;
	}

	private static String getTurnLanesString(RouteSegmentResult segment) {
		if (segment.getObject().getOneway() == 0) {
			if (segment.isForwardDirection()) {
				return segment.getObject().getValue("turn:lanes:forward");
			} else {
				return segment.getObject().getValue("turn:lanes:backward");
			}
		} else {
			return segment.getObject().getValue("turn:lanes");
		}
	}

	private static List<Map<String, Object>> buildLaneGuidanceJson(TurnType turnType, String turnLanesValue) {
		int[] lanes = turnType == null ? null : turnType.getLanes();
		int activeCommonLaneTurn = turnType == null ? -1 : turnType.getActiveCommonLaneTurn();
		String[] turnLanesValueParsed = null;
		if (turnLanesValue != null && !turnLanesValue.isEmpty()) {
			turnLanesValueParsed = turnLanesValue.split("\\|", -1);
			if (lanes == null || turnLanesValueParsed.length != lanes.length) {
				turnLanesValueParsed = null;
			}
		}
		List<Map<String, Object>> lanesJson = new ArrayList<>();
		if (lanes != null) {
			for (int laneIndex = 0; laneIndex < lanes.length; laneIndex++) {
				int laneValue = lanes[laneIndex];
				Map<String, Object> laneJson = new LinkedHashMap<>();
				int primary = TurnType.getPrimaryTurn(laneValue);
				if (primary == 0) {
					primary = TurnType.C;
				}
				boolean[] isNone = new boolean[] {false, false, false};
				if (turnLanesValueParsed != null) {
					String laneTag = turnLanesValueParsed[laneIndex] == null ? "" : turnLanesValueParsed[laneIndex].trim();
					if (!laneTag.isEmpty()) {
						String[] laneTagParts = laneTag.split(";", -1);
						int i = 0;
						for (String laneTagPart : laneTagParts) {
							isNone[i++] =  laneTagPart != null && "none".equals(laneTagPart.trim());
						}
					}
				}
				laneJson.put("active", activeCommonLaneTurn != -1 && primary == activeCommonLaneTurn);
				laneJson.put("primary", isNone[0] ? "NONE" : TurnType.valueOf(primary, false).toXmlString());
				int secondary = TurnType.getSecondaryTurn(laneValue);
				laneJson.put("secondary", isNone[1] ? "NONE" : (secondary == 0 ? "" : TurnType.valueOf(secondary, false).toXmlString()));
				int tertiary = TurnType.getTertiaryTurn(laneValue);
				laneJson.put("tertiary", isNone[2] ? "NONE" : (tertiary == 0 ? "" : TurnType.valueOf(tertiary, false).toXmlString()));
				lanesJson.add(laneJson);
			}
		}
		return lanesJson;
	}

	private static Reader getUTF8Reader(InputStream f) throws IOException {
		BufferedInputStream bis = new BufferedInputStream(f);
		assert bis.markSupported();
		bis.mark(3);
		boolean reset = true;
		byte[] t = new byte[3];
		bis.read(t);
		if (t[0] == ((byte) 0xef) && t[1] == ((byte) 0xbb) && t[2] == ((byte) 0xbf)) {
			reset = false;
		}
		if (reset) {
			bis.reset();
		}
		return new InputStreamReader(bis, "UTF-8");
	}

	public List<Way> parseGPX(File f) {
		List<Way> res = new ArrayList<Way>();
		try {
			StringBuilder content = new StringBuilder();
			BufferedReader reader = new BufferedReader(getUTF8Reader(new FileInputStream(f)));
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
                decodedValues.add((rawDecodedValue) / 1.0e5);
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


	protected Collection<Entity> hhRoute(LatLon startRoute, LatLon endRoute) {
		// specialize testing
		try {
			String profile = DataExtractionSettings.getSettings().getRouteMode();
			if(profile.indexOf(',') != -1) {
				profile = profile.substring(0, profile.indexOf(',')).trim();
			}
			HHRoutePlanner<?> hhRoutePlanner ;
//			hhRoutePlanner = hhPlanners.get(profile);
//			if (hhRoutePlanner == null) {
				File hhFile = getHHFile(profile);
				BinaryMapIndexReader[] readers = DataExtractionSettings.getSettings().getObfReaders();
				final RoutingContext ctx = prepareRoutingContext(null, DataExtractionSettings.getSettings().getRouteMode(),
						RouteCalculationMode.NORMAL, readers, new RoutePlannerFrontEnd());
				if (hhFile.exists()) {
					Connection conn = DBDialect.SQLITE.getDatabaseConnection(hhFile.getAbsolutePath(), log);
					hhRoutePlanner = HHRoutePlanner.createDB(ctx, new HHRoutingDB(hhFile, conn));
				} else {
					hhRoutePlanner = HHRoutePlanner.create(ctx);
				}
//				hhPlanners.put(profile, hhRoutePlanner);
//			}

			HHNetworkRouteRes route = hhRoutePlanner.runRouting(startRoute, endRoute, null);
			List<Entity> lst = new ArrayList<Entity>();
			if (route.getError() != null) {
				JOptionPane.showMessageDialog(OsmExtractionUI.MAIN_APP.getFrame(), route.getError(), "Routing error",
						JOptionPane.ERROR_MESSAGE);
				System.err.println(route.getError());
			}
			if (!route.getList().isEmpty()) {
				calculateResult(lst, route.getList());
			} else {
				TLongObjectHashMap<Entity> entities = new TLongObjectHashMap<Entity>();
				for (HHNetworkSegmentRes r : route.segments) {
					if (r.list != null) {
						for (RouteSegmentResult rs : r.list) {
							HHRoutingUtilities.addWay(entities, rs, "highway", "secondary");
						}
					} else if (r.segment != null) {
						HHRoutingUtilities.addWay(entities, r.segment, "highway", "primary");
					}
				}
				lst.addAll(entities.valueCollection());
			}
			for (HHNetworkRouteRes altRoute : route.altRoutes) {
				TLongObjectHashMap<Entity> entities = new TLongObjectHashMap<Entity>();
				for (HHNetworkSegmentRes r : altRoute.segments) {
					if (r.list != null) {
						for (RouteSegmentResult rs : r.list) {
							HHRoutingUtilities.addWay(entities, rs, "highway", "tertiary");
						}
					} else if (r.segment != null) {
						HHRoutingUtilities.addWay(entities, r.segment, "highway", "tertiary");
					}
				}
				lst.addAll(entities.valueCollection());
			}

			return lst;
		} catch (Exception e) {
			ExceptionHandler.handle(e);
			return new ArrayList<>();
		}
	}


	public List<Entity> selfRoute(LatLon start, LatLon end, List<LatLon> intermediates,
			boolean gpx, List<RouteSegmentResult> previousRoute, RoutePlannerFrontEnd router, RouteCalculationMode rm) {
		this.gpx = gpx;
		List<Entity> res = new ArrayList<Entity>();
		long time = System.currentTimeMillis();


		final boolean animateRoutingCalculation = DataExtractionSettings.getSettings().isAnimateRouting();
		if(animateRoutingCalculation) {
			nextTurn.setVisible(true);
			playPauseButton.setVisible(true);
			stopButton.setVisible(true);
			pause = true;
			playPauseButton.setText("Play");
		}
		stop = false;

		System.out.println("Self made route from " + start + " to " + end);
		if (start != null && end != null) {
			try {
				BinaryMapIndexReader[] files = DataExtractionSettings.getSettings().getObfReaders();
				if (files.length == 0) {
					JOptionPane.showMessageDialog(OsmExtractionUI.MAIN_APP.getFrame(),
							"Please specify obf file in settings", "Obf file not found", JOptionPane.ERROR_MESSAGE);
					return null;
				}
				PrecalculatedRouteDirection precalculatedRouteDirection = null;
				// Test gpx precalculation
				// LatLon[] lts = parseGPXDocument("/home/victor/projects/osmand/temp/esya.gpx");
				// start = lts[0];
				// end = lts[lts.length - 1];
				// System.out.println("Start " + start + " end " + end);
				// precalculatedRouteDirection = PrecalculatedRouteDirection.build(lts, config.router.getMaxSpeed());
				// precalculatedRouteDirection.setFollowNext(true);

				RoutingContext ctx = cacheRctx;
				if(!DataExtractionSettings.getSettings().getRouteMode().equals(cacheRouteParams) || ctx == null) {
					cacheRouteParams = DataExtractionSettings.getSettings().getRouteMode();
					ctx = prepareRoutingContext(previousRoute, cacheRouteParams, rm, files, router);
				} else {
					ctx.calculationProgress = new RouteCalculationProgress();
				}
				final DataTileManager<Entity> points = map.getPoints();
				map.setPoints(points);
				ctx.setVisitor(createSegmentVisitor(animateRoutingCalculation, points));
				// Choose native or not native
				long nt = System.nanoTime();
				startProgressThread(ctx);
				try {
					GpxRouteApproximation gctx = new GpxRouteApproximation(ctx);
					List<GpxPoint> gpxPoints = router.generateGpxPoints(gctx, new LocationsHolder(intermediates));
					RouteCalcResult searchRoute = gpx ? getGpxAproximation(router, gctx, gpxPoints)
							: router.searchRoute(ctx, start, end, intermediates, precalculatedRouteDirection);
					throwExceptionIfRouteNotFound(ctx, searchRoute);
					System.out.println("Routing time " + (System.nanoTime() - nt) / 1.0e9f);
					if (animateRoutingCalculation) {
						playPauseButton.setVisible(false);
						nextTurn.setText("FINISH");
						waitNextPress();
						nextTurn.setText(">>");
					}
					this.previousRoute = searchRoute.getList();
					calculateResult(res, searchRoute.getList());
				} finally {
					if (ctx.calculationProgress != null) {
						ctx.calculationProgress.isCancelled = true;
					}
					if (USE_CACHE_CONTEXT) {
						cacheRctx = ctx;
					}
				}
			} catch (Exception e) {
				ExceptionHandler.handle(e);
			} finally {
				playPauseButton.setVisible(false);
				nextTurn.setVisible(false);
				stopButton.setVisible(false);
				if (map.getPoints() != null) {
					map.getPoints().clear();
				}
			}
			System.out.println(
					"!!! Finding self route: " + res.size() + " " + (System.currentTimeMillis() - time) + " ms");
		}
		return res;
	}

	private RoutingContext prepareRoutingContext(List<RouteSegmentResult> previousRoute, String routeMode, RouteCalculationMode rm,
			BinaryMapIndexReader[] files, RoutePlannerFrontEnd router) throws IOException {
		String[] props = routeMode.split("\\,");
		Map<String, String> paramsR = new LinkedHashMap<String, String>();
		for (String p : props) {
			if (p.contains("=")) {
				paramsR.put(p.split("=")[0], p.split("=")[1]);
			} else {
				paramsR.put(p, "true");
			}
		}
		RoutingMemoryLimits memoryLimit = new RoutingMemoryLimits(2000, DEFAULT_NATIVE_MEMORY_LIMIT * 10);
		GeneralRouter.IMPASSABLE_ROAD_SHIFT = 6;
		RoutingConfiguration config = DataExtractionSettings.getSettings().getRoutingConfig().setDirectionPoints(directionPointsFile)
//				.addImpassableRoad(70088213l)
//				.addImpassableRoad(896092207l)
				.build(props[0], /* RoutingConfiguration.DEFAULT_MEMORY_LIMIT */ memoryLimit, paramsR);

		// config.planRoadDirection = 1;
		// Test initial direction
		// config.initialDirection = 90d / 180d * Math.PI; // EAST
		// config.initialDirection = 180d / 180d * Math.PI; // SOUTH
		// config.initialDirection = -90d / 180d * Math.PI; // WEST
		// config.initialDirection = 0 / 180d * Math.PI; // NORTH
		// config.NUMBER_OF_DESIRABLE_TILES_IN_MEMORY = 300;
		// config.ZOOM_TO_LOAD_TILES = 14;
//		config.initialDirection = 30 / 180.0 * Math.PI;
		try {
			config.minPointApproximation = RoutingConfiguration.parseSilentFloat(
					paramsR.get("minPointApproximation"), config.minPointApproximation);
		} catch (NumberFormatException e){
			e.printStackTrace();
		}
//		try {
//			config.routeCalculationTime = new SimpleDateFormat("dd.MM.yyyy HH:mm", Locale.US)
//					.parse("19.07.2019 12:40").getTime();
//		} catch (Exception e) {
//		}
//		config.routeCalculationTime = System.currentTimeMillis();

		final RoutingContext ctx = router.buildRoutingContext(config,
				DataExtractionSettings.getSettings().useNativeRouting()
						? NativeSwingRendering.getDefaultFromSettings()
						: null,
				files, rm);

		ctx.leftSideNavigation = false;
		ctx.previouslyCalculatedRoute = previousRoute;
		log.info("Use " + config.routerName + " mode for routing");
		return ctx;
	}

	private RouteCalcResult getGpxAproximation(RoutePlannerFrontEnd router, GpxRouteApproximation gctx,
			List<GpxPoint> gpxPoints) throws IOException, InterruptedException {
		if (DataExtractionSettings.getSettings().useNativeRouting()) {
			router.setUseNativeApproximation(true);
		}
		GpxRouteApproximation r = router.searchGpxRoute(gctx, gpxPoints, null, false);
		return new RouteCalcResult(r.collectFinalPointsAsRoute());
	}

	private void throwExceptionIfRouteNotFound(final RoutingContext ctx, RouteCalcResult searchRoute) {
		if (searchRoute == null) {
			String reason = "unknown";
			if (ctx.calculationProgress.segmentNotFound >= 0) {
				if (ctx.calculationProgress.segmentNotFound == 0) {
					reason = " start point is too far from road";
				} else {
					reason = " target point " + ctx.calculationProgress.segmentNotFound + " is too far from road";
				}
			} else if (ctx.calculationProgress.directSegmentQueueSize == 0) {
				reason = " route can not be found from start point ("
						+ ctx.calculationProgress.distanceFromBegin / 1000.0f + " km)";
			} else if (ctx.calculationProgress.reverseSegmentQueueSize == 0) {
				reason = " route can not be found from end point (" + ctx.calculationProgress.distanceFromEnd / 1000.0f
						+ " km)";
			}
			throw new RuntimeException("Route not found : " + reason);
		} else if (!searchRoute.isCorrect()) {
			throw new RuntimeException("Route not found : " + searchRoute.getError());
		}
	}

	private void startProgressThread(final RoutingContext ctx) {
		new Thread(){
			@Override
			public void run() {
				while(ctx.calculationProgress != null && !ctx.calculationProgress.isCancelled) {
//					float p = Math.max(ctx.calculationProgress.distanceFromBegin, ctx.calculationProgress.distanceFromEnd);
//					float all = 1.25f * ctx.calculationProgress.totalEstimatedDistance;
//							while (p > all * 0.9) {
//								all *= 1.2;
//							}
//					if(all > 0 ) {
//						int  t = (int) (p*p/(all*all)* 100.0f);
//								int  t = (int) (p/all*100f);
//						System.out.println("Progress " + t + " % " +
//								ctx.calculationProgress.distanceFromBegin + " " + ctx.calculationProgress.distanceFromEnd+" " + all);
//					}
					try {
						sleep(100);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			};
		}.start();
	}

	private void calculateResult(List<Entity> res, List<RouteSegmentResult> searchRoute) {
		RouteSegmentResult prevSegm = null;
		int indVisual = 0;
		for (RouteSegmentResult segm : searchRoute) {
			// double dist = MapUtils.getDistance(s.startPoint, s.endPoint);
			Way way = new Way(-1);
//					String name = String.format("time %.2f ", s.getSegmentTime());
			TurnType tt = segm.getTurnType();
			String name = "";
			if (tt != null) {
				name = (++indVisual) + ". ";
			}

			way.putTag(OSMTagKey.NAME.getValue(), name);
			way.putTag("colour", "blue");

			if (prevSegm != null
					&& MapUtils.getDistance(prevSegm.getEndPoint(), segm.getStartPoint()) > 0) {
				net.osmand.osm.edit.Node pp = new net.osmand.osm.edit.Node(prevSegm.getEndPoint().getLatitude(), prevSegm.getEndPoint().getLongitude(), -1);
				res.add(pp);
				pp.putTag("colour", "blue");
				net.osmand.osm.edit.Node pn = new net.osmand.osm.edit.Node(segm.getStartPoint().getLatitude(), segm.getStartPoint().getLongitude() , -1);
				pn.putTag("colour", "red");
				res.add(pn);
				System.out.println(String.format("Not connected road '%f m' (%.5f/%.5f -> %.5f/%.5f) [%d: %s -> %d: %s]",
						MapUtils.getDistance(prevSegm.getEndPoint(), segm.getStartPoint()),
						pp.getLatLon().getLatitude(), pp.getLatLon().getLongitude(), pn.getLatLon().getLatitude(), pn.getLatLon().getLongitude(),
						segm.getStartPointIndex(), segm.getObject(), prevSegm.getStartPointIndex(), prevSegm.getObject() ));
			}
			boolean plus = segm.getStartPointIndex() < segm.getEndPointIndex();
			int ind = segm.getStartPointIndex();
			while (true) {
				LatLon l = segm.getPoint(ind);
				net.osmand.osm.edit.Node n = new net.osmand.osm.edit.Node(l.getLatitude(), l.getLongitude(), -1);

				int[] pointTypes = segm.getObject().getPointTypes(ind);
				if (pointTypes != null && pointTypes.length == 1 && segm.getObject().region.routeEncodingRules.size() > pointTypes[0]) {
					RouteTypeRule rtr = segm.getObject().region.quickGetEncodingRule(pointTypes[0]);
					if (rtr == null || !rtr.getTag().equals("osmand_dp")) {
						// skip all intermediate added points (should no be visual difference)
						way.addNode(n);
					}
				} else {
					way.addNode(n);
				}
				if (ind == segm.getEndPointIndex()) {
					break;
				}
				if (plus) {
					ind++;
				} else {
					ind--;
				}
			}
			if (way.getNodes().size() > 0) {
				res.add(way);
			}
			prevSegm = segm;
		}
	}

	private RouteSegmentVisitor createSegmentVisitor(final boolean animateRoutingCalculation, final DataTileManager<Entity> points) {
		return new RouteSegmentVisitor() {

			private List<RouteSegment> cache = new ArrayList<RouteSegment>();
			private List<RouteSegment> pollCache = new ArrayList<RouteSegment>();
			private List<Integer> cacheInt = new ArrayList<Integer>();

			@Override
			public void visitSegment(RouteSegment s, int  endSegment, boolean poll) {
				if (gpx) {
					return;
				}
				if (stop) {
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

			@Override
			public void visitApproximatedSegments(List<RouteSegmentResult> segment, GpxPoint start, GpxPoint target) {
				if (stop) {
					throw new RuntimeException("Interrupted");
				}
				if (!animateRoutingCalculation) {
					return;
				}
//				points.clear();
				for(List<Entity> list : points.getAllEditObjects()) {
					Iterator<Entity> it = list.iterator();
					while(it.hasNext()) {
						Entity e = it.next();
						if (!"yes".equals(e.getTag("gpx"))) {
							it.remove();
						}
					}
				}
				startRoute = start.loc;
				endRoute = target.loc;
				for (int i = 0; i < segment.size(); i++) {
					cache.add(new RouteSegment(segment.get(i).getObject(), segment.get(i).getStartPointIndex()));
					cacheInt.add(segment.get(i).getEndPointIndex());
				}
				if (cache.size() < steps) {
					return;
				}
				if (pause) {
					registerObjects(points, false, pollCache, null);
					pollCache.clear();
				}
				registerObjects(points, false, cache, cacheInt);
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
							if (i == from || i == to) {
								n.putTag("colour", "red");
							}
							points.registerObject(n.getLatitude(), n.getLongitude(), n);
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

	public LatLon[] parseGPXDocument(String fileName) throws SAXException, IOException, ParserConfigurationException {
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		DocumentBuilder dom = factory.newDocumentBuilder();
		Document doc = dom.parse(new InputSource(new FileInputStream(fileName)));
		NodeList list = doc.getElementsByTagName("trkpt");
		LatLon[] x = new LatLon[list.getLength()];
		for(int i=0; i<list.getLength(); i++){
			Element item = (Element) list.item(i);
			x[i] = new LatLon(Double.parseDouble(item.getAttribute("lat")), Double.parseDouble(item.getAttribute("lon")));
		}
		return x;
	}

	private List<LatLon> selectedGPXFileToPolyline() {
		if (selectedGPXFile.hasTrkPt()) {
			TrkSegment trkSegment = selectedGPXFile.getTracks().get(0).getSegments().get(0);
			startRoute = toLatLon(trkSegment.getPoints().get(0));
			endRoute = toLatLon(trkSegment.getPoints().get(trkSegment.getPoints().size() - 1));
			List<LatLon> polyline = new ArrayList<LatLon>(trkSegment.getPoints().size());
			for (WptPt p : trkSegment.getPoints()) {
				polyline.add(toLatLon(p));
			}
			return polyline;
		}
		return new ArrayList<LatLon>();
	}

    @Override
	public void applySettings() {
	}

}
