package net.osmand.swing;


import java.awt.Color;
import java.awt.Component;
import java.awt.Graphics2D;
import java.awt.Point;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.io.Reader;
import java.io.StringReader;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.net.URLConnection;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;

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

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.stream.JsonReader;

import net.osmand.GPXUtilities;
import net.osmand.GPXUtilities.GPXFile;
import net.osmand.GPXUtilities.GPXTrackAnalysis;
import net.osmand.GPXUtilities.Track;
import net.osmand.GPXUtilities.TrkSegment;
import net.osmand.GPXUtilities.WptPt;
import net.osmand.Location;
import net.osmand.LocationsHolder;
import net.osmand.PlatformUtil;
import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.binary.BinaryMapRouteReaderAdapter.RouteTypeRule;
import net.osmand.data.DataTileManager;
import net.osmand.data.LatLon;
import net.osmand.data.QuadRect;
import net.osmand.data.QuadTree;
import net.osmand.obf.preparation.IndexHeightData;
import net.osmand.osm.edit.Entity;
import net.osmand.osm.edit.OSMSettings.OSMTagKey;
import net.osmand.osm.edit.OsmMapUtils;
import net.osmand.osm.edit.Way;
import net.osmand.router.BinaryRoutePlanner.RouteSegment;
import net.osmand.router.BinaryRoutePlanner.RouteSegmentVisitor;
import net.osmand.router.PrecalculatedRouteDirection;
import net.osmand.router.RouteColorize.ColorizationType;
import net.osmand.router.RouteExporter;
import net.osmand.router.RoutePlannerFrontEnd;
import net.osmand.router.RoutePlannerFrontEnd.GpxPoint;
import net.osmand.router.RoutePlannerFrontEnd.GpxRouteApproximation;
import net.osmand.router.RoutePlannerFrontEnd.RouteCalculationMode;
import net.osmand.router.RouteSegmentResult;
import net.osmand.router.RoutingConfiguration;
import net.osmand.router.RoutingContext;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;

import static net.osmand.router.RoutingConfiguration.*;


public class MapRouterLayer implements MapPanelLayer {

	private final static Log log = PlatformUtil.getLog(MapRouterLayer.class);

	private static final double MIN_STRAIGHT_DIST = 50000;
	private static final double ANGLE_TO_DECLINE = 15;


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
	private GPXFile selectedGPXFile;
	private QuadTree<net.osmand.osm.edit.Node> directionPointsFile;

	private List<RouteSegmentResult> previousRoute;
	public ActionListener setStartActionListener = new ActionListener(){
		@Override
		public void actionPerformed(ActionEvent e) {
			setStart(new LatLon(map.getLatitude(), map.getLongitude()));
		}
	};

	public ActionListener setEndActionListener = new ActionListener(){
		@Override
		public void actionPerformed(ActionEvent e) {
			setEnd(new LatLon(map.getLatitude(), map.getLongitude()));
		}
	};



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
		Action complexRoute = new AbstractAction("Build route (OsmAnd standard|COMPLEX)") {
			private static final long serialVersionUID = 8049785829806139142L;

			@Override
			public void actionPerformed(ActionEvent e) {
				previousRoute = null;
				calcRoute(RouteCalculationMode.COMPLEX);
			}
		};
		directions.add(complexRoute);


		Action selfRoute = new AbstractAction("Build route (OsmAnd short|NORMAL)") {
			private static final long serialVersionUID = 507156107455281238L;

			@Override
			public void actionPerformed(ActionEvent e) {
				previousRoute = null;
				calcRoute(RouteCalculationMode.NORMAL);
			}
		};
		directions.add(selfRoute);

		Action selfBaseRoute = new AbstractAction("Build route (OsmAnd long|BASE)") {
			private static final long serialVersionUID = 8049785829806139142L;

			@Override
			public void actionPerformed(ActionEvent e) {
				previousRoute = null;
				calcRoute(RouteCalculationMode.BASE);
			}
		};
		directions.add(selfBaseRoute);

		if (selectedGPXFile != null) {
			Action recalculate = new AbstractAction("Calculate GPX route (OsmAnd)") {
				private static final long serialVersionUID = 507156107455281238L;

				@Override
				public void actionPerformed(ActionEvent e) {
					if (selectedGPXFile.hasTrkPt()) {
						TrkSegment trkSegment = selectedGPXFile.tracks.get(0).segments.get(0);
						startRoute = toLatLon(trkSegment.points.get(0));
						endRoute = toLatLon(trkSegment.points.get(trkSegment.points.size() - 1));
						List<LatLon> polyline = new ArrayList<LatLon>(trkSegment.points.size());
						for (WptPt p : trkSegment.points) {
							polyline.add(toLatLon(p));
						}
						calcRouteGpx(polyline);
					}
				}

			};
			directions.add(recalculate);
		}

		if (previousRoute != null) {
			Action recalculate = new AbstractAction("Rebuild route (OsmAnd)") {
				private static final long serialVersionUID = 507156107455281238L;

				@Override
				public void actionPerformed(ActionEvent e) {
					calcRoute(RouteCalculationMode.NORMAL);
				}
			};
			directions.add(recalculate);
		}

		Action route_YOURS = new AbstractAction("Build route (YOURS)") {
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
						map.fillPopupActions();
					}
				}.start();
			}
		};
		directions.add(route_YOURS);
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
					List<Way> ways = new ArrayList<>();
					calculateResult(ways, previousRoute);
					List<Location> locations = new ArrayList<>();
					for (Way way : ways) {
						for (net.osmand.osm.edit.Node node : way.getNodes()) {
							locations.add(new Location("", node.getLatitude(), node.getLongitude()));
						}
					}
					String name = new SimpleDateFormat("yyyy-MM-dd_HH-mm_EEE", Locale.US).format(new Date());
					RouteExporter exporter = new RouteExporter(name, previousRoute, locations, null);
					GPXFile gpxFile = exporter.exportRoute();
					JFileChooser fileChooser = new JFileChooser(
							DataExtractionSettings.getSettings().getDefaultWorkingDir());
					if (fileChooser.showSaveDialog(map) == JFileChooser.APPROVE_OPTION) {
						File file = fileChooser.getSelectedFile();
						GPXUtilities.writeGpxFile(file, gpxFile);
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
							DataExtractionSettings.getSettings().getDefaultWorkingDir());
					if (fileChooser.showOpenDialog(map) == JFileChooser.APPROVE_OPTION) {
						File file = fileChooser.getSelectedFile();
						selectedGPXFile = GPXUtilities.loadGPXFile(file);
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
					DataTileManager<Way> points = new DataTileManager<Way>(11);
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
					GPXFile res = calculateAltitude(selectedGPXFile, missingFile);
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


	protected void displayTrackInfo(GPXFile gpxFile, String header) {
		GPXTrackAnalysis analysis = selectedGPXFile.getAnalysis(gpxFile.modifiedTime);
		StringBuilder msg = new StringBuilder();
		
		msg.append(String.format("Track: distance %.1f, distance no gaps %.1f, tracks %d, points %d\n", analysis.totalDistance,
				analysis.totalDistanceWithoutGaps, analysis.totalTracks, analysis.wptPoints));
		
		if (analysis.hasElevationData) {
			msg.append(String.format("Ele: min - %.1f, max - %.1f, avg - %.1f, uphill - %.1f, downhill - %.1f\n",
					analysis.minElevation, analysis.maxElevation, analysis.avgElevation, analysis.diffElevationUp,
					analysis.diffElevationDown));
		}
		if (analysis.hasSpeedData) {
			msg.append(String.format("Speed: min - %.1f, max - %.1f, avg - %.1f, dist+speed - %.1f, dist+speed no gaps - %.1f\n",
					analysis.minSpeed, analysis.maxSpeed, analysis.avgSpeed,
					analysis.totalDistanceMoving,analysis.totalDistanceMovingWithoutGaps));
		}
		if (analysis.startTime != analysis.endTime) {
			msg.append(String.format("Time: start - %s, end - %s, span - %.1f min, span no gaps - %.1f min\n",
					new Date(analysis.startTime), new Date(analysis.endTime), analysis.timeSpan / 60000.0,
					analysis.timeSpanWithoutGaps / 60000.0));
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
	
	
	protected GPXFile calculateAltitude(GPXFile gpxFile, File[] missingFile) {
		File srtmFolder = new File(DataExtractionSettings.getSettings().getBinaryFilesDir(), "srtm");
		if (!srtmFolder.exists()) {
			return null;
		}
		IndexHeightData hd = new IndexHeightData();
		hd.setSrtmData(srtmFolder);
		for (Track tr : gpxFile.tracks) {
			for (TrkSegment s : tr.segments) {
				for (int i = 0; i < s.points.size(); i++) {
					WptPt wpt = s.points.get(i);
					double h = hd.getPointHeight(wpt.lat, wpt.lon, missingFile);
					if (h != IndexHeightData.INEXISTENT_HEIGHT) {
						wpt.ele = h;
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
			for (Track t : selectedGPXFile.tracks) {
				for (TrkSegment ts : t.segments) {
					Way w = new Way(-1);
					for (WptPt p : ts.points) {
						w.addNode(new net.osmand.osm.edit.Node(p.lat, p.lon, -1));
					}
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
		DataTileManager<Way> points = new DataTileManager<Way>(11);
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
		return new LatLon(wptPt.lat, wptPt.lon);
	}

	private void calcRouteGpx(List<LatLon> polyline) {
		new Thread() {
			@Override
			public void run() {
				List<Way> ways = selfRoute(startRoute, endRoute, polyline, true, null, RouteCalculationMode.NORMAL);
				if (ways != null) {
					DataTileManager<Way> points = new DataTileManager<Way>(11);
					for (Way w : ways) {
						LatLon n = w.getLatLon();
						points.registerObject(n.getLatitude(), n.getLongitude(), w);
					}
					map.setPoints(points);
					map.fillPopupActions();
				}

				if (selectedGPXFile != null) {
					JOptionPane.showMessageDialog(OsmExtractionUI.MAIN_APP.getFrame(), "Check validity of GPX File",
							"GPX route correction", JOptionPane.INFORMATION_MESSAGE);
				}
			}
		}.start();
	}


	private void calcRoute(final RouteCalculationMode m) {
		new Thread() {
			@Override
			public void run() {
				List<Way> ways = selfRoute(startRoute, endRoute, intermediates, false, previousRoute, m);
				if (ways != null) {
					DataTileManager<Way> points = new DataTileManager<Way>(11);
					for (Way w : ways) {
						LatLon n = w.getLatLon();
						points.registerObject(n.getLatitude(), n.getLongitude(), w);
					}
					map.setPoints(points);
					map.fillPopupActions();
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



	public List<Way> selfRoute(LatLon start, LatLon end, List<LatLon> intermediates,
			boolean gpx, List<RouteSegmentResult> previousRoute, RouteCalculationMode rm) {
		List<Way> res = new ArrayList<Way>();
		long time = System.currentTimeMillis();
		List<File> files = new ArrayList<File>();
		for (File f : Algorithms.getSortedFilesVersions(new File(DataExtractionSettings.getSettings().getBinaryFilesDir()))) {
			if(f.getName().endsWith(".obf")){
				files.add(f);
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
					rs[it++] = new BinaryMapIndexReader(raf, f);
				}
				String m = DataExtractionSettings.getSettings().getRouteMode();
				String[] props = m.split("\\,");
				RoutePlannerFrontEnd router = new RoutePlannerFrontEnd();

				Map<String, String> paramsR = new LinkedHashMap<String, String>();
				for(String p : props) {
					if(p.contains("=")) {
						paramsR.put(p.split("=")[0], p.split("=")[1]);
					} else {
						paramsR.put(p, "true");
					}
				}
				RoutingMemoryLimits memoryLimit = new RoutingMemoryLimits(1000, DEFAULT_NATIVE_MEMORY_LIMIT * 10);
				RoutingConfiguration config = DataExtractionSettings.getSettings().getRoutingConfig().
//						addImpassableRoad(6859437l).
//						addImpassableRoad(46859655089l).
						setDirectionPoints(directionPointsFile).build(props[0],
						/*RoutingConfiguration.DEFAULT_MEMORY_LIMIT*/ memoryLimit, paramsR);
				PrecalculatedRouteDirection precalculatedRouteDirection = null;
				// Test gpx precalculation
//				LatLon[] lts = parseGPXDocument("/home/victor/projects/osmand/temp/esya.gpx");
//				start = lts[0];
//				end = lts[lts.length - 1];
//				System.out.println("Start " + start + " end " + end);
//				precalculatedRouteDirection = PrecalculatedRouteDirection.build(lts, config.router.getMaxSpeed());
//				precalculatedRouteDirection.setFollowNext(true);
//				config.planRoadDirection = 1;
				// Test initial direction
//				config.initialDirection = 90d / 180d * Math.PI; // EAST
//				config.initialDirection = 180d / 180d * Math.PI; // SOUTH
//				config.initialDirection = -90d / 180d * Math.PI; // WEST
//				config.initialDirection = 0 / 180d * Math.PI; // NORTH
				// config.NUMBER_OF_DESIRABLE_TILES_IN_MEMORY = 300;
				// config.ZOOM_TO_LOAD_TILES = 14;
				try {
					config.routeCalculationTime = new SimpleDateFormat("dd.MM.yyyy HH:mm", Locale.US).parse("19.07.2019 12:40").getTime();
				} catch (Exception e) {
				}
				config.routeCalculationTime = System.currentTimeMillis();
				
				final RoutingContext ctx = router.buildRoutingContext(config, DataExtractionSettings.getSettings().useNativeRouting() ? NativeSwingRendering.getDefaultFromSettings() :
					null, rs, rm);

				ctx.leftSideNavigation = false;
				ctx.previouslyCalculatedRoute = previousRoute;
				log.info("Use " + config.routerName + " mode for routing");
				final DataTileManager<Entity> points = new DataTileManager<Entity>(11);
				map.setPoints(points);
				ctx.setVisitor(createSegmentVisitor(animateRoutingCalculation, points));
				// Choose native or not native
				long nt = System.nanoTime();
				startProgressThread(ctx);
				try {
					GpxRouteApproximation gctx = new GpxRouteApproximation(ctx);
					List<GpxPoint> gpxPoints = router.generateGpxPoints(gctx, new LocationsHolder(intermediates));
					List<RouteSegmentResult> searchRoute = gpx ?
							getGpxAproximation(router, gctx, gpxPoints) :
							router.searchRoute(ctx, start, end, intermediates, precalculatedRouteDirection);
					throwExceptionIfRouteNotFound(ctx, searchRoute);
					System.out.println("External native time " + (System.nanoTime() - nt) / 1.0e9f);
					if (animateRoutingCalculation) {
						playPauseButton.setVisible(false);
						nextTurn.setText("FINISH");
						waitNextPress();
						nextTurn.setText(">>");
					}
					this.previousRoute = searchRoute;
					calculateResult(res, searchRoute);
				} finally {
					if (ctx.calculationProgress != null) {
						ctx.calculationProgress.isCancelled = true;
					}
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
			System.out.println("!!! Finding self route: " + res.size() + " " + (System.currentTimeMillis() - time) + " ms");
		}
		return res;
	}

	private static boolean TEST_INTERMEDIATE_POINTS = true;
	private List<RouteSegmentResult> getGpxAproximation(RoutePlannerFrontEnd router, GpxRouteApproximation gctx,
			List<GpxPoint> gpxPoints) throws IOException, InterruptedException {
		GpxRouteApproximation r = router.searchGpxRoute(gctx, gpxPoints, null);
		if(!TEST_INTERMEDIATE_POINTS) {
			return r.result;
		}
		List<RouteSegmentResult> rsr = new ArrayList<RouteSegmentResult>();
		for(GpxPoint pnt : r.finalPoints) {
			rsr.addAll(pnt.routeToTarget);
		}
		return rsr;
	}

	private void throwExceptionIfRouteNotFound(final RoutingContext ctx, List<RouteSegmentResult> searchRoute) {
		if (searchRoute == null) {
			String reason = "unknown";
			if (ctx.calculationProgress.segmentNotFound >= 0) {
				if (ctx.calculationProgress.segmentNotFound == 0) {
					reason = " start point is too far from road";
				} else {
					reason = " target point " + ctx.calculationProgress.segmentNotFound + " is too far from road";
				}
			} else if (ctx.calculationProgress.directSegmentQueueSize == 0) {
				reason = " route can not be found from start point (" + ctx.calculationProgress.distanceFromBegin / 1000.0f
						+ " km)";
			} else if (ctx.calculationProgress.reverseSegmentQueueSize == 0) {
				reason = " route can not be found from end point (" + ctx.calculationProgress.distanceFromEnd / 1000.0f + " km)";
			}
			throw new RuntimeException("Route not found : " + reason);
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
						System.out.println(String.format("Not connected road '%f m' (prev %s - current %s),  %d ind %s", OsmMapUtils.getDistance(prevWayNode, n), prevWayNode.getLatLon(), n.getLatLon(), i,
								s.getObject()));
					}
					prevWayNode = null;
				}
				int[] pointTypes = s.getObject().getPointTypes(i);
				if (pointTypes != null && pointTypes.length == 1) {
					RouteTypeRule rtr = s.getObject().region.quickGetEncodingRule(pointTypes[0]);
					if (rtr == null || !rtr.getTag().equals("osmand_dp")) {
						// skip all intermediate added points (should no be visual difference)
						way.addNode(n);
					}
				} else {
					way.addNode(n);
				}
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
				res.add(way);
			}

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

	@Override
	public void applySettings() {
	}

}
