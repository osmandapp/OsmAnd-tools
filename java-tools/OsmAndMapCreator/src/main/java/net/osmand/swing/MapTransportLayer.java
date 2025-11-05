package net.osmand.swing;


import java.awt.Color;
import java.awt.Component;
import java.awt.Graphics2D;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.swing.AbstractAction;
import javax.swing.Action;
import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JMenu;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JPopupMenu;
import javax.swing.SwingUtilities;

import org.apache.commons.logging.Log;

import net.osmand.PlatformUtil;
import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.data.DataTileManager;
import net.osmand.data.LatLon;
import net.osmand.osm.edit.Entity;
import net.osmand.osm.edit.Node;
import net.osmand.osm.edit.OSMSettings.OSMTagKey;
import net.osmand.osm.edit.Way;
import net.osmand.router.GeneralRouter;
import net.osmand.router.NativeTransportRoutingResult;
import net.osmand.router.RoutingConfiguration.Builder;
import net.osmand.router.TransportRoutePlanner;
import net.osmand.router.TransportRoutePlanner.TransportRouteResultSegment;
import net.osmand.router.TransportRouteResult;
import net.osmand.router.TransportRoutingConfiguration;
import net.osmand.router.TransportRoutingContext;
import net.osmand.util.MapUtils;



public class MapTransportLayer implements MapPanelLayer {

	private final static Log log = PlatformUtil.getLog(MapTransportLayer.class);

	private static final boolean DRAW_WALK_SEGMENTS = false;

	private MapPanel map;
	private JButton prevRoute;
	private JButton infoButton;
	private JButton nextRoute;
	

	private LatLon start;
	private LatLon end;
	private List<TransportRouteResult> results = new ArrayList<TransportRouteResult>();
	private int currentRoute = 0; 

	@Override
	public void destroyLayer() {

	}

	
	@Override
	public void initLayer(MapPanel map) {
		this.map = map;
		JPanel btnPanel = new JPanel();
		btnPanel.setLayout(new BoxLayout(btnPanel, BoxLayout.LINE_AXIS));
//		btnPanel.setBackground(new Color(255, 255, 255, 0));
		btnPanel.setOpaque(false);

		prevRoute = new JButton("<<"); //$NON-NLS-1$
		prevRoute.addActionListener(new ActionListener(){
			@Override
			public void actionPerformed(ActionEvent e) {
				if(currentRoute >= 0) {
					currentRoute--;
					redrawRoute();
				}
			}
		});
		prevRoute.setVisible(false);
		prevRoute.setAlignmentY(Component.TOP_ALIGNMENT);
		btnPanel.add(prevRoute);
		
		
		nextRoute = new JButton(">>"); //$NON-NLS-1$
		nextRoute.addActionListener(new ActionListener(){
			@Override
			public void actionPerformed(ActionEvent e) {
				if(currentRoute < results.size() - 1) {
					currentRoute++;
					redrawRoute();
				}
			}
		});
		nextRoute.setVisible(false);
		nextRoute.setAlignmentY(Component.TOP_ALIGNMENT);
		btnPanel.add(nextRoute);
		
		infoButton = new JButton("Info about route"); //$NON-NLS-1$
		infoButton.addActionListener(new ActionListener(){
			@Override
			public void actionPerformed(ActionEvent e) {
			}
		});
		infoButton.setVisible(false);
		infoButton.setAlignmentY(Component.TOP_ALIGNMENT);
		btnPanel.add(infoButton);
		
		map.add(btnPanel, 0);
//		map.add(Box.createHorizontalGlue());
		btnPanel.add(Box.createHorizontalGlue());
	}

	public void fillPopupMenuWithActions(JPopupMenu menu) {
		Action transportRoute = new AbstractAction("Calculate route") {
			private static final long serialVersionUID = 5071561074552411238L;

			@Override
			public void actionPerformed(ActionEvent e) {
				calcRoute(false);
			}
		};
		Action transportRouteSchedule = new AbstractAction("Calculate route by schedule") {
			private static final long serialVersionUID = 5071561074552411238L;

			@Override
			public void actionPerformed(ActionEvent e) {
				calcRoute(true);
			}
		};
		JMenu jmenu = new JMenu("Public Transport");
		jmenu.add(transportRoute);
		jmenu.add(transportRouteSchedule);
		menu.add(jmenu);

	}


	private void redrawRoute() {
		List<Way> ways = new ArrayList<Way>();
		TransportRouteResult r  = null;
		if (results.size() > currentRoute && currentRoute >= 0) {
			r = results.get(currentRoute);
		}
		calculateResult(ways, r);
		DataTileManager<Entity> points = new DataTileManager<>(11);
		int ind = -1;
		for (Way w : ways) {
			Way wl = new Way(ind--);
			Node node = w.getFirstNode();
			wl.addNode(node);
			long tile = points.evaluateTile(node.getLatitude(), node.getLongitude());
			for(int i = 1; i < w.getNodes().size(); i++) {
				Node nnode = w.getNodes().get(i);
				long ntile = points.evaluateTile(nnode.getLatitude(), nnode.getLongitude());
				wl.addNode(nnode);
				if(tile != ntile) {
					points.registerObject(wl.getFirstNode().getLatitude(), wl.getFirstNode().getLongitude(), wl);
					tile = ntile;
					wl = new Way(ind--);
					wl.addNode(nnode);
				}
			}
			if(wl.getNodes().size() > 0) {
				points.registerObject(wl.getFirstNode().getLatitude(), wl.getFirstNode().getLongitude(), wl);
			}
		}
		map.setPoints(points);
		
		nextRoute.setVisible(r != null);
		infoButton.setVisible(r != null);
		if (r != null) {
			String refs = "";
			List<TransportRouteResult> alternativeRoutes = r.getAlternativeRoutes();
			for(int i = 0; i < r.getSegments().size(); i++) {
				TransportRouteResultSegment res = r.getSegments().get(i);
				Set<String> segmentRefs = new LinkedHashSet<String>();
				segmentRefs.add(res.route.getRef());
				for(TransportRouteResult alt : alternativeRoutes) {
					TransportRouteResultSegment altSeg = alt.getSegments().get(i);
					segmentRefs.add(altSeg.route.getRef());
				}
				if(i > 0) {
					refs += ", ";
				}
				if (segmentRefs.size() > 1) {
					refs += segmentRefs.toString();
				} else {
					refs += res.route.getRef();
				}
			}
			infoButton.setText(String.format("%d. %.1f min (T %.1f min, W %.1f min): %s", currentRoute + 1,
					r.getRouteTime() / 60.0, r.getTravelTime() / 60.0, r.getWalkTime() / 60.0, refs));
		}
		prevRoute.setVisible(r != null);
		
		map.prepareImage();
	}

	private void calcRoute(boolean schedule) {
		new Thread() {
			@Override
			public void run() {
				start = DataExtractionSettings.getSettings().getStartLocation();
				end = DataExtractionSettings.getSettings().getEndLocation();
				buildRoute(schedule);
				redrawRoute();
			}

			
		}.start();
	}


	public void buildRoute(boolean schedule) {
		long time = System.currentTimeMillis();
		if (start != null && end != null) {
			try {
				BinaryMapIndexReader[] files = DataExtractionSettings.getSettings().getObfReaders();
				if (files.length == 0) {
					JOptionPane.showMessageDialog(OsmExtractionUI.MAIN_APP.getFrame(), "Please specify obf file in settings", "Obf file not found",
							JOptionPane.ERROR_MESSAGE);
					return;
				}
				System.out.println("Transport route from " + start + " to " + end);
				Builder builder = DataExtractionSettings.getSettings().getRoutingConfig();
				String m = DataExtractionSettings.getSettings().getRouteMode();
				String[] props = m.split("\\,");
				Map<String, String> paramsR = new LinkedHashMap<String, String>();
				for(String p : props) {
					if(p.contains("=")) {
						paramsR.put(p.split("=")[0], p.split("=")[1]);
					} else {
						paramsR.put(p, "true");
					}
				}
				GeneralRouter prouter = builder.getRouter("public_transport");
				TransportRoutingConfiguration cfg = new TransportRoutingConfiguration(prouter, paramsR);
				cfg.useSchedule = schedule;
				TransportRoutePlanner planner = new TransportRoutePlanner();

				TransportRoutingContext ctx = new TransportRoutingContext(cfg, 
						DataExtractionSettings.getSettings().useNativeRouting() ? NativeSwingRendering.getDefaultFromSettings() : null, files); 
				if (ctx.library != null) {
					NativeTransportRoutingResult[] nativeRes = ctx.library.runNativePTRouting(
							MapUtils.get31TileNumberX(start.getLongitude()),
							MapUtils.get31TileNumberY(start.getLatitude()),
							MapUtils.get31TileNumberX(end.getLongitude()),
							MapUtils.get31TileNumberY(end.getLatitude()),
							cfg, ctx.calculationProgress);
					if (nativeRes.length > 0) {						
						this.results = TransportRoutePlanner.convertToTransportRoutingResult(nativeRes, cfg);
					} else {
						System.out.println("No luck, empty result from Native");
					}
				} else {
					startProgressThread(ctx);
					this.results = planner.buildRoute(ctx, start, end);	
				}
				
				this.currentRoute = 0;
				throwExceptionIfRouteNotFound(ctx, results);
			} catch (Exception e) {
				ExceptionHandler.handle(e);
			} finally {
				infoButton.setVisible(false);
				prevRoute.setVisible(false);
				nextRoute.setVisible(false);
				if(map.getPoints() != null) {
					map.getPoints().clear();
				}
			}
			System.out.println("Finding self routes " + results.size() + " " + (System.currentTimeMillis() - time) + " ms");
		}
		return ;
	}

	

	private void throwExceptionIfRouteNotFound(TransportRoutingContext ctx, List<TransportRouteResult> res) {
		if(res.isEmpty()) {
			throw new IllegalArgumentException("There is no public transport route for selected start/stop.");
		}
	}


	private void startProgressThread(final TransportRoutingContext ctx) {
		new Thread() {
			@Override
			public void run() {
				while (ctx.calculationProgress != null && !ctx.calculationProgress.isCancelled) {
					float p = Math.max(ctx.calculationProgress.distanceFromBegin,
							ctx.calculationProgress.distanceFromEnd);
					float all = 1.25f * ctx.calculationProgress.totalEstimatedDistance;
					// while (p > all * 0.9) {
					// all *= 1.2;
					// }
					if (all > 0) {
						int t = (int) (p * p / (all * all) * 100.0f);
						// int t = (int) (p/all*100f);
						// System.out.println("Progress " + t + " % " +
						// ctx.calculationProgress.distanceFromBegin + " " + ctx.calculationProgress.distanceFromEnd+" "
						// + all);
					}
					try {
						sleep(100);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			};
		}.start();
	}

	private void calculateResult(List<Way> res, TransportRouteResult r) {
		if (r != null) {
			LatLon p = start; 
			for (TransportRouteResultSegment s : r.getSegments()) {
				LatLon floc = s.getStart().getLocation();
				if(DRAW_WALK_SEGMENTS) {
					addWalk(res, p, floc);
				}
				
				res.addAll(s.getGeometry());
				// String name = String.format("time %.2f ", s.getSegmentTime());
//				String name = String.format("%d st, %.1f m, %s", s.end - s.start, s.getTravelDist(), s.route.getName());
//				Way way = new Way(-1);
//				way.putTag(OSMTagKey.NAME.getValue(), name);
//				for (int i = s.start; i <= s.end; i++) {
//					LatLon l = s.getStop(i).getLocation();
//					Node n = new Node(l.getLatitude(), l.getLongitude(), -1);
//					way.addNode(n);
//				}
//				res.add(way);
				
				p = s.getEnd().getLocation();
			}
			if(DRAW_WALK_SEGMENTS) {
				addWalk(res, p, end);
			}

		}
	}


	private void addWalk(List<Way> res, LatLon s, LatLon e) {
		double dist = MapUtils.getDistance(s, e);
		if(dist > 50) {
			Way way = new Way(-1);
			way.putTag(OSMTagKey.NAME.getValue(), String.format("Walk %.1f m", dist));
			way.addNode(new Node(s.getLatitude(), s.getLongitude(), -1));
			way.addNode(new Node(e.getLatitude(), e.getLongitude(), -1));
			res.add(way);
		}
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

	@Override
	public void prepareToDraw() {
	}


	@Override
	public void paintLayer(Graphics2D g) {
		
		TransportRouteResult r  = null;
		if (results.size() > currentRoute && currentRoute >= 0) {
			r = results.get(currentRoute);
		}
		if(r != null){
			g.setColor(Color.blue);
			int rad = 10;
			for (TransportRouteResultSegment s : r.getSegments()) {
				LatLon l = s.getStart().getLocation();
				int x = map.getMapXForPoint(l.getLongitude());
				int y = map.getMapYForPoint(l.getLatitude());
				g.drawOval(x, y, rad, rad);
				g.fillOval(x, y, rad, rad);
			}
			rad = 9;
			g.setColor(Color.red);
			for (TransportRouteResultSegment s : r.getSegments()) {
				LatLon l = s.getEnd().getLocation();
				int x = map.getMapXForPoint(l.getLongitude());
				int y = map.getMapYForPoint(l.getLatitude());
				g.drawOval(x, y, rad, rad);
				g.fillOval(x, y, rad, rad);
			}
		}
		
	}


	@Override
	public void applySettings() {
	}

}
