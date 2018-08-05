package net.osmand.swing;


import java.awt.Component;
import java.awt.Graphics2D;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;
import java.io.RandomAccessFile;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

import javax.swing.AbstractAction;
import javax.swing.Action;
import javax.swing.JButton;
import javax.swing.JOptionPane;
import javax.swing.JPopupMenu;
import javax.swing.SwingUtilities;

import net.osmand.PlatformUtil;
import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.data.DataTileManager;
import net.osmand.data.LatLon;
import net.osmand.osm.edit.Node;
import net.osmand.osm.edit.OSMSettings.OSMTagKey;
import net.osmand.osm.edit.Way;
import net.osmand.router.TransportRoutePlanner;
import net.osmand.router.TransportRoutePlanner.TransportRouteResult;
import net.osmand.router.TransportRoutePlanner.TransportRouteResultSegment;
import net.osmand.router.TransportRoutePlanner.TransportRoutingContext;
import net.osmand.router.TransportRoutingConfiguration;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;

import org.apache.commons.logging.Log;



public class MapTransportLayer implements MapPanelLayer {

	private final static Log log = PlatformUtil.getLog(MapTransportLayer.class);

	private MapPanel map;
	private JButton prevRoute;
	private JButton infoButton;
	private JButton nextRoute;

	private List<TransportRouteResult> results = new ArrayList<TransportRouteResult>();

	@Override
	public void destroyLayer() {

	}

	
	@Override
	public void initLayer(MapPanel map) {
		this.map = map;
		fillPopupMenuWithActions(map.getPopupMenu());
		

		prevRoute = new JButton("<<"); //$NON-NLS-1$
		prevRoute.addActionListener(new ActionListener(){
			@Override
			public void actionPerformed(ActionEvent e) {
			}
		});
		prevRoute.setVisible(false);
		prevRoute.setAlignmentY(Component.TOP_ALIGNMENT);
		map.add(prevRoute, 0);
		infoButton = new JButton("Route"); //$NON-NLS-1$
		infoButton.addActionListener(new ActionListener(){
			@Override
			public void actionPerformed(ActionEvent e) {
			}
		});
		infoButton.setVisible(false);
		infoButton.setAlignmentY(Component.TOP_ALIGNMENT);
		map.add(infoButton, 0);
		nextRoute = new JButton(">>"); //$NON-NLS-1$
		nextRoute.addActionListener(new ActionListener(){
			@Override
			public void actionPerformed(ActionEvent e) {
			}
		});
		nextRoute.setVisible(false);
		nextRoute.setAlignmentY(Component.TOP_ALIGNMENT);
		map.add(nextRoute);
	}

	public void fillPopupMenuWithActions(JPopupMenu menu) {
		Action selfRoute = new AbstractAction("Calculate transport route") {
			private static final long serialVersionUID = 5071561074552411238L;

			@Override
			public void actionPerformed(ActionEvent e) {
				calcRoute();
			}
		};
		menu.add(selfRoute);

	}



	private void calcRoute() {
		new Thread() {
			@Override
			public void run() {
				LatLon startRoute = DataExtractionSettings.getSettings().getStartLocation();
				LatLon endRoute = DataExtractionSettings.getSettings().getEndLocation();
				List<Way> ways = selfRoute(startRoute, endRoute);
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


	public List<Way> selfRoute(LatLon start, LatLon end) {
		List<Way> res = new ArrayList<Way>();
		long time = System.currentTimeMillis();
		List<File> files = new ArrayList<File>();
		for (File f : Algorithms.getSortedFilesVersions(new File(DataExtractionSettings.getSettings().getBinaryFilesDir()))) {
			if(f.getName().endsWith(".obf")){
				files.add(f);
			}
		}
		if(files.isEmpty()){
			JOptionPane.showMessageDialog(OsmExtractionUI.MAIN_APP.getFrame(), "Please specify obf file in settings", "Obf file not found",
					JOptionPane.ERROR_MESSAGE);
			return null;
		}
		System.out.println("Transport route from " + start + " to " + end);
		if (start != null && end != null) {
			try {
				BinaryMapIndexReader[] rs = new BinaryMapIndexReader[files.size()];
				int it = 0;
				for (File f : files) {
					RandomAccessFile raf = new RandomAccessFile(f, "r"); //$NON-NLS-1$ //$NON-NLS-2$
					rs[it++] = new BinaryMapIndexReader(raf, f);
				}
				TransportRoutePlanner planner = new TransportRoutePlanner();
				TransportRoutingConfiguration cfg = new TransportRoutingConfiguration();
//				cfg.maxNumberOfChanges = 2;
				TransportRoutingContext ctx = new TransportRoutingContext(cfg, rs);
				
//				String m = DataExtractionSettings.getSettings().getRouteMode();
//				String[] props = m.split("\\,");
//				RoutePlannerFrontEnd router = new RoutePlannerFrontEnd();
//				Map<String, String> paramsR = new LinkedHashMap<String, String>();
//				for(String p : props) {
//					if(p.contains("=")) {
//						paramsR.put(p.split("=")[0], p.split("=")[1]);
//					} else {
//						paramsR.put(p, "true");
//					}
//				}
//				RoutingConfiguration config = DataExtractionSettings.getSettings().getRoutingConfig().build(props[0],
//						/*RoutingConfiguration.DEFAULT_MEMORY_LIMIT*/ 1000, paramsR);
				startProgressThread(ctx);
				List<TransportRouteResult> results = planner.buildRoute(ctx, start, end);
				throwExceptionIfRouteNotFound(ctx, results);
				calculateResult(res, results, start, end);
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
			System.out.println("Finding self routes " + res.size() + " " + (System.currentTimeMillis() - time) + " ms");
		}
		return res;
	}

	

	private void throwExceptionIfRouteNotFound(TransportRoutingContext ctx, List<TransportRouteResult> results2) {
		// TODO Auto-generated method stub
		
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

	private void calculateResult(List<Way> res, List<TransportRouteResult> results, LatLon start, LatLon end) {
		if (results.size() > 0) {
			TransportRouteResult r = results.get(0);
			LatLon p = start; 
			for (TransportRouteResultSegment s : r.getSegments()) {
				LatLon floc = s.getStart().getLocation();
				addWalk(res, p, floc);
				Way way = new Way(-1);
				// String name = String.format("time %.2f ", s.getSegmentTime());
				String name = String.format("%d st, %.1f m, %s", s.end - s.start, s.getTravelDist(), s.route.getName());
				way.putTag(OSMTagKey.NAME.getValue(), name);
				for (int i = s.start; i <= s.end; i++) {
					LatLon l = s.getStop(i).getLocation();
					Node n = new Node(l.getLatitude(), l.getLongitude(), -1);
					way.addNode(n);
					p = l;
				}
				res.add(way);
			}
			addWalk(res, p, end);

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
//		g.setColor(Color.green);
//		if(startRoute != null){
//			int x = map.getMapXForPoint(startRoute.getLongitude());
//			int y = map.getMapYForPoint(startRoute.getLatitude());
//			g.drawOval(x, y, 12, 12);
//			g.fillOval(x, y, 12, 12);
//		}
		
	}


	@Override
	public void applySettings() {
	}

}
