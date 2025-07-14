package net.osmand.swing;

import net.osmand.MapCreatorVersion;
import net.osmand.NativeJavaRendering;
import net.osmand.NativeJavaRendering.MapDiff;
import net.osmand.NativeJavaRendering.RenderingImageContext;
import net.osmand.PlatformUtil;
import net.osmand.data.DataTileManager;
import net.osmand.data.LatLon;
import net.osmand.data.QuadRect;
import net.osmand.data.RotatedTileBox;
import net.osmand.map.IMapLocationListener;
import net.osmand.map.ITileSource;
import net.osmand.map.MapTileDownloader;
import net.osmand.map.MapTileDownloader.DownloadRequest;
import net.osmand.map.MapTileDownloader.IMapDownloaderCallback;
import net.osmand.map.TileSourceManager;
import net.osmand.map.TileSourceManager.TileSourceTemplate;
import net.osmand.osm.edit.Entity;
import net.osmand.osm.io.NetworkUtils;
import net.osmand.shared.gpx.GpxFile;
import net.osmand.shared.routing.RouteColorize.ColorizationType;
import net.osmand.swing.MapPanelSelector.MapSelectionArea;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;

import org.apache.commons.logging.Log;
import org.xmlpull.v1.XmlPullParser;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Component;
import java.awt.Container;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.Image;
import java.awt.KeyEventDispatcher;
import java.awt.KeyboardFocusManager;
import java.awt.Point;
import java.awt.Rectangle;
import java.awt.Toolkit;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.ComponentAdapter;
import java.awt.event.ComponentEvent;
import java.awt.event.KeyEvent;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.awt.event.MouseWheelEvent;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

import javax.imageio.IIOException;
import javax.imageio.ImageIO;
import javax.swing.AbstractAction;
import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.ButtonGroup;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JCheckBoxMenuItem;
import javax.swing.JFrame;
import javax.swing.JMenu;
import javax.swing.JMenuBar;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JPopupMenu;
import javax.swing.JRadioButton;
import javax.swing.JTextField;
import javax.swing.KeyStroke;
import javax.swing.UIManager;

public class MapPanel extends JPanel implements IMapDownloaderCallback {

	private static final long serialVersionUID = 1L;

	private static final int EXPAND_X = 16;
	private static final int EXPAND_Y = 16;

	protected static final Log log = PlatformUtil.getLog(MapPanel.class);
	public static final int divNonLoadedImage = 16;



	public static void main(String[] args) throws IOException {
		showMainWindow(512, 512, null);
	}


	public static MapPanel showMainWindow(int wx, int hy, NativeJavaRendering nativeLib) {
		JFrame frame = new JFrame(Messages.getString("MapPanel.MAP.VIEW")); //$NON-NLS-1$
	    try {
			UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
		} catch (Exception e) {
			e.printStackTrace();
		}

		final MapPanel panel = new MapPanel(DataExtractionSettings.getSettings().getTilesDirectory());
		panel.nativeLibRendering = nativeLib;
//		panel.longitude = longitude;
//		panel.latitude = latitude;
//		panel.zoom = zoom;
	    frame.addWindowListener(new WindowAdapter(){
	    	@Override
	    	public void windowClosing(WindowEvent e) {
	    		DataExtractionSettings.getSettings().saveLocation(panel.getLatitude(), panel.getLongitude(), panel.getZoom(), true);
	    		System.exit(0);
	    	}
	    });
	    Container content = frame.getContentPane();
	    content.add(panel, BorderLayout.CENTER);


	    JMenuBar bar = new JMenuBar();
	    bar.add(getMenuToChooseSource(panel));
	    frame.setJMenuBar(bar);
	    frame.setSize(wx, hy);
	    frame.setVisible(true);
	    return panel;
	}

	private File tilesLocation = null;

	// name of source map
	private ITileSource map = TileSourceManager.getMapnikSource();

	private NativeJavaRendering nativeLibRendering;
	private NativeRendererRunnable lastAddedRunnable;
	private Image nativeRenderingImg;
	private RenderingImageContext lastContext;
	private Rect nativeRect;
    private class Rect {
        int left31;
        int top31;
        int nativeZoom;
    }

	private ThreadPoolExecutor nativeRenderer = new ThreadPoolExecutor(1, 1, 30, TimeUnit.SECONDS,
			new ArrayBlockingQueue<Runnable>(1));

	// zoom level
	private int zoom = 1;
	private float mapDensity = 1;

	// degree measurements (-180, 180)
	private double longitude;
	// degree measurements (90, -90)
	private double latitude;

	private List<IMapLocationListener> listeners = new ArrayList<IMapLocationListener>();


	private List<MapPanelLayer> layers = new ArrayList<MapPanelLayer>();

	// cached data to draw image
	private Image[][] images;
	private int xStartingImage = 0;
	private int yStartingImage = 0;

	private MapTileDownloader downloader = MapTileDownloader.getInstance(MapCreatorVersion.APP_MAP_CREATOR_VERSION); // FIXME no commit
	Map<String, Image> cache = new ConcurrentHashMap<String, Image>();

	private final JPopupMenu popupMenu;
	private Point popupMenuPoint;
	private boolean willBePopupShown = false;

	private JTextField statusField;

	private JPanel diffButton;

	private JButton buttonOnlineRendering;

	private MapPanelSelector mapPanelSelector;

    private final MapDataPrinter printer = new MapDataPrinter(this, log);

	public MapPanel(File fileWithTiles) {
		mapPanelSelector = new MapPanelSelector(this);
		ImageIO.setUseCache(false);

		tilesLocation = fileWithTiles;
		loadSettingsLocation();
		if(map != null){
			if(zoom > map.getMaximumZoomSupported()){
				zoom = map.getMaximumZoomSupported();
			}
			if(zoom < map.getMinimumZoomSupported()){
				zoom = map.getMinimumZoomSupported();
			}
		}

		popupMenu = new JPopupMenu();
		downloader.addDownloaderCallback(this);
		setFocusable(true);
		addComponentListener(new ComponentAdapter(){
			@Override
			public void componentResized(ComponentEvent e) {
				prepareImage();
			}
		});
		KeyboardFocusManager kfm = KeyboardFocusManager.getCurrentKeyboardFocusManager();
		kfm.addKeyEventDispatcher(new KeyEventDispatcher() {

			@Override
			@SuppressWarnings("deprecation")
			public boolean dispatchKeyEvent(KeyEvent e) {
				KeyStroke key2 = KeyStroke.getKeyStroke(KeyEvent.VK_L, Toolkit.getDefaultToolkit().getMenuShortcutKeyMask());
				KeyStroke key1 = KeyStroke.getKeyStroke(KeyEvent.VK_P, Toolkit.getDefaultToolkit().getMenuShortcutKeyMask());
				KeyStroke keyStroke = KeyStroke.getKeyStrokeForEvent(e);
				if (keyStroke.equals(key2)) {
					if (statusField != null) {
						statusField.requestFocus();
					}
					return true;
				} else if (keyStroke.equals(key1)) {
					if (buttonOnlineRendering != null) {
						buttonOnlineRendering.doClick();
					}
					return true;
				}
				return false;
			}
		});
		setOpaque(false);
		MapMouseAdapter mouse = new MapMouseAdapter();
		addMouseListener(mouse);
		addMouseMotionListener(mouse);
		addMouseWheelListener(mouse);

		initDefaultLayers();
	}

	public void applySettings() {
		for (MapPanelLayer layer: layers) {
			layer.applySettings();
		}
	}

	public void loadSettingsLocation() {
		LatLon defaultLocation = DataExtractionSettings.getSettings().getDefaultLocation();
		latitude = defaultLocation.getLatitude();
		longitude = defaultLocation.getLongitude();
		zoom = DataExtractionSettings.getSettings().getDefaultZoom();
	}


	public void refresh() {
        printer.searchPOIs(false);

		prepareImage();
		fireMapLocationListeners();
	}

	public void setStatusField(JTextField statusField) {
		this.statusField = statusField;
	}

	private static Map<String, TileSourceTemplate> getCommonTemplates(File dir){
		final List<TileSourceTemplate> list = TileSourceManager.getKnownSourceTemplates();
		Map<String, TileSourceTemplate> map = new LinkedHashMap<String, TileSourceTemplate>();
		for(TileSourceTemplate t : list){
			map.put(t.getName(), t);
		}
		if (!dir.isDirectory()) {
			return map;
		}
		for(File f : dir.listFiles()){
			if(f.isDirectory()){
				if(map.containsKey(f.getName())){
					if(TileSourceManager.isTileSourceMetaInfoExist(f)){
						map.put(f.getName(), TileSourceManager.createTileSourceTemplate(f));
					} else {
						try {
							TileSourceManager.createMetaInfoFile(f, map.get(f.getName()), false);
						} catch (IOException e) {
						}
					}
				} else {
					map.put(f.getName(), TileSourceManager.createTileSourceTemplate(f));
				}

			}
		}
		return map;
	}

	public static JMenu getMenuToChooseSource(final MapPanel panel){
		final JMenu tiles = new JMenu(Messages.getString("MapPanel.SOURCE.OF.TILES")); //$NON-NLS-1$
		final JMenu downloadedMenu = new JMenu("Additional"); //$NON-NLS-1$
		final File tilesDirectory = DataExtractionSettings.getSettings().getTilesDirectory();
		Map<String, TileSourceTemplate> udf = getCommonTemplates(tilesDirectory);
		final List<TileSourceTemplate> downloaded = TileSourceManager.downloadTileSourceTemplates(MapCreatorVersion.APP_VERSION, false);
		final Map<TileSourceTemplate, JCheckBoxMenuItem> items = new IdentityHashMap<TileSourceTemplate, JCheckBoxMenuItem>();

		tiles.add(downloadedMenu);
		for(final TileSourceTemplate l : udf.values()){
			if(l == null){
				continue;
			}
			JCheckBoxMenuItem menuItem = new JCheckBoxMenuItem(l.getName());
			tiles.add(menuItem);
			items.put(l, menuItem);
			menuItem.addActionListener(new ActionListener() {
				@Override
				public void actionPerformed(ActionEvent e) {
					for (final Map.Entry<TileSourceTemplate, JCheckBoxMenuItem> es : items.entrySet()) {
						es.getValue().setSelected(l.equals(es.getKey()));
					}
					File dir = new File(tilesDirectory, l.getName());
					try {
						dir.mkdirs();
						TileSourceManager.createMetaInfoFile(dir, l, false);
					} catch (IOException e1) {
					}
					panel.setMapName(l);
				}
			});
		}

		if (downloaded != null) {
			for (final TileSourceTemplate l : downloaded) {
				if (l == null) {
					continue;
				}
				JCheckBoxMenuItem menuItem = new JCheckBoxMenuItem(l.getName());
				downloadedMenu.add(menuItem);
				items.put(l, menuItem);
				menuItem.addActionListener(new ActionListener() {
					@Override
					public void actionPerformed(ActionEvent e) {
						for (final Map.Entry<TileSourceTemplate, JCheckBoxMenuItem> es : items.entrySet()) {
							es.getValue().setSelected(l.equals(es.getKey()));
						}
						File dir = new File(tilesDirectory, l.getName());
						try {
							dir.mkdirs();
							TileSourceManager.createMetaInfoFile(dir, l, true);
						} catch (IOException e1) {
						}
						panel.setMapName(l);
					}
				});
			}
		}

		for (final Map.Entry<TileSourceTemplate, JCheckBoxMenuItem> em : items.entrySet()) {
			if(Algorithms.objectEquals(panel.getMap(), em.getKey())){
				em.getValue().setSelected(true);
			}
		}

		tiles.addSeparator();
		tiles.add(createNewTileSourceAction(panel, tiles, items));
		return tiles;
	}


	private static AbstractAction createNewTileSourceAction(final MapPanel panel, final JMenu tiles,
			final Map<TileSourceTemplate, JCheckBoxMenuItem> items) {
		return new AbstractAction(Messages.getString("MapPanel.NEW.TILE.SRC")){ //$NON-NLS-1$
			private static final long serialVersionUID = -8286622335859339130L;

			@Override
			public void actionPerformed(ActionEvent e) {
				NewTileSourceDialog dlg = new NewTileSourceDialog(panel);
				dlg.showDialog();
				final TileSourceTemplate l = dlg.getTileSourceTemplate();
				if(l != null){
					JCheckBoxMenuItem menuItem = new JCheckBoxMenuItem(l.getName());
					tiles.add(menuItem);
					items.put(l, menuItem);
					menuItem.addActionListener(new ActionListener() {
						@Override
						public void actionPerformed(ActionEvent e) {
							for (final Map.Entry<TileSourceTemplate, JCheckBoxMenuItem> es : items.entrySet()) {
								es.getValue().setSelected(l.equals(es.getKey()));
							}
							panel.setMapName(l);
						}
					});
					for (final Map.Entry<TileSourceTemplate, JCheckBoxMenuItem> es : items.entrySet()) {
						es.getValue().setSelected(l.equals(es.getKey()));
					}
					panel.setMapName(l);
				}
			}
		};
	}




	@Override
	public void setVisible(boolean flag) {
		super.setVisible(flag);
		if(!flag){
			downloader.removeDownloaderCallback(this);
 		} else {
 			downloader.addDownloaderCallback(this);
 		}
	}

	public double getXTile() {
		return MapUtils.getTileNumberX(zoom, longitude);
	}

	public double getYTile() {
		return MapUtils.getTileNumberY(zoom, latitude);
	}

	public double getTileSize() {
//		return (map == null ?  256 : map.getTileSize()) * mapDensity;
		return 256 * mapDensity;
	}

	public QuadRect getLatLonBBox() {
		double xTileLeft = getXTile() - getCenterPointX() / getTileSize();
		double xTileRight = getXTile() + getCenterPointX() / getTileSize();
		double yTileUp = getYTile() - getCenterPointY() / getTileSize();
		double yTileDown = getYTile() + getCenterPointY() / getTileSize();
		QuadRect qr = new QuadRect();
		qr.left = MapUtils.getLongitudeFromTile(zoom, xTileLeft);
		qr.top = MapUtils.getLatitudeFromTile(zoom, yTileUp);
		qr.right = MapUtils.getLongitudeFromTile(zoom, xTileRight);
		qr.bottom = MapUtils.getLatitudeFromTile(zoom, yTileDown);
		return qr;
	}

	public QuadRect getLatLonPoiBBox(int x, int y) {
		RotatedTileBox.RotatedTileBoxBuilder bld = new RotatedTileBox.RotatedTileBoxBuilder();
		RotatedTileBox rotatedTileBox = bld.setPixelDimensions(getWidth(), getHeight()).setLocation(latitude, longitude)
				.setZoom(zoom).build();
		rotatedTileBox.setMapDensity(mapDensity);
		int searchRadius = (int) ((rotatedTileBox.getDefaultRadiusPoi()) * 1.5);
		LatLon minLatLon = rotatedTileBox.getLatLonFromPixel(x - searchRadius, y - searchRadius);
		LatLon maxLatLon = rotatedTileBox.getLatLonFromPixel(x + searchRadius, y + searchRadius);
		return new QuadRect(minLatLon.getLongitude(), minLatLon.getLatitude(), maxLatLon.getLongitude(),
				maxLatLon.getLatitude());
	}

	public int getMapXForPoint(double longitude) {
		double tileX = MapUtils.getTileNumberX(zoom, longitude);
		return (int) ((tileX - getXTile()) * getTileSize() + getCenterPointX());
	}

	public double getCenterPointX() {
		return getWidth() / 2.0;
	}


	public int getMapYForPoint(double latitude) {
		double tileY = MapUtils.getTileNumberY(zoom, latitude);
		return (int) ((tileY - getYTile()) * getTileSize() + getCenterPointY());
	}

	public double getCenterPointY() {
		return getHeight() / 2.0;
	}

	public NativeJavaRendering getNativeLibrary() {
		return nativeLibRendering;
	}

	public void setNativeLibrary(NativeJavaRendering nl) {
		nativeLibRendering = nl;
		fullMapRedraw();
	}


	@Override
	protected void paintComponent(Graphics g) {
		if (nativeLibRendering != null) {
			if (nativeRect != null && zoom == nativeRect.nativeZoom) {
				double xTileLeft = getXTile() - getWidth() / (2.0d * getTileSize());
				double yTileUp = getYTile() - getHeight() / (2.0d * getTileSize());
				int shx = (int) (-xTileLeft * getTileSize()
						+ (nativeRect.left31) / (MapUtils.getPowZoom(31 - zoom - 8) / mapDensity));
				int shy = (int) (-yTileUp * getTileSize()
						+ (nativeRect.top31) / (MapUtils.getPowZoom(31 - zoom - 8) / mapDensity));
				g.drawImage(nativeRenderingImg, shx, shy, this);
			}
		} else if (images != null) {
			for (int i = 0; i < images.length; i++) {
				for (int j = 0; j < images[i].length; j++) {
					if (images[i][j] == null) {
						int div = divNonLoadedImage;
						double tileDiv = getTileSize() / div;
						for (int k1 = 0; k1 < div; k1++) {
							for (int k2 = 0; k2 < div; k2++) {
								if ((k1 + k2) % 2 == 0) {
									g.setColor(Color.gray);
								} else {
									g.setColor(Color.white);
								}
								g.fillRect((int)(i * getTileSize() + xStartingImage + k1 * tileDiv), (int)(j * getTileSize() + yStartingImage + k2
										* tileDiv), (int)tileDiv, (int) tileDiv);

							}
						}
					} else {
						g.drawImage(images[i][j],
								(int)(i * getTileSize() + xStartingImage),(int)( j * getTileSize() + yStartingImage),
								(int)getTileSize(), (int)getTileSize(), this);
					}
				}
			}
		}
		updateMapDiffs((Graphics2D) g, false);
		for(MapPanelLayer l : layers){
			l.paintLayer((Graphics2D) g);
		}

		if(getSelectionArea().isVisible()){
			g.setColor(new Color(0, 0, 230, 50));
			Rectangle r = getSelectionArea().getSelectedArea();
			g.fillRect(r.x, r.y, r.width, r.height);
		}
		super.paintComponent(g);
		if (statusField != null) {
			statusField.setText("http://www.openstreetmap.org/#map=" + zoom + "/" + ((float) latitude) + "/"
							+ ((float) longitude));

		}
	}

	private void updateMapDiffs(final Graphics2D g, boolean delete) {
		final MapDiff md = nativeLibRendering != null ? nativeLibRendering.getMapDiffs(latitude, longitude) : null;
		if (md == null || zoom < 13 || delete) {
			if (diffButton != null) {
				remove(diffButton);
				diffButton = null;
			}
		} else {
			if(diffButton == null) {
				diffButton = new JPanel(); //$NON-NLS-1$
				diffButton.setLayout(new BoxLayout(diffButton, BoxLayout.X_AXIS));
				diffButton.setOpaque(false);
				diffButton.add(Box.createHorizontalGlue());

				JPanel vertLayout = new JPanel(); //$NON-NLS-1$
				vertLayout.setLayout(new BoxLayout(vertLayout, BoxLayout.Y_AXIS));
				vertLayout.setOpaque(false);
				JCheckBox ch = new JCheckBox(md.baseName);
				ch.setSelected(md.enableBaseMap);
				ch.addActionListener(new ActionListener() {
					@Override
					public void actionPerformed(ActionEvent e) {
						nativeLibRendering.enableBaseFile(md, !md.enableBaseMap);
						fullMapRedraw();
					}
				});
				ch.setAlignmentX(Component.RIGHT_ALIGNMENT);
				vertLayout.add(ch);
				JButton downloadDiffs = new JButton("Download live");
				downloadDiffs.addActionListener(new ActionListener() {
					@Override
					public void actionPerformed(ActionEvent e) {
						downloadDiffs(g, md);
					}
				});
				downloadDiffs.setAlignmentX(Component.RIGHT_ALIGNMENT);
				vertLayout.add(downloadDiffs);
				if (md.diffs.size() > 0) {
					ButtonGroup bG = new ButtonGroup();
					addButton(bG, md.baseName, vertLayout, md, "base");
					for (String s : md.diffs.keySet()) {
						addButton(bG, s, vertLayout, md, null);
					}
				}
				diffButton.add(vertLayout);
				add(diffButton);
				revalidate();
			}
		}
	}


	protected void downloadDiffs(Graphics2D g, MapDiff md) {
		try {
			String url = "https://download.osmand.net/check_live?aosmc=true&timestamp=" + md.timestamp +
					"&file=" + URLEncoder.encode(md.baseName, StandardCharsets.UTF_8.toString());
			System.out.println("Loading " + url);
			HttpURLConnection conn = NetworkUtils.getHttpURLConnection(url);
			XmlPullParser parser = PlatformUtil.newXMLPullParser();
			parser.setInput(conn.getInputStream(), "UTF-8");
			Map<String, Long> updateFiles = new TreeMap<String, Long>();
			while (parser.next() != XmlPullParser.END_DOCUMENT) {
				if (parser.getEventType() == XmlPullParser.START_TAG) {
					if (parser.getName().equals("update")) {
						if (!parser.getAttributeValue("", "name").endsWith("00.obf.gz")) {
							updateFiles.put(parser.getAttributeValue("", "name"),
									Long.parseLong(parser.getAttributeValue("", "timestamp")));
						}
					}
				}
			}
			conn.disconnect();
			File dir = new File(DataExtractionSettings.getSettings().getBinaryFilesDir());
			for(String file : updateFiles.keySet()) {
				long time = updateFiles.get(file);
				File targetFile = new File(dir, file.substring(0, file.length() - 3));
				if(!targetFile.exists() || targetFile.lastModified() != time) {
					String nurl = "https://download.osmand.net/download?aosmc=yes&" + md.timestamp + "&file=" + URLEncoder.encode(file, StandardCharsets.UTF_8.toString());
					System.out.println("Loading " + nurl);
					HttpURLConnection c = NetworkUtils.getHttpURLConnection(nurl);
					GZIPInputStream gzip = new GZIPInputStream(c.getInputStream());
					FileOutputStream fout = new FileOutputStream(targetFile);
					Algorithms.streamCopy(gzip, fout);
					gzip.close();
					fout.close();
					targetFile.setLastModified(time);
					nativeLibRendering.closeMapFile(targetFile.getAbsolutePath());
					nativeLibRendering.initMapFile(targetFile.getAbsolutePath(), true);
				}
			}
			updateMapDiffs(g, true);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}


	private void addButton(ButtonGroup bG, final String s, JPanel parent, final MapDiff md, String name) {
		boolean selected = s.equals(md.selected);
		JRadioButton m = new JRadioButton(name == null ? s.replace('_', ' ') : name);
		bG.add(m);
		m.setAlignmentX(Component.RIGHT_ALIGNMENT);
		parent.add(m);
		m.addActionListener(new ActionListener() {

			@Override
			public void actionPerformed(ActionEvent e) {
				nativeLibRendering.enableMapFile(md, s);
				fullMapRedraw();
			}

		});
		m.setSelected(selected);
	}

	public void fullMapRedraw() {
		lastAddedRunnable = null;
		prepareImage();
		repaint();
	}

	public File getTilesLocation() {
		return tilesLocation;
	}

	public void setTilesLocation(File tilesLocation) {
		this.tilesLocation = tilesLocation;
		cache.clear();
		prepareImage();
	}

	public String getFileForImage (int x, int y, int zoom, String ext){
		return map.getName() +"/"+zoom+"/"+(x) +"/"+y+ext+".tile"; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
	}

	public Image getImageFor(int x, int y, int zoom, boolean loadIfNeeded) throws IOException {
		if (map == null) {
			return null;
		}
		long pz = (long) MapUtils.getPowZoom(zoom );
		while (x < 0) {
			x += pz;
		}
		while (x >= pz) {
			x -= pz;
		}
		while (y < 0) {
			y += pz;
		}
		while (y >= pz) {
			y -= pz;
		}
		String file = getFileForImage(x, y, zoom, map.getTileFormat());
		if (cache.get(file) == null) {
			File en = new File(tilesLocation, file);
			if (cache.size() > 100) {
				ArrayList<String> list = new ArrayList<String>(cache.keySet());
				for (int i = 0; i < list.size(); i += 2) {
					Image remove = cache.remove(list.get(i));
					if (remove != null) {
						remove.flush();
					}
				}
				if (log.isInfoEnabled()) {
					log.info("Before running gc on map tiles. Total Memory : " + (Runtime.getRuntime().totalMemory() >> 20) + " Mb. Used memory : " //$NON-NLS-1$ //$NON-NLS-2$
							+ ((Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) >> 20) + " Mb"); //$NON-NLS-1$
				}
				System.gc();
				if (log.isInfoEnabled()) {
					log.info("After running gc on map tiles. Total Memory : " + (Runtime.getRuntime().totalMemory() >> 20) + " Mb. Used memory : " //$NON-NLS-1$ //$NON-NLS-2$
							+ ((Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) >> 20) + " Mb"); //$NON-NLS-1$
				}
			}
			if (!downloader.isFileCurrentlyDownloaded(en)) {
				if (en.exists()) {
					// long time = System.currentTimeMillis();
					try {
						cache.put(file, ImageIO.read(en));
						// if (log.isDebugEnabled()) {
						// log.debug("Loaded file : " + file + " " + (System.currentTimeMillis() - time) + " ms");
						// }
					} catch (IIOException e) {
						log.error("Eror reading png " + x + " " + y + " zoom : " + zoom, e); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
					}
				}
				if (loadIfNeeded && cache.get(file) == null) {
					String urlToLoad = map.getUrlToLoad(x, y, zoom);
					if (urlToLoad != null) {
						downloader.requestToDownload(new DownloadRequest(urlToLoad, en, null, x, y, zoom));
					}
				}
			}
		}

		return cache.get(file);
	}

	@Override
	public void tileDownloaded(DownloadRequest request) {
		if (request == null) {
			prepareRasterImage(false);
			return;
		}
		double tileSize = getTileSize();
		double xTileLeft = getXTile() - getSize().width / (2.0d * tileSize);
		double yTileUp = getYTile() - getSize().height / (2.0d * tileSize);
		int i = request.xTile - (int) xTileLeft;
		int j = request.yTile - (int) yTileUp;
		if (request.zoom == this.zoom && (i >= 0 && i < images.length) && (j >= 0 && j < images[i].length)) {
			try {
				images[i][j] = getImageFor(request.xTile, request.yTile, zoom, false);
				repaint();
			} catch (IOException e) {
				log.error("Eror reading png " + request.xTile + " " + request.yTile + " zoom : " + zoom, e); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
			}

		}
	}

	public void prepareImage(){
		if(nativeLibRendering != null) {
			prepareNativeImage();
		} else {
			prepareRasterImage(DataExtractionSettings.getSettings().useInternetToLoadImages());
		}
	}

	private synchronized void prepareNativeImage() {
		NativeRendererRunnable runnable = new NativeRendererRunnable(getWidth(), getHeight());
		if(lastAddedRunnable == null || !lastAddedRunnable.contains(runnable)) {
			lastAddedRunnable = runnable;
			nativeRenderer.getQueue().clear();
			nativeRenderer.execute(runnable);
		}
		for(MapPanelLayer l : layers){
			l.prepareToDraw();
		}
		repaint();
	}


	private void prepareRasterImage(boolean loadNecessaryImages){
		try {
			double tileSize = getTileSize();
			if (images != null) {
				for (int i = 0; i < images.length; i++) {
					for (int j = 0; j < images[i].length; j++) {
						// dispose
					}
				}
			}
			double xTileLeft = getXTile() - getCenterPointX() / tileSize;
			double xTileRight = getXTile() + getCenterPointX() / tileSize;
			double yTileUp = getYTile() - getCenterPointY() / tileSize;
			double yTileDown = getYTile() + getCenterPointY() / tileSize;
			int ixTileLeft = (int) Math.floor(xTileLeft);
			int iyTileUp = (int) Math.floor(yTileUp);
			int ixTileRight = (int) Math.ceil(xTileRight);
			int iyTileDown = (int) Math.ceil(yTileDown);

			xStartingImage = -(int) ((xTileLeft - ixTileLeft) * tileSize);
			yStartingImage = -(int) ((yTileUp - iyTileUp) * tileSize);
			if (loadNecessaryImages) {
				downloader.refuseAllPreviousRequests();
			}
			int tileXCount = ixTileRight - ixTileLeft;
			int tileYCount = iyTileDown - iyTileUp;
			images = new BufferedImage[tileXCount][tileYCount];
			for (int i = 0; i < images.length; i++) {
				for (int j = 0; j < images[i].length; j++) {
					int x = ixTileLeft + i;
					int y = iyTileUp + j;
					images[i][j] = getImageFor(x, y, zoom, loadNecessaryImages);
				}
			}

			for (MapPanelLayer l : layers) {
				l.prepareToDraw();
			}
			repaint();
		} catch (IOException e) {
			log.error("Eror reading png preparing images"); //$NON-NLS-1$
		}
	}


	public int getMaximumZoomSupported(){
		if(nativeLibRendering != null) {
			return 21;
		}
		if (map == null) {
			return 18;
		}
		return map.getMaximumZoomSupported();
	}

	public int getMinimumZoomSupported(){
		if(nativeLibRendering != null || map == null) {
			return 1;
		}
		return map.getMinimumZoomSupported();
	}

	public void setMapDensity(float mapDensity) {
		this.mapDensity = mapDensity;
		prepareImage();
	}

	public void setZoom(int zoom){
		if(map != null && (zoom > getMaximumZoomSupported() || zoom < getMinimumZoomSupported())){
			return;
		}
		this.zoom = zoom;
		refresh();
	}

	public void setLatLon(double latitude, double longitude){
		this.latitude = latitude;
		this.longitude = longitude;
		refresh();
		DataExtractionSettings.getSettings().saveLocation(latitude, longitude, zoom, false);
	}

	public double getLatitude() {
		return latitude;
	}

	public double getLongitude() {
		return longitude;
	}

	public int getZoom() {
		return zoom;
	}

	public float getMapDensity() {
		return mapDensity;
	}

	public MapSelectionArea getSelectionArea() {
		return mapPanelSelector.getSelectionArea();
	}

	public ITileSource getMap(){
		return map;
	}

	public void setMapName(ITileSource map){
		if(!map.couldBeDownloadedFromInternet()){
			JOptionPane.showMessageDialog(this, "That map is not downloadable from internet");
		}
		this.map = map;
		if(map.getMaximumZoomSupported() < this.zoom){
			zoom = map.getMaximumZoomSupported();
		}
		if(map.getMinimumZoomSupported() > this.zoom){
			zoom = map.getMinimumZoomSupported();
		}
		prepareImage();
	}

	public void addMapLocationListener(IMapLocationListener l){
		listeners.add(l);
	}

	public void removeMapLocationListener(IMapLocationListener l){
		listeners.remove(l);
	}


	protected void fireMapLocationListeners(){
		for(IMapLocationListener l : listeners){
			l.locationChanged(latitude, longitude, null);
		}
	}

	public List<MapPanelLayer> getLayers() {
		return layers;
	}

	protected void initDefaultLayers() {
		MapInformationLayer mapInformationLayer = new MapInformationLayer();
		MapRouterLayer mapRouterLayer = new MapRouterLayer();
		addLayer(mapInformationLayer);
		addLayer(mapRouterLayer);
		addLayer(new MapTransportLayer());
		addLayer(new MapPointsLayer());
		addLayer(new MapAddressLayer());
		addLayer(new MapClusterLayer());
//		addLayer(new CoastlinesLayer());
		mapInformationLayer.addSetStartActionListener(mapRouterLayer.setStartActionListener);
		mapInformationLayer.addSetEndActionListener(mapRouterLayer.setEndActionListener);
		fillPopupActions();
	}

	public void fillPopupActions() {
		getPopupMenu().removeAll();
		for (MapPanelLayer l : layers) {
			l.fillPopupMenuWithActions(getPopupMenu());
		}
	}

	public void addLayer(MapPanelLayer l){
		l.initLayer(this);
		layers.add(l);
	}

	public void addLayer(int ind, MapPanelLayer l){
		l.initLayer(this);
		layers.add(ind, l);
	}

	public boolean removeLayer(MapPanelLayer l){
		return layers.remove(l);
	}

	@SuppressWarnings("unchecked")
	public <T extends MapPanelLayer> T getLayer(Class<T> cl){
		for(MapPanelLayer l : layers){
			if(cl.isInstance(l)){
				return (T) l;
			}
		}
		return null;
	}

	@Override
	protected void processKeyEvent(KeyEvent e) {
		boolean processed = false;
		if (e.getID() == KeyEvent.KEY_RELEASED) {
			if (e.getKeyCode() == 37) {
				// LEFT button
				longitude = MapUtils.getLongitudeFromTile(zoom + Math.log(mapDensity) / Math.log(2), getXTile()-0.5);
				processed = true;
			} else if (e.getKeyCode() == 39) {
				// RIGHT button
				longitude = MapUtils.getLongitudeFromTile(zoom + Math.log(mapDensity) / Math.log(2), getXTile()+0.5);
				processed = true;
			} else if (e.getKeyCode() == 38) {
				// UP button
				latitude = MapUtils.getLatitudeFromTile((float) (zoom + Math.log(mapDensity) / Math.log(2)), getYTile()-0.5);
				processed = true;
			} else if (e.getKeyCode() == 40) {
				// DOWN button
				latitude = MapUtils.getLatitudeFromTile((float) (zoom + Math.log(mapDensity) / Math.log(2)), getYTile()+0.5);
				processed = true;
			}
		}
		if(e.getID() == KeyEvent.KEY_TYPED){
			if(e.getKeyChar() == '+' || e.getKeyChar() == '=' ){
				if(zoom < getMaximumZoomSupported()){
					zoom ++;
					processed = true;
				}
			} else if(e.getKeyChar() == '-'){
				if(zoom > getMinimumZoomSupported()){
					zoom --;
					processed = true;
				}
			}
		}

		if(processed){
			e.consume();
			refresh();
		}
		super.processKeyEvent(e);
	}

	public DataTileManager<Entity> getPoints() {
		return getLayer(MapPointsLayer.class).getPoints();
	}

	public void setPoints(DataTileManager<Entity> points) {
		getLayer(MapPointsLayer.class).setPoints(points);
		prepareImage();
	}

	public void setColorizationType(GpxFile gpxFile, ColorizationType colorizationType, boolean grey) {
		getLayer(MapPointsLayer.class).setColorizationType(gpxFile, colorizationType, grey);
		repaint();
	}

	public Point getPopupMenuPoint() {
		return popupMenuPoint;
	}

	public JPopupMenu getPopupMenu() {
		return popupMenu;
	}




	public class MapMouseAdapter extends MouseAdapter {
		private Point startDragging = null;
		private Point startSelecting = null;

		@Override
		public void mouseClicked(MouseEvent e) {
			requestFocus();

            printer.searchAndPrintObjects(e);
			printer.clearPOIs();
        }

		public void dragTo(Point p){
			double dx = (startDragging.x - (double) p.x) / getTileSize();
			double dy = (startDragging.y - (double) p.y) / getTileSize();

			double lat = MapUtils.getLatitudeFromTile(zoom, getYTile() + dy);
			double lon = MapUtils.getLongitudeFromTile(zoom, getXTile() + dx);
			setLatLon(lat, lon);
		}

		@Override
		public void mouseDragged(MouseEvent e) {
			willBePopupShown = false;
			if(startDragging != null){
				if(Math.abs(e.getPoint().x - startDragging.x) +  Math.abs(e.getPoint().y - startDragging.y) >= 8){
					dragTo(e.getPoint());
					startDragging = e.getPoint();
				}
			}
			if(startSelecting != null){
				getSelectionArea().setSelectedArea(startSelecting.x, startSelecting.y, e.getPoint().x, e.getPoint().y);
				updateUI();
			}
		}

		@Override
		public void mouseWheelMoved(MouseWheelEvent e) {
			double dy = e.getPoint().y - getCenterPointY();
			double dx = e.getPoint().x - getCenterPointX();
			double lat = MapUtils.getLatitudeFromTile(zoom, getYTile() + dy / getTileSize());
			double lon = MapUtils.getLongitudeFromTile(zoom, getXTile() + dx / getTileSize());
			setLatLon(lat, lon);
			if(e.getWheelRotation() < 0){
				setZoom(getZoom() + 1);
			} else if(e.getWheelRotation() > 0) {
				setZoom(getZoom() - 1);
			}
			lat = MapUtils.getLatitudeFromTile(zoom, getYTile() - dy / getTileSize());
			lon = MapUtils.getLongitudeFromTile(zoom, getXTile() - dx / getTileSize());
			setLatLon(lat, lon);
			super.mouseWheelMoved(e);
		}
		@Override
		public void mousePressed(MouseEvent e) {
			if(e.getButton() == MouseEvent.BUTTON3){
				if(startDragging == null){
					startDragging  = e.getPoint();
				}
			} else if(e.getButton() == MouseEvent.BUTTON1){
				startSelecting = e.getPoint();
			}
			willBePopupShown = true;
		}

		@Override
		public void mouseReleased(MouseEvent e) {
			if(e.getButton() == MouseEvent.BUTTON3){
				if(startDragging != null){
					dragTo(e.getPoint());
					fireMapLocationListeners();
					startDragging = null;
				}
			}
			if(e.getButton() == MouseEvent.BUTTON1){
				if(startSelecting != null){
					getSelectionArea().setSelectedArea(startSelecting.x, startSelecting.y, e.getPoint().x, e.getPoint().y);
					startSelecting = null;
				}
				if (getSelectionArea().getSelectedArea().getWidth() < 4
						&& getSelectionArea().getSelectedArea().getHeight() < 4) {
					if (lastContext != null && zoom == lastContext.zoom) {
						mapPanelSelector.select(lastContext, e);
					}
				}
			}

			// possible bug if popup neither button1|| button3
			if (willBePopupShown && (e.isPopupTrigger() || e.getButton() == MouseEvent.BUTTON3)) {
				popupMenuPoint = new Point(e.getX(), e.getY());
				popupMenu.show(MapPanel.this, e.getX(), e.getY());
				willBePopupShown = false;
			}
			super.mouseReleased(e);
		}

	}


	class NativeRendererRunnable implements Runnable {
		int sleft;
		int sright;
		int stop;
		int sbottom;
		int oright;
		int oleft;
		int otop;
		int obottom;
		int z;
		final int cf;

		public NativeRendererRunnable(int w, int h) {
			LatLon latLon = new LatLon(latitude, longitude);
			this.z = zoom;
			cf = (int) (MapUtils.getPowZoom(31 - z - 8) / mapDensity);
			int minTile = 1;//1000;
			int mxTile = (1 << 31) - 1;// (1<<26);
			oleft = MapUtils.get31TileNumberX(latLon.getLongitude()) - (w / 2) * cf;
			oright = MapUtils.get31TileNumberX(latLon.getLongitude()) + (w / 2) * cf;
			otop = MapUtils.get31TileNumberY(latLon.getLatitude()) - (h / 2) * cf;
			obottom = MapUtils.get31TileNumberY(latLon.getLatitude()) + (h / 2) * cf;
			sleft = oleft - EXPAND_X * cf;
			sright = oright + EXPAND_X * cf;
			stop = otop - EXPAND_Y * cf;
			sbottom = obottom + EXPAND_Y * cf;
			if (sleft < minTile) {
				sleft = minTile;
			}
			if (sright > mxTile || sright < 0) {
				sright = mxTile;
			}
			if (stop < minTile) {
				stop = minTile;
			}
			if (sbottom > mxTile || sbottom < 0) {
				sbottom = mxTile;
			}
		}

		public boolean contains(NativeRendererRunnable r) {
			if(r.oright > sright ||
					r.oleft < sleft ||
				r.otop < stop ||
				r.obottom > sbottom ) {
				return false;
			}
			if(r.z != z){
				return false;
			}
			return true;
		}

		@Override
		public void run() {
			if (nativeRenderer.getQueue().isEmpty()) {
				try {
					lastContext = new RenderingImageContext(sleft, sright, stop, sbottom, zoom);
					NativeJavaRendering.RenderingImageResult result = nativeLibRendering.renderImage(lastContext);
					nativeRenderingImg = result.getImage();
					Rect rect = new Rect();
					rect.left31 = sleft;
					rect.top31 = stop;
					rect.nativeZoom = z;
					nativeRect = rect;
				} catch (Exception e) {
					log.error(e.getMessage(), e);
					e.printStackTrace();
				}
				repaint();
			}
		}
	}

	public void setOnlineRendering(JButton buttonOnlineRendering) {
		this.buttonOnlineRendering = buttonOnlineRendering;
	}

    public MapDataPrinter getPrinter() {
        return printer;
    }
}
