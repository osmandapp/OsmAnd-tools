package net.osmand.swing;

import java.awt.Frame;
import java.awt.Point;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.KeyEvent;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.awt.event.MouseWheelEvent;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import javax.swing.JTextField;

import net.osmand.PlatformUtil;
import net.osmand.core.jni.AreaI;
import net.osmand.core.jni.AtlasMapRendererConfiguration;
import net.osmand.core.jni.CoreResourcesEmbeddedBundle;
import net.osmand.core.jni.IMapRenderer;
import net.osmand.core.jni.MapObjectsSymbolsProvider;
import net.osmand.core.jni.MapPresentationEnvironment;
import net.osmand.core.jni.MapPrimitivesProvider;
import net.osmand.core.jni.MapPrimitiviser;
import net.osmand.core.jni.MapRasterLayerProvider_Software;
import net.osmand.core.jni.MapRendererClass;
import net.osmand.core.jni.MapRendererDebugSettings;
import net.osmand.core.jni.MapRendererSetupOptions;
import net.osmand.core.jni.MapStylesCollection;
import net.osmand.core.jni.ObfMapObjectsProvider;
import net.osmand.core.jni.ObfsCollection;
import net.osmand.core.jni.OsmAndCore;
import net.osmand.core.jni.PointI;
import net.osmand.core.jni.QStringStringHash;
import net.osmand.core.jni.ResolvedMapStyle;
import net.osmand.core.jni.QIODeviceLogSink;
import net.osmand.core.jni.Logger;
import net.osmand.data.LatLon;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;

import com.jogamp.opengl.GLAutoDrawable;
import com.jogamp.opengl.GLEventListener;
import com.jogamp.opengl.util.Animator;
import org.xmlpull.v1.XmlPullParser;
import org.xmlpull.v1.XmlPullParserException;

public class QtCorePanel implements GLEventListener {


	private static CoreResourcesEmbeddedBundle coreResourcesEmbeddedBundle;
	public static Boolean loaded = null;
	private static String OS = System.getProperty("os.name").toLowerCase();

	public static boolean isWindows() {
		return (OS.indexOf("win") >= 0);
	}

	public static boolean isMac() {
		return (OS.indexOf("mac") >= 0);
	}

	public static boolean isUnix() {
		return (OS.indexOf("nix") >= 0 || OS.indexOf("nux") >= 0 || OS.indexOf("aix") > 0);
	}

	public static void loadNative(String folder) {
		if (loaded == null) {
			try {

				System.load(folder + "/" + System.mapLibraryName("OsmAndCoreWithJNI_standalone"));
				coreResourcesEmbeddedBundle = CoreResourcesEmbeddedBundle.loadFromLibrary(folder + "/"
						+ System.mapLibraryName("OsmAndCore_ResourcesBundle_shared"));
				OsmAndCore.InitializeCore(coreResourcesEmbeddedBundle);
				loaded = true;
			} catch (Throwable e) {
				System.err.println("Failed to load OsmAndCoreWithJNI:" + e);
				System.exit(0);
				loaded = false;
			}

		}

	}


	private MapCanvas mapCanvas;
	private JTextField zoomField;
	private IMapRenderer mapRenderer;
	private RenderRequestCallback callback;
	private String styleFile;
	private String renderingProperties;
	private float referenceTileSize;

	public QtCorePanel(LatLon location, int zoom) {
		this.mapCanvas = new MapCanvas(location, zoom);
		callback = new RenderRequestCallback();
	}

	public void setRenderingStyleFile(String styleFile) {
		this.styleFile = styleFile;
	}

	public void setRenderingProperties(String renderingProperties) {
		this.renderingProperties = renderingProperties;
	}

	protected void saveLocation(boolean save) {
		DataExtractionSettings settings = DataExtractionSettings.getSettings();
		settings.saveLocation(mapCanvas.latitude, mapCanvas.longitude, mapCanvas.zoom, save);
	}

	public Frame showFrame(int w, int h) {
		final Frame frame = new Frame("OsmAnd Core");
		frame.setSize(w, h);
		final Animator animator = new Animator();
		frame.setLayout(new java.awt.BorderLayout());
		animator.add(mapCanvas);
		mapCanvas.registerListeners();
		frame.addWindowListener(new java.awt.event.WindowAdapter() {
			@Override
			public void windowClosing(java.awt.event.WindowEvent e) {
				saveLocation(true);
				new Thread(new Runnable() {
					@Override
					public void run() {
						animator.stop();
						frame.dispose();
					}

				}).start();
			}
		});
		frame.add(mapCanvas, java.awt.BorderLayout.CENTER);
		zoomField = new JTextField();
		zoomField.addActionListener(new ActionListener() {

			@Override
			public void actionPerformed(ActionEvent arg0) {
				String txt = zoomField.getText();
				int i = txt.indexOf("#map=");
				String[] vs = txt.substring(i + "#map=".length()).split("/");
				mapCanvas.setLatLon(Float.parseFloat(vs[1]), Float.parseFloat(vs[2]));
				mapCanvas.setZoom(Integer.parseInt(vs[0]));

			}
		});
		frame.add(zoomField, java.awt.BorderLayout.NORTH);
		frame.setBounds(DataExtractionSettings.getSettings().getWindowBounds());
		frame.validate();
		frame.setVisible(true);
		animator.start();
		return frame;
	}

	private class NativeEngineOptions {
		private MapRendererDebugSettings debugSettings = new MapRendererDebugSettings();
		private String localeLanguageId = "en";
		private float density = 1;
		private float symbolsScale = 1;
		private MapPresentationEnvironment.LanguagePreference languagePreference =
				MapPresentationEnvironment.LanguagePreference.LocalizedOrNative;
		private final QStringStringHash styleSettings = new QStringStringHash();

		public void parseRenderingProperties(String renderingProperties) {
			styleSettings.clear();
			localeLanguageId = "en";
			if (renderingProperties == null)
				return;
			for (String s : renderingProperties.split(",")) {
				int i = s.indexOf('=');
				if (i > 0) {
					String name = s.substring(0, i).trim();
					String value = s.substring(i + 1).trim();
					if (value.contains(";")) {
						value = value.substring(0, value.indexOf(';'));
					}
					if (name.equals("lang")) {
						localeLanguageId = value;
					} else if (name.equals("density")) {
						density = Float.parseFloat(value);
					} else if (name.equals("languagePreference")) {
						if (value.equals("nativeOnly")) {
							languagePreference = MapPresentationEnvironment.LanguagePreference.NativeOnly;
						} else if (value.equals("localizedOrNative")) {
							languagePreference = MapPresentationEnvironment.LanguagePreference.LocalizedOrNative;
						} else if (value.equals("nativeAndLocalized")) {
							languagePreference = MapPresentationEnvironment.LanguagePreference.NativeAndLocalized;
						} else if (value.equals("nativeAndLocalizedOrTransliterated")) {
							languagePreference = MapPresentationEnvironment.LanguagePreference.NativeAndLocalizedOrTransliterated;
						} else if (value.equals("localizedAndNative")) {
							languagePreference = MapPresentationEnvironment.LanguagePreference.LocalizedAndNative;
						} else if (value.equals("localizedOrTransliteratedAndNative")) {
							languagePreference = MapPresentationEnvironment.LanguagePreference.LocalizedOrTransliteratedAndNative;
						}
					} else if (name.equals("textScale")) {
						symbolsScale = Float.parseFloat(value);
					} else if (name.equals("debugStageEnabled")) {
						debugSettings.setDebugStageEnabled(value.equals("true"));
					} else if (name.equals("excludeOnPathSymbols")) {
						debugSettings.setExcludeOnPathSymbolsFromProcessing(value.equals("true"));
					} else if (name.equals("excludeBillboardSymbols")) {
						debugSettings.setExcludeBillboardSymbolsFromProcessing(value.equals("true"));
					} else if (name.equals("excludeOnSurfaceSymbols")) {
						debugSettings.setExcludeOnSurfaceSymbolsFromProcessing(value.equals("true"));
					} else if (name.equals("skipSymbolsIntersection")) {
						debugSettings.setSkipSymbolsIntersectionCheck(value.equals("true"));
					} else if (name.equals("showSymbolsBBoxesAccByIntersection")) {
						debugSettings.setShowSymbolsBBoxesAcceptedByIntersectionCheck(value.equals("true"));
					} else if (name.equals("showSymbolsBBoxesRejByIntersection")) {
						debugSettings.setShowSymbolsBBoxesRejectedByIntersectionCheck(value.equals("true"));
					} else if (name.equals("skipSymbolsMinDistance")) {
						debugSettings.setSkipSymbolsMinDistanceToSameContentFromOtherSymbolCheck(value.equals("true"));
					} else if (name.equals("showSymbolsBBoxesRejectedByMinDist")) {
						debugSettings.setShowSymbolsBBoxesRejectedByMinDistanceToSameContentFromOtherSymbolCheck(value
								.equals("true"));
					} else if (name.equals("showSymbolsCheckBBoxesRejectedByMinDist")) {
						debugSettings
								.setShowSymbolsCheckBBoxesRejectedByMinDistanceToSameContentFromOtherSymbolCheck(value
										.equals("true"));
					} else if (name.equals("skipSymbolsPresentationModeCheck")) {
						debugSettings.setSkipSymbolsPresentationModeCheck(value.equals("true"));
					} else if (name.equals("showSymbolsBBoxesRejectedByPresentationMode")) {
						debugSettings.setShowSymbolsBBoxesRejectedByPresentationMode(value.equals("true"));
					} else if (name.equals("showOnPathSymbolsRenderablesPaths")) {
						debugSettings.setShowOnPathSymbolsRenderablesPaths(value.equals("true"));
					} else if (name.equals("showOnPath2dSymbolGlyphDetails")) {
						debugSettings.setShowOnPath2dSymbolGlyphDetails(value.equals("true"));
					} else if (name.equals("showOnPath3dSymbolGlyphDetails")) {
						debugSettings.setShowOnPath3dSymbolGlyphDetails(value.equals("true"));
					} else if (name.equals("allSymbolsTransparentForIntersectionLookup")) {
						debugSettings.setAllSymbolsTransparentForIntersectionLookup(value.equals("true"));
					} else if (name.equals("showTooShortOnPathSymbolsRenderablesPaths")) {
						debugSettings.setShowTooShortOnPathSymbolsRenderablesPaths(value.equals("true"));
					} else if (name.equals("showAllPaths")) {
						debugSettings.setShowAllPaths(value.equals("true"));
					} else {
						styleSettings.set(name, value);
					}
				}
			}
		}

		public MapRendererDebugSettings getDebugSettings() {
			return debugSettings;
		}

		public String getLocaleLanguageId() {
			return localeLanguageId;
		}

		public MapPresentationEnvironment.LanguagePreference getLanguagePreference() {
			return languagePreference;
		}

		public QStringStringHash getStyleSettings() {
			return styleSettings;
		}
	}


	@Override
	public void init(GLAutoDrawable drawable) {
		NativeEngineOptions options = new NativeEngineOptions();
		options.parseRenderingProperties(renderingProperties);
		MapStylesCollection mapStylesCollection = new MapStylesCollection();
		ResolvedMapStyle mapStyle = null;
		if (this.styleFile != null) {
			File styleDir = new File(styleFile).getParentFile();
			loadRenderer(mapStylesCollection, this.styleFile, styleDir);
			loadRendererAddons(mapStylesCollection, styleDir);
			mapStyle = mapStylesCollection.getResolvedStyleByName((new File(this.styleFile)).getName());
		} else {
			System.out.println("Going to use embedded map style");
			mapStyle = mapStylesCollection.getResolvedStyleByName("default");
		}
		if (mapStyle == null) {
			System.err.println("Failed to resolve style");
			release();
			OsmAndCore.ReleaseCore();
			System.exit(0);
		}

		ObfsCollection obfsCollection = new ObfsCollection();
		String filesDir = DataExtractionSettings.getSettings().getBinaryFilesDir();
		obfsCollection.addDirectory(filesDir, false);
		MapPresentationEnvironment mapPresentationEnvironment = new MapPresentationEnvironment(mapStyle,
				options.density, 1.0f, options.symbolsScale / options.density);
        mapPresentationEnvironment.setLocaleLanguageId(options.getLocaleLanguageId());
        mapPresentationEnvironment.setLanguagePreference(options.getLanguagePreference());
		referenceTileSize = 256 * options.density;
		int rasterTileSize = Integer.highestOneBit((int) referenceTileSize - 1) * 2;
		mapPresentationEnvironment.setSettings(options.getStyleSettings());
		MapPrimitiviser mapPrimitiviser = new MapPrimitiviser(mapPresentationEnvironment);
		ObfMapObjectsProvider obfMapObjectsProvider = new ObfMapObjectsProvider(obfsCollection);
		MapPrimitivesProvider mapPrimitivesProvider = new MapPrimitivesProvider(
				obfMapObjectsProvider, mapPrimitiviser, rasterTileSize);
		MapObjectsSymbolsProvider mapObjectsSymbolsProvider = new MapObjectsSymbolsProvider(
				mapPrimitivesProvider, rasterTileSize);
		MapRasterLayerProvider_Software mapRasterLayerProvider = new MapRasterLayerProvider_Software(
				mapPrimitivesProvider);

		mapRenderer = OsmAndCore.createMapRenderer(MapRendererClass.AtlasMapRenderer_OpenGL2plus);
		if (mapRenderer == null) {
			System.err.println("Failed to create map renderer 'AtlasMapRenderer_OpenGL2plus'");
			release();
			OsmAndCore.ReleaseCore();
			System.exit(0);
		}
		QIODeviceLogSink logSink = QIODeviceLogSink.createFileLogSink(OsmExtractionUI.getUserLogDirectoryPath() + "/osmandcore.log");
		Logger.get().addLogSink(logSink);

		MapRendererSetupOptions rendererSetupOptions = new MapRendererSetupOptions();
		rendererSetupOptions.setGpuWorkerThreadEnabled(false);
		rendererSetupOptions.setFrameUpdateRequestCallback(callback.getBinding());
        rendererSetupOptions.setPathToOpenGLShadersCache(DataExtractionSettings.getSettings().getBinaryFilesDir());
        rendererSetupOptions.setMaxNumberOfRasterMapLayersInBatch(8);
		mapRenderer.setup(rendererSetupOptions);

		AtlasMapRendererConfiguration atlasRendererConfiguration = AtlasMapRendererConfiguration.Casts
				.upcastFrom(mapRenderer.getConfiguration());
		atlasRendererConfiguration.setReferenceTileSizeOnScreenInPixels(referenceTileSize);
		mapRenderer.setConfiguration(AtlasMapRendererConfiguration.Casts
				.downcastTo_MapRendererConfiguration(atlasRendererConfiguration));

		mapRenderer.addSymbolsProvider(mapObjectsSymbolsProvider);
		mapRenderer.setAzimuth(0.0f);
		mapRenderer.setElevationAngle(90);
		mapRenderer.setDebugSettings(options.getDebugSettings());

		mapCanvas.updateRenderer();
		/*
		 * IMapRasterLayerProvider mapnik = OnlineTileSources.getBuiltIn().createProviderFor("Mapnik (OsmAnd)"); if
		 * (mapnik == null) Log.e(TAG, "Failed to create mapnik");
		 */
		mapRenderer.setMapLayerProvider(0, mapRasterLayerProvider);
	}

	private void loadRenderer(MapStylesCollection mapStylesCollection, String styleFile, File styleDir) {
		String depends = getDepends(styleFile);
		if (!Algorithms.isEmpty(depends)) {
			String dependsStyle = new File(styleDir, depends + ".render.xml").getAbsolutePath();
			loadRenderer(mapStylesCollection, dependsStyle, styleDir);
		}
		mapStylesCollection.addStyleFromFile(styleFile);
		System.out.println("Going to use map style from: " + styleFile);
	}

	private String getDepends(String styleFile) {
		String depends = null;
		try {
			XmlPullParser parser = PlatformUtil.newXMLPullParser();
			parser.setInput(new FileInputStream(styleFile), null);
			int tok;
			while ((tok = parser.next()) != XmlPullParser.END_DOCUMENT) {
				if (tok == XmlPullParser.START_TAG) {
					if ("renderingStyle".equals(parser.getName())) {
						depends = parser.getAttributeValue(null, "depends");
						break;
					}
				}
			}
		} catch (XmlPullParserException | IOException e) {
			throw new RuntimeException(e);
		}
		return depends;
	}

	private void loadRendererAddons(MapStylesCollection mapStylesCollection, File stylesDir) {
		File[] files = stylesDir.listFiles();
		if (files != null) {
			for (File file : files) {
				if (file.isFile() && file.getName().endsWith("addon.render.xml")) {
					mapStylesCollection.addStyleFromFile(file.getAbsolutePath());
				}
			}
		}
	}

	private class RenderRequestCallback extends MapRendererSetupOptions.IFrameUpdateRequestCallback {
		@Override
		public void method(IMapRenderer mapRenderer) {
			mapCanvas.repaint();
		}
	}

	@Override
	public void reshape(GLAutoDrawable drawable, int x, int y, int width, int height) {
		mapRenderer.setViewport(new AreaI(0, 0, height, width));
		mapRenderer.setWindowSize(new PointI(width, height));

		if (!mapRenderer.isRenderingInitialized()) {
			if (!mapRenderer.initializeRendering(true))
				System.err.println("Failed to initialize rendering");
		}
	}

	@Override
	public void display(GLAutoDrawable drawable) {
		if (mapRenderer == null) {
			return;
		}
		if (!mapRenderer.isRenderingInitialized()) {
			return;
		}
		mapRenderer.update();

		if (mapRenderer.prepareFrame())
			mapRenderer.renderFrame();
	}

	@Override
	public void dispose(GLAutoDrawable drawable) {
		if (mapRenderer != null && mapRenderer.isRenderingInitialized())
			mapRenderer.releaseRendering();
		release();
		saveDefaultSettings();
		System.out.println("GL window is disposed.");
	}

	protected void saveDefaultSettings() {
		DataExtractionSettings settings = DataExtractionSettings.getSettings();
		settings.saveDefaultLocation(mapCanvas.latitude, mapCanvas.longitude);
		settings.saveDefaultZoom((int) mapCanvas.zoom);
		// settings.saveWindowBounds(frame.getBounds());
	}

	private class MapCanvas extends com.jogamp.opengl.awt.GLCanvas {
		private int zoom;
		private double longitude;
		private double latitude;

		public MapCanvas(LatLon location, int zoom) {
			this.latitude = location.getLatitude();
			this.longitude = location.getLongitude();
			this.zoom = zoom;
		}

		public void updateRenderer() {
			mapRenderer
					.setTarget(new PointI(MapUtils.get31TileNumberX(longitude), MapUtils.get31TileNumberY(latitude)));
			mapRenderer.setZoom(zoom);
			zoomField.setText("http://www.openstreetmap.org/#map=" + ((int) zoom) + "/" + ((float) latitude) + "/"
					+ ((float) longitude));
		}

		public void registerListeners() {
			MapMouseAdapter mouse = new MapMouseAdapter();
			mapCanvas.addMouseListener(mouse);
			mapCanvas.addMouseMotionListener(mouse);
			mapCanvas.addMouseWheelListener(mouse);
			mapCanvas.addGLEventListener(QtCorePanel.this);
		}

		public int getZoom() {
			return zoom;
		}

		public double getCenterPointX() {
			return getWidth() / 2;
		}

		public double getCenterPointY() {
			return getHeight() / 2;
		}

		public double getTileSize() {
			return referenceTileSize;
		}

		private void setLatLon(double lat, double lon) {
			latitude = lat;
			longitude = lon;
			updateRenderer();
			saveLocation(false);

		}

		@Override
		protected void processKeyEvent(KeyEvent e) {
			boolean processed = false;
			if (e.getID() == KeyEvent.KEY_RELEASED) {
				if (e.getKeyCode() == 37) {
					// LEFT button
					longitude = MapUtils.getLongitudeFromTile(zoom, getXTile() - 0.5);
					processed = true;
				} else if (e.getKeyCode() == 39) {
					// RIGHT button
					longitude = MapUtils.getLongitudeFromTile(zoom, getXTile() + 0.5);
					processed = true;
				} else if (e.getKeyCode() == 38) {
					// UP button
					latitude = MapUtils.getLatitudeFromTile(zoom, getYTile() - 0.5);
					processed = true;
				} else if (e.getKeyCode() == 40) {
					// DOWN button
					latitude = MapUtils.getLatitudeFromTile(zoom, getYTile() + 0.5);
					processed = true;
				}
			}
			if (e.getID() == KeyEvent.KEY_TYPED) {
				if (e.getKeyChar() == '+' || e.getKeyChar() == '=') {
					if (zoom < getMaximumZoomSupported()) {
						zoom++;
						processed = true;
					}
				} else if (e.getKeyChar() == '-') {
					if (zoom > getMinimumZoomSupported()) {
						zoom--;
						processed = true;
					}
				}
			}

			if (processed) {
				e.consume();
			}
			super.processKeyEvent(e);
		}

		private int getMinimumZoomSupported() {
			return 1;
		}

		private int getMaximumZoomSupported() {
			return 21;
		}

		private double getYTile() {
			return MapUtils.getTileNumberY(zoom, latitude);
		}

		private double getXTile() {
			return MapUtils.getTileNumberX(zoom, longitude);
		}

		public class MapMouseAdapter extends MouseAdapter {
			private Point startDragging = null;

			@Override
			public void mouseClicked(MouseEvent e) {
				requestFocus();
			}

			public void dragTo(Point p) {
				double dx = (startDragging.x - (double) p.x) / referenceTileSize;
				double dy = (startDragging.y - (double) p.y) / referenceTileSize;
				double lat = MapUtils.getLatitudeFromTile(zoom, getYTile() + dy);
				double lon = MapUtils.getLongitudeFromTile(zoom, getXTile() + dx);
				setLatLon(lat, lon);
			}

			@Override
			public void mouseDragged(MouseEvent e) {
				if (startDragging != null) {
					if (Math.abs(e.getPoint().x - startDragging.x) + Math.abs(e.getPoint().y - startDragging.y) >= 8) {
						dragTo(e.getPoint());
						startDragging = e.getPoint();
					}
				}
			}

			@Override
			public void mouseWheelMoved(MouseWheelEvent e) {
				double dy = e.getPoint().y - getCenterPointY();
				double dx = e.getPoint().x - getCenterPointX();
				double lat = MapUtils.getLatitudeFromTile(zoom, getYTile() + dy / getTileSize());
				double lon = MapUtils.getLongitudeFromTile(zoom, getXTile() + dx / getTileSize());
				setLatLon(lat, lon);
				if (e.getWheelRotation() < 0) {
					setZoom(getZoom() + 1);
				} else if (e.getWheelRotation() > 0) {
					setZoom(getZoom() - 1);
				}
				lat = MapUtils.getLatitudeFromTile(zoom, getYTile() - dy / getTileSize());
				lon = MapUtils.getLongitudeFromTile(zoom, getXTile() - dx / getTileSize());
				setLatLon(lat, lon);
				super.mouseWheelMoved(e);
			}

			@Override
			public void mousePressed(MouseEvent e) {
				if (e.getButton() == MouseEvent.BUTTON3) {
					if (startDragging == null) {
						startDragging = e.getPoint();
					}
				}
			}

			@Override
			public void mouseReleased(MouseEvent e) {
				if (e.getButton() == MouseEvent.BUTTON3) {
					if (startDragging != null) {
						dragTo(e.getPoint());
						startDragging = null;
					}
				}
				super.mouseReleased(e);
			}

		}

		public void setZoom(int f) {
			zoom = f;
			updateRenderer();
		}
	}

	private void release() {
	}

	public static void main(String[] args) {
		// load QT
		// System.load("/home/victor/temp/test/libOsmAndCore_shared.so");
		// System.load("/home/victor/temp/test/libOsmAndCoreJNI.so");
		String nativePath = "/home/victor/temp/OsmAndMapCreator-main/lib";
		if (args.length > 0) {
			nativePath = args[0];
		}
		loadNative(nativePath);
		final QtCorePanel sample = new QtCorePanel(DataExtractionSettings.getSettings().getDefaultLocation(),
				DataExtractionSettings.getSettings().getDefaultZoom());
		Frame frame = sample.showFrame(800, 600);
		frame.addWindowListener(new java.awt.event.WindowAdapter() {
			@Override
			public void windowClosing(java.awt.event.WindowEvent e) {
				new Thread(new Runnable() {
					@Override
					public void run() {
//						sample.saveDefaultSettings();
						OsmAndCore.ReleaseCore();
						System.exit(0);
					}
				}).start();
			}
		});
	}


}
