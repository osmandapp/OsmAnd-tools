package net.osmand.swing;

import java.awt.Frame;
import java.awt.Point;
import java.awt.event.KeyEvent;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.awt.event.MouseWheelEvent;

import javax.media.opengl.GLAutoDrawable;
import javax.media.opengl.GLEventListener;
import javax.media.opengl.awt.GLCanvas;

import net.osmand.core.jni.AreaI;
import net.osmand.core.jni.AtlasMapRendererConfiguration;
import net.osmand.core.jni.BinaryMapDataProvider;
import net.osmand.core.jni.BinaryMapPrimitivesProvider;
import net.osmand.core.jni.BinaryMapRasterBitmapTileProvider_Software;
import net.osmand.core.jni.BinaryMapStaticSymbolsProvider;
import net.osmand.core.jni.CoreResourcesEmbeddedBundle;
import net.osmand.core.jni.IMapRenderer;
import net.osmand.core.jni.IMapStylesCollection;
import net.osmand.core.jni.MapPresentationEnvironment;
import net.osmand.core.jni.MapRendererClass;
import net.osmand.core.jni.MapRendererSetupOptions;
import net.osmand.core.jni.MapStyle;
import net.osmand.core.jni.MapStylesCollection;
import net.osmand.core.jni.ObfsCollection;
import net.osmand.core.jni.OsmAndCore;
import net.osmand.core.jni.PointI;
import net.osmand.core.jni.Primitiviser;
import net.osmand.core.jni.RasterMapLayerId;
import net.osmand.data.LatLon;
import net.osmand.util.MapUtils;

import com.jogamp.opengl.util.Animator;

public class QtCorePanel implements GLEventListener {
	private static final float displayDensityFactor = 1.0f;
	private static final int referenceTileSize = 256;
	private static final int rasterTileSize = 256;
	
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
		return (OS.indexOf("nix") >= 0 || OS.indexOf("nux") >= 0 || OS.indexOf("aix") > 0 );
	}

	public static void loadNative(String folder) {
		if (loaded == null) {
			try {

				System.load(folder + "/" + System.mapLibraryName("OsmAndCoreWithJNI"));
				coreResourcesEmbeddedBundle = CoreResourcesEmbeddedBundle.loadFromLibrary(folder
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

	public static void main(String[] args) {
		// load QT
		// System.load("/home/victor/temp/test/libOsmAndCore_shared.so");
		// System.load("/home/victor/temp/test/libOsmAndCoreJNI.so");
		String nativePath = "/home/victor/temp/test/";
		if(args.length > 0){
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

	private MapCanvas mapCanvas;
	private IMapRenderer mapRenderer;

	public QtCorePanel(LatLon location, int zoom) {
		this.mapCanvas = new MapCanvas(location, zoom);
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
		frame.setBounds(DataExtractionSettings.getSettings().getWindowBounds());
		frame.validate();
		frame.setVisible(true);
		animator.start();
		return frame;
	}

	@Override
	public void init(GLAutoDrawable drawable) {

		IMapStylesCollection mapStylesCollection = new MapStylesCollection();
		MapStyle mapStyle = mapStylesCollection.getBakedStyle("default");
		if (mapStyle == null) {
			System.err.println("Failed to resolve style 'default'");
			release();
			OsmAndCore.ReleaseCore();
			System.exit(0);
		}

		ObfsCollection obfsCollection = new ObfsCollection();
		String filesDir = DataExtractionSettings.getSettings().getBinaryFilesDir();
		obfsCollection.addDirectory(filesDir, false);
		MapPresentationEnvironment mapPresentationEnvironment = new MapPresentationEnvironment(mapStyle,
				displayDensityFactor, "en");
		Primitiviser primitiviser = new Primitiviser(mapPresentationEnvironment);
		BinaryMapDataProvider binaryMapDataProvider = new BinaryMapDataProvider(obfsCollection);
		BinaryMapPrimitivesProvider binaryMapPrimitivesProvider = new BinaryMapPrimitivesProvider(
				binaryMapDataProvider, primitiviser, rasterTileSize);
		BinaryMapStaticSymbolsProvider binaryMapStaticSymbolsProvider = new BinaryMapStaticSymbolsProvider(
				binaryMapPrimitivesProvider, rasterTileSize);
		BinaryMapRasterBitmapTileProvider_Software binaryMapRasterBitmapTileProvider = new BinaryMapRasterBitmapTileProvider_Software(
				binaryMapPrimitivesProvider);

		mapRenderer = OsmAndCore.createMapRenderer(MapRendererClass.AtlasMapRenderer_OpenGL2plus);
		if (mapRenderer == null) {
			System.err.println("Failed to create map renderer 'AtlasMapRenderer_OpenGL2plus'");
			release();
			OsmAndCore.ReleaseCore();
			System.exit(0);
		}

		MapRendererSetupOptions rendererSetupOptions = new MapRendererSetupOptions();
		rendererSetupOptions.setGpuWorkerThreadEnabled(false);
		rendererSetupOptions.setFrameUpdateRequestCallback(new RenderRequestCallback().getBinding());
		mapRenderer.setup(rendererSetupOptions);

		AtlasMapRendererConfiguration atlasRendererConfiguration = AtlasMapRendererConfiguration.Casts
				.upcastFrom(mapRenderer.getConfiguration());
		atlasRendererConfiguration.setReferenceTileSizeOnScreenInPixels(referenceTileSize);
		mapRenderer.setConfiguration(AtlasMapRendererConfiguration.Casts
				.downcastTo_MapRendererConfiguration(atlasRendererConfiguration));

		mapRenderer.addSymbolProvider(binaryMapStaticSymbolsProvider);
		mapRenderer.setAzimuth(0.0f);
		mapRenderer.setElevationAngle(90);

		mapCanvas.updateRenderer();
		/*
		 * IMapRasterBitmapTileProvider mapnik = OnlineTileSources.getBuiltIn().createProviderFor("Mapnik (OsmAnd)"); if
		 * (mapnik == null) Log.e(TAG, "Failed to create mapnik");
		 */
		mapRenderer.setRasterLayerProvider(RasterMapLayerId.BaseLayer, binaryMapRasterBitmapTileProvider);
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
			if (!mapRenderer.initializeRendering())
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
	}

	protected void saveDefaultSettings() {
		DataExtractionSettings settings = DataExtractionSettings.getSettings();
		settings.saveDefaultLocation(mapCanvas.latitude, mapCanvas.longitude);
		settings.saveDefaultZoom((int) mapCanvas.zoom);
		// settings.saveWindowBounds(frame.getBounds());
	}

	private class MapCanvas extends GLCanvas {
		private float zoom;
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
		}

		public void registerListeners() {
			MapMouseAdapter mouse = new MapMouseAdapter();
			mapCanvas.addMouseListener(mouse);
			mapCanvas.addMouseMotionListener(mouse);
			mapCanvas.addMouseWheelListener(mouse);
			mapCanvas.addGLEventListener(QtCorePanel.this);
		}

		public float getZoom() {
			return zoom;
		}

		private void setLatLon(double lat, double lon) {
			latitude = lat;
			longitude = lon;
			updateRenderer();

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
				if (e.getWheelRotation() < 0) {
					setZoom(getZoom() + 1);
				} else if (e.getWheelRotation() > 0) {
					setZoom(getZoom() - 1);
				}
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

		public void setZoom(float f) {
			zoom = f;
			updateRenderer();
		}
	}

	private void release() {
	}

}