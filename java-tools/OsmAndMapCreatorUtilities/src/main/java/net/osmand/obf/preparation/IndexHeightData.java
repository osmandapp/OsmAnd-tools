package net.osmand.obf.preparation;

import java.awt.Shape;
import java.awt.image.BufferedImage;
import java.awt.image.DataBufferShort;
import java.io.*;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.stream.ImageInputStream;

import net.osmand.binary.ObfConstants;
import org.apache.commons.logging.Log;
import org.xmlpull.v1.XmlPullParser;
import org.xmlpull.v1.XmlPullParserException;

import com.vividsolutions.jts.awt.ShapeWriter;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.geom.util.AffineTransformation;

import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.list.array.TIntArrayList;
import net.osmand.PlatformUtil;
import net.osmand.data.LatLon;
import net.osmand.osm.edit.Node;
import net.osmand.osm.edit.Way;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

public class IndexHeightData {
	public static int MAXIMUM_LOADED_DATA = 150; 
	private static final double MINIMAL_DISTANCE = 0;
	private static final int HEIGHT_ACCURACY = 4;
	private static boolean USE_BILINEAR_INTERPOLATION = false;

	private String srtmDataUrl;
	private File srtmWorkingDir;
	
	public static final String ELE_ASC_START = "osmand_ele_start";
	public static final String ELE_ASC_END = "osmand_ele_end";
	public static final String ELE_INCLINE = "osmand_ele_incline_";
	public static final String ELE_INCLINE_MAX = "osmand_ele_incline_max";
	public static final String ELE_DECLINE = "osmand_ele_decline_";
	public static final String ELE_DECLINE_MAX = "osmand_ele_decline_max";
	public static final String ELE_ASC_TAG = "osmand_ele_asc";
	public static final String ELE_DESC_TAG = "osmand_ele_desc";
	public static final double INEXISTENT_HEIGHT = Double.MIN_VALUE;

	public static final int MAX_SRTM_COUNT_DOWNLOAD = 20000;
	private int srtmCountDownload;
	public static final double MAX_LAT_LON_DIST = 500 * 1000; // 500 km
	
	public static final Set<String> ELEVATION_TAGS = new TreeSet<>(); 
	
	static {
		ELEVATION_TAGS.add(IndexHeightData.ELE_ASC_START);
		ELEVATION_TAGS.add(IndexHeightData.ELE_ASC_END);
		ELEVATION_TAGS.add(IndexHeightData.ELE_INCLINE);
		ELEVATION_TAGS.add(IndexHeightData.ELE_INCLINE_MAX);
		ELEVATION_TAGS.add(IndexHeightData.ELE_DECLINE);
		ELEVATION_TAGS.add(IndexHeightData.ELE_DECLINE_MAX);
		ELEVATION_TAGS.add(IndexHeightData.ELE_ASC_TAG);
		ELEVATION_TAGS.add(IndexHeightData.ELE_DESC_TAG);
	}

	
	private Map<Integer, TileData> map = new HashMap<Integer, TileData>();

	private static final Log log = PlatformUtil.getLog(IndexHeightData.class);
	
	private static class TileData {
		DataBufferShort data;
		private int id;
		private boolean dataLoaded;
		private int height;
		private int width;
		public int accessed;
		public int loaded;
		
		private TileData(int id) {
			this.id = id;
			
		}
		
		public File loadData(String srtmDataUrl, File workDir) throws IOException {
			dataLoaded = true;
			File f = loadFile(getFileName() + ".tif", srtmDataUrl, workDir);
			BufferedImage img;
			if (f.exists()) {
				try (FileInputStream fis = new FileInputStream(f)) {
					img = ImageIO.read(fis);
					readSRTMData(img);
				} catch (Exception e) {
					iterativeReadData(f);
				}
				
				// remove all downloaded files to save disk space
				if (!srtmDataUrl.startsWith("/") && !srtmDataUrl.startsWith(".")) {
					f.delete();
				}
				return null;
			}
			return f;
		}
		
		private BufferedImage iterativeReadData(File file) {
			boolean readSuccess = false;
			BufferedImage img = null;
			Iterator<ImageReader> readers = ImageIO.getImageReadersByFormatName("tiff");
			ImageInputStream iis = null;
			while (readers.hasNext() && !readSuccess) {
				ImageReader reader = readers.next();
				if (!(reader instanceof com.sun.media.imageioimpl.plugins.tiff.TIFFImageReader)) {
					try {
						iis = ImageIO.createImageInputStream(file);
						reader.setInput(iis, true);
						img = reader.read(0);
						readSRTMData(img);
						readSuccess = true;
					} catch (IOException e) {
						log.info("Error reading TIFF file with reader " + reader.getClass().getName() + ": " + e.getMessage());
					} finally {
						reader.dispose();
						if (iis != null) {
							try {
								iis.close();
							} catch (IOException e) {
								log.error("Error closing ImageInputStream: " + e.getMessage());
							}
						}
					}
				}
			}
			
			if (!readSuccess) {
				log.error("Failed to read TIFF file with all available readers.");
			}
			return img;
		}
		
		private void readSRTMData(BufferedImage img) {
			if (img != null) {
				width = img.getWidth();
				height = img.getHeight();
				data = (DataBufferShort) img.getRaster().getDataBuffer();
			}
		}
		

		private String getFileName() {
			int ln = (id >> 10) - 180;
			int lt = (id - ((id >> 10) << 10)) - 90;
			String nd = getId(ln, lt);
			return nd;
		}
		
		public double getHeight(double x, double y, double[] array) {
			if (data == null) {
				return INEXISTENT_HEIGHT;
			}
			if(USE_BILINEAR_INTERPOLATION) {
				return bilinearInterpolation(x, y, array);
			} else {
				return bicubicInterpolation(x, y, array);
			}
//			System.out.println(" --- " + (h1 - h2) + " " + h1 + " " + h2);
		}
		
		protected double bicubicInterpolation(double ix, double iy, double[] cf) {
			double pdx = (width - 2) * ix + 1;
			double pdy = (height - 2) * (1 - iy) + 1;
			int px = (int) Math.round(pdx);
			int py = (int) Math.round(pdy);
			double x = pdx - px + 0.5;
			double y = pdy - py + 0.5;
//			int px = (int) pdx;
//			int py = (int) pdy;
//			double x = pdx - px;
//			double y = pdy - py;
			// pdx = 26.6 -> px = 27, x = 0.1 (26.5=0, 27.5=1)
			// pdx = 26.4 -> px = 26, x = 0.9 (25.5=0, 26.5=1)
			// pdx = 27.0 -> px = 27, x = 0.5 (26.5=0, 27.5=1)
			px --;
			py --;
			if(cf == null) {
				cf = new double[16]; 
			}
			for(int i = 0; i < cf.length; i++) {
				cf[i] = 0;
			}
			//https://en.wikipedia.org/wiki/Bicubic_interpolation
			// swap formula
			double tx = y;
			y = x;
			x = tx;
			cf[0] = (x-1)*(x-2)*(x+1)*(y-1)*(y-2)*(y+1) / 4 * getElem(px, py);
			cf[1] = -(x)*(x-2)*(x+1)*(y-1)*(y-2)*(y+1) / 4 * getElem(px, py + 1);
			cf[2] = -(x-1)*(x-2)*(x+1)*(y)*(y-2)*(y+1) / 4 * getElem(px + 1, py);
			cf[3] = (x)*(x-2)*(x+1)*(y)*(y-2)*(y+1) / 4 * getElem(px + 1, py + 1);
			cf[4] = -(x)*(x-2)*(x-1)*(y-1)*(y-2)*(y+1) / 12 * getElem(px, py - 1);
			cf[5] = -(x+1)*(x-2)*(x-1)*(y-1)*(y-2)*(y) / 12 * getElem(px - 1, py);
			cf[6] = (x)*(x-2)*(x-1)*(y+1)*(y-2)*(y) / 12 * getElem(px + 1, py - 1);
			cf[7] = (x)*(x-2)*(x+1)*(y-1)*(y-2)*(y) / 12 * getElem(px - 1, py + 1);
			cf[8] = (x)*(x-1)*(x+1)*(y-1)*(y-2)*(y+1) / 12 * getElem(px, py + 2);
			cf[9] = (x-2)*(x-1)*(x+1)*(y-1)*(y)*(y+1) / 12 * getElem(px + 2, py);
			cf[10] = (x)*(x-1)*(x-2)*(y)*(y-1)*(y-2) / 36 * getElem(px - 1, py - 1);
			cf[11] = -(x)*(x-1)*(x+1)*(y)*(y+1)*(y-2) / 12 * getElem(px + 1, py + 2);
			cf[12] = -(x)*(x+1)*(x-2)*(y)*(y-1)*(y+1) / 12 * getElem(px + 2, py + 1);
			cf[13] = -(x)*(x-1)*(x+1)*(y)*(y-1)*(y-2) / 36 * getElem(px - 1, py + 2);
			cf[14] = -(x)*(x-1)*(x-2)*(y)*(y-1)*(y+1) / 36 * getElem(px + 2, py - 1);
			cf[15] =  (x)*(x-1)*(x+1)*(y)*(y-1)*(y+1) / 36 * getElem(px + 2, py + 2);
			double h = 0;
			for(int i = 0; i < cf.length; i++) {
				h += cf[i];
			}
			return h;
		}

		protected double bilinearInterpolation(double x, double y, double[] array) {
			double pdx = (width - 2) * x + 1;
			double pdy = (height - 2) * (1 - y) + 1;
			int px = (int) Math.round(pdx);
			int py = (int) Math.round(pdy);
			if(array == null) {
				array = new double[4]; 
			}
			array[0] = getElem(px - 1, py - 1);
			array[1] = getElem(px, py - 1);
			array[2] = getElem(px - 1, py);
			array[3] = getElem(px, py);
			double cx = 0.5 + pdx - px;
			double cy = 0.5 + pdy - py;
			// 1.3 pdx ->  px = 1, px - 1 = 0, cx = 0.8, 1 - cx = 0.2,
			// 1.7 pdx ->  px = 2, px - 1 = 1, cx = 0.2, 1 - cx = 0.8, 
			
			double h = (1 - cx) * (1 - cy) * array[0] + 
					         cx * (1 - cy) * array[1] + 
					   (1 - cx) * cy       * array[2] + 
					         cx * cy       * array[3];
			return h;
		}

		private double getElem(int px, int py) {
			if (px <= 0) {
				px = 1;
			}
			if(py <= 0) {
				py = 1;
			}
			if (px >= width - 1) {
				px = width - 2;
			}
			if (py >= height - 1) {
				py = height - 2;
			}
			
			int ind = px + py * width;
			if (ind >= data.getSize()) {
				throw new IllegalArgumentException("Illegal access (" + px + ", " + py + ") " + ind + " - "
						+ getFileName());
			}
			int h = data.getElem(ind) & 0xffff;
			if(h > 0x7fff) {
				return h - (0xffff);
			}
			return h;
		}

		private String getId(int ln, int lt) {
			String id = "";
			if(lt >= 0) {
				id += "N";
			} else {
				id += "S";
			}
			lt = Math.abs(lt);
			if(lt < 10) {
				id += "0";
			}
			id += lt;
			
			if(ln >= 0) {
				id += "E";
			} else {
				id += "W";
			}
			ln = Math.abs(ln);
			if(ln < 10) {
				id += "0";
			}
			if(ln < 100) {
				id += "0";
			}
			id += ln;
			return id;
		}
	}
	
	private class WayHeightStats {
		double firstHeight = INEXISTENT_HEIGHT;
		double lastHeight = INEXISTENT_HEIGHT;
		
		int DEGREE_START = 1;
		int DEGREE_PRECISION = 2;
		int DEGREE_MAX = 30;
		int SIZE = (DEGREE_MAX - DEGREE_START) / DEGREE_PRECISION;
		
		double[] ascIncline = new double[SIZE];
		double[] descIncline = new double[SIZE];
		
		
		float processHeight(double pointHeight, double prevHeight, double dist, Node n) {
			if (firstHeight == INEXISTENT_HEIGHT) {
				firstHeight = prevHeight;
			}
			lastHeight = pointHeight;
			double diff = (pointHeight - prevHeight);
			int df = (int) Math.round(diff * HEIGHT_ACCURACY);
			if(df == 0) {
				return 0 ;
			}
			// double deg = Math.abs(Math.atan2(pointHeight - prevHeight, dist) / Math.PI * 180);
			double degIncline = Math.abs((pointHeight - prevHeight) /  dist * 100); 
			
			double[] arr ;
			if (df > 0) {
				arr = ascIncline;
				n.putTag(ELE_ASC_TAG, df / (float)HEIGHT_ACCURACY + "");
			} else  {
				arr = descIncline;
				n.putTag(ELE_DESC_TAG, -df / (float)HEIGHT_ACCURACY + "");
			}
			int maxDeg = 0;
			for(int k = 0; k < SIZE; k++) {
				int bs = DEGREE_START + k * DEGREE_PRECISION;
				if(degIncline >= bs) {
					arr[k] += dist;
					maxDeg = bs;
				}
			}
			if (maxDeg > 0) {
				if (df > 0) {
					n.putTag(ELE_INCLINE + "", "1");
					n.putTag(ELE_INCLINE_MAX, maxDeg+"");
				} else {
					n.putTag(ELE_DECLINE_MAX, maxDeg +"");
					n.putTag(ELE_DECLINE + maxDeg, "1");
				}
			}
			return df / 4.0f ;
		}
	}
	
	public boolean proccess(Way e) {
		if (!isHeightDataNeeded(e)) {
			// true processed
			return true;
		}

		WayHeightStats wh = new WayHeightStats();

		List<Node> ns = e.getNodes();
		double prevHeight = INEXISTENT_HEIGHT;
		Node prev = null;
		for (int i = 0; i < ns.size(); i++) {
			Node n = ns.get(i);
			if (n != null && n.getId() <= ObfConstants.PROPAGATE_NODE_BIT) {
				double pointHeight = getPointHeight(n.getLatitude(), n.getLongitude());
				if (prev == null) {
					if (pointHeight != INEXISTENT_HEIGHT) {
						prevHeight = pointHeight;
						prev = n;
					}
				} else {
					if (MapUtils.getDistance(prev.getLatLon(), n.getLatLon()) > MAX_LAT_LON_DIST) {
						System.err.printf("Skip long line %d dist %.1f km\n", e.getId() / 64,
								MapUtils.getDistance(prev.getLatLon(), n.getLatLon()) / 1000);
						return false;
					}
					double segm = MapUtils.getDistance(prev.getLatitude(), prev.getLongitude(), n.getLatitude(),
							n.getLongitude());
					if (segm > MINIMAL_DISTANCE && pointHeight != INEXISTENT_HEIGHT) {
						wh.processHeight(pointHeight, prevHeight, segm, n);
						prevHeight = pointHeight;
						prev = n;
					}
				}
			}
		}
		if (wh.firstHeight != INEXISTENT_HEIGHT && wh.lastHeight != INEXISTENT_HEIGHT) {
			e.putTag(ELE_ASC_START, ((int) Math.round(wh.firstHeight)) + "");
		}
		if (wh.lastHeight != INEXISTENT_HEIGHT && wh.firstHeight != wh.lastHeight) {
			e.putTag(ELE_ASC_END, ((int) Math.round(wh.lastHeight)) + "");
		}
		// for(int k = 0; k < wh.SIZE; k++) {
		// int deg = wh.DEGREE_START + k * wh.DEGREE_PRECISION;
		// if (wh.ascIncline[k] > 0) {
		// e.putTag(ELE_INCLINE + deg, ((int) wh.ascIncline[k]) + "");
		// }
		// if (wh.descIncline[k] > 0) {
		// e.putTag(ELE_DECLINE + deg, ((int) wh.descIncline[k]) + "");
		// }
		// }
		// if(wh.asc >= 1){
		// e.putTag(ELE_ASC_TAG, ((int)wh.asc)+"");
		// }
		// if(wh.desc >= 1){
		// e.putTag(ELE_DESC_TAG, ((int)wh.desc)+"");
		// }
		return true;
	}



	public static boolean isHeightDataNeeded(Way e) {
		if (e.getTag("highway") == null && e.getTag("cycleway") == null && e.getTag("footway") == null
				&& e.getTag("waterway") == null && e.getTag("piste:type") == null) {
			return false;
		}
		if (e.getTag("osmand_change") != null) {
			return false;
		}
		if (e.getTag("tunnel") != null || e.getTag("bridge") != null) {
			return false;
		}
		return true;
	}
	
	
	
	public static class WayGeneralStats {
		public double startEle = 0;
		public double endEle = 0;
		public double minEle = 0;
		public double maxEle = 0;
		public double sumEle = 0;
		public int eleCount = 0;
		public double up = 0;
		public double down = 0;
		public double dist = 0;
		public int step = 10;
		public TDoubleArrayList altitudes = new TDoubleArrayList();
		public TDoubleArrayList dists = new TDoubleArrayList();
		public TIntArrayList altIncs = new TIntArrayList();
		
	}
	
	public WayGeneralStats calculateWayGeneralStats(Way w, double DIST_STEP) {
		Node pnode = null; 
		WayGeneralStats wg = new WayGeneralStats();
		int I_DIST_STEP = (int) DIST_STEP;
		double dist = 0;
		for (int i = 0; i < w.getNodes().size(); i++) {
			Node node = w.getNodes().get(i);
			double step = 0;
			if(i > 0) {
				step = MapUtils.getDistance(pnode.getLatitude(), pnode.getLongitude(), node.getLatitude(), node.getLongitude());
			}
			double h = getPointHeight(node.getLatitude(), node.getLongitude());
			if (step > I_DIST_STEP) {
				int extraFragments = (int) (step / DIST_STEP);
				// in case way is very long calculate alt each DIST_STEP
				for (int st = 1; st < extraFragments; st++) {
					double midlat = pnode.getLatitude()
							+ (node.getLatitude() - pnode.getLatitude()) * st / ((double) extraFragments);
					double midlon = pnode.getLongitude()
							+ (node.getLongitude() - pnode.getLongitude()) * st / ((double) extraFragments);
					double midh = getPointHeight(midlat, midlon);
					double d = MapUtils.getDistance(pnode.getLatitude(), pnode.getLongitude(), midlat, midlon);
					wg.dists.add(dist + d);
					wg.altitudes.add(midh);
				}
			}
			dist += step;
			wg.dists.add(dist);
			wg.altitudes.add(h);
			pnode = node;
		}
		calculateEleStats(wg, I_DIST_STEP);
		return wg;
	}



	public static void calculateEleStats(WayGeneralStats wg, int DIST_STEP) {
		wg.step = DIST_STEP;
		wg.dist = wg.dists.get(wg.dists.size() - 1);
		double prevUpDownDist = 0;
		double prevUpDownH = 0;

		double prevGraphDist = 0;
		double prevGraphH = 0;
		for (int i = 0; i < wg.dists.size(); i++) {
			double h = wg.altitudes.get(i);
			double sumdist = wg.dists.get(i);
			if (h != IndexHeightData.INEXISTENT_HEIGHT) {
				wg.endEle = h;
				if (wg.eleCount == 0) {
					prevGraphH = prevUpDownH = wg.startEle = wg.minEle = wg.maxEle = wg.sumEle = h;
					wg.eleCount = 1;
				} else {
					wg.minEle = Math.min(h, wg.minEle);
					wg.maxEle = Math.max(h, wg.maxEle);
					wg.sumEle += h;
					wg.eleCount++;
				}

				if (sumdist >= prevUpDownDist + DIST_STEP) {
					if (h > prevUpDownH) {
						wg.up += (h - prevUpDownH);
					} else {
						wg.down += (prevUpDownH - h);
					}
					prevUpDownDist = sumdist;
					prevUpDownH = h;
				}
				
				while (sumdist >= prevGraphDist + DIST_STEP) {
					// here could be interpolation but probably it's not needed in most of the cases
					wg.altIncs.add((int) (h - prevGraphH));
					prevGraphH = h;
					prevGraphDist += DIST_STEP;
				}
			}
		}
	}
	
	
	
	public void setSrtmData(String srtmData, File workingDir) {
		this.srtmDataUrl = srtmData;
		this.srtmWorkingDir = workingDir;
	}
	
	public double getPointHeight(double lat, double lon) {
		return getPointHeight(lat, lon, null, null);
	}
	
	public double getPointHeight(double lat, double lon, File[] fileName) {
		return getPointHeight(lat, lon, fileName, null);
	}
	
	private double getPointHeight(double lat, double lon, File[] fileName, double[] neighboors) {
		int lt = (int) lat;
		int ln = (int) lon;
		double lonDelta = lon - ln;
		double latDelta = lat - lt;
		if (lonDelta < 0) {
			lonDelta += 1;
			ln -= 1;
		}
		if (latDelta < 0) {
			latDelta += 1;
			lt -= 1;
		}
		int id = getTileId(lt, ln);
		TileData tileData = map.get(id);
		if (tileData == null) {
			tileData = new TileData(id);
			map.put(id, tileData);
		}
		if (!tileData.dataLoaded) {
			gcTiles();
			try {
				tileData.loaded++;
				log.info(String.format("SRTM: Load srtm data %d: %d %d", id, (int) lt, (int) ln));
				File missingFile = tileData.loadData(srtmDataUrl, srtmWorkingDir);
				if (fileName != null && fileName.length > 0) {
					fileName[0] = missingFile;
				}
				srtmCountDownload++;
				if (srtmCountDownload > MAX_SRTM_COUNT_DOWNLOAD) {
					throw new RuntimeException("Max count of download SRTM data " + MAX_SRTM_COUNT_DOWNLOAD);
				}
			} catch (IOException e) {
				log.error(e.getMessage(), e);
			}
		}
		tileData.accessed++;
		return tileData.getHeight(lonDelta, latDelta, neighboors);
	}



	private void gcTiles() {
		List<TileData> lst = new ArrayList<>(map.values());
		Iterator<TileData> it = lst.iterator();
		while (it.hasNext()) {
			if (it.next().data == null) {
				it.remove();
			}
		}
		if (MAXIMUM_LOADED_DATA != -1 && lst.size() >= MAXIMUM_LOADED_DATA) {
			int toGC = MAXIMUM_LOADED_DATA / 2;
			log.info(String.format("SRTM: GC srtm data %d of %d (total %d).", toGC, lst.size(), map.size()));
			// sort that more accessed are not gc
			Collections.sort(lst, new Comparator<TileData>() {

				@Override
				public int compare(TileData o1, TileData o2) {
					return -Integer.compare(o1.accessed + 100 * o1.loaded, o2.accessed + 100 * o2.loaded);
				}
			});
			for (int i = 0; i < lst.size(); i++) {
				TileData tile = lst.get(i);
				if (i > toGC) {
					// unload
					tile.dataLoaded = false;
					tile.data = null;
				}
				tile.accessed = 0;
			}
			System.gc();
		}
	}
	
	private static File loadFile(String fl, String folderURL, File workDir) {
		if (folderURL.startsWith("http://") || folderURL.startsWith("https://")) {
			File res = new File(workDir, fl);

			try {
				InputStream is = new URL(folderURL + fl).openStream();
				FileOutputStream fous = new FileOutputStream(res);
				Algorithms.streamCopy(is, fous);
				is.close();
				fous.close();
			} catch (IOException | RuntimeException e) {
				log.warn(String.format("Couldn't access height data %s at %s: %s", fl, folderURL, e.getMessage()), e);
			}
			return res;
		} else if(folderURL.startsWith("s3://")) {
			String url = folderURL.substring("s3://".length()) + fl;
			File res = new File(workDir, fl);
			int i = url.indexOf('/');
			String bucket = url.substring(0, i);
			String key = url.substring(i + 1);
			try {
				S3Client client = S3Client.builder().build();
				GetObjectRequest request = GetObjectRequest.builder().bucket(bucket).key(key).build();
				ResponseInputStream<GetObjectResponse> obj = client.getObject(request);
				FileOutputStream fous = new FileOutputStream(res);
				Algorithms.streamCopy(obj, fous);
				obj.close();
				fous.close();
			} catch (IOException | RuntimeException e) {
				log.warn(String.format("Couldn't access height data %s at %s: %s", fl, folderURL, e.getMessage()), e);
			}
			return res;
		} else {
			return new File(folderURL, fl);
		}
	}



	public static int getTileId(int lat, int lon) {
		int ln = (int) (lon + 180);
		int lt = (int) (lat  + 90);
		int ind = lt + (ln << 10);
		return ind;
	}

	public static void test(double lat, double lon) {
		int lt = (int) lat;
		int ln = (int) lon;
		double lonDelta = lon - ln;
		double latDelta = lat - lt;
		if(lonDelta < 0) {
			lonDelta += 1;
			ln -= 1;
		}
		if(latDelta < 0) {
			latDelta += 1;
			lt -= 1;
		}
		int id = getTileId(lt, ln);
		TileData td = new TileData(id);
		System.out.println(lat + " "  +lon + " (lat/lon) -> file " + td.getFileName() + " (y, x in %) " + (float) latDelta + " " + (float) lonDelta);
	}
	
	public static void main(String[] args) throws XmlPullParserException, IOException, ClassNotFoundException, InstantiationException, IllegalAccessException {
		IndexHeightData hd = new IndexHeightData();
		hd.setSrtmData("/Users/victorshcherb/osmand/maps/srtm/", null);
		
		test(50.5841136, 2.8897935);
		test(50.5841013, 2.8898731);
		USE_BILINEAR_INTERPOLATION = false;
		cmp(hd, 50.5841136, 2.8897935, 44);
		cmp(hd, 50.5841013, 2.8898731, 45);
//		USE_BILINEAR_INTERPOLATION = false;
//		simpleTestHeight(hd);
//		testHeight(hd);
		
//		Polygon plg = testFileSmoothness(hd);
//		long tms = System.currentTimeMillis();
//		List<Geometry> lst = generateGridPrecision(plg, 2.7, hd);
//		System.out.println((System.currentTimeMillis() - tms)  + " ms");
//		draw(plg, lst);
	}




	protected static Polygon testFileSmoothness(IndexHeightData hd) throws XmlPullParserException, IOException {
//		File fl = new File("/Users/victorshcherb/osmand/maps/route_laspi.gpx");
		File fl = new File("/Users/victorshcherb/osmand/route.gpx");
		List<Coordinate> res = new ArrayList<Coordinate>();
		XmlPullParser parser = PlatformUtil.newXMLPullParser();
		parser.setInput(new FileReader(fl));
		int next;
		List<LatLon> l = new ArrayList<>();
		List<Float> h = new ArrayList<>();
		double ele = 0;
		double SPECIAL_VALUE = -18000;
		double ROUTE_PRECISION = 150;
		String name = null;
		while ((next = parser.next()) != XmlPullParser.END_DOCUMENT) {
			if (next == XmlPullParser.START_TAG) {
				if (parser.getName().equals("trkpt")) {
					ele = SPECIAL_VALUE;
					LatLon ls = new LatLon(Float.parseFloat(parser.getAttributeValue("", "lat")),
							Float.parseFloat(parser.getAttributeValue("", "lon")));
					l.add(ls);
				} 
				name = parser.getName();
			} else if(next == XmlPullParser.TEXT) {
				if (name.equals("ele") && ele == SPECIAL_VALUE) {
					ele = Double.parseDouble(parser.getText());
					h.add((float) ele);
				}
			} else if (next == XmlPullParser.END_TAG) {
				if (parser.getName().equals("trkpt") && ele == SPECIAL_VALUE) {
					h.add(0f);
				} 
			}
		}
		
		float d = 0;
		int pnt = 0;
		for (int i = 0; i < l.size(); i++) {
			if (i == 0) {
				Coordinate c = new Coordinate(l.get(i).getLongitude(), l.get(i).getLatitude());
				res.add(c);
				System.out.println(0 + " " + h.get(0) + " ");
			} else {
				LatLon nxt = l.get(i);
				LatLon prv = l.get(i - 1);
				double dist = MapUtils.getDistance(prv, nxt);
				int cf = 1;
				while (dist / cf > ROUTE_PRECISION) {
					cf *= 2;
				}
				double plat = prv.getLatitude();
				double plon = prv.getLongitude();
				for (int j = 0; j < cf; j++) {
					double nlat = (nxt.getLatitude() - prv.getLatitude()) / cf + plat;
					double nlon = (nxt.getLongitude() - prv.getLongitude()) / cf + plon;
					d += MapUtils.getDistance(plat, plon, nlat, nlon);
					Coordinate c = new Coordinate(nlon, nlat);
					res.add(c);
					USE_BILINEAR_INTERPOLATION = true;
					double blh = hd.getPointHeight(nlat, nlon, null);
					USE_BILINEAR_INTERPOLATION = false;
					double bch = hd.getPointHeight(nlat, nlon, null);
					System.out.println(String.format("%d %.6f %.6f %.2f %.2f", pnt++, nlat, nlon, d, bch, blh));
					plat = nlat;
					plon = nlon;
				}
			}
		}
		
		Polygon polygon = new GeometryFactory().createPolygon(res.toArray(new Coordinate[res.size()]));
		return polygon;
	}



	private static List<Geometry> generateGridPrecision(Polygon plg, double precision, IndexHeightData hd) {
		Envelope e = plg.getEnvelopeInternal();
		GeometryFactory factory = plg.getFactory();
		int pw = (int) Math.pow(10, precision);
		int top = (int) Math.ceil((e.getMaxY() * pw));
		int bottom = (int) Math.floor((e.getMinY() * pw));
		int left = (int) Math.floor((e.getMinX() * pw));
		int right = (int) Math.ceil((e.getMaxX() * pw));
		
		System.out.println(
				String.format("Width %d, height %d, Top %.5f, bottom %.5f, left %.5f, right %.5f", 
						(right - left), (top - bottom), e.getMaxY(), e.getMinY(), e.getMinX(), e.getMaxX()));
		for(int x = left; x <= right; x++) {
			for(int y = top; y >= bottom; y--) {
				double nlon = x / (pw * 1.0);
				double nlat = y / (pw * 1.0);
				String fmt = "%." + (int)(Math.ceil(precision)) + "f";
				System.out.println(
						String.format(fmt + " " + fmt +" %.2f", nlat, nlon, hd.getPointHeight(nlat, nlon, null)));
			}
		}
		List<Geometry> r = new ArrayList<>();
		
		int numVertices = 0;
		int numPolygons = 0;
		for(int x = left; x < right; x++) {
			for(int y = top; y > bottom; y--) {
				for (int tr = 0; tr < 4; tr++) {
					Polygon gridCell = genCellTriangle(factory, pw, x, y, tr);
//					Polygon gridCell = genCell(factory, pw, x, y);
					if (gridCell.coveredBy(plg)) {
						r.add(gridCell);
						// sz += 4;
						numVertices += 1;
						numPolygons++;
					} else {
						// 1. intersection
						Geometry intersection = gridCell.intersection(plg);
						r.add(intersection);
						if (intersection.getNumPoints() >= 3) {
							numVertices += (intersection.getNumPoints() - 2);
							numPolygons++;
						}
						// 2. triangulation
//						DelaunayTriangulationBuilder builder = new DelaunayTriangulationBuilder();
//						builder.setSites(intersection.getBoundary());
//						Geometry triangles = builder.getTriangles(plg.getFactory());
//						System.out.println(intersection + " " + triangles);
//						for (int i = 0; i < triangles.getNumGeometries(); i++) {
//							Polygon triangle = (Polygon) triangles.getGeometryN(i);
//							r.add(triangle);
//						}
					}
				}
			}
		}
		System.out.println("Polygons " + numPolygons + " Min triangles " + numVertices);
		return r;
	}

	protected static Polygon genCellTriangle(GeometryFactory factory, int pw, int x, int y, int ind) {
		List<Coordinate> ln = new ArrayList<Coordinate>();
		ln.add(new Coordinate((x + 0.5)/ (pw * 1.0), (y - 0.5)/ (pw * 1.0)));
		// ind == 0: x 0, 1, y 0, 0
		// ind == 1: x 1, 1, y 0, 1
		// ind == 2: x 0, 1, y 1, 1
		// ind == 3: x 0, 0, y 0, 1
		ln.add(new Coordinate((x + (ind % 2 == 0 ? 0 : (ind == 1 ? 1 : 0))) / (pw * 1.0), (y - (ind % 2 == 1 ? 0 : (ind == 0 ? 0 : 1))) / (pw * 1.0)));
		ln.add(new Coordinate((x + (ind % 2 == 0 ? 1 : (ind == 1 ? 1 : 0))) / (pw * 1.0), (y - (ind % 2 == 1 ? 1 : (ind == 0 ? 0 : 1))) / (pw * 1.0)));
		ln.add(new Coordinate((x + 0.5) / (pw * 1.0), (y - 0.5) / (pw * 1.0)));
		Polygon gridCell = factory.createPolygon(ln.toArray(new Coordinate[ln.size()]));
		return gridCell;
	}

	protected static Polygon genCell(GeometryFactory factory, int pw, int x, int y) {
		List<Coordinate> ln = new ArrayList<Coordinate>();
		ln.add(new Coordinate(x / (pw * 1.0), y / (pw * 1.0)));
		ln.add(new Coordinate((x + 1)/ (pw * 1.0), y / (pw * 1.0)));
		ln.add(new Coordinate((x + 1)/ (pw * 1.0), (y - 1) / (pw * 1.0)));
		ln.add(new Coordinate(x / (pw * 1.0), (y - 1) / (pw * 1.0)));
		ln.add(new Coordinate(x / (pw * 1.0), y / (pw * 1.0)));
		Polygon gridCell = factory.createPolygon(ln.toArray(new Coordinate[ln.size()]));
		return gridCell;
	}
	
	protected static void simpleTestHeight(IndexHeightData hd) {
	    cmp(hd, 44.428722,33.711246, 255);
	}

	protected static void testHeight(IndexHeightData hd) {
		cmp(hd, 48.57522, 45.72296, -3);
		cmp(hd, 56.18137, 40.50929, 116);
		cmp(hd, 44.3992045, 33.9498114, 129.1);
		cmp(hd, 56.17828284774868, 40.5031156539917, 116);
		
		cmp(hd, 46.0, 9.0, 272);
		cmp(hd, 46.0, 9.99999, 1748);
		cmp(hd, 46.999999, 9.999999, 1121);
		cmp(hd, 46.999999, 9.0, 2834);

		cmp(hd, 46.0, 9.5, 1822);
		cmp(hd, 46.99999, 9.5, 531);
		
		cmp(hd, 46.05321, 9.94346, 1515);
		cmp(hd, 46.735, 9.287, 2311.3);
		cmp(hd, 46.291, 9.297, 1646.9);
		cmp(hd, 46.793, 9.878, 2174.2);
		cmp(hd, 46.774, 9.888, 1985.7);
		
		cmp(hd, 46.5, 9.5, 2436);		
		cmp(hd, 46.1, 9, 1441);
		cmp(hd, 46.7, 9, 2303);
	}

	private static void cmp(IndexHeightData hd, double lat, double lon, double exp) {
		double[] nh = new double[16];
		double pointHeight = hd.getPointHeight(lat, lon, null, nh);
		System.out.println("Lat " + lat + " lon " + lon);
		if (pointHeight != exp) {
			String extra = " NO NEIGHBOR FOUND " + Arrays.toString(nh);
			for (int k = 0; k < 9; k++) {
				if (nh[k] == exp) {
					extra = " neighbor (" + (k / 3 - 1) + " y, " + (k % 3 - 1) + " x) ";
				}
			}
			System.out.println(pointHeight + " (eval) != " + exp + " (exp) - " + extra);

		} else {
			System.out.println(pointHeight + " (eval) == " + exp + " (exp) ");
		}
		System.out.println("----------");

	}
	
	
	
	private static void draw(Polygon plg, List<Geometry> gothers) {
		int WIDTH = 1200;
		int HEIGHT = 800;
		int MARGIN = 250;

		AffineTransformation affine = new AffineTransformation();
		Envelope e = plg.getEnvelopeInternal();
		affine.translate(-e.getMinX(), -e.getMaxY());
		affine.scale((WIDTH - MARGIN) / e.getWidth(), (HEIGHT - MARGIN) / e.getHeight());
		affine.reflect(1, 0);
		affine.translate(MARGIN / 2, MARGIN / 2);

		ShapeWriter sw = new ShapeWriter();
		final Shape[] finalShapes = new Shape[1 + (gothers == null ? 0 : gothers.size())];
		int ind = 0;
		finalShapes[ind++] = sw.toShape(affine.transform(plg));
		if (gothers != null) {
			for (Geometry g : gothers) {
//				System.out.println(affine.transform(g));
				finalShapes[ind++] = sw.toShape(affine.transform(g));
			}
		}
		
//		JPanel tg = new JPanel() {
//			private static final long serialVersionUID = -371731683316195900L;
//
//			public void paintComponent(Graphics gphcs) {
//				super.paintComponent(gphcs);
//				this.setBackground(Color.WHITE);
//				for(int ind = 0; ind < finalShapes.length; ind++) {
//					if(ind == 0) {
//						gphcs.setColor(Color.GRAY );
//						((Graphics2D) gphcs).fill(finalShapes[ind]);
//					} else {
//						gphcs.setColor(Color.BLUE);
//						((Graphics2D) gphcs).draw(finalShapes[ind]);
//					}
//				}
//			}
//		};
//		JFrame frame = new JFrame("Test"); //$NON-NLS-1$
////		UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
//		Container content = frame.getContentPane();
//		frame.setFocusable(true);
//		frame.setBounds(0, 0, WIDTH, HEIGHT);
//		content.add(tg);
//		frame.setVisible(true);
//		frame.addWindowListener(new WindowAdapter() {
//			@Override
//			public void windowClosing(WindowEvent e) {
//				System.exit(0);
//			}
//		});

	}
}
