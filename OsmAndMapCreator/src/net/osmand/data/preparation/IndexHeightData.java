package net.osmand.data.preparation;

import java.awt.image.BufferedImage;
import java.awt.image.DataBufferShort;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.imageio.ImageIO;

import net.osmand.PlatformUtil;
import net.osmand.osm.edit.Node;
import net.osmand.osm.edit.Way;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;

import org.apache.commons.logging.Log;

public class IndexHeightData {
	private File srtmData;
	
	private String ELE_ASC_START = "osmand_ele_start";
	private String ELE_ASC_END = "osmand_ele_end";
	private String ELE_INCLINE = "osmand_ele_incline_";
	private String ELE_INCLINE_MAX = "osmand_ele_incline_max";
	private String ELE_DECLINE = "osmand_ele_decline_";
	private String ELE_DECLINE_MAX = "osmand_ele_decline_max";
	private String ELE_ASC_TAG = "osmand_ele_asc";
	private String ELE_DESC_TAG = "osmand_ele_desc";
	private static double INEXISTENT_HEIGHT = Double.MIN_VALUE;
	private Map<Integer, TileData> map = new HashMap<Integer, TileData>();

	private Log log = PlatformUtil.getLog(IndexHeightData.class);
	
	private static class TileData {
		DataBufferShort data;
		private int id;
		private boolean dataLoaded;
		private int height;
		private int width;
		
		private TileData(int id) {
			this.id = id;
			
		}
		
		public void loadData(File folder) throws IOException {
			dataLoaded = true;
			String nd = getFileName();
			File f = new File(folder, nd +".tif");
			BufferedImage img;
			if(f.exists()) {
				img = ImageIO.read(f);
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
			// 1.3 pdx ->  0 ( 0.5 : 0.2 = 1 - cx), 1 px (1.5 : 0.8 = cx),
			// 1.7 pdx ->  1 ( 1.5 : 0.8 = 1 - cx), 2 px (2.5 : 0.2 = cx), 
			double cx = 0.5 + pdx - px;
			double cy = 0.5 + pdy - py;
			double h = (1 - cx) * (1 - cy) * array[0] + 
					         cx * (1 - cy) * array[1] + 
					   (1 - cx) * cy       * array[2] + 
					         cx * cy       * array[3];
			
			System.out.println(" Y:" + (float) pdy + " X:" + (float) pdx);
			return h;
		}

		private double getElem(int px, int py) {
			int ind = px + py * width;
			if (ind >= data.getSize()) {
				throw new IllegalArgumentException("Illegal access (" + px + ", " + py + ") " + ind + " - "
						+ getFileName());
			}
			return data.getElem(ind) & 0xffff;
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
		
		
		void processHeight(double pointHeight, double prevHeight, double dist, Node n) {
			if (firstHeight == INEXISTENT_HEIGHT) {
				firstHeight = prevHeight;
			}
			lastHeight = pointHeight;
			double diff = (pointHeight - prevHeight);
			int df = (int) diff;
			if(df == 0) {
				return;
			}
			// double deg = Math.abs(Math.atan2(pointHeight - prevHeight, dist) / Math.PI * 180);
			double degIncline = Math.abs((pointHeight - prevHeight) /  dist * 100); 
			
			double[] arr ;
			if (df > 0) {
				arr = ascIncline;
				n.putTag(ELE_ASC_TAG, df+"");
			} else  {
				arr = descIncline;
				n.putTag(ELE_DESC_TAG, -df+"");
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
			
		}
	}
	
	public void proccess(Way e) {
		if(e.getTag("highway") == null && 
				e.getTag("cycleway") == null &&
				e.getTag("footway") == null) {
			return;
		}
		if(e.getTag("tunnel") != null || e.getTag("bridge") != null) {
			return;
		}
		WayHeightStats wh = new WayHeightStats();
		
		List<Node> ns = e.getNodes();
		double prevHeight = INEXISTENT_HEIGHT;
		Node prev = null;
		for(int i = 0; i < ns.size(); i++) {
			Node n = ns.get(i);
			if (n != null) {
				double pointHeight = getPointHeight(n.getLatitude(), n.getLongitude());
				if (prev == null) {
					if(pointHeight != INEXISTENT_HEIGHT) {
						prevHeight = pointHeight;
						prev = n;
					}
				} else {
					double segm = MapUtils.getDistance(prev.getLatitude(), prev.getLongitude(), n.getLatitude(),
							n.getLongitude());
					if (segm > 20 && pointHeight != INEXISTENT_HEIGHT) {
						wh.processHeight(pointHeight, prevHeight, segm, n);
						prevHeight = pointHeight;
						prev = n;
					}
				}
			}
		}
		if(wh.firstHeight != INEXISTENT_HEIGHT && wh.lastHeight != INEXISTENT_HEIGHT) {
			e.putTag(ELE_ASC_START, ((int)wh.firstHeight)+"");
		}
		if(wh.lastHeight != INEXISTENT_HEIGHT && wh.firstHeight != wh.lastHeight) {
			e.putTag(ELE_ASC_END, ((int)wh.lastHeight)+"");
		}
//		for(int k = 0; k < wh.SIZE; k++) {
//			int deg = wh.DEGREE_START + k * wh.DEGREE_PRECISION;
//			if (wh.ascIncline[k] > 0) {
//				e.putTag(ELE_INCLINE + deg, ((int) wh.ascIncline[k]) + "");
//			}
//			if (wh.descIncline[k] > 0) {
//				e.putTag(ELE_DECLINE + deg, ((int) wh.descIncline[k]) + "");
//			}
//		}
//		if(wh.asc >= 1){
//			e.putTag(ELE_ASC_TAG, ((int)wh.asc)+"");
//		}
//		if(wh.desc >= 1){
//			e.putTag(ELE_DESC_TAG, ((int)wh.desc)+"");
//		}
	}
	
	public void setSrtmData(File srtmData) {
		this.srtmData = srtmData;
	}
	
	public double getPointHeight(double lat, double lon) {
		return getPointHeight(lat, lon, null);
	}
	
	public double getPointHeight(double lat, double lon, double[] neighboors) {
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
		TileData tileData = map.get(id);
		if(tileData == null) {
			tileData = new TileData(id);
			map.put(id, tileData);
		}
		if(!tileData.dataLoaded) {
			try {
				tileData.loadData(srtmData);
			} catch (IOException e) {
				log.error(e.getMessage(), e);
			}
		}
		
		return tileData.getHeight(lonDelta, latDelta, neighboors);
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
	
	public static void main(String[] args) {
//		test(0.3, 3.1);
//		test(-0.4, 3.2);
//		test(-1.3, -3.1);
		testHeight();
	}

	private static void testHeight() {
		IndexHeightData hd = new IndexHeightData();
		hd.setSrtmData(new File("/Users/victorshcherb/osmand/maps/srtm/"));
		
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
		double[] nh = new double[9];
		double pointHeight = hd.getPointHeight(lat, lon, nh);
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
	
	
}
