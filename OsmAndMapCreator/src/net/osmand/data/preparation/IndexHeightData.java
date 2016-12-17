package net.osmand.data.preparation;

import java.awt.image.BufferedImage;
import java.awt.image.DataBufferShort;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.imageio.ImageIO;

import net.osmand.PlatformUtil;
import net.osmand.osm.edit.Node;
import net.osmand.osm.edit.Way;

import org.apache.commons.logging.Log;

public class IndexHeightData {
	private File srtmData;
	
	private String ELE_ASC_START = "osmand_ele_start";
	private String ELE_ASC_END = "osmand_ele_end";
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
			int ln = (id >> 10) - 180;
			int lt = (id - ((id >> 10) << 10)) - 90;
			String nd = getId(ln, lt);
			File f = new File(folder, nd +".tif");
			BufferedImage img;
			if(f.exists()) {
				img = ImageIO.read(f);
				width = img.getWidth();
				height = img.getHeight();
				data = (DataBufferShort) img.getRaster().getDataBuffer(); 
			}
		}
		
		public double getHeight(double x, double y) {
			if (data == null) {
				return INEXISTENT_HEIGHT;
			}
			int px = (int) ((width - 1) * x);
			int py = (int) ((height - 1) * (1 - y));
			return data.getElem((px + 1) + (py + 1) * width) & 0xffff;
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
			lt = Math.abs(ln);
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
	
	public void proccess(Way e) {
		if(e.getTag("highway") == null && 
				e.getTag("cycleway") == null &&
				e.getTag("footway") == null) {
			return;
		}
		if(e.getTag("tunnel") != null || e.getTag("bridge") != null) {
			return;
		}
		double asc = 0;
		double desc = 0;
		
		List<Node> ns = e.getNodes();
		double prevHeight = INEXISTENT_HEIGHT;
		double firstHeight = INEXISTENT_HEIGHT;
		for(int i = 0; i < ns.size(); i++) {
			Node n = ns.get(i);
			if(n != null) {
				double pointHeight = getPointHeight(n.getLatitude(), n.getLongitude());
				if(prevHeight == INEXISTENT_HEIGHT) {
					firstHeight = pointHeight;
				} else {
					if(pointHeight > prevHeight) {
						asc += (pointHeight - prevHeight);
					} else if(prevHeight > pointHeight ) {
						desc += (prevHeight - pointHeight);
					}
				}
				prevHeight = pointHeight;
			}
			
		}
		if(firstHeight != INEXISTENT_HEIGHT) {
			e.putTag(ELE_ASC_START, ((int)firstHeight)+"");
		}
		if(prevHeight != INEXISTENT_HEIGHT) {
			e.putTag(ELE_ASC_END, ((int)prevHeight)+"");
		}
		if(asc >= 1){
			e.putTag(ELE_ASC_TAG, ((int)asc)+"");
		}
		if(desc >= 1){
			e.putTag(ELE_DESC_TAG, ((int)desc)+"");
		}
	}
	
	public void setSrtmData(File srtmData) {
		this.srtmData = srtmData;
	}
	
	public double getPointHeight(double lat, double lon) {
		int lt = (int) lat;
		int ln = (int) lon;
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
		return tileData.getHeight(lon - ln, lat - lt);
	}
	
	public int getTileId(int lat, int lon) {
		int ln = (int) (lon + 180);
		int lt = (int) (lat  + 90);
		int ind = lt + (ln << 10);
		return ind;
	}

	
	
	
}
