package net.osmand.obf.preparation;

import net.osmand.PlatformUtil;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.awt.image.BufferedImage;
import java.awt.image.DataBufferFloat;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.metadata.IIOMetadata;
import javax.imageio.metadata.IIOMetadataFormatImpl;
import javax.imageio.stream.ImageInputStream;

public class IndexWeatherData {
	
	private static final Log log = PlatformUtil.getLog(IndexWeatherData.class);
	public static final int NEAREST_NEIGHBOOR_INTERPOLATION = 0;
	public static final int BILINEAR_INTERPOLATION = 1;
	public static final int BICUBIC_INTERPOLATION = 2;
	// BILINEAR_INTERPOLATION is used instead of BICUBIC_INTERPOLATION for precipitation and cloud data
	// to avoid the issue of negative values.
	public static int INTERPOLATION = BILINEAR_INTERPOLATION;
	public static final double INEXISTENT_VALUE = Double.MIN_VALUE;
	// 1440, 721 - -180.125, 90.125 - 179.8750000, -90.1250000
	public static final int REF_WIDTH = 1440;
	public static final int REF_HEIGHT = 721;
	private static final String ECWMF_WEATHER_TYPE = "ecmwf";

	private static final String TIFF_PLUGIN = "com_sun_media_imageio_plugins_tiff_image_1.0";
	private static final String TIFF_FIELD  = "TIFFField";
	private static final String METADATA_TAG_NUMBER  = "42112";
	
	public static class WeatherTiff {
		
		// it could vary base on files
		// gfs
		public double ORIGIN_LON = -180.125;
		public double ORIGIN_LAT = 90.125;
		public double PX_SIZE_LON = 0.25;
		public double PX_SIZE_LAT = -0.25;
		// ecmwf
		public double ECMWF_ORIGIN_LON = -180.1999;
		public double ECMWF_ORIGIN_LAT = 90.2000;
		public double ECMWF_PX_SIZE_LON = 0.28;
		public double ECMWF_PX_SIZE_LAT = -0.28;

		public final File file;
		private DataBufferFloat data;
		private int height;
		private int width;
		private int bands;
		private Map<Integer, String> bandData;
		
		public WeatherTiff(File file) {
			this.file = file;
			readTiffFile(file);
		}
		
		public int getBands() {
			return bands;
		}

		public Map<Integer, String> getBandData() { return bandData; }
		
		
		private void readTiffFile(File file) {
			if (file.exists()) {
				Pair<BufferedImage, String> p = iterativeReadData(file);
				BufferedImage img = p.getLeft();
				String metadata = p.getRight();
				readWeatherData(img);
				bandData = parseBandData(metadata);
				if (img == null) {
					log.error("Failed to read image data from file: " + file.getAbsolutePath());
				}
			} else {
				log.error("File does not exist: " + file.getAbsolutePath());
			}
		}
		
		private Pair<BufferedImage, String> iterativeReadData(File file) {
			boolean readSuccess = false;
			BufferedImage img = null;
			String metaData = null;
			Iterator<ImageReader> readers = ImageIO.getImageReadersBySuffix("tiff");
			ImageInputStream iis = null;
			while (readers.hasNext() && !readSuccess) {
				ImageReader reader = readers.next();
				if (reader instanceof com.sun.media.imageioimpl.plugins.tiff.TIFFImageReader) {
					try {
						iis = ImageIO.createImageInputStream(file);
						reader.setInput(iis, true);
						metaData = readMetaData(reader);
						img = reader.read(0);
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
			return Pair.of(img, metaData);
		}

		private String readMetaData(ImageReader reader) throws IOException {
			IIOMetadata md = reader.getImageMetadata(0);
			Node root = null;
			try { root = md.getAsTree(TIFF_PLUGIN); } catch (IllegalArgumentException ignore) {}
			if (root == null) {
				try { root = md.getAsTree(IIOMetadataFormatImpl.standardMetadataFormatName); } catch (IllegalArgumentException ignore) {}
			}
			if (!(root instanceof org.w3c.dom.Element)) return null;

			NodeList fields = ((Element) root).getElementsByTagName(TIFF_FIELD);
			String metadata = null;
			for (int i = 0; i < fields.getLength() && metadata == null; i++) {
				Element f = (Element) fields.item(i);
				if (METADATA_TAG_NUMBER.equals(f.getAttribute("number"))) {
					for (Node c = f.getFirstChild(); c != null; c = c.getNextSibling()) {
						String n = c.getNodeName();
						if ("TIFFAscii".equals(n)) {
							NamedNodeMap av = c.getAttributes();
							if (av != null && av.getNamedItem("value") != null) metadata = av.getNamedItem("value").getNodeValue();
							else if (c.getFirstChild() != null) metadata = c.getFirstChild().getNodeValue();
						} else if ("TIFFAsciis".equals(n)) {
							StringBuilder sb = new StringBuilder();
							for (Node a = c.getFirstChild(); a != null; a = a.getNextSibling()) {
								if ("TIFFAscii".equals(a.getNodeName())) {
									NamedNodeMap av = a.getAttributes();
									if (av != null && av.getNamedItem("value") != null) sb.append(av.getNamedItem("value").getNodeValue());
									else if (a.getFirstChild() != null) sb.append(a.getFirstChild().getNodeValue());
								}
							}
							if (!sb.isEmpty()) metadata = sb.toString();
						}
					}
				}
			}
			return metadata;
		}

		private Map<Integer, String> parseBandData(String metadata) {
			Map<Integer, String> out = new HashMap<>();
			if (metadata == null || metadata.isEmpty()) return out;
			// ex. <Item name="DESCRIPTION" sample="0" role="description">TCDC:entire atmosphere</Item>
			Pattern p = Pattern.compile(
					"<Item\\s+name\\s*=\\s*\"DESCRIPTION\"\\s+sample\\s*=\\s*\"(\\d+)\"[^>]*>(.*?)</Item>",
					Pattern.CASE_INSENSITIVE | Pattern.DOTALL
			);
			Matcher m = p.matcher(metadata);
			while (m.find()) {
				int sample = Integer.parseInt(m.group(1));
				String text = m.group(2) == null ? "" : m.group(2).trim();
				int colon = text.indexOf(':');
				String code = (colon >= 0 ? text.substring(0, colon) : text).trim();
				if (!code.isEmpty()) out.put(sample, code);
			}
			return out;
		}
		
		private void readWeatherData(BufferedImage img) {
			if (img != null) {
				width = img.getWidth();
				height = img.getHeight();
				data = (DataBufferFloat) img.getRaster().getDataBuffer();
				bands = data.getSize() / width / height;
			}
		}
		
		public double getValue(int band, double lat, double lon, String weatherType) {
			double y;
			double x;
			if (weatherType.equals(ECWMF_WEATHER_TYPE)) {
				y = (lat - ECMWF_ORIGIN_LAT) / ECMWF_PX_SIZE_LAT;
				x = (lon - ECMWF_ORIGIN_LON) / ECMWF_PX_SIZE_LON;
			} else {
				y = (lat - ORIGIN_LAT) / PX_SIZE_LAT;
				x = (lon - ORIGIN_LON) / PX_SIZE_LON;
			}
			if (y < 0 || y > height || x < 0 || x > width) {
				return INEXISTENT_VALUE;
			}
			return getInterpolationValue(band, x, y, null);
		}
		
		public double getInterpolationValue(int band, double x, double y, double[] array) {
			if (data == null) {
				return INEXISTENT_VALUE;
			}
			if (INTERPOLATION == BILINEAR_INTERPOLATION) {
				return bilinearInterpolation(band, x, y, array);
			} else if (INTERPOLATION == BICUBIC_INTERPOLATION) {
				return bicubicInterpolation(band, x, y, array);
			} else {
				return nearestNeighboor(band, x, y);
			}
		}
		
		protected double nearestNeighboor(int band, double x, double y) {
			int px = (int) Math.round(x);
			int py = (int) Math.round(y);
			return getElem(band, px, py);
		}

		protected double getElem(int band, int px, int py) {
			if (px < 0) {
				px = 0;
			}
			if(py < 0) {
				py = 1;
			}
			if (px > width - 1) {
				px = width - 1;
			}
			if (py >= height - 1) {
				py = height - 1;
			}
			int ind = (px + py * width) * bands + band;
			if (ind >= data.getSize()) {
				throw new IllegalArgumentException("Illegal access (" + px + ", " + py + ") " + ind + " - "
						+ file.getName());
			}
			return data.getElemDouble(ind) ;
		}
		
		protected double bilinearInterpolation(int band, double x, double y, double[] array) {
			int px = (int) Math.ceil(x);
			int py = (int) Math.ceil(y);
			if(array == null) {
				array = new double[4];
			}
			array[0] = getElem(band, px - 1, py - 1);
			array[1] = getElem(band, px, py - 1);
			array[2] = getElem(band, px - 1, py);
			array[3] = getElem(band, px, py);
			double cx = x + 1 - px;
			double cy = y + 1 - py;
			// 1.1 x  -> px = 2, cx = 0.1
			// 1.99 y ->  py = 2, cy = 0.99
			// array[2] -> maximize

			return (1 - cx) * (1 - cy) * array[0] +
					         cx * (1 - cy) * array[1] +
					   (1 - cx) * cy       * array[2] +
					         cx * cy       * array[3];
		}
		
		protected double bicubicInterpolation(int band, double ix, double iy, double[] cf) {
			int px = (int) Math.floor(ix);
			int py = (int) Math.floor(iy);
			double x = ix - px;
			double y = iy - py;
			if(cf == null) {
				cf = new double[16];
			}
			Arrays.fill(cf, 0);
			// https://en.wikipedia.org/wiki/Bicubic_interpolation
			cf[0] = (x-1)*(x-2)*(x+1)*(y-1)*(y-2)*(y+1) / 4 * getElem(band, px, py);
			cf[1] = -(x)*(x-2)*(x+1)*(y-1)*(y-2)*(y+1) / 4 * getElem(band, px, py + 1);
			cf[2] = -(x-1)*(x-2)*(x+1)*(y)*(y-2)*(y+1) / 4 * getElem(band, px + 1, py);
			cf[3] = (x)*(x-2)*(x+1)*(y)*(y-2)*(y+1) / 4 * getElem(band, px + 1, py + 1);
			cf[4] = -(x)*(x-2)*(x-1)*(y-1)*(y-2)*(y+1) / 12 * getElem(band, px, py - 1);
			cf[5] = -(x+1)*(x-2)*(x-1)*(y-1)*(y-2)*(y) / 12 * getElem(band, px - 1, py);
			cf[6] = (x)*(x-2)*(x-1)*(y+1)*(y-2)*(y) / 12 * getElem(band, px + 1, py - 1);
			cf[7] = (x)*(x-2)*(x+1)*(y-1)*(y-2)*(y) / 12 * getElem(band, px - 1, py + 1);
			cf[8] = (x)*(x-1)*(x+1)*(y-1)*(y-2)*(y+1) / 12 * getElem(band, px, py + 2);
			cf[9] = (x-2)*(x-1)*(x+1)*(y-1)*(y)*(y+1) / 12 * getElem(band, px + 2, py);
			cf[10] = (x)*(x-1)*(x-2)*(y)*(y-1)*(y-2) / 36 * getElem(band, px - 1, py - 1);
			cf[11] = -(x)*(x-1)*(x+1)*(y)*(y+1)*(y-2) / 12 * getElem(band, px + 1, py + 2);
			cf[12] = -(x)*(x+1)*(x-2)*(y)*(y-1)*(y+1) / 12 * getElem(band, px + 2, py + 1);
			cf[13] = -(x)*(x-1)*(x+1)*(y)*(y-1)*(y-2) / 36 * getElem(band, px - 1, py + 2);
			cf[14] = -(x)*(x-1)*(x-2)*(y)*(y-1)*(y+1) / 36 * getElem(band, px + 2, py - 1);
			cf[15] =  (x)*(x-1)*(x+1)*(y)*(y-1)*(y+1) / 36 * getElem(band, px + 2, py + 2);
			double h = 0;
			for (double v : cf) {
				h += v;
			}
			return h;
		}
	}

	public static void main(String[] args) {
		readWeatherData("/Users/plotva/osmand/weather/gfs/",
				"20250913_%02d00.tiff", 8, 23, 1);
	}

	private static void readWeatherData(String folder, String fmt, int min, int max, int step) {
		double lat = 52.3121;
		double lon = 4.8880;
		int len = (max + 1 - min) / step;
		double[][] wth = new double[6][len];

		for (int i = 0; i < len; i++) {
			int vl = min + step * i;
			File f = new File(folder, String.format(fmt, vl));
			WeatherTiff td = new WeatherTiff(f);

			Map<Integer, String> codes = td.getBandData();
			Integer iCloud = bandIndexByCode(codes, WeatherParam.CLOUD.code);
			Integer iTemp  = bandIndexByCode(codes, WeatherParam.TEMP.code);
			Integer iPres  = bandIndexByCode(codes, WeatherParam.PRESSURE.code);
			Integer iWind  = bandIndexByCode(codes, WeatherParam.WIND.code);
			Integer iPrec  = bandIndexByCode(codes, WeatherParam.PRECIP.code);

			wth[0][i] = vl;
			wth[1][i] = iTemp  != null ? td.getValue(iTemp,  lat, lon, ECWMF_WEATHER_TYPE) : INEXISTENT_VALUE;
			wth[2][i] = iPrec  != null ? td.getValue(iPrec,  lat, lon, ECWMF_WEATHER_TYPE) : INEXISTENT_VALUE;
			wth[3][i] = iWind  != null ? td.getValue(iWind,  lat, lon, ECWMF_WEATHER_TYPE) : INEXISTENT_VALUE;
			wth[4][i] = iPres  != null ? td.getValue(iPres,  lat, lon, ECWMF_WEATHER_TYPE) : INEXISTENT_VALUE;
			wth[5][i] = iCloud != null ? td.getValue(iCloud, lat, lon, ECWMF_WEATHER_TYPE) : INEXISTENT_VALUE;
		}

		System.out.println("TIME    :      " + format("%3.0f:00", wth[0]));
		System.out.println("Temp (C):      " + format("%6.1f", wth[1]));
		System.out.println("Precipitation: " + format("%6.2f", wth[2], 1000 * 1000));
		System.out.println("Wind (m/s):    " + format("%6.2f", wth[3]));
		System.out.println("Pressure (kPa):" + format("%6.2f", wth[4], 0.001));
		System.out.println("Cloud %%:      " + format("%6.2f", wth[5]));
	}

	private static String format(String fmt, double[] ds) {
		return format(fmt, ds, 1);
	}

	private static String format(String fmt, double[] ds, double mult) {
		String s = "";
		for (int i = 0; i < ds.length; i++) {
			if (i > 0) {
				s += "  ";
			}
			s += String.format(fmt, ds[i] * mult);

		}
		return s;
	}

	public enum WeatherParam {
		TEMP("TMP",    "temp"),
		PRESSURE("PRMSL","press"),
		WIND("GUST",   "wind"),
		PRECIP("PRATE","precip"),
		CLOUD("TCDC",  "cloud");

		public final String code;
		final String field;
		WeatherParam(String code, String field) { this.code = code; this.field = field; }
	}

	public static Integer bandIndexByCode(Map<Integer,String> sampleToCode, String code) {
		if (sampleToCode == null) return null;
		for (Map.Entry<Integer,String> e : sampleToCode.entrySet()) {
			if (code.equals(e.getValue())) return e.getKey();
		}
		return null;
	}
}
