package net.osmand.obf.preparation;

import java.awt.image.BufferedImage;
import java.awt.image.DataBufferFloat;
import java.io.File;
import java.io.IOException;

import javax.imageio.ImageIO;

public class IndexWeatherData {
	public static final int NEAREST_NEIGHBOOR_INTERPOLATION = 0;
	public static final int BILINEAR_INTERPOLATION = 1;
	public static final int BICUBIC_INTERPOLATION = 2;
	public static int INTERPOLATION = BICUBIC_INTERPOLATION;


	public static final double INEXISTENT_VALUE = Double.MIN_VALUE;
	// 1440, 721 - -180.125, 90.125 - 179.8750000, -90.1250000
	public static final int REF_WIDTH = 1440;
	public static final int REF_HEIGHT = 721;

	public static class WeatherTiff {

		// it could vary base on files
		public double ORIGIN_LON = -180.125;
		public double ORIGIN_LAT = 90.125;
		public double PX_SIZE_LON = 0.25;
		public double PX_SIZE_LAT = -0.25;

		public final File file;
        private DataBufferFloat data;
		private int height;
		private int width;
		private int bands;

		public WeatherTiff(File file) throws IOException {
			this.file = file;
			readFile(file);
		}

		public int getBands() {
			return bands;
		}


		private BufferedImage readFile(File file) throws IOException {
			BufferedImage img = null;
			if (file.exists()) {
				img = ImageIO.read(file);
				width = img.getWidth();
				height = img.getHeight();
                data = (DataBufferFloat) img.getRaster().getDataBuffer();
				bands = data.getSize() / width / height;
			}
			return img;
		}


		public double getValue(int band, double lat, double lon) {
			double y = (lat - ORIGIN_LAT) / PX_SIZE_LAT;
			double x = (lon - ORIGIN_LON) / PX_SIZE_LON;
			if (y < 0 || y > height || x < 0 || x > width) {
				return INEXISTENT_VALUE;
			}
			return getValue(band, x, y, null);
		}

		public double getValue(int band, double x, double y, double[] array) {
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
			// System.out.println(" --- " + (h1 - h2) + " " + h1 + " " + h2);
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

			double h = (1 - cx) * (1 - cy) * array[0] +
					         cx * (1 - cy) * array[1] +
					   (1 - cx) * cy       * array[2] +
					         cx * cy       * array[3];
			return h;
		}

		protected double bicubicInterpolation(int band, double ix, double iy, double[] cf) {
			int px = (int) Math.floor(ix);
			int py = (int) Math.floor(iy);
			double x = ix - px;
			double y = iy - py;
			if(cf == null) {
				cf = new double[16];
			}
			for (int i = 0; i < cf.length; i++) {
				cf[i] = 0;
			}
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
			for(int i = 0; i < cf.length; i++) {
				h += cf[i];
			}
			return h;
		}
	}

	public static void main(String[] args) throws IOException {
        readWeatherData("/Users/nnngrach/Downloads/Weather_results/8_09/new/tiff/", "20220908_0000.tiff", 8, 23, 1);

//		readWeatherData("/Users/victorshcherb/osmand/maps/weather/",
//				"20220206_%02d00.tiff", 8, 23, 1);
	}

	private static void readWeatherData(String folder, String fmt, int min, int max, int step) throws IOException {
		double lat = 52.3121;
		double lon = 4.8880;
		int len = (max + 1 - min) / step;
		double[][] wth = new double[6][len];
		long ms = System.currentTimeMillis();
		for (int i = 0; i < len; i++) {
			int vl = min + step * i;
			WeatherTiff td = new WeatherTiff(new File(folder, String.format(fmt, vl)));
//			System.out.println(vl + ":00");
			for (int j = 0; j < 5; j++) {
				wth[j + 1][i] = td.getValue(j, lat, lon);
//				System.out.println(td.getElem(j, 740, 151));
			}
			wth[0][i] = vl;
		}
		System.out.println("TIME    :      " + format("%3.0f:00", wth[0]));
		System.out.println("Cloud %%:      " + format("%6.2f", wth[1]));
		System.out.println("Temp (C):      " + format("%6.1f", wth[2]));
		System.out.println("Pressure (kPa):" + format("%6.2f", wth[3], 0.001));
		System.out.println("Wind (m/s):    " + format("%6.2f", wth[4]));
		System.out.println("Precipitation: " + format("%6.2f", wth[5], 1000 * 1000)); // (mg/(m^2 s)
		System.out.println((System.currentTimeMillis() - ms) + " ms");
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
}
