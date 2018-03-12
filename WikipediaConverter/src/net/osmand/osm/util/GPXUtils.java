package net.osmand.osm.util;

import org.xmlpull.v1.XmlSerializer;

import java.io.File;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;

public class GPXUtils {

	private final static String GPX_TIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'"; //$NON-NLS-1$

	private final static NumberFormat latLonFormat = new DecimalFormat("0.00#####", new DecimalFormatSymbols(
			new Locale("EN", "US")));
	
	public static class GPXExtensions {
		Map<String, String> extensions = null;

		public Map<String, String> getExtensionsToRead() {
			if (extensions == null) {
				return Collections.emptyMap();
			}
			return extensions;
		}

		public int getColor(int defColor) {
			if (extensions != null && extensions.containsKey("color")) {
				
			}
			return defColor;
		}

		public void setColor(int color) {
			getExtensionsToWrite().put("color", colorToString(color));
		}

		public Map<String, String> getExtensionsToWrite() {
			if (extensions == null) {
				extensions = new LinkedHashMap<>();
			}
			return extensions;
		}

	}

	
	public static class WptPt extends GPXExtensions {
		public double lat;
		public double lon;
		public String name = null;
		public String link = null;
		// previous undocumented feature 'category' ,now 'type'
		public String category = null;
		public String desc = null;
		public String comment = null;

		public WptPt() {
		}
		
		public void setColor() {
			if (category != null) {
				if (category.equals("see") || category.equals("do")) {
					super.setColor(15461130);
				} else if (category.equals("eat") || category.equals("drink")) {
					super.setColor(15400960);
				} else if (category.equals("sleep")) {
					super.setColor(3279595);
				} else if (category.equals("buy") || category.equals("listing")) {
					super.setColor(3336970);
				}
			}
		}

		public double getLatitude() {
			return lat;
		}

		public double getLongitude() {
			return lon;
		}

		public WptPt(double lat, double lon) {
			this.lat = lat;
			this.lon = lon;
		}

		public boolean hasLocation() {
			return (lat != 0 && lon != 0);
		}
	}

	public static class GPXFile extends GPXExtensions {
		public String author;
		private List<WptPt> points = new ArrayList<>();

		public String warning = null;
		public String path = "";
		public boolean showCurrentTrack;
		public long modifiedTime = 0;

		public List<WptPt> getPoints() {
			return Collections.unmodifiableList(points);
		}

		public boolean isPointsEmpty() {
			return points.isEmpty();
		}

		int getPointsSize() {
			return points.size();
		}

		boolean containsPoint(WptPt point) {
			return points.contains(point);
		}

		void clearPoints() {
			points.clear();
			modifiedTime = System.currentTimeMillis();
		}

		public void addPoint(WptPt point) {
			points.add(point);
			modifiedTime = System.currentTimeMillis();
		}

		void addPoints(Collection<? extends WptPt> collection) {
			points.addAll(collection);
			modifiedTime = System.currentTimeMillis();
		}

		public boolean hasWptPt() {
			return points.size() > 0;
		}

		public WptPt addWptPt(double lat, double lon, String description, String name, String category, int color) {
			double latAdjusted = Double.parseDouble(latLonFormat.format(lat));
			double lonAdjusted = Double.parseDouble(latLonFormat.format(lon));
			final WptPt pt = new WptPt(latAdjusted, lonAdjusted);
			pt.name = name;
			pt.category = category;
			pt.desc = description;
			if (color != 0) {
				pt.setColor(color);
			}
			points.add(pt);
			modifiedTime = System.currentTimeMillis();
			return pt;
		}

		public boolean deleteWptPt(WptPt pt) {
			modifiedTime = System.currentTimeMillis();
			return points.remove(pt);
		}

		public boolean isEmpty() {
			return points.isEmpty();
		}

		public List<String> getWaypointCategories() {
			List<String> categories = new ArrayList<>();
			for (WptPt pt : points) {
				String category = pt.category;
				if (!category.isEmpty()) {
					if (!categories.contains(category)) {
						categories.add(category);
					}
				}
			}
			return categories;
		}
	}

    public static String asString(GPXFile file) {
           final Writer writer = new StringWriter();
           GPXUtils.writeGpx(writer, file);
           return writer.toString();
       }

	public static String writeGpxFile(File fout, GPXFile file) {
		Writer output = null;
		try {
			output = new OutputStreamWriter(new FileOutputStream(fout), "UTF-8"); //$NON-NLS-1$
			return writeGpx(output, file);
		} catch (IOException e) {
			return "";
		} finally {
			if (output != null) {
				try {
					output.close();
				} catch (IOException ignore) {
					// ignore
				}
			}
		}
	}

	public static String writeGpx(Writer output, GPXFile file) {
		try {
			SimpleDateFormat format = new SimpleDateFormat(GPX_TIME_FORMAT, Locale.US);
			format.setTimeZone(TimeZone.getTimeZone("UTC"));
			XmlSerializer serializer = new org.kxml2.io.KXmlSerializer();;
			serializer.setOutput(output);
			serializer.setFeature("http://xmlpull.org/v1/doc/features.html#indent-output", true); //$NON-NLS-1$
			serializer.startDocument("UTF-8", true); //$NON-NLS-1$
			serializer.startTag(null, "gpx"); //$NON-NLS-1$
			serializer.attribute(null, "version", "1.1"); //$NON-NLS-1$ //$NON-NLS-2$
			if (file.author == null) {
				serializer.attribute(null, "creator", "OsmAnd"); //$NON-NLS-1$
			} else {
				serializer.attribute(null, "creator", file.author); //$NON-NLS-1$
			}
			serializer.attribute(null, "xmlns", "http://www.topografix.com/GPX/1/1"); //$NON-NLS-1$ //$NON-NLS-2$
			serializer.attribute(null, "xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance");
			serializer.attribute(null, "xsi:schemaLocation",
					"http://www.topografix.com/GPX/1/1 http://www.topografix.com/GPX/1/1/gpx.xsd");

			for (WptPt l : file.points) {
				serializer.startTag(null, "wpt"); //$NON-NLS-1$
				writeWpt(format, serializer, l);
				serializer.endTag(null, "wpt"); //$NON-NLS-1$
			}

			serializer.endTag(null, "gpx"); //$NON-NLS-1$
			serializer.flush();
			serializer.endDocument();
		} catch (RuntimeException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	private static void writeNotNullText(XmlSerializer serializer, String tag, String value) throws IOException {
		if (value != null) {
			serializer.startTag(null, tag);
			serializer.text(value);
			serializer.endTag(null, tag);
		}
	}

	private static void writeExtensions(XmlSerializer serializer, GPXExtensions p) throws IOException {
		if (!p.getExtensionsToRead().isEmpty()) {
			serializer.startTag(null, "extensions");
			for (Map.Entry<String, String> s : p.getExtensionsToRead().entrySet()) {
				writeNotNullText(serializer, s.getKey(), s.getValue());
			}
			serializer.endTag(null, "extensions");
		}
	}

	private static void writeWpt(SimpleDateFormat format, XmlSerializer serializer, WptPt p) throws IOException {
		serializer.attribute(null, "lat", latLonFormat.format(p.lat)); //$NON-NLS-1$ //$NON-NLS-2$
		serializer.attribute(null, "lon", latLonFormat.format(p.lon)); //$NON-NLS-1$ //$NON-NLS-2$		
		writeNotNullText(serializer, "name", p.name);
		writeNotNullText(serializer, "desc", p.desc);
		if (p.link != null) {
			serializer.startTag(null, "link");
			serializer.attribute(null, "href", p.link);
			serializer.endTag(null, "link");
		}
		writeNotNullText(serializer, "type", p.category);
		if (p.comment != null) {
			writeNotNullText(serializer, "cmt", p.comment);
		}
		writeExtensions(serializer, p);
	}
	
	public static String colorToString(int color) {
		if ((0xFF000000 & color) == 0xFF000000) {
			return "#" + format(6, Integer.toHexString(color & 0x00FFFFFF)); //$NON-NLS-1$
		} else {
			return "#" + format(8, Integer.toHexString(color)); //$NON-NLS-1$
		}
	}
	
	private static String format(int i, String hexString) {
		while (hexString.length() < i) {
			hexString = "0" + hexString;
		}
		return hexString;
	}
}
