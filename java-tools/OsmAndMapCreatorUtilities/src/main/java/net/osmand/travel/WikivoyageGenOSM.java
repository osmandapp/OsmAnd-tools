package net.osmand.travel;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.logging.Log;
import org.xmlpull.v1.XmlSerializer;

import net.osmand.GPXUtilities;
import net.osmand.GPXUtilities.GPXFile;
import net.osmand.GPXUtilities.WptPt;
import net.osmand.PlatformUtil;
import net.osmand.data.LatLon;
import net.osmand.obf.preparation.DBDialect;
import net.osmand.util.Algorithms;

public class WikivoyageGenOSM {
	public static final String CAT_SEE = "see"; // 27%
	public static final String CAT_MARKER = "marker"; // 20%
	public static final String CAT_SLEEP = "sleep"; // 15%
	public static final String CAT_EAT = "eat"; // 12%
	public static final String CAT_DO = "do"; // 10%
	public static final String CAT_DRINK = "drink"; // 5%
	public static final String CAT_BUY = "buy"; // 4%
	public static final String CAT_OTHER = "other"; // 10%

	private static final Log log = PlatformUtil.getLog(WikivoyageGenOSM.class);
	private static final int LIMIT = 1000;
	private final static NumberFormat latLonFormat = new DecimalFormat("0.00#####", new DecimalFormatSymbols());
	
	
	static long NODE_ID = -1000;
	static Map<String, Integer> categories = new HashMap<>();
	static Map<String, String> categoriesExample = new HashMap<>();
	
	
	
	
	public static void main(String[] args) throws SQLException, IOException {
		File f = new File("/Users/victorshcherb/osmand/maps/wikivoyage.sqlite");
		genWikivoyageOsm(f, new File(f.getParentFile(), "wikivoyage.osm.gz"));
	}

	
	private static class CombinedWikivoyageArticle {
		long tripId = -1;
		List<String> titles = new ArrayList<>();
		List<LatLon> latlons = new ArrayList<>();
		List<String> langs = new ArrayList<>();
		List<String> contents = new ArrayList<>();
		List<GPXFile> points = new ArrayList<>();
		
		
		public void addArticle(String lang, String title, GPXFile gpxFile, double lat, double lon, String content) {
			points.add(gpxFile);
			titles.add(title);
			langs.add(lang);
			LatLon ll = null;
			if(lat != 0 && lon != 0) {
				ll = new LatLon(lat, lon);
			}
			latlons.add(ll);
			contents.add(content);
		}
		
		public void clear() {
			titles.clear();
			langs.clear();
			points.clear();
			contents.clear();
			latlons.clear();
		}

		public List<String> getCategories(List<String> names) {
			List<String> cats = new ArrayList<>();
			int i = 0;
			for (GPXFile f : points) {
				for (WptPt p : f.getPoints()) {
					String cat = simplifyWptCategory(p.category, null);
					cats.add(cat);
					if (names != null) {
						names.add(langs.get(i) + " " + titles.get(i) + " " + p.name);
					}
				}
				i++;
			}
			return cats;
		}
	}

	/**
	 * @param filepath
	 * @throws SQLException
	 * @throws IOException
	 */
	/**
	 * @param wikivoyageFile
	 * @throws SQLException
	 * @throws IOException
	 */
	private static void genWikivoyageOsm(File wikivoyageFile, File outputFile) throws SQLException, IOException {
		DBDialect dialect = DBDialect.SQLITE;
		Connection connection = (Connection) dialect.getDatabaseConnection(wikivoyageFile.getCanonicalPath(), log );
		Statement statement = connection.createStatement();
		// popular_articles : trip_id, popularity_index, order_index, population, title, lat, lon, lang
		// travel_articles: 	image_title
		// 					 	is_part_of, aggregated_part_of, contents_json
		// 						population, country, region, city_type, osm_i,
		ResultSet rs = statement.executeQuery("select trip_id, title, lang, lat, lon, content_gz, gpx_gz from travel_articles order by trip_id asc");
		int count = 0, emptyLocation = 0, emptyContent = 0;
		CombinedWikivoyageArticle cac = new CombinedWikivoyageArticle();
		XmlSerializer serializer = null;
		OutputStream outputStream = null;
		if(outputFile != null) {
			outputStream = new FileOutputStream(outputFile);
			if(outputFile.getName().endsWith(".gz")) {
				outputStream = new GZIPOutputStream(outputStream);
			}
			serializer = PlatformUtil.newSerializer();
			serializer.setOutput(new OutputStreamWriter(outputStream));
			serializer.setFeature("http://xmlpull.org/v1/doc/features.html#indent-output", true); //$NON-NLS-1$
			serializer.startDocument("UTF-8", true); //$NON-NLS-1$
			serializer.startTag(null, "osm"); //$NON-NLS-1$
			serializer.attribute(null, "version", "0.6"); //$NON-NLS-1$ //$NON-NLS-2$
		}
		
		while (rs.next()) {
			int rind = 1;
			long tripId = rs.getLong(rind++);
			if (tripId != cac.tripId && cac.tripId != -1) {
				wrap(cac, serializer);
			}
			cac.tripId = tripId;
			String title = rs.getString(rind++);
			String lang = rs.getString(rind++);
			double lat = rs.getDouble(rind++);
			double lon = rs.getDouble(rind++);
//			rind++;
			String content = Algorithms.gzipToString(rs.getBytes(rind++));
			GZIPInputStream bytesStream = new GZIPInputStream(new ByteArrayInputStream(rs.getBytes(rind++)));
			GPXFile gpxFile = GPXUtilities.loadGPXFile(bytesStream);
			cac.addArticle(lang, title, gpxFile, lat, lon, content);
			if (gpxFile == null || gpxFile.isPointsEmpty()) {
				if(lat == 0 && lon == 0) {
					emptyLocation++;
				} else {
					emptyContent++;
				}
			}
			if (count >= LIMIT && LIMIT != -1) {
				break;
			}
			count++;
		}
		wrap(cac, serializer);
		if(serializer != null) {
			serializer.endTag(null, "osm");
			serializer.flush();
			outputStream.close();
		}
		List<String > l = new ArrayList<String>(categories.keySet());
		Collections.sort(l, new Comparator<String>() {

			@Override
			public int compare(String s1, String s2) {
				return Integer.compare(categories.get(s1), categories.get(s2));
			}
		});
		int total = 0;
		for(String s : l) {
			total += categories.get(s);
		}
		for(String s : l) {
			int cnt = categories.get(s);
			System.out.println(String.format("%#.2f%% %s  %d %s", cnt * 100.0 / total, s,  cnt, 
					categoriesExample.get(s)));
		}
		System.out.println(String.format("Empty article: %d no points  + %d no location out of %d", emptyContent, emptyLocation, count));
	}
	
	private static void tagValue(XmlSerializer serializer, String tag, String value) throws IOException {
		if (Algorithms.isEmpty(value)) {
			return;
		}
		serializer.startTag(null, "tag");
		serializer.attribute(null, "k", tag);
		serializer.attribute(null, "v", value);
		serializer.endTag(null, "tag");
	}

	

	private static void wrap(CombinedWikivoyageArticle article, XmlSerializer serializer) throws IOException {
		List<String> lst = new ArrayList<>();
		List<String> cats = article.getCategories(lst);
		for(int i = 0; i< cats.size() ; i++) {
			String cat = cats.get(i);
			String name = lst.get(i);
			Integer nt = categories.get(cat);
			if(nt == null) {
				categoriesExample.put(cat, name);
			}
			categories.put(cat, nt == null ? 1 : (nt.intValue() + 1));
		}
		int i = 0;
		for (GPXFile f : article.points) {
			for (WptPt p : f.getPoints()) {
				if (p.lat >= 90 || p.lat <= -90 || p.lon >= 180 || p.lon <= -180) {
					continue;
				}
				String cat = simplifyWptCategory(p.category, CAT_OTHER);
				serializer.startTag(null, "node");
				serializer.attribute(null, "id", NODE_ID-- + "");
				serializer.attribute(null, "action", "modify");
				serializer.attribute(null, "version", "1");
				serializer.attribute(null, "lat", latLonFormat.format(p.lat));
				serializer.attribute(null, "lon", latLonFormat.format(p.lon));
				String lng = article.langs.get(i);
				tagValue(serializer, "description:" + lng, p.desc);
				tagValue(serializer, "name:" + lng, p.name);
				tagValue(serializer, "wikivoyage_title:" + lng, article.titles.get(i));
				tagValue(serializer, "wikivoyage", "point");
				tagValue(serializer, "wikivoyage:" + lng, "yes");
				if ("en".equals(lng)) {
					tagValue(serializer, "description", p.desc);
					tagValue(serializer, "name", p.name);
				}
				tagValue(serializer, "wikivoyage_category", cat);
				serializer.endTag(null, "node");
				cats.add(cat);
			}
			i++;
		}
		article.clear();
	}
	
	
	
	public static String simplifyWptCategory(String category, String defaultCat) {
		if (category == null) {
			category = "";
		}
		category = category.toLowerCase();
		switch (category) {
		case "מוקדי":
		case "tourist information":
			category = CAT_OTHER;
			break;
		case "aller":
		case "go":
		case "around": // ?
			category = CAT_DO;
			break;
		case "university":
		case "port":
		case "post":
		case "pharmacy":
		case "hospital":
		case "train":
		case "bus":
		case "police":
		case "embassy":
		case "bank":
		case "library":
		case "office":
		case "school":
		case "town hall":
		case "airport":
		case "surgery":
		case "clinic":
		case "other":
		case "municipality":
			category = CAT_OTHER;
			break;
		case "cinema":
		case "aquarium":
		case "theater":
		case "swimming pool":
		case "swimming":
		case "beach":
		case "amusement park":
		case "golf":
		case "club":
		case "sports":
		case "music":
		case "spa":
		case "انجام‌دادن":
			category = CAT_DO;
			break;
		case "mall":
		case "market":
		case "shop":
		case "supermarket":
			category = CAT_BUY;
			break;
		case "beer garden":
		case "bar":
		case "brewery":
		case "שתייה":
		case "discotheque":
		case "نوشیدن":
		case "pub":
			category = CAT_DRINK;
			break;
		case "veja":
		case "voir":
		case "zoo":
		case "temple":
		case "mosque":
		case "synagogue":
		case "monastery":
		case "church":
		case "palace":
		case "building":
		case "château":
		case "museum":
		case "attraction":
		case "gallery":
		case "memorial":
		case "archaeological site":
		case "fort":
		case "monument":
		case "castle":
		case "park":
		case "دیدن":
		case "tower":
		case "cave":
		case "botanical garden":
		case "square":
		case "cathedral":
		case "lake":
		case "landmark":
		case "cemetery":
		case "garden":
		case "arts centre":
		case "national park":
		case "waterfall":
		case "viewpoint":
		case "mountain":
		case "mill":
		case "house":
		case "ruins":
			category = CAT_SEE;
			break;
		case "restaurant":
		case "restaurant and_bar":
		case "restaurant and bar":
		case "cafe":
		case "bakery":
		case "manger":
		case "coma":
		case "אוכל":
		case "bistro":
		case "snack bar":
			category = CAT_EAT;
			break;
		case "destination":
		case "listing":
		case "destinationlist":
		case "רשימה": // listing - example airport
			category = CAT_MARKER;
			break;
		case "hotel":
		case "motel":
		case "לינה":
		case "hostel":
		case "se loger":
		case "guest house":
		case "campsite":
		case "holiday flat":
		case "alpine hut":
		case "youth hostel":
		case "caravan site":
		case "appartment":
		case "خوابیدن":
		case "boarding house":
		case "hotel garni":
		case "durma":
			category = CAT_SLEEP;
			break;
		case "city":
		case "vicinity":
		case "village":
		case "town":
			category = CAT_MARKER;
			break;
		case "":
			category = CAT_OTHER;
			break;
		default:
			if(defaultCat != null) {
				category = defaultCat;
				break;
			}
		}
		return category;
	}
}
