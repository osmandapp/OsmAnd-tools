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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.logging.Log;
import org.xmlpull.v1.XmlSerializer;

import net.osmand.PlatformUtil;
import net.osmand.data.LatLon;
import net.osmand.gpx.GPXFile;
import net.osmand.gpx.GPXUtilities;
import net.osmand.gpx.GPXUtilities.WptPt;
import net.osmand.obf.preparation.DBDialect;
import net.osmand.osm.MapRenderingTypesEncoder;
import net.osmand.travel.WikivoyageLangPreparation.WikivoyageOSMTags;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;

public class WikivoyageGenOSM {
	public static final String CAT_SEE = "see"; // 27%
	public static final String CAT_NATURAL = "natural"; // new
	public static final String CAT_MARKER = "marker"; // 20%
	public static final String CAT_SLEEP = "sleep"; // 15%
	public static final String CAT_EAT = "eat"; // 12%
	public static final String CAT_DO = "do"; // 10%
	public static final String CAT_GO = "go";
	public static final String CAT_DRINK = "drink"; // 5%
	public static final String CAT_BUY = "buy"; // 4%
	public static final String CAT_OTHER = "other"; // 10%

	private static final Log log = PlatformUtil.getLog(WikivoyageGenOSM.class);
	private final static NumberFormat latLonFormat = new DecimalFormat("0.00#####", new DecimalFormatSymbols());
	private static final String LANG = "LANG";
	private static final String TITLE = "TITLE";
	
	
	static long NODE_ID = -1000;
	static Map<String, Integer> categories = new HashMap<>();
	static Map<String, String> categoriesExample = new HashMap<>();
	private static Map<String, String> wptCategories;
	

	// FUTURE:
	// - Combine point languages and extra tags (merge points possibly by wikidata id) 
	// 	 NOTE: do not duplicate description:* (they are all  visible in context menu)
	public static void main(String[] args) throws SQLException, IOException {
		File f = new File("/Users/victorshcherb/osmand/maps/wikivoyage/wikivoyage.sqlite");
		// TOTAL 100 000
		genWikivoyageOsm(f, new File(f.getParentFile(), "wikivoyage.osm.gz"), 10000);
	}

	
	private static class CombinedWikivoyageArticle {
		long tripId = -1;
		List<String> titles = new ArrayList<>();
		List<LatLon> latlons = new ArrayList<>();
		List<String> langs = new ArrayList<>();
		List<String> contents = new ArrayList<>();
		List<GPXFile> points = new ArrayList<>();
		List<String> imageTitles = new ArrayList<>();
		List<String> partsOf = new ArrayList<>();
		List<String> parentOf = new ArrayList<>();
		List<String> aggrPartsOf = new ArrayList<>();
		List<String> jsonContents = new ArrayList<>();

		public void addArticle(String lang, String title, GPXFile gpxFile, double lat, double lon, String content,
							   String imageTitle, String bannerTitle, String partOf, String isParentOf,
							   String aggrPartOf, String jsonContent) {
			int ind = size();
			if (lang.equals("en")) {
				ind = 0;
			}
			points.add(ind, gpxFile);
			titles.add(ind, title);
			langs.add(ind, lang);
			LatLon ll = null;
			if (lat != 0 && lon != 0) {
				ll = new LatLon(lat, lon);
			}
			latlons.add(ind, ll);
			contents.add(ind, content);
			imageTitles.add(ind, bannerTitle);
			partsOf.add(ind, partOf);
			parentOf.add(ind, isParentOf);
			aggrPartsOf.add(ind, aggrPartOf);
			jsonContents.add(jsonContent);
		}
		
		public void clear() {
			titles.clear();
			langs.clear();
			points.clear();
			contents.clear();
			latlons.clear();
			imageTitles.clear();
			partsOf.clear();
			aggrPartsOf.clear();
			parentOf.clear();
			jsonContents.clear();
		}

		public int size() {
			return langs.size();
		}

		public void updateCategoryCounts() {
			List<String> cats = new ArrayList<>();
			List<String> names = new ArrayList<>();
			int i = 0;
			for (GPXFile f : points) {
				for (WptPt p : f.getPoints()) {
					String cat = simplifyWptCategory(p.category, null);
					cats.add(cat);
					names.add(langs.get(i) + " " + titles.get(i) + " " + p.name);
				}
				i++;
			}
			for (i = 0; i < cats.size(); i++) {
				String cat = cats.get(i);
				Integer nt = categories.get(cat);
				if (nt == null) {
					categoriesExample.put(cat, names.get(i));
				}
				categories.put(cat, nt == null ? 1 : (nt.intValue() + 1));
			}
		}
	}

	public static void genWikivoyageOsm(File wikivoyageFile, File outputFile, int LIMIT) throws SQLException, IOException {
		DBDialect dialect = DBDialect.SQLITE;
		Connection connection = (Connection) dialect.getDatabaseConnection(wikivoyageFile.getCanonicalPath(), log );
		Statement statement = connection.createStatement();
		// travel_articles: 	
		// 						population, country, region, city_type, osm_i,
		ResultSet rs = statement.executeQuery("select trip_id, title, lang, lat, lon, content_gz, "
				+ "gpx_gz, image_title, banner_title, src_banner_title, is_part_of, is_parent_of, aggregated_part_of, contents_json from travel_articles order by trip_id asc");
		int count = 0, totalArticles = 0, emptyLocation = 0, emptyContent = 0;
		CombinedWikivoyageArticle combinedArticle = new CombinedWikivoyageArticle();
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
			if (tripId != combinedArticle.tripId && combinedArticle.tripId != -1) {
				boolean res = combineAndSave(combinedArticle, serializer);
				if (res) {
					totalArticles++;
				}
				combinedArticle.clear();
			}
			combinedArticle.tripId = tripId;
			String title = rs.getString(rind++);
			String lang = rs.getString(rind++);
			double lat = rs.getDouble(rind++);
			double lon = rs.getDouble(rind++);
//			rind++;
			String content = Algorithms.gzipToString(rs.getBytes(rind++));
			GZIPInputStream bytesStream = new GZIPInputStream(new ByteArrayInputStream(rs.getBytes(rind++)));
			GPXFile gpxFile = GPXUtilities.loadGPXFile(bytesStream);
			String imageTitle = rs.getString(rind++);
			String bannerTitle = rs.getString(rind++);
			String srcBannerTitle = rs.getString(rind++);
			String isPartOf = rs.getString(rind++);
			String isParentOf = rs.getString(rind++);
			String isAggrPartOf = rs.getString(rind++);
			String contentJson = rs.getString(rind);
			combinedArticle.addArticle(lang, title, gpxFile, lat, lon, content, imageTitle,
					Algorithms.isEmpty(srcBannerTitle) ? bannerTitle : srcBannerTitle, isPartOf,
					isParentOf, isAggrPartOf, contentJson);
			if (gpxFile == null || gpxFile.isPointsEmpty()) {
				if (lat == 0 && lon == 0) {
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
		combineAndSave(combinedArticle, serializer);
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
		for (String s : l) {
			total += categories.get(s);
		}
		for (String s : l) {
			int cnt = categories.get(s);
			System.out.println(String.format("%#.2f%% %s  %d %s", cnt * 100.0 / total, s, cnt, categoriesExample.get(s)));
		}
		System.out.println(String.format("Total saved articles: %d", totalArticles));
		System.out.println(String.format("Empty article (no gpx): %d no points in article + %d no location page articles (total points %d) ",
				emptyContent, emptyLocation, total));
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

	private static boolean combineAndSave(CombinedWikivoyageArticle article, XmlSerializer serializer) throws IOException {
		article.updateCategoryCounts();		
		long idStart = NODE_ID ;
		LatLon mainArticlePoint = article.latlons.get(0);
		List<WptPt> points = new ArrayList<GPXUtilities.WptPt>();
		for(int i = 0; i < article.size(); i++) {
			String lng = article.langs.get(i);
			GPXFile file = article.points.get(i);
			String titleId = article.titles.get(i);

			if (mainArticlePoint == null) {
				mainArticlePoint = article.latlons.get(i);
			}
			for (WptPt p : file.getPoints()) {
				if (p.lat >= 90 || p.lat <= -90 || p.lon >= 180 || p.lon <= -180) {
					continue;
				}
				p.getExtensionsToWrite().put(LANG, lng);
				p.getExtensionsToWrite().put(TITLE, titleId);
				points.add(p);
				if (mainArticlePoint == null) {
					mainArticlePoint = new LatLon(p.getLatitude(), p.getLongitude());
				}
			}
		}
		if (mainArticlePoint == null && Algorithms.isEmpty(article.partsOf)) {
			// System.out.println(String.format("Skip article as it has no points: %s", article.titles));
			return false;
		}

		if (mainArticlePoint != null) {
			points = sortPoints(mainArticlePoint, points);
			serializer.startTag(null, "node");
			long mainArticleid = NODE_ID--;
			serializer.attribute(null, "id", mainArticleid + "");
			serializer.attribute(null, "action", "modify");
			serializer.attribute(null, "version", "1");
			serializer.attribute(null, "lat", latLonFormat.format(mainArticlePoint.getLatitude()));
			serializer.attribute(null, "lon", latLonFormat.format(mainArticlePoint.getLongitude()));
			tagValue(serializer, "route", "point");
			tagValue(serializer, "route_type", "article");
			addArticleTags(article, serializer, true);
			serializer.endTag(null, "node");
		}
		
		for (WptPt p : points) {
			String category = simplifyWptCategory(p.category, CAT_OTHER);
			serializer.startTag(null, "node");
			long id = NODE_ID--;
			serializer.attribute(null, "id", id + "");
			serializer.attribute(null, "action", "modify");
			serializer.attribute(null, "version", "1");
			serializer.attribute(null, "lat", latLonFormat.format(p.lat));
			serializer.attribute(null, "lon", latLonFormat.format(p.lon));
			
			tagValue(serializer, "route", "point");
			tagValue(serializer, "route_type", "article_point");
			tagValue(serializer, "category", category);
			String lng = p.getExtensionsToRead().get(LANG);
			tagValue(serializer, "lang:" + lng, "yes");
//			addPointTags(article, serializer, p, ":" + lng);
			addPointTags(article, serializer, p, "");
			
			tagValue(serializer, "route_source", "wikivoyage");
			tagValue(serializer, "route_id", "Q"+article.tripId);
			serializer.endTag(null, "node");
		}
		
		
		long idEnd = NODE_ID;
		serializer.startTag(null, "way");
		long wayId = NODE_ID--;
		serializer.attribute(null, "id", wayId + "");
		serializer.attribute(null, "action", "modify");
		serializer.attribute(null, "version", "1");
		
		tagValue(serializer, "route", "points_collection");
		tagValue(serializer, "route_type", "article_points");
		addArticleTags(article, serializer, false);
		
		for(long nid  = idStart ; nid > idEnd; nid--  ) {
			serializer.startTag(null, "nd");
			serializer.attribute(null, "ref", nid +"");
			serializer.endTag(null, "nd");
		}
		serializer.endTag(null, "way");
		return true;	
	}
	
	private static void addArticleTags(CombinedWikivoyageArticle article, XmlSerializer serializer, boolean addDescription) throws IOException {
		tagValue(serializer, "route_id", "Q"+article.tripId);
		tagValue(serializer, "route_source", "wikivoyage");
		for (int it = 0; it < article.langs.size(); it++) {
			String lng = article.langs.get(it);
			String title = article.titles.get(it);
			tagValue(serializer, "route_name:" + lng, title);
			tagValue(serializer, "name:" + lng, title);
			tagValue(serializer, "image_title:" + lng, article.imageTitles.get(it));
			tagValue(serializer, "is_part:" + lng, article.partsOf.get(it));
			tagValue(serializer, "is_aggr_part:" + lng, article.aggrPartsOf.get(it));
			tagValue(serializer, "is_parent_of:" + lng, article.parentOf.get(it));
			tagValue(serializer, "content_json:" + lng, article.jsonContents.get(it));
			if (addDescription) {
				tagValue(serializer, "description:" + lng, article.contents.get(it));
			}
		}
	}

	private static void addPointTags(CombinedWikivoyageArticle article, XmlSerializer serializer, WptPt p, String lngSuffix)
			throws IOException {
		String title = p.getExtensionsToRead().get(TITLE);
		tagValue(serializer, "description" + lngSuffix, p.desc);
		tagValue(serializer, "name" + lngSuffix, p.name);
		tagValue(serializer, "route_name" + lngSuffix, title);
		for (WikivoyageOSMTags tg : WikivoyageOSMTags.values()) {
			String v = p.getExtensionsToRead().get(tg.tag());
			if (!Algorithms.isEmpty(v)) {
				tagValue(serializer, tg.tag() + lngSuffix, v);
			}
		}
		if (!Algorithms.isEmpty(p.link)) {
			tagValue(serializer, "url" + lngSuffix, p.link);
		}
		if (!Algorithms.isEmpty(p.comment)) {
			tagValue(serializer, "note" + lngSuffix, p.comment);
		}

		if (lngSuffix.isEmpty()) {
			if (!Algorithms.isEmpty(p.getIconName())) {
				tagValue(serializer, "gpx_icon", p.getIconName());
			}
			if (!Algorithms.isEmpty(p.getBackgroundType())) {
				tagValue(serializer, "gpx_bg", p.getBackgroundType());
			}
			int color = p.getColor(0);
			if (color != 0) {
				tagValue(serializer, "colour",
						MapRenderingTypesEncoder.formatColorToPalette(Algorithms.colorToString(color), false));
				tagValue(serializer, "colour_int", Algorithms.colorToString(color));
			}
		}

	}
	
	
	private static List<WptPt> sortPoints(LatLon pnt, List<WptPt> points) {
		List<WptPt> res = new ArrayList<WptPt>();
//		res.add(pn)
		WptPt p = null;
		while ((p = peakClosest(pnt, points)) != null) {
			res.add(p);
			pnt = new LatLon(p.getLatitude(), p.getLongitude());
		}
		return res;
	}
	
	private static WptPt peakClosest(LatLon pnt, List<WptPt> points) {
		WptPt r = null;
		double d = Double.POSITIVE_INFINITY;
		for (int i = 0; i < points.size(); i++) {
			double ds = MapUtils.getDistance(pnt, points.get(i).getLatitude(), points.get(i).getLongitude());
			if (ds < d) {
				d = ds;
				r = points.get(i);
			}
		}
		points.remove(r);
		return r;
	}

	public static String simplifyWptCategory(String category, String defaultCat) {
		if (category == null) {
			category = "";
		}
		category = category.toLowerCase();
		if (wptCategories == null) {
			wptCategories = buildCategories();
		}
		String cat = wptCategories.get(category);
		if (cat != null) {
			return cat;
		}
		for (String key : wptCategories.keySet()) {
			// check if it is present as a first tag
			if (category.endsWith(" " + key) || category.startsWith(key + ",") || category.contains(" " + key + ",")) {
				return wptCategories.get(key);
			}
		}
		if (category.isEmpty()) {
			return CAT_OTHER;
		}
		if (defaultCat != null) {
			return defaultCat;
		}
		return category;
	}
	
	private static Map<String, String> buildCategories() {
		Map<String, String> categories = new LinkedHashMap<String, String>();
        categories.put("מוקדי", CAT_OTHER);
		categories.put("tourist information", CAT_OTHER);
		categories.put(CAT_OTHER, CAT_OTHER);

		categories.put(CAT_GO, CAT_GO);

		categories.put("aller", CAT_DO);
		categories.put("around", CAT_DO); // ?
		categories.put(CAT_DO, CAT_DO);
			
		categories.put("university", CAT_OTHER);
		categories.put("port", CAT_OTHER);
		categories.put("post", CAT_OTHER);
		categories.put("pharmacy", CAT_OTHER);
		categories.put("hospital", CAT_OTHER);
		categories.put("train", CAT_OTHER);
		categories.put("bus", CAT_OTHER);
		categories.put("police", CAT_OTHER);
		categories.put("embassy", CAT_OTHER);
		categories.put("bank", CAT_OTHER);
		categories.put("library", CAT_OTHER);
		categories.put("office", CAT_OTHER);
		categories.put("school", CAT_OTHER);
		categories.put("town hall", CAT_OTHER);
		categories.put("airport", CAT_OTHER);
		categories.put("surgery", CAT_OTHER);
		categories.put("clinic", CAT_OTHER);
		categories.put("municipality", CAT_OTHER);
			
		categories.put("cinema", CAT_DO);
		categories.put("aquarium", CAT_DO);
		categories.put("theater", CAT_DO);
		categories.put("swimming pool", CAT_DO);
		categories.put("swimming", CAT_DO);
		categories.put("beach", CAT_DO);
		categories.put("amusement park", CAT_DO);
		categories.put("golf", CAT_DO);
		categories.put("club", CAT_DO);
		categories.put("sports", CAT_DO);
		categories.put("music", CAT_DO);
		categories.put("spa", CAT_DO);
		categories.put("انجام‌دادن", CAT_DO);
			
		categories.put("mall", CAT_BUY);
		categories.put("market", CAT_BUY);
		categories.put("shop", CAT_BUY);
		categories.put("supermarket", CAT_BUY);
		categories.put(CAT_BUY, CAT_BUY);

		categories.put(CAT_NATURAL, CAT_NATURAL);
		
		categories.put("veja", CAT_SEE);
		categories.put("voir", CAT_SEE);
		categories.put("zoo", CAT_SEE);
		categories.put("temple", CAT_SEE);
		categories.put("mosque", CAT_SEE);
		categories.put("synagogue", CAT_SEE);
		categories.put("monastery", CAT_SEE);
		categories.put("church", CAT_SEE);
		categories.put("palace", CAT_SEE);
		categories.put("building", CAT_SEE);
		categories.put("château", CAT_SEE);
		categories.put("museum", CAT_SEE);
		categories.put("attraction", CAT_SEE);
		categories.put("gallery", CAT_SEE);
		categories.put("memorial", CAT_SEE);
		categories.put("archaeological site", CAT_SEE);
		categories.put("fort", CAT_SEE);
		categories.put("monument", CAT_SEE);
		categories.put("castle", CAT_SEE);
		categories.put("park", CAT_SEE);
		categories.put("دیدن", CAT_SEE);
		categories.put("tower", CAT_SEE);
		categories.put("cave", CAT_SEE);
		categories.put("botanical garden", CAT_SEE);
		categories.put("square", CAT_SEE);
		categories.put("cathedral", CAT_SEE);
		categories.put("lake", CAT_SEE);
		categories.put("landmark", CAT_SEE);
		categories.put("cemetery", CAT_SEE);
		categories.put("garden", CAT_SEE);
		categories.put("arts centre", CAT_SEE);
		categories.put("national park", CAT_SEE);
		categories.put("waterfall", CAT_SEE);
		categories.put("viewpoint", CAT_SEE);
		categories.put("mountain", CAT_SEE);
		categories.put("mill", CAT_SEE);
		categories.put("house", CAT_SEE);
		categories.put("ruins", CAT_SEE);
		categories.put(CAT_SEE, CAT_SEE);

			
		categories.put("restaurant", CAT_EAT);
		categories.put("cafe", CAT_EAT);
		categories.put("bakery", CAT_EAT);
		categories.put("manger", CAT_EAT);
		categories.put("coma", CAT_EAT);
		categories.put("אוכל", CAT_EAT);
		categories.put("bistro", CAT_EAT);
		categories.put("snack bar", CAT_EAT);
		categories.put(CAT_EAT, CAT_EAT);


		categories.put("beer garden", CAT_DRINK);
		categories.put("bar", CAT_DRINK);
		categories.put("brewery", CAT_DRINK);
		categories.put("שתייה", CAT_DRINK);
		categories.put("discotheque", CAT_DRINK);
		categories.put("نوشیدن", CAT_DRINK);
		categories.put("pub", CAT_DRINK);
		categories.put(CAT_DRINK, CAT_DRINK);

			
		categories.put("destination", CAT_MARKER);
		categories.put("listing", CAT_MARKER);
		categories.put("destinationlist", CAT_MARKER);
		categories.put("רשימה", CAT_MARKER); // listing - example airport
		categories.put(CAT_MARKER, CAT_MARKER);

			
		categories.put("hotel", CAT_SLEEP);
		categories.put("motel", CAT_SLEEP);
		categories.put("לינה", CAT_SLEEP);
		categories.put("hostel", CAT_SLEEP);
		categories.put("se loger", CAT_SLEEP);
		categories.put("guest house", CAT_SLEEP);
		categories.put("campsite", CAT_SLEEP);
		categories.put("holiday flat", CAT_SLEEP);
		categories.put("alpine hut", CAT_SLEEP);
		categories.put("youth hostel", CAT_SLEEP);
		categories.put("caravan site", CAT_SLEEP);
		categories.put("appartment", CAT_SLEEP);
		categories.put("خوابیدن", CAT_SLEEP);
		categories.put("boarding house", CAT_SLEEP);
		categories.put("hotel garni", CAT_SLEEP);
		categories.put("durma", CAT_SLEEP);
		categories.put(CAT_SLEEP, CAT_SLEEP);

			
		categories.put("city", CAT_MARKER);
		categories.put("vicinity", CAT_MARKER);
		categories.put("village", CAT_MARKER);
		categories.put("town", CAT_MARKER);
		return categories;
	}
}
