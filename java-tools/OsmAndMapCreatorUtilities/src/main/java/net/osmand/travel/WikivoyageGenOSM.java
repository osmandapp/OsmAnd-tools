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
import java.util.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.logging.Log;
import org.xmlpull.v1.XmlSerializer;

import gnu.trove.list.array.TIntArrayList;
import net.osmand.PlatformUtil;
import net.osmand.data.LatLon;
import net.osmand.gpx.GPXFile;
import net.osmand.gpx.GPXUtilities;
import net.osmand.gpx.GPXUtilities.WptPt;
import net.osmand.obf.preparation.DBDialect;
import net.osmand.osm.MapRenderingTypesEncoder;
import net.osmand.wiki.WikivoyageOSMTags;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;

import static net.osmand.travel.WikivoyageLangPreparation.WID_DESTINATIONS;

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

	private static final double ARTICLE_NO_COORDS_LAT = 0;
	private static final double ARTICLE_NO_COORDS_LON = 0;
	private static final double ARTICLE_NO_COORDS_DELTA = 1;

	private static final Log log = PlatformUtil.getLog(WikivoyageGenOSM.class);
	private final static NumberFormat latLonFormat = new DecimalFormat("0.00#####", new DecimalFormatSymbols(Locale.US));
	private static final String LANG = "LANG";
	private static final String TITLE = "TITLE";
	private static final boolean WRITE_POINTS_COLLECTION = false;


	static long NODE_ID = -1000;
	static Map<String, Integer> categories = new HashMap<>();
	static Map<String, String> categoriesExample = new HashMap<>();
	private static Map<String, String> wptCategories;


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
		List<String> aggrPartsOfWid = new ArrayList<>();
		List<String> jsonContents = new ArrayList<>();

		public void addArticle(String lang, String title, GPXFile gpxFile, double lat, double lon, String content,
							   String imageTitle, String bannerTitle, String partOf, String isParentOf,
							   String aggrPartOf, String aggrPartOfWid, String contentJson) {
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
			aggrPartsOfWid.add(ind, aggrPartOfWid);
			jsonContents.add(ind, contentJson);
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
			aggrPartsOfWid.clear();
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

	private static class WikivoyageOutputFile {
		File outputFile;
		boolean filter = false;
		List<String> qids = new ArrayList<String>();
		TIntArrayList qidsType = new TIntArrayList();
		XmlSerializer serializer;
		OutputStream outputStream = null;

		public WikivoyageOutputFile(File outFile) throws IllegalArgumentException, IllegalStateException, IOException {
			this.outputFile = outFile;
			outputStream = new FileOutputStream(outputFile);
			if (outputFile.getName().endsWith(".gz")) {
				outputStream = new GZIPOutputStream(outputStream);
			}
			serializer = PlatformUtil.newSerializer();
			serializer.setOutput(new OutputStreamWriter(outputStream));
			serializer.setFeature("http://xmlpull.org/v1/doc/features.html#indent-output", true); //$NON-NLS-1$
			serializer.startDocument("UTF-8", true); //$NON-NLS-1$
			serializer.startTag(null, "osm"); //$NON-NLS-1$
			serializer.attribute(null, "version", "0.6"); //$NON-NLS-1$ //$NON-NLS-2$
		}

		public boolean accept(CombinedWikivoyageArticle combinedArticle) {
			if (!filter) {
				return true;
			}
			// exact match
			if (combinedArticle.tripId > 0) {
				if ((WID_DESTINATIONS + "").equals(combinedArticle.tripId + "")){
					return true;
				}
				for (int tp = 0; tp < qids.size(); tp++) {
					if (qids.get(tp).equals(combinedArticle.tripId + "")) {
						return true;
					}
				}
			}
			for (String aggWid : combinedArticle.aggrPartsOfWid) {
				String[] wids = aggWid.split(",");
				boolean match = false;
				// compare all parent wikidata ids with
				loop: for (int tp = 0; tp < qids.size() && !match; tp++) {
					for (int i = 0; i < wids.length && !match; i++) {
						if (qids.get(tp).equals(wids[i])) {
							int tpMatch = qidsType.get(tp);
							if (tpMatch < 0) {
								match = true; // Q1111%
							} else if (tpMatch > 0 && i == 0) {
								match = true; // Q1111.
							}
							break loop;
						}
					}
				}
				if (match) {
					return true;
				}
			}
			return false;
		}

		public void close() throws IOException {
			serializer.endTag(null, "osm");
			serializer.flush();
			outputStream.close();
		}

	}

	public static void genWikivoyageOsm(File wikivoyageFile, List<String> genOsm) throws SQLException, IOException {
		List<WikivoyageOutputFile> outs = new ArrayList<>();
		for(String s : genOsm) {
			int t = s.indexOf(':');
			WikivoyageOutputFile out = new WikivoyageOutputFile(new File(s.substring(t + 1)));
			if (t > 0) {
				String[] keys = s.substring(0, t).split(",");
				out.filter = true;
				for (String k : keys) {
					if (k.startsWith("Q")) {
						int type = 0;
						String id = k.substring(1);
						if (id.endsWith("%")) {
							id = id.substring(0, id.length() - 1);
							type = -1;
						} else if (id.endsWith(".")) {
							id = id.substring(0, id.length() - 1);
							type = 1;
						}
						out.qids.add(id);
						out.qidsType.add(type);
					}
				}
				System.out.printf("Write to %s : %s - %s\n", out.outputFile.getName(), out.qids, out.qidsType);
			}
			outs.add(out);
		}
		genWikivoyageOsm(wikivoyageFile, outs, -1);
	}
	public static void genWikivoyageOsm(File wikivoyageFile, File outFile, int LIMIT) throws SQLException, IOException {
		List<WikivoyageOutputFile> outs = new ArrayList<>();
		if (outFile != null) {
			outs.add(new WikivoyageOutputFile(outFile));
		}
		genWikivoyageOsm(wikivoyageFile, outs, LIMIT);
	}

	public static void genWikivoyageOsm(File wikivoyageFile, List<WikivoyageOutputFile> outs, int LIMIT) throws SQLException, IOException {
		DBDialect dialect = DBDialect.SQLITE;
		Connection connection = (Connection) dialect.getDatabaseConnection(wikivoyageFile.getCanonicalPath(), log );
		Statement statement = connection.createStatement();
		// travel_articles:
		// 						population, country, region, city_type, osm_i,
		ResultSet rs = statement.executeQuery("select trip_id, title, lang, lat, lon, content_gz, "
				+ "gpx_gz, image_title, banner_title, src_banner_title, is_part_of, is_parent_of, "
				+ "aggregated_part_of, agg_part_of_wid, contents_json "
				+ "from travel_articles order by trip_id asc");
		int count = 0, totalArticles = 0, emptyLocation = 0, emptyContent = 0;
		CombinedWikivoyageArticle combinedArticle = new CombinedWikivoyageArticle();

		while (rs.next()) {
			int rind = 1;
			long tripId = rs.getLong(rind++);
			if ((tripId != combinedArticle.tripId || combinedArticle.tripId == 0) &&
					combinedArticle.tripId != -1) {
				boolean res = false;
				for (WikivoyageOutputFile out : outs) {
					if (out.accept(combinedArticle)) {
						res |= combineAndSave(combinedArticle, out.serializer);
					}
				}
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
			String isAggrPartOfWid = rs.getString(rind++);
			String contentJson = rs.getString(rind);
			combinedArticle.addArticle(lang, title, gpxFile, lat, lon, content, imageTitle,
					Algorithms.isEmpty(srcBannerTitle) ? bannerTitle : srcBannerTitle, isPartOf,
					isParentOf, isAggrPartOf, isAggrPartOfWid, contentJson);
			if (gpxFile.error != null || gpxFile.isPointsEmpty()) {
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
		for (WikivoyageOutputFile out : outs) {
			if (out.accept(combinedArticle)) {
				combineAndSave(combinedArticle, out.serializer);
			}
			out.close();
		}
		List<String> l = new ArrayList<String>(categories.keySet());
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
		if (mainArticlePoint == null) {
			Random rnd = new Random();
			double lat = ARTICLE_NO_COORDS_LAT + (rnd.nextDouble() - 0.5) * ARTICLE_NO_COORDS_DELTA;
			double lon = ARTICLE_NO_COORDS_LON + (rnd.nextDouble() - 0.5) * ARTICLE_NO_COORDS_DELTA;
			mainArticlePoint = new LatLon(lat, lon);
//			 System.out.println(String.format("Skip article as it has no points: %s", article.titles));
		}

		points = sortPoints(mainArticlePoint, points);
		serializer.startTag(null, "node");
		long mainArticleid = NODE_ID--;
		serializer.attribute(null, "id", mainArticleid + "");
		serializer.attribute(null, "action", "modify");
		serializer.attribute(null, "version", "1");
		serializer.attribute(null, "lat", formatLat(mainArticlePoint.getLatitude()));
		serializer.attribute(null, "lon", formatLon(mainArticlePoint.getLongitude()));
		tagValue(serializer, "route", "point");
		tagValue(serializer, "route_type", "article");
		addArticleTags(article, serializer, true);
		serializer.endTag(null, "node");

		// points could combined across TAG_WIKIDATA on UI
		// so better to duplicate points with unique route_id (instead of array)
		for (WptPt p : points) {
			String category = simplifyWptCategory(p.category, CAT_OTHER);
			serializer.startTag(null, "node");
			long id = NODE_ID--;
			serializer.attribute(null, "id", id + "");
			serializer.attribute(null, "action", "modify");
			serializer.attribute(null, "version", "1");
			serializer.attribute(null, "lat", formatLat(p.lat));
			serializer.attribute(null, "lon", formatLon(p.lon));

			tagValue(serializer, "route", "point");
			tagValue(serializer, "route_type", "article_point");
			tagValue(serializer, "category", category);
			String lng = p.getExtensionsToRead().get(LANG);
			tagValue(serializer, "lang:" + lng, "yes");
//			addPointTags(article, serializer, p, ":" + lng);
			addPointTags(article, serializer, p, "");

			tagValue(serializer, "route_source", "wikivoyage");
			tagValue(serializer, "route_id", "Q" + article.tripId);
			for (int i = 0; i < article.size(); i++) {
				String lnga = article.langs.get(i);
				String titleId = article.titles.get(i);
				tagValue(serializer, "route_name:" + lnga, titleId);
			}
			serializer.endTag(null, "node");
		}


		if (WRITE_POINTS_COLLECTION) {
			long idEnd = NODE_ID;
			serializer.startTag(null, "way");
			long wayId = NODE_ID--;
			serializer.attribute(null, "id", wayId + "");
			serializer.attribute(null, "action", "modify");
			serializer.attribute(null, "version", "1");

			tagValue(serializer, "route", "points_collection");
			tagValue(serializer, "route_type", "article_points");
			addArticleTags(article, serializer, false);

			for (long nid = idStart; nid > idEnd; nid--) {
				serializer.startTag(null, "nd");
				serializer.attribute(null, "ref", nid + "");
				serializer.endTag(null, "nd");
			}
			serializer.endTag(null, "way");
		}
		return true;
	}

	private static String formatLon(double lon) {
		return latLonFormat.format(lon);
	}

	private static String formatLat(double lat) {
		if (lat < MapUtils.MIN_LATITUDE) {
			lat = MapUtils.MIN_LATITUDE + 0.5;
		}
		if (lat > MapUtils.MAX_LATITUDE) {
			lat = MapUtils.MAX_LATITUDE - 0.5;
		}
		return latLonFormat.format(lat);
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
		tagValue(serializer, "description" + lngSuffix, p.desc);
		tagValue(serializer, "name" + lngSuffix, p.name);
		String title;
		for (int it = 0; it < article.langs.size(); it++) {
			String lng = article.langs.get(it);
			title = article.titles.get(it);
			tagValue(serializer, "route_name:" + lng, title);
		}
		title = p.getExtensionsToRead().get(TITLE);
		tagValue(serializer, "route_name", title);
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
				// tagValue(serializer, "colour_int", Algorithms.colorToString(color)); // unsupported
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
