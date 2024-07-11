package net.osmand.data.changeset;

import java.io.IOException;
import java.io.StringReader;
import java.net.URL;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.TreeSet;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.X509TrustManager;

import net.osmand.PlatformUtil;
import net.osmand.binary.BinaryMapDataObject;
import net.osmand.map.OsmandRegions;
import net.osmand.map.WorldRegion;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;

import org.apache.commons.logging.Log;
import org.kxml2.io.KXmlParser;
import org.xmlpull.v1.XmlPullParser;

public class CalculateOsmChangesets {

	private static final int BATCH_SIZE = 1000;
	private static final int FETCH_LIMIT = 1000000;
	private static final int MAX_COUNTRY_SIZE = 7;
	private static final long INITIAL_CHANGESET = 60714185l;
	private static final Log LOG = PlatformUtil.getLog(CalculateOsmChangesets.class);
	private static final int MAX_QUERIES = 50;
	private static final int MAX_QUERY_1 = 49;

	static SimpleDateFormat FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
	static SimpleDateFormat FORMAT2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z");
	static {
		FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
		FORMAT2.setTimeZone(TimeZone.getTimeZone("UTC"));
	}

	public static void main(String[] args) throws Exception {
		downloadChangesets();
	}

	public static void downloadChangesets() throws Exception {
		// configure the SSLContext with a TrustManager
		SSLContext ctx = SSLContext.getInstance("TLS");
		ctx.init(new KeyManager[0], new X509TrustManager[] { new X509TrustManager() {
			@Override
			public X509Certificate[] getAcceptedIssuers() {
				return null;
			}

			@Override
			public void checkServerTrusted(X509Certificate[] arg0, String arg1) throws CertificateException {
			}

			@Override
			public void checkClientTrusted(X509Certificate[] arg0, String arg1) throws CertificateException {
			}
		} }, new SecureRandom());
		SSLContext.setDefault(ctx);

		// jdbc:postgresql://user:secret@localhost
		Connection conn = DriverManager.getConnection(System.getenv("DB_CONN"),
				isEmpty(System.getenv("DB_USER")) ? "test" : System.getenv("DB_USER"),
				isEmpty(System.getenv("DB_PWD")) ? "test" : System.getenv("DB_PWD"));
		List<String> toQuery = new LinkedList<String>();
		long start = INITIAL_CHANGESET;
		PreparedStatement insChangeset = conn
				.prepareStatement("INSERT INTO changesets(id, bot, created_at, closed_at, closed_at_day, "
						+ "minlat, minlon, maxlat, maxlon, username, uid, created_by, changes_count)"
						+ " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
		PreparedStatement selChangeset = conn.prepareStatement("SELECT 1 from changesets where id = ?");
		PreparedStatement delPrepare = conn.prepareStatement("DELETE FROM pending_changesets where id = ?");
		PreparedStatement selPrepare = conn.prepareStatement("SELECT 1 from pending_changesets where id = ?");
		PreparedStatement insPrepare = conn
				.prepareStatement("INSERT INTO pending_changesets(id, created_at) VALUES(?, ?)");
		int insChangesets = 0;
		int pChangesets = 0;
		int insPChangesets = 0;
		int delPChangesets = 0;
		try {
			// read pending changesets
			ResultSet rs = conn.createStatement().executeQuery("SELECT distinct id from pending_changesets");
			while (rs.next()) {
				long pend = rs.getLong(1);

				toQuery.add(pend + "");
				// start = Math.max(pend + 1, start);
			}

			LOG.info("Pending " + toQuery);
			rs.close();
			pChangesets = toQuery.size();
			// get maximum available changeset & read other
			ResultSet max = conn.createStatement().executeQuery("SELECT max(id) from changesets");
			if (max.next()) {
				long maxAvailbleId = max.getLong(1);
				start = Math.max(maxAvailbleId + 1, start);
			}
			max.close();
			while (toQuery.size() < MAX_QUERIES * MAX_QUERY_1) {
				toQuery.add(start + "");
				start++;
			}

			// load changesets
			// maxdate = None
			while (!toQuery.isEmpty()) {
				String url = "https://api.openstreetmap.org//api/0.6/changesets?changesets=";
				for (int i = 0; i < MAX_QUERY_1; i++) {
					if (i > 0) {
						url += ",";
					}
					url += toQuery.remove(0);
				}
				Thread.sleep(500);
				LOG.info("Query " + url);
				HttpsURLConnection con = (HttpsURLConnection) new URL(url).openConnection();
				con.setHostnameVerifier(new HostnameVerifier() {
					@Override
					public boolean verify(String arg0, SSLSession arg1) {
						return true;
					}
				});
				con.setRequestProperty("User-Agent", "OsmAndDownloadChangesets");
				con.setConnectTimeout(90000);//90sec.
				StringBuilder sb = Algorithms.readFromInputStream(con.getInputStream());
				XmlPullParser parser = PlatformUtil.newXMLPullParser();
				parser.setInput(new StringReader(sb.toString()));
				int tok;
				ChangesetProps p = null;
				while ((tok = parser.next()) != XmlPullParser.END_DOCUMENT) {
					if (tok == XmlPullParser.START_TAG) {
						if (parser.getName().equals("changeset")) {
							p = new ChangesetProps();
							p.closedAt = parser.getAttributeValue("", "closed_at");
							p.changesCount = Integer.parseInt(parser.getAttributeValue("", "changes_count"));
							p.minLat = getAttributeDoubleValue(parser, "min_lat");
							p.minLon = getAttributeDoubleValue(parser, "min_lon");
							p.maxLat = getAttributeDoubleValue(parser, "max_lat");
							p.maxLon = getAttributeDoubleValue(parser, "max_lon");
							p.bot = 0;
							p.user = parser.getAttributeValue("", "user");
							p.uid = parser.getAttributeValue("", "uid");
							p.id = Long.parseLong(parser.getAttributeValue("", "id"));
							p.createdAt = parser.getAttributeValue("", "created_at");
						} else if (parser.getName().equals("tag") && p != null) {
							String k = parser.getAttributeValue("", "k");
							String v = parser.getAttributeValue("", "v");
							if (k.equals("created_by")) {
								p.createdBy = v;
							}
							p.tags.put(k, v);
						}
					} else if (tok == XmlPullParser.END_TAG) {
						if (parser.getName().equals("changeset")) {
							if (!isEmpty(p.closedAt)) {
								selChangeset.setLong(1, p.id);
								if (!selChangeset.executeQuery().next()) {
									insChangeset.setLong(1, p.id);
									insChangeset.setInt(2, p.bot);
									insChangeset.setTimestamp(3, parseTimestamp(p.createdAt));
									insChangeset.setTimestamp(4, parseTimestamp(p.closedAt));
									insChangeset.setString(5, p.closedAt.substring(0, 10));
									insChangeset.setDouble(6, Double.parseDouble(p.minLat));
									insChangeset.setDouble(7, Double.parseDouble(p.minLon));
									insChangeset.setDouble(8, Double.parseDouble(p.maxLat));
									insChangeset.setDouble(9, Double.parseDouble(p.maxLon));
									insChangeset.setString(10, p.user);
									insChangeset.setString(11, p.uid);
									insChangeset.setString(12, p.createdBy);
									insChangeset.setInt(13, p.changesCount);
									// p.tags.remove("comment");
									// insChangeset.setObject(13, new JSONObject(p.tags).toString(), Types.OTHER);
									insChangeset.executeUpdate();
									insChangesets++;
								}

								delPrepare.setLong(1, p.id);
								int upd = delPrepare.executeUpdate();
								if (upd > 0) {
									delPChangesets++;
								}
							} else {
								selPrepare.setLong(1, p.id);

								if (!selPrepare.executeQuery().next()) {
									insPrepare.setLong(1, p.id);
									insPrepare.setString(2, p.createdAt.replace('T', ' '));
									insPrepare.execute();
									insPChangesets++;
								}
							}
							p = null;

						}
					}
				}
			}

			System.out.println("Pending changesets before: " + pChangesets + ", processed changesets: "
					+ (MAX_QUERY_1 * MAX_QUERIES - pChangesets));
			if (insChangesets > 0) {
				System.out.println("Successfully inserted changeset: " + insChangesets);
			}
			if (insPChangesets > 0) {
				System.out.println("Successfully inserted pending changesets: " + insPChangesets);
			}
			if (delPChangesets > 0) {
				System.out.println("Successfully deleted pending changesets: " + delPChangesets);
			}
			System.out.println("DONE");
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
			throw new RuntimeException(e);
		} finally {
			conn.close();
		}

	}

	private static Timestamp parseTimestamp(String p) throws ParseException {
		try {
			return new Timestamp(FORMAT2.parse(p).getTime());
		} catch (ParseException e) {
			return new Timestamp(FORMAT.parse(p).getTime());
		}
	}

	private static String getAttributeDoubleValue(XmlPullParser parser, String key) {
		String vl = parser.getAttributeValue("", key);
		if (isEmpty(vl)) {
			return "0";
		}
		return vl;
	}

	private static boolean isEmpty(String vl) {
		return vl == null || vl.equals("");
	}

	public static void calculateCountries() throws Exception {
		// jdbc:postgresql://user:secret@localhost
		Connection conn = DriverManager.getConnection(System.getenv("DB_CONN"), System.getenv("DB_USER"),
				System.getenv("DB_PWD"));
		try {
			Statement stat = conn.createStatement();
			ResultSet rs = stat.executeQuery("SELECT COUNT(*) FROM countries");
			boolean empty = !rs.next() || rs.getInt(1) == 0;
			rs.close();
			Map<WorldRegion, Integer> map = new LinkedHashMap<WorldRegion, Integer>();
			OsmandRegions or = initCountriesTable(conn, empty, map);
			PreparedStatement ps = conn.prepareStatement(
					"INSERT INTO changeset_country(changesetid, countryid, small)" + " VALUES(?, ?, ?)");
			rs = stat.executeQuery("select id, minlat, minlon, maxlat, maxlon from changesets C "
					+ " where (maxlat <> 0 or minlat <> 0 or maxlon <> 0 or minlon <> 0) and "
					+ "not exists (select 1 from changeset_country CC where CC.changesetid=C.id) limit " + FETCH_LIMIT);
			int batch = 0;
			int batchInd = 1;
			while (rs.next()) {
				double minlat = rs.getDouble(2);
				double minlon = rs.getDouble(3);
				double maxlat = rs.getDouble(4);
				double maxlon = rs.getDouble(5);
				long changesetId = rs.getLong(1);
				int lx = MapUtils.get31TileNumberX(minlon);
				int rx = MapUtils.get31TileNumberX(maxlon);
				int ty = MapUtils.get31TileNumberY(maxlat);
				int by = MapUtils.get31TileNumberY(minlat);
				List<BinaryMapDataObject> objs = or.query(lx, rx, ty, by);
				int cid = 0;
				for (BinaryMapDataObject o : objs) {
					if (!or.intersect(o, lx, ty, rx, by)) {
						continue;
					}
					String full = or.getFullName(o);
					WorldRegion reg = or.getRegionData(full);
					if (reg.isRegionMapDownload() && !full.toLowerCase().startsWith("world_")) {
						cid++;
						if (cid > MAX_COUNTRY_SIZE) {
							continue;
						}
						// System.out.println(changesetId + " " + full + " " + reg.getLocaleName() + " " +
						// map.get(reg));
						if (map.get(reg) == null) {
							throw new UnsupportedOperationException("Not found " + changesetId + " " + full);
						}
						boolean small = true;
						List<WorldRegion> subs = reg.getSubregions();
						if (subs != null) {
							for (WorldRegion sub : subs) {
								if (sub.isRegionMapDownload()) {
									small = false;
									break;
								}
							}
						}
						ps.setLong(1, changesetId);
						ps.setInt(2, map.get(reg));
						ps.setInt(3, small ? 1 : 0);
						ps.addBatch();
					}

				}
				if (batch++ > BATCH_SIZE) {

					System.out.println("Execute batch " + (batchInd++) + " by " + BATCH_SIZE);
					ps.executeBatch();
					batch = 0;
				}
			}
			ps.executeBatch();

		} finally {
			conn.close();
		}

	}

	private static boolean testMissing = true;
	private static boolean insertMissing = true;

	private static OsmandRegions initCountriesTable(Connection conn, boolean empty, Map<WorldRegion, Integer> map)
			throws IOException, SQLException {

		OsmandRegions or = new OsmandRegions();
		or.prepareFile();
		or.cacheAllCountries();
		WorldRegion worldRegion = or.getWorldRegion();
		boolean newCountriesInserted = false;
		if (testMissing) {
			ResultSet allCountriesRs = conn.createStatement().executeQuery("SELECT id, fullname, map from countries");
			Set<String> existingMaps = new TreeSet<>();
			int maxid = 0;
			while (allCountriesRs.next()) {
				maxid = Math.max(maxid, allCountriesRs.getInt(1));
				existingMaps.add(allCountriesRs.getString(2));
			}
			allCountriesRs.close();
			PreparedStatement insert = conn.prepareStatement(
					"INSERT INTO countries(id, parentid, name, fullname, downloadname, clat, clon, map)"
							+ " VALUES(?, ?, ?, ?, ?, ?, ?, ?)");
			PreparedStatement update = conn.prepareStatement("UPDATE countries SET map = ? WHERE fullname = ?");
			PreparedStatement test = conn
					.prepareStatement("SELECT id, downloadname, map FROM countries WHERE fullname = ?");
			LinkedList<WorldRegion> queue = new LinkedList<WorldRegion>();
			queue.add(worldRegion);
			while (!queue.isEmpty()) {
				WorldRegion wr = queue.pollFirst();
				test.setString(1, wr.getRegionId());
				ResultSet rs = test.executeQuery();
				if (rs.next()) {
					existingMaps.remove(wr.getRegionId());
					map.put(wr, rs.getInt(1));
					String downloadName = rs.getString(2);
					int mp = rs.getInt(3);
					if (wr.isRegionMapDownload()
							&& !Algorithms.objectEquals(downloadName, wr.getRegionDownloadName())) {
						LOG.error(String.format("Country download name doesn't match '%s' != '%s'", downloadName,
								wr.getRegionDownloadName()));
					}
					boolean isMap = mp == 1;
					if (wr.isRegionMapDownload() != isMap) {
						// It became available for download
						if (wr.isRegionMapDownload()) {
							LOG.info(String.format(
									"Country '%s' changed it is downloaded state, so database will be updated! '%d' -> '1'",
									downloadName, mp));
							update.setInt(1, 1);
							update.setString(2, wr.getRegionId());
							update.execute();
						} else {
							LOG.error(String.format(
									"Country '%s' couldn't be downloaded anymore, so database should be updated! '%d' -> '0'",
									downloadName, mp));
							LOG.info(String.format("UPDATE countries SET map = 0 WHERE fullname = '%s';", wr.getRegionId()));
						}
					}

				} else {
					maxid++;
					LOG.info(String.format("Insert MISSING country %s with id %d", wr.getRegionId(), maxid));
					if (insertMissing) {
						newCountriesInserted = true;
						insertRegion(map, maxid, insert, wr);
					}
				}
				rs.close();

				List<WorldRegion> lst = wr.getSubregions();
				if (lst != null) {
					queue.addAll(lst);
				}

			}
			insert.executeBatch();
			insert.close();
			if (!existingMaps.isEmpty()) {
				for (String mp : existingMaps) {
					LOG.error(String.format("Country '%s' should be deleted (update to map = 0 in db)", mp));
					LOG.info(String.format("DELETE FROM countries WHERE fullname = '%s';", mp));
				}
			}
		}

		if (newCountriesInserted) {
			PreparedStatement del = conn.prepareStatement(
					"delete from changeset_country where changesetid in (select id from changesets where closed_at_day > ?)");
			String currentMonth = String.format("%1$tY-%1$tm", new Date());
			LOG.info(String.format("Regenerate all countries changeset since %s", currentMonth + "-01"));
			del.setString(1, currentMonth + "-01");
			del.executeUpdate();
			del.close();
			//
		}
		map.clear();
		ResultSet rs = conn.createStatement().executeQuery("select id, fullname from countries where map = 1");
		StringBuilder updateMissingRegions = new StringBuilder();
		StringBuilder missingRegions = new StringBuilder();
		while (rs.next()) {
			int id = rs.getInt(1);
			String regionName = rs.getString(2);
			WorldRegion rd;
			if (regionName.equals("world")) {
				rd = worldRegion;
			} else {
				rd = or.getRegionData(regionName);
			}
			if (rd == null) {
				updateMissingRegions.append("\n UPDATE countries set map = 0 where id =  " + id + ";");
				missingRegions.append(regionName).append(", ");
			} else {
				map.put(rd, id);
			}
		}
		if (updateMissingRegions.length() > 0) {
			throw new UnsupportedOperationException("Some regions (" + missingRegions.toString()
					+ ") were deleted and not present any more. Consider to run in database: " + updateMissingRegions);
		}

		// Iterator<Entry<WorldRegion, Integer>> it = map.entrySet().iterator();
		// while(it.hasNext()){
		// Entry<WorldRegion, Integer> e = it.next();
		// }

		return or;
	}

	private static void insertRegion(Map<WorldRegion, Integer> map, int id, PreparedStatement ps, WorldRegion wr)
			throws SQLException {
		map.put(wr, id);
		ps.setInt(1, id);
		WorldRegion parent = wr.getSuperregion();
		if (parent != null) {
			if (!map.containsKey(parent)) {
				throw new IllegalStateException(
						String.format("Regions are disconnected %s %s", wr.getLocaleName(), parent.getLocaleName()));
			}
			ps.setInt(2, map.get(parent));
		} else {
			ps.setInt(2, 0);
		}

		ps.setString(3, wr.getLocaleName());
		ps.setString(4, wr.getRegionId());
		ps.setString(5, wr.getRegionDownloadName());
		if (wr.getRegionCenter() != null) {
			ps.setDouble(6, wr.getRegionCenter().getLatitude());
			ps.setDouble(7, wr.getRegionCenter().getLongitude());
		} else {
			ps.setDouble(6, 0);
			ps.setDouble(7, 0);
		}
		ps.setInt(8, wr.isRegionMapDownload() ? 1 : 0);
		ps.addBatch();
	}

	private static class ChangesetProps {

		public String uid;
		public String user;
		public Map<String, String> tags = new TreeMap<String, String>();
		public String createdBy;
		public String createdAt;
		public long id;
		public int bot;
		public int changesCount;
		public String closedAt;
		public String minLat;
		public String minLon;
		public String maxLat;
		public String maxLon;

	}
}
