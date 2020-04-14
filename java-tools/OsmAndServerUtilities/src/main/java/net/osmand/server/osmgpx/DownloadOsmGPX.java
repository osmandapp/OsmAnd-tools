package net.osmand.server.osmgpx;


import static net.osmand.util.Algorithms.readFromInputStream;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.X509TrustManager;
import javax.xml.stream.FactoryConfigurationError;
import javax.xml.stream.XMLStreamException;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.logging.Log;
import org.kxml2.io.KXmlParser;
import org.xmlpull.v1.XmlPullParser;
import org.xmlpull.v1.XmlPullParserException;

import net.osmand.GPXUtilities;
import net.osmand.GPXUtilities.GPXFile;
import net.osmand.GPXUtilities.Track;
import net.osmand.GPXUtilities.TrkSegment;
import net.osmand.GPXUtilities.WptPt;
import net.osmand.PlatformUtil;
import net.osmand.osm.edit.Entity;
import net.osmand.osm.edit.EntityInfo;
import net.osmand.osm.edit.Node;
import net.osmand.osm.edit.Way;
import net.osmand.osm.edit.Entity.EntityId;
import net.osmand.osm.io.Base64;
import net.osmand.osm.io.OsmStorageWriter;
import net.osmand.util.Algorithms;

public class DownloadOsmGPX {

	private static final int BATCH_SIZE = 100;
	protected static final Log LOG = PlatformUtil.getLog(DownloadOsmGPX.class);
	private static final String MAIN_GPX_API_ENDPOINT = "https://api.openstreetmap.org/api/0.6/gpx/";
	
	private static final int PS_UPDATE_GPX_DATA = 1;
	private static final int PS_UPDATE_GPX_DETAILS = 2;
	private static final int PS_INSERT_GPX_FILE = 3;
	private static final int PS_INSERT_GPX_DETAILS = 4;
	private static final long FETCH_INTERVAL = 500;
	private static final long FETCH_MAX_INTERVAL = 10000;
	
	// preindex before 76787 with maxlat/minlat
	private static final long INITIAL_ID = 1000; // start with 1000
	private static final String GPX_METADATA_TABLE_NAME = "osm_gpx_data";
	private static final String GPX_FILES_TABLE_NAME = "osm_gpx_files";
	private static final long FETCH_INTERVAL_SLEEP = 2000;

	static SimpleDateFormat FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
	static SimpleDateFormat FORMAT2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z");
	static {
		FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
		FORMAT2.setTimeZone(TimeZone.getTimeZone("UTC"));
	}
	private boolean sslInit;
	private Connection dbConn;
	private PreparedStatementWrapper[] preparedStatements = new PreparedStatementWrapper[PS_INSERT_GPX_DETAILS + 1];
	
	public DownloadOsmGPX() throws SQLException {
		initDBConnection();
	}
	
	public static void main(String[] args) throws Exception {
		String main = args.length  > 0 ? args[0] : "";
		DownloadOsmGPX utility = new DownloadOsmGPX();
		if ("test_download".equals(main)) {
			String gpx = utility.downloadGpx(57905, "");
			ByteArrayInputStream is = new ByteArrayInputStream(gpx.getBytes());
			System.out.println(gpx);
			GPXUtilities.loadGPXFile(is);
		} else if ("test".equals(main)) {
			QueryParams qp = new QueryParams();
//			qp.minlat = qp.maxlat = 52.35;
//			qp.minlon = qp.maxlon = 4.89;
			qp.minlat = qp.maxlat = 59.1;
			qp.minlon = qp.maxlon = 17.4;
			if (args.length > 1) {
				qp.osmFile = new File(args[1]);
			}
			utility.queryGPXForBBOX(qp);
		} else if ("redownload_tags_and_description".equals(main)) {
			utility.redownloadTagsDescription();
		} else if ("recalculateminmax".equals(main)) {
			utility.recalculateMinMaxLatLon(false);
		} else if ("recalculateminmax_and_download".equals(main)) {
			utility.recalculateMinMaxLatLon(true);
		} else {
			utility.downloadGPXMain();
		}
		utility.commitAllStatements();
	}
	
	
	protected void queryGPXForBBOX(QueryParams qp) throws SQLException, IOException, FactoryConfigurationError, XMLStreamException {
		String query = String.format("SELECT t.id, s.data, t.name, t.lat, t.lon from " + GPX_METADATA_TABLE_NAME
				+ " t join " + GPX_FILES_TABLE_NAME + " s on s.id = t.id "
				+ " where t.maxlat >= %1$f and t.minlat <= %2$f and t.maxlon >= %3$f and t.minlon <= %4$f ",
				qp.minlat, qp.maxlat, qp.minlon, qp.maxlon);
		System.out.println(query);
		ResultSet rs = dbConn.createStatement().executeQuery(query);
		int tracks = 0;
		GPXFile res = new GPXFile("");
		while (rs.next()) {
			byte[] cont = rs.getBytes(2);
			if(cont != null) {
				ByteArrayInputStream is = new ByteArrayInputStream(Algorithms.gzipToString(cont).getBytes());
				GPXFile gpxFile = GPXUtilities.loadGPXFile(is);
				res.tracks.addAll(gpxFile.tracks);
				tracks++;
			}
		}
		System.out.println(String.format("Fetched %d tracks", tracks));
		if (qp.osmFile != null) {
			Map<EntityId, Entity> entities = new LinkedHashMap<Entity.EntityId, Entity>();
			long id = -10;
			for (Track t : res.tracks) {
				for (TrkSegment s : t.segments) {
					Way w = new Way(id--);
					for (WptPt p : s.points) {
						Node n = new Node(p.lat, p.lon, id--);
						w.addNode(n);
						entities.put(EntityId.valueOf(n), n);
					}
					entities.put(EntityId.valueOf(w), w);
				}
			}
			Map<EntityId, EntityInfo> entityInfo = null;
			new OsmStorageWriter().saveStorage(new FileOutputStream(qp.osmFile), entities, entityInfo, null, true);
		}
	}
	
	private String downloadGpx(long id, String name)
			throws NoSuchAlgorithmException, KeyManagementException, IOException, MalformedURLException {
		HttpsURLConnection httpFileConn = getHttpConnection(MAIN_GPX_API_ENDPOINT + id + "/data");
		// content-type: application/x-bzip2
		// content-type: application/x-gzip
		InputStream inputStream = null;
		try {
			List<String> hs = httpFileConn.getHeaderFields().get("Content-Type");
			String type = hs == null || hs.size() == 0 ? "" : hs.get(0);
			inputStream = httpFileConn.getInputStream();
			boolean zip = name != null && name.endsWith(".zip");
			if (type.equals("application/x-gzip")) {
				GZIPInputStream gzipIs = new GZIPInputStream(inputStream);
				if (zip) {
					return parseZip(gzipIs);
				}
				return Algorithms.readFromInputStream(gzipIs).toString();
			} else if (type.equals("application/x-zip")) {
				return parseZip(inputStream);
			} else if (type.equals("application/gpx+xml")) {
				return Algorithms.readFromInputStream(inputStream).toString();
			} else if (type.equals("application/x-bzip2")) {
				BZip2CompressorInputStream bzis = new BZip2CompressorInputStream(inputStream);
				if(zip) {
					return parseZip(bzis);
				}
				return Algorithms.readFromInputStream(bzis).toString();
			}
			throw new UnsupportedOperationException("Unsupported content-type: " + type);
		} finally {
			if (inputStream != null) {
				inputStream.close();
			}
		}
	}

	private String parseZip(InputStream inputStream) throws IOException {
		ZipInputStream zp = new ZipInputStream(inputStream);
		ZipEntry ze = zp.getNextEntry();
		while (ze != null) {
			if (ze.getName().endsWith(".gpx")) {
				return readFromInputStream(zp).toString();
			}
			ze = zp.getNextEntry();
		}
		return null;
	}
	
	protected void redownloadTagsDescription() throws SQLException, IOException {
		PreparedStatementWrapper wgpx = new PreparedStatementWrapper();
		preparedStatements[PS_UPDATE_GPX_DETAILS] = wgpx;
		wgpx.ps = dbConn.prepareStatement("UPDATE " + GPX_METADATA_TABLE_NAME
				+ " SET description = ?, tags = ? where id = ?");
		ResultSet rs = dbConn.createStatement().executeQuery("SELECT id, name from " + GPX_FILES_TABLE_NAME 
				+ " where description is null order by 1 asc");
		long minId = 0;
		long maxId = 0;
		int batchSize = 0;
		while (rs.next()) {
			OsmGpxFile r = new OsmGpxFile();
			try {
				r.id = rs.getLong(1);
				r.name = rs.getString(2);
				if (++batchSize == FETCH_INTERVAL) {
					System.out
							.println(String.format("Downloaded %d %d - %d, %s ", batchSize, minId, maxId, new Date()));
					minId = r.id;
					batchSize = 0;
				}
				maxId = r.id;
				HttpsURLConnection httpConn = getHttpConnection(MAIN_GPX_API_ENDPOINT + r.id + "/details");
				StringBuilder sb = Algorithms.readFromInputStream(httpConn.getInputStream());
				r = parseGPXFiles(new StringReader(sb.toString()), null);
				wgpx.ps.setString(1, r.description);
				wgpx.ps.setArray(2, r.tags == null ? null : dbConn.createArrayOf("text", r.tags));
				wgpx.ps.setLong(3, r.id);
				wgpx.addBatch();
			} catch (Exception e) {
				errorReadingGpx(r, e);
			}

		}
		commitAllStatements();
	}

	protected void recalculateMinMaxLatLon(boolean redownload) throws SQLException, IOException {
		PreparedStatementWrapper wgpx = new PreparedStatementWrapper();
		preparedStatements[PS_UPDATE_GPX_DETAILS] = wgpx;
		wgpx.ps = dbConn.prepareStatement("UPDATE " + GPX_METADATA_TABLE_NAME
				+ " SET minlat = ?, minlon = ?, maxlat = ?, maxlon = ? where id = ? ");
		PreparedStatementWrapper wdata = new PreparedStatementWrapper();
		preparedStatements[PS_UPDATE_GPX_DATA] = wdata;
		wdata.ps = dbConn.prepareStatement("UPDATE " + GPX_FILES_TABLE_NAME
				+ " SET data = ? where id = ? ");
		ResultSet rs = dbConn.createStatement().executeQuery("SELECT t.id, t.name, t.lat, t.lon, s.data from "
				+ GPX_METADATA_TABLE_NAME + " t join " + GPX_FILES_TABLE_NAME + " s on s.id = t.id "
				+ " where t.maxlat is null order by 1 asc");

		long minId = 0;
		long maxId = 0;
		int batchSize = 0;
		while (rs.next()) {
			OsmGpxFile r = new OsmGpxFile();
			try {
				r.id = rs.getLong(1);
				r.name= rs.getString(2);
				r.lat = rs.getDouble(3);
				r.lon = rs.getDouble(4);
				if (++batchSize == FETCH_INTERVAL) {
					System.out.println(
							String.format("Downloaded %d %d - %d, %s ", batchSize, minId, maxId, new Date()));
					minId = r.id;
					batchSize = 0;
				}
				maxId = r.id;
				r.gpxGzip = rs.getBytes(5);
				boolean download = redownload || r.gpxGzip == null;
				if (!download) {
					r.gpx = Algorithms.gzipToString(r.gpxGzip);
				} else {
					r.gpx = downloadGpx(r.id, r.name);
					if (!Algorithms.isEmpty(r.gpx)) {
						r.gpxGzip = Algorithms.stringToGzip(r.gpx);
						wdata.ps.setBytes(1, r.gpxGzip);
						wdata.ps.setLong(2, r.id);
						wdata.addBatch();
					}
				}
				GPXFile res = calculateMinMaxLatLon(r);
				if (res != null && r.minlat != OsmGpxFile.ERROR_NUMBER && r.minlon != OsmGpxFile.ERROR_NUMBER) {
					wgpx.ps.setDouble(1, r.minlat);
					wgpx.ps.setDouble(2, r.minlon);
					wgpx.ps.setDouble(3, r.maxlat);
					wgpx.ps.setDouble(4, r.maxlon);
					wgpx.ps.setLong(5, r.id);
					wgpx.addBatch();
				}
			} catch (Exception e) {
				errorReadingGpx(r, e);
			}

		}
		commitAllStatements();
	}

	protected void downloadGPXMain() throws Exception {
		Long maxId = (Long) executeSQLQuery("SELECT max(id) from " + GPX_METADATA_TABLE_NAME);
		long ID_INIT = Math.max(INITIAL_ID, maxId == null ? 0 : (maxId.longValue()  + 1));
		long ID_END = ID_INIT + FETCH_MAX_INTERVAL;
		int batchFetch = 0; 
		int success = 0;
		OsmGpxFile lastSuccess = null;
		System.out.println("Start with id: " + ID_INIT);
		for (long id = ID_INIT; id < ID_END; id++) {
			String url = MAIN_GPX_API_ENDPOINT + id + "/details";
			HttpsURLConnection httpConn = getHttpConnection(url);
			int responseCode = httpConn.getResponseCode();
			if (responseCode == 404 || responseCode == 403) {
				// skip non-accessible id && forgotten
//				System.out.println("SKIP forbidden gpx: " + id);
			} else if (responseCode == 200) {
				StringBuilder sb = Algorithms.readFromInputStream(httpConn.getInputStream());
				OsmGpxFile r = parseGPXFiles(new StringReader(sb.toString()), null);
				if (r.pending) {
					System.out.println("STOP on first pending gpx: " + id);
					break;
				}
				try {
					r.gpx = downloadGpx(id, r.name);
					r.gpxGzip = Algorithms.stringToGzip(r.gpx);
					calculateMinMaxLatLon(r);
				} catch (Exception e) {
					errorReadingGpx(r, e);
				} finally {
					lastSuccess = r;
					insertGPXFile(r);
					success++;

				}
			} else {
				throw new UnsupportedOperationException("Code: " + responseCode + " id " + id);
			}
			if (++batchFetch >= FETCH_INTERVAL) {
				String lastTime = lastSuccess == null ? "" : lastSuccess.timestamp.toString();
				if (success > 0) {
					System.out.println(String.format("Fetched %d gpx from %d - %d (%s). Now: %s ", success,
							id - FETCH_INTERVAL + 1, id, lastTime, new Date()));
				} else {
					System.out.println(String.format("STOP no successful fetch after %d %s",
							lastSuccess == null ? ID_INIT : lastSuccess.id, lastTime));
					break;
				}
				batchFetch = 0;
				success = 0;
				Thread.sleep(FETCH_INTERVAL_SLEEP);
			}
			
		}
		commitAllStatements();
	}



	private GPXFile calculateMinMaxLatLon(OsmGpxFile r) {
		GPXFile gpxFile = GPXUtilities.loadGPXFile(new ByteArrayInputStream(r.gpx.getBytes()));
		if (gpxFile != null && gpxFile.error == null) {
			r.minlat = r.maxlat = r.lat;
			r.minlon = r.maxlon = r.lon;
			for (Track t : gpxFile.tracks) {
				for (TrkSegment s : t.segments) {
					for (WptPt p : s.points) {
						r.minlat = Math.min(r.minlat, p.lat);
						r.maxlat = Math.max(r.maxlat, p.lat);
						r.minlon = Math.min(r.minlon, p.lon);
						r.maxlon = Math.max(r.maxlon, p.lon);
					}
				}
			}
			return gpxFile;
		} else {
			errorReadingGpx(r, gpxFile.error);
			return null;
		}
	}



	private void errorReadingGpx(OsmGpxFile r, Exception e) {
		LOG.error(String.format("### ERROR while reading GPX %d - %s: %s", r.id, r.name,
				e != null ? e.getMessage() : ""));
	}

	private void insertGPXFile(OsmGpxFile r) throws SQLException {
		PreparedStatementWrapper wrapper = preparedStatements[PS_INSERT_GPX_DETAILS];
		if(wrapper == null) {
			wrapper = new PreparedStatementWrapper();
			wrapper.ps = dbConn.prepareStatement("INSERT INTO " + GPX_METADATA_TABLE_NAME
					+ "(id, \"user\", \"date\", name, lat, lon, minlat, minlon, maxlat, maxlon, pending, visibility, tags, description) "
					+ " VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
			preparedStatements[PS_INSERT_GPX_DETAILS] = wrapper;
		}
		int ind = 1;
		wrapper.ps.setLong(ind++, r.id);
		wrapper.ps.setString(ind++, r.user);
		wrapper.ps.setDate(ind++, new java.sql.Date(r.timestamp.getTime()));
		wrapper.ps.setString(ind++, r.name);
		wrapper.ps.setDouble(ind++, r.lat);
		wrapper.ps.setDouble(ind++, r.lon);
		if (r.minlat == OsmGpxFile.ERROR_NUMBER || r.minlon == OsmGpxFile.ERROR_NUMBER) {
			wrapper.ps.setNull(ind++, java.sql.Types.FLOAT);
			wrapper.ps.setNull(ind++, java.sql.Types.FLOAT);
			wrapper.ps.setNull(ind++, java.sql.Types.FLOAT);
			wrapper.ps.setNull(ind++, java.sql.Types.FLOAT);
		} else {
			wrapper.ps.setDouble(ind++, r.minlat);
			wrapper.ps.setDouble(ind++, r.minlon);
			wrapper.ps.setDouble(ind++, r.maxlat);
			wrapper.ps.setDouble(ind++, r.maxlon);
		}
		wrapper.ps.setBoolean(ind++, r.pending);
		wrapper.ps.setString(ind++, r.visibility);
		wrapper.ps.setArray(ind++, r.tags == null ? null : dbConn.createArrayOf("text", r.tags));
		wrapper.ps.setString(ind++, r.description);
		wrapper.addBatch();
		
		PreparedStatementWrapper wrapperFile = preparedStatements[PS_INSERT_GPX_FILE];
		if(wrapperFile == null) {
			wrapperFile = new PreparedStatementWrapper();
			wrapperFile.ps = dbConn.prepareStatement("INSERT INTO " + GPX_FILES_TABLE_NAME
					+ "(id, data) "
					+ " VALUES(?, ?)");
			preparedStatements[PS_INSERT_GPX_FILE] = wrapperFile;
		}
		wrapperFile.ps.setLong(1, r.id);
		wrapperFile.ps.setBytes(2, r.gpxGzip);
		wrapperFile.addBatch();
	}



	private void commitAllStatements() throws SQLException {
		for(PreparedStatementWrapper w : preparedStatements) {
			if(w != null && w.ps != null) {
				if(w.pending > 0) {
					w.ps.executeBatch();
					w.pending = 0;
				}
				w.ps.close();
				w.ps = null;
			}
		}
	}



	private void initDBConnection() throws SQLException {
		if (dbConn == null) {
			dbConn = DriverManager.getConnection(System.getenv("DB_CONN"),
					isEmpty(System.getenv("DB_USER")) ? "test" : System.getenv("DB_USER"),
					isEmpty(System.getenv("DB_PWD")) ? "test" : System.getenv("DB_PWD"));
//			executeSQL("DROP TABLE " + TABLE_NAME);
			ResultSet rs = dbConn.getMetaData().getTables(null, null, GPX_METADATA_TABLE_NAME, null);
			if (!rs.next()) {
				executeSQL("CREATE TABLE " + GPX_FILES_TABLE_NAME + " (id bigint primary key, data bytea)");
				executeSQL("CREATE TABLE " + GPX_METADATA_TABLE_NAME
						+ "(id bigint primary key, \"user\" text, \"date\" timestamp, name text, lat float, lon float, "
						+ " minlat float, minlon float, maxlat float, maxlon float, "
						+ "pending boolean, tags text[], description text, visibility text)");
			}
		}
	}


	private void executeSQL(String sql) throws SQLException {
		Statement statement = dbConn.createStatement();
		statement.execute(sql);
		statement.close();
	}
	
	private Object executeSQLQuery(String sql) throws SQLException {
		Statement statement = dbConn.createStatement();
		ResultSet rs = statement.executeQuery(sql);
		if(rs.next()) {
			return rs.getObject(1);
		}
		statement.close();
		return null;
	}



	private HttpsURLConnection getHttpConnection(String url)
			throws NoSuchAlgorithmException, KeyManagementException, IOException, MalformedURLException {
		if (!sslInit) {
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
			sslInit = true;
		}
        String name = System.getenv("OSM_USER");
        String pwd = System.getenv("OSM_PASSWORD");
        HttpsURLConnection con = (HttpsURLConnection) new URL(url).openConnection();
		if (name != null && pwd != null) {
			con.setRequestProperty("Authorization", "Basic " + Base64.encode(name + ":" + pwd));
		}

		con.setHostnameVerifier(new HostnameVerifier() {
            @Override
            public boolean verify(String arg0, SSLSession arg1) {
                return true;
            }
        });
		return con;
	}


	private static OsmGpxFile parseGPXFiles(StringReader inputReader, List<OsmGpxFile> gpxFiles)
			throws XmlPullParserException, IOException, ParseException {
		XmlPullParser parser = new KXmlParser();
		parser.setInput(inputReader);
		int tok;
		OsmGpxFile p = null;
		List<String> tags = new ArrayList<String>();
		while((tok = parser.next()) != XmlPullParser.END_DOCUMENT) {
			if(tok == XmlPullParser.START_TAG) {
				if(parser.getName().equals("gpx_file")) {
					p = new OsmGpxFile();
					p.id = Long.parseLong(parser.getAttributeValue("", "id"));
					p.user = parser.getAttributeValue("", "user");
					p.name = parser.getAttributeValue("", "name");
					p.visibility = parser.getAttributeValue("", "visibility");
					p.pending = "true".equals(parser.getAttributeValue("", "visibility"));
					p.id = Long.parseLong(parser.getAttributeValue("", "id"));
					p.timestamp = FORMAT.parse(parser.getAttributeValue("", "timestamp"));
					p.lat = Double.parseDouble(getAttributeDoubleValue(parser, "lat"));
					p.lon = Double.parseDouble(getAttributeDoubleValue(parser, "lon"));
				} else if(parser.getName().equals("description") && p != null) {
					p.description = readText(parser, parser.getName());
				} else if(parser.getName().equals("tag")) {
					String value = readText(parser, parser.getName());
					tags.add(value);
				} 
			} else if(tok == XmlPullParser.END_TAG) {
				if(parser.getName().equals("gpx_file")) {
					if(p != null && gpxFiles != null) {
						gpxFiles.add(p);
					}
				} 
			}
		}
		if (tags.size() > 0) {
			p.tags = tags.toArray(new String[tags.size()]);
		}
		return p;
	}

	private static String readText(XmlPullParser parser, String key) throws XmlPullParserException, IOException {
		int tok;
		StringBuilder text = null;
		while ((tok = parser.next()) != XmlPullParser.END_DOCUMENT) {
			if (tok == XmlPullParser.END_TAG && parser.getName().equals(key)) {
				break;
			} else if (tok == XmlPullParser.TEXT) {
				if (text == null) {
					text = new StringBuilder(parser.getText());
				} else {
					text.append(parser.getText());
				}
			}
		}
		return text == null ? null : text.toString();
	}

	protected static class OsmGpxFile {
		
		private static final double ERROR_NUMBER = -1000;
		long id;
		String name;
		Date timestamp;
		boolean pending;
		String user;
		String visibility;
		double lat;
		double lon;
		String description;
		
		double minlat = ERROR_NUMBER;
		double minlon = ERROR_NUMBER;
		double maxlat = ERROR_NUMBER;
		double maxlon = ERROR_NUMBER;
		
		String[] tags;
		String gpx;
		byte[] gpxGzip;
	}

	protected static class QueryParams {
		File osmFile;
		double minlat;
		double maxlat;
		double maxlon;
		double minlon;
	}

	private static class PreparedStatementWrapper {
		PreparedStatement ps;
		int pending;

		public boolean addBatch() throws SQLException {
			ps.addBatch();
			pending++;
			if (pending > BATCH_SIZE) {
				ps.executeBatch();
				pending = 0;
				return true;
			}
			return false;

		}
	}
	
	private static String getAttributeDoubleValue(XmlPullParser parser, String key) {
		String vl = parser.getAttributeValue("", key);
		if(isEmpty(vl)) {
			return "0";
		}
		return vl;
	}

	private static boolean isEmpty(String vl) {
		return vl == null || vl.equals("");
	}

	
	
}
