package net.osmand.server.osmgpx;


import java.io.IOException;
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
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.zip.GZIPInputStream;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.X509TrustManager;

import org.apache.commons.logging.Log;
import org.kxml2.io.KXmlParser;
import org.xmlpull.v1.XmlPullParser;
import org.xmlpull.v1.XmlPullParserException;

import net.osmand.PlatformUtil;
import net.osmand.osm.io.Base64;
import net.osmand.util.Algorithms;

public class DownloadOsmGPX {

	private static final int BATCH_SIZE = 100;
	protected static final Log LOG = PlatformUtil.getLog(DownloadOsmGPX.class);
	private static final String MAIN_GPX_API_ENDPOINT = "https://api.openstreetmap.org/api/0.6/gpx/";
	
	private static final int PS_INSERT_GPX_FILE = 3;
	private static final int PS_INSERT_GPX_DETAILS = 4;
	private static final long FETCH_INTERVAL = 200;
	private static final long FETCH_MAX_INTERVAL = 10000;
	private static final long INITIAL_ID = 1000;
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
	
	
	public static void main(String[] args) throws Exception {
		new DownloadOsmGPX().downloadGPXMain();
	}

	

	private void downloadGPXMain() throws Exception {
		initDBConnection();
		Long maxId = (Long) executeSQLQuery("SELECT max(id) from " + GPX_METADATA_TABLE_NAME);
		long ID_INIT = maxId == null ? INITIAL_ID : maxId.longValue()  + 1;
		long ID_END = ID_INIT + FETCH_MAX_INTERVAL;
		int batchFetch = 0; 
		int success = 0;
		OsmGpxFile lastSuccess = null;
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
				HttpsURLConnection httpFileConn = getHttpConnection(MAIN_GPX_API_ENDPOINT + id + "/data");
				GZIPInputStream gzipIs = new GZIPInputStream(httpFileConn.getInputStream());
				r.gpx = Algorithms.readFromInputStream(gzipIs).toString();
				r.gpxGzip = Algorithms.stringToGzip(r.gpx);
				lastSuccess = r;
				insertGPXFile(r);
				success++;
			} else {
				throw new UnsupportedOperationException("Code: " + responseCode + " id " + id);
			}
			if (batchFetch++ > FETCH_INTERVAL) {
				String lastTime = lastSuccess == null ? "" : lastSuccess.timestamp.toString();
				if (success > 0) {
					System.out.println(String.format("Fetched %d gpx from %d - %d (%s). Now: %s ",
							success, id - FETCH_INTERVAL, id,  lastTime, new Date()));
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





	private void insertGPXFile(OsmGpxFile r) throws SQLException {
		PreparedStatementWrapper wrapper = preparedStatements[PS_INSERT_GPX_DETAILS];
		if(wrapper == null) {
			wrapper = new PreparedStatementWrapper();
			wrapper.ps = dbConn.prepareStatement("INSERT INTO " + GPX_METADATA_TABLE_NAME
					+ "(id, \"user\", \"date\", name, lat, lon, pending, visibility, description) "
					+ " VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)");
			preparedStatements[PS_INSERT_GPX_DETAILS] = wrapper;
		}
		wrapper.ps.setLong(1, r.id);
		wrapper.ps.setString(2, r.user);
		wrapper.ps.setDate(3, new java.sql.Date(r.timestamp.getTime()));
		wrapper.ps.setString(4, r.name);
		wrapper.ps.setDouble(5, r.lat);
		wrapper.ps.setDouble(6, r.lon);
		wrapper.ps.setBoolean(7, r.pending);
		wrapper.ps.setString(8, r.visibility);
		wrapper.ps.setString(9, r.description);
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
			if(w != null && w.pending > 0) {
				w.ps.executeBatch();
			}
			if(w != null) {
				w.ps.close();
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
						+ "pending boolean, description text, visibility text)");
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
					p.description = parser.getText();
				}
			} else if(tok == XmlPullParser.END_TAG) {
				if(parser.getName().equals("gpx_file")) {
					if(p != null && gpxFiles != null) {
						gpxFiles.add(p);
					}
				}
			}
		}
		return p;
	}


	protected static class OsmGpxFile {
		
		long id;
		String name;
		Date timestamp;
		boolean pending;
		String user;
		String visibility;
		double lat;
		double lon;
		String description;
		
		String gpx;
		byte[] gpxGzip;
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
