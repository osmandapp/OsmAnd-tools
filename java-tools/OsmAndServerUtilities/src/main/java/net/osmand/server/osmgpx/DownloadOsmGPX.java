package net.osmand.server.osmgpx;


import static net.osmand.util.Algorithms.readFromInputStream;

import java.io.ByteArrayInputStream;
import java.io.File;
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
import java.sql.Array;
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
import java.util.HashSet;
import java.util.List;
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

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.logging.Log;
import org.xmlpull.v1.XmlPullParser;
import org.xmlpull.v1.XmlPullParserException;

import net.osmand.IProgress;
import net.osmand.PlatformUtil;
import net.osmand.binary.MapZooms;
import net.osmand.data.QuadRect;
import net.osmand.gpx.GPXFile;
import net.osmand.gpx.GPXTrackAnalysis;
import net.osmand.gpx.GPXUtilities;
import net.osmand.impl.ConsoleProgressImplementation;
import net.osmand.obf.OsmGpxWriteContext;
import net.osmand.obf.OsmGpxWriteContext.OsmGpxFile;
import net.osmand.obf.OsmGpxWriteContext.QueryParams;
import net.osmand.obf.preparation.IndexCreator;
import net.osmand.obf.preparation.IndexCreatorSettings;
import net.osmand.osm.MapRenderingTypesEncoder;
import net.osmand.osm.OsmRouteType;
import net.osmand.osm.io.Base64;
import net.osmand.util.Algorithms;
import oauth.signpost.OAuthConsumer;
import oauth.signpost.basic.DefaultOAuthConsumer;
import oauth.signpost.exception.OAuthCommunicationException;
import oauth.signpost.exception.OAuthExpectationFailedException;
import oauth.signpost.exception.OAuthMessageSignerException;
import rtree.RTree;

public class DownloadOsmGPX {

	private static final int BATCH_SIZE = 100;
	protected static final Log LOG = PlatformUtil.getLog(DownloadOsmGPX.class);
	private static final String MAIN_GPX_API_ENDPOINT = "https://api.openstreetmap.org/api/0.6/gpx/";
	
	private static final int PS_UPDATE_GPX_DATA = 1;
	private static final int PS_UPDATE_GPX_DETAILS = 2;
	private static final int PS_INSERT_GPX_FILE = 3;
	private static final int PS_INSERT_GPX_DETAILS = 4;
	private static final long FETCH_INTERVAL = 200;
	private static final long FETCH_MAX_INTERVAL = 50000;
	private static final int MAX_EMPTY_FETCH = 5;
	
	// preindex before 76787 with maxlat/minlat
	private static final long INITIAL_ID = 1000; // start with 1000
	private static final String GPX_METADATA_TABLE_NAME = "osm_gpx_data";
	private static final String GPX_FILES_TABLE_NAME = "osm_gpx_files";
	private static final long FETCH_INTERVAL_SLEEP = 10000;
	
	private static final int HTTP_TIMEOUT = 5000;
	private static final int MAX_RETRY_TIMEOUT = 5;
	private static final int RETRY_TIMEOUT = 15000;
	

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
		String main = args.length > 0 ? args[0] : "";
		DownloadOsmGPX utility = new DownloadOsmGPX();
		if ("test_download".equals(main)) {
			String gpx = utility.downloadGpx(57905, "");
			ByteArrayInputStream is = new ByteArrayInputStream(gpx.getBytes());
			System.out.println(gpx);
			GPXUtilities.loadGPXFile(is);
		} else if ("test".equals(main)) {
			QueryParams qp = new QueryParams();
			// qp.minlat = qp.maxlat = 52.35;
			// qp.minlon = qp.maxlon = 4.89;
			qp.minlat = qp.maxlat = 59.1;
			qp.minlon = qp.maxlon = 17.4;
			// qp.tag = "car";
			if (args.length > 1) {
				qp.osmFile = new File(args[1]);
			}
			utility.queryGPXForBBOX(qp);
		} else if ("query".equals(main) || "obf-gen".equals(main)) {
			QueryParams qp = new QueryParams();
			for (int i = 1; i < args.length; i++) {
				String[] s = args[i].split("=");
				if (s.length == 1) {
					continue;
				}
				String val = s[1].trim();
				if (val.isEmpty()) {
					continue;
				}
				switch (s[0]) {
				case "--acitivity-type":
					if (val.trim().length() > 0) {
						qp.activityTypes = new HashSet<>();
						String[] avls = val.split(",");
						for (String av : avls) {
							if (av.trim().length() == 0) {
								continue;
							}
							qp.activityTypes.add(OsmRouteType.getOrCreateTypeFromName(av.trim()));
						}
					}
					break;
				case "--bbox":
					String[] vls = val.split(",");
					qp.minlat = Double.parseDouble(vls[0]);
					qp.minlon = Double.parseDouble(vls[1]);
					qp.maxlat = Double.parseDouble(vls[2]);
					qp.maxlon = Double.parseDouble(vls[3]);
					break;
				case "--details":
					qp.details = Integer.parseInt(val);
					break;
				case "--out":
					if (val.endsWith(".obf")) {
						qp.obfFile = new File(val);
						File dir = qp.obfFile.getParentFile();
						qp.osmFile = new File(dir, Algorithms.getFileNameWithoutExtension(qp.obfFile) + ".osm.gz");
					} else {
						qp.osmFile = new File(val);
					}
					break;
				case "--user":
					qp.user = val;
					break;
				case "--limit":
					qp.limit = Integer.parseInt(val);
					break;
				case "--datestart":
					qp.datestart = val;
					break;
				case "--dateend":
					qp.dateend = val;
					break;
				case "--tag":
					qp.tag = val;
					break;
				}
			}
			if("query".equals(main)) {
				utility.queryGPXForBBOX(qp);
			} else {
				utility.generateObfFile(qp);
			}
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

	protected void queryGPXForBBOX(QueryParams qp) throws SQLException, IOException, FactoryConfigurationError, XMLStreamException, InterruptedException, XmlPullParserException {
		String conditions = "";
		if (!Algorithms.isEmpty(qp.user)) {
			conditions += " and t.\"user\" = '" + qp.user + "'";
		}
		if (!Algorithms.isEmpty(qp.tag)) {
//			conditions += " and '" + qp.tag + "' = ANY(t.tags)";
			String[] tagsAnd = qp.tag.split(",");
			for (String tagAnd : tagsAnd) {
				conditions += " and (";
				String[] tagsOr = tagAnd.split("\\;");
				boolean t = false;
				for(String tagOr : tagsOr) {
					if (t) {
						conditions += " or ";
					}
					conditions += " lower('^'||array_to_string(t.tags,'^','')||'^') like '%^" + tagOr.trim().toLowerCase() + "^%'";
					t = true;
				}
				conditions += ")";
			}
		}
		if (!Algorithms.isEmpty(qp.datestart)) {
			conditions += " and t.date >= '" + qp.datestart + "'";
		}
		if (!Algorithms.isEmpty(qp.dateend)) {
			conditions += " and t.date <= '" + qp.dateend + "'";
		}
		
		if (qp.minlat != OsmGpxFile.ERROR_NUMBER) {
			conditions += " and t.maxlat >= " + qp.minlat;
			conditions += " and t.minlat <= " + qp.maxlat;
			conditions += " and t.maxlon >= " + qp.minlon;
			conditions += " and t.minlon <= " + qp.maxlon;
		}
		String query = "SELECT t.id, s.data, t.name, t.description, t.\"user\", t.date, t.tags from " + GPX_METADATA_TABLE_NAME
				+ " t join " + GPX_FILES_TABLE_NAME + " s on s.id = t.id "
				+ " where 1 = 1 " + conditions + " order by t.date asc";
		if (qp.limit != -1) {
			query += " limit " + qp.limit;
		}
		System.out.println(query);
		ResultSet rs = dbConn.createStatement().executeQuery(query);
		OsmGpxWriteContext ctx = new OsmGpxWriteContext(qp);
		ctx.startDocument();
		Date lastTimestamp = null; 
		while (rs.next()) {
			if ((ctx.tracks + 1) % 1000 == 0) {
				System.out.println(
						String.format("Fetched %d tracks %d segments - last %s (now %s)", ctx.tracks + 1, ctx.segments, lastTimestamp,
								new Date()));
			}
			OsmGpxFile gpxInfo = new OsmGpxFile();
			gpxInfo.id = rs.getLong(1);
			byte[] cont = rs.getBytes(2);
			if (cont == null) {
				continue;
			}
			gpxInfo.name = rs.getString(3);
			gpxInfo.description = rs.getString(4);
			gpxInfo.user = rs.getString(5);
			gpxInfo.timestamp = new Date(rs.getDate(6).getTime());
			lastTimestamp = gpxInfo.timestamp;
			Array tags = rs.getArray(7);
			List<String> trackTags = new ArrayList<>();
			if (tags != null) {
				ResultSet rsar = tags.getResultSet();
				while (rsar.next()) {
					String tg = rsar.getString(2);
					if (tg != null) {
						trackTags.add(tg.toLowerCase());
					}
				}
			}
			gpxInfo.tags = trackTags.toArray(new String[0]);
			if (qp.activityTypes != null) {
				OsmRouteType rat = OsmRouteType.getTypeFromTags(gpxInfo.tags);
				if (rat == null || !qp.activityTypes.contains(rat)) {
					continue;
				}
			}

			ByteArrayInputStream is = new ByteArrayInputStream(Algorithms.gzipToString(cont).getBytes());
			GPXFile gpxFile = GPXUtilities.loadGPXFile(is);
			GPXTrackAnalysis analysis = gpxFile.getAnalysis(gpxInfo.timestamp.getTime());
			ctx.writeTrack(gpxInfo, null, gpxFile, analysis, "OG");
		}
		ctx.endDocument();
		
		System.out.println(String.format("Fetched %d tracks %d segments", ctx.tracks, ctx.segments));
		generateObfFile(qp);
	}

	private void generateObfFile(QueryParams qp)
			throws IOException, SQLException, InterruptedException, XmlPullParserException {
		if(qp.obfFile != null) {
			IndexCreatorSettings settings = new IndexCreatorSettings();
			settings.indexMap = true;
			settings.indexAddress = false;
			settings.indexPOI = true;
			settings.indexTransport = false;
			settings.indexRouting = false;
			RTree.clearCache();
			File folder = new File(qp.obfFile.getParentFile(), "gen");
			String fileName = qp.obfFile.getName();
			File targetObf = qp.obfFile;
			try {
				folder.mkdirs();
				IndexCreator ic = new IndexCreator(folder, settings);
				MapRenderingTypesEncoder types = new MapRenderingTypesEncoder(null, fileName);
				ic.setMapFileName(fileName);
				IProgress prog = IProgress.EMPTY_PROGRESS;
				prog = new ConsoleProgressImplementation();
				ic.generateIndexes(qp.osmFile, prog, null, MapZooms.getDefault(), types, null);
				new File(folder, ic.getMapFileName()).renameTo(targetObf);
			} finally {
				Algorithms.removeAllFiles(folder);
			}
		}
	}

	
	private String downloadGpx(long id, String name)
			throws Exception {
		HttpsURLConnection httpFileConn = getHttpConnection(MAIN_GPX_API_ENDPOINT + id + "/data", MAX_RETRY_TIMEOUT);
		// content-type: application/x-bzip2
		// content-type: application/x-gzip
		InputStream inputStream = null;
		try {
			List<String> hs = httpFileConn.getHeaderFields().get("Content-Type");
			String type = hs == null || hs.size() == 0 ? "" : hs.get(0);
			inputStream = httpFileConn.getInputStream();
			boolean zip = name != null && name.endsWith(".zip");
			if (type.equals("application/x-gzip") || type.equals("application/gzip")) {
				GZIPInputStream gzipIs = new GZIPInputStream(inputStream);
				if (zip) {
					return parseZip(gzipIs);
				}
				return Algorithms.readFromInputStream(gzipIs).toString();
			} else if (type.equals("application/x-zip") || type.equals("application/zip")) {
				return parseZip(inputStream);
			} else if (type.equals("application/gpx+xml")) {
				return Algorithms.readFromInputStream(inputStream).toString();
			} else if (type.equals("application/x-bzip2")) {
				BZip2CompressorInputStream bzis = new BZip2CompressorInputStream(inputStream);
				if (zip) {
					return parseZip(bzis);
				}
				return Algorithms.readFromInputStream(bzis).toString();
			} else if (type.equals("application/x-tar+gzip")) {
				TarArchiveInputStream tarIs = new TarArchiveInputStream(new GZIPInputStream(inputStream));
				return parseTar(tarIs);
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

	private String parseTar(TarArchiveInputStream inputStream) throws IOException {
		TarArchiveEntry tarEntry = inputStream.getNextTarEntry();
		while (tarEntry != null) {
			if (tarEntry.getName().endsWith(".gpx")) {
				return readFromInputStream(inputStream).toString();
			}
			tarEntry = inputStream.getNextTarEntry();
		}
		return null;
	}

	protected void redownloadTagsDescription() throws SQLException, IOException {
		PreparedStatementWrapper wgpx = new PreparedStatementWrapper();
		preparedStatements[PS_UPDATE_GPX_DETAILS] = wgpx;
		wgpx.ps = dbConn.prepareStatement("UPDATE " + GPX_METADATA_TABLE_NAME
				+ " SET description = ?, tags = ? where id = ?");
		ResultSet rs = dbConn.createStatement().executeQuery("SELECT id, name from " + GPX_METADATA_TABLE_NAME 
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
					Thread.sleep(FETCH_INTERVAL_SLEEP);
				}
				maxId = r.id;
				HttpsURLConnection httpConn = getHttpConnection(MAIN_GPX_API_ENDPOINT + r.id + "/details", MAX_RETRY_TIMEOUT);
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
					if (redownload) {
						Thread.sleep(FETCH_INTERVAL_SLEEP);
					}
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
		int emptyFetch = 0;
		for (long id = ID_INIT; id < ID_END; id++) {
			String url = MAIN_GPX_API_ENDPOINT + id + "/details";
			HttpsURLConnection httpConn = getHttpConnection(url, MAX_RETRY_TIMEOUT);
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
				} catch (Throwable e) {
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
					emptyFetch = 0;
				} else {
					long last = (lastSuccess == null ? ID_INIT : lastSuccess.id) + emptyFetch * FETCH_INTERVAL;
					System.out.println(String.format("No successful fetch after %d - %d %s ",
							last,  last + FETCH_INTERVAL, lastTime));
					if (++emptyFetch >= MAX_EMPTY_FETCH) {
						break;
					}
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
		if (gpxFile.error == null) {
			QuadRect rect = gpxFile.getBounds(r.lat, r.lon);
			r.minlon = rect.left;
			r.minlat = rect.bottom;
			r.maxlon = rect.right;
			r.maxlat = rect.top;
			return gpxFile;
		} else {
			errorReadingGpx(r, gpxFile.error);
			return null;
		}
	}



	private void errorReadingGpx(OsmGpxFile r, Throwable e) {
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



	private HttpsURLConnection getHttpConnection(String url, int retry)
			throws NoSuchAlgorithmException, KeyManagementException, IOException, MalformedURLException, OAuthMessageSignerException, OAuthExpectationFailedException, OAuthCommunicationException {
		HttpsURLConnection con;
		try {
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
			String accessToken = System.getenv("OSM_USER_ACCESS_TOKEN");
			String accessTokenSecret = System.getenv("OSM_USER_ACCESS_TOKEN_SECRET");
			String consumerKey = System.getenv("OSM_OAUTH_CONSUMER_KEY");
			String consumerSecret = System.getenv("OSM_OAUTH_CONSUMER_SECRET");
			
			con = (HttpsURLConnection) new URL(url).openConnection();
			con.setConnectTimeout(HTTP_TIMEOUT);
			if (!Algorithms.isEmpty(accessToken)) {
				OAuthConsumer consumer = new DefaultOAuthConsumer(consumerKey, consumerSecret);
				consumer.setTokenWithSecret(accessToken, accessTokenSecret);
				consumer.sign(con);
//				con.setRequestProperty("Authorization", "Basic " + Base64.encode(name + ":" + pwd));
			} else if (name != null && pwd != null) {
				con.setRequestProperty("Authorization", "Basic " + Base64.encode(name + ":" + pwd));
			}

			con.setHostnameVerifier(new HostnameVerifier() {
			    @Override
			    public boolean verify(String arg0, SSLSession arg1) {
			        return true;
			    }
			});
			
			con.getResponseCode();
			return con;
		} catch (IOException e) {
			if (retry > 0) {
				try {
					Thread.sleep(RETRY_TIMEOUT);
				} catch (InterruptedException e1) {
					e1.printStackTrace();
				}
				return getHttpConnection(url, retry - 1);
			} else {
				throw e;
			}
		}
	}


	private static OsmGpxFile parseGPXFiles(StringReader inputReader, List<OsmGpxFile> gpxFiles)
			throws XmlPullParserException, IOException, ParseException {
		XmlPullParser parser = PlatformUtil.newXMLPullParser();
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
