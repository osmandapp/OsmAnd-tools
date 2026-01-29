package net.osmand.server.osmgpx;


import static net.osmand.util.Algorithms.readFromInputStream;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
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
import java.util.*;
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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.osmand.shared.data.KQuadRect;
import net.osmand.shared.gpx.primitives.WptPt;
import okio.Source;
import okio.Buffer;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.logging.Log;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.xmlpull.v1.XmlPullParser;
import org.xmlpull.v1.XmlPullParserException;

import net.osmand.IProgress;
import net.osmand.PlatformUtil;
import net.osmand.binary.MapZooms;
import net.osmand.shared.gpx.GpxFile;
import net.osmand.shared.gpx.GpxTrackAnalysis;
import net.osmand.shared.gpx.GpxUtilities;
import net.osmand.impl.ConsoleProgressImplementation;
import net.osmand.obf.OsmGpxWriteContext;
import net.osmand.obf.OsmGpxWriteContext.OsmGpxFile;
import net.osmand.obf.OsmGpxWriteContext.QueryParams;
import net.osmand.obf.preparation.IndexCreator;
import net.osmand.obf.preparation.IndexCreatorSettings;
import net.osmand.osm.MapRenderingTypesEncoder;
import net.osmand.osm.OsmRouteType;
import net.osmand.util.Algorithms;
import oauth.signpost.exception.OAuthCommunicationException;
import oauth.signpost.exception.OAuthExpectationFailedException;
import oauth.signpost.exception.OAuthMessageSignerException;
import rtree.RTree;

public class DownloadOsmGPX {

	private static final int BATCH_SIZE = 100;
	protected static final Log LOG = PlatformUtil.getLog(DownloadOsmGPX.class);
	private static final String MAIN_GPX_API_ENDPOINT = "https://api.openstreetmap.org/api/0.6/gpx/";

	private static final String ENV_OAUTH2_AUTH_CODE = "OSM_OAUTH2_AUTH_CODE"; // setup-only
	private static final String ENV_OAUTH2_CLIENT_ID = "OSM_OAUTH2_CLIENT_ID"; // setup-only
	private static final String ENV_OAUTH2_CLIENT_SECRET = "OSM_OAUTH2_CLIENT_SECRET"; // setup-only
	private static final String ENV_OAUTH2_ACCESS_TOKEN = "OSM_OAUTH2_ACCESS_TOKEN"; // finally required

	private static final int PS_UPDATE_GPX_DATA = 1;
	private static final int PS_UPDATE_GPX_DETAILS = 2;
	private static final int PS_INSERT_GPX_FILE = 3;
	private static final int PS_INSERT_GPX_DETAILS = 4;
	private static final long FETCH_INTERVAL = 200;
	private static final long FETCH_MAX_INTERVAL = 50000;
	private static int MAX_EMPTY_FETCH = 7;
	
	// preindex before 76787 with maxlat/minlat
	private static final long INITIAL_ID = 1000; // start with 1000
	private static final String GPX_METADATA_TABLE_NAME = "osm_gpx_data";
	private static final String GPX_FILES_TABLE_NAME = "osm_gpx_files";
	private static final long FETCH_INTERVAL_SLEEP = 10000;
	
	private static final int HTTP_TIMEOUT = 5000;
	private static final int MAX_RETRY_TIMEOUT = 5;
	private static final int RETRY_TIMEOUT = 15000;

	private static final String GARBAGE_ACTIVITY_TYPE = "garbage";
	private static final String ERROR_ACTIVITY_TYPE = "error";
	private static final String NOSPEED_ACTIVITY_TYPE = "nospeed";

	private static final int MIN_POINTS_SIZE = 100;
	private static final int MIN_DISTANCE = 1000;
	private static final int MAX_DISTANCE_BETWEEN_POINTS = 1000;

	private static final String GPX_FILE_PREIX = "OG";

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
		//createActivitiesMap("../../");
		if ("test_download".equals(main)) {
			String gpx = utility.downloadGpx(57905, "");
			Source src = new Buffer().write(gpx.getBytes());
			GpxUtilities.INSTANCE.loadGpxFile(src);
			System.out.println(gpx);
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
		} else if ("add_activity".equals(main)) {
			utility.addActivityColumnAndPopulate(args[1]);
		} else if ("update_activity".equals(main)) {
			utility.updateActivity(args[1]);
		} else {
			System.out.println("Arguments " + Arrays.toString(args));
			for (int i = 0; i < args.length; i++) {
				String[] s = args[i].split("=");
				if (s.length == 1) {
					continue;
				}
				String val = s[1].trim();
				if (val.isEmpty()) {
					continue;
				}
				switch (s[0]) {
				case "--max-empty-fetch":
					MAX_EMPTY_FETCH = Integer.parseInt(val);
					System.out.println("Max empty fetch " + MAX_EMPTY_FETCH);
					break;
				}
			}
			utility.downloadGPXMain();
		}
		utility.commitAllStatements();
	}

	private void ensureActivitySchema() throws SQLException {
		try (Statement statement = dbConn.createStatement()) {
			statement.executeUpdate("CREATE INDEX IF NOT EXISTS idx_osm_gpx_data_id ON " + GPX_METADATA_TABLE_NAME + " (id)");
			statement.executeUpdate("ALTER TABLE " + GPX_METADATA_TABLE_NAME + " ADD COLUMN IF NOT EXISTS activity text");

			statement.executeUpdate("ALTER TABLE " + GPX_METADATA_TABLE_NAME + " ADD COLUMN IF NOT EXISTS speed float");
			statement.executeUpdate("ALTER TABLE " + GPX_METADATA_TABLE_NAME + " ADD COLUMN IF NOT EXISTS distance float");
			statement.executeUpdate("ALTER TABLE " + GPX_METADATA_TABLE_NAME + " ADD COLUMN IF NOT EXISTS points integer");

			statement.executeUpdate("CREATE INDEX IF NOT EXISTS idx_osm_gpx_speed ON " + GPX_METADATA_TABLE_NAME + " (speed) WHERE speed > 0");
			statement.executeUpdate("CREATE INDEX IF NOT EXISTS idx_osm_gpx_distance ON " + GPX_METADATA_TABLE_NAME + " (distance) WHERE distance > 0");
			statement.executeUpdate("CREATE INDEX IF NOT EXISTS idx_osm_gpx_points ON " + GPX_METADATA_TABLE_NAME + " (points) WHERE points > 0");

			statement.executeUpdate("CREATE INDEX IF NOT EXISTS idx_osm_gpx_activity ON " + GPX_METADATA_TABLE_NAME + " (activity)");
			statement.executeUpdate("CREATE INDEX IF NOT EXISTS idx_osm_gpx_tags_gin ON " + GPX_METADATA_TABLE_NAME + " USING GIN (tags)");

			statement.executeUpdate("CREATE EXTENSION IF NOT EXISTS postgis");
			statement.executeUpdate(
					"CREATE INDEX IF NOT EXISTS idx_osm_gpx_bbox_gist " +
							"ON " + GPX_METADATA_TABLE_NAME +
							" USING GIST (ST_MakeEnvelope(minlon, minlat, maxlon, maxlat, 4326))"
			);
			statement.executeUpdate("CREATE INDEX IF NOT EXISTS idx_osm_gpx_year ON " + GPX_METADATA_TABLE_NAME + " ((extract(year from date)))");
		}
		LOG.info("Activity schema (columns and indexes) ensured.");
	}

	protected void addActivityColumnAndPopulate(String rootPath) throws SQLException {
		LOG.info("Starting add_activity (column/indexes + populate only activity IS NULL)...");
		ensureActivitySchema();
		Map<String, List<String>> activitiesMap = createActivitiesMap(rootPath);
		if (activitiesMap.isEmpty()) {
			LOG.info("Activities map is empty. Skipping the 'activity' column population.");
		} else {
			fillActivityColumn(activitiesMap, false);
		}
	}

	protected void updateActivity(String rootPath) throws SQLException {
		LOG.info("Starting update_activity (all records)...");
		ensureActivitySchema();
		Map<String, List<String>> activitiesMap = createActivitiesMap(rootPath);
		if (activitiesMap.isEmpty()) {
			LOG.info("Activities map is empty. Skipping.");
			return;
		}
		fillActivityColumn(activitiesMap, true);
		LOG.info("Update activity finished.");
	}

	private void fillActivityColumn(Map<String, List<String>> activitiesMap, boolean update) throws SQLException {
		LOG.info("Starting to populate the 'activity' column" + (update ? " (all records)" : " (only activity IS NULL)") + "...");
		dbConn.setAutoCommit(false);
		PreparedStatement updateStmtMetrics = dbConn.prepareStatement(
				"UPDATE " + GPX_METADATA_TABLE_NAME + " SET activity = ?, speed = ?, distance = ?, points = ? WHERE id = ?"
		);
		PreparedStatement updateStmtActivityOnly = dbConn.prepareStatement(
				"UPDATE " + GPX_METADATA_TABLE_NAME + " SET activity = ? WHERE id = ?"
		);

		int batchSize = 0;
		final int BATCH_LIMIT = 1000;
		int processedCount = 0;
		int identifiedActivityCount = 0;
		long lastUpdatedId = -1; // so id=0 is included when present
		boolean hasMoreRecords = true;
		try {
			while (hasMoreRecords) {
				hasMoreRecords = false;

				String selectSql = update
						? "SELECT id, name, description, tags FROM " + GPX_METADATA_TABLE_NAME + " WHERE id > " + lastUpdatedId + " ORDER BY id LIMIT " + BATCH_LIMIT
						: "SELECT id, name, description, tags FROM " + GPX_METADATA_TABLE_NAME + " WHERE activity IS NULL LIMIT " + BATCH_LIMIT;
				try (Statement selectStmt = dbConn.createStatement();
				     ResultSet rs = selectStmt.executeQuery(selectSql)) {
					if (!rs.isBeforeFirst()) {
						break; // no more records to process
					}
					while (rs.next()) {
						String activity = null;
						hasMoreRecords = true;
						long id = rs.getLong("id");
						GpxFile gpxFile = null;
						GpxTrackAnalysis analysis = null;
						int pointsCount = 0;
						float distanceMeters = 0f;
						float avgSpeedKmh = 0f;
						try (Statement dataStmt = dbConn.createStatement();
						     ResultSet rf = dataStmt.executeQuery(
								     "SELECT data FROM " + GPX_FILES_TABLE_NAME + " WHERE id = " + id
						     )) {
							if (rf.next()) {
								byte[] bytes = rf.getBytes("data");
								if (bytes != null) {
									try (Source src = new Buffer().write(Objects.requireNonNull(Algorithms.gzipToString(bytes)).getBytes())) {
										gpxFile = GpxUtilities.INSTANCE.loadGpxFile(src);
									} catch (IOException e) {
										LOG.error("Error loading GPX file", e);
										activity = ERROR_ACTIVITY_TYPE;
									}
								}
							}
						}

						if (activity == null) {
							if (gpxFile != null && gpxFile.getError() == null) {
								analysis = gpxFile.getAnalysis(System.currentTimeMillis());
								List<WptPt> points = gpxFile.getAllSegmentsPoints();
								int pointsSize = points.size();
								float totalDistance = analysis.getTotalDistance();
								if (pointsSize < MIN_POINTS_SIZE
										|| totalDistance < MIN_DISTANCE
										|| analysis.getMaxDistanceBetweenPoints() >= MAX_DISTANCE_BETWEEN_POINTS) {
									activity = GARBAGE_ACTIVITY_TYPE;
								} else {
									// only for non-garbage & non-error tracks
									pointsCount = pointsSize;
									distanceMeters = totalDistance;
									if (analysis.getHasSpeedInTrack()) {
										double avgSpeedMs = analysis.getAvgSpeed();
										if (avgSpeedMs > 0) {
											avgSpeedKmh = (float) (avgSpeedMs * 3.6d);
										}
									} else if (distanceMeters > 0) {
										double timeMs = analysis.getTimeMoving();
										if (timeMs <= 0) {
											timeMs = analysis.getTimeSpan();
										}
										if (timeMs > 0) {
											double speedMs = distanceMeters / (timeMs / 1000d);
											avgSpeedKmh = (float) (speedMs * 3.6d);
										}
									}
								}
							} else {
								activity = ERROR_ACTIVITY_TYPE;
							}
						}

						if (activity == null) {
							activity = analyzeActivity(rs, activitiesMap);
						}

						if (activity == null) {
							activity = analyzeActivityFromGpx(analysis);
						}

						if (activity == null) {
							activity = GARBAGE_ACTIVITY_TYPE;
						}

						if (!GARBAGE_ACTIVITY_TYPE.equals(activity) && !ERROR_ACTIVITY_TYPE.equals(activity)) {
							identifiedActivityCount++;
						}

						boolean fillMetrics = !GARBAGE_ACTIVITY_TYPE.equals(activity) && !ERROR_ACTIVITY_TYPE.equals(activity);
						if (fillMetrics) {
							updateStmtMetrics.setString(1, activity);
							updateStmtMetrics.setFloat(2, avgSpeedKmh);
							updateStmtMetrics.setFloat(3, distanceMeters);
							updateStmtMetrics.setInt(4, pointsCount);
							updateStmtMetrics.setLong(5, id);
							updateStmtMetrics.addBatch();
						} else {
							updateStmtActivityOnly.setString(1, activity);
							updateStmtActivityOnly.setLong(2, id);
							updateStmtActivityOnly.addBatch();
						}

						batchSize++;
						processedCount++;
						if (update) {
							lastUpdatedId = id;
						}

						if (batchSize >= BATCH_LIMIT) {
							updateStmtMetrics.executeBatch();
							updateStmtActivityOnly.executeBatch();
							dbConn.commit();
							batchSize = 0;
							LOG.info("Processed " + processedCount + " records so far. Identified " + identifiedActivityCount + " activities.");
						}
					}
				}
			}

			if (batchSize > 0) {
				updateStmtMetrics.executeBatch();
				updateStmtActivityOnly.executeBatch();
				dbConn.commit();
			}
		} catch (SQLException e) {
			dbConn.rollback();
			throw e;
		} finally {
			dbConn.setAutoCommit(true);
			updateStmtMetrics.close();
			updateStmtActivityOnly.close();
		}
		LOG.info("Finished populating the 'activity' column. Total records processed: " + processedCount);
	}

	private String analyzeActivity(ResultSet rs, Map<String, List<String>> activitiesMap) throws SQLException {
		if (activitiesMap.isEmpty()) {
			return null;
		}
		String name = rs.getString("name");
		String desc = rs.getString("description");
		Array tagsArray = rs.getArray("tags");
		List<String> tags = new ArrayList<>();

		if (tagsArray != null) {
			try (ResultSet tagRs = tagsArray.getResultSet()) {
				while (tagRs.next()) {
					String tag = tagRs.getString(2);
					if (tag != null) {
						tags.add(tag.toLowerCase());
					}
				}
			}
		}

		Map<String, String> tagMap = new LinkedHashMap<>();

		activitiesMap.forEach((activityId, tagList) ->
				tagList.stream()
						.sorted((tag1, tag2) -> Integer.compare(tag2.length(), tag1.length()))
						.forEach(tag -> tagMap.put(tag, activityId))
		);

		for (Map.Entry<String, String> entry : tagMap.entrySet()) {
			String tag = entry.getKey();
			String activityId = entry.getValue();
			if (tags.contains(tag)) {
				return activityId;
			}
			if (name != null && name.toLowerCase().contains(tag)) {
				return activityId;
			}
			if (desc != null && desc.toLowerCase().contains(tag)) {
				return activityId;
			}
		}
		return null;
	}

	private static Map<String, List<String>> createActivitiesMap(String rootPath) {
		File activityFile = new File(rootPath, "resources/poi/activities.json");
		if (!activityFile.exists()) {
			return Collections.emptyMap();
		}

		ObjectMapper mapper = new ObjectMapper();
		Map<String, List<String>> activitiesMap = new LinkedHashMap<>();

		try {
			JsonNode rootNode = mapper.readTree(activityFile);

			JsonNode groups = rootNode.path("groups");
			for (JsonNode group : groups) {
				String groupId = group.path("id").asText();
				List<String> groupTags = new ArrayList<>();
				JsonNode groupTagsNode = group.path("tags");
				if (groupTagsNode.isArray()) {
					for (JsonNode tag : groupTagsNode) {
						groupTags.add(tag.asText());
					}
				}
				activitiesMap.put(groupId, groupTags);
				JsonNode activities = group.path("activities");
				for (JsonNode activity : activities) {
					String activityId = activity.path("id").asText();
					List<String> activityTags = new ArrayList<>();
					JsonNode tagsNode = activity.path("tags");
					if (tagsNode.isArray()) {
						for (JsonNode tag : tagsNode) {
							activityTags.add(tag.asText());
						}
					}
					activitiesMap.put(activityId, activityTags);
				}
			}
		} catch (IOException e) {
			return Collections.emptyMap();
		}

		return activitiesMap;
	}

	private String analyzeActivityFromGpx(GpxTrackAnalysis analysis) {
		if (analysis != null) {
			if (analysis.getHasSpeedInTrack()) {
				float avgSpeed = analysis.getAvgSpeed() * 3.6f;
				if (avgSpeed > 0 && avgSpeed <= 12) {
					return "foot";
				} else if (avgSpeed <= 25) {
					return "cycling";
				} else if (avgSpeed <= 150) {
					return "driving";
				} else if (avgSpeed > 150) {
					return "aviation";
				} else {
					return "other";
				}
			}
			return NOSPEED_ACTIVITY_TYPE;
		}
		return ERROR_ACTIVITY_TYPE;
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
			OsmGpxFile gpxInfo = new OsmGpxFile(GPX_FILE_PREIX);
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

			Source src = new Buffer().write(Algorithms.gzipToString(cont).getBytes());
			GpxFile gpxFile = GpxUtilities.INSTANCE.loadGpxFile(src);
			GpxTrackAnalysis analysis = gpxFile.getAnalysis(gpxInfo.timestamp.getTime());
			ctx.writeTrack(gpxInfo, gpxFile, analysis);
		}
		ctx.endDocument();
		
		System.out.println(String.format("Fetched %d tracks %d segments", ctx.tracks, ctx.segments));
		generateObfFile(qp);
	}

	private void generateObfFile(QueryParams qp)
			throws IOException, SQLException, InterruptedException, XmlPullParserException {
		if (qp.obfFile != null) {
			IndexCreatorSettings settings = new IndexCreatorSettings();
			settings.indexMap = true;
			settings.indexAddress = false;
			settings.indexPOI = true;
			settings.indexTransport = false;
			settings.indexRouting = false;
			// reduce memory footprint for single thread generation
			// Remove it if it is called in multithread
//			RTree.clearCache();
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
			OsmGpxFile r = new OsmGpxFile(GPX_FILE_PREIX);
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
			OsmGpxFile r = new OsmGpxFile(GPX_FILE_PREIX);
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
				GpxFile res = calculateMinMaxLatLon(r);
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

	private GpxFile calculateMinMaxLatLon(OsmGpxFile r) {
		GpxFile gpxFile = GpxUtilities.INSTANCE.loadGpxFile(new Buffer().write(r.gpx.getBytes()));
		if (gpxFile.getError() == null) {
			KQuadRect rect = gpxFile.getBounds(r.lat, r.lon);
			r.minlon = rect.getLeft();
			r.minlat = rect.getBottom();
			r.maxlon = rect.getRight();
			r.maxlat = rect.getTop();
			return gpxFile;
		} else {
			errorReadingGpx(r, gpxFile.getError());
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

			con = (HttpsURLConnection) new URL(url).openConnection();

			con.setConnectTimeout(HTTP_TIMEOUT);
			String accessToken = setupAccessToken();
			con.setRequestProperty("Authorization", "Bearer" + " " + accessToken); // oauth2

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
					p = new OsmGpxFile(GPX_FILE_PREIX);
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

	private static String setupAccessToken() {
		String accessToken = System.getenv(ENV_OAUTH2_ACCESS_TOKEN); // unset this ENV if the token is dead
		if (accessToken != null) {
			// System.out.println("Using " + ENV_OAUTH2_ACCESS_TOKEN + " for API requests");
			return accessToken; // success
		}

		final String APPLICATIONS_URL = "https://www.openstreetmap.org/oauth2/applications"; // manual
		final String AUTHORIZE_URL = "https://www.openstreetmap.org/oauth2/authorize"; // manual
		final String ACCESS_TOKEN_URL = "https://www.openstreetmap.org/oauth2/token"; // auto
		final String REDIRECT_INTERNAL = "urn:ietf:wg:oauth:2.0:oob";
		final String SCOPE = "read_gpx";

		final String authCode = System.getenv(ENV_OAUTH2_AUTH_CODE);
		final String clientId = System.getenv(ENV_OAUTH2_CLIENT_ID);
		final String clientSecret = System.getenv(ENV_OAUTH2_CLIENT_SECRET);

		final String errorClientIdSecret = String.format("\n\n" +
						"Setup step 1/3...\n" +
						"Missing ENV vars: %s / %s\n" +
						"Log in to %s and register App.\n" +
						"Use Name (any) Redirect (%s) and Permissions (%s)\n" +
						"Set ENV variables with the corresponding Client ID and Client Secret and run again.\n",
				ENV_OAUTH2_CLIENT_ID, ENV_OAUTH2_CLIENT_SECRET, APPLICATIONS_URL, REDIRECT_INTERNAL, SCOPE);

		if (clientId == null || clientSecret == null) {
			System.out.println(errorClientIdSecret);
			System.exit(1);
		}

		final String errorAuthCode = String.format("\n\n" +
						"Setup step 2/3...\n" +
						"Incorrect ENV var: %s\n" +
						"Please follow URL and press Authorize Access:\n" +
						"%s?response_type=code&client_id=%s&redirect_uri=%s&scope=%s\n" +
						"Set ENV variable with the corresponding Authorization code and run again.\n",
				ENV_OAUTH2_AUTH_CODE, AUTHORIZE_URL, clientId, URLEncoder.encode(REDIRECT_INTERNAL), SCOPE);

		if (authCode == null) {
			System.out.println(errorAuthCode);
			System.exit(1);
		}

		List<NameValuePair> params = new ArrayList<>();
		params.add(new BasicNameValuePair("grant_type", "authorization_code"));
		params.add(new BasicNameValuePair("redirect_uri", REDIRECT_INTERNAL));
		params.add(new BasicNameValuePair("client_secret", clientSecret));
		params.add(new BasicNameValuePair("client_id", clientId));
		params.add(new BasicNameValuePair("code", authCode));

		try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
			HttpPost post = new HttpPost(ACCESS_TOKEN_URL);
			post.setEntity(new UrlEncodedFormEntity(params));
			post.addHeader("Content-Type", "application/x-www-form-urlencoded");
			try (CloseableHttpResponse response = httpClient.execute(post)) {
				String jsonResponse = EntityUtils.toString(response.getEntity());
				ObjectMapper mapper = new ObjectMapper();
				JsonNode rootNode = mapper.readTree(jsonResponse);
				String tokenToSave = rootNode.path("access_token").asText();
				if (tokenToSave == null || tokenToSave.isEmpty()) {
					String error = rootNode.path("error").asText();
					String errorDescription = rootNode.path("error_description").asText();
					final String errorAccessToken = String.format("\n\n" +
									"ACCESS_TOKEN_REQUEST failed (%s)\n" +
									"Details: %s\n" +
									"Try again: %s",
							error, errorDescription, errorAuthCode);
					System.out.println(errorAccessToken);
				} else {
					final String successAccessToken = String.format("\n\n" +
									"Setup finished 3/3...\n" +
									"You have got the permanent accessToken!\n" +
									"Save it as ENV %s and run again.\n" +
									"%s\n\n",
							ENV_OAUTH2_ACCESS_TOKEN, tokenToSave);
					System.out.println(successAccessToken);
				}
				System.exit(1);
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		throw new RuntimeException(); // never reached
	}
}
