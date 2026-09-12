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
import java.sql.Types;
import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
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
import net.osmand.obf.ToolsOsmAndContextImpl;
import net.osmand.shared.data.KQuadRect;
import net.osmand.shared.gpx.RouteActivityHelper;
import net.osmand.shared.gpx.primitives.Route;
import net.osmand.shared.gpx.primitives.RouteActivity;
import net.osmand.shared.gpx.primitives.Track;
import net.osmand.shared.gpx.primitives.TrkSegment;
import net.osmand.shared.gpx.primitives.WptPt;
import net.osmand.util.MapUtils;
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

	private static final String ERROR_ACTIVITY_TYPE = "error";
	private static final String NOSPEED_ACTIVITY_TYPE = "nospeed";
	private static final String AVIATION_ACTIVITY_TYPE = "aviation";
	private static final String FOOT_GROUP = "foot";
	private static final String CYCLING_GROUP = "cycling";
	private static final String WINTER_SPORT_GROUP = "winter_sport";
	private static final String DRIVING_GROUP = "driving";
	private static final String MOTORCYCLING_GROUP = "motorcycling";
	private static final String OTHER_GROUP = "other";

	private static final Map<String, String> ACTIVITY_GROUPS = new LinkedHashMap<>();
	private static final Map<String, Double> GROUP_AVG_LIMIT_KMH = Map.of(
			FOOT_GROUP, 12d,
			CYCLING_GROUP, 25d,
			WINTER_SPORT_GROUP, 45d,
			DRIVING_GROUP, 130d,
			MOTORCYCLING_GROUP, 130d,
			AVIATION_ACTIVITY_TYPE, 1000d);
	private static final Map<String, Double> GROUP_MAX_LIMIT_KMH = Map.of(
			FOOT_GROUP, 25d,
			CYCLING_GROUP, 45d,
			WINTER_SPORT_GROUP, 130d,
			DRIVING_GROUP, 250d,
			MOTORCYCLING_GROUP, 300d,
			AVIATION_ACTIVITY_TYPE, 1200d);
	private static final List<String> ACTIVITY_BY_SPEED = List.of(
			FOOT_GROUP, CYCLING_GROUP, DRIVING_GROUP, AVIATION_ACTIVITY_TYPE);

	// Garmin exports named "COURSE_<id>.gpx" would otherwise match "road_running"
	private static final Set<String> ACTIVITY_KEYWORD_EXCLUSIONS = Set.of("course");

	private static final long MIN_SPEED_INTERVAL_MS = 500; // min elapsed time to trust a speed sample
	private static final double MIN_MOVING_SPEED_MPS = 0.1; // below this the interval counts as standing still
	private static final int SRID_WGS84 = 4326;
	private static final int TRACK_STATS_VERSION = 1;
	private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
	private static final int PARSE_BATCH_LIMIT = 1000;
	private static final int CLASSIFY_BATCH_LIMIT = 10000;
	private static final long TRACK_TIMEOUT_MS = 120_000;
	// lower edges (km/h) of the speed_hist_time / speed_hist_dist bins in track_stats
	private static final double[] SPEED_BINS_KMH = {1, 3, 5, 7, 9, 12, 16, 20, 25, 32, 45, 60, 90, 130, 180, 300};
	private static final long SPEED_WINDOW_MIN_MS = 10_000; // windows of >= 10 s and >= 30 m smooth out GPS jitter
	private static final double SPEED_WINDOW_MIN_M = 30;
	private static final long SPEED_WINDOW_STANDING_MS = 120_000; // a window this long counts even if it moved less
	private static final long SPEED_WINDOW_MAX_MS = 600_000; // longer gaps are pauses, not movement
	private static final double MOVING_MIN_KMH = 1;

	private static final String GPX_FILE_PREIX = "OG";
	private final RouteActivityHelper routeActivityHelper = RouteActivityHelper.INSTANCE;

	private static float round2(float value) {
		return Math.round(value * 100) / 100.0f;
	}

	private boolean sslInit;
	private Connection dbConn;
	private PreparedStatementWrapper[] preparedStatements = new PreparedStatementWrapper[PS_INSERT_GPX_DETAILS + 1];

	public DownloadOsmGPX() throws SQLException {
		net.osmand.shared.util.PlatformUtil.INSTANCE.initialize(new ToolsOsmAndContextImpl());
		java.util.logging.Logger.getLogger("GpxUtilities").setLevel(java.util.logging.Level.OFF);
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
			if ("query".equals(main)) {
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
			// kept for existing jobs: parses tracks without activity
			utility.addActivityColumnAndPopulate(args[1]);
		} else if ("update_activity".equals(main)) {
			// kept for existing jobs: update_activity <rootPath> foot,cycling,garbage parses those tracks again
			String categories = args.length > 2 ? args[2] : null;
			utility.updateActivity(args[1], categories);
		} else if ("parse_tracks".equals(main)) {
			// parse_tracks <rootPath> [--threads=N] [--categories=foot,garbage] [--where=<SQL condition>] [--force]
			// parses tracks without track_stats or with an older TRACK_STATS_VERSION (every track with --force),
			// so a stopped run continues where it stopped
			JobOptions options = JobOptions.parse(args, 2);
			utility.parseTracks(args[1], options.force ? TrackSelection.ALL : TrackSelection.OUTDATED_STATS, options);
		} else if ("classify_tracks".equals(main)) {
			// classify_tracks <rootPath> [--categories=foot,garbage] [--where=<SQL condition>]
			// sets activity again from stored columns and track_stats, without reading GPX
			utility.classifyTracks(args[1], JobOptions.parse(args, 2));
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
			statement.executeUpdate("ALTER TABLE " + GPX_METADATA_TABLE_NAME + " ADD COLUMN IF NOT EXISTS activity text");

			statement.executeUpdate("ALTER TABLE " + GPX_METADATA_TABLE_NAME + " ADD COLUMN IF NOT EXISTS speed float");
			statement.executeUpdate("ALTER TABLE " + GPX_METADATA_TABLE_NAME + " ADD COLUMN IF NOT EXISTS distance float");
			statement.executeUpdate("ALTER TABLE " + GPX_METADATA_TABLE_NAME + " ADD COLUMN IF NOT EXISTS points integer");
			statement.executeUpdate("ALTER TABLE " + GPX_METADATA_TABLE_NAME + " ADD COLUMN IF NOT EXISTS max_speed float");
			statement.executeUpdate("ALTER TABLE " + GPX_METADATA_TABLE_NAME + " ADD COLUMN IF NOT EXISTS max_dist_between_points float");
			statement.executeUpdate("ALTER TABLE " + GPX_METADATA_TABLE_NAME + " ADD COLUMN IF NOT EXISTS time_minutes integer");
			statement.executeUpdate("ALTER TABLE " + GPX_METADATA_TABLE_NAME + " ADD COLUMN IF NOT EXISTS waypoints integer");
			statement.executeUpdate("ALTER TABLE " + GPX_METADATA_TABLE_NAME + " ADD COLUMN IF NOT EXISTS simplified_geometry bytea");
			statement.executeUpdate("ALTER TABLE " + GPX_METADATA_TABLE_NAME + " ADD COLUMN IF NOT EXISTS speed_matches_activity boolean");
			// activity written in the file itself (osmand:activity), kept apart from the computed activity
			statement.executeUpdate("ALTER TABLE " + GPX_METADATA_TABLE_NAME + " ADD COLUMN IF NOT EXISTS file_activity text");
			// rule that set activity: file, keyword, speed, garbage, error
			statement.executeUpdate("ALTER TABLE " + GPX_METADATA_TABLE_NAME + " ADD COLUMN IF NOT EXISTS activity_source text");
			// facts from one GPX parse, so classification rules can change without parsing again (see computeTrackStats)
			statement.executeUpdate("ALTER TABLE " + GPX_METADATA_TABLE_NAME + " ADD COLUMN IF NOT EXISTS track_stats json");

			statement.executeUpdate("CREATE INDEX IF NOT EXISTS idx_osm_gpx_speed ON " + GPX_METADATA_TABLE_NAME + " (speed)");
			statement.executeUpdate("CREATE INDEX IF NOT EXISTS idx_osm_gpx_distance ON " + GPX_METADATA_TABLE_NAME + " (distance)");
			statement.executeUpdate("CREATE INDEX IF NOT EXISTS idx_osm_gpx_points ON " + GPX_METADATA_TABLE_NAME + " (points)");
			statement.executeUpdate("CREATE INDEX IF NOT EXISTS idx_osm_gpx_max_speed ON " + GPX_METADATA_TABLE_NAME + " (max_speed)");
			statement.executeUpdate("CREATE INDEX IF NOT EXISTS idx_osm_gpx_max_dist_between_points ON " + GPX_METADATA_TABLE_NAME + " (max_dist_between_points)");
			statement.executeUpdate("CREATE INDEX IF NOT EXISTS idx_osm_gpx_time_minutes ON " + GPX_METADATA_TABLE_NAME + " (time_minutes)");
			statement.executeUpdate("CREATE INDEX IF NOT EXISTS idx_osm_gpx_waypoints ON " + GPX_METADATA_TABLE_NAME + " (waypoints)");

			statement.executeUpdate("CREATE INDEX IF NOT EXISTS idx_osm_gpx_activity ON " + GPX_METADATA_TABLE_NAME + " (activity)");
			statement.executeUpdate("CREATE INDEX IF NOT EXISTS idx_osm_gpx_tags_gin ON " + GPX_METADATA_TABLE_NAME + " USING GIN (tags)");

			statement.executeUpdate("CREATE EXTENSION IF NOT EXISTS postgis");
			statement.executeUpdate("ALTER TABLE " + GPX_METADATA_TABLE_NAME + " ADD COLUMN IF NOT EXISTS bbox geometry"
					+ " GENERATED ALWAYS AS (ST_MakeEnvelope(minlon, minlat, maxlon, maxlat, " + SRID_WGS84 + ")) STORED");
			statement.executeUpdate("CREATE INDEX IF NOT EXISTS idx_osm_gpx_bbox_geom ON " + GPX_METADATA_TABLE_NAME
					+ " USING GIST (bbox)");
			statement.executeUpdate("CREATE INDEX IF NOT EXISTS idx_osm_gpx_year ON " + GPX_METADATA_TABLE_NAME + " ((extract(year from date)))");
			statement.executeUpdate("CREATE INDEX IF NOT EXISTS idx_osm_gpx_ranges_optimized ON " + GPX_METADATA_TABLE_NAME
					+ " (minlat, maxlat, minlon, maxlon, activity) INCLUDE (distance, speed)"
					+ " WHERE (distance > 0::double precision OR speed > 0::double precision)"
					+ " AND activity NOT LIKE 'garbage%' AND activity <> 'error'::text");
			statement.executeUpdate("CREATE INDEX IF NOT EXISTS idx_osm_gpx_ranges_composite ON " + GPX_METADATA_TABLE_NAME
					+ " (activity, minlat, minlon, maxlat, maxlon) INCLUDE (distance, speed)"
					+ " WHERE (distance > 0::double precision OR speed > 0::double precision)"
					+ " AND activity NOT LIKE 'garbage%' AND activity <> 'error'::text");
		}
		LOG.info("Activity schema (columns and indexes) ensured.");
	}

	protected void addActivityColumnAndPopulate(String rootPath) throws SQLException, InterruptedException {
		parseTracks(rootPath, TrackSelection.WITHOUT_ACTIVITY, new JobOptions());
	}

	protected void updateActivity(String rootPath, String categories) throws SQLException, InterruptedException {
		JobOptions options = new JobOptions();
		options.categories = categories;
		parseTracks(rootPath, TrackSelection.ALL, options);
	}

	private enum TrackSelection {
		WITHOUT_ACTIVITY, // add_activity: tracks not processed yet
		OUTDATED_STATS, // parse_tracks: no track_stats or an older TRACK_STATS_VERSION
		ALL // update_activity, parse_tracks --force
	}

	// options of parse_tracks and classify_tracks
	static class JobOptions {
		int threads = 1;
		String categories;
		String where; // e.g. "id % 1000 = 0" for a sample, "(track_stats->>'speed_p95')::float > 40" for a subgroup
		boolean force;

		static JobOptions parse(String[] args, int from) {
			JobOptions options = new JobOptions();
			for (int i = from; i < args.length; i++) {
				String arg = args[i];
				String value = arg.substring(arg.indexOf('=') + 1).trim();
				if (arg.startsWith("--threads=")) {
					options.threads = Math.max(1, Integer.parseInt(value));
				} else if (arg.startsWith("--categories=")) {
					options.categories = value;
				} else if (arg.startsWith("--where=")) {
					options.where = value;
				} else if (arg.equals("--force")) {
					options.force = true;
				} else {
					throw new IllegalArgumentException("Unknown option: " + arg);
				}
			}
			return options;
		}
	}

	private Set<String> expandCategories(String categories, Map<String, List<String>> activitiesMap) {
		Set<String> groupIds = new HashSet<>(ACTIVITY_GROUPS.values());
		Set<String> result = new LinkedHashSet<>();
		for (String raw : categories.split(",")) {
			String token = raw.trim().toLowerCase();
			if (token.isEmpty()) {
				continue;
			}
			if (GarbageClassifier.GARBAGE.equals(token)) {
				result.addAll(GarbageClassifier.TYPES);
			} else if (groupIds.contains(token)) {
				result.add(token);
				ACTIVITY_GROUPS.forEach((activityId, groupId) -> {
					if (groupId.equals(token)) {
						result.add(activityId);
					}
				});
			} else if (activitiesMap.containsKey(token) || GarbageClassifier.TYPES.contains(token)
					|| ERROR_ACTIVITY_TYPE.equals(token) || NOSPEED_ACTIVITY_TYPE.equals(token)) {
				result.add(token);
			} else {
				LOG.info("Unknown category '" + token + "' ignored.");
			}
		}
		return result;
	}

	// Comma-separated, single-quoted values for a SQL IN (...) clause.
	private static String sqlList(Set<String> values) {
		StringBuilder sb = new StringBuilder();
		for (String v : values) {
			if (!sb.isEmpty()) {
				sb.append(',');
			}
			sb.append('\'').append(v.replace("'", "''")).append('\'');
		}
		return sb.toString();
	}

	// SQL conditions for --categories and --where, null when none of the categories is known
	private String trackCondition(Map<String, List<String>> activitiesMap, JobOptions options) {
		StringBuilder condition = new StringBuilder();
		if (!isEmpty(options.categories)) {
			Set<String> categoryFilter = expandCategories(options.categories, activitiesMap);
			if (categoryFilter.isEmpty()) {
				LOG.info("No known categories matched '" + options.categories + "'. Nothing to do.");
				return null;
			}
			condition.append(" AND activity IN (").append(sqlList(categoryFilter)).append(")");
		}
		if (!isEmpty(options.where)) {
			condition.append(" AND (").append(options.where).append(")");
		}
		return condition.toString();
	}

	protected void parseTracks(String rootPath, TrackSelection selection, JobOptions options)
			throws SQLException, InterruptedException {
		ensureActivitySchema();
		Map<String, List<String>> activitiesMap = createActivitiesMap(rootPath);
		if (activitiesMap.isEmpty()) {
			LOG.info("Activities map is empty. Skipping.");
			return;
		}
		String condition = trackCondition(activitiesMap, options);
		if (condition == null) {
			return;
		}
		if (selection == TrackSelection.WITHOUT_ACTIVITY) {
			condition += " AND activity IS NULL";
		} else if (selection == TrackSelection.OUTDATED_STATS) {
			condition += " AND (track_stats IS NULL OR COALESCE((track_stats->>'v')::int, 0) < " + TRACK_STATS_VERSION + ")";
		}
		LOG.info("Parsing tracks: " + selection + condition + ", " + options.threads + " threads...");
		String selectSql = "SELECT id, name, description, tags FROM " + GPX_METADATA_TABLE_NAME
				+ " WHERE id > ?" + condition + " ORDER BY id LIMIT " + PARSE_BATCH_LIMIT;
		// only GPX parsing runs in the pool; reads, classification and writes stay on this thread and dbConn
		ExecutorService pool = Executors.newCachedThreadPool(r -> {
			Thread t = new Thread(r);
			t.setDaemon(true); // a track stuck in the parser must not keep the job alive
			return t;
		});
		CompletionService<TrackData> completed = new ExecutorCompletionService<>(pool);
		Map<Future<TrackData>, TrackRow> running = new HashMap<>();
		Deque<TrackRow> queued = new ArrayDeque<>();
		long lastId = -1; // so id=0 is included when present
		boolean moreRows = true;
		dbConn.setAutoCommit(false);
		try (PreparedStatement selectRows = dbConn.prepareStatement(selectSql);
			 PreparedStatement selectData = dbConn.prepareStatement("SELECT data FROM " + GPX_FILES_TABLE_NAME + " WHERE id = ?");
			 ParseBatch batch = new ParseBatch(activitiesMap)) {
			while (moreRows || !queued.isEmpty() || !running.isEmpty()) {
				if (moreRows && queued.isEmpty()) {
					selectRows.setLong(1, lastId);
					try (ResultSet rs = selectRows.executeQuery()) {
						while (rs.next()) {
							TrackRow row = new TrackRow(rs);
							queued.add(row);
							lastId = row.id;
						}
					}
					moreRows = !queued.isEmpty();
				}
				while (running.size() < options.threads && !queued.isEmpty()) {
					TrackRow row = queued.poll();
					byte[] data = loadGpxData(selectData, row.id);
					if (data == null) {
						batch.writeError(row, "no_data");
					} else {
						row.startMs = System.currentTimeMillis();
						running.put(completed.submit(() -> computeTrackData(data)), row);
					}
				}
				Future<TrackData> done = running.isEmpty() ? null : completed.poll(1, TimeUnit.SECONDS);
				while (done != null) {
					TrackRow row = running.remove(done);
					if (row != null) { // null for a track that finished after its timeout
						batch.writeTrack(row, done);
					}
					done = completed.poll();
				}
				long now = System.currentTimeMillis();
				Iterator<Map.Entry<Future<TrackData>, TrackRow>> it = running.entrySet().iterator();
				while (it.hasNext()) {
					Map.Entry<Future<TrackData>, TrackRow> task = it.next();
					if (now - task.getValue().startMs > TRACK_TIMEOUT_MS) {
						task.getKey().cancel(true);
						it.remove();
						LOG.error("Timeout (>" + TRACK_TIMEOUT_MS / 1000 + "s) processing id=" + task.getValue().id
								+ ", marking as error");
						batch.writeError(task.getValue(), "timeout");
					}
				}
			}
			batch.commit();
			LOG.info("Finished parsing tracks. Total records processed: " + batch.processed);
		} catch (SQLException e) {
			dbConn.rollback();
			throw e;
		} finally {
			pool.shutdownNow();
			dbConn.setAutoCommit(true);
		}
	}

	protected void classifyTracks(String rootPath, JobOptions options) throws SQLException {
		ensureActivitySchema();
		Map<String, List<String>> activitiesMap = createActivitiesMap(rootPath);
		if (activitiesMap.isEmpty()) {
			LOG.info("Activities map is empty. Skipping.");
			return;
		}
		String condition = trackCondition(activitiesMap, options);
		if (condition == null) {
			return;
		}
		LOG.info("Classifying tracks from stored columns" + condition + "...");
		String selectSql = "SELECT id, name, description, tags, activity, activity_source, speed_matches_activity, "
				+ "file_activity, points, distance, max_dist_between_points, speed, max_speed, track_stats FROM "
				+ GPX_METADATA_TABLE_NAME + " WHERE id > ? AND track_stats IS NOT NULL" + condition
				+ " ORDER BY id LIMIT " + CLASSIFY_BATCH_LIMIT;
		int read = 0;
		int changed = 0;
		int skipped = 0;
		long lastId = -1;
		dbConn.setAutoCommit(false);
		try (PreparedStatement selectRows = dbConn.prepareStatement(selectSql);
			 PreparedStatement update = dbConn.prepareStatement("UPDATE " + GPX_METADATA_TABLE_NAME
					 + " SET activity = ?, activity_source = ?, speed_matches_activity = ? WHERE id = ?")) {
			boolean moreRows = true;
			while (moreRows) {
				moreRows = false;
				selectRows.setLong(1, lastId);
				try (ResultSet rs = selectRows.executeQuery()) {
					while (rs.next()) {
						moreRows = true;
						read++;
						TrackRow row = new TrackRow(rs);
						lastId = row.id;
						TrackFacts facts = storedFacts(row, rs);
						if (facts == null) {
							skipped++; // error rows, and stats written before the keys classification needs
							continue;
						}
						Classification c = classify(facts, activitiesMap);
						if (!Objects.equals(c.activity(), rs.getString("activity"))
								|| !Objects.equals(c.source(), rs.getString("activity_source"))
								|| !Objects.equals(c.speedMatches(), rs.getObject("speed_matches_activity"))) {
							update.setString(1, c.activity());
							update.setString(2, c.source());
							update.setObject(3, c.speedMatches(), Types.BOOLEAN);
							update.setLong(4, row.id);
							update.addBatch();
							changed++;
						}
					}
				}
				update.executeBatch();
				dbConn.commit();
				LOG.info("Classified " + read + " tracks, changed " + changed + ", skipped " + skipped);
			}
		} catch (SQLException e) {
			dbConn.rollback();
			throw e;
		} finally {
			dbConn.setAutoCommit(true);
		}
	}

	private static byte[] loadGpxData(PreparedStatement selectData, long id) throws SQLException {
		selectData.setLong(1, id);
		try (ResultSet rs = selectData.executeQuery()) {
			return rs.next() ? rs.getBytes(1) : null;
		}
	}

	// metadata of one track, read on the DB thread
	private static class TrackRow {
		final long id;
		final String name;
		final String description;
		final List<String> tags = new ArrayList<>();
		long startMs;

		TrackRow(ResultSet rs) throws SQLException {
			id = rs.getLong("id");
			name = rs.getString("name");
			description = rs.getString("description");
			Array tagsArray = rs.getArray("tags");
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
		}
	}

	// what classification reads, from a fresh parse (parse_tracks) or from stored columns (classify_tracks)
	private static class TrackFacts {
		TrackRow row;
		String fileActivity;
		int points;
		double distance;
		double maxDistBetweenPoints;
		float avgSpeedKmh;
		float maxSpeedKmh;
		boolean hasSpeed;
		boolean teleport;
	}

	record Classification(String activity, String source, Boolean speedMatches) {
	}

	private Classification classify(TrackFacts facts, Map<String, List<String>> activitiesMap) {
		String activity = GarbageClassifier.classify(facts.points, facts.distance, facts.maxDistBetweenPoints, facts.teleport);
		String source = activity != null ? "garbage" : null;
		// each step runs only when the previous ones found nothing, so the last source set is the one used
		if (activity == null) {
			activity = getActivityByFileActivity(facts.fileActivity, activitiesMap);
			source = "file";
		}
		if (activity == null) {
			activity = analyzeActivity(facts.row.name, facts.row.description, facts.row.tags, activitiesMap);
			source = "keyword";
		}
		Boolean speedMatches = speedMatchesActivity(activity, facts.avgSpeedKmh, facts.maxSpeedKmh);
		if (activity == null) {
			activity = analyzeActivityBySpeed(facts.hasSpeed, facts.avgSpeedKmh, facts.maxSpeedKmh);
			source = "speed";
		}
		return new Classification(activity, source, speedMatches);
	}

	// facts from stored columns, null when track_stats lacks what classification needs
	private static TrackFacts storedFacts(TrackRow row, ResultSet rs) throws SQLException {
		JsonNode stats;
		try {
			stats = JSON_MAPPER.readTree(rs.getString("track_stats"));
		} catch (IOException e) {
			return null;
		}
		if (!stats.has("has_speed")) {
			return null;
		}
		TrackFacts facts = new TrackFacts();
		facts.row = row;
		facts.fileActivity = rs.getString("file_activity");
		facts.points = rs.getInt("points");
		facts.distance = rs.getDouble("distance");
		facts.maxDistBetweenPoints = rs.getDouble("max_dist_between_points");
		facts.avgSpeedKmh = rs.getFloat("speed");
		facts.maxSpeedKmh = rs.getFloat("max_speed");
		facts.hasSpeed = stats.get("has_speed").asBoolean();
		facts.teleport = stats.path("teleport").asBoolean();
		return facts;
	}

	// writes results of parse_tracks in batches; used only on the thread that owns dbConn
	private class ParseBatch implements AutoCloseable {
		private final Map<String, List<String>> activitiesMap;
		private final PreparedStatement metricsStmt;
		private final PreparedStatement errorStmt;
		private final long startMs = System.currentTimeMillis();
		private int pending;
		private int processed;
		private int identified;

		ParseBatch(Map<String, List<String>> activitiesMap) throws SQLException {
			this.activitiesMap = activitiesMap;
			metricsStmt = dbConn.prepareStatement(
					"UPDATE " + GPX_METADATA_TABLE_NAME + " SET activity = ?, speed = ?, distance = ?, points = ?, " +
							"max_speed = ?, max_dist_between_points = ?, time_minutes = ?, waypoints = ?, " +
							"simplified_geometry = ?, speed_matches_activity = ?, file_activity = ?, track_stats = ?::json, " +
							"activity_source = ? WHERE id = ?");
			errorStmt = dbConn.prepareStatement(
					"UPDATE " + GPX_METADATA_TABLE_NAME + " SET activity = ?, activity_source = ?, track_stats = ?::json WHERE id = ?");
		}

		void writeTrack(TrackRow row, Future<TrackData> future) throws SQLException, InterruptedException {
			TrackData d;
			try {
				d = future.get();
			} catch (ExecutionException e) {
				LOG.error("Error processing id=" + row.id, e.getCause());
				writeError(row, e.getCause().getClass().getSimpleName());
				return;
			}
			if (d.error) {
				writeError(row, d.errorReason);
				return;
			}
			TrackFacts facts = new TrackFacts();
			facts.row = row;
			facts.fileActivity = d.fileActivity;
			facts.points = d.pointsCount;
			facts.distance = d.distanceMeters;
			facts.maxDistBetweenPoints = d.maxDistBetweenPoints;
			facts.avgSpeedKmh = d.avgSpeedKmh;
			facts.maxSpeedKmh = d.maxSpeedKmh;
			facts.hasSpeed = d.hasSpeed;
			facts.teleport = d.teleport;
			Classification c = classify(facts, activitiesMap);
			metricsStmt.setString(1, c.activity());
			metricsStmt.setFloat(2, round2(d.avgSpeedKmh));
			metricsStmt.setFloat(3, round2(d.distanceMeters));
			metricsStmt.setInt(4, d.pointsCount);
			metricsStmt.setFloat(5, round2(d.maxSpeedKmh));
			metricsStmt.setFloat(6, round2(d.maxDistBetweenPoints));
			metricsStmt.setInt(7, d.timeMinutes);
			metricsStmt.setInt(8, d.waypointsCount);
			metricsStmt.setBytes(9, d.simplifiedGeometry);
			metricsStmt.setObject(10, c.speedMatches(), Types.BOOLEAN);
			metricsStmt.setString(11, d.fileActivity);
			metricsStmt.setString(12, d.trackStats);
			metricsStmt.setString(13, c.source());
			metricsStmt.setLong(14, row.id);
			metricsStmt.addBatch();
			if (!GarbageClassifier.isGarbage(c.activity())) {
				identified++;
			}
			added();
		}

		void writeError(TrackRow row, String reason) throws SQLException {
			errorStmt.setString(1, ERROR_ACTIVITY_TYPE);
			errorStmt.setString(2, "error");
			errorStmt.setString(3, errorTrackStats(reason));
			errorStmt.setLong(4, row.id);
			errorStmt.addBatch();
			added();
		}

		private void added() throws SQLException {
			processed++;
			if (++pending >= PARSE_BATCH_LIMIT) {
				commit();
			}
		}

		void commit() throws SQLException {
			metricsStmt.executeBatch();
			errorStmt.executeBatch();
			dbConn.commit();
			pending = 0;
			double seconds = Math.max(1, System.currentTimeMillis() - startMs) / 1000d;
			LOG.info(String.format("Processed %d records so far (%.1f per second). Identified %d activities.",
					processed, processed / seconds, identified));
		}

		@Override
		public void close() throws SQLException {
			metricsStmt.close();
			errorStmt.close();
		}
	}

	// Parse GPX + analysis + build simplified geometry. Runs in a worker thread guarded by a timeout,
	// touches no DB state so it is safe off the main thread.
	private TrackData computeTrackData(byte[] bytes) {
		TrackData d = new TrackData();
		GpxFile gpxFile;
		try (Source src = new Buffer().write(Objects.requireNonNull(Algorithms.gzipToString(bytes)).getBytes())) {
			gpxFile = GpxUtilities.INSTANCE.loadGpxFile(src);
		} catch (IOException e) {
			LOG.error("Error loading GPX file", e);
			d.error = true;
			d.errorReason = e.getClass().getSimpleName();
			return d;
		}
		d.gpxFile = gpxFile;
		if (gpxFile.getError() != null) {
			d.error = true;
			Throwable error = gpxFile.getError();
			d.errorReason = error.getMessage() != null ? error.getMessage() : error.getClass().getSimpleName();
			return d;
		}
		GpxTrackAnalysis analysis = gpxFile.getAnalysis(System.currentTimeMillis());
		d.analysis = analysis;
		d.pointsCount = gpxFile.getAllSegmentsPoints().size();
		d.distanceMeters = analysis.getTotalDistance();
		SpeedMetrics speed = computeSpeedMetrics(gpxFile);
		d.avgSpeedKmh = speed.avgKmh();
		d.maxSpeedKmh = speed.maxKmh();
		d.maxDistBetweenPoints = analysis.getMaxDistanceBetweenPoints();
		double timeSpanMs = analysis.getTimeSpan();
		if (timeSpanMs > 0) {
			d.timeMinutes = (int) (timeSpanMs / 60000d);
		}
		d.waypointsCount = gpxFile.getPointsList().size();
		d.simplifiedGeometry = TrackSimplifyEncoder.encodeGeometry(
				TrackSimplifyEncoder.simplifyGpx(gpxFile, TrackSimplifyEncoder.SIMPLIFY_ZOOM));

		d.hasSpeed = analysis.getHasSpeedInTrack();
		d.teleport = GarbageClassifier.hasTeleportGap(gpxFile);
		d.fileActivity = gpxFile.getMetadata().getExtensionsToRead().get(GpxUtilities.ACTIVITY_TYPE);
		d.trackStats = computeTrackStats(gpxFile, analysis, d.teleport);
		return d;
	}

	// JSON for the track_stats column. Add keys and bump TRACK_STATS_VERSION instead of adding table columns.
	static String computeTrackStats(GpxFile gpxFile, GpxTrackAnalysis analysis, boolean teleport) {
		int points = 0;
		for (Track track : gpxFile.getTracks(false)) {
			for (TrkSegment seg : track.getSegments()) {
				points += seg.getPoints().size();
			}
		}
		int segments = 0;
		int timed = 0;
		int intervals = 0;
		long[] intervalsMs = new long[Math.max(points, 1)];
		long startTime = Long.MAX_VALUE;
		long endTime = 0;
		for (Track track : gpxFile.getTracks(false)) {
			for (TrkSegment seg : track.getSegments()) {
				if (seg.getPoints().isEmpty()) {
					continue;
				}
				segments++;
				long prevTime = 0;
				for (WptPt p : seg.getPoints()) {
					long t = p.getTime();
					if (t <= 0) {
						continue;
					}
					timed++;
					startTime = Math.min(startTime, t);
					endTime = Math.max(endTime, t);
					if (prevTime > 0 && t > prevTime) {
						intervalsMs[intervals++] = t - prevTime;
					}
					prevTime = t;
				}
			}
		}
		int routePoints = 0;
		for (Route route : gpxFile.getRoutes()) {
			routePoints += route.getPoints().size();
		}
		Map<String, Object> stats = new LinkedHashMap<>();
		stats.put("v", TRACK_STATS_VERSION);
		if (!Algorithms.isEmpty(gpxFile.getAuthor())) {
			stats.put("creator", gpxFile.getAuthor()); // app that wrote the file: openpilot car logs, GPSies plans
		}
		stats.put("segments", segments);
		stats.put("route_points", routePoints); // planned route instead of a recording
		stats.put("time_frac", points > 0 ? Math.round(timed * 1000d / points) / 1000d : 0);
		if (timed > 0) {
			stats.put("start_time", startTime / 1000); // recording time, the date column is the upload date
			stats.put("end_time", endTime / 1000);
		}
		if (intervals > 0) {
			Arrays.sort(intervalsMs, 0, intervals);
			stats.put("interval_median_s", intervalsMs[intervals / 2] / 1000d); // sampling rate
			stats.put("interval_max_s", intervalsMs[intervals - 1] / 1000d);
		}
		stats.put("has_speed", analysis.getHasSpeedInTrack()); // <speed> in the file, the track is nospeed without it
		stats.put("teleport", teleport); // a gap crossed faster than an airliner, see GarbageClassifier
		if (analysis.hasElevationData()) {
			stats.put("ele_min", Math.round(analysis.getMinElevation()));
			stats.put("ele_max", Math.round(analysis.getMaxElevation()));
			stats.put("ele_up", Math.round(analysis.getDiffElevationUp()));
			stats.put("ele_down", Math.round(analysis.getDiffElevationDown()));
		}
		putWindowSpeeds(gpxFile, stats);
		return toJson(stats);
	}

	// Speeds over windows of >= SPEED_WINDOW_MIN_MS and >= SPEED_WINDOW_MIN_M: time-weighted percentiles and the share of
	// moving time and distance per SPEED_BINS_KMH bin, so speed thresholds can change without parsing again
	static void putWindowSpeeds(GpxFile gpxFile, Map<String, Object> stats) {
		List<double[]> windows = new ArrayList<>(); // km/h, seconds, meters of moving windows
		double totalS = 0;
		for (Track track : gpxFile.getTracks(false)) {
			for (TrkSegment seg : track.getSegments()) {
				WptPt anchor = null;
				for (WptPt p : seg.getPoints()) {
					if (p.getTime() <= 0) {
						continue;
					}
					long dtMs = anchor == null ? 0 : p.getTime() - anchor.getTime();
					if (anchor == null || dtMs < 0) {
						anchor = p;
						continue;
					}
					double dist = MapUtils.getDistance(anchor.getLat(), anchor.getLon(), p.getLat(), p.getLon());
					if (dtMs >= SPEED_WINDOW_MIN_MS && (dist >= SPEED_WINDOW_MIN_M || dtMs >= SPEED_WINDOW_STANDING_MS)) {
						if (dtMs < SPEED_WINDOW_MAX_MS) {
							totalS += dtMs / 1000d;
							double kmh = dist / dtMs * 3_600; // m/ms -> km/h
							if (kmh > MOVING_MIN_KMH) {
								windows.add(new double[] {kmh, dtMs / 1000d, dist});
							}
						}
						anchor = p;
					}
				}
			}
		}
		if (windows.isEmpty()) {
			return;
		}
		windows.sort(Comparator.comparingDouble(w -> w[0]));
		double movingS = 0;
		double movingM = 0;
		for (double[] w : windows) {
			movingS += w[1];
			movingM += w[2];
		}
		stats.put("moving_s", Math.round(movingS));
		stats.put("stop_frac", round3(1 - movingS / totalS)); // share of windowed time below MOVING_MIN_KMH
		double meanKmh = movingM / movingS * 3.6;
		double[] percentiles = {0.5, 0.85, 0.95, 0.99};
		String[] keys = {"speed_p50", "speed_p85", "speed_p95", "speed_p99"};
		double[] binTime = new double[SPEED_BINS_KMH.length];
		double[] binDist = new double[SPEED_BINS_KMH.length];
		double variance = 0;
		double accS = 0;
		int next = 0;
		for (double[] w : windows) {
			accS += w[1];
			while (next < percentiles.length && accS >= percentiles[next] * movingS) {
				stats.put(keys[next++], round1(w[0]));
			}
			int bin = SPEED_BINS_KMH.length - 1;
			while (bin > 0 && w[0] < SPEED_BINS_KMH[bin]) {
				bin--;
			}
			binTime[bin] += w[1];
			binDist[bin] += w[2];
			variance += w[1] * (w[0] - meanKmh) * (w[0] - meanKmh);
		}
		stats.put("speed_cv", round3(Math.sqrt(variance / movingS) / meanKmh)); // near 0: generated timestamps
		stats.put("speed_hist_time", perMille(binTime, movingS));
		stats.put("speed_hist_dist", perMille(binDist, movingM));
	}

	private static int[] perMille(double[] values, double total) {
		int[] res = new int[values.length];
		for (int i = 0; i < values.length; i++) {
			res[i] = (int) Math.round(values[i] * 1000 / total);
		}
		return res;
	}

	private static double round1(double value) {
		return Math.round(value * 10) / 10d;
	}

	private static double round3(double value) {
		return Math.round(value * 1000) / 1000d;
	}

	// track_stats for rows marked error: why, so they can be counted and retried when the reason is fixable
	static String errorTrackStats(String reason) {
		Map<String, Object> stats = new LinkedHashMap<>();
		stats.put("v", TRACK_STATS_VERSION);
		if (reason != null) {
			// parser messages carry a position or a parser instance, drop them to group rows by reason
			for (String tail : new String[] {" (position:", " in org.kxml2"}) {
				int i = reason.indexOf(tail);
				if (i > 0) {
					reason = reason.substring(0, i);
				}
			}
			stats.put("error", reason);
		}
		return toJson(stats);
	}

	private static String toJson(Map<String, Object> stats) {
		try {
			return JSON_MAPPER.writeValueAsString(stats);
		} catch (IOException e) {
			LOG.error("Error writing track stats", e);
			return null;
		}
	}

	// Measures each move from the last distinct position (skipping frozen duplicate coordinates),
	// so a stuck-then-jumping GPS doesn't inflate speed.
	static SpeedMetrics computeSpeedMetrics(GpxFile gpxFile) {
		double movingDist = 0d;
		long movingTimeMs = 0L;
		double coordMaxMps = 0d;
		float recordedMaxMps = 0f;
		for (Track track : gpxFile.getTracks(false)) {
			for (TrkSegment seg : track.getSegments()) {
				WptPt anchor = null; // last distinct position, with the time we first reached it
				for (WptPt p : seg.getPoints()) {
					recordedMaxMps = Math.max(recordedMaxMps, p.getSpeed());
					if (anchor == null) {
						anchor = p;
						continue;
					}
					if (p.getLat() == anchor.getLat() && p.getLon() == anchor.getLon()) {
						continue; // frozen: keep the anchor and its arrival time
					}
					long dtMs = p.getTime() - anchor.getTime();
					if (dtMs >= MIN_SPEED_INTERVAL_MS) { // ignore sub-second bursts with unreliable timestamps
						double dist = MapUtils.getDistance(anchor.getLat(), anchor.getLon(), p.getLat(), p.getLon());
						double speedMps = dist * 1000d / dtMs;
						if (speedMps > MIN_MOVING_SPEED_MPS) {
							movingDist += dist;
							movingTimeMs += dtMs;
							coordMaxMps = Math.max(coordMaxMps, speedMps);
						}
					}
					anchor = p;
				}
			}
		}
		float avgKmh = movingTimeMs > 0 ? (float) (movingDist * 1000d / movingTimeMs * 3.6d) : 0f;
		double maxMps = recordedMaxMps > 0 ? recordedMaxMps : coordMaxMps;
		return new SpeedMetrics(avgKmh, (float) (maxMps * 3.6d));
	}

	record SpeedMetrics(float avgKmh, float maxKmh) {
	}

	private static class TrackData {
		GpxFile gpxFile;
		GpxTrackAnalysis analysis;
		boolean error;
		int pointsCount;
		int timeMinutes;
		int waypointsCount;
		float distanceMeters;
		float avgSpeedKmh;
		float maxSpeedKmh;
		float maxDistBetweenPoints;
		byte[] simplifiedGeometry;
		String fileActivity;
		String trackStats;
		String errorReason;
		boolean hasSpeed;
		boolean teleport;
	}

	private String getActivityByFileActivity(String fileActivity, Map<String, List<String>> activitiesMap) {
		if (fileActivity == null || activitiesMap.isEmpty()) {
			return null;
		}
		for (RouteActivity routeActivity : routeActivityHelper.getActivities()) {
			if (routeActivity.getId().equals(fileActivity)) {
				return activitiesMap.containsKey(fileActivity) ? fileActivity : null;
			}
		}
		return null;
	}

	private String analyzeActivity(String name, String desc, List<String> tags, Map<String, List<String>> activitiesMap) {
		if (activitiesMap.isEmpty()) {
			return null;
		}

		// check tags first
		for (String tag : tags) {
			RouteActivity activity = routeActivityHelper.findActivityByTag(tag);
			if (activity != null && activitiesMap.containsKey(activity.getId())) {
				return activity.getId();
			}
		}

		// check name/desc
		Map<String, String> tagMap = new LinkedHashMap<>();
		activitiesMap.forEach((activityId, tagList) ->
				tagList.stream()
						.sorted((tag1, tag2) -> Integer.compare(tag2.length(), tag1.length()))
						.forEach(tag -> tagMap.put(tag, activityId))
		);

		for (Map.Entry<String, String> entry : tagMap.entrySet()) {
			String tag = entry.getKey();
			String activityId = entry.getValue();
			if (containsWord(name, tag)) {
				if (ACTIVITY_KEYWORD_EXCLUSIONS.contains(tag)) {
					continue;
				}
				return activityId;
			}
			if (containsWord(desc, tag)) {
				return activityId;
			}
		}
		return null;
	}

	private static boolean containsWord(String text, String word) {
		if (text == null) {
			return false;
		}
		for (String part : text.split("[\\s_]+")) {
			if (part.equalsIgnoreCase(word)) {
				return true;
			}
		}
		return false;
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
					ACTIVITY_GROUPS.put(activityId, groupId);
				}
			}
		} catch (IOException e) {
			return Collections.emptyMap();
		}

		return activitiesMap;
	}

	private String analyzeActivityBySpeed(boolean hasSpeedInTrack, float avgSpeed, float maxSpeed) {
		if (!hasSpeedInTrack || avgSpeed <= 0) {
			return NOSPEED_ACTIVITY_TYPE;
		}
		for (String type : ACTIVITY_BY_SPEED) {
			if (avgSpeed <= GROUP_AVG_LIMIT_KMH.get(type) && maxSpeed <= GROUP_MAX_LIMIT_KMH.get(type)) {
				return type;
			}
		}
		return OTHER_GROUP;
	}

	@Nullable
	private static Boolean speedMatchesActivity(String activity, float avgSpeedKmh, float maxSpeedKmh) {
		if (activity == null || avgSpeedKmh <= 0) {
			return null;
		}
		String group = ACTIVITY_GROUPS.get(activity);
		if (group == null) {
			return null; // unknown/garbage/error activity
		}
		Double avgLimitKmh = GROUP_AVG_LIMIT_KMH.get(group);
		Double maxLimitKmh = GROUP_MAX_LIMIT_KMH.get(group);
		if (avgLimitKmh == null || maxLimitKmh == null) {
			return null;
		}
		return avgSpeedKmh <= avgLimitKmh && maxSpeedKmh <= maxLimitKmh;
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
				for (String tagOr : tagsOr) {
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
				r.name = rs.getString(2);
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
		long ID_INIT = Math.max(INITIAL_ID, maxId == null ? 0 : (maxId.longValue() + 1));
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
							last, last + FETCH_INTERVAL, lastTime));
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
		if (wrapper == null) {
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
		if (wrapperFile == null) {
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
		for (PreparedStatementWrapper w : preparedStatements) {
			if (w != null && w.ps != null) {
				if (w.pending > 0) {
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
		if (rs.next()) {
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
				ctx.init(new KeyManager[0], new X509TrustManager[]{new X509TrustManager() {
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
				}}, new SecureRandom());
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
			throws XmlPullParserException, IOException {
		XmlPullParser parser = PlatformUtil.newXMLPullParser();
		parser.setInput(inputReader);
		int tok;
		OsmGpxFile p = null;
		List<String> tags = new ArrayList<String>();
		while ((tok = parser.next()) != XmlPullParser.END_DOCUMENT) {
			if (tok == XmlPullParser.START_TAG) {
				if (parser.getName().equals("gpx_file")) {
					p = new OsmGpxFile(GPX_FILE_PREIX);
					p.id = Long.parseLong(parser.getAttributeValue("", "id"));
					p.user = parser.getAttributeValue("", "user");
					p.name = parser.getAttributeValue("", "name");
					p.visibility = parser.getAttributeValue("", "visibility");
					p.pending = "true".equals(parser.getAttributeValue("", "visibility"));
					p.id = Long.parseLong(parser.getAttributeValue("", "id"));
					String tsStr = parser.getAttributeValue("", "timestamp");
					p.timestamp = tsStr != null ? new Date(GpxUtilities.INSTANCE.parseTime(tsStr)) : new Date(0);
					p.lat = Double.parseDouble(getAttributeDoubleValue(parser, "lat"));
					p.lon = Double.parseDouble(getAttributeDoubleValue(parser, "lon"));
				} else if (parser.getName().equals("description") && p != null) {
					p.description = readText(parser, parser.getName());
				} else if (parser.getName().equals("tag")) {
					String value = readText(parser, parser.getName());
					tags.add(value);
				}
			} else if (tok == XmlPullParser.END_TAG) {
				if (parser.getName().equals("gpx_file")) {
					if (p != null && gpxFiles != null) {
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
		if (isEmpty(vl)) {
			return "0";
		}
		// Normalize decimal separator: API/XML may use comma (e.g. "0,000000")
		return vl.replace(',', '.');
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
