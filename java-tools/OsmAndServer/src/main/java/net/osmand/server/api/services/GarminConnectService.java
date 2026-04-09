package net.osmand.server.api.services;

import com.google.gson.*;
import net.osmand.server.api.repo.CloudUserDevicesRepository;
import net.osmand.server.api.repo.CloudUserFilesRepository;
import net.osmand.server.api.repo.GarminUserConnectionRepository;
import net.osmand.server.utils.GarminFitToGpxParser;
import net.osmand.server.utils.exception.OsmAndPublicApiException;
import net.osmand.shared.KException;
import net.osmand.shared.gpx.GpxFile;
import net.osmand.shared.gpx.GpxUtilities;
import net.osmand.shared.gpx.primitives.Track;
import net.osmand.shared.io.KFile;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import okio.Okio;
import okio.Source;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

import static net.osmand.server.api.services.UserdataService.FILE_TYPE_GPX;

@Service
public class GarminConnectService {

	private static final Log LOG = LogFactory.getLog(GarminConnectService.class);

	@Autowired
	private CloudUserDevicesRepository devicesRepository;

	@Autowired
	private UserdataService userdataService;

	@Autowired
	private CloudUserFilesRepository cloudUserFilesRepository;

	@Autowired
	private GarminUserConnectionRepository garminUserConnectionRepository;

	@Value("${GARMIN_CLIENT_ID:}")
	private String clientId;

	@Value("${GARMIN_CLIENT_SECRET:}")
	private String clientSecret;

	@Value("${GARMIN_REDIRECT_URI:}")
	private String redirectUri;

	@Value("${GARMIN_SUCCESS_REDIRECT:}")
	private String successRedirect;

	public String getClientId() {
		return clientId;
	}

	public String getRedirectUri() {
		return redirectUri;
	}

	public String getSuccessRedirect() {
		return successRedirect;
	}

	public boolean isOAuthConfigured() {
		return clientId != null && !clientId.isBlank()
				&& clientSecret != null && !clientSecret.isBlank()
				&& redirectUri != null && !redirectUri.isBlank();
	}

	public static final String AUTH_URL = "https://connect.garmin.com/oauth2Confirm";
	public static final String TOKEN_URL = "https://diauth.garmin.com/di-oauth2-service/oauth/token";
	public static final String USER_ID_URL = "https://apis.garmin.com/wellness-api/rest/user/id";
	public static final String USER_PERMISSIONS_URL = "https://apis.garmin.com/wellness-api/rest/user/permissions";
	private static final String PARTNER_REGISTRATION_URL = "https://apis.garmin.com/wellness-api/rest/user/registration";
	private static final String BACKFILL_ACTIVITIES_URL = "https://apis.garmin.com/wellness-api/rest/backfill/activities";


	private static final String JSON_PERMISSIONS = "permissions";
	private static final String JSON_ACTIVITY_FILES = "activityFiles";
	private static final String JSON_USER_PERMISSIONS_CHANGE = "userPermissionsChange";
	private static final String JSON_DEREGISTRATIONS = "deregistrations";
	private static final String JSON_USER_ID = "userId";
	private static final String JSON_ACCESS_TOKEN = "access_token";
	private static final String JSON_REFRESH_TOKEN = "refresh_token";
	private static final String JSON_EXPIRES_IN = "expires_in";

	private static final String PERM_HISTORICAL_DATA_EXPORT = "HISTORICAL_DATA_EXPORT";
	private static final String PERM_ACTIVITY_EXPORT = "ACTIVITY_EXPORT";

	private static final Pattern SAFE_GARMIN_SUMMARY_ID = Pattern.compile("^[a-zA-Z0-9._-]+$");
	private static final Pattern NON_FILE_SAFE_ACTIVITY_NAME = Pattern.compile("[^a-zA-Z0-9._-]+");
	private static final int MAX_ACTIVITY_NAME_PREFIX_LEN = 100;
	private static final String GPX_FOLDER_GARMIN = "Garmin";

	private static final String GARMIN_ACTIVITY_FILE_TYPE_FIT = "FIT";
	private static final String GARMIN_ACTIVITY_FILE_TYPE_GPX = "GPX";

	private static final Set<String> GARMIN_TRACK_ACTIVITY_TYPES = Set.of(
			"BMX",
			"BOARD",
			"BIKING",
			"CYCLOCROSS",
			"CYCLING",
			"E_BIKE",
			"HIKING",
			"KAYAK",
			"MTB",
			"MULTI",
			"RIDE",
			"ROWING",
			"RUNNING",
			"RUN",
			"RUCKING",
			"SKATING",
			"SKIING",
			"SNOW",
			"SWIMMING",
			"TRACK",
			"WALKING");

	private static final long MAX_BACKFILL_RANGE_SEC = 30L * 24 * 3600;
	private static final int ACTIVITY_BACKFILL_DEFAULT_DAYS_BACK = 180;

	private static final Duration GARMIN_HTTP_TIMEOUT = Duration.ofSeconds(30);

	// for tests
	private static final long SEC_PER_DAY = 24L * 3600;
	private static final long backfillTestSingleDayStartUtcSec = 1775433600L;

	private final HttpClient httpClient = HttpClient.newHttpClient();

	public GarminUserConnectionRepository.GarminUserConnection getConnectionOrNull(int userid) {
		return garminUserConnectionRepository.findByUserid(userid);
	}

	public enum PartnerDisconnectResult {
		OK,
		NOT_LINKED
	}

	public enum GarminLinkStatus {
		OK,
		TOKEN_EXCHANGE_FAILED,
		GARMIN_USER_ID_FAILED,
		CONFLICT
	}

	public record GarminLinkResult(GarminLinkStatus status, boolean historicalDataExport) {
	}

	public GarminLinkResult linkAfterAuthorization(int userid, String code, String codeVerifier)
			throws IOException, InterruptedException {
		JsonObject tokenJson = exchangeAuthorizationCode(code, codeVerifier, redirectUri);
		if (tokenJson == null) {
			return new GarminLinkResult(GarminLinkStatus.TOKEN_EXCHANGE_FAILED, false);
		}
		GarminUserConnectionRepository.GarminUserConnection row = garminUserConnectionRepository.findByUserid(userid);
		if (row == null) {
			row = new GarminUserConnectionRepository.GarminUserConnection();
			row.userid = userid;
		}
		applyTokenResponse(row, tokenJson);

		String garminUserId = fetchGarminUserId(row.accessToken);
		if (garminUserId == null) {
			LOG.warn("Garmin OAuth: could not fetch Garmin user id for userid=" + userid);
			return new GarminLinkResult(GarminLinkStatus.GARMIN_USER_ID_FAILED, false);
		}

		GarminUserConnectionRepository.GarminUserConnection existing = garminUserConnectionRepository.findByGarminUserId(garminUserId);
		if (existing != null && existing.userid != userid) {
			return new GarminLinkResult(GarminLinkStatus.CONFLICT, false);
		}

		row.garminUserId = garminUserId;
		applyUserPermissionsFromGarmin(row);
		garminUserConnectionRepository.save(row);

		LOG.info("Garmin linked for OsmAnd userid=" + userid);
		return new GarminLinkResult(GarminLinkStatus.OK, row.historicalDataExport);
	}

	// Refresh token lives 3 months and is reissued on every token response
	// When access token is expired, POST grant_type=refresh_token to token URL with last refresh.
	// ensureAccessTokenFresh calls this when current time has passed access_expires_time
	public boolean refreshAccessToken(GarminUserConnectionRepository.GarminUserConnection row) throws IOException, InterruptedException {
		if (row == null || row.refreshToken == null || row.refreshToken.isBlank()) {
			if (row != null) {
				LOG.warn("Garmin refresh skipped: no refresh_token for userid=" + row.userid);
			}
			return false;
		}
		String form = "grant_type=refresh_token"
				+ "&client_id=" + urlEncode(clientId)
				+ "&client_secret=" + urlEncode(clientSecret)
				+ "&refresh_token=" + urlEncode(row.refreshToken);
		HttpRequest req = HttpRequest.newBuilder()
				.uri(URI.create(TOKEN_URL))
				.timeout(GARMIN_HTTP_TIMEOUT)
				.header("Content-Type", "application/x-www-form-urlencoded")
				.POST(HttpRequest.BodyPublishers.ofString(form))
				.build();
		HttpResponse<String> res = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
		if (res.statusCode() / 100 != 2) {
			LOG.warn("Garmin refresh_token failed: HTTP " + res.statusCode() + " " + res.body());
			return false;
		}
		JsonObject tokenJson = new JsonParser().parse(res.body()).getAsJsonObject();
		applyTokenResponse(row, tokenJson);
		garminUserConnectionRepository.save(row);
		return true;
	}

	public void applyUserPermissionsWebhookPayload(String body) {
		if (body == null || body.isBlank()) {
			return;
		}
		JsonElement root = new JsonParser().parse(body);
		if (!root.isJsonObject()) {
			return;
		}
		JsonObject obj = root.getAsJsonObject();
		JsonElement changes = obj.get(JSON_USER_PERMISSIONS_CHANGE);
		if (changes == null || !changes.isJsonArray()) {
			return;
		}
		for (JsonElement el : changes.getAsJsonArray()) {
			if (!el.isJsonObject()) {
				continue;
			}
			JsonObject entry = el.getAsJsonObject();
			String garminUserId = jsonObjectMemberAsString(entry, JSON_USER_ID);
			JsonElement perms = entry.get(JSON_PERMISSIONS);
			if (garminUserId == null || perms == null || !perms.isJsonArray()) {
				continue;
			}
			GarminUserConnectionRepository.GarminUserConnection conn = garminUserConnectionRepository.findByGarminUserId(garminUserId);
			if (conn == null) {
				continue;
			}
			applyGarminPermissionFlags(conn, permissionsToSet(perms.getAsJsonArray()));
			garminUserConnectionRepository.save(conn);
		}
	}

	public void applyDeregistrationsWebhookPayload(String body) {
		if (body == null || body.isBlank()) {
			return;
		}
		JsonElement root = new JsonParser().parse(body);
		if (!root.isJsonObject()) {
			return;
		}
		JsonObject obj = root.getAsJsonObject();
		JsonElement entries = obj.get(JSON_DEREGISTRATIONS);
		if (entries == null || !entries.isJsonArray()) {
			return;
		}
		for (JsonElement el : entries.getAsJsonArray()) {
			if (!el.isJsonObject()) {
				continue;
			}
			String garminUserId = jsonObjectMemberAsString(el.getAsJsonObject(), JSON_USER_ID);
			if (garminUserId == null) {
				continue;
			}
			GarminUserConnectionRepository.GarminUserConnection conn = garminUserConnectionRepository.findByGarminUserId(garminUserId);
			if (conn != null) {
				garminUserConnectionRepository.delete(conn);
				LOG.info("Garmin deregistration: removed link userid=" + conn.userid + " garminUserId=" + garminUserId);
			}
		}
	}

	public PartnerDisconnectResult partnerDisconnect(int userid) throws IOException, InterruptedException {
		GarminUserConnectionRepository.GarminUserConnection row = garminUserConnectionRepository.findByUserid(userid);
		if (row == null) {
			return PartnerDisconnectResult.NOT_LINKED;
		}
		if (!ensureAccessTokenFresh(row)) {
			LOG.warn("Garmin disconnect: token refresh failed for userid=" + userid);
		}
		HttpRequest del = HttpRequest.newBuilder()
				.uri(URI.create(PARTNER_REGISTRATION_URL))
				.timeout(GARMIN_HTTP_TIMEOUT)
				.header("Authorization", "Bearer " + row.accessToken)
				.DELETE()
				.build();
		HttpResponse<String> res = httpClient.send(del, HttpResponse.BodyHandlers.ofString());
		int code = res.statusCode();
		if (code / 100 != 2 && code != 404) {
			LOG.warn("Garmin DELETE user/registration failed: HTTP " + code + " " + res.body());
		}
		garminUserConnectionRepository.delete(row);
		return PartnerDisconnectResult.OK;
	}

	// Get historical activities backfill for the user.
	// Garmin will prepare the data asynchronously and return 202 until it's ready, then return 200 with the same format as activity ping.
	// Backfill range is limited to 30 days per request, so we loop until we cover the whole desired range.
	public void runActivityBackfillForUser(int userid) throws IOException, InterruptedException {
		GarminUserConnectionRepository.GarminUserConnection row = garminUserConnectionRepository.findByUserid(userid);
		if (row == null || !row.historicalDataExport) {
			return;
		}
		if (!isOAuthConfigured()) {
			LOG.warn("Garmin backfill: OAuth client not configured");
			return;
		}
		if (!ensureAccessTokenFresh(row)) {
			LOG.warn("Garmin backfill: cannot refresh access token userid=" + userid);
			return;
		}
		long endSec;
		long startSec;
		if (backfillTestSingleDayStartUtcSec >= 0L) {
			startSec = backfillTestSingleDayStartUtcSec;
			endSec = startSec + SEC_PER_DAY - 1;
		} else {
			endSec = System.currentTimeMillis() / 1000L;
			startSec = endSec - ACTIVITY_BACKFILL_DEFAULT_DAYS_BACK * 24L * 3600L;
		}
		for (long wStart = startSec; wStart <= endSec; ) {
			long wEnd = Math.min(wStart + MAX_BACKFILL_RANGE_SEC - 1, endSec);
			String uri = BACKFILL_ACTIVITIES_URL
					+ "?summaryStartTimeInSeconds=" + wStart
					+ "&summaryEndTimeInSeconds=" + wEnd;
			HttpRequest req = HttpRequest.newBuilder()
					.uri(URI.create(uri))
					.timeout(Duration.ofSeconds(60))
					.header("Authorization", "Bearer " + row.accessToken)
					.GET()
					.build();
			HttpResponse<String> res = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
			int code = res.statusCode();
			if (code == 202 || code == 409) {
				LOG.info("Garmin backfill/activities: HTTP " + code + " range " + wStart + ".." + wEnd + " userid=" + userid);
			} else {
				LOG.warn("Garmin backfill/activities: HTTP " + code + " " + res.body());
			}
			wStart = wEnd + 1;
		}
	}

	// Process activity file ping from Garmin webhook.
	// The body contains an array of new or updated activity files with metadata and callback URLs to download the files.
	// We check permissions, skip unsupported activity types and already existing files, download FIT or GPX files, convert FIT to GPX (GPX is stored as-is), upload to user cloud storage.
	public void processActivityPingBody(String body) {
		if (body == null || body.isBlank()) {
			return;
		}
		JsonElement root = new JsonParser().parse(body);
		if (!root.isJsonObject()) {
			return;
		}
		JsonObject obj = root.getAsJsonObject();
		if (!obj.has(JSON_ACTIVITY_FILES) || !obj.get(JSON_ACTIVITY_FILES).isJsonArray()) {
			return;
		}
		Map<String, GarminUserConnectionRepository.GarminUserConnection> connByGarminUserId = new HashMap<>();
		Map<String, CloudUserDevicesRepository.CloudUserDevice> webDevByGarminUserId = new HashMap<>();
		for (JsonElement el : obj.getAsJsonArray(JSON_ACTIVITY_FILES)) {
			if (el.isJsonObject()) {
				processOneActivityFile(el.getAsJsonObject(), connByGarminUserId, webDevByGarminUserId);
			}
		}
	}

	public JsonArray extractPermissionsArray(String jsonBody) {
		JsonElement root = new JsonParser().parse(jsonBody);
		if (root.isJsonObject()) {
			JsonObject obj = root.getAsJsonObject();
			if (obj.has(JSON_PERMISSIONS) && obj.get(JSON_PERMISSIONS).isJsonArray()) {
				return obj.getAsJsonArray(JSON_PERMISSIONS);
			}
		} else if (root.isJsonArray()) {
			return root.getAsJsonArray();
		}
		LOG.warn("Garmin user permissions: expected object with \"permissions\" array or a JSON array");
		return null;
	}

	private boolean ensureAccessTokenFresh(GarminUserConnectionRepository.GarminUserConnection row) throws IOException, InterruptedException {
		if (row == null) {
			return false;
		}
		if (System.currentTimeMillis() < row.accessExpiresTime) {
			return true;
		}
		return refreshAccessToken(row);
	}

	private void processOneActivityFile(JsonObject o, Map<String, GarminUserConnectionRepository.GarminUserConnection> connByGarminUserId,
	                                    Map<String, CloudUserDevicesRepository.CloudUserDevice> webDevByGarminUserId) {
		// Check activity type and skip if not supported
		String activityType = jsonObjectMemberAsString(o, "activityType");
		if (activityType != null && GARMIN_TRACK_ACTIVITY_TYPES.stream().noneMatch(activityType::contains)) {
			return;
		}
		// Check user
		String garminUserId = jsonObjectMemberAsString(o, JSON_USER_ID);
		String callbackUrl = jsonObjectMemberAsString(o, "callbackURL");
		if (callbackUrl == null || garminUserId == null) {
			LOG.warn("Garmin activityFiles: missing userId or callbackURL");
			return;
		}
		GarminUserConnectionRepository.GarminUserConnection conn = connByGarminUserId.computeIfAbsent(garminUserId,
				id -> garminUserConnectionRepository.findByGarminUserId(id));
		if (conn == null) {
			LOG.warn("Garmin activityFiles: no linked OsmAnd user for garminUserId=" + garminUserId);
			return;
		}
		// Check permissions
		if (!conn.activityExport) {
			LOG.warn("Garmin activityFiles: ACTIVITY_EXPORT not granted, userid=" + conn.userid + " garminUserId=" + garminUserId);
			return;
		}
		CloudUserDevicesRepository.CloudUserDevice dev = webDevByGarminUserId.computeIfAbsent(garminUserId,
				id -> devicesRepository.findTopByUseridAndDeviceidOrderByUdpatetimeDesc(conn.userid,
						UserdataService.TOKEN_DEVICE_WEB));
		if (dev == null) {
			LOG.warn("Garmin activityFiles: no web device for userid=" + conn.userid);
			return;
		}
		String fileType = jsonObjectMemberAsString(o, "fileType");
		if (!isGarminActivityFileTypeSupported(fileType)) {
			return;
		}
		// Check file name and skip if already exists in cloud
		String baseFileName = buildGarminActivityBaseFileName(o);
		if (baseFileName == null) {
			LOG.warn("Garmin activityFiles: missing or invalid summaryId / file base name");
			return;
		}
		String cloudName = GPX_FOLDER_GARMIN + "/" + baseFileName + ".gpx";
		if (cloudUserFilesRepository.existsByUseridAndNameAndType(conn.userid, cloudName, FILE_TYPE_GPX)) {
			LOG.info("Garmin activityFiles: skip duplicate " + cloudName + " userid=" + conn.userid);
			return;
		}
		// Check access token and refresh if needed
		try {
			if (!ensureAccessTokenFresh(conn)) {
				LOG.warn("Garmin activityFiles: token refresh failed userid=" + conn.userid);
				return;
			}
		} catch (IOException | InterruptedException e) {
			reinterruptIfInterrupted(e);
			LOG.warn("Garmin activityFiles: token refresh error userid=" + conn.userid, e);
			return;
		}
		byte[] raw;
		try {
			raw = httpGetCallbackBytes(callbackUrl, conn.accessToken);
		} catch (IOException | InterruptedException e) {
			reinterruptIfInterrupted(e);
			LOG.warn("Garmin activityFiles: download failed baseFileName=" + baseFileName, e);
			return;
		}
		GpxFile gpx = gpxFileFromGarminActivityFile(raw, fileType, baseFileName, conn.userid);
		if (gpx == null) {
			LOG.warn("Garmin activityFiles: cannot parse activity file as " + fileType + " baseFileName=" + baseFileName + " userid=" + conn.userid);
			return;
		}
		File tmp = null;
		try {
			tmp = File.createTempFile("garmin-act-", ".gpx");
			KException werr = GpxUtilities.INSTANCE.writeGpxFile(new KFile(tmp.getAbsolutePath()), gpx);
			if (werr != null) {
				LOG.warn("Garmin activityFiles: GPX write failed userid=" + conn.userid + " " + werr.getMessage());
				return;
			}
			long clientTimeMs = gpx.getMetadata().getTime();
			if (clientTimeMs <= 0L) {
				clientTimeMs = System.currentTimeMillis();
			}
			StorageService.InternalZipFile zip = StorageService.InternalZipFile.buildFromFileAndDelete(tmp);
			tmp = null;
			userdataService.validateUserForUpload(dev, FILE_TYPE_GPX, zip.getSize());
			userdataService.uploadFile(zip, dev, cloudName, FILE_TYPE_GPX, clientTimeMs);
			LOG.info("Garmin activityFiles: stored " + fileType + " as GPX " + cloudName + " userid=" + conn.userid);
		} catch (OsmAndPublicApiException e) {
			LOG.warn("Garmin activityFiles: cloud upload rejected userid=" + conn.userid + " " + e.getMessage());
		} catch (Exception e) {
			LOG.warn("Garmin activityFiles: upload failed userid=" + conn.userid, e);
		} finally {
			if (tmp != null) {
				try {
					Files.deleteIfExists(tmp.toPath());
				} catch (IOException ignored) {
					LOG.info("Garmin activityFiles: failed to delete temp file " + tmp.getAbsolutePath());
				}
			}
		}
	}

	/**
	 * Garmin Activity Files may be FIT, TCX, or GPX per the API. Modern devices record natively in FIT;
	 * TCX is a legacy Training Center XML interchange format and is no longer the primary path.
	 * We only accept FIT and GPX since TCX is not widely used.
	 */
	private static boolean isGarminActivityFileTypeSupported(String fileType) {
		return GARMIN_ACTIVITY_FILE_TYPE_FIT.equals(fileType) || GARMIN_ACTIVITY_FILE_TYPE_GPX.equals(fileType);
	}

	private GpxFile gpxFileFromGarminActivityFile(byte[] raw, String fileType, String baseFileName, int userid) {
		if (raw == null || raw.length == 0) {
			LOG.warn("Garmin activityFiles: empty download userid=" + userid);
			return null;
		}
		if (GARMIN_ACTIVITY_FILE_TYPE_FIT.equals(fileType)) {
			GpxFile gpx = GarminFitToGpxParser.fromFitBytes(raw, baseFileName);
			if (gpx == null) {
				LOG.warn("Garmin activityFiles: FIT decode failed userid=" + userid);
			}
			return gpx;
		}
		if (GARMIN_ACTIVITY_FILE_TYPE_GPX.equals(fileType)) {
			try (Source source = Okio.source(new ByteArrayInputStream(raw))) {
				GpxFile gpx = GpxUtilities.INSTANCE.loadGpxFile(source);
				if (gpx.getError() != null) {
					LOG.warn("Garmin activityFiles: GPX invalid userid=" + userid + " " + gpx.getError().getMessage());
					return null;
				}
				gpx.getMetadata().setName(baseFileName);
				for (Track t : gpx.getTracks()) {
					t.setName(baseFileName);
				}
				return gpx;
			} catch (IOException e) {
				LOG.warn("Garmin activityFiles: GPX load IO userid=" + userid, e);
				return null;
			}
		}
		return null;
	}

	private static void reinterruptIfInterrupted(Exception e) {
		if (e instanceof InterruptedException) {
			Thread.currentThread().interrupt();
		}
	}

	private static String buildGarminActivityBaseFileName(JsonObject activityFilePingEntry) {
		String summaryId = jsonObjectMemberAsString(activityFilePingEntry, "summaryId");
		if (summaryId == null) {
			return null;
		}
		String sid = summaryId.trim();
		if (sid.endsWith("-file")) {
			sid = sid.substring(0, sid.length() - 5);
		}
		if (sid.length() > 200 || !SAFE_GARMIN_SUMMARY_ID.matcher(sid).matches()) {
			return null;
		}
		String activityName = jsonObjectMemberAsString(activityFilePingEntry, "activityName");
		String baseName = sid;
		if (activityName != null && !activityName.isBlank()) {
			String t = NON_FILE_SAFE_ACTIVITY_NAME.matcher(activityName.trim()).replaceAll("_").replaceAll("_+", "_");
			t = t.replaceAll("^_+|_+$", "");
			if (!t.isEmpty() && SAFE_GARMIN_SUMMARY_ID.matcher(t).matches()) {
				if (t.length() > MAX_ACTIVITY_NAME_PREFIX_LEN) {
					t = t.substring(0, MAX_ACTIVITY_NAME_PREFIX_LEN).replaceAll("_+$", "");
				}
				if (!t.isEmpty()) {
					baseName = t + "-" + sid;
				}
			}
		}
		return baseName;
	}

	private byte[] httpGetCallbackBytes(String url, String bearerAccessToken) throws IOException, InterruptedException {
		assertTrustedGarminActivityCallbackUrl(url);
		HttpRequest.Builder rb = HttpRequest.newBuilder()
				.uri(URI.create(url.trim()).normalize())
				.timeout(Duration.ofSeconds(60));
		if (bearerAccessToken != null && !bearerAccessToken.isBlank()) {
			rb.header("Authorization", "Bearer " + bearerAccessToken);
		}
		HttpRequest req = rb.GET().build();
		HttpResponse<byte[]> res = httpClient.send(req, HttpResponse.BodyHandlers.ofByteArray());
		int code = res.statusCode();
		if (code == 410) {
			throw new IOException("Garmin callback URL expired (HTTP 410)");
		}
		if (code / 100 != 2) {
			throw new IOException("Garmin callback HTTP " + code);
		}
		return res.body();
	}

	private static void assertTrustedGarminActivityCallbackUrl(String url) throws IOException {
		if (url == null || url.isBlank()) {
			throw new IOException("Garmin callback URL empty");
		}
		final URI uri;
		try {
			uri = URI.create(url.trim()).normalize();
		} catch (IllegalArgumentException e) {
			throw new IOException("Garmin callback URL invalid", e);
		}
		String host = uri.getHost();
		if (host != null && host.endsWith(".")) {
			host = host.substring(0, host.length() - 1);
		}
		int port = uri.getPort();
		String path = uri.getPath();
		if (!"https".equalsIgnoreCase(uri.getScheme())
				|| uri.getUserInfo() != null
				|| uri.getFragment() != null
				|| host == null
				|| !host.equalsIgnoreCase("apis.garmin.com")
				|| (port != -1 && port != 443)
				|| path == null
				|| !path.startsWith("/wellness-api/")) {
			throw new IOException("Garmin callback URL not allowed");
		}
	}

	public static String urlEncode(String v) {
		return URLEncoder.encode(Objects.toString(v, ""), StandardCharsets.UTF_8);
	}

	/** Returns null if member is missing, null, not a string or blank */
	private static String jsonObjectMemberAsString(JsonObject o, String member) {
		JsonElement el = o.get(member);
		if (el == null || el.isJsonNull() || !el.isJsonPrimitive()) {
			return null;
		}
		JsonPrimitive p = el.getAsJsonPrimitive();
		if (!p.isString()) {
			return null;
		}
		String s = p.getAsString();
		return s.isBlank() ? null : s;
	}

	private JsonObject exchangeAuthorizationCode(String code, String codeVerifier, String redirectUriParam)
			throws IOException, InterruptedException {
		String form = "grant_type=authorization_code"
				+ "&client_id=" + urlEncode(clientId)
				+ "&client_secret=" + urlEncode(clientSecret)
				+ "&code=" + urlEncode(code)
				+ "&code_verifier=" + urlEncode(codeVerifier)
				+ "&redirect_uri=" + urlEncode(redirectUriParam);
		HttpRequest tokenReq = HttpRequest.newBuilder()
				.uri(URI.create(TOKEN_URL))
				.timeout(GARMIN_HTTP_TIMEOUT)
				.header("Content-Type", "application/x-www-form-urlencoded")
				.POST(HttpRequest.BodyPublishers.ofString(form))
				.build();
		HttpResponse<String> tokenRes = httpClient.send(tokenReq, HttpResponse.BodyHandlers.ofString());
		if (tokenRes.statusCode() / 100 != 2) {
			LOG.warn("Garmin token exchange failed: HTTP " + tokenRes.statusCode() + " " + tokenRes.body());
			return null;
		}
		return new JsonParser().parse(tokenRes.body()).getAsJsonObject();
	}

	private String fetchGarminUserId(String accessToken) throws IOException, InterruptedException {
		HttpRequest userReq = HttpRequest.newBuilder()
				.uri(URI.create(USER_ID_URL))
				.timeout(Duration.ofSeconds(15))
				.header("Authorization", "Bearer " + accessToken)
				.GET()
				.build();
		HttpResponse<String> userRes = httpClient.send(userReq, HttpResponse.BodyHandlers.ofString());
		if (userRes.statusCode() / 100 != 2) {
			LOG.warn("Garmin user id failed: HTTP " + userRes.statusCode() + " " + userRes.body());
			return null;
		}
		JsonObject userJson = new JsonParser().parse(userRes.body()).getAsJsonObject();
		return jsonObjectMemberAsString(userJson, JSON_USER_ID);
	}

	private void applyUserPermissionsFromGarmin(GarminUserConnectionRepository.GarminUserConnection row) {
		applyGarminPermissionFlags(row, Set.of());
		try {
			JsonArray permArray = fetchGarminPermissionsArray(row.accessToken);
			if (permArray == null) {
				return;
			}
			applyGarminPermissionFlags(row, permissionsToSet(permArray));
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			LOG.warn("Garmin user permissions interrupted");
		} catch (Exception e) {
			LOG.warn("Garmin user permissions request failed", e);
		}
	}

	private JsonArray fetchGarminPermissionsArray(String accessToken) throws IOException, InterruptedException {
		HttpRequest req = HttpRequest.newBuilder()
				.uri(URI.create(USER_PERMISSIONS_URL))
				.timeout(Duration.ofSeconds(15))
				.header("Authorization", "Bearer " + accessToken)
				.GET()
				.build();
		HttpResponse<String> res = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
		if (res.statusCode() / 100 != 2) {
			LOG.warn("Garmin user permissions failed: HTTP " + res.statusCode() + " " + res.body());
			return null;
		}
		return extractPermissionsArray(res.body());
	}

	private static Set<String> permissionsToSet(JsonArray permArray) {
		Set<String> granted = new HashSet<>();
		for (JsonElement e : permArray) {
			if (e.isJsonPrimitive() && e.getAsJsonPrimitive().isString()) {
				granted.add(e.getAsString());
			}
		}
		return granted;
	}

	private static void applyGarminPermissionFlags(GarminUserConnectionRepository.GarminUserConnection row, Set<String> granted) {
		row.historicalDataExport = granted.contains(PERM_HISTORICAL_DATA_EXPORT);
		row.activityExport = granted.contains(PERM_ACTIVITY_EXPORT);
	}

	private void applyTokenResponse(GarminUserConnectionRepository.GarminUserConnection row, JsonObject tokenJson) {
		String access = jsonObjectMemberAsString(tokenJson, JSON_ACCESS_TOKEN);
		row.accessToken = access != null ? access : "";
		String refresh = jsonObjectMemberAsString(tokenJson, JSON_REFRESH_TOKEN);
		if (refresh != null) {
			row.refreshToken = refresh;
		}
		int expiresIn = tokenJson.has(JSON_EXPIRES_IN) ? tokenJson.get(JSON_EXPIRES_IN).getAsInt() : 86400;
		row.accessExpiresTime = System.currentTimeMillis() + Math.max(60, expiresIn - 600) * 1000L;
	}
}
