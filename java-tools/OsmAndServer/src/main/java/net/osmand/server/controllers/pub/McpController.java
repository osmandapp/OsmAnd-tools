package net.osmand.server.controllers.pub;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import okio.Buffer;
import okio.Source;
import net.osmand.server.api.repo.CloudUserDevicesRepository;
import net.osmand.server.api.repo.CloudUserDevicesRepository.CloudUserDevice;
import net.osmand.server.api.repo.CloudUserFilesRepository;
import net.osmand.server.api.repo.CloudUserFilesRepository.UserFile;
import net.osmand.server.api.repo.CloudUserFilesRepository.UserFileNoData;
import net.osmand.server.api.repo.CloudUsersRepository;
import net.osmand.server.api.repo.CloudUsersRepository.CloudUser;
import net.osmand.server.api.repo.OAuthGrantsRepository.OAuthGrant;
import net.osmand.server.api.services.FavoriteService;
import net.osmand.server.api.services.OAuthService;
import net.osmand.server.api.services.OAuthService.CloudGroup;
import net.osmand.server.api.services.OsmAndMapsService;
import net.osmand.server.api.services.RoutingService;
import net.osmand.server.api.services.ShareFileService;
import net.osmand.server.api.services.WikiService;
import net.osmand.server.api.services.mcp.McpRoutes;
import net.osmand.server.api.services.mcp.McpSearch;
import net.osmand.server.api.services.mcp.McpTracks;
import net.osmand.server.utils.WebGpxParser;
import net.osmand.server.api.services.StorageService.InternalZipFile;
import net.osmand.server.api.services.UserSubscriptionService;
import net.osmand.server.api.services.UserdataService;
import net.osmand.shared.gpx.GpxFile;
import net.osmand.shared.gpx.GpxUtilities;
import net.osmand.shared.gpx.primitives.WptPt;

/**
 * Remote MCP server (streamable HTTP, JSON responses only) for OAuth clients such as Claude.
 * Tools work with the user's OsmAnd Cloud files; every Cloud group has its own read and write scope.
 * A write or delete adds a new file version, earlier versions stay in Cloud.
 */
@RestController
@ConditionalOnProperty(name = "osmand.oauth.enabled", havingValue = "true")
public class McpController {

	protected static final Log LOG = LogFactory.getLog(McpController.class);

	public static final String MCP_PATH = "/mcp";
	private static final List<String> PROTOCOL_VERSIONS = List.of("2025-06-18", "2025-03-26", "2024-11-05");
	private static final int MAX_ITEMS = 500;
	private static final long MAX_READ_SIZE = 1024 * 1024;
	private static final int MAX_WRITE_SIZE = 5 * 1024 * 1024;
	private static final int MAX_NAME_LENGTH = 512;
	// tracks are parsed on the server for analyze_track / read_track_points
	private static final long MAX_TRACK_SIZE = 64 * 1024 * 1024;
	private static final long DOWNLOAD_LINK_TTL_MIN = 10;
	// guide for assistants, kept in web-server-config to edit without a server release
	private static final String GUIDE_FILE = "api/mcp_guide.md";
	private static final String GUIDE_URI = "osmand://guide";

	@Autowired
	private OAuthService oauthService;

	@Autowired
	private WikiService wikiService;

	@Autowired
	private CloudUsersRepository usersRepository;

	@Autowired
	private CloudUserDevicesRepository devicesRepository;

	@Autowired
	private CloudUserFilesRepository filesRepository;

	@Autowired
	private UserdataService userdataService;

	@Autowired
	private UserSubscriptionService userSubService;

	@Autowired
	private ShareFileService shareFileService;

	@Autowired
	private RoutingController routingController;

	@Autowired
	private RoutingService routingService;

	@Autowired
	private OsmAndMapsService osmAndMapsService;

	@Autowired
	private WebGpxParser webGpxParser;

	private volatile McpRoutes routes;

	private volatile McpSearch search;

	@Value("${osmand.mcp.server-api:}")
	private String serverApi;

	@Value("${osmand.web.location}")
	private String websiteLocation;

	private final Gson gson = new GsonBuilder().disableHtmlEscaping().create();

	// one-time download links: sha256(token) -> file version, bound to the grant that made it; lost on restart
	private record DownloadLink(int grantId, long fileId, String fileName) {
	}

	private final Cache<String, DownloadLink> downloadLinks = CacheBuilder.newBuilder()
			.expireAfterWrite(DOWNLOAD_LINK_TTL_MIN, java.util.concurrent.TimeUnit.MINUTES).maximumSize(10000).build();

	private enum Access { NONE, READ, WRITE }

	private record Tool(String name, Access access, String description, Map<String, Object> inputSchema) {
	}

	private static final Map<String, Object> TYPE_ARG = Map.of("type", "string",
			"description", "Cloud file type as returned by list_files, e.g. GPX, FAVOURITES, PROFILE");
	private static final Map<String, Object> NAME_ARG = Map.of("type", "string",
			"description", "File name with folder as returned by list_files");

	private static final Map<String, Object> POINTS_ARG = Map.of("type", "array", "description",
			"2-" + McpRoutes.MAX_POINTS + " points in order; profile/params of a point apply to the leg from it to the next",
			"items", Map.of("type", "object", "properties", Map.of("lat", Map.of("type", "number"),
					"lon", Map.of("type", "number"),
					"profile", Map.of("type", "string", "description", "Leg profile: car, bicycle, ..., line or gap"),
					"params", Map.of("type", "object")), "required", List.of("lat", "lon")));

	// tools without Cloud data: any connection may use them
	private static final Set<String> OPEN_TOOLS = Set.of("get_guide", "get_routing_profiles", "build_route", "search",
			"get_poi_categories", "search_popular_places");

	private static final List<Tool> TOOLS = List.of(
			new Tool("get_guide", Access.NONE,
					"How OsmAnd Cloud files are organized and formatted (favorites, tracks...). Read before writing files.",
					schema(Map.of(), List.of())),
			new Tool("get_account", Access.NONE,
					"OsmAnd account of the connected user: email and OsmAnd Pro status.",
					schema(Map.of(), List.of())),
			new Tool("list_files", Access.READ,
					"Files stored in OsmAnd Cloud (latest versions, deleted files excluded): group, type, name, size, "
							+ "last change. Groups: " + groupKeys() + ".",
					schema(Map.of("group", Map.of("type", "string", "description", "Only this group"),
							"folder", Map.of("type", "string", "description", "Only names starting with this prefix")),
							List.of())),
			new Tool("read_file", Access.READ,
					"Text content of one Cloud file (GPX, JSON, XML...), up to 1 MB. Pass version to read an older "
							+ "version from list_versions.",
					schema(Map.of("type", TYPE_ARG, "name", NAME_ARG,
							"version", Map.of("type", "integer", "description", "Version time in ms from list_versions")),
							List.of("type", "name"))),
			new Tool("analyze_track", Access.READ,
					"Summary of a GPX track computed on the server, for tracks of any size: distance, duration, moving "
							+ "time, average and max speed (where and when), elevation, stops (where, when, how long) and "
							+ "a profile of equal-distance parts with time, average speed and elevation.",
					schema(Map.of("type", TYPE_ARG, "name", NAME_ARG,
							"version", Map.of("type", "integer", "description", "Version time in ms from list_versions"),
							"profile_parts", Map.of("type", "integer", "description",
									"Number of profile parts, default 50, max " + McpTracks.MAX_PROFILE),
							"stop_minutes", Map.of("type", "number", "description", "Shortest stop to report, default 3")),
							List.of("type", "name"))),
			new Tool("read_track_points", Access.READ,
					"Track points of a GPX file in slices: index, time, lat, lon, elevation, distance from start, "
							+ "recorded speed. Select by index or time range and take every step-th point; at most "
							+ McpTracks.MAX_POINTS + " points per call, continue from nextFromIndex.",
					schema(Map.of("type", TYPE_ARG, "name", NAME_ARG,
							"version", Map.of("type", "integer", "description", "Version time in ms from list_versions"),
							"from_index", Map.of("type", "integer"), "to_index", Map.of("type", "integer"),
							"from_time", Map.of("type", "string", "description", "ISO time, e.g. 2025-12-28T13:40:00Z"),
							"to_time", Map.of("type", "string", "description", "ISO time"),
							"step", Map.of("type", "integer", "description", "Every step-th point; default fits the range "
									+ "into " + McpTracks.MAX_POINTS + " points")),
							List.of("type", "name"))),
			new Tool("get_download_link", Access.READ,
					"One-time HTTPS link to download a whole Cloud file (any size), valid " + DOWNLOAD_LINK_TTL_MIN
							+ " minutes, for assistants that can run code (e.g. curl and a script). Do not show the "
							+ "link to other people. Prefer analyze_track / read_track_points when no code can be run.",
					schema(Map.of("type", TYPE_ARG, "name", NAME_ARG,
							"version", Map.of("type", "integer", "description", "Version time in ms from list_versions")),
							List.of("type", "name"))),
			new Tool("get_routing_profiles", Access.NONE,
					"Navigation profiles of the OsmAnd router (car, bicycle, pedestrian, truck...) with their "
							+ "parameters (avoid motorways, short way, driving style...) and the special profiles line and gap.",
					schema(Map.of(), List.of())),
			new Tool("build_route", Access.NONE,
					"Route through points with the OsmAnd router. Each point may set the profile of the leg that starts "
							+ "at it, e.g. car then line then bicycle,avoid_unpaved; the others use the default profile. "
							+ "Returns distance per leg and a simplified geometry. Does not save anything.",
					schema(Map.of("points", POINTS_ARG,
							"profile", Map.of("type", "string", "description", "Default profile, e.g. car or "
									+ "\"bicycle,avoid_unpaved\" (see get_routing_profiles)"),
							"params", Map.of("type", "object", "description", "Default profile parameters, e.g. "
									+ "{\"avoid_motorway\": true, \"width\": 2.5}")),
							List.of("points"))),
			new Tool("search", Access.NONE,
					"Search places as the OsmAnd search box: POIs by name or type (\"cafe\", \"Golden Gate\", \"fuel\"), "
							+ "addresses, streets, cities. Results near lat/lon come first, with distance, address, opening "
							+ "hours, phone, website, OSM link. Up to " + McpSearch.MAX_RESULTS + " results.",
					schema(Map.of("text", Map.of("type", "string"),
							"lat", Map.of("type", "number", "description", "Search near this point"),
							"lon", Map.of("type", "number"),
							"locale", Map.of("type", "string", "description", "Language of names, e.g. en, de, uk; default en"),
							"limit", Map.of("type", "integer", "description", "Default 20, max " + McpSearch.MAX_RESULTS)),
							List.of("text", "lat", "lon"))),
			new Tool("get_poi_categories", Access.NONE,
					"OsmAnd POI categories with their types (e.g. sustenance: cafe, restaurant...), names that search "
							+ "understands, and the topics of search_popular_places.",
					schema(Map.of("locale", Map.of("type", "string", "description", "Language, default en")), List.of())),
			new Tool("search_popular_places", Access.NONE,
					"Popular places around a point, as the OsmAnd map Explore layer: Wikipedia / Wikidata places ranked "
							+ "by popularity, with a short description, Wikipedia link and photo. Good for sightseeing and "
							+ "planning walks.",
					schema(Map.of("lat", Map.of("type", "number"), "lon", Map.of("type", "number"),
							"radius_km", Map.of("type", "number", "description", "Default 3, max " + (int) McpSearch.MAX_RADIUS_KM),
							"topics", Map.of("type", "array", "items", Map.of("type", "string"), "description",
									"Topic ids, default all: " + McpSearch.TOPICS),
							"locale", Map.of("type", "string", "description", "Language, default en"),
							"limit", Map.of("type", "integer", "description", "Default 20, max " + McpSearch.MAX_RESULTS)),
							List.of("lat", "lon"))),
			new Tool("create_track", Access.WRITE,
					"Build a route through points (as build_route) and save it as a GPX track in OsmAnd Cloud "
							+ "(a new file or a new version). The track keeps the route points with their profiles, so "
							+ "it can be edited in OsmAnd Plan route.",
					schema(Map.of("name", Map.of("type", "string", "description", "Track file name with folder, "
									+ "ending with .gpx, e.g. Trips/Alps day 1.gpx"),
							"points", POINTS_ARG,
							"profile", Map.of("type", "string", "description", "Default profile"),
							"params", Map.of("type", "object", "description", "Default profile parameters"),
							"title", Map.of("type", "string", "description", "Track name shown in OsmAnd"),
							"description", Map.of("type", "string")),
							List.of("name", "points"))),
			new Tool("list_versions", Access.READ,
					"All stored versions of one Cloud file, newest first; deleted=true marks a deletion.",
					schema(Map.of("type", TYPE_ARG, "name", NAME_ARG), List.of("type", "name"))),
			new Tool("list_favorites", Access.READ,
					"Favorite points grouped by favorites group: name, lat, lon, description.",
					schema(Map.of("group", Map.of("type", "string", "description", "Only this favorites group")),
							List.of())),
			new Tool("write_file", Access.WRITE,
					"Create or replace a Cloud file with new text content (GPX must be valid GPX, .json/.info valid JSON). "
							+ "The previous version stays in Cloud. OsmAnd apps pick the change up on next sync.",
					schema(Map.of("type", TYPE_ARG, "name", NAME_ARG,
							"content", Map.of("type", "string", "description", "Full new file content")),
							List.of("type", "name", "content"))),
			new Tool("delete_file", Access.WRITE,
					"Delete a Cloud file. Earlier versions stay in Cloud and can be restored with read_file + write_file.",
					schema(Map.of("type", TYPE_ARG, "name", NAME_ARG), List.of("type", "name"))));

	// GET /mcp?download=<token> from get_download_link: the token is the only credential, so it is one-time,
	// short-lived, bound to one file version and dies with the connection (revoke, access off)
	@RequestMapping(path = MCP_PATH, method = RequestMethod.GET, params = "download")
	public void download(@RequestParam("download") String token, HttpServletResponse response) throws Exception {
		DownloadLink link = token == null ? null : downloadLinks.asMap().remove(OAuthService.hash(token));
		OAuthGrant grant = link == null ? null : oauthService.activeGrant(link.grantId);
		UserFile uf = grant == null ? null : filesRepository.findById(link.fileId).orElse(null);
		if (uf == null || uf.userid != grant.userid || uf.filesize < 0
				|| !can(grant, CloudGroup.ofType(uf.type), Access.READ)) {
			response.sendError(HttpStatus.NOT_FOUND.value(), "Link is invalid, used or expired");
			return;
		}
		LOG.info("MCP download " + uf.type + " " + uf.name + " user " + grant.userid + " client " + grant.clientid);
		String fileName = link.fileName.replaceAll("[^\\w.\\- ]", "_");
		response.setContentType(fileName.toLowerCase().endsWith(".gpx") ? "application/gpx+xml"
				: MediaType.APPLICATION_OCTET_STREAM_VALUE);
		response.setHeader(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + fileName + "\"");
		response.setHeader(HttpHeaders.CACHE_CONTROL, "no-store");
		response.setHeader("Referrer-Policy", "no-referrer");
		try (InputStream in = new GZIPInputStream(openStream(uf))) {
			in.transferTo(response.getOutputStream());
		}
	}

	@RequestMapping(path = MCP_PATH, method = { RequestMethod.GET, RequestMethod.DELETE })
	public ResponseEntity<String> notSupported() {
		// no server-initiated stream and no sessions
		return ResponseEntity.status(HttpStatus.METHOD_NOT_ALLOWED).header(HttpHeaders.ALLOW, "POST").build();
	}

	@RequestMapping(path = MCP_PATH, method = RequestMethod.POST, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> handle(@RequestBody(required = false) String body, HttpServletRequest request) {
		String auth = request.getHeader(HttpHeaders.AUTHORIZATION);
		String token = auth != null && auth.startsWith("Bearer ") ? auth.substring(7).trim() : null;
		OAuthGrant grant = oauthService.validateAccessToken(token);
		if (grant == null) {
			String resourceMetadata = OAuthController.baseUrl() + OAuthController.PROTECTED_RESOURCE_PATH + MCP_PATH;
			String challenge = "Bearer resource_metadata=\"" + resourceMetadata + "\""
					+ (token != null ? ", error=\"invalid_token\"" : "");
			return ResponseEntity.status(HttpStatus.UNAUTHORIZED).header(HttpHeaders.WWW_AUTHENTICATE, challenge)
					.body(gson.toJson(Map.of("error", "unauthorized")));
		}
		JsonObject req;
		try {
			req = new JsonParser().parse(body == null ? "" : body).getAsJsonObject();
		} catch (RuntimeException e) {
			return ResponseEntity.badRequest().body(gson.toJson(error(null, -32700, "Parse error")));
		}
		// keep the id as sent (Gson would turn 1 into 1.0)
		JsonElement id = req.get("id");
		String method = req.has("method") && req.get("method").isJsonPrimitive() ? req.get("method").getAsString() : null;
		if (method == null || method.startsWith("notifications/")) {
			// client responses and notifications need no answer
			return ResponseEntity.accepted().build();
		}
		Map<?, ?> params = req.get("params") instanceof JsonObject p ? gson.fromJson(p, Map.class) : Map.of();
		Map<String, Object> res = switch (method) {
		case "initialize" -> result(id, initialize(params));
		case "ping" -> result(id, Map.of());
		case "tools/list" -> result(id, Map.of("tools", listTools(grant)));
		case "tools/call" -> callTool(id, grant, params);
		case "resources/list" -> result(id, Map.of("resources", listResources()));
		case "resources/read" -> readResource(id, params);
		default -> error(id, -32601, "Method not found: " + method);
		};
		return ResponseEntity.ok(gson.toJson(res));
	}

	private Map<String, Object> initialize(Map<?, ?> params) {
		Object asked = params.get("protocolVersion");
		String version = PROTOCOL_VERSIONS.contains(asked) ? (String) asked : PROTOCOL_VERSIONS.get(0);
		return Map.of("protocolVersion", version,
				"capabilities", Map.of("tools", Map.of("listChanged", false), "resources", Map.of("listChanged", false)),
				"serverInfo", Map.of("name", "osmand", "version", "0.2"),
				"instructions", "Tools work with the user's OsmAnd Cloud files (favorites, tracks, markers, settings...). "
						+ "They do not control the app on the phone; changes reach the apps on their next Cloud sync. "
						+ "Call get_guide before writing files. Ask the user before write_file or delete_file. "
						+ "For GPX tracks use analyze_track and read_track_points instead of read_file. "
						+ "search, search_popular_places and build_route work with the OsmAnd map, not with Cloud files.");
	}

	private List<Map<String, Object>> listTools(OAuthGrant grant) {
		List<Map<String, Object>> res = new ArrayList<>();
		for (Tool t : TOOLS) {
			if (isAvailable(grant, t)) {
				Map<String, Object> annotations = t.access == Access.WRITE
						? Map.of("readOnlyHint", false, "destructiveHint", true)
						: Map.of("readOnlyHint", true);
				res.add(Map.of("name", t.name, "description", t.description, "inputSchema", t.inputSchema,
						"annotations", annotations));
			}
		}
		return res;
	}

	private Map<String, Object> callTool(JsonElement id, OAuthGrant grant, Map<?, ?> params) {
		String name = (String) params.get("name");
		Map<?, ?> args = params.get("arguments") instanceof Map<?, ?> m ? m : Map.of();
		Tool tool = TOOLS.stream().filter(t -> t.name.equals(name)).findFirst().orElse(null);
		if (tool == null) {
			return error(id, -32602, "Unknown tool: " + name);
		}
		if (!isAvailable(grant, tool)) {
			return toolResult(id, "Access to this tool was not granted", true);
		}
		LOG.info("MCP tool call " + name + " user " + grant.userid + " client " + grant.clientid);
		try {
			Object data = switch (name) {
			case "get_guide" -> guide();
			case "get_account" -> getAccount(grant.userid);
			case "list_files" -> listFiles(grant, str(args, "group"), str(args, "folder"));
			case "read_file" -> readFile(grant, str(args, "type"), str(args, "name"), args.get("version"));
			case "analyze_track" -> analyzeTrack(grant, args);
			case "read_track_points" -> readTrackPoints(grant, args);
			case "get_download_link" -> downloadLink(grant, str(args, "type"), str(args, "name"), args.get("version"));
			case "get_routing_profiles" -> routes().describe();
			case "build_route" -> routes().summary(buildRoute(args), true);
			case "create_track" -> createTrack(grant, args);
			case "search", "get_poi_categories", "search_popular_places" -> searchTool(name, args);
			case "list_versions" -> listVersions(grant, str(args, "type"), str(args, "name"));
			case "list_favorites" -> listFavorites(grant, str(args, "group"));
			case "write_file" -> writeFile(grant, str(args, "type"), str(args, "name"), str(args, "content"));
			case "delete_file" -> deleteFile(grant, str(args, "type"), str(args, "name"));
			default -> throw new IllegalArgumentException("Unknown tool " + name);
			};
			return toolResult(id, data instanceof String s ? s : gson.toJson(data), false);
		} catch (ToolException e) {
			return toolResult(id, e.getMessage(), true);
		} catch (Exception e) {
			LOG.error("MCP tool " + name + " failed for user " + grant.userid, e);
			return toolResult(id, "Tool failed: " + e.getMessage(), true);
		}
	}

	// ---------- guide ----------

	private String readGuide() {
		File f = new File(websiteLocation, GUIDE_FILE);
		try {
			return f.isFile() ? Files.readString(f.toPath()) : null;
		} catch (Exception e) {
			LOG.error("Can't read " + f, e);
			return null;
		}
	}

	private String guide() throws ToolException {
		String text = readGuide();
		if (text == null) {
			throw new ToolException("Guide is not available");
		}
		return text;
	}

	private List<Map<String, Object>> listResources() {
		return readGuide() == null ? List.of()
				: List.of(Map.of("uri", GUIDE_URI, "name", "OsmAnd Cloud guide", "mimeType", "text/markdown"));
	}

	private Map<String, Object> readResource(JsonElement id, Map<?, ?> params) {
		String text = GUIDE_URI.equals(params.get("uri")) ? readGuide() : null;
		if (text == null) {
			return error(id, -32002, "Resource not found");
		}
		return result(id, Map.of("contents", List.of(Map.of("uri", GUIDE_URI, "mimeType", "text/markdown", "text", text))));
	}

	// ---------- tools ----------

	private Map<String, Object> getAccount(int userId) {
		CloudUser pu = usersRepository.findById(userId);
		long proExpiry = userSubService.latestProExpiry(userId);
		Map<String, Object> res = new LinkedHashMap<>();
		res.put("email", pu != null ? pu.email : null);
		res.put("pro", proExpiry > System.currentTimeMillis());
		res.put("proExpires", proExpiry > 0 ? new Date(proExpiry).toInstant().toString() : null);
		return res;
	}

	private List<Map<String, Object>> listFiles(OAuthGrant grant, String group, String folder) throws ToolException {
		if (group != null && Arrays.stream(CloudGroup.values()).noneMatch(g -> g.key.equals(group))) {
			throw new ToolException("Unknown group " + group + ", use one of " + groupKeys());
		}
		List<Map<String, Object>> res = new ArrayList<>();
		for (UserFileNoData f : userdataService.generateFiles(grant.userid, null, false, false,
				Collections.emptySet()).uniqueFiles) {
			CloudGroup g = CloudGroup.ofType(f.type);
			if (f.filesize < 0 || !can(grant, g, Access.READ) || (group != null && !g.key.equals(group))
					|| (folder != null && !f.name.startsWith(folder))) {
				continue;
			}
			if (res.size() >= MAX_ITEMS) {
				res.add(Map.of("truncated", "only the first " + MAX_ITEMS + " files are listed, filter by group or folder"));
				break;
			}
			Map<String, Object> m = new LinkedHashMap<>();
			m.put("group", g.key);
			m.put("type", f.type);
			m.put("name", f.name);
			m.put("size", f.filesize);
			m.put("updated", iso(f.updatetime));
			res.add(m);
		}
		return res;
	}

	// latest or the given version of a file the grant may read
	private UserFile readableFile(OAuthGrant grant, String type, String name, Object version) throws ToolException {
		checkFileAccess(grant, type, name, Access.READ);
		UserFile uf = version instanceof Number n ? findVersion(grant.userid, type, name, n.longValue())
				: userdataService.getLastFileVersion(grant.userid, name, type);
		if (uf == null) {
			throw new ToolException("File not found");
		}
		if (uf.filesize < 0) {
			throw new ToolException("This version is a deletion; use list_versions for earlier versions");
		}
		return uf;
	}

	private String readFile(OAuthGrant grant, String type, String name, Object version) throws Exception {
		UserFile uf = readableFile(grant, type, name, version);
		if (uf.filesize > MAX_READ_SIZE) {
			throw new ToolException("File is too large to read (" + uf.filesize + " bytes). For a GPX track use "
					+ "analyze_track or read_track_points; get_download_link gives the whole file to code.");
		}
		byte[] bytes;
		try (InputStream in = new GZIPInputStream(openStream(uf))) {
			bytes = in.readAllBytes();
		}
		try {
			return StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
					.onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(bytes)).toString();
		} catch (CharacterCodingException e) {
			throw new ToolException("Binary file (" + bytes.length + " bytes), only text files can be read");
		}
	}

	private McpTracks loadTrack(OAuthGrant grant, Map<?, ?> args, GpxFile[] gpxOut) throws Exception {
		String type = str(args, "type"), name = str(args, "name");
		UserFile uf = readableFile(grant, type, name, args.get("version"));
		if (name == null || !name.toLowerCase().endsWith(".gpx")) {
			throw new ToolException("Not a GPX file");
		}
		if (uf.filesize > MAX_TRACK_SIZE) {
			throw new ToolException("Track is too large to analyze (" + uf.filesize + " bytes), use get_download_link");
		}
		GpxFile gpx = shareFileService.getFile(uf);
		if (gpx == null) {
			throw new ToolException("Could not read the GPX file");
		}
		if (gpxOut != null) {
			gpxOut[0] = gpx;
		}
		return new McpTracks(gpx);
	}

	private Map<String, Object> analyzeTrack(OAuthGrant grant, Map<?, ?> args) throws Exception {
		GpxFile[] gpx = new GpxFile[1];
		McpTracks t = loadTrack(grant, args, gpx);
		int parts = Math.max(1, Math.min(McpTracks.MAX_PROFILE, num(args, "profile_parts", 50).intValue()));
		double stopMin = Math.max(0.5, num(args, "stop_minutes", 3).doubleValue());
		return t.analyze(gpx[0], parts, stopMin);
	}

	private Map<String, Object> readTrackPoints(OAuthGrant grant, Map<?, ?> args) throws Exception {
		McpTracks t = loadTrack(grant, args, null);
		Number from = num(args, "from_index", null), to = num(args, "to_index", null), step = num(args, "step", null);
		return t.slice(from == null ? null : from.intValue(), to == null ? null : to.intValue(),
				time(args, "from_time"), time(args, "to_time"), step == null ? null : step.intValue());
	}

	private Map<String, Object> downloadLink(OAuthGrant grant, String type, String name, Object version)
			throws ToolException {
		UserFile uf = readableFile(grant, type, name, version);
		String token = "odl_" + java.util.UUID.randomUUID().toString().replace("-", "")
				+ java.util.UUID.randomUUID().toString().replace("-", "");
		String fileName = name.substring(name.lastIndexOf('/') + 1);
		downloadLinks.put(OAuthService.hash(token), new DownloadLink(grant.id, uf.id, fileName));
		LOG.info("MCP download link " + type + " " + name + " user " + grant.userid + " client " + grant.clientid);
		Map<String, Object> res = new LinkedHashMap<>();
		res.put("url", OAuthController.baseUrl() + MCP_PATH + "?download=" + token);
		res.put("fileName", fileName);
		res.put("size", uf.filesize);
		res.put("expiresInMinutes", DOWNLOAD_LINK_TTL_MIN);
		res.put("oneTime", true);
		return res;
	}

	private McpSearch search() {
		McpSearch s = search;
		if (s == null) {
			s = new McpSearch(serverApi, OAuthController.baseUrl(), wikiService);
			search = s;
		}
		return s;
	}

	private Object searchTool(String name, Map<?, ?> args) throws ToolException {
		String locale = str(args, "locale");
		int limit = args.get("limit") instanceof Number n ? Math.min(Math.max(n.intValue(), 1), McpSearch.MAX_RESULTS) : 20;
		try {
			if (name.equals("get_poi_categories")) {
				return search().categories(locale);
			}
			if (!(args.get("lat") instanceof Number lat) || !(args.get("lon") instanceof Number lon)) {
				throw new ToolException("lat and lon are required");
			}
			if (name.equals("search")) {
				return search().search(str(args, "text"), lat.doubleValue(), lon.doubleValue(), locale, limit);
			}
			Set<String> topics = new java.util.LinkedHashSet<>();
			if (args.get("topics") instanceof List<?> l) {
				l.forEach(t -> topics.add(t instanceof Number n ? String.valueOf(n.intValue()) : String.valueOf(t)));
			}
			double r = args.get("radius_km") instanceof Number n ? n.doubleValue() : 3;
			return search().popular(lat.doubleValue(), lon.doubleValue(), r, topics, locale, limit);
		} catch (McpSearch.SearchException e) {
			throw new ToolException(e.getMessage());
		}
	}

	private McpRoutes routes() {
		McpRoutes r = routes;
		if (r == null) {
			r = new McpRoutes(routingService, osmAndMapsService, webGpxParser, routingController.routingParams().getBody(),
					serverApi);
			routes = r;
		}
		return r;
	}

	private McpRoutes.Built buildRoute(Map<?, ?> args) throws Exception {
		try {
			return routes().build(args.get("points") instanceof List<?> l ? l : null, str(args, "profile"),
					args.get("params") instanceof Map<?, ?> m ? m : null);
		} catch (McpRoutes.RouteException e) {
			throw new ToolException(e.getMessage());
		}
	}

	private Map<String, Object> createTrack(OAuthGrant grant, Map<?, ?> args) throws Exception {
		String name = str(args, "name");
		if (name == null || !name.toLowerCase().endsWith(".gpx")) {
			throw new ToolException("name must end with .gpx");
		}
		checkFileAccess(grant, "GPX", name, Access.WRITE);
		McpRoutes.Built b = buildRoute(args);
		String title = str(args, "title");
		if (title == null) {
			title = name.substring(name.lastIndexOf('/') + 1, name.length() - 4);
		}
		String gpx = routes().toGpx(b, title, str(args, "description"));
		Map<String, Object> res = routes().summary(b, false);
		res.put("saved", writeFile(grant, "GPX", name, gpx));
		res.put("name", name);
		return res;
	}

	private static Number num(Map<?, ?> args, String key, Number def) throws ToolException {
		Object v = args.get(key);
		if (v == null) {
			return def;
		}
		if (v instanceof Number n) {
			return n;
		}
		try {
			return Double.parseDouble(v.toString());
		} catch (NumberFormatException e) {
			throw new ToolException(key + " must be a number");
		}
	}

	private static Long time(Map<?, ?> args, String key) throws ToolException {
		String v = str(args, key);
		try {
			return v == null ? null : java.time.Instant.parse(v).toEpochMilli();
		} catch (java.time.format.DateTimeParseException e) {
			throw new ToolException(key + " must be an ISO time like 2025-12-28T13:40:00Z");
		}
	}

	// by the millisecond time list_versions shows (stored timestamps may be more precise)
	private UserFile findVersion(int userId, String type, String name, long version) {
		for (UserFileNoData f : userdataService.generateFiles(userId, name, true, false, Set.of(type)).allFiles) {
			if (f.updatetime != null && f.updatetime.getTime() == version) {
				return filesRepository.findById(f.id).orElse(null);
			}
		}
		return null;
	}

	private List<Map<String, Object>> listVersions(OAuthGrant grant, String type, String name) throws ToolException {
		checkFileAccess(grant, type, name, Access.READ);
		List<Map<String, Object>> res = new ArrayList<>();
		for (UserFileNoData f : userdataService.generateFiles(grant.userid, name, true, false, Set.of(type)).allFiles) {
			if (res.size() >= MAX_ITEMS) {
				break;
			}
			Map<String, Object> m = new LinkedHashMap<>();
			m.put("version", f.updatetime != null ? f.updatetime.getTime() : null);
			m.put("updated", iso(f.updatetime));
			m.put("size", f.filesize);
			m.put("deleted", f.filesize < 0);
			res.add(m);
		}
		return res;
	}

	private Map<String, Object> listFavorites(OAuthGrant grant, String onlyGroup) throws Exception {
		if (!can(grant, CloudGroup.FAVORITES, Access.READ)) {
			throw new ToolException("Access to favorites was not granted");
		}
		Map<String, Object> groups = new LinkedHashMap<>();
		int count = 0;
		for (UserFileNoData f : userdataService.generateFiles(grant.userid, null, false, false,
				Set.of(FavoriteService.FILE_TYPE_FAVOURITES)).uniqueFiles) {
			UserFile uf = f.filesize < 0 ? null
					: userdataService.getLastFileVersion(grant.userid, f.name, FavoriteService.FILE_TYPE_FAVOURITES);
			GpxFile gpx = uf == null || uf.filesize < 0 ? null : shareFileService.getFile(uf);
			if (gpx == null) {
				continue;
			}
			for (WptPt p : gpx.getPointsList()) {
				String group = p.getCategory() == null ? "" : p.getCategory();
				if (onlyGroup != null && !onlyGroup.equals(group)) {
					continue;
				}
				if (count++ >= MAX_ITEMS) {
					groups.put("truncated", "only the first " + MAX_ITEMS + " points are returned");
					return groups;
				}
				Map<String, Object> pt = new LinkedHashMap<>();
				pt.put("name", p.getName());
				pt.put("lat", p.getLat());
				pt.put("lon", p.getLon());
				if (p.getDesc() != null) {
					pt.put("description", p.getDesc());
				}
				@SuppressWarnings("unchecked")
				List<Object> list = (List<Object>) groups.computeIfAbsent(group, k -> new ArrayList<>());
				list.add(pt);
			}
		}
		return groups;
	}

	private Map<String, Object> writeFile(OAuthGrant grant, String type, String name, String content) throws Exception {
		checkFileAccess(grant, type, name, Access.WRITE);
		if (content == null) {
			throw new ToolException("content is required");
		}
		byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
		if (bytes.length > MAX_WRITE_SIZE) {
			throw new ToolException("Content is larger than " + MAX_WRITE_SIZE + " bytes");
		}
		validateContent(type, name, bytes);
		CloudUserDevice dev = webDevice(grant.userid);
		InternalZipFile zip = InternalZipFile.buildFromBytes(bytes);
		userdataService.validateUserForUpload(dev, type, zip.getSize());
		userdataService.uploadFile(zip, dev, name, type, System.currentTimeMillis());
		LOG.info("MCP write " + type + " " + name + " user " + grant.userid + " client " + grant.clientid);
		UserFile uf = userdataService.getLastFileVersion(grant.userid, name, type);
		return Map.of("status", "ok", "version", uf != null ? uf.updatetime.getTime() : 0, "size", bytes.length);
	}

	private Map<String, Object> deleteFile(OAuthGrant grant, String type, String name) throws ToolException {
		checkFileAccess(grant, type, name, Access.WRITE);
		UserFile uf = userdataService.getLastFileVersion(grant.userid, name, type);
		if (uf == null || uf.filesize < 0) {
			throw new ToolException("File not found");
		}
		userdataService.deleteFile(name, type, null, System.currentTimeMillis(), webDevice(grant.userid));
		LOG.info("MCP delete " + type + " " + name + " user " + grant.userid + " client " + grant.clientid);
		return Map.of("status", "deleted", "previousVersion", uf.updatetime.getTime());
	}

	// ---------- helpers ----------

	private static class ToolException extends Exception {
		private static final long serialVersionUID = 1L;

		ToolException(String message) {
			super(message);
		}
	}

	private void checkFileAccess(OAuthGrant grant, String type, String name, Access access) throws ToolException {
		if (type == null || name == null) {
			throw new ToolException("type and name are required");
		}
		if (name.isEmpty() || name.length() > MAX_NAME_LENGTH || name.startsWith("/") || name.contains("..")
				|| name.chars().anyMatch(Character::isISOControl)) {
			throw new ToolException("Invalid file name");
		}
		CloudGroup g = CloudGroup.ofType(type);
		if (access == Access.WRITE && !g.types.contains(type)) {
			throw new ToolException("Unknown file type " + type);
		}
		if (!can(grant, g, access)) {
			throw new ToolException("Access to " + g.key + (access == Access.WRITE ? OAuthService.WRITE : OAuthService.READ)
					+ " was not granted");
		}
	}

	private void validateContent(String type, String name, byte[] bytes) throws ToolException {
		String lower = name.toLowerCase();
		if (lower.endsWith(".json") || lower.endsWith(".info")) {
			try {
				new JsonParser().parse(new String(bytes, StandardCharsets.UTF_8));
			} catch (RuntimeException e) {
				throw new ToolException("Content is not valid JSON: " + e.getMessage());
			}
		} else if (lower.endsWith(".gpx")) {
			// GpxUtilities is lenient with garbage, so check the XML first
			try {
				javax.xml.parsers.DocumentBuilderFactory f = javax.xml.parsers.DocumentBuilderFactory.newInstance();
				f.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
				f.setNamespaceAware(true);
				org.w3c.dom.Element root = f.newDocumentBuilder().parse(new ByteArrayInputStream(bytes)).getDocumentElement();
				if (!"gpx".equals(root.getLocalName())) {
					throw new ToolException("Content is not valid GPX: root element is " + root.getLocalName());
				}
			} catch (ToolException e) {
				throw e;
			} catch (Exception e) {
				throw new ToolException("Content is not valid GPX: " + e.getMessage());
			}
			try (Source source = new Buffer().readFrom(new ByteArrayInputStream(bytes))) {
				GpxFile gpx = GpxUtilities.INSTANCE.loadGpxFile(source);
				if (gpx.getError() != null) {
					throw new ToolException("Content is not valid GPX: " + gpx.getError().getMessage());
				}
			} catch (ToolException e) {
				throw e;
			} catch (Exception e) {
				throw new ToolException("Content is not valid GPX: " + e.getMessage());
			}
		}
	}

	// writes are stored as made by the user's web session device (it exists, the user signed in on the web to connect)
	private CloudUserDevice webDevice(int userId) throws ToolException {
		CloudUserDevice dev = devicesRepository.findTopByUseridAndDeviceidOrderByUdpatetimeDesc(userId,
				UserdataService.TOKEN_DEVICE_WEB);
		if (dev == null) {
			throw new ToolException("Sign in to OsmAnd Cloud on the web once to allow changes");
		}
		return dev;
	}

	private InputStream openStream(UserFile uf) {
		return uf.data != null ? new ByteArrayInputStream(uf.data) : userdataService.getInputStream(uf);
	}

	private boolean isAvailable(OAuthGrant grant, Tool t) {
		if (OPEN_TOOLS.contains(t.name)) {
			return true;
		}
		if (t.name.equals("create_track")) {
			return can(grant, CloudGroup.TRACKS, Access.WRITE);
		}
		if (t.access == Access.NONE) {
			return hasScope(grant, OAuthService.SCOPE_ACCOUNT_READ);
		}
		if (t.name.equals("list_favorites")) {
			return can(grant, CloudGroup.FAVORITES, Access.READ);
		}
		return Arrays.stream(CloudGroup.values()).anyMatch(g -> can(grant, g, t.access));
	}

	private static boolean can(OAuthGrant grant, CloudGroup g, Access access) {
		return hasScope(grant, g.key + (access == Access.WRITE ? OAuthService.WRITE : OAuthService.READ));
	}

	private static boolean hasScope(OAuthGrant grant, String scope) {
		return grant.scope != null && Arrays.asList(grant.scope.split(" ")).contains(scope);
	}

	private static String str(Map<?, ?> args, String key) {
		Object v = args.get(key);
		return v == null ? null : v.toString();
	}

	private static String iso(Date d) {
		return d != null ? d.toInstant().toString() : null;
	}

	private static String groupKeys() {
		return String.join(", ", Arrays.stream(CloudGroup.values()).map(g -> g.key).toList());
	}

	private static Map<String, Object> schema(Map<String, Object> properties, List<String> required) {
		return required.isEmpty() ? Map.of("type", "object", "properties", properties)
				: Map.of("type", "object", "properties", properties, "required", required);
	}

	private static Map<String, Object> toolResult(JsonElement id, String text, boolean isError) {
		return result(id, Map.of("content", List.of(Map.of("type", "text", "text", text)), "isError", isError));
	}

	private static Map<String, Object> result(JsonElement id, Object result) {
		Map<String, Object> m = new LinkedHashMap<>();
		m.put("jsonrpc", "2.0");
		m.put("id", id);
		m.put("result", result);
		return m;
	}

	private static Map<String, Object> error(JsonElement id, int code, String message) {
		Map<String, Object> m = new LinkedHashMap<>();
		m.put("jsonrpc", "2.0");
		m.put("id", id);
		m.put("error", Map.of("code", code, "message", message));
		return m;
	}
}
