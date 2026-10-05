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
import org.springframework.web.bind.annotation.RestController;

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
import net.osmand.server.api.services.ShareFileService;
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
	// guide for assistants, kept in web-server-config to edit without a server release
	private static final String GUIDE_FILE = "api/mcp_guide.md";
	private static final String GUIDE_URI = "osmand://guide";

	@Autowired
	private OAuthService oauthService;

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

	@Value("${osmand.web.location}")
	private String websiteLocation;

	private final Gson gson = new GsonBuilder().disableHtmlEscaping().create();

	private enum Access { NONE, READ, WRITE }

	private record Tool(String name, Access access, String description, Map<String, Object> inputSchema) {
	}

	private static final Map<String, Object> TYPE_ARG = Map.of("type", "string",
			"description", "Cloud file type as returned by list_files, e.g. GPX, FAVOURITES, PROFILE");
	private static final Map<String, Object> NAME_ARG = Map.of("type", "string",
			"description", "File name with folder as returned by list_files");

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
						+ "Call get_guide before writing files. Ask the user before write_file or delete_file.");
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

	private String readFile(OAuthGrant grant, String type, String name, Object version) throws Exception {
		checkFileAccess(grant, type, name, Access.READ);
		UserFile uf = version instanceof Number n ? findVersion(grant.userid, type, name, n.longValue())
				: userdataService.getLastFileVersion(grant.userid, name, type);
		if (uf == null) {
			throw new ToolException("File not found");
		}
		if (uf.filesize < 0) {
			throw new ToolException("This version is a deletion; use list_versions for earlier versions");
		}
		if (uf.filesize > MAX_READ_SIZE) {
			throw new ToolException("File is too large to read (" + uf.filesize + " bytes)");
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
		if (t.name.equals("get_guide")) {
			return true;
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
