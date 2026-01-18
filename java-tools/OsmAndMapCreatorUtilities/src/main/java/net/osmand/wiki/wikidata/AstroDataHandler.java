package net.osmand.wiki.wikidata;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import net.osmand.PlatformUtil;
import org.apache.commons.logging.Log;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

public class AstroDataHandler {
	private record Quantity(double amount, String unitQid) {
	}

	private record PhysicalProperties(Double radius, Double distance, Double mass) {
	}

	private record CatalogEntry(String code, String catalogQid, String catalogName) {
	}

	private record EntityData(
			long qid,
			Map<String, Object> map,
			ArticleMapper.Article article,
			PhysicalProperties physics,
			List<CatalogEntry> catalogs,
			Map<String, List<Map<String, JsonElement>>> claims,
			Map<String, String> descriptions,
			String json
	) {
		public Map<String, String> getSiteLinks() {
			Map<String, String> wiki = new HashMap<>();
			for (ArticleMapper.SiteLink link : article.getSiteLinks()) {
				wiki.put(link.lang(), link.title());
			}
			return wiki;
		}
	}

	private static class NameAgg {
		final String display;
		final Set<String> sources = new LinkedHashSet<>();

		private NameAgg(String display) {
			this.display = display;
		}
	}

	private static final Log log = PlatformUtil.getLog(AstroDataHandler.class);
	private static final String ASTRO_JSON_DIR_ENV = "ASTRO_JSON_DIR";
	private static final List<String> INPUT_FILES = List.of(
			"solar_system.json",
			"galaxies.json",
			"black_holes.json",
			"constellations.json",
			"stars.json",
			"open_clusters.json",
			"globular_clusters.json",
			"nebulae.json",
			"galaxy_clusters.json"
	);
	private static final double MAGNITUDE_ONLY_EN = 3.0;
	private static final String PROP_RADIUS = "P2120";
	private static final String PROP_DISTANCE = "P2583";
	private static final String PROP_MASS = "P2067";
	private static final String PROP_CATALOG = "P528";
	private static final String PROP_CATALOG_REF = "P972";
	private static final Map<String, String> KEEP_CATALOGS = Map.of(
			"Q14530", "Messier",
			"Q857461", "Caldwell",
			"Q2661779", "Collinder",
			"Q55712879", "Supernova Catalog",
			"Q3247327", "Barnard",
			"Q91442269", "Trumpler catalogue",
			"Q4999741", "Burnham"
	);
	private static final double LY_TO_PC = 0.306601;
	private static final double M_TO_PC = 3.24078e-17;
	private static final double AU_TO_PC = 4.84814e-6;
	private static final double KG_TO_SOLAR = 5.029e-31;
	private static final double YOTTAGRAM_TO_SOLAR = 1e21 * KG_TO_SOLAR;
	private static final Map<String, Double> CONVERSIONS_MASS = Map.of(
			"Q180892", 1.0,
			"Q11570", KG_TO_SOLAR,
			"Q613726", YOTTAGRAM_TO_SOLAR
	);
	private static final double M_TO_SOLAR_R = 1.4374e-9;
	private static final double KM_TO_SOLAR_R = 1.4374e-6;
	private static final Map<String, Double> CONVERSIONS_DISTANCE = Map.of(
			"Q12129", 1.0,
			"Q531", LY_TO_PC,
			"Q11573", M_TO_PC,
			"Q828224", M_TO_PC * 1000,
			"Q1811", AU_TO_PC
	);
	private static final Map<String, Double> CONVERSIONS_RADIUS = Map.of(
			"Q48440", 1.0,
			"Q11573", M_TO_SOLAR_R,
			"Q828224", KM_TO_SOLAR_R,
			"Q1811", (AU_TO_PC / M_TO_PC) * M_TO_SOLAR_R
	);
	private static final String ASTRO_MAPPING_TYPE_LABEL = "label";
	private static final String ASTRO_MAPPING_TYPE_SITELINK = "sitelink";
	private static final String ASTRO_MAPPING_TYPE_DESCRIPTION = "description";

	private final ObjectMapper objectMapper = new ObjectMapper();
	private final Map<Long, Map<String, Object>> items;
	private final List<EntityData> entities = new ArrayList<>();

	public AstroDataHandler() throws IOException {
		this.items = getAstroMap(resolveAstroInputDir());
	}

	private static Path resolveAstroInputDir() throws IOException {
		String astroJsonDir = System.getenv(ASTRO_JSON_DIR_ENV);
		Path dir;
		if (astroJsonDir != null && !astroJsonDir.isBlank()) {
			dir = Path.of(astroJsonDir).toAbsolutePath().normalize();
		} else {
			throw new IOException("Environment variable " + ASTRO_JSON_DIR_ENV + " is not set!");
		}
		if (!Files.isDirectory(dir)) {
			throw new IOException("Astro JSON directory not found or not a directory: " + dir
					+ " (set " + ASTRO_JSON_DIR_ENV + " env var to override)");
		}
		return dir;
	}

	private static String normalizeNameKey(String s) {
		if (s == null) {
			return null;
		}
		String t = s.trim();
		if (t.isEmpty()) {
			return null;
		}
		return t.toLowerCase(Locale.ROOT);
	}

	private static String stripExtension(String fileName) {
		int ind = fileName.lastIndexOf('.');
		return ind >= 0 ? fileName.substring(0, ind) : fileName;
	}

	private static String asString(Object o) {
		return o == null ? null : String.valueOf(o);
	}

	private static Double asDouble(Object o) {
		if (o == null) {
			return null;
		}
		if (o instanceof Number n) {
			return n.doubleValue();
		}
		try {
			return Double.parseDouble(String.valueOf(o));
		} catch (NumberFormatException e) {
			return null;
		}
	}

	private static Integer asInt(Object o) {
		if (o == null) {
			return null;
		}
		if (o instanceof Number n) {
			return n.intValue();
		}
		try {
			return Integer.parseInt(String.valueOf(o));
		} catch (NumberFormatException e) {
			return null;
		}
	}

	private static Long asLong(Object o) {
		if (o == null) {
			return null;
		}
		if (o instanceof Number n) {
			return n.longValue();
		}
		try {
			return Long.parseLong(String.valueOf(o));
		} catch (NumberFormatException e) {
			return null;
		}
	}

	private Map<Long, Map<String, Object>> getAstroMap(Path inputDir) throws IOException {
		Map<Long, Map<String, Object>> astroMap = new LinkedHashMap<>();
		for (String fileName : INPUT_FILES) {
			String groupKey = stripExtension(fileName);
			Path filePath = inputDir.resolve(fileName);
			if (!Files.exists(filePath)) {
				throw new IOException("File not found: " + filePath);
			}

			log.info("Processing " + groupKey + "...");
			List<Map<String, Object>> items = readJsonList(filePath);
			for (Map<String, Object> item : items) {
				String wid = asString(item.get("wid"));
				if (wid == null || wid.isBlank()) {
					continue;
				}
				Long qid = asLong(wid.substring(1));

				item.put("astro_group", groupKey);
				astroMap.put(qid, item);
			}
		}
		return astroMap;
	}

	public void addItem(long qid, ArticleMapper.Article article, String json) {
		if (json == null || json.isBlank()) {
			return;
		}

		Map<String, Object> map = items.get(qid);
		if (map != null) {
			JsonObject obj = com.google.gson.JsonParser.parseString(json).getAsJsonObject();
			EntityData entityData = processEntityData(qid, map, article, json, parseClaims(obj), parseDescriptions(obj));
			entities.add(entityData);
		}
	}

	public void run(Connection conn) throws SQLException {
		log.info("Initializing Astro tables ...");
		initDb(conn);

		int inserted = 0;
		try (PreparedStatement insertObject = conn.prepareStatement(
				"INSERT OR REPLACE INTO Objects (id, name, type, ra, dec, lines, mag, centerwid, radius, distance, mass, hip, json, image) "
						+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")) {
			try (PreparedStatement insertCatalog = conn.prepareStatement(
					"INSERT OR IGNORE INTO Catalogs (catalogWid, catalogName) VALUES (?, ?)")) {
				try (PreparedStatement upsertCatalogId = conn.prepareStatement(
						"INSERT OR REPLACE INTO CatalogIds (id, catalogWid, catalogId) VALUES (?, ?, ?)")) {
					try (PreparedStatement insertName = conn.prepareStatement(
							"INSERT OR IGNORE INTO Names (id, name, type) VALUES (?, ?, ?)")) {
						try (PreparedStatement deleteAstroMappings = conn.prepareStatement(
								"DELETE FROM AstroMappings WHERE id = ?")) {
							try (PreparedStatement upsertAstroMapping = conn.prepareStatement(
									"INSERT OR REPLACE INTO AstroMappings (id, type, lang, value) VALUES (?, ?, ?, ?)")) {

								for (EntityData entityData : entities) {
									if (entityData.article == null) {
										continue;
									}

									insertObject(insertObject, entityData);

									deleteAstroMappings.setLong(1, entityData.qid);
									deleteAstroMappings.execute();
									upsertAstroMappings(upsertAstroMapping, entityData);

									inserted++;
									for (CatalogEntry c : entityData.catalogs) {
										insertCatalog.setString(1, c.catalogQid);
										insertCatalog.setString(2, c.catalogName);
										insertCatalog.execute();

										upsertCatalogId.setLong(1, entityData.qid);
										upsertCatalogId.setString(2, c.catalogQid);
										upsertCatalogId.setString(3, c.code);
										upsertCatalogId.execute();
									}

									insertNames(insertName, entityData);
								}
							}
						}
					}
				}
			}
		}
		conn.commit();
		log.info("Total Astro entities: " + items.size() + " (" + inserted + " inserted)");

		String placeholders = KEEP_CATALOGS.keySet().stream()
				.map(qid -> "'" + qid + "'")
				.reduce((a, b) -> a + "," + b)
				.orElse("");
		try (Statement st = conn.createStatement()) {
			st.execute("DELETE FROM CatalogIds WHERE catalogWid NOT IN (" + placeholders + ")");
			st.execute("CREATE INDEX IF NOT EXISTS idx_ids_catalog_nocase ON CatalogIds (catalogId COLLATE NOCASE)");
		}
		conn.commit();

		boolean previousAutoCommit = conn.getAutoCommit();
		conn.setAutoCommit(true);
		try (Statement st = conn.createStatement()) {
			st.execute("PRAGMA journal_mode = DELETE");
			st.execute("PRAGMA page_size = 4096");
			st.execute("VACUUM");
		} finally {
			conn.setAutoCommit(previousAutoCommit);
		}
	}

	private void initDb(Connection conn) throws SQLException {
		try (Statement st = conn.createStatement()) {
			st.execute("""
					CREATE TABLE IF NOT EXISTS Objects (
						id BIGINT, 
						name TEXT, 
						type TEXT, 
						ra REAL, 
						dec REAL,
						lines TEXT,
						centerwid TEXT,
						mag REAL,
						radius REAL,
						distance REAL,
						mass REAL, 
						hip INTEGER, 
						json TEXT, 
						image TEXT, 
					PRIMARY KEY (id, name, type))""");

			st.execute("""
					CREATE TABLE IF NOT EXISTS Names (
						id BIGINT, 
						name TEXT, 
						type TEXT, 
						PRIMARY KEY (id, type)
					) WITHOUT ROWID""");

			st.execute("""
					CREATE TABLE IF NOT EXISTS Catalogs (
						catalogWid TEXT PRIMARY KEY,
						catalogName TEXT
					) WITHOUT ROWID""");

			st.execute("""
					CREATE TABLE IF NOT EXISTS CatalogIds (
						id BIGINT,
						catalogWid TEXT,
						catalogId TEXT,
						PRIMARY KEY (id, catalogWid)
					) WITHOUT ROWID
					""");

			st.execute("""
					CREATE TABLE IF NOT EXISTS AstroMappings (
						id BIGINT,
						type TEXT,
						lang TEXT,
						value TEXT,
						PRIMARY KEY (id, type, lang)
					) WITHOUT ROWID""");

			st.execute("CREATE INDEX IF NOT EXISTS idx_astro_mappings_type_lang ON AstroMappings (type, lang)");
			st.execute("CREATE INDEX IF NOT EXISTS idx_objects_name_nocase ON Objects (name COLLATE NOCASE)");
			st.execute("CREATE INDEX IF NOT EXISTS idx_names_name_nocase ON Names (name COLLATE NOCASE)");
			st.execute("CREATE INDEX IF NOT EXISTS idx_ids_catalog_nocase ON CatalogIds (catalogId COLLATE NOCASE)");
		}
		conn.setAutoCommit(false);
	}

	private void insertObject(PreparedStatement insertObject, EntityData data) throws SQLException {
		PhysicalProperties physics = data.physics;
		Map<String, Object> item = data.map;
		int ind = 0;
		insertObject.setLong(++ind, data.qid);
		insertObject.setString(++ind, asString(item.get("name")));
		insertObject.setString(++ind, asString(item.get("astro_group")));
		insertObject.setObject(++ind, asDouble(item.get("ra")));
		insertObject.setObject(++ind, asDouble(item.get("dec")));

		Object lines = item.get("lines");
		insertObject.setString(++ind, lines != null ? writeJsonSilently(lines) : null);

		insertObject.setObject(++ind, asDouble(item.get("mag")));
		insertObject.setString(++ind, asString(item.get("centerwid")));
		insertObject.setObject(++ind, physics.radius);
		insertObject.setObject(++ind, physics.distance);
		insertObject.setObject(++ind, physics.mass);
		insertObject.setObject(++ind, asInt(item.get("hip")));
		insertObject.setString(++ind, data.json);
		insertObject.setString(++ind, asString(data.article.getImage()));
		insertObject.execute();
	}

	private void insertNames(PreparedStatement insertName, EntityData data) throws SQLException {
		String groupKey = asString(data.map.get("astro_group"));
		Map<String, Object> item = data.map;
		Map<String, String> labels = data.article.getLabels();
		Map<String, String> siteLinks = data.getSiteLinks();

		Double mag = asDouble(item.get("mag"));
		boolean allLangs = !Objects.equals(groupKey, "stars") || (mag == null ? 99.0 : mag) <= MAGNITUDE_ONLY_EN;

		Map<String, NameAgg> nameData = new HashMap<>();

		for (Map.Entry<String, String> e : labels.entrySet()) {
			String lang = e.getKey();
			String val = e.getValue();
			if (val == null) {
				continue;
			}
			if (!lang.equals("en") && !allLangs) {
				continue;
			}
			String key = normalizeNameKey(val);
			if (key == null) {
				continue;
			}
			nameData.computeIfAbsent(key, k -> new NameAgg(val.trim())).sources.add(lang);
		}

		for (Map.Entry<String, String> e : siteLinks.entrySet()) {
			String site = e.getKey();
			String title = e.getValue();
			if (title == null) {
				continue;
			}
			if (!(site.length() == 6 && site.endsWith("wiki"))) {
				continue;
			}
			if (!site.equals("en") && !allLangs) {
				continue;
			}
			String key = normalizeNameKey(title);
			if (key == null) {
				continue;
			}
			nameData.computeIfAbsent(key, k -> new NameAgg(title.trim())).sources.add(site);
		}

		for (NameAgg agg : nameData.values()) {
			String typesStr = String.join(",", agg.sources.stream().sorted().toList());
			insertName.setLong(1, data.qid);
			insertName.setString(2, agg.display);
			insertName.setString(3, typesStr);
			insertName.execute();
		}
	}

	private void upsertAstroMappings(PreparedStatement upsertAstroMapping, EntityData entityData) throws SQLException {
		Map<String, String> labels = entityData.article.getLabels();
		if (labels != null) {
			for (Map.Entry<String, String> e : labels.entrySet()) {
				String lang = e.getKey();
				String value = e.getValue();
				if (lang == null || lang.isBlank() || value == null || value.isBlank()) {
					continue;
				}
				upsertAstroMapping.setLong(1, entityData.qid);
				upsertAstroMapping.setString(2, ASTRO_MAPPING_TYPE_LABEL);
				upsertAstroMapping.setString(3, lang);
				upsertAstroMapping.setString(4, value);
				upsertAstroMapping.execute();
			}
		}
		for (ArticleMapper.SiteLink siteLink : entityData.article.getSiteLinks()) {
			String lang = siteLink.lang();
			String title = siteLink.title();
			if (lang == null || lang.isBlank() || title == null || title.isBlank()) {
				continue;
			}
			upsertAstroMapping.setLong(1, entityData.qid);
			upsertAstroMapping.setString(2, ASTRO_MAPPING_TYPE_SITELINK);
			upsertAstroMapping.setString(3, lang);
			upsertAstroMapping.setString(4, title);
			upsertAstroMapping.execute();
		}

		Map<String, String> descriptions =  entityData.descriptions;
		if (descriptions != null) {
			for (Map.Entry<String, String> e : descriptions.entrySet()) {
				String lang = e.getKey();
				String value = e.getValue();
				if (lang == null || lang.isBlank() || value == null || value.isBlank()) {
					continue;
				}
				upsertAstroMapping.setLong(1, entityData.qid);
				upsertAstroMapping.setString(2, ASTRO_MAPPING_TYPE_DESCRIPTION);
				upsertAstroMapping.setString(3, lang);
				upsertAstroMapping.setString(4, value);
				upsertAstroMapping.execute();
			}
		}
	}

	private Map<String, List<Map<String, JsonElement>>> parseClaims(JsonObject obj) {
		Map<String, List<Map<String, JsonElement>>> claimsMap = new HashMap<>();
		try {
			JsonObject claims = (JsonObject) obj.get("claims");
			claims.keySet().forEach(key -> {
				JsonArray array = claims.getAsJsonArray(key);
				List<Map<String, JsonElement>> list = new ArrayList<>();
				for (int i = 0; i < array.size(); i++) {
					Map<String, JsonElement> item = new HashMap<>();
					array.get(i).getAsJsonObject().entrySet().forEach(entry -> {
						item.put(entry.getKey(), entry.getValue());
					});
					list.add(item);
				}
				claimsMap.put(key, list);
			});
		} catch (Exception e) {
			return null;
		}
		return claimsMap;
	}

	private Map<String, String> parseDescriptions(JsonObject obj) {
		try {
			JsonElement el = obj.get("descriptions");
			if (el == null || !el.isJsonObject()) {
				return null;
			}
			JsonObject descriptions = el.getAsJsonObject();
			Map<String, String> result = new LinkedHashMap<>();
			for (Map.Entry<String, JsonElement> entry : descriptions.entrySet()) {
				JsonObject v = entry.getValue().isJsonObject() ? entry.getValue().getAsJsonObject() : null;
				if (v == null) {
					continue;
				}
				JsonElement langEl = v.get(ArticleMapper.LANGUAGE_KEY);
				JsonElement valueEl = v.get(ArticleMapper.VALUE_KEY);
				if (langEl == null || valueEl == null) {
					continue;
				}
				String lang = langEl.getAsString();
				String value = valueEl.getAsString();
				if (lang == null || lang.isBlank() || value == null || value.isBlank()) {
					continue;
				}
				result.put(lang, value);
			}
			return result;
		} catch (Exception e) {
			return null;
		}
	}

	private List<Map<String, Object>> readJsonList(Path filePath) throws IOException {
		try (InputStream is = Files.newInputStream(filePath)) {
			return objectMapper.readValue(is, new TypeReference<>() {
			});
		}
	}

	private EntityData processEntityData(long qid, Map<String, Object> map, ArticleMapper.Article article, String json,
	                                     Map<String, List<Map<String, JsonElement>>> claims, Map<String, String> descriptions) {
		if (article == null) {
			return new EntityData(qid, map, null, new PhysicalProperties(null, null, null), List.of(), claims, descriptions, json);
		}

		PhysicalProperties physics = new PhysicalProperties(
				getPhysicalProperty(claims, PROP_RADIUS, CONVERSIONS_RADIUS),
				getPhysicalProperty(claims, PROP_DISTANCE, CONVERSIONS_DISTANCE),
				getPhysicalProperty(claims, PROP_MASS, CONVERSIONS_MASS)
		);

		List<CatalogEntry> catalogs = extractCatalogs(claims, article.getLabels());
		return new EntityData(qid, map, article, physics, catalogs, claims, descriptions, json);
	}

	private Double getPhysicalProperty(Map<String, List<Map<String, JsonElement>>> claims, String propId, Map<String, Double> conversionMap) {
		if (claims == null) {
			return null;
		}

		List<Map<String, JsonElement>> list = claims.get(propId);
		if (list == null) {
			return null;
		}
		for (Map<String, JsonElement> claim : list) {
			String rank = claim.get("rank").getAsString();
			if ("deprecated".equals(rank)) {
				continue;
			}
			JsonObject valData = (JsonObject) ((JsonObject) ((JsonObject) claim.get("mainsnak")).get("datavalue")).get("value");
			if (valData == null) {
				continue;
			}

			Quantity q = parseQuantity(valData);
			if (q == null || q.unitQid == null || q.unitQid.equals("1")) {
				continue;
			}
			Double mult = conversionMap.get(q.unitQid);
			if (mult == null) {
				continue;
			}
			return q.amount * mult;
		}
		return null;
	}

	private Quantity parseQuantity(JsonObject valueNode) {
		try {
			String amountStr = valueNode.get("amount").getAsString();
			if (amountStr == null) {
				return null;
			}
			double amount = Double.parseDouble(amountStr);

			String unitUrl = valueNode.get("unit").getAsString();
			String unitQid = null;
			int ind = unitUrl.lastIndexOf("entity/");
			if (ind >= 0) {
				unitQid = unitUrl.substring(ind + "entity/".length());
			}
			return new Quantity(amount, unitQid);
		} catch (Exception e) {
			return null;
		}
	}

	private List<CatalogEntry> extractCatalogs(Map<String, List<Map<String, JsonElement>>> claims, Map<String, String> labels) {
		List<Map<String, JsonElement>> list = claims.get(PROP_CATALOG);
		if (list == null) {
			return List.of();
		}

		List<CatalogEntry> results = new ArrayList<>();
		for (Map<String, JsonElement> claim : list) {
			String rank = claim.get("rank").getAsString();
			if ("deprecated".equals(rank)) {
				continue;
			}

			String code = ((JsonObject) ((JsonObject) claim.get("mainsnak")).get("datavalue")).get("value").getAsString();
			if (code == null) {
				continue;
			}

			JsonArray qualifiers = (JsonArray) ((JsonObject) claim.get("qualifiers")).get(PROP_CATALOG_REF);
			String catQid = null;
			if (qualifiers != null && !qualifiers.isEmpty()) {
				JsonElement qVal = ((JsonObject) ((JsonObject) qualifiers.get(0)).get("datavalue")).get("value");
				if (qVal != null && qVal.isJsonObject()) {

					JsonElement numericId = ((JsonObject) qVal).get("numeric-id");
					JsonElement id = ((JsonObject) qVal).get("id");
					if (numericId != null) {
						catQid = "Q" + numericId.getAsString();
					} else if (id != null) {
						catQid = id.getAsString();
					}
				} else if (qVal.isJsonPrimitive()) {
					String v = qVal.getAsString();
					catQid = v.replace("http://www.wikidata.org/entity/", "");
				}
			}

			if (catQid == null || catQid.isBlank()) {
				continue;
			}

			String catName = labels.get("en");
			results.add(new CatalogEntry(code, catQid, catName));
		}
		return results;
	}

	private String writeJsonSilently(Object v) {
		try {
			return objectMapper.writeValueAsString(v);
		} catch (JsonProcessingException e) {
			return null;
		}
	}
}
