package net.osmand.wiki.wikidata;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import net.osmand.PlatformUtil;
import net.osmand.util.Algorithms;
import org.apache.commons.logging.Log;

import java.io.IOException;
import java.io.InputStream;
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
			Map<String, Object> astroParams,
			ArticleMapper.Article article,
			PhysicalProperties physics,
			List<CatalogEntry> catalogs,
			Map<String, List<Map<String, JsonElement>>> claims,
			Map<String, String> descriptions,
			String json
	) {
		public Map<String, String> getSiteLinks() {
			Map<String, String> siteLinks = new HashMap<>();
			for (ArticleMapper.SiteLink link : article.getSiteLinks()) {
				siteLinks.put(link.lang(), link.title());
			}
			return siteLinks;
		}
	}

	private static class NameAggregation {
		final String displayLabel;
		final Set<String> sources = new LinkedHashSet<>();

		private NameAggregation(String displayLabel) {
			this.displayLabel = displayLabel;
		}
	}

	private static final Log log = PlatformUtil.getLog(AstroDataHandler.class);
	private static final int WIKI_WITH_LANG_LENGTH = 6;
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
	private static final double MAGNITUDE_BRIGHT_THRESHOLD_EN_ONLY = 3.0;
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
	// https://en.wikipedia.org/wiki/Light-year to https://en.wikipedia.org/wiki/Parsec
	private static final double LY_TO_PC = 0.306601;
	// https://en.wikipedia.org/wiki/Metre to https://en.wikipedia.org/wiki/Parsec
	private static final double M_TO_PC = 3.24078e-17;
	// https://en.wikipedia.org/wiki/Astronomical_unit to https://en.wikipedia.org/wiki/Parsec
	private static final double AU_TO_PC = 4.84814e-6;
	// https://en.wikipedia.org/wiki/Kilogram to https://en.wikipedia.org/wiki/Solar_mass
	private static final double KG_TO_SOLAR = 5.029e-31;
	// https://en.wikipedia.org/wiki/Yottagram to https://en.wikipedia.org/wiki/Solar_mass
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
		this.items = getAstroMap();
	}

	private Map<Long, Map<String, Object>> getAstroMap() throws IOException {
		Map<Long, Map<String, Object>> astroItems = new LinkedHashMap<>();
		for (String fileName : INPUT_FILES) {
			String groupKey = stripExtension(fileName);
			String resourcePath = "astro/" + fileName;

			log.info("Processing " + groupKey + "...");
			List<Map<String, Object>> items = readJsonList(resourcePath);
			for (Map<String, Object> item : items) {
				String wid = asString(item.get("wid"));
				if (wid == null || wid.isBlank()) {
					continue;
				}
				long qid = Algorithms.parseLongSilently(wid.substring(1), 0L);
				if (qid == 0L) {
					continue;
				}
				item.put("astro_group", groupKey);
				astroItems.put(qid, item);
			}
		}
		return astroItems;
	}

	public void addItem(long qid, ArticleMapper.Article article, String json) {
		if (article == null) {
			return;
		}

		Map<String, Object> astroParams = items.get(qid);
		if (astroParams != null) {
			JsonObject obj = com.google.gson.JsonParser.parseString(json).getAsJsonObject();
			EntityData entityData = processEntityData(qid, astroParams, article, json, parseClaims(obj), parseDescriptions(obj));
			entities.add(entityData);
		}
	}

	public void run(Connection conn) throws SQLException {
		log.info("Initializing Astro tables ...");
		initDb(conn);

		int inserted = 0;
		try (PreparedStatement insertObject = conn.prepareStatement(
				"INSERT OR REPLACE INTO astro_object (id, name, type, ra, dec, lines, mag, centerwid, radius, distance, mass, hip, json, image) "
						+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")) {
			try (PreparedStatement insertCatalog = conn.prepareStatement(
					"INSERT OR IGNORE INTO astro_catalog (catalogWid, catalogName) VALUES (?, ?)")) {
				try (PreparedStatement upsertCatalogId = conn.prepareStatement(
						"INSERT OR REPLACE INTO astro_catalog_id (id, catalogWid, catalogId) VALUES (?, ?, ?)")) {
					try (PreparedStatement insertName = conn.prepareStatement(
							"INSERT OR IGNORE INTO astro_name (id, name, type) VALUES (?, ?, ?)")) {
						try (PreparedStatement deleteAstroMappings = conn.prepareStatement(
								"DELETE FROM astro_mapping WHERE id = ?")) {
							try (PreparedStatement upsertAstroMapping = conn.prepareStatement(
									"INSERT OR REPLACE INTO astro_mapping (id, type, lang, value) VALUES (?, ?, ?, ?)")) {

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
			st.execute("DELETE FROM astro_catalog_id WHERE catalogWid NOT IN (" + placeholders + ")");
			st.execute("CREATE INDEX IF NOT EXISTS idx_ids_catalog_nocase ON astro_catalog_id (catalogId COLLATE NOCASE)");
		}
		conn.commit();

		boolean previousAutoCommit = conn.getAutoCommit();
		conn.setAutoCommit(true);
		try (Statement st = conn.createStatement()) {
			st.execute("VACUUM");
		} finally {
			conn.setAutoCommit(previousAutoCommit);
		}
	}

	private void initDb(Connection conn) throws SQLException {
		try (Statement st = conn.createStatement()) {
			st.execute("""
					CREATE TABLE IF NOT EXISTS astro_object (
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
					PRIMARY KEY (id, name, type))
					""");

			st.execute("""
					CREATE TABLE IF NOT EXISTS astro_name (
						id BIGINT, 
						name TEXT, 
						type TEXT, 
						PRIMARY KEY (id, type)
					) WITHOUT ROWID
					""");

			st.execute("""
					CREATE TABLE IF NOT EXISTS astro_catalog (
						catalogWid TEXT PRIMARY KEY,
						catalogName TEXT
					) WITHOUT ROWID
					""");

			st.execute("""
					CREATE TABLE IF NOT EXISTS astro_catalog_id (
						id BIGINT,
						catalogWid TEXT,
						catalogId TEXT,
						PRIMARY KEY (id, catalogWid)
					) WITHOUT ROWID
					""");

			st.execute("""
					CREATE TABLE IF NOT EXISTS astro_mapping (
						id BIGINT,
						type TEXT,
						lang TEXT,
						value TEXT,
						PRIMARY KEY (id, type, lang)
					) WITHOUT ROWID
					""");

			st.execute("CREATE INDEX IF NOT EXISTS idx_astro_mappings_type_lang ON astro_mapping (type, lang)");
			st.execute("CREATE INDEX IF NOT EXISTS idx_objects_name_nocase ON astro_object (name COLLATE NOCASE)");
			st.execute("CREATE INDEX IF NOT EXISTS idx_names_name_nocase ON astro_name (name COLLATE NOCASE)");
			st.execute("CREATE INDEX IF NOT EXISTS idx_ids_catalog_nocase ON astro_catalog_id (catalogId COLLATE NOCASE)");
		}
		conn.setAutoCommit(false);
	}

	private void insertObject(PreparedStatement insertObject, EntityData data) throws SQLException {
		PhysicalProperties physics = data.physics;
		Map<String, Object> item = data.astroParams;
		int ind = 0;
		insertObject.setLong(++ind, data.qid);
		insertObject.setString(++ind, asString(item.get("name")));
		insertObject.setString(++ind, asString(item.get("astro_group")));
		insertObject.setObject(++ind, Algorithms.parseDoubleSilently(asString(item.get("ra")), 0.0));
		insertObject.setObject(++ind, Algorithms.parseDoubleSilently(asString(item.get("dec")), 0.0));

		Object lines = item.get("lines");
		insertObject.setString(++ind, lines != null ? writeJsonSilently(lines) : null);

		insertObject.setObject(++ind, Algorithms.parseDoubleSilently(asString(item.get("mag")), 0.0));
		insertObject.setString(++ind, asString(item.get("centerwid")));
		insertObject.setObject(++ind, physics.radius);
		insertObject.setObject(++ind, physics.distance);
		insertObject.setObject(++ind, physics.mass);
		insertObject.setObject(++ind, Algorithms.parseLongSilently(asString(item.get("hip")), 0L));
		insertObject.setString(++ind, data.json);
		insertObject.setString(++ind, asString(data.article.getImage()));
		insertObject.execute();
	}

	private void insertNames(PreparedStatement insertName, EntityData data) throws SQLException {
		String groupKey = asString(data.astroParams.get("astro_group"));
		Map<String, Object> params = data.astroParams;
		Map<String, String> labels = data.article.getLabels();
		Map<String, String> siteLinks = data.getSiteLinks();

		double mag = Algorithms.parseDoubleSilently(asString(params.get("mag")), 0.0);
		boolean allLangs = !Objects.equals(groupKey, "stars") || (mag == 0.0 ? 99.0 : mag) <= MAGNITUDE_BRIGHT_THRESHOLD_EN_ONLY;

		Map<String, NameAggregation> nameData = new LinkedHashMap<>();

		for (Map.Entry<String, String> e : labels.entrySet()) {
			String lang = e.getKey();
			String label = e.getValue();
			if (label == null) {
				continue;
			}
			if (!allLangs && !lang.equals("en")) {
				continue;
			}
			String key = normalizeNameKey(label);
			if (key == null) {
				continue;
			}
			nameData.computeIfAbsent(key, k -> new NameAggregation(label.trim())).sources.add(lang);
		}

		for (Map.Entry<String, String> e : siteLinks.entrySet()) {
			String site = e.getKey();
			String title = e.getValue();
			if (title == null) {
				continue;
			}
			if (!(site.length() == WIKI_WITH_LANG_LENGTH && site.endsWith("wiki"))) {
				continue;
			}
			if (!allLangs && !site.equals("en")) {
				continue;
			}
			String key = normalizeNameKey(title);
			if (key == null) {
				continue;
			}
			nameData.computeIfAbsent(key, k -> new NameAggregation(title.trim())).sources.add(site);
		}

		for (NameAggregation agg : nameData.values()) {
			String typesStr = String.join(",", agg.sources.stream().sorted().toList());
			insertName.setLong(1, data.qid);
			insertName.setString(2, agg.displayLabel);
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

		Map<String, String> descriptions = entityData.descriptions;
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
		Map<String, List<Map<String, JsonElement>>> claimsMap = new LinkedHashMap<>();
		try {
			JsonObject claims = (JsonObject) obj.get("claims");
			claims.keySet().forEach(key -> {
				JsonArray array = claims.getAsJsonArray(key);
				List<Map<String, JsonElement>> list = new ArrayList<>();
				for (int i = 0; i < array.size(); i++) {
					Map<String, JsonElement> item = new LinkedHashMap<>();
					array.get(i).getAsJsonObject().entrySet().forEach(entry -> {
						item.put(entry.getKey(), entry.getValue());
					});
					list.add(item);
				}
				claimsMap.put(key, list);
			});
		} catch (Exception e) {
			log.error("Failed to parse claims", e);
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
			log.error("Failed to parse descriptions", e);
			return null;
		}
	}

	private List<Map<String, Object>> readJsonList(String resourcePath) throws IOException {
		InputStream inputStream = AstroDataHandler.class.getClassLoader().getResourceAsStream(resourcePath);
		if (inputStream == null) {
			throw new IOException("Resource not found: " + resourcePath);
		}
		try (InputStream is = inputStream) {
			return objectMapper.readValue(is, new TypeReference<>() {
			});
		}
	}

	private EntityData processEntityData(long qid, Map<String, Object> astroParams, ArticleMapper.Article article, String json,
	                                     Map<String, List<Map<String, JsonElement>>> claims, Map<String, String> descriptions) {
		if (article == null) {
			return new EntityData(qid, astroParams, null, new PhysicalProperties(null, null, null), List.of(), claims, descriptions, json);
		}

		PhysicalProperties physics = new PhysicalProperties(
				getPhysicalProperty(claims, PROP_RADIUS, CONVERSIONS_RADIUS),
				getPhysicalProperty(claims, PROP_DISTANCE, CONVERSIONS_DISTANCE),
				getPhysicalProperty(claims, PROP_MASS, CONVERSIONS_MASS)
		);

		List<CatalogEntry> catalogs = extractCatalogs(claims, article.getLabels());
		return new EntityData(qid, astroParams, article, physics, catalogs, claims, descriptions, json);
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
		String amountStr = valueNode.get("amount").getAsString();
		if (amountStr == null) {
			return null;
		}
		double amount = Algorithms.parseDoubleSilently(amountStr, 0.0);

		String unitUrl = valueNode.get("unit").getAsString();
		String unitQid = null;
		final String suffix = "entity/";
		int ind = unitUrl.lastIndexOf(suffix);
		if (ind >= 0) {
			unitQid = unitUrl.substring(ind + suffix.length());
		}
		return new Quantity(amount, unitQid);
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
			log.error("Failed to write JSON", e);
			return null;
		}
	}

	private static String normalizeNameKey(String s) {
		if (Algorithms.isBlank(s)) {
			return null;
		}
		return s.trim().toLowerCase(Locale.ROOT);
	}

	private static String stripExtension(String fileName) {
		int ind = fileName.lastIndexOf('.');
		return ind >= 0 ? fileName.substring(0, ind) : fileName;
	}

	private static String asString(Object o) {
		return o == null ? null : String.valueOf(o);
	}
}
