package net.osmand.server.api.searchtest;

import net.osmand.data.LatLon;
import net.osmand.util.Algorithms;
import org.sqlite.Function;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.util.*;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public interface TokenAnalystService extends OBFService {
    int TAGS_DB_PAGE_SIZE = 100;

    default Path getTagsDatasourceDir() throws IOException {
        String url = getSearchTestDatasourceUrl();
        if (Algorithms.isEmpty(url)) {
            throw new IOException("spring.searchtestdatasource.url is not configured");
        }
        String path = url.startsWith("jdbc:sqlite:") ? url.substring("jdbc:sqlite:".length()) : url;
        Path parent = Path.of(path).toAbsolutePath().getParent();
        if (parent == null) {
            throw new IOException("Could not resolve datasource parent directory");
        }
        Path dir = parent.resolve("tags_db");
        Files.createDirectories(dir);
        return dir;
    }

    default Path resolveTagsDatasource(String name) throws IOException {
        if (Algorithms.isEmpty(name)) {
            throw new IOException("Datasource name is required");
        }
        String fileName = Path.of(name).getFileName().toString();
        if (!fileName.endsWith(".db") && !fileName.endsWith(".sqlite")) {
            fileName += ".db";
        }
        Path dir = getTagsDatasourceDir();
        Path file = dir.resolve(fileName).normalize();
        if (!file.startsWith(dir)) {
            throw new IOException("Invalid datasource name");
        }
        return file;
    }

    default List<Datasource> getTagsDatasources() throws IOException {
        Path dir = getTagsDatasourceDir();
        List<Datasource> result = new ArrayList<>();
        try (var stream = Files.list(dir)) {
            stream.filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().endsWith(".db") || path.getFileName().toString().endsWith(".sqlite"))
                    .sorted(Comparator.comparing(path -> path.getFileName().toString(), String.CASE_INSENSITIVE_ORDER))
                    .forEach(path -> {
                        try {
                            result.add(new Datasource(path.getFileName().toString(), Files.size(path),
                                    Files.getLastModifiedTime(path).toMillis(), true, ""));
                        } catch (IOException ignored) {
                        }
                    });
        }
        return result;
    }

    default boolean deleteTagsDatasource(String name) throws IOException {
        return Files.deleteIfExists(resolveTagsDatasource(name));
    }

    default void downloadTagsDatasource(String name, OutputStream out) throws IOException {
        Path dbFile = resolveTagsDatasource(name);
        if (!Files.exists(dbFile)) {
            throw new IOException("Datasource not found: " + name);
        }
        try (ZipOutputStream zip = new ZipOutputStream(out)) {
            zip.putNextEntry(new ZipEntry("db.sqlite"));
            Files.copy(dbFile, zip);
            zip.closeEntry();
            zip.finish();
        }
    }

    default List<DbTagName> getTagsDbTagNames(String datasource) throws IOException, SQLException {
        Path dbFile = resolveTagsDatasource(datasource);
        String sql = """
                SELECT t.name, COALESCE(SUM(ts.objects_count), 0) objects, MIN(t.isSkipped) isSkipped
                FROM tag t
                LEFT JOIN tag_stats ts ON ts.tag_id = t.id
                GROUP BY t.name
                ORDER BY t.name COLLATE NOCASE ASC
                """;
        try (Connection conn = openTagsDbConnection(dbFile);
             PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            List<DbTagName> result = new ArrayList<>();
            while (rs.next()) {
                result.add(new DbTagName(rs.getString(1), rs.getLong(2), rs.getInt(3) != 0));
            }
            return result;
        }
    }

    default List<DbTagValue> getTagsDbTagValues(String datasource, String tag) throws IOException, SQLException {
        Path dbFile = resolveTagsDatasource(datasource);
        if (Algorithms.isEmpty(tag)) {
            return Collections.emptyList();
        }
        try (Connection conn = openTagsDbConnection(dbFile);
             PreparedStatement ps = conn.prepareStatement("""
                     SELECT otv.value, COUNT(DISTINCT otv.object_id) objects_count
                     FROM object_tag_value otv
                     JOIN tag t ON t.id = otv.tag_id
                     WHERE t.name = ?
                     GROUP BY otv.value
                     ORDER BY otv.value COLLATE NOCASE ASC
                     """)) {
            ps.setString(1, tag);
            try (ResultSet rs = ps.executeQuery()) {
                List<DbTagValue> result = new ArrayList<>();
                while (rs.next()) {
                    result.add(new DbTagValue(rs.getString(1), rs.getLong(2)));
                }
                return result;
            }
        }
    }

    default DbTokenPage getTagsDbTokens(String datasource, String prefix, String objectType, boolean perObf,
                                        String tag, List<String> values,
                                        int pageToShow, int pageSizeLimit, String sortBy, String sortOrder) throws IOException, SQLException {
        Path dbFile = resolveTagsDatasource(datasource);
        TagFilter tagFilter = buildTagFilter(tag, values);
        String tokenFilter = tagFilter.enabled() ? getTokenSourceFilter(objectType) : "";
        String where = buildTagsDbTokenWhere(prefix, tokenFilter, tagFilter);
        Map<String, String> orderColumns = Map.of(
                "name", "t.name",
                "matched", "matched",
                "alone", "alone",
                "common", "t.isCommon",
                "frequent", "t.isFrequent",
                "new", "t.isGenerated",
                "generated", "t.isGenerated",
                "isgenerated", "t.isGenerated");
        int safePage = Math.max(0, pageToShow);
        try (Connection conn = openTagsDbConnection(dbFile)) {
            String grouped = buildTagsDbTokenGroupedSql(objectType, tagFilter, where);
            List<Object> tokenParams = buildTagTokenParams(prefix, tagFilter);
            long total = queryLong(conn, "SELECT COUNT(*) FROM (" + grouped + ") q", tokenParams.toArray());
            String sql = grouped + buildTagsDbOrderBy(sortBy, sortOrder, orderColumns, "t.name ASC") + " LIMIT ? OFFSET ?";
            List<DbToken> content = new ArrayList<>();
            DbTokenSummary summary;
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                int idx = bindParams(ps, 1, tokenParams);
                ps.setInt(idx++, TAGS_DB_PAGE_SIZE);
                ps.setInt(idx, safePage * TAGS_DB_PAGE_SIZE);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        content.add(new DbToken(rs.getLong(1), rs.getString(2), rs.getLong(6), rs.getLong(7),
                                rs.getInt(3) != 0, rs.getInt(4) != 0, rs.getInt(5) != 0));
                    }
                }
            }
            summary = buildTagsDbTokenSummary(conn, grouped, tokenParams);
            int totalPages = total == 0 ? 0 : (int) ((total + TAGS_DB_PAGE_SIZE - 1) / TAGS_DB_PAGE_SIZE);
            return new DbTokenPage(content, safePage, TAGS_DB_PAGE_SIZE, total, totalPages, summary);
        }
    }

    private String buildTagsDbTokenGroupedSql(String objectType, TagFilter tagFilter, String where) {
        String normalized = Algorithms.isEmpty(objectType) ? "all" : objectType.trim().toLowerCase(Locale.ROOT);
        boolean poiOnly = "poi".equals(normalized);
        boolean addressOnly = "address".equals(normalized) || "addr".equals(normalized);
        if (tagFilter.enabled()) {
            return "SELECT t.id, t.name, t.isCommon, t.isFrequent, t.isGenerated, COUNT(p.object_id) matched, COALESCE(SUM(CASE WHEN p.isAlone = 1 THEN 1 ELSE 0 END), 0) alone "
                    + " FROM token t JOIN posting p ON p.token_id = t.id JOIN \"object\" o ON o.id = p.object_id"
                    + appendObjectTypeWhere(where, objectType)
                    + " GROUP BY t.id, t.name, t.isCommon, t.isFrequent, t.isGenerated";
        }
        String matchedColumn = poiOnly ? "ts.poi_matched_count" : addressOnly ? "ts.address_matched_count" : "ts.matched_count";
        String aloneColumn = poiOnly ? "ts.poi_alone_count" : addressOnly ? "ts.address_alone_count" : "ts.alone_count";
        return "SELECT t.id, t.name, t.isCommon, t.isFrequent, t.isGenerated, "
                + matchedColumn + " matched, " + aloneColumn + " alone "
                + " FROM token t JOIN token_stats ts ON ts.token_id = t.id"
                + where;
    }

    default DbObjectPage getTagsDbObjects(String datasource, long tokenId, String objectType, boolean perObf, String regExp,
                                      String tag, List<String> values,
                                      int pageToShow, int pageSizeLimit, String sortBy, String sortOrder) throws IOException, SQLException {
        Path dbFile = resolveTagsDatasource(datasource);
        String source = perObf ? "posting" : "(SELECT token_id, object_id, isAlone, MIN(sequenceId) sequenceId FROM posting GROUP BY token_id, object_id, isAlone)";
        String filter = Algorithms.isEmpty(regExp) ? "" : " AND (o.name REGEXP ? OR o.commonTags REGEXP ? OR o.extraTags REGEXP ?)";
        String objectTypeFilter = getObjectTypeObjectFilter(objectType);
        TagFilter tagFilter = buildTagFilter(tag, values);
        String tagSql = tagFilter.enabled() ? " AND " + tagFilter.sql() : "";
        String obfJoin = perObf ? " LEFT JOIN obf b ON b.id = p.obf_id" : "";
        String base = " FROM " + source + " p JOIN \"object\" o ON o.id = p.object_id" + obfJoin + " WHERE p.token_id = ?" + objectTypeFilter + filter + tagSql;
        Map<String, String> orderColumns = new HashMap<>();
        orderColumns.put("sequenceid", "p.sequenceId");
        orderColumns.put("name", "o.name");
        orderColumns.put("type", "o.type");
        orderColumns.put("osmid", "o.id");
        orderColumns.put("alone", "p.isAlone");
        orderColumns.put("isalone", "p.isAlone");
        orderColumns.put("obf", perObf ? "b.name" : "p.sequenceId");
        orderColumns.put("obfname", perObf ? "b.name" : "p.sequenceId");
        int safePage = Math.max(0, pageToShow);
        int safeSize = TAGS_DB_PAGE_SIZE;
        try (Connection conn = openTagsDbConnection(dbFile)) {
            List<Object> objectParams = buildTagObjectParams(tokenId, regExp, tagFilter);
            long total = queryLong(conn, "SELECT COUNT(*)" + base, objectParams.toArray());
            String sql = "SELECT p.sequenceId, o.name, o.lat, o.lon, o.commonTags, o.extraTags, o.type, o.id, o.osmType, p.isAlone"
                    + (perObf ? ", b.name" : ", NULL") + base
                    + buildTagsDbOrderBy(sortBy, sortOrder, orderColumns, "p.sequenceId ASC") + " LIMIT ? OFFSET ?";
            List<DbObject> content = new ArrayList<>();
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                int idx = bindParams(ps, 1, objectParams);
                ps.setInt(idx++, safeSize);
                ps.setInt(idx, safePage * safeSize);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        Double lat = rs.getObject(3) == null ? null : rs.getDouble(3);
                        Double lon = rs.getObject(4) == null ? null : rs.getDouble(4);
                        LatLon point = lat == null || lon == null ? null : new LatLon(lat, lon);
                        Map<String, String> commonTags = parseObjectValues(rs.getString(5));
                        Map<String, Object> extraTags = parseObjectExtraTags(rs.getString(6));
                        content.add(new DbObject(rs.getInt(1), rs.getString(2), point, commonTags, extraTags, rs.getString(7),
                                rs.getLong(8), rs.getString(9), rs.getInt(10) != 0, getTagsObfDisplayName(rs.getString(11)), 0));
                    }
                }
            }
            int totalPages = total == 0 ? 0 : (int) ((total + safeSize - 1) / safeSize);
            return new DbObjectPage(content, safePage, safeSize, total, totalPages);
        }
    }

    private String getTagsObfDisplayName(String name) {
        if (Algorithms.isEmpty(name)) {
            return "";
        }
        String fileName = Path.of(name).getFileName().toString();
        int dot = fileName.lastIndexOf('.');
        return dot > 0 ? fileName.substring(0, dot) : fileName;
    }

    private String appendObjectTypeWhere(String where, String objectType) {
        String objectFilter = getObjectTypeObjectFilter(objectType);
        if (Algorithms.isEmpty(objectFilter)) {
            return where;
        }
        return Algorithms.isEmpty(where) ? " WHERE " + objectFilter.substring(" AND ".length()) : where + objectFilter;
    }

    private String getObjectTypeObjectFilter(String objectType) {
        String normalized = Algorithms.isEmpty(objectType) ? "all" : objectType.trim().toLowerCase(Locale.ROOT);
        return switch (normalized) {
            case "poi" -> " AND o.type = 'POI'";
            case "address", "addr" -> " AND o.type <> 'POI'";
            default -> "";
        };
    }

    private String getTokenSourceFilter(String objectType) {
        String normalized = Algorithms.isEmpty(objectType) ? "all" : objectType.trim().toLowerCase(Locale.ROOT);
        return switch (normalized) {
            case "poi" -> "EXISTS (SELECT 1 FROM posting pp JOIN \"object\" po ON po.id = pp.object_id WHERE pp.token_id = t.id AND po.type = 'POI')";
            case "address", "addr" -> "EXISTS (SELECT 1 FROM posting ap JOIN \"object\" ao ON ao.id = ap.object_id WHERE ap.token_id = t.id AND ao.type <> 'POI')";
            default -> "";
        };
    }

    private String buildTagsDbTokenWhere(String prefix, String tokenFilter, TagFilter tagFilter) {
        List<String> conditions = new ArrayList<>();
        if (!Algorithms.isEmpty(prefix)) {
            conditions.add("t.name LIKE ? || '%' COLLATE NOCASE");
        }
        if (!Algorithms.isEmpty(tokenFilter)) {
            conditions.add(tokenFilter);
        }
        if (tagFilter != null && tagFilter.enabled()) {
            conditions.add(tagFilter.sql());
        }
        return conditions.isEmpty() ? "" : " WHERE " + String.join(" AND ", conditions);
    }

    private String buildTagsDbOrderBy(String sortBy, String sortOrder, Map<String, String> columns, String fallback) {
        String[] sortKeys = Algorithms.isEmpty(sortBy) ? new String[0] : sortBy.split(",");
        String[] sortOrders = Algorithms.isEmpty(sortOrder) ? new String[0] : sortOrder.split(",");
        List<String> terms = new ArrayList<>();
        Set<String> usedColumns = new HashSet<>();
        for (int i = 0; i < sortKeys.length; i++) {
            String key = sortKeys[i] == null ? "" : sortKeys[i].trim().toLowerCase(Locale.ROOT);
            String column = columns.get(key);
            if (Algorithms.isEmpty(column) || !usedColumns.add(column)) {
                continue;
            }
            String direction = i < sortOrders.length && "asc".equalsIgnoreCase(sortOrders[i].trim()) ? "ASC" : "DESC";
            terms.add(column + " " + direction);
        }
        if (!Algorithms.isEmpty(fallback)) {
            terms.add(fallback);
        }
        return " ORDER BY " + String.join(", ", terms);
    }

    private Connection openTagsDbConnection(Path dbFile) throws SQLException {
        Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbFile.toAbsolutePath());
        conn.createStatement().execute("PRAGMA query_only = ON");
        Function.create(conn, "REGEXP", new Function() {
            @Override
            protected void xFunc() throws SQLException {
                String pattern = value_text(0);
                String value = value_text(1);
                result(pattern != null && value != null && Pattern.compile(pattern, Pattern.CASE_INSENSITIVE).matcher(value).find() ? 1 : 0);
            }
        });
        return conn;
    }

    private long queryLong(Connection conn, String sql, Object... params) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            int idx = 1;
            for (Object param : params) {
                if (param == null || (param instanceof String s && Algorithms.isEmpty(s))) {
                    continue;
                }
                if (param instanceof Number number) {
                    ps.setLong(idx++, number.longValue());
                } else {
                    ps.setString(idx++, String.valueOf(param));
                }
            }
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getLong(1) : 0;
            }
        }
    }

    private int bindParams(PreparedStatement ps, int idx, List<Object> params) throws SQLException {
        for (Object param : params) {
            if (param instanceof Number number) {
                ps.setLong(idx++, number.longValue());
            } else {
                ps.setString(idx++, String.valueOf(param));
            }
        }
        return idx;
    }

    private List<Object> buildTagTokenParams(String prefix, TagFilter tagFilter) {
        List<Object> params = new ArrayList<>();
        if (!Algorithms.isEmpty(prefix)) {
            params.add(prefix);
        }
        if (tagFilter != null && tagFilter.enabled()) {
            params.addAll(tagFilter.params());
        }
        return params;
    }

    private List<Object> buildTagObjectParams(long tokenId, String regExp, TagFilter tagFilter) {
        List<Object> params = new ArrayList<>();
        params.add(tokenId);
        if (!Algorithms.isEmpty(regExp)) {
            params.add(regExp);
            params.add(regExp);
            params.add(regExp);
        }
        if (tagFilter != null && tagFilter.enabled()) {
            params.addAll(tagFilter.params());
        }
        return params;
    }

    private TagFilter buildTagFilter(String tag, List<String> values) {
        if (Algorithms.isEmpty(tag)) {
            return new TagFilter(false, "", Collections.emptyList());
        }
        if (values == null || values.isEmpty()) {
            return buildTagExistenceFilter(tag);
        }
        List<String> cleanValues = values.stream().filter(v -> !Algorithms.isEmpty(v)).toList();
        if (cleanValues.isEmpty()) {
            return buildTagExistenceFilter(tag);
        }
        List<String> conditions = new ArrayList<>();
        List<Object> params = new ArrayList<>();
        String placeholders = String.join(", ", Collections.nCopies(cleanValues.size(), "?"));
        conditions.add("EXISTS (SELECT 1 FROM object_tag_value otv JOIN tag tg ON tg.id = otv.tag_id "
                + "WHERE otv.object_id = o.id AND tg.name = ? AND otv.value IN (" + placeholders + "))");
        params.add(tag);
        params.addAll(cleanValues);
        return new TagFilter(true, "(" + String.join(" OR ", conditions) + ")", params);
    }

    private TagFilter buildTagExistenceFilter(String tag) {
        return new TagFilter(true, "EXISTS (SELECT 1 FROM object_tag_value otv JOIN tag tg ON tg.id = otv.tag_id "
                + "WHERE otv.object_id = o.id AND tg.name = ?)", List.of(tag));
    }

    private DbTokenSummary buildTagsDbTokenSummary(Connection conn, String groupedSql, List<Object> params) throws SQLException {
        String sql = "SELECT SUM(matched), SUM(alone), SUM(isCommon), SUM(isFrequent), SUM(isGenerated), MAX(matched), MAX(alone) FROM ("
                + groupedSql + ")";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            bindParams(ps, 1, params);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? new DbTokenSummary(rs.getLong(1), rs.getLong(2), rs.getLong(3), rs.getLong(4), rs.getLong(5), rs.getLong(6), rs.getLong(7))
                        : new DbTokenSummary(0, 0, 0, 0, 0, 0, 0);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, String> parseObjectValues(String json) throws IOException {
        if (Algorithms.isEmpty(json)) {
            return Collections.emptyMap();
        }
        return getObjectMapper().readValue(json, Map.class);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> parseObjectExtraTags(String json) throws IOException {
        if (Algorithms.isEmpty(json)) {
            return Collections.emptyMap();
        }
        return getObjectMapper().readValue(json, Map.class);
    }

    record TagFilter(boolean enabled, String sql, List<Object> params) {
    }
}
