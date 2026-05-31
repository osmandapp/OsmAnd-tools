package net.osmand.server.api.searchtest;

import net.osmand.data.LatLon;
import net.osmand.util.Algorithms;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.*;
import java.util.*;
import java.util.regex.Pattern;

public interface AddressPOIAnalystService extends TokenAnalystService {

    default DbObjectPage getTagsDbAddressPoiObjects(String datasource, String objectType, String regExp,
                                                    String tokenFind, String tag, List<String> values,
                                                    boolean perObf,
                                                    int pageToShow, int pageSizeLimit,
                                                    String sortBy, String sortOrder) throws IOException, SQLException {
        Path dbFile = resolveTagsDatasource(datasource);
        String normalizedType = "poi".equalsIgnoreCase(objectType) ? "poi" : "address";
        String typeSql = "poi".equals(normalizedType) ? "o.type = 'POI'" : "o.type <> 'POI'";
        String nameSql = Algorithms.isEmpty(regExp) ? "" : " AND o.name REGEXP ?";
        String tokenCountSql = buildAddressPoiTokenCountSql(tokenFind);
        String tokenFilterSql = Algorithms.isEmpty(tokenFind) ? "" : " AND (" + tokenCountSql + ") > 0";
        AddressPoiTagFilter tagFilter = buildAddressPoiTagFilter(tag, values);
        String tagSql = tagFilter.enabled() ? " AND " + tagFilter.sql() : "";
        String obfJoin = perObf ? " JOIN (SELECT DISTINCT object_id, obf_id FROM posting) po ON po.object_id = o.id LEFT JOIN obf b ON b.id = po.obf_id" : "";
        String base = " FROM \"object\" o" + obfJoin + " WHERE " + typeSql + nameSql + tagSql + tokenFilterSql;
        Map<String, String> orderColumns = Map.of(
                "sequenceid", "o.id",
                "name", "o.name",
                "type", "o.type",
                "osmid", "o.id",
                "tokens", "tokens",
                "obf", perObf ? "obfName" : "o.id",
                "obfname", perObf ? "obfName" : "o.id");
        int safePage = Math.max(0, pageToShow);
        int safeSize = Math.max(1, Math.min(pageSizeLimit, 500));
        try (Connection conn = openAddressPoiTagsDbConnection(dbFile)) {
            List<Object> params = buildAddressPoiObjectParams(regExp, tokenFind, tagFilter, false);
            long total = queryAddressPoiLong(conn, "SELECT COUNT(*)" + base, params.toArray());
            List<Object> selectParams = buildAddressPoiObjectParams(regExp, tokenFind, tagFilter, true);
            String obfTokenFilter = perObf ? " AND p.obf_id = po.obf_id" : "";
            String perObfTokenCountSql = tokenCountSql.replace("WHERE p.object_id = o.id", "WHERE p.object_id = o.id" + obfTokenFilter);
            String sql = "SELECT ROW_NUMBER() OVER (ORDER BY o.id ASC" + (perObf ? ", b.name ASC" : "") + ") sequenceId, "
                    + "o.name, o.lat, o.lon, o.commonTags, o.extraTags, o.type, o.id, o.osmType, 0 isAlone, "
                    + (perObf ? "b.name" : "NULL") + " obfName, "
                    + perObfTokenCountSql + " tokens"
                    + base + buildAddressPoiOrderBy(sortBy, sortOrder, orderColumns, "o.id ASC") + " LIMIT ? OFFSET ?";
            List<DbObject> content = new ArrayList<>();
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                int idx = bindAddressPoiParams(ps, 1, selectParams);
                ps.setInt(idx++, safeSize);
                ps.setInt(idx, safePage * safeSize);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        Double lat = rs.getObject(3) == null ? null : rs.getDouble(3);
                        Double lon = rs.getObject(4) == null ? null : rs.getDouble(4);
                        LatLon point = lat == null || lon == null ? null : new LatLon(lat, lon);
                        content.add(new DbObject(rs.getInt(1), rs.getString(2), point,
                                parseAddressPoiObjectValues(rs.getString(5)),
                                parseAddressPoiObjectExtraTags(rs.getString(6)),
                                rs.getString(7), rs.getLong(8), rs.getString(9), false,
                                getTagsObfDisplayName(rs.getString(11)), rs.getLong(12)));
                    }
                }
            }
            int totalPages = total == 0 ? 0 : (int) ((total + safeSize - 1) / safeSize);
            return new DbObjectPage(content, safePage, safeSize, total, totalPages);
        }
    }

    private String buildAddressPoiTokenCountSql(String tokenFind) {
        String tokenFilter = Algorithms.isEmpty(tokenFind) ? "" : " AND t.name REGEXP ?";
        return "(SELECT COUNT(DISTINCT t.id) FROM posting p JOIN token t ON t.id = p.token_id WHERE p.object_id = o.id" + tokenFilter + ")";
    }

    default DbObjectTokenPage getTagsDbObjectTokens(String datasource, long objectId, String find,
                                                    int pageToShow, int pageSizeLimit,
                                                    String sortBy, String sortOrder) throws IOException, SQLException {
        Path dbFile = resolveTagsDatasource(datasource);
        String filter = Algorithms.isEmpty(find) ? "" : " AND t.name REGEXP ?";
        Map<String, String> orderColumns = Map.of(
                "name", "t.name",
                "common", "t.isCommon",
                "frequent", "t.isFrequent",
                "new", "t.isGenerated",
                "generated", "t.isGenerated",
                "isgenerated", "t.isGenerated");
        int safePage = Math.max(0, pageToShow);
        int safeSize = Math.max(1, Math.min(pageSizeLimit, 500));
        String grouped = "SELECT DISTINCT t.id, t.name, t.isCommon, t.isFrequent, t.isGenerated, NULL obfName "
                + "FROM posting p JOIN token t ON t.id = p.token_id WHERE p.object_id = ?" + filter;
        try (Connection conn = openAddressPoiTagsDbConnection(dbFile)) {
            List<Object> params = new ArrayList<>();
            params.add(objectId);
            if (!Algorithms.isEmpty(find)) {
                params.add(find);
            }
            long total = queryAddressPoiLong(conn, "SELECT COUNT(*) FROM (" + grouped + ") q", params.toArray());
            String sql = grouped + buildAddressPoiOrderBy(sortBy, sortOrder, orderColumns, "t.name ASC") + " LIMIT ? OFFSET ?";
            List<DbObjectToken> content = new ArrayList<>();
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                int idx = bindAddressPoiParams(ps, 1, params);
                ps.setInt(idx++, safeSize);
                ps.setInt(idx, safePage * safeSize);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        content.add(new DbObjectToken(rs.getLong(1), rs.getString(2), rs.getInt(3) != 0, rs.getInt(4) != 0,
                                rs.getInt(5) != 0, getTagsObfDisplayName(rs.getString(6))));
                    }
                }
            }
            int totalPages = total == 0 ? 0 : (int) ((total + safeSize - 1) / safeSize);
            return new DbObjectTokenPage(content, safePage, safeSize, total, totalPages);
        }
    }

    default DbReport getTagsDbReport(String datasource, String objectType) throws IOException, SQLException {
        Path dbFile = resolveTagsDatasource(datasource);
        String typeWhere = reportTypePredicate(objectType);
        String join = typeWhere.isEmpty() ? "" : " JOIN \"object\" o ON o.id = p.object_id";
        String cond = typeWhere.isEmpty() ? "" : " WHERE " + typeWhere;
        try (Connection conn = openAddressPoiTagsDbConnection(dbFile)) {
            long totalTokens = queryAddressPoiLong(conn,
                    "SELECT COUNT(DISTINCT t.id) FROM token t JOIN posting p ON p.token_id = t.id" + join + cond);
            long totalPostings = queryAddressPoiLong(conn,
                    "SELECT COUNT(*) FROM posting p" + join + cond);
            return new DbReport(totalTokens, totalPostings,
                    loadReportDistribution(conn, typeWhere),
                    loadReportPruning(conn, totalPostings, typeWhere),
                    loadReportCandidates(conn, typeWhere),
                    loadReportAttribution(conn, typeWhere));
        }
    }

    private String reportTypePredicate(String objectType) {
        String normalized = objectType == null ? "all" : objectType.trim().toLowerCase(Locale.ROOT);
        return switch (normalized) {
            case "poi" -> "o.type = 'POI'";
            case "address", "addr" -> "o.type <> 'POI'";
            default -> "";
        };
    }

    private String getTagsObfDisplayName(String name) {
        if (Algorithms.isEmpty(name)) {
            return "";
        }
        String fileName = Path.of(name).getFileName().toString();
        int dot = fileName.lastIndexOf('.');
        return dot > 0 ? fileName.substring(0, dot) : fileName;
    }

    private List<DbReportDistribution> loadReportDistribution(Connection conn, String typeWhere) throws SQLException {
        String join = typeWhere.isEmpty() ? "" : " JOIN \"object\" o ON o.id = p.object_id";
        String cond = typeWhere.isEmpty() ? "" : " WHERE " + typeWhere;
        String sql = "WITH tm AS ("
                + " SELECT t.id, COUNT(p.object_id) m"
                + " FROM token t JOIN posting p ON p.token_id = t.id" + join + cond
                + " GROUP BY t.id)"
                + " SELECT bucket, ord, COUNT(*) tokens, SUM(m) postings FROM ("
                + "   SELECT m,"
                + "     CASE WHEN m = 1 THEN '1' WHEN m <= 5 THEN '2-5' WHEN m <= 20 THEN '6-20'"
                + "          WHEN m <= 100 THEN '21-100' WHEN m <= 500 THEN '101-500' ELSE '500+' END bucket,"
                + "     CASE WHEN m = 1 THEN 1 WHEN m <= 5 THEN 2 WHEN m <= 20 THEN 3"
                + "          WHEN m <= 100 THEN 4 WHEN m <= 500 THEN 5 ELSE 6 END ord"
                + "   FROM tm"
                + " ) GROUP BY bucket, ord ORDER BY ord";
        List<DbReportDistribution> result = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(sql); ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                result.add(new DbReportDistribution(rs.getString("bucket"), rs.getLong("tokens"), rs.getLong("postings")));
            }
        }
        return result;
    }

    private List<DbReportPruneToken> loadReportPruning(Connection conn, long totalPostings, String typeWhere) throws SQLException {
        String join = typeWhere.isEmpty() ? "" : " JOIN \"object\" o ON o.id = p.object_id";
        String cond = typeWhere.isEmpty() ? "" : " WHERE " + typeWhere;
        String sql = "SELECT t.name, t.isCommon, t.isFrequent, COUNT(p.object_id) matched, SUM(p.isAlone) alone"
                + " FROM token t JOIN posting p ON p.token_id = t.id" + join + cond
                + " GROUP BY t.id"
                + " HAVING alone = 0 OR length(t.name) < 2"
                + " ORDER BY matched DESC LIMIT 100";
        List<DbReportPruneToken> result = new ArrayList<>();
        long cumulative = 0;
        try (PreparedStatement ps = conn.prepareStatement(sql); ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                long matched = rs.getLong("matched");
                long alone = rs.getLong("alone");
                String name = rs.getString("name");
                long removable = name != null && name.length() < 2 ? matched : matched - alone;
                cumulative += removable;
                double cumulativePct = totalPostings > 0 ? 100.0 * cumulative / totalPostings : 0;
                result.add(new DbReportPruneToken(name, rs.getInt("isCommon") != 0,
                        rs.getInt("isFrequent") != 0, matched, alone, removable, cumulativePct));
            }
        }
        return result;
    }

    private List<DbReportCandidateTag> loadReportCandidates(Connection conn, String typeWhere) throws SQLException {
        String join = typeWhere.isEmpty() ? "" : " JOIN \"object\" o ON o.id = otv.object_id";
        String cond = typeWhere.isEmpty() ? "" : " WHERE " + typeWhere;
        String sql = "SELECT t.name, COUNT(DISTINCT otv.object_id) objs, COUNT(DISTINCT otv.value_id) vals"
                + " FROM tag t JOIN object_tag_value otv ON otv.tag_id = t.id" + join + cond
                + " GROUP BY t.id"
                + " HAVING objs >= 20"
                + " ORDER BY (1.0 * vals / objs) DESC, objs DESC LIMIT 50";
        List<DbReportCandidateTag> result = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(sql); ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                long objects = rs.getLong("objs");
                long distinctValues = rs.getLong("vals");
                double selectivity = objects > 0 ? 1.0 * distinctValues / objects : 0;
                result.add(new DbReportCandidateTag(rs.getString("name"), objects, distinctValues, selectivity));
            }
        }
        return result;
    }

    private List<DbReportAttribution> loadReportAttribution(Connection conn, String typeWhere) throws SQLException {
        String topJoin = typeWhere.isEmpty() ? "" : " JOIN \"object\" o ON o.id = p.object_id";
        String topCond = typeWhere.isEmpty() ? "" : " WHERE " + typeWhere;
        String attrCond = typeWhere.isEmpty() ? "" : " AND " + typeWhere;
        String sql = "WITH top_tokens AS ("
                + " SELECT t.id, t.name, COUNT(p.object_id) matched"
                + " FROM token t JOIN posting p ON p.token_id = t.id" + topJoin + topCond
                + " GROUP BY t.id ORDER BY matched DESC LIMIT 15),"
                + " attr AS ("
                + " SELECT tt.id token_id, tt.name token, tt.matched, tg.name tag, COUNT(DISTINCT otv.object_id) hits"
                + " FROM top_tokens tt"
                + " JOIN posting p ON p.token_id = tt.id"
                + " JOIN \"object\" o ON o.id = p.object_id"
                + " JOIN object_tag_value otv ON otv.object_id = p.object_id"
                + " JOIN tag tg ON tg.id = otv.tag_id"
                + " WHERE instr(lower(otv.value), lower(tt.name)) > 0" + attrCond
                + " GROUP BY tt.id, tg.id),"
                + " ranked AS ("
                + " SELECT token, matched, tag, hits,"
                + " ROW_NUMBER() OVER (PARTITION BY token_id ORDER BY hits DESC, tag ASC) rn"
                + " FROM attr)"
                + " SELECT token, matched, tag, hits FROM ranked WHERE rn = 1 ORDER BY matched DESC";
        List<DbReportAttribution> result = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(sql); ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                long matched = rs.getLong("matched");
                long hits = rs.getLong("hits");
                double sharePct = matched > 0 ? 100.0 * hits / matched : 0;
                result.add(new DbReportAttribution(rs.getString("token"), matched, rs.getString("tag"), hits, sharePct));
            }
        }
        return result;
    }

    private Connection openAddressPoiTagsDbConnection(Path dbFile) throws SQLException {
        Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbFile.toAbsolutePath());
        conn.createStatement().execute("PRAGMA query_only = ON");
        org.sqlite.Function.create(conn, "REGEXP", new org.sqlite.Function() {
            @Override
            protected void xFunc() throws SQLException {
                String pattern = value_text(0);
                String value = value_text(1);
                result(pattern != null && value != null && Pattern.compile(pattern, Pattern.CASE_INSENSITIVE).matcher(value).find() ? 1 : 0);
            }
        });
        return conn;
    }

    private String buildAddressPoiOrderBy(String sortBy, String sortOrder, Map<String, String> columns, String fallback) {
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

    private AddressPoiTagFilter buildAddressPoiTagFilter(String tag, List<String> values) {
        if (Algorithms.isEmpty(tag)) {
            return new AddressPoiTagFilter(false, "", Collections.emptyList());
        }
        List<Object> params = new ArrayList<>();
        if (values == null || values.stream().filter(v -> !Algorithms.isEmpty(v)).findAny().isEmpty()) {
            params.add(tag);
            return new AddressPoiTagFilter(true, "EXISTS (SELECT 1 FROM object_tag_value otv JOIN tag tg ON tg.id = otv.tag_id "
                    + "WHERE otv.object_id = o.id AND tg.name = ?)", params);
        }
        List<String> cleanValues = values.stream().filter(v -> !Algorithms.isEmpty(v)).toList();
        String placeholders = String.join(", ", Collections.nCopies(cleanValues.size(), "?"));
        params.add(tag);
        params.addAll(cleanValues);
        return new AddressPoiTagFilter(true, "EXISTS (SELECT 1 FROM object_tag_value otv JOIN tag tg ON tg.id = otv.tag_id "
                + "WHERE otv.object_id = o.id AND tg.name = ? AND otv.value IN (" + placeholders + "))", params);
    }

    private List<Object> buildAddressPoiObjectParams(String regExp, String tokenFind, AddressPoiTagFilter tagFilter, boolean includeTokenCountParams) {
        List<Object> params = new ArrayList<>();
        if (includeTokenCountParams && !Algorithms.isEmpty(tokenFind)) {
            params.add(tokenFind);
        }
        if (!Algorithms.isEmpty(regExp)) {
            params.add(regExp);
        }
        if (tagFilter != null && tagFilter.enabled()) {
            params.addAll(tagFilter.params());
        }
        if (!Algorithms.isEmpty(tokenFind)) {
            params.add(tokenFind);
        }
        return params;
    }

    private long queryAddressPoiLong(Connection conn, String sql, Object... params) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            bindAddressPoiParams(ps, 1, Arrays.asList(params));
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getLong(1) : 0;
            }
        }
    }

    private int bindAddressPoiParams(PreparedStatement ps, int idx, List<Object> params) throws SQLException {
        for (Object param : params) {
            if (param instanceof Number number) {
                ps.setLong(idx++, number.longValue());
            } else {
                ps.setString(idx++, String.valueOf(param));
            }
        }
        return idx;
    }

    @SuppressWarnings("unchecked")
    private Map<String, String> parseAddressPoiObjectValues(String json) throws IOException {
        if (Algorithms.isEmpty(json)) {
            return Collections.emptyMap();
        }
        return getObjectMapper().readValue(json, Map.class);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> parseAddressPoiObjectExtraTags(String json) throws IOException {
        if (Algorithms.isEmpty(json)) {
            return Collections.emptyMap();
        }
        return getObjectMapper().readValue(json, Map.class);
    }

    record AddressPoiTagFilter(boolean enabled, String sql, List<Object> params) {
    }
}
