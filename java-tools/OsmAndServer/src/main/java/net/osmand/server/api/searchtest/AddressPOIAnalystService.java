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
        String nameSql = Algorithms.isEmpty(regExp) ? "" : " AND o.name LIKE ? || '%' COLLATE NOCASE";
        String tokenCountSql = buildAddressPoiTokenCountSql(tokenFind);
        String tokenFilterSql = Algorithms.isEmpty(tokenFind) ? "" : " AND " + buildAddressPoiTokenExistsSql(tokenFind);
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
        try (Connection conn = openAddressPoiTagsDbConnection(dbFile)) {
            List<Object> params = buildAddressPoiObjectParams(regExp, tokenFind, tagFilter, false);
            long total = queryAddressPoiLong(conn, "SELECT COUNT(*)" + base, params.toArray());
            List<Object> selectParams = buildAddressPoiObjectParams(regExp, tokenFind, tagFilter, true);
            String obfTokenFilter = perObf ? " AND p.obf_id = po.obf_id" : "";
            String perObfTokenCountSql = tokenCountSql.replace("WHERE p.object_id = o.id", "WHERE p.object_id = o.id" + obfTokenFilter);
            String sql = "SELECT ROW_NUMBER() OVER (ORDER BY o.id ASC" + (perObf ? ", b.name ASC" : "") + ") sequenceId, "
                    + "o.name, o.lat, o.lon, o.commonTags, o.type, o.id, o.osmType, 0 isAlone, "
                    + (perObf ? "b.name" : "NULL") + " obfName, "
                    + perObfTokenCountSql + " tokens"
                    + base + buildAddressPoiOrderBy(sortBy, sortOrder, orderColumns, "o.id ASC") + " LIMIT ? OFFSET ?";
            List<DbObject> content = new ArrayList<>();
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                int idx = bindAddressPoiParams(ps, 1, selectParams);
                ps.setInt(idx++, TAGS_DB_PAGE_SIZE);
                ps.setInt(idx, safePage * TAGS_DB_PAGE_SIZE);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        Double lat = rs.getObject(3) == null ? null : rs.getDouble(3);
                        Double lon = rs.getObject(4) == null ? null : rs.getDouble(4);
                        LatLon point = lat == null || lon == null ? null : new LatLon(lat, lon);
                        content.add(new DbObject(rs.getInt(1), rs.getString(2), point,
                                parseAddressPoiObjectValues(rs.getString(5)),
                                rs.getString(6), rs.getLong(7), rs.getString(8), false,
                                getTagsObfDisplayName(rs.getString(10)), rs.getLong(11)));
                    }
                }
            }
            int totalPages = total == 0 ? 0 : (int) ((total + TAGS_DB_PAGE_SIZE - 1) / TAGS_DB_PAGE_SIZE);
            return new DbObjectPage(content, safePage, TAGS_DB_PAGE_SIZE, total, totalPages);
        }
    }

    private String buildAddressPoiTokenCountSql(String tokenFind) {
        String tokenFilter = Algorithms.isEmpty(tokenFind) ? "" : " AND t.name LIKE ? || '%' COLLATE NOCASE";
        return "(SELECT COUNT(DISTINCT t.id) FROM posting p JOIN token t ON t.id = p.token_id WHERE p.object_id = o.id" + tokenFilter + ")";
    }

    private String buildAddressPoiTokenExistsSql(String tokenFind) {
        String tokenFilter = Algorithms.isEmpty(tokenFind) ? "" : " AND t.name LIKE ? || '%' COLLATE NOCASE";
        return "EXISTS (SELECT 1 FROM posting p JOIN token t ON t.id = p.token_id WHERE p.object_id = o.id" + tokenFilter + ")";
    }

    default DbObjectTokenPage getTagsDbObjectTokens(String datasource, long objectId, String find,
                                                    int pageToShow, int pageSizeLimit,
                                                    String sortBy, String sortOrder) throws IOException, SQLException {
        Path dbFile = resolveTagsDatasource(datasource);
        String filter = Algorithms.isEmpty(find) ? "" : " AND t.name LIKE ? || '%' COLLATE NOCASE";
        Map<String, String> orderColumns = Map.of(
                "name", "t.name",
                "common", "t.isCommon",
                "frequent", "t.isFrequent",
                "new", "t.isGenerated",
                "generated", "t.isGenerated",
                "isgenerated", "t.isGenerated");
        int safePage = Math.max(0, pageToShow);
        int safeSize = TAGS_DB_PAGE_SIZE;
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

    default DbReport getReport(String datasource, String objectType, String pruneGenerated, String pruneSort,
                               long bucketMin, long bucketMax) throws IOException, SQLException {
        Path dbFile = resolveTagsDatasource(datasource);
        String typeWhere = reportTypePredicate(objectType);
        String join = typeWhere.isEmpty() ? "" : " JOIN \"object\" o ON o.id = p.object_id";
        String cond = typeWhere.isEmpty() ? "" : " WHERE " + typeWhere;
        try (Connection conn = openAddressPoiTagsDbConnection(dbFile)) {
            long totalTokens = queryAddressPoiLong(conn,
                    "SELECT COUNT(DISTINCT p.token_id) FROM posting p" + join + cond);
            long totalPostings = queryAddressPoiLong(conn,
                    "SELECT COUNT(*) FROM posting p" + join + cond);
            return new DbReport(totalTokens, totalPostings,
                    loadReportDistribution(conn, typeWhere),
                    loadReportRanking(conn, totalPostings, typeWhere, pruneGenerated, pruneSort, bucketMin, bucketMax),
                    List.of());
        }
    }

    default List<TestCaseObject> getTestCases(String datasource, String objectType) throws IOException, SQLException {
        Path dbFile = resolveTagsDatasource(datasource);
        String typeWhere = reportTypePredicate(objectType);
        try (Connection conn = openAddressPoiTagsDbConnection(dbFile)) {
            return loadObjectTestCases(conn, typeWhere);
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

    private String reportGeneratedPredicate(String generated) {
        String normalized = generated == null ? "all" : generated.trim().toLowerCase(Locale.ROOT);
        return switch (normalized) {
            case "current" -> "t.isGenerated = 0";
            case "new" -> "t.isGenerated = 1";
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
        String tokenMatched;
        if (typeWhere.isEmpty()) {
            tokenMatched = "SELECT t.id, t.isGenerated, COUNT(p.object_id) m"
                    + " FROM token t LEFT JOIN posting p ON p.token_id = t.id"
                    + " GROUP BY t.id";
        } else {
            tokenMatched = "SELECT t.id, t.isGenerated, COUNT(o.id) m"
                    + " FROM token t LEFT JOIN posting p ON p.token_id = t.id"
                    + " LEFT JOIN \"object\" o ON o.id = p.object_id AND " + typeWhere
                    + " GROUP BY t.id";
        }
        String sql = "SELECT bucket, ord, COUNT(*) tokens, SUM(m) postings,"
                + " SUM(CASE WHEN isGenerated = 1 THEN 1 ELSE 0 END) tokensNew,"
                + " SUM(CASE WHEN isGenerated = 1 THEN m ELSE 0 END) postingsNew FROM ("
                + "   SELECT m, isGenerated,"
                + "     CASE WHEN m = 1 THEN '1' WHEN m <= 3 THEN '2-3' WHEN m <= 5 THEN '4-5'"
                + "          WHEN m <= 10 THEN '6-10' WHEN m <= 20 THEN '11-20'"
                + "          WHEN m <= 100 THEN '21-100' WHEN m <= 500 THEN '101-500' ELSE '500+' END bucket,"
                + "     CASE WHEN m = 1 THEN 1 WHEN m <= 3 THEN 2 WHEN m <= 5 THEN 3"
                + "          WHEN m <= 10 THEN 4 WHEN m <= 20 THEN 5"
                + "          WHEN m <= 100 THEN 6 WHEN m <= 500 THEN 7 ELSE 8 END ord"
                + "   FROM (" + tokenMatched + ")"
                + "   WHERE m > 0"
                + " ) GROUP BY bucket, ord ORDER BY ord";
        List<DbReportDistribution> result = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(sql); ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                result.add(new DbReportDistribution(rs.getString("bucket"), rs.getInt("ord"),
                        rs.getLong("tokens"), rs.getLong("postings"),
                        rs.getLong("tokensNew"), rs.getLong("postingsNew")));
            }
        }
        return result;
    }

    private List<DbReportPruneToken> loadReportRanking(Connection conn, long totalPostings, String typeWhere,
                                                       String pruneGenerated, String pruneSort, long bucketMin, long bucketMax) throws SQLException {
        List<String> conds = new ArrayList<>();
        String generatedCond = reportGeneratedPredicate(pruneGenerated);
        if (!generatedCond.isEmpty()) {
            conds.add(generatedCond);
        }
        String where = conds.isEmpty() ? "" : " WHERE " + String.join(" AND ", conds);
        List<String> having = new ArrayList<>();
        having.add("matched > 0");
        if (bucketMin >= 0) {
            having.add("matched >= " + bucketMin);
        }
        if (bucketMax >= 0) {
            having.add("matched <= " + bucketMax);
        }
        String havingClause = having.isEmpty() ? "" : " HAVING " + String.join(" AND ", having);
        String order = "asc".equalsIgnoreCase(pruneSort) ? "ASC" : "DESC";
        String matchedColumn = typeWhere.isEmpty() ? "COUNT(p.object_id)" : "COUNT(o.id)";
        String aloneColumn = typeWhere.isEmpty()
                ? "COALESCE(SUM(CASE WHEN p.isAlone = 1 THEN 1 ELSE 0 END), 0)"
                : "COALESCE(SUM(CASE WHEN o.id IS NOT NULL AND p.isAlone = 1 THEN 1 ELSE 0 END), 0)";
        String rankingJoin = typeWhere.isEmpty()
                ? " LEFT JOIN posting p ON p.token_id = t.id"
                : " LEFT JOIN posting p ON p.token_id = t.id LEFT JOIN \"object\" o ON o.id = p.object_id AND " + typeWhere;
        String sql = "SELECT t.id, t.name, t.isCommon, t.isFrequent, " + matchedColumn + " matched, " + aloneColumn + " alone"
                + " FROM token t" + rankingJoin + where
                + " GROUP BY t.id" + havingClause
                + " ORDER BY matched " + order + ", t.name ASC LIMIT 100";
        List<long[]> rows = new ArrayList<>();
        List<String> names = new ArrayList<>();
        Map<Long, Long> matchedByToken = new LinkedHashMap<>();
        try (PreparedStatement ps = conn.prepareStatement(sql); ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                long id = rs.getLong("id");
                long matched = rs.getLong("matched");
                rows.add(new long[]{id, matched, rs.getLong("alone"), rs.getInt("isCommon"), rs.getInt("isFrequent")});
                names.add(rs.getString("name"));
                matchedByToken.put(id, matched);
            }
        }
        Map<Long, List<DbReportTagHit>> topTags = loadTopTags(conn, matchedByToken, typeWhere);
        List<DbReportPruneToken> result = new ArrayList<>();
        long cumulative = 0;
        for (int i = 0; i < rows.size(); i++) {
            long[] row = rows.get(i);
            cumulative += row[1];
            double cumulativePct = totalPostings > 0 ? 100.0 * cumulative / totalPostings : 0;
            result.add(new DbReportPruneToken(names.get(i), row[3] != 0, row[4] != 0, row[1], row[2],
                    cumulativePct, topTags.getOrDefault(row[0], List.of())));
        }
        return result;
    }

    private List<TestCaseObject> loadObjectTestCases(Connection conn, String typeWhere) throws SQLException {
        String typeCond = typeWhere.isEmpty() ? "" : " WHERE " + typeWhere;
        String matchedJoin = typeWhere.isEmpty() ? "" : " JOIN \"object\" mo ON mo.id = mp.object_id";
        String matchedCond = typeWhere.isEmpty() ? "" : " WHERE " + typeWhere.replace("o.", "mo.");
        String normalizedName = "(' ' || lower(replace(replace(replace(replace(replace(replace(o.name, '''', ' '), '.', ' '), ',', ' '), '-', ' '), '/', ' '), char(8217), ' ')) || ' ')";
        String normalizedToken = "lower(replace(replace(replace(replace(replace(replace(t.name, '''', ' '), '.', ' '), ',', ' '), '-', ' '), '/', ' '), char(8217), ' '))";
        String nameTokenMatch = "length(trim(" + normalizedToken + ")) > 0 AND instr(" + normalizedName + ", ' ' || " + normalizedToken + " || ' ') > 0";
        String demotedNameToken = "nameToken = 1 AND (isCommon = 1 OR isFrequent = 1)";
        String potentialMainToken = "nameToken = 1 AND isCommon = 0 AND isFrequent = 0";
        String sql = "WITH token_matched AS ("
                + " SELECT mp.token_id, COUNT(*) matched"
                + " FROM posting mp" + matchedJoin + matchedCond
                + " GROUP BY mp.token_id),"
                + " name_tokens AS ("
                + " SELECT o.id objectId, o.name, o.type, o.lat, o.lon, t.name tokenName, t.isCommon, t.isFrequent, t.isGenerated,"
                + " COALESCE(tm.matched, 0) matched,"
                + " CASE WHEN " + nameTokenMatch + " THEN 1 ELSE 0 END nameToken"
                + " FROM (SELECT DISTINCT object_id, token_id FROM posting) p"
                + " JOIN \"object\" o ON o.id = p.object_id"
                + " JOIN token t ON t.id = p.token_id"
                + " LEFT JOIN token_matched tm ON tm.token_id = t.id"
                + typeCond
                + ")"
                + " SELECT name, type, lat, lon,"
                + " SUM(nameToken) tokens,"
                + " SUM(CASE WHEN " + demotedNameToken + " THEN 1 ELSE 0 END) commonFrequentTokens,"
                + " SUM(CASE WHEN nameToken = 1 AND isCommon = 1 THEN 1 ELSE 0 END) commonTokens,"
                + " SUM(CASE WHEN nameToken = 1 AND isFrequent = 1 THEN 1 ELSE 0 END) frequentTokens,"
                + " SUM(CASE WHEN nameToken = 1 AND isGenerated = 1 THEN 1 ELSE 0 END) newTokens,"
                + " SUM(CASE WHEN " + potentialMainToken + " THEN 1 ELSE 0 END) potentialMainTokens,"
                + " MAX(CASE WHEN " + potentialMainToken + " THEN matched ELSE 0 END) potentialMainMatched,"
                + " MAX(CASE WHEN " + potentialMainToken + " AND isGenerated = 1 THEN 1 ELSE 0 END) hasGeneratedPotentialMain,"
                + " GROUP_CONCAT(CASE WHEN " + demotedNameToken + " THEN tokenName ELSE NULL END, ', ') commonFrequentNames"
                + " FROM name_tokens"
                + " GROUP BY objectId"
                + " HAVING commonFrequentTokens >= 2"
                + " ORDER BY (1.0 * commonFrequentTokens * commonFrequentTokens / CASE WHEN tokens > 0 THEN tokens ELSE 1 END)"
                + " * CASE WHEN potentialMainTokens <= 1 THEN 2.0 ELSE 1.0 / potentialMainTokens END"
                + " * (1.0 + potentialMainMatched / 1000.0 + hasGeneratedPotentialMain) DESC,"
                + " commonFrequentTokens DESC, potentialMainTokens ASC, potentialMainMatched DESC, tokens DESC, name ASC"
                + " LIMIT 100";
        List<TestCaseObject> result = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(sql); ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                long tokens = rs.getLong("tokens");
                long commonFrequentTokens = rs.getLong("commonFrequentTokens");
                long potentialMainTokens = rs.getLong("potentialMainTokens");
                long potentialMainMatched = rs.getLong("potentialMainMatched");
                double baseScore = tokens > 0 ? 1.0 * commonFrequentTokens * commonFrequentTokens / tokens : 0;
                double mainTokenScarcity = potentialMainTokens <= 1 ? 2.0 : 1.0 / potentialMainTokens;
                double mainTokenFrequency = 1.0 + potentialMainMatched / 1000.0 + rs.getLong("hasGeneratedPotentialMain");
                double proneScore = baseScore * mainTokenScarcity * mainTokenFrequency;
                Double lat = rs.getObject("lat") == null ? null : rs.getDouble("lat");
                Double lon = rs.getObject("lon") == null ? null : rs.getDouble("lon");
                LatLon point = lat == null || lon == null ? null : new LatLon(lat, lon);
                result.add(new TestCaseObject(rs.getString("name"), rs.getString("type"),
                        point, tokens, commonFrequentTokens,
                        rs.getLong("commonTokens"), rs.getLong("frequentTokens"),
                        rs.getLong("newTokens"), proneScore, rs.getString("commonFrequentNames")));
            }
        }
        return result;
    }

    private Map<Long, List<DbReportTagHit>> loadTopTags(Connection conn, Map<Long, Long> matchedByToken, String typeWhere)
            throws SQLException {
        Map<Long, List<DbReportTagHit>> result = new LinkedHashMap<>();
        if (matchedByToken.isEmpty()) {
            return result;
        }
        String objectJoin = typeWhere.isEmpty() ? "" : " JOIN \"object\" o ON o.id = otv.object_id";
        String typeCond = typeWhere.isEmpty() ? "" : " AND " + typeWhere;
        String placeholders = String.join(",", Collections.nCopies(matchedByToken.size(), "?"));
        String sql = "SELECT token_id, tag, hits FROM ("
                + " SELECT otv.token_id, tg.name tag, COUNT(DISTINCT otv.object_id) hits,"
                + " ROW_NUMBER() OVER (PARTITION BY otv.token_id ORDER BY COUNT(DISTINCT otv.object_id) DESC, tg.name ASC) rn"
                + " FROM object_tag_value otv JOIN tag tg ON tg.id = otv.tag_id" + objectJoin
                + " WHERE otv.token_id IN (" + placeholders + ")" + typeCond
                + " GROUP BY otv.token_id, tg.id)"
                + " WHERE rn <= 3 ORDER BY token_id, hits DESC";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            int idx = 1;
            for (Long id : matchedByToken.keySet()) {
                ps.setLong(idx++, id);
            }
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    long tokenId = rs.getLong("token_id");
                    long hits = rs.getLong("hits");
                    long matched = matchedByToken.getOrDefault(tokenId, 0L);
                    double sharePct = matched > 0 ? 100.0 * hits / matched : 0;
                    result.computeIfAbsent(tokenId, k -> new ArrayList<>())
                            .add(new DbReportTagHit(rs.getString("tag"), hits, sharePct));
                }
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

    record AddressPoiTagFilter(boolean enabled, String sql, List<Object> params) {
    }
}
