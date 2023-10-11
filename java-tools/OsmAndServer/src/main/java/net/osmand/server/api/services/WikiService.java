package net.osmand.server.api.services;

import com.google.gson.Gson;
import net.osmand.obf.preparation.DBDialect;
import net.osmand.util.Algorithms;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

@Service
public class WikiService {
	protected static final Log log = LogFactory.getLog(WikiService.class);
	public static final String WIKIMEDIA_COMMON_SPECIAL_FILE_PATH = "https://commons.wikimedia.org/wiki/Special:FilePath/";

	@Value("${osmand.wiki.location}")
	private String pathToWikiSqlite;

	public void processWikiImages(HttpServletRequest request, HttpServletResponse response, Gson gson) {
		try {
			DBDialect osmDBdialect = DBDialect.SQLITE;
			Set<String> images = new LinkedHashSet<>();
			File sqliteFile = new File(pathToWikiSqlite, "commonswiki.sqlite");
			if (sqliteFile.exists()) {
				Connection conn = osmDBdialect.getDatabaseConnection(sqliteFile.getAbsolutePath(), log);
				String articleId = request.getParameter("article");
				if (articleId != null) {
					articleId = articleId.startsWith("Q") ? articleId.substring(1) : articleId;
					addImage(conn, articleId, images);
					addImagesFromCategory(conn, articleId, images);
					addImagesFromDepict(conn, articleId, images);
				}
				String categoryName = request.getParameter("category");
				if (categoryName != null) {
					addImagesFromCategoryByName(conn, categoryName, images);
				}
				response.setCharacterEncoding("UTF-8");
				response.getWriter().println(gson.toJson(Collections.singletonMap("features", images)));
			} else {
				log.error("commonswiki.sqlite file doesn't exist");
			}
		} catch (IOException | SQLException e) {
			throw new RuntimeException(e);
		}
	}

	private void addImage(Connection conn, String articleId, Set<String> images) throws SQLException {
		String selectQuery = "SELECT value FROM wikidata_properties where id=? and type='P18'";
		addImagesFromQuery(conn, articleId, images, selectQuery);
	}

	private void addImagesFromDepict(Connection conn, String articleId, Set<String> images) throws SQLException {
		String selectQuery = "SELECT name FROM common_content " +
				"INNER JOIN common_depict ON common_depict.id = common_content.id " +
				"WHERE common_depict.depict_qid = ?";
		addImagesFromQuery(conn, articleId, images, selectQuery);
	}

	private void addImagesFromCategory(Connection conn, String articleId, Set<String> images) throws SQLException {
		String selectQuery = "SELECT common_content_1.name FROM common_content " +
				"INNER JOIN wikidata_properties ON common_content.name = value " +
				"INNER JOIN common_category_links ON category_id = common_content.id " +
				"INNER JOIN common_content common_content_1 ON common_category_links.id = common_content_1.id " +
				"WHERE wikidata_properties.id = ? AND type = 'P373'";
		addImagesFromQuery(conn, articleId, images, selectQuery);
	}

	private void addImagesFromCategoryByName(Connection conn, String categoryName, Set<String> images) throws SQLException {
		String selectQuery = "SELECT common_content_1.name FROM common_content " +
				"JOIN common_category_links ON common_category_links.category_id = common_content.id AND common_content.name = ? " +
				"JOIN common_content common_content_1 ON common_category_links.id = common_content_1.id";
		addImagesFromQuery(conn, categoryName, images, selectQuery);
	}

	private void addImagesFromQuery(Connection conn, String param, Set<String> images, String selectQuery) throws SQLException {
		PreparedStatement selectImageFileNames = conn.prepareStatement(selectQuery);
		selectImageFileNames.setString(1, param);
		ResultSet rs = selectImageFileNames.executeQuery();
		while (rs.next()) {
			String fileName = rs.getString(1);
			if (!Algorithms.isEmpty(fileName)) {
				String url = WIKIMEDIA_COMMON_SPECIAL_FILE_PATH + fileName;
				images.add(url);
			}
		}
	}
}
