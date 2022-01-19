package net.osmand.wiki;

import net.osmand.util.Algorithms;
import net.osmand.util.SqlInsertValuesReader;
import org.apache.commons.codec.digest.DigestUtils;

import java.io.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class WikiImageUrlStorage {
	public static final int FILE_NAME = 0;
	public static final int NAME = 1;
	public static final int THUMB_URL_SELECT = 1;
	public static final int THUMB_URL = 2;
	public static final int WIDTH = 2;
	public static final String LOCALIZED_WIKIPEDIA = "https://upload.wikimedia.org/wikipedia/";
	public static final String LATEST_IMAGE_SQL_DUMP = "wiki-latest-image.sql.gz";
	private final PreparedStatement urlSelectStat;
	private final PreparedStatement urlInsertStat;

	public WikiImageUrlStorage(Connection conn, String workDir, String lang) throws SQLException {
		conn.createStatement().execute("DROP TABLE IF EXISTS image");
		conn.createStatement().execute("CREATE TABLE IF NOT EXISTS image(name text unique, thumb_url text)");
		urlSelectStat = conn.prepareStatement("SELECT thumb_url FROM image where name = ? ");
		urlInsertStat = conn.prepareStatement("INSERT INTO image(name, thumb_url) VALUES(?, ?) ");
		SqlInsertValuesReader.InsertValueProcessor p = vs -> {
			try {
				String fileName = vs.get(FILE_NAME);
				String md5 = DigestUtils.md5Hex(fileName);
				String hash1 = md5.substring(0, 1);
				String hash2 = md5.substring(0, 2);
				boolean isThumb = Integer.parseInt(vs.get(WIDTH)) >= 320;
				String thumbUrl = LOCALIZED_WIKIPEDIA + lang + (isThumb ? "/thumb/" : "/") + hash1 + "/" + hash2 + "/"
						+ fileName + (isThumb ? "/320px-" + fileName : "");
				urlInsertStat.setString(NAME, fileName);
				urlInsertStat.setString(THUMB_URL, thumbUrl);
				urlInsertStat.execute();
			} catch (SQLException | NumberFormatException e) {
				e.printStackTrace();
			}
		};
		try {
			SqlInsertValuesReader.readInsertValuesFile(new File(workDir,lang + LATEST_IMAGE_SQL_DUMP).getAbsolutePath(), p);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public String getThumbUrl(String imageFileName) {
		imageFileName = Algorithms.capitalizeFirstLetter(imageFileName.trim());
		String url = "";
		try {
			urlSelectStat.setString(NAME, imageFileName);
			ResultSet rs = urlSelectStat.executeQuery(); {
				if (rs.next()) {
					url = rs.getString(THUMB_URL_SELECT);
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return url;
	}
}
