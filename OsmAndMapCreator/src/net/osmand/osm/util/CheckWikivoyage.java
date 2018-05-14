package net.osmand.osm.util;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import net.osmand.util.Algorithms;

public class CheckWikivoyage {

	public static void main(String[] args) throws SQLException, IOException {
		File f = new File("../../../maps/wikivoyage/World_wikivoyage.sqlite");
		System.out.println(System.currentTimeMillis() - 24*60*60*1000*3
				);
//		testWikivoyage(f.getCanonicalPath());
	}

	private static void testWikivoyage(String filepath) throws SQLException, IOException {
		Connection connection = DriverManager.getConnection("jdbc:sqlite:" + filepath);
		Statement statement = connection.createStatement();
		ResultSet rs = statement.executeQuery("Select title, lang, content_gz From travel_articles");
		int count = 0;
		while(rs.next()) {
			byte[] bytes = rs.getBytes(3);
			String cont = Algorithms.gzipToString(bytes);
			if(//cont.contains("[[") || 
					cont.contains("]]")) {
				System.out.println(rs.getString(1) + " " + rs.getString(2));
				if(count > 0) {
					if(count < 20) {
						System.out.println(cont);
					} else {
						break;
					}
				}
				count++;
			}
		}
		System.out.println(count);
	}
}
