package net.osmand.data.changeset;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class CalculateCountryForChangesets {
	
	public static void main(String[] args) throws SQLException {
		if(args[0].equals("calculate_countries")) {
			calculateCountries();
		}
	}

	private static void calculateCountries() throws SQLException {
		System.out.println("Connect " + System.getenv("DB_USER"));
		Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/changeset",
				System.getProperty("DB_USER"), System.getenv("DB_PWD"));
		try {
			Statement stat = conn.createStatement();
			ResultSet rs = stat.executeQuery("SELECT COUNT(*) FROM changesets");
			System.out.println(rs.next());
			System.out.println(rs.getInt(1));
		} finally {
			conn.close();
		}
		
	}

}
