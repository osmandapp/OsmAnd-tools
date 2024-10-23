package net.osmand.server.utilities;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.concurrent.TimeUnit;

public class WikidataUtilities {

	private static final long PARSE_LICENSE_INTERVAL = TimeUnit.DAYS.toMillis(30);

	public static void main(String[] args) {
		parseWikidataLicenses("../../../osmand/web/map/src/resources/wiki_data_licenses.json");
	}

	public static void parseWikidataLicenses(String filePath) {
		File file = new File(filePath);
		if (!file.exists() || file.lastModified() + PARSE_LICENSE_INTERVAL > System.currentTimeMillis()) {
			String data = fetchWikidataLicenses();
			if (data != null) {
				saveToFile(data, filePath);
			}
		}
	}

	private static String fetchWikidataLicenses() {
		HttpURLConnection connection = null;
		String query = """
				SELECT ?item ?itemLabel ?value
				WHERE {
				  ?item wdt:P2479 ?value .
				  SERVICE wikibase:label {
				    bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en".
				  }
				}
				""";

		String encodedQuery = URLEncoder.encode(query, StandardCharsets.UTF_8);
		String urlStr = "https://query.wikidata.org/sparql?query=" + encodedQuery;

		try {
			URL url = new URL(urlStr);
			connection = (HttpURLConnection) url.openConnection();
			connection.setRequestMethod("GET");
			connection.setRequestProperty("User-Agent", "OsmAnd Java Server");
			connection.setRequestProperty("Accept", "application/json");

			int responseCode = connection.getResponseCode();

			try (BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()))) {
				StringBuilder content = new StringBuilder();
				String inputLine;

				while ((inputLine = in.readLine()) != null) {
					content.append(inputLine).append("\n");
				}

				String rawData = content.toString();

				if (responseCode != HttpURLConnection.HTTP_OK) {
					return null;
				}
				return rawData;
			}
		} catch (IOException e) {
			return null;
		} finally {
			if (connection != null) {
				connection.disconnect();
			}
		}
	}

	private static void saveToFile(String data, String filePath) {
		try {
			File file = new File(filePath);
			file.getParentFile().mkdirs();
			file.createNewFile();
			Files.write(file.toPath(), data.getBytes());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
