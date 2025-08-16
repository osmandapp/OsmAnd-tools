package net.osmand.server.api.services;

import net.osmand.map.OsmandRegions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static net.osmand.server.controllers.pub.GeojsonClasses.Feature;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class SearchServiceTest {

	@InjectMocks
	private SearchService searchService;

	@Mock
	private OsmAndMapsService osmAndMapsService;

	@Mock
	private OsmandRegions osmandRegions;

	private static final String CSV_FILE_PATH = "search_data.csv";

	@BeforeEach
	void setUp() throws IOException {
		// Mock the behavior of validateAndInitConfig to return true
		when(osmAndMapsService.validateAndInitConfig()).thenReturn(true);
		// Mock getReaders to return an empty list or specific mock readers if needed
		when(osmAndMapsService.getReaders(any(), any())).thenReturn(Collections.emptyList());
		// Mock unlockReaders to do nothing
		// Mock getMapPoiTypes to return a dummy MapPoiTypes object
		// This part needs more detailed mocking depending on how deep the search method goes.
		// For now, let's assume it doesn't fail immediately due to these.
	}

	@Test
	void testSearchWithCsvData() {
		List<SearchDataEntry> searchData = readCsvData(CSV_FILE_PATH);
		assertFalse(searchData.isEmpty(), "CSV data should not be empty");

		for (SearchDataEntry entry : searchData) {
			try {
				List<Feature> results = searchService.search(entry.lat, entry.lon, entry.text, entry.locale, true,
						null, null);
				assertNotNull(results, "Search results should not be null for entry: " + entry);
				// Further assertions can be added here based on expected search results
				// For example: assertTrue(results.size() > 0, "Expected some results for " + entry.text);

			} catch (IOException e) {
				fail("IOException occurred during search for entry: " + entry + ": " + e.getMessage());
			}
		}
	}

	private List<SearchDataEntry> readCsvData(String fileName) {
		List<SearchDataEntry> data = new ArrayList<>();
		try (InputStream is = getClass().getClassLoader().getResourceAsStream(fileName);
			 BufferedReader reader = new BufferedReader(new InputStreamReader(is))) {
			String line;
			boolean firstLine = true; // Skip header
			while ((line = reader.readLine()) != null) {
				if (firstLine) {
					firstLine = false;
					continue;
				}
				String[] parts = line.split(",");
				if (parts.length == 4) {
					double lat = Double.parseDouble(parts[0].trim());
					double lon = Double.parseDouble(parts[1].trim());
					String text = parts[2].trim();
					String locale = parts[3].trim();
					data.add(new SearchDataEntry(lat, lon, text, locale));
				} else {
					System.err.println("Skipping malformed CSV line: " + line);
				}
			}
		} catch (IOException e) {
			fail("Could not read CSV file: " + fileName + ": " + e.getMessage());
		} catch (NumberFormatException e) {
			fail("Error parsing numbers in CSV file: " + fileName + ": " + e.getMessage());
		}
		return data;
	}

	private static class SearchDataEntry {
		double lat;
		double lon;
		String text;
		String locale;

		public SearchDataEntry(double lat, double lon, String text, String locale) {
			this.lat = lat;
			this.lon = lon;
			this.text = text;
			this.locale = locale;
		}
	}
}
