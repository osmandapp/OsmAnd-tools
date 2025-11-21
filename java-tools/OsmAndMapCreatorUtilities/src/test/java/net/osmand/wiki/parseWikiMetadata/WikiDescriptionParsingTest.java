package net.osmand.wiki.parseWikiMetadata;

import net.osmand.travel.WikivoyageLangPreparation.WikivoyageTemplates;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import net.osmand.wiki.WikiDatabasePreparation;

public class WikiDescriptionParsingTest {

	@Test
	public void test1() throws IOException, SQLException {
		Map<String, String> webResults = new HashMap<>();
		invoke(informationBlock("|description={{uk|1=Kyiv Pechersk Lavra}}\n"), webResults, "uk", null);
		assertEquals("Kyiv Pechersk Lavra", webResults.get("description"));
	}

	@Ignore
	@Test
	public void test2() throws IOException, SQLException {
		Map<String, String> webResults = new HashMap<>();
		invoke(informationBlock("|description=Some text description\n"), webResults, "en", null);
		assertEquals("Some text description", webResults.get("description"));
	}

	@Test
	public void test3() throws IOException, SQLException {
		Map<String, String> webResults = new HashMap<>();
		invoke(informationBlock("|description={{en|1=English description}}{{uk|1=Ukrainian description}}\n"), webResults, "en", null);
		assertEquals("English description", webResults.get("description"));
	}

	@Test
	public void test4() throws IOException, SQLException {
		Map<String, String> webResults = new HashMap<>();
		invoke(informationBlock("|description={{uk|1=Ukrainian description}}\n"), webResults, "en", null);
		assertEquals("Ukrainian description", webResults.get("description"));
	}

	private void invoke(String text, Map<String, String> webResults, String lang, Boolean allLangs)
			throws IOException, SQLException {
		Map<WikivoyageTemplates, List<String>> blockResults = new EnumMap<>(WikivoyageTemplates.class);
		WikiDatabasePreparation.removeMacroBlocks(new StringBuilder(text), webResults, blockResults,
				null, lang, null, null, allLangs);
		WikiDatabasePreparation.prepareMetaData(webResults);
	}

	private static String informationBlock(String content) {
		return "=={{int:filedesc}}==\n" +
				"{{Information\n" +
				content +
				"}}\n" +
				"\n";
	}
}

