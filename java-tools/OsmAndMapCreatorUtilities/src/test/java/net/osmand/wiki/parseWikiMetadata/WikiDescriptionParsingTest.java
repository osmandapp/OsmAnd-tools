package net.osmand.wiki.parseWikiMetadata;

import net.osmand.travel.WikivoyageLangPreparation.WikivoyageTemplates;
import org.junit.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import net.osmand.wiki.WikiDatabasePreparation;

public class WikiDescriptionParsingTest {

	@Test
	public void test1() throws IOException, SQLException {
		Map<String, String> webResults = new HashMap<>();
		invoke(informationBlock("|description={{uk|1=Test Place}}\n"), webResults, "uk");
		assertEquals("Test Place", webResults.get("description"));
	}

	@Test
	public void test2() throws IOException, SQLException {
		Map<String, String> webResults = new HashMap<>();
		invoke(informationBlock("|description=Some text description\n"), webResults, "en");
		assertEquals("Some text description", webResults.get("description"));
	}

	@Test
	public void test3() throws IOException, SQLException {
		Map<String, String> webResults = new HashMap<>();
		invoke(informationBlock("|description={{en|1=English description}}{{uk|1=Ukrainian description}}\n"), webResults, "en");
		assertEquals("English description", webResults.get("description"));
	}

	@Test
	public void test4() throws IOException, SQLException {
		Map<String, String> webResults = new HashMap<>();
		invoke(informationBlock("|description={{uk|1=Ukrainian description}}\n"), webResults, "en");
		assertEquals("Ukrainian description", webResults.get("description"));
	}

	@Test
	public void test5() throws IOException, SQLException {
		Map<String, String> webResults = new HashMap<>();
		invoke(blockPhotograph("| Description    = 500px provided description: TestCity [#tag1 ,#tag2 ,#tag3]\n"), webResults, "en");
		assertEquals("TestCity [#tag1 ,#tag2 ,#tag3]", webResults.get("description"));
	}

	@Test
	public void test6() throws IOException, SQLException {
		Map<String, String> webResults = new HashMap<>();
		invoke(informationBlock("|description={{Monument Ukraine|80-391-0151}}\n"), webResults, "en");
		String result = webResults.get("description");
		assertEquals("Monument Ukraine", result);
	}

	@Test
	public void test7() throws IOException, SQLException {
		Map<String, String> webResults = new HashMap<>();
		invoke(informationBlock("|author=Test Author\n|date=2023-01-01\n"), webResults, "en");
		assertNull("Description should be null when not found", webResults.get("description"));
	}

	@Test
	public void test8() throws IOException, SQLException {
		Map<String, String> webResults = new HashMap<>();
		invoke(informationBlock("|Description={{en|Paris: Eiffel tower}} {{de|Paris: Eiffelturm}} {{pl|Wieża Eiffla w Paryżu}} {{fi|Eiffel-torni Pariisissa}} {{ru|Эйфелева башня в Париже}}\n"), webResults, "en");
		String result = webResults.get("description");
		assertEquals("Paris: Eiffel tower", result);
	}

	@Test
	public void test9() throws IOException, SQLException {
		Map<String, String> webResults = new HashMap<>();
		invoke(informationBlock("| Description = Paris from the Arc de Triomphe\n"), webResults, "en");
		String result = webResults.get("description");
		assertEquals("Paris from the Arc de Triomphe", result);
	}

	private void invoke(String text, Map<String, String> webResults, String lang)
			throws IOException, SQLException {
		Map<WikivoyageTemplates, List<String>> blockResults = new EnumMap<>(WikivoyageTemplates.class);
		WikiDatabasePreparation.removeMacroBlocks(new StringBuilder(text), webResults, blockResults,
				null, lang, null, null, null);
		WikiDatabasePreparation.prepareMetaData(webResults);
	}

	private static String informationBlock(String content) {
		return "=={{int:filedesc}}==\n" +
				"{{Information\n" +
				content +
				"}}\n" +
				"\n";
	}

	private static String blockPhotograph(String content) {
		return "== {{int:filedesc}} ==\n" +
				"{{Photograph\n" +
				content +
				"}}\n" +
				"\n";
	}
}

