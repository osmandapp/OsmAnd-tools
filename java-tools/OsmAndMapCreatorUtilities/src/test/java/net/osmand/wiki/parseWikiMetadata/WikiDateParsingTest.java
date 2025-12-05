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

public class WikiDateParsingTest {

	@Test
	public void test1() throws IOException, SQLException {
		Map<String, String> webResults = new HashMap<>();
		invoke(informationBlock("|date={{Original upload date|2015-04-15}}\n"), webResults);
		assertEquals("2015-04-15", webResults.get("date"));
	}

	@Test
	public void test2() throws IOException, SQLException {
		Map<String, String> webResults = new HashMap<>();
		invoke(informationBlock("|Date={{original upload date|2006-11-05}}\n"), webResults);
		assertEquals("2006-11-05", webResults.get("date"));
	}

	@Test
	public void test3() throws IOException, SQLException {
		Map<String, String> webResults = new HashMap<>();
		invoke(informationBlock("|date=2011-10-08\n"), webResults);
		assertEquals("2011-10-08", webResults.get("date"));
	}

	@Test
	public void test4() throws IOException, SQLException {
		Map<String, String> webResults = new HashMap<>();
		invoke(informationBlock("|Date=2009-12-06 23:11\n"), webResults);
		assertEquals("2009-12-06", webResults.get("date"));
	}

	@Test
	public void test5() throws IOException, SQLException {
		Map<String, String> webResults = new HashMap<>();
		invoke(informationBlock("| Date = 2018-04-06 12:25\n"), webResults);
		assertEquals("2018-04-06", webResults.get("date"));
	}

	@Test
	public void test6() throws IOException, SQLException {
		Map<String, String> webResults = new HashMap<>();
		invoke(informationBlock("|date={{Taken on|2014-03-09|location=Test Location}}\n"), webResults);
		assertEquals("2014-03-09", webResults.get("date"));
	}

	@Test
	public void test7() throws IOException, SQLException {
		Map<String, String> webResults = new HashMap<>();
		invoke(blockPhotograph("|Date=2017-09-15 09:36:26 (UTC)\n"), webResults);
		assertEquals("2017-09-15", webResults.get("date"));
	}

	@Test
	public void test8() throws IOException, SQLException {
		Map<String, String> webResults = new HashMap<>();
		invoke(informationBlock("|description=Test description\n|author=Test Author\n"), webResults);
		assertNull("Date should be null when not found", webResults.get("date"));
	}

	private void invoke(String text, Map<String, String> webResults)
			throws IOException, SQLException {
		Map<WikivoyageTemplates, List<String>> blockResults = new EnumMap<>(WikivoyageTemplates.class);
		WikiDatabasePreparation.removeMacroBlocks(new StringBuilder(text), webResults, blockResults,
				null, null, null, null, true);
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

