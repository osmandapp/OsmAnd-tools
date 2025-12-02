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
import net.osmand.wiki.WikiDatabasePreparation;

public class WikiAuthorParsingTest {

	@Test
	public void test1() throws IOException, SQLException {
		Map<String, String> webResults = new HashMap<>();
		invoke(informationBlock("|author=[[User:TestUser|TestUser]]\n"), webResults);
		assertEquals("TestUser", webResults.get("author"));
	}

	@Test
	public void test2() throws IOException, SQLException {
		Map<String, String> webResults = new HashMap<>();
		invoke(informationBlock("|author={{Creator:John Doe}}\n"), webResults);
		assertEquals("John Doe", webResults.get("author"));
	}

	@Test
	public void test3() throws IOException, SQLException {
		Map<String, String> webResults = new HashMap<>();
		invoke(informationBlock("|author=[https://example.com/archive/photo/123 Test User]\n"), webResults);
		assertEquals("Test User", webResults.get("author"));
	}

	@Test
	public void test4() throws IOException, SQLException {
		Map<String, String> webResults = new HashMap<>();
		invoke(informationBlock("|author={{self2|GFDL|cc-by-sa-3.0|author=[[User:TestUser|Test Author]]}}\n"), webResults);
		assertEquals("Test Author", webResults.get("author"));
	}

	@Test
	public void test5() throws IOException, SQLException {
		Map<String, String> webResults = new HashMap<>();
		invoke(artworkBlock("|author=Plain Author Name\n"), webResults);
		assertEquals("Plain Author Name", webResults.get("author"));
	}

	@Test
	public void test6() throws IOException, SQLException {
		Map<String, String> webResults = new HashMap<>();
		invoke(informationBlock("|author=[[User]]\n"), webResults);
		assertEquals("User", webResults.get("author"));
	}

	@Test
	public void test7() throws IOException, SQLException {
		Map<String, String> webResults = new HashMap<>();
		invoke(informationBlock("|author=[[User:Name]]\n"), webResults);
		assertEquals("Name", webResults.get("author"));
	}

	@Test
	public void test8() throws IOException, SQLException {
		Map<String, String> webResults = new HashMap<>();
		invoke(informationBlock("|author=[https://example.com SomeUser]\n"), webResults);
		assertEquals("SomeUser", webResults.get("author"));
	}

	@Test
	public void test9() throws IOException, SQLException {
		Map<String, String> webResults = new HashMap<>();
		invoke(informationBlock("|author={{User:Name}}\n"), webResults);
		assertEquals("Name", webResults.get("author"));
	}

	@Test
	public void test10() throws IOException, SQLException {
		Map<String, String> webResults = new HashMap<>();
		invoke(informationBlock("|author=[https://example.com]\n"), webResults);
		assertEquals("Unknown", webResults.get("author"));
	}

	@Test
	public void test11() throws IOException, SQLException {
		Map<String, String> webResults = new HashMap<>();
		invoke(informationBlock("|author={{FlickreviewR|author=John Doe, Ph.D. - Test Archive|other=value}}\n"), webResults);
		assertEquals("John Doe", webResults.get("author"));
	}

	@Test
	public void test12() throws IOException, SQLException {
		Map<String, String> webResults = new HashMap<>();
		invoke(informationBlock("|author={{User:TestAuthor/Author}}\n"), webResults);
		assertEquals("TestAuthor", webResults.get("author"));
	}

	@Test
	public void test13() throws IOException, SQLException {
		Map<String, String> webResults = new HashMap<>();
		invoke(informationBlock("|author=Name edited by Someone\n"), webResults);
		assertEquals("Name", webResults.get("author"));
	}

	@Test
	public void test14() throws IOException, SQLException {
		Map<String, String> webResults = new HashMap<>();
		invoke(informationBlock("|author=Publisher:Name\n"), webResults);
		assertEquals("Name", webResults.get("author"));
	}

	@Test
	public void test15() throws IOException, SQLException {
		Map<String, String> webResults = new HashMap<>();
		invoke(informationBlock("|author={{creator:Test Author}}\n"), webResults);
		assertEquals("Test Author", webResults.get("author"));
	}

	@Test
	public void test16() throws IOException, SQLException {
		Map<String, String> webResults = new HashMap<>();
		invoke(informationBlock("|author=[[:en:Test Organization|Test Organization]]\n"), webResults);
		assertEquals("Test Organization", webResults.get("author"));
	}

	@Test
	public void test17() throws IOException, SQLException {
		Map<String, String> webResults = new HashMap<>();
		invoke(informationBlock("|author=[[:en:Test Organization]]\n"), webResults);
		assertEquals("Test Organization", webResults.get("author"));
	}

	@Test
	public void test18() throws IOException, SQLException {
		Map<String, String> webResults = new HashMap<>();
		invoke(blockPhotograph("|photographer=[https://example.com/testuser Test Author]\n"), webResults);
		assertEquals("Test Author", webResults.get("author"));
	}

	@Test
	public void test19() throws IOException, SQLException {
		Map<String, String> webResults = new HashMap<>();
		invoke(blockMilim("|author=Test Author\n|date=2009-07-24\n"), webResults);
		assertEquals("Test Author", webResults.get("author"));
		assertEquals("2009-07-24", webResults.get("date"));
	}

	@Test
	public void test20() throws IOException, SQLException {
		Map<String, String> webResults = new HashMap<>();
		invoke(informationBlock("|author={{Author assumed|[[User:TestUser~commonswiki|TestUser~commonswiki]]}}\n"), webResults);
		assertEquals("TestUser~commonswiki", webResults.get("author"));
	}

	@Test
	public void test21() throws IOException, SQLException {
		Map<String, String> webResults = new HashMap<>();
		invoke(informationBlock("|author={{various}}\n"), webResults);
		assertEquals("various", webResults.get("author"));
	}

	@Test
	public void test22() throws IOException, SQLException {
		Map<String, String> webResults = new HashMap<>();
		invoke(informationBlock("|author=[[:Category:TestAuthor|TestAuthor (1920-1993)]]\n"), webResults);
		assertEquals("TestAuthor (1920-1993)", webResults.get("author"));
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

	private static String artworkBlock(String content) {
		return "=={{int:filedesc}}==\n" +
				"{{Artwork\n" +
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

	private static String blockMilim(String content) {
		return "== {{int:filedesc}} ==\n" +
				"{{milim\n" +
				content +
				"}}\n" +
				"\n";
	}
}

