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
		invoke(informationBlock("|author=[[User:PersianDutchNetwork|PersianDutchNetwork]]\n"), webResults);
		assertEquals("PersianDutchNetwork", webResults.get("author"));
	}

	@Test
	public void test2() throws IOException, SQLException {
		Map<String, String> webResults = new HashMap<>();
		invoke(informationBlock("|author={{Creator:Johannes Petrus Albertus Antonietti}}\n"), webResults);
		assertEquals("Johannes Petrus Albertus Antonietti", webResults.get("author"));
	}

	@Test
	public void test3() throws IOException, SQLException {
		Map<String, String> webResults = new HashMap<>();
		invoke(informationBlock("|author=[https://web.archive.org/web/20161031223609/http://www.panoramio.com/user/4678999?with_photo_id=118704129 Ben Bender]\n"), webResults);
		assertEquals("Ben Bender", webResults.get("author"));
	}

	@Test
	public void test4() throws IOException, SQLException {
		Map<String, String> webResults = new HashMap<>();
		invoke(informationBlock("|author={{self2|GFDL|cc-by-sa-3.0|author=[[User:Butko|Andrew Butko]]}}\n"), webResults);
		assertEquals("Andrew Butko", webResults.get("author"));
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
		invoke(informationBlock("|author={{FlickreviewR|author=Adam Jones, Ph.D. - Global Photo Archive|other=value}}\n"), webResults);
		assertEquals("Adam Jones", webResults.get("author"));
	}

	@Test
	public void test12() throws IOException, SQLException {
		Map<String, String> webResults = new HashMap<>();
		invoke(informationBlock("|author={{User:Ralf Roletschek/Autor}}\n"), webResults);
		assertEquals("Ralf Roletschek", webResults.get("author"));
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
		invoke(informationBlock("|author={{creator:Diego Delso}}\n"), webResults);
		assertEquals("Diego Delso", webResults.get("author"));
	}

	@Test
	public void test16() throws IOException, SQLException {
		Map<String, String> webResults = new HashMap<>();
		invoke(informationBlock("|author=[[:en:State Emergency Service of Ukraine|State Emergency Service of Ukraine]]\n"), webResults);
		assertEquals("State Emergency Service of Ukraine", webResults.get("author"));
	}

	@Test
	public void test17() throws IOException, SQLException {
		Map<String, String> webResults = new HashMap<>();
		invoke(informationBlock("|author=[[:en:State Emergency Service of Ukraine]]\n"), webResults);
		assertEquals("State Emergency Service of Ukraine", webResults.get("author"));
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
}

