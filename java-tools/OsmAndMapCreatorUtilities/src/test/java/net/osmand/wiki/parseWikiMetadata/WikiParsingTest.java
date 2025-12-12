package net.osmand.wiki.parseWikiMetadata;

import net.osmand.travel.WikivoyageLangPreparation.WikivoyageTemplates;
import net.osmand.wiki.WikiDatabasePreparation;
import org.junit.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class WikiParsingTest {

	@Test
	public void test1() throws IOException, SQLException {
		Map<String, String> webResults = new HashMap<>();
		invoke(textWithNowikiAndUnbalancedBraces(), webResults, "en");
		assertEquals("Base description", webResults.get("description"));
	}

	@Test
	public void test2() throws IOException, SQLException {
		Map<String, String> webResults = new HashMap<>();
		invoke(textWithNowikiWithoutClosingTag(), webResults, "en");
		assertEquals("Base description", webResults.get("description"));
	}

	@Test
	public void test3() throws IOException, SQLException {
		Map<String, String> webResults = new HashMap<>();
		invoke(textWithMultipleNowikiBlocks(), webResults, "en");
		assertEquals("Base description", webResults.get("description"));
	}

	@Test
	public void test4() throws IOException, SQLException {
		Map<String, String> webResults = new HashMap<>();
		invoke(textWithMixedCaseNowiki(), webResults, "en");
		assertEquals("Base description", webResults.get("description"));
	}

	@Test
	public void test5() throws IOException, SQLException {
		Map<String, String> webResults = new HashMap<>();
		invoke(textWithEmptyNowiki(), webResults, "en");
		assertEquals("Base description", webResults.get("description"));
	}

	private void invoke(String text, Map<String, String> webResults, String lang)
			throws IOException, SQLException {
		Map<WikivoyageTemplates, List<String>> blockResults = new EnumMap<>(WikivoyageTemplates.class);
		WikiDatabasePreparation.removeMacroBlocks(new StringBuilder(text), webResults, blockResults,
				null, lang, null, null, null);
		WikiDatabasePreparation.prepareMetaData(webResults);
	}

	private static String baseInformationBlock() {
		return "=={{int:filedesc}}==\n" +
				"{{Information\n" +
				"|Description=Base description\n" +
				"}}\n";
	}

	private static String textWithNowikiAndUnbalancedBraces() {
		return baseInformationBlock() +
				"\n" +
				"== {{original upload log}} ==\n" +
				"''<nowiki>{{Information |Description= [[:ua:TestTopic|Test topic]]-3 regions of Exampleland. " +
				"|Source=http://example.org/wiki/File:Test_Regions.png |Date=2006-01-26 |Author=[[User:FictionalUser]] " +
				"|Permission={{GFDL-with-disclaimers|</nowiki>''\n";
	}

	private static String textWithNowikiWithoutClosingTag() {
		return baseInformationBlock() +
				"\n" +
				"<nowiki>{{Unclosed template with {{braces}\n";
	}

	private static String textWithMultipleNowikiBlocks() {
		return baseInformationBlock() +
				"\n" +
				"<nowiki>First nowiki block {{with braces}}</nowiki>\n" +
				"Some normal text between.\n" +
				"<nowiki>Second nowiki block {{also with braces}}</nowiki>\n";
	}

	private static String textWithMixedCaseNowiki() {
		return baseInformationBlock() +
				"\n" +
				"<NoWiki>Upper/mixed case nowiki {{braces}}</NoWIKI>\n";
	}

	private static String textWithEmptyNowiki() {
		return baseInformationBlock() +
				"\n" +
				"<nowiki></nowiki>\n";
	}
}

