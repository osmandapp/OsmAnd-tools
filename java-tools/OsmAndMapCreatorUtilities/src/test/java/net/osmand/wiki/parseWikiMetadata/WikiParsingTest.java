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
		assertEquals("Test", webResults.get("description"));
	}

	private void invoke(String text, Map<String, String> webResults, String lang)
			throws IOException, SQLException {
		Map<WikivoyageTemplates, List<String>> blockResults = new EnumMap<>(WikivoyageTemplates.class);
		WikiDatabasePreparation.removeMacroBlocks(new StringBuilder(text), webResults, blockResults,
				null, lang, null, null, null);
		WikiDatabasePreparation.prepareMetaData(webResults);
	}

	private static String textWithNowikiAndUnbalancedBraces() {
		return "=={{int:filedesc}}==\n" +
				"{{Information\n" +
				"|Description=Test\n" +
				"}}\n" +
				"\n" +
				"== {{original upload log}} ==\n" +
				"''<nowiki>{{Information |Description= [[:ua:TestTopic|Test topic]]-3 regions of Exampleland. " +
				"|Source=http://example.org/wiki/File:Test_Regions.png |Date=2006-01-26 |Author=[[User:FictionalUser]] " +
				"|Permission={{GFDL-with-disclaimers|</nowiki>''\n";
	}
}

