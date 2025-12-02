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

public class WikiLicenseParsingTest {

	@Test
	public void test1() throws IOException, SQLException {
		Map<String, String> webResults = new HashMap<>();
		invoke(licenseBlockWithTemplate("{{Self|author={{user at project|TestUser|testwiki|en}}|GFDL|CC-BY-SA-2.5|migration=relicense}}"), webResults);
		assertEquals("SELF - GFDL - CC BY-SA-2.5 - MIGRATION=RELICENSE", webResults.get("license"));
	}

	@Test
	public void test2() throws IOException, SQLException {
		Map<String, String> webResults = new HashMap<>();
		invoke(licenseBlockWithTemplate("{{self|cc-by-sa-3.0}}"), webResults);
		assertEquals("SELF - CC-BY-SA-3.0", webResults.get("license"));
	}

	@Test
	public void test3() throws IOException, SQLException {
		Map<String, String> webResults = new HashMap<>();
		invoke(licenseBlockWithTemplate("{{cc-by-2.0}}"), webResults);
		assertEquals("CC-BY-2.0", webResults.get("license"));
	}

	@Test
	public void test4() throws IOException, SQLException {
		Map<String, String> webResults = new HashMap<>();
		invoke(licenseBlockWithTemplate("{{RCE-license}}"), webResults);
		assertEquals("RCE-LICENSE", webResults.get("license"));
	}

	@Test
	public void test5() throws IOException, SQLException {
		Map<String, String> webResults = new HashMap<>();
		invoke(informationBlock("|license={{cc-by-sa-3.0|Author Name}}\n"), webResults);
		assertEquals("CC-BY-SA-3.0", webResults.get("license"));
	}

	@Test
	public void test6() throws IOException, SQLException {
		Map<String, String> webResults = new HashMap<>();
		invoke(informationBlock("|permission={{cc-by-sa-3.0|TestUser}}\n"), webResults);
		assertEquals("CC-BY-SA-3.0", webResults.get("license"));
	}

	@Test
	public void test7() throws IOException, SQLException {
		Map<String, String> webResults = new HashMap<>();
		invoke(informationBlock("|permission={{User:FlickreviewR/reviewed-pass|Test Archive|https://example.com/photo/123|2016-11-27 10:53:09|No known copyright restrictions|}}\n"), webResults);
		assertEquals("NO KNOWN COPYRIGHT RESTRICTIONS", webResults.get("license"));
	}

	@Test
	public void test8() throws IOException, SQLException {
		Map<String, String> webResults = new HashMap<>();
		invoke(blockPhotograph("| Permission     = {{Cc-by-3.0}}\n"), webResults);
		assertEquals("CC-BY-3.0", webResults.get("license"));
	}

	@Test
	public void test9() throws IOException, SQLException {
		Map<String, String> webResults = new HashMap<>();
		invoke(blockMilim("| author = Author\n") +
				"== {{int:license-header}} ==\n" +
				"{{PD-USGov-Military-Navy}}\n" +
				"\n", webResults);
		assertEquals("PD USGOV-MILITARY-NAVY", webResults.get("license"));
	}

	@Test
	public void test10() throws IOException, SQLException {
		Map<String, String> webResults = new HashMap<>();
		invoke(informationBlock("|author={{unknown|author}}\n") +
				"== {{int:license-header}} ==\n" +
				"{{PD-art}}\n" +
				"\n", webResults);
		assertEquals("PD ART", webResults.get("license"));
	}

	private void invoke(String text, Map<String, String> webResults)
			throws IOException, SQLException {
		Map<WikivoyageTemplates, List<String>> blockResults = new EnumMap<>(WikivoyageTemplates.class);
		WikiDatabasePreparation.removeMacroBlocks(new StringBuilder(text), webResults, blockResults,
				null, null, null, null, true);
		WikiDatabasePreparation.prepareMetaData(webResults);
	}

	private static String licenseBlockWithTemplate(String template) {
		return "=={{int:filedesc}}==\n" +
				"=={{int:license-header}}==\n" +
				template + "\n" +
				"\n";
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

	private static String blockMilim(String content) {
		return "== {{int:filedesc}} ==\n" +
				"{{milim\n" +
				content +
				"}}\n" +
				"\n";
	}
}

