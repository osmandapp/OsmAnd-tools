package net.osmand.wiki.parseWikiMetadata;

public class WikiTestUtils {
	static String informationBlock(String content) {
		return "=={{int:filedesc}}==\n" +
				"{{Information\n" +
				content +
				"}}\n" +
				"\n";
	}

	static String artworkBlock(String content) {
		return "=={{int:filedesc}}==\n" +
				"{{Artwork\n" +
				content +
				"}}\n" +
				"\n";
	}

	static String blockPhotograph(String content) {
		return "== {{int:filedesc}} ==\n" +
				"{{Photograph\n" +
				content +
				"}}\n" +
				"\n";
	}

	static String blockMilim(String content) {
		return "== {{int:filedesc}} ==\n" +
				"{{milim\n" +
				content +
				"}}\n" +
				"\n";
	}
}
