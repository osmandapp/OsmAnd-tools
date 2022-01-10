package net.osmand.wiki.wikidata;

import net.osmand.wiki.WikiDatabasePreparation;
import net.osmand.wiki.WikipediaByCountryDivider;
import org.xml.sax.SAXException;
import org.xmlpull.v1.XmlPullParserException;
import org.xwiki.component.manager.ComponentLookupException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.*;
import java.sql.SQLException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class ArticleExtractor {
	// for large files if error occurs:
	// "The accumulated size of entities is "50,000,001" that exceeded the "50,000,000" limit set by "FEATURE_SECURE_PROCESSING"
	// need to add three java arguments -DentityExpansionLimit=2147480000 -DtotalEntitySizeLimit=2147480000
	// -Djdk.xml.totalEntitySizeLimit=2147480000
	public static final String helpMessage = " --mode=<cut|test> --lang=<lang> --title=<title> --dir=<work_folder> " +
			"--testID=<article_ID> --LatLon=<testLatLon>\n" +
			"mode cut - cut out article with <title> from <lang>wiki-latest-pages-articles.xml.gz and create obf file \n" +
			"to the <LatLon> coordinates(lat;lon)\n" +
			"mode test - create obf file for article with <articleID> to the <LatLon> coordinates(lat;lon)\n";

	public static void main(String[] args) throws IOException {
		String lang = "en";
		String workDir = "";
		String mode = "test";
		String articleID = "";
		String articleLatLon = "50.45191;30.59195";
		String articleTitle = "";
		for (String arg : args) {
			String val = arg.substring(arg.indexOf("=") + 1);
			String key = arg.substring(0, arg.indexOf("=") + 1);
			switch (key) {
				case "--mode=":
					mode = val;
					break;
				case "--lang=":
					lang = val;
					break;
				case "--title=":
					articleTitle = val;
					break;
				case "--dir=":
					workDir = val;
					break;
				case "--articleID=":
					articleID = val;
					break;
				case "--LatLon=":
					articleLatLon = val;
					break;
			}
		}
		if (mode.isEmpty() || !(mode.equals("cut") || mode.equals("test")) || workDir.isEmpty()
				|| mode.equals("cut") && articleTitle.isEmpty() || (mode.equals("test") && articleID.isEmpty())) {
			System.out.println(helpMessage);
			return;
		}
		SAXParserFactory factory = SAXParserFactory.newInstance();
		ArticleExtractor articleExtractor = new ArticleExtractor();
		final String fileName = workDir + lang + "wiki-latest-pages-articles.xml.gz";
		File inFile = new File(fileName);
		File outputDir = new File(inFile.getParent(), "out");
		outputDir.mkdirs();
		try {
			if ("cut".equals(mode)) {
				File outFile = new File(outputDir, inFile.getName());
				ExtractorHandler handler = new ExtractorHandler(articleTitle);
				GZIPInputStream is = new GZIPInputStream(new FileInputStream(fileName));
				SAXParser saxParser = factory.newSAXParser();
				try {
					saxParser.parse(is, handler);
				} catch (ExtractorHandler.StopParsingException e) {
					System.out.printf("Trim lines number: header end=%d article start=%d, article end =%d\n",
							handler.getEndHeaderLine(), handler.getFirstArticleLine(), handler.getEndArticleLine());
				}

				long startTime = System.currentTimeMillis();
				articleExtractor.cutArticle(inFile, outFile, handler.getEndHeaderLine(), handler.getFirstArticleLine(),
						handler.getEndArticleLine());
				System.out.println("Trim time: " + (System.currentTimeMillis() - startTime) / 1000);
				articleID = handler.getArticleId();
			}
			String[] arguments = {
					"--lang=" + lang,
					"--dir=" + outputDir.getAbsolutePath() + File.separator,
					"--mode=test-wikipedia",
					"--testID=" + articleID};
			WikiDatabasePreparation.main(arguments);

			arguments = new String[]{
					"generate_test_obf",
					outputDir.getAbsolutePath() + File.separator,
					"--testLatLon=" + articleLatLon};
			WikipediaByCountryDivider.main(arguments);

		} catch (ComponentLookupException | ParserConfigurationException | SAXException | IOException | SQLException
				| InterruptedException | XmlPullParserException e) {
			e.printStackTrace();
		}
	}

	void cutArticle(File inFile, File outFile, int endHeaderLine, int startArticleLine, int endArticleLine)
			throws IOException {

		GZIPOutputStream zip = new GZIPOutputStream(new FileOutputStream(outFile));
		BufferedReader br = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(inFile))));
		PrintWriter pw = new PrintWriter(new OutputStreamWriter(zip));
		String line;
		int lineCount = 0;
		while ((line = br.readLine()) != null && lineCount < endArticleLine) {
			if (lineCount >= startArticleLine || lineCount < endHeaderLine) {
				pw.println(line);
				pw.flush();
			}
			lineCount++;
		}
		pw.println("</mediawiki>");
		br.close();
		pw.close();
	}
}
