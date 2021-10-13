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
	private static final String WORK_DIR = "/home/user/osmand/issues/he/";
//	private static final String TITLE = "מרכז התערוכות הבינלאומי";
	private static final String TITLE = "Міжнародний виставковий центр (Київ)";
//	private static final String LANG = "he";
	private static final String LANG = "uk";
	private static final String LAT_LON = "50.45191;30.59195";

	public static void main(String[] args) throws IOException {
		SAXParserFactory factory = SAXParserFactory.newInstance();
		ArticleExtractor articleExtractor = new ArticleExtractor();
		final String fileName = WORK_DIR + LANG + "wiki-latest-pages-articles.xml.gz";
		File inFile = new File(fileName);
		File outputDir = new File(inFile.getParent(), "out");
		outputDir.mkdirs();
		File outFile = new File(outputDir, inFile.getName());
		ExtractorHandler handler = new ExtractorHandler(TITLE);
		try {
			GZIPInputStream is = new GZIPInputStream(new FileInputStream(fileName));
			SAXParser saxParser = factory.newSAXParser();
			saxParser.parse(is, handler);
			System.out.printf("Trim lines number: before begin=%d end=%d, after begin=%d end=%d\n",
					handler.getStartNumberBefore(), handler.getEndNumberBefore(),
					handler.getStartNumberAfter(), handler.getEndNumberAfter());
			long startTime = System.currentTimeMillis();
			articleExtractor.cutArticle(inFile, outFile, handler.getStartNumberBefore(), handler.getEndNumberBefore(),
					handler.getStartNumberAfter(), handler.getEndNumberAfter());
			System.out.println("Trim time: " + (System.currentTimeMillis() - startTime) / 1000);

			String[] arguments = {
					"--lang=" + LANG,
					"--dir=" + outputDir.getAbsolutePath() + File.separator,
					"--mode=test-wikipedia",
					"--testID=" + handler.getArticleId()};
			WikiDatabasePreparation.main(arguments);

			arguments = new String[]{
					"generate_test_obf",
					outputDir.getAbsolutePath() + File.separator,
					"--testLatLon=" + LAT_LON};
			WikipediaByCountryDivider.main(arguments);

		} catch (ComponentLookupException | ParserConfigurationException | SAXException | IOException | SQLException
				| InterruptedException | XmlPullParserException e) {
			e.printStackTrace();
		}
	}

	void cutArticle(File inFile, File outFile, int startNumberBefore, int endNumberBefore, int startNumberAfter,
	                int endNumberAfter) throws IOException {

		GZIPOutputStream zip = new GZIPOutputStream(new FileOutputStream(outFile));
		BufferedReader br = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(inFile))));
		PrintWriter pw = new PrintWriter(new OutputStreamWriter(zip));
		String line;
		int lineCount = 0;
		while ((line = br.readLine()) != null) {
			if (lineCount >= endNumberAfter) {
				pw.println(line);
				pw.flush();
			} else if (lineCount >= startNumberAfter - 1) {
			} else if (lineCount >= endNumberBefore) {
				pw.println(line);
				pw.flush();
			} else if (lineCount >= startNumberBefore) {
			} else {
				pw.println(line);
				pw.flush();
			}
			lineCount++;
		}
		br.close();
		pw.close();
	}
}
