package net.osmand.wiki.wikidata;

import net.osmand.util.Algorithms;
import net.osmand.wiki.WikiDatabasePreparation;
import net.osmand.wiki.WikipediaByCountryDivider;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
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

import static net.osmand.wiki.WikiDatabasePreparation.WIKIDATA_ARTICLES_GZ;
import static net.osmand.wiki.WikiDatabasePreparation.WIKI_ARTICLES_GZ;

public class ArticleExtractor {
	// for large files if error occurs:
	// "The accumulated size of entities is "50,000,001" that exceeded the "50,000,000" limit set by "FEATURE_SECURE_PROCESSING"
	// need to add three java arguments -DentityExpansionLimit=2147480000 -DtotalEntitySizeLimit=2147480000
	// -Djdk.xml.totalEntitySizeLimit=2147480000
	public static final String helpMessage = " --mode=<cut|test> --lang=<lang> --title=<title> --amount=<amount>--dir=<work_folder> " +
			"--testID=<article_ID> --LatLon=<testLatLon> --f=<file>\n" +
			"mode cut without lang - cut out article with <title> (wikidata title 'Q1072074') from <file> or from wikidatawiki-latest-pages-articles.xml.gz\n" +
			"if --f is absent" +
			"mode cut with lang - cut out <amount> of articles, beginning with <title> from <lang>wiki-latest-pages-articles.xml.gz and create obf file \n" +
			"on the <LatLon> coordinates(lat;lon)\n" +
			"mode test - creates obf file for the article with <articleID> by <LatLon> coordinates(lat;lon)\n" +
			"results are written to a '<work_folder>/out' folder";

	public static void main(String[] args) throws Exception {
		String lang = null; //lang = null - process wikidata;
		String workDir = "";
		String mode = "test";
		String articleID = "";
		String articleLatLon = "50.45191;30.59195";
		String articleTitle = "";
		String fileName = "";
		int amount = 1;
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
				case "--amount=":
					try {
						amount = Integer.parseInt(val);
					} catch (NumberFormatException e) {
						amount = 1;
					}
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
				case "--f=":
					fileName = val;
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
		boolean processWikidata = lang == null;
		if (!Algorithms.isEmpty(fileName)) {
			fileName = workDir + fileName;
		} else {
			if (processWikidata) {
				fileName = workDir + WIKIDATA_ARTICLES_GZ;
			} else {
				fileName = workDir + lang + WIKI_ARTICLES_GZ;
			}
		}
		File readFile = new File(fileName);
		File outputDir = new File(readFile.getParent(), "out");
		outputDir.mkdirs();
		try {
			if ("cut".equals(mode)) {
				File outFile = new File(outputDir, readFile.getName());
				ExtractorHandler handler = new ExtractorHandler(articleTitle, amount);
				InputStream is = getInputStream(readFile);
				SAXParser saxParser = factory.newSAXParser();
				try {
					saxParser.parse(is, handler);
				} catch (ExtractorHandler.StopParsingException e) {
					System.out.printf("Trim lines number: header end=%d article start=%d, article end =%d\n",
							handler.getEndHeaderLine(), handler.getFirstArticleLine(), handler.getEndArticleLine());
				}

				if (handler.getFirstArticleLine() == 0 && handler.getEndArticleLine() == 0) {
					System.out.printf("Article %s not found\n", articleTitle);
				} else {
					long startTime = System.currentTimeMillis();
					articleExtractor.cutArticle(readFile, outFile, handler.getEndHeaderLine(), handler.getFirstArticleLine(),
							handler.getEndArticleLine());
					System.out.printf("Trim time: %d sec\n", (System.currentTimeMillis() - startTime) / 1000);
					articleID = handler.getArticleId();
				}
			}
			if (!processWikidata && !Algorithms.isEmpty(articleID)) {
				String[] arguments = {
						"--lang=" + lang,
						"--wikipediaDir=" + outputDir.getAbsolutePath() + File.separator,
						"--mode=test-wikipedia",
						"--testID=" + articleID};
				WikiDatabasePreparation.main(arguments);

				arguments = new String[]{
						"generate_test_obf",
						outputDir.getAbsolutePath() + File.separator,
						"--testLatLon=" + articleLatLon};
				WikipediaByCountryDivider.main(arguments);
			}
		} catch (ComponentLookupException | ParserConfigurationException | SAXException | IOException | SQLException
				| InterruptedException | XmlPullParserException e) {
			e.printStackTrace();
		}
	}

	void cutArticle(File readFile, File outFile, int endHeaderLine, int startArticleLine, int endArticleLine)
			throws IOException {
		BufferedReader br = new BufferedReader(new InputStreamReader(getInputStream(readFile)));
		PrintWriter pw = new PrintWriter(new OutputStreamWriter(getOutputStream(outFile)));
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
		pw.close();
		br.close();
	}

	private static InputStream getInputStream(File file) throws IOException {
		InputStream is = new BufferedInputStream(new FileInputStream(file));
		if (file.getName().endsWith(".bz2")) {
			is = new BZip2CompressorInputStream(is);
		} else if (file.getName().endsWith(".gz")) {
			is = new GZIPInputStream(is);
		}
		return is;
	}

	private static OutputStream getOutputStream(File file) throws IOException {
		OutputStream os = new FileOutputStream(file);
		if (file.getName().endsWith(".bz2")) {
			os = new BZip2CompressorOutputStream(os);
		} else if (file.getName().endsWith(".gz")) {
			os = new GZIPOutputStream(os);
		}
		return os;
	}
}
