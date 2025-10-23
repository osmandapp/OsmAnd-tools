package net.osmand.wiki.commonswiki;

import net.osmand.PlatformUtil;
import net.osmand.impl.FileProgressImplementation;
import net.osmand.obf.preparation.DBDialect;
import net.osmand.wiki.AbstractWikiFilesDownloader;
import net.osmand.wiki.AbstractWikiFilesDownloader.DownloadHandler;
import net.osmand.wiki.WikiDatabasePreparation;
import org.apache.commons.logging.Log;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPInputStream;


public class CommonsWikimediaPreparation {
	private static final Log log = PlatformUtil.getLog(CommonsWikimediaPreparation.class);
	private static final int THREAD_COUNT = 8;
	public static final int BATCH_SIZE = 5000;
	private static final BlockingQueue<Article> QUEUE = new LinkedBlockingQueue<>(10_000);
	private static final Article END_SIGNAL = new Article(-1L, "", "", "", "", "");
	private static final AtomicLong articlesCount = new AtomicLong(0);

	public static final String RESULT_SQLITE = "meta_commonswiki.sqlite";
	public static final String FILE_NAMESPACE = "6";
	public static final String FILE = "File:";

	public static void main(String[] args) {
		String folder = "";
		String mode = "";
		String database = "";
		boolean recreateDb = false;

		for (String arg : args) {
			String val = arg.substring(arg.indexOf("=") + 1);
			if (arg.startsWith("--dir=")) {
				folder = val;
			} else if (arg.startsWith("--mode=")) {
				mode = val;
			} else if (arg.startsWith("--result_db=")) {
				database = val;
			} else if (arg.startsWith("--recreate_db")) {
				recreateDb = true;
			}
		}

		if (mode.isEmpty() || folder.isEmpty()) {
			printHelpException("Correct arguments weren't supplied");
		}

		final String sqliteFileName = database.isEmpty() ? folder + RESULT_SQLITE : database;
		CommonsWikimediaPreparation p = new CommonsWikimediaPreparation();
		File commonsWikiDB = new File(sqliteFileName);
		try {
			switch (mode) {
				case "parse-img-meta":
					p.updateCommonsWiki(commonsWikiDB, true, false);
					break;
				case "update-img-meta":
					p.updateCommonsWiki(commonsWikiDB, recreateDb, false);
					break;
				case "update-img-meta-daily":
					p.updateCommonsWiki(commonsWikiDB, false, true);
					break;
				default:
					printHelpException("Unknown mode: " + mode);
			}
		} catch (ParserConfigurationException | SAXException | IOException | SQLException | InterruptedException e) {
			throw new RuntimeException("Error during parsing: " + e.getMessage(), e);
		}
	}

	private static void printHelpException(String errorMessage) {
		String helpMessage = """
		--mode=parse-img-meta
		  Recreates the RESULT_SQLITE database and downloads, then parses commonswiki-latest-pages-articles.xml.gz
		  from https://dumps.wikimedia.org/commonswiki/latest/ in parts.
		
		--mode=update-img-meta
		  Downloads and parses commonswiki-latest-pages-articles.xml.gz
		  from https://dumps.wikimedia.org/commonswiki/latest/ in parts.
		  If a part is already downloaded, it is not re-downloaded. Parsing starts immediately.
		
		--mode=update-img-meta-daily
		  Downloads and parses commonswiki-[date]-pages-meta-hist-incr.xml.bz2
		  from https://dumps.wikimedia.org/other/incr/commonswiki/[date]/,
		  starting from the date after the one recorded in timestamp.txt.
		""";
		System.out.println(helpMessage);
		throw new RuntimeException(errorMessage);
	}


	private void updateCommonsWiki(File commonsWikiDB, boolean recreateDb, boolean dailyUpdate)
			throws ParserConfigurationException, SAXException, IOException, SQLException, InterruptedException {
		long start = System.currentTimeMillis();
		String commonsWikiDBPath = commonsWikiDB.getAbsolutePath();
		initDatabase(commonsWikiDBPath, recreateDb);
		ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
		Thread writerThread = new Thread(() -> writeToDatabase(commonsWikiDBPath));
		writerThread.start();
		log.info("Updating wikidata...");
		DownloadHandler dh = fileName -> {
			log.info("Updating from " + fileName);
			executor.submit(() -> parseCommonArticles(fileName));
		};
		AbstractWikiFilesDownloader wfd = new CommonsWikiFilesDownloader(commonsWikiDB, dailyUpdate, dh);
		executor.shutdown();
		executor.awaitTermination(24, TimeUnit.HOURS);
		wfd.removeDownloadedPages();
		QUEUE.put(END_SIGNAL);
		writerThread.join();
		createIndex(commonsWikiDBPath);
		log.info("========= All tasks done in %.0fs total parsed %d articles =========%n"
				.formatted((System.currentTimeMillis() - start) / 1000.0, articlesCount.get()));
	}

	private static void initDatabase(String sqliteFileName, boolean recreateDb) throws SQLException {

		try (Connection conn = DBDialect.SQLITE.getDatabaseConnection(sqliteFileName, log);
			 Statement stmt = conn.createStatement()) {
			stmt.execute("PRAGMA journal_mode = WAL;");
			stmt.execute("PRAGMA synchronous = NORMAL;");
			stmt.execute("PRAGMA busy_timeout = 5000;");
			if (recreateDb) {
				log.info("========= DROP old TABLE common_meta =========");
				stmt.execute("DROP TABLE IF EXISTS common_meta");
			}
			log.info("========= CREATE TABLE common_meta =========");
			stmt.execute("""
					CREATE TABLE IF NOT EXISTS common_meta(\
					id long PRIMARY KEY, \
					name text, \
					author text, \
					date text, \
					license text, \
					description text)""");
		}
	}

	private static void writeToDatabase(String sqliteFileName) {
		System.out.println("Writer started...");
		try (Connection conn = DBDialect.SQLITE.getDatabaseConnection(sqliteFileName, log)) {
			conn.setAutoCommit(false);
			try (PreparedStatement insertStatement = conn.prepareStatement("""
					INSERT INTO common_meta(id, name, author,  date, license, description) VALUES (?, ?, ?, ?, ?, ?) \
					ON CONFLICT(id) DO UPDATE SET \
					name = excluded.name, \
					author = excluded.author, \
					date = excluded.date, \
					license = excluded.license, \
					description = excluded.description;""")) {

				int counter = 0;

				while (true) {
					Article a = QUEUE.take();
					if (a == END_SIGNAL) break;

					insertStatement.setLong(1, a.id);
					insertStatement.setString(2, a.title);
					insertStatement.setString(3, a.author);
					insertStatement.setString(4, a.date);
					insertStatement.setString(5, a.license);
					insertStatement.setString(6, a.description);
					insertStatement.addBatch();
					counter++;

					if (counter % BATCH_SIZE == 0) {
						insertStatement.executeBatch();
						conn.commit();
						counter = 0;
					}
				}

				insertStatement.executeBatch();
				conn.commit();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("Writer finished (queue empty).");
	}

	public void createIndex(String sqliteFileName) {
		log.info("========= DONE =========");
		log.info("Create indexes");

		try (Connection conn = DBDialect.SQLITE.getDatabaseConnection(sqliteFileName, log);
			 Statement stmt = conn.createStatement()) {
			stmt.execute("CREATE INDEX IF NOT EXISTS name_common_meta_index ON common_meta(name)");

		} catch (SQLException e) {
			System.err.println("Failed to create index: " + e.getMessage());
			e.printStackTrace();
		}
	}

	private record Article(Long id, String title, String author, String date, String license, String description) {
	}

	private void parseCommonArticles(String articles) {
		try {
			SAXParser sx = SAXParserFactory.newInstance().newSAXParser();
			FileProgressImplementation progress = new FileProgressImplementation("Read commonswiki articles file", new File(articles));
			InputStream streamFile = progress.openFileInputStream();
			InputSource is = getInputSource(streamFile);
			final CommonsWikiHandler handler = new CommonsWikiHandler(sx, progress);
			sx.parse(is, handler);
		} catch (ParserConfigurationException | SQLException | IOException | SAXException e) {
			throw new RuntimeException(e);
		}
	}

	private static class CommonsWikiHandler extends DefaultHandler {
		private final SAXParser saxParser;
		private boolean page = false;
		private boolean pageIdParsed = false;
		private boolean pageTextParsed = false;
		private StringBuilder ctext = null;
		private final StringBuilder title = new StringBuilder();
		private final StringBuilder ns = new StringBuilder();
		private final StringBuilder id = new StringBuilder();
		private final FileProgressImplementation progress;

		private final StringBuilder textContent = new StringBuilder();

		CommonsWikiHandler(SAXParser saxParser, FileProgressImplementation progress) throws SQLException {
			this.saxParser = saxParser;
			this.progress = progress;
		}

		@Override
		public void startElement(String uri, String localName, String qName, Attributes attributes) {
			String name = saxParser.isNamespaceAware() ? localName : qName;
			if (!page) {
				page = name.equals("page");
			} else {
				switch (name) {
					case "title" -> {
						title.setLength(0);
						ctext = title;
					}
					case "ns" -> {
						ns.setLength(0);
						ctext = ns;
					}
					case "id" -> {
						if (!pageIdParsed) {
							id.setLength(0);
							ctext = id;
							pageIdParsed = true;
						}
					}
					case "text" -> {
						if (!pageTextParsed) {
							textContent.setLength(0);
							ctext = textContent;
						}
					}
					default -> {
						// do nothing
					}
				}
			}
		}

		@Override
		public void characters(char[] ch, int start, int length) {
			if (page && ctext != null) {
				ctext.append(ch, start, length);
			}
		}

		@Override
		public void endElement(String uri, String localName, String qName) {
			String name = saxParser.isNamespaceAware() ? localName : qName;
			if (page) {
				switch (name) {
					case "page" -> {
						page = false;
						pageIdParsed = false;
						pageTextParsed = false;
						progress.update();
					}
					case "title", "ns", "id" -> ctext = null;
					case "text" -> {
						if (!pageTextParsed) {
							parseMeta();
							pageTextParsed = true;
						}
					}
					default -> {
						// do nothing
					}
				}
			}
		}

		private void parseMeta() {
			try {
				if (FILE_NAMESPACE.contentEquals(ns)) {
					String imageTitle = title.toString().startsWith(FILE) ? title.substring(FILE.length()) : null;
					if (imageTitle == null) {
						return;
					}
					Map<String, String> meta = new HashMap<>();
					WikiDatabasePreparation.removeMacroBlocks(textContent, meta, new HashMap<>(), null, "en", imageTitle, null, true);
					WikiDatabasePreparation.prepareMetaData(meta);
					String author = meta.getOrDefault("author", "");
					String license = meta.getOrDefault("license", "");
					String description = meta.getOrDefault("description", "");
					String date = meta.getOrDefault("date", "");
					try {
						QUEUE.put(new Article(Long.parseLong(id.toString()), imageTitle.replace(" ", "_"),
								author, date, license, description));
						if (articlesCount.incrementAndGet() % 100000 == 0) {
							System.out.println(articlesCount.get() + " Articles processed");
						}
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
					}
				}
			} catch (Exception exception) {
				log.error(exception.getMessage() + " on page id : " + id + " title : " + title, exception);
			}
		}
	}

	private static InputSource getInputSource(InputStream streamFile) throws IOException {
		GZIPInputStream zis = new GZIPInputStream(streamFile);
		Reader reader = new InputStreamReader(zis, StandardCharsets.UTF_8);
		InputSource is = new InputSource(reader);
		is.setEncoding("UTF-8");
		return is;
	}
}
