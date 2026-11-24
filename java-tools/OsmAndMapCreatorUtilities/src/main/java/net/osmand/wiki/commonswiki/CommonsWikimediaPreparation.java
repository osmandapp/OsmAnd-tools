package net.osmand.wiki.commonswiki;

import net.osmand.MainUtilities;
import net.osmand.PlatformUtil;
import net.osmand.impl.FileProgressImplementation;
import net.osmand.wiki.AbstractWikiFilesDownloader;
import net.osmand.wiki.AbstractWikiFilesDownloader.DownloadHandler;
import net.osmand.wiki.WikiDatabasePreparation;
import net.osmand.wiki.commonswiki.CommonsMetaWriter.Article;
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
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPInputStream;

import static net.osmand.wiki.commonswiki.parser.ParserUtils.FILE_PREFIX;


public class CommonsWikimediaPreparation {
	private static final Log log = PlatformUtil.getLog(CommonsWikimediaPreparation.class);
	
	private static final int DEFAULT_THREADS = 8; // it's parsing threads (downloads always run in a single thread https://dumps.wikimedia.org/)

	public static final String RESULT_SQLITE = "meta_commonswiki.sqlite";
	public static final String FILE_NAMESPACE = "6";

	private final AtomicLong articlesCount = new AtomicLong(0);
	private final AtomicLong totalPagesCount = new AtomicLong(0);
	private final AtomicLong otherErrorsCount = new AtomicLong(0);

	private enum Mode {
		RECREATE(true, false),
		UPDATE(false, false),
		DAILY(false, true);

		final boolean recreateDb;
		final boolean daily;

		Mode(boolean recreateDb, boolean daily) {
			this.recreateDb = recreateDb;
			this.daily = daily;
		}
	}

	public static void main(String[] args) {
		applyCommandLineOpts(new MainUtilities.CommandLineOpts(args));
	}

	private static void applyCommandLineOpts(MainUtilities.CommandLineOpts opts) {
		if (opts.getBoolean("--help")) {
			printHelp();
			System.exit(0);
		}
		String folder = opts.getOrDefault("--dir","");
		String mode = opts.getOrDefault("--mode","");
		String database = opts.getOrDefault("--result_db","");
		boolean recreateDb = opts.getBoolean("--recreate_db");
		boolean keepFiles = opts.getBoolean("--keep_files");
		int threads = opts.getIntOrDefault("--threads", DEFAULT_THREADS);
		boolean showLog = opts.getBoolean("--show_log");

		if (mode.isEmpty() || folder.isEmpty()) {
			printHelp();
			throw new RuntimeException("Correct arguments weren't supplied");
		}

		File commonsWikiDB = database.isEmpty()
				? new File(folder, RESULT_SQLITE)
				: new File(database);
		Mode updateMode = switch (mode) {
			case "parse-img-meta" -> Mode.RECREATE;
			case "update-img-meta" -> recreateDb ? Mode.RECREATE : Mode.UPDATE;
			case "update-img-meta-daily" -> Mode.DAILY;
			default -> {
				printHelp();
				throw new RuntimeException("Unknown mode: " + mode);
			}
		};
		try {
			new CommonsWikimediaPreparation().updateCommonsWiki(commonsWikiDB, updateMode, keepFiles, threads, showLog);
		} catch (SQLException | InterruptedException e) {
			throw new RuntimeException("Error during parsing: " + e.getMessage(), e);
		}
	}

	private static void printHelp() {
		System.out.println("""
		Usage: process-commonswiki [--options]
		
		<dir> and <mode> are mandatory options
		
		--help print this help
		--dir=/path/to/wiki_files/*.xml.gz
		--result_db=/path/to/result_db/meta_commonswiki.sqlite
		--recreate_db recreate meta_commonswiki.sqlite
		--keep_files keep downloaded files instead of deleting them
		--threads=8 number of parsing threads (downloads always run in a single thread, daily dumps are parsed one at a time in date order)
		--show_log show license update log
		
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
		""");
	}

	private void updateCommonsWiki(File commonsWikiDB, Mode mode, boolean keepFiles, int threads, boolean showLog)
			throws SQLException, InterruptedException {
		long start = System.currentTimeMillis();
		CommonsMetaWriter writer = new CommonsMetaWriter(commonsWikiDB.getAbsolutePath(), mode.recreateDb, showLog);
		ExecutorService executor = Executors.newFixedThreadPool(mode.daily ? 1 : threads);
		log.info("Updating wikidata...");
		ExecutorCompletionService<Void> completionService = new ExecutorCompletionService<>(executor);
		AtomicInteger taskCount = new AtomicInteger(0);
		DownloadHandler dh = fileName -> {
			log.info("Updating from " + fileName);
			completionService.submit(() -> {
				parseCommonArticles(fileName, writer);
				return null;
			});
			taskCount.incrementAndGet();
		};
		AbstractWikiFilesDownloader wfd;
		try {
			wfd = new CommonsWikiFilesDownloader(commonsWikiDB, mode.daily, dh);
		} catch (RuntimeException e) {
			stopWorkers(executor, writer);
			throw e;
		}
		executor.shutdown();
		
		// Check tasks as they complete, stop immediately on critical error
		int completed = 0;
		while (completed < taskCount.get()) {
			try {
				Future<Void> future = completionService.take();
				future.get();
				completed++;
			} catch (ExecutionException | InterruptedException e) {
				stopWorkers(executor, writer);
				Throwable cause = e instanceof ExecutionException ? e.getCause() : e;
				if (e instanceof InterruptedException) {
					Thread.currentThread().interrupt();
				}
				throw new RuntimeException("Error during parsing", cause);
			}
		}
		
		if (!keepFiles) {
			wfd.removeDownloadedPages();
		}
		writer.finish();
		writer.createIndex();
		wfd.saveUpdateTimestamp();
		log.info("========= All tasks done in %.0fs ========="
				.formatted((System.currentTimeMillis() - start) / 1000.0));
		log.info("========= Statistics =========");
		log.info("Total pages with namespace=6: " + totalPagesCount.get());
		log.info("Added to queue: " + articlesCount.get());
		log.info("Written to database: " + writer.getWrittenCount());
		log.info("Other errors: " + otherErrorsCount.get());
		if (showLog) {
			log.info("Changed licenses: " + writer.getChangedLicenseCount());
		}
		log.info("========= End Statistics =========");
	}

	private static void stopWorkers(ExecutorService executor, CommonsMetaWriter writer) {
		executor.shutdownNow();
		writer.abort();
	}

	private void parseCommonArticles(String articles, CommonsMetaWriter writer) {
		try {
			SAXParser sx = SAXParserFactory.newInstance().newSAXParser();
			FileProgressImplementation progress = new FileProgressImplementation("Read commonswiki articles file", new File(articles));
			InputStream streamFile = progress.openFileInputStream();
			InputSource is = getInputSource(streamFile);
			final CommonsWikiHandler handler = new CommonsWikiHandler(sx, progress, writer);
			sx.parse(is, handler);
		} catch (ParserConfigurationException | IOException | SAXException e) {
			throw new RuntimeException(e);
		}
	}

	private class CommonsWikiHandler extends DefaultHandler {
		private final SAXParser saxParser;
		private boolean pageTag = false;
		private boolean pageIdParsed = false;
		private boolean pageTextParsed = false;
		private boolean revisionTag = false;
		private StringBuilder ctext = null;
		private final StringBuilder title = new StringBuilder();
		private final StringBuilder ns = new StringBuilder();
		private final StringBuilder id = new StringBuilder();
		private final FileProgressImplementation progress;
		private final CommonsMetaWriter writer;

		private final StringBuilder textContent = new StringBuilder();
		private final StringBuilder lastRevisionText = new StringBuilder();

		CommonsWikiHandler(SAXParser saxParser, FileProgressImplementation progress, CommonsMetaWriter writer) {
			this.saxParser = saxParser;
			this.progress = progress;
			this.writer = writer;
		}

		@Override
		public void startElement(String uri, String localName, String qName, Attributes attributes) {
			String name = saxParser.isNamespaceAware() ? localName : qName;
			if (!pageTag) {
				pageTag = name.equals("page");
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
						if (!revisionTag && !pageIdParsed) {
							id.setLength(0);
							ctext = id;
							pageIdParsed = true;
						}
					}
					case "revision" -> {
						revisionTag = true;
						pageTextParsed = false;
					}
					case "text" -> {
						if (revisionTag && !pageTextParsed) {
							textContent.setLength(0);
							ctext = textContent;
						}
					}
				}
			}
		}

		@Override
		public void characters(char[] ch, int start, int length) {
			if (pageTag && ctext != null) {
				ctext.append(ch, start, length);
			}
		}

		@Override
		public void endElement(String uri, String localName, String qName) {
			String name = saxParser.isNamespaceAware() ? localName : qName;
			if (pageTag) {
				switch (name) {
					case "revision" -> {
						revisionTag = false;
					}
					case "page" -> {
						parseMeta();
						lastRevisionText.setLength(0);
						pageTag = false;
						pageIdParsed = false;
						pageTextParsed = false;
						revisionTag = false;
						ctext = null;
						progress.update();
					}
					case "title", "ns", "id" -> ctext = null;
					case "text" -> {
						if (revisionTag && !pageTextParsed) {
							pageTextParsed = true;
							lastRevisionText.setLength(0);
							lastRevisionText.append(textContent);
						}
						ctext = null;
					}
				}
			}
		}

		private void parseMeta() {
			try {
				if (!lastRevisionText.isEmpty() && FILE_NAMESPACE.contentEquals(ns)) {
					String imageTitle = title.toString().startsWith(FILE_PREFIX) ? title.substring(FILE_PREFIX.length()) : null;
					if (imageTitle == null) {
						return;
					}
					totalPagesCount.incrementAndGet();
					Map<String, String> meta = new HashMap<>();
					WikiDatabasePreparation.removeMacroBlocks(lastRevisionText, meta, new HashMap<>(), null, "en", imageTitle, null, true);
					WikiDatabasePreparation.prepareMetaData(meta);
					String author = meta.get("author");
					String date = meta.get("date");
					String license = meta.get("license");
					String description = meta.get("description");
					String source = meta.get("source");
					try {
						writer.add(new Article(Long.parseLong(id.toString()), imageTitle.replace(" ", "_"),
								author, date, license, description, source));
						if (articlesCount.incrementAndGet() % 100000 == 0) {
							log.info(articlesCount.get() + " Articles processed");
						}
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
					}
				}
			} catch (Exception exception) {
				otherErrorsCount.incrementAndGet();
				log.error(exception.getMessage() + " on page id : " + id + " title : " + title, exception);
			}
		}
	}

	private static InputSource getInputSource(InputStream streamFile) throws IOException {
		GZIPInputStream zis = new GZIPInputStream(streamFile);
		Reader reader = new InputStreamReader(zis, StandardCharsets.UTF_8);
		return new InputSource(reader);
	}
}
