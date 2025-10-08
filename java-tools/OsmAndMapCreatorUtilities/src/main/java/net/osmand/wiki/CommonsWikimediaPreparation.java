package net.osmand.wiki;

import net.osmand.PlatformUtil;
import net.osmand.impl.FileProgressImplementation;
import net.osmand.obf.preparation.DBDialect;
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
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;


public class CommonsWikimediaPreparation {

	public static final String DATA_SOURCE = "commonswiki-latest-pages-articles.xml.gz";
	public static final String RESULT_SQLITE = "wikidata_commons_osm.sqlitedb";
	public static final String FILE_NAMESPACE = "6";
	public static final String FILE = "File:";

	private static final Log log = PlatformUtil.getLog(CommonsWikimediaPreparation.class);

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
			throw new RuntimeException("Correct arguments weren't supplied");
		}

		final String commonWikiArticles = folder + DATA_SOURCE;
		final String sqliteFileName = database.isEmpty() ? folder + RESULT_SQLITE : database;
		CommonsWikimediaPreparation p = new CommonsWikimediaPreparation();
		try {
			switch (mode) {
				case "parse-commonswiki-img-meta":
					p.parseCommonArticles(commonWikiArticles, sqliteFileName, recreateDb);
					break;
				default:
					throw new RuntimeException("Unknown mode: " + mode);
			}
		} catch (ParserConfigurationException | SAXException | IOException | SQLException e) {
			throw new RuntimeException("Error during parsing: " + e.getMessage(), e);
		}
	}

	private void parseCommonArticles(String articles, String sqliteFileName, boolean recreateDb) throws ParserConfigurationException, SAXException, IOException, SQLException {
		SAXParser sx = SAXParserFactory.newInstance().newSAXParser();
		FileProgressImplementation progress = new FileProgressImplementation("Read commonswiki articles file", new File(articles));
		InputStream streamFile = progress.openFileInputStream();
		InputSource is = getInputSource(streamFile);

		final CommonsWikiHandler handler = new CommonsWikiHandler(sx, progress, new File(sqliteFileName), recreateDb);
		sx.parse(is, handler);
		handler.finish();
	}


	private static class CommonsWikiHandler extends DefaultHandler {
		private final SAXParser saxParser;
		private DBDialect dialect = DBDialect.SQLITE;
		private Connection conn;
		private PreparedStatement prepContent;
		private int[] contentBatch = new int[]{0};
		private boolean page = false;
        private boolean pageIdParsed = false;
		private StringBuilder ctext = null;
		private final StringBuilder title = new StringBuilder();
		private final StringBuilder ns = new StringBuilder();
		private final StringBuilder id = new StringBuilder();
		private final FileProgressImplementation progress;
		public static final int BATCH_SIZE = 5000;
		private final StringBuilder textContent = new StringBuilder();


		CommonsWikiHandler(SAXParser saxParser, FileProgressImplementation progress, File sqliteFile, boolean recreateDb) throws SQLException {
			this.saxParser = saxParser;
			this.progress = progress;

			conn = dialect.getDatabaseConnection(sqliteFile.getAbsolutePath(), log);

			log.info("========= CREATE TABLE common_meta =========");
			try (Statement st = conn.createStatement()) {
				if (recreateDb) {
					log.info("========= DROP old TABLE common_meta =========");
					st.execute("DROP TABLE IF EXISTS common_meta");
				}
				st.execute("CREATE TABLE IF NOT EXISTS common_meta(" +
						"id long, " +
						"name text, " +
						"author text, " +
						"date text, " +
						"license text, " +
						"description text)");
				st.execute("DELETE FROM common_meta");
			}

			prepContent = conn.prepareStatement(
					"INSERT INTO common_meta(id, name, author,  date, license, description) VALUES (?, ?, ?, ?, ?, ?)"
			);
		}

		public void finish() throws SQLException {
			log.info("========= DONE =========");
			log.info("Create indexes");

			try (Statement st = conn.createStatement()) {
				st.execute("CREATE INDEX IF NOT EXISTS id_common_meta_index ON common_meta(id)");
				st.execute("CREATE INDEX IF NOT EXISTS name_common_meta_index ON common_meta(name)");
			}

			prepContent.executeBatch();

			if (!conn.getAutoCommit()) {
				conn.commit();
			}

			prepContent.close();
			conn.close();
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
                        if(!pageIdParsed) {
                            id.setLength(0);
                            ctext = id;
                            pageIdParsed = true;
                        }
					}
					case "text" -> {
						textContent.setLength(0);
						ctext = textContent;
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
						progress.update();
					}
					case "title", "ns", "id" -> ctext = null;
					case "text" -> {
						String nameSpace = ns.toString();
						if (!FILE_NAMESPACE.equals(nameSpace)) {
							break;
						}
						parseMeta(nameSpace);
					}
					default -> {
						// do nothing
					}
				}
			}
		}

		public void addBatch(PreparedStatement prep, int[] bt) throws SQLException {
			prep.addBatch();
			bt[0] = bt[0] + 1;
			int batch = bt[0];
			if (batch > BATCH_SIZE) {
				prep.executeBatch();
				bt[0] = 0;
			}
		}

		private void parseMeta(String nameSpace) {
			try {
				if (FILE_NAMESPACE.equals(nameSpace)) {
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
					prepContent.setLong(1, Long.parseLong(id.toString()));
					prepContent.setString(2, imageTitle.replace(" ", "_"));
					prepContent.setString(3, author);
					prepContent.setString(4, date);
					prepContent.setString(5, license);
					prepContent.setString(6, description);
					addBatch(prepContent, contentBatch);
				}
			} catch (SQLException | IOException exception) {
				log.error(exception.getMessage(), exception);
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
