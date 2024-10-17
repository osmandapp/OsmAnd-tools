package net.osmand.wiki;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import net.osmand.IndexConstants;
import net.osmand.PlatformUtil;
import net.osmand.binary.BinaryMapDataObject;
import net.osmand.binary.MapZooms;
import net.osmand.data.QuadRect;
import net.osmand.impl.ConsoleProgressImplementation;
import net.osmand.map.OsmandRegions;
import net.osmand.map.WorldRegion;
import net.osmand.obf.preparation.DBDialect;
import net.osmand.obf.preparation.IndexCreator;
import net.osmand.obf.preparation.IndexCreatorSettings;
import net.osmand.osm.MapRenderingTypesEncoder;
import net.osmand.util.Algorithms;

import org.apache.commons.logging.Log;
import org.xmlpull.v1.XmlPullParserException;
import org.xmlpull.v1.XmlSerializer;

import rtree.RTree;

public class WikipediaByCountryDivider {
	private static final Log log = PlatformUtil.getLog(WikipediaByCountryDivider.class);

	public static void main(String[] args) throws IOException, SQLException, InterruptedException, XmlPullParserException {
		String mode = "";
		String folder = "";
		boolean skip = false;
		String database = "";

		for (String arg : args) {
			String val = arg.substring(arg.indexOf("=") + 1);
			if (arg.startsWith("--skip-existing=")) {
				skip = true;
			} else if (arg.startsWith("--dir=")) {
				folder = val;
			} else if (arg.startsWith("--mode=")) {
				mode = val;
			} else if (arg.startsWith("--database=")) {
				database = val;
			}
		}

		if (mode.isEmpty()) {
			throw new RuntimeException("Set --mode=inspect OR --mode=generate_country_sqlite OR --mode=generate_test_obf");
		}

		if (!mode.equals("inspect") && folder.isEmpty()) {
			throw new RuntimeException("Set --dir=/path/to/wikipedia/source");
		}

		final String sqliteFileName = database.isEmpty() ? folder + WikiDatabasePreparation.WIKIPEDIA_SQLITE : database;

		switch (mode) {
			case "inspect":
				inspectWikiFile(sqliteFileName);
				break;
			case "generate_country_sqlite":
				WikiDatabasePreparation.processWikidataRegions(sqliteFileName);
				generateCountrySqlite(folder, sqliteFileName, skip);
				break;
			case "generate_test_obf":
				String[] latLon = null;
				if (args.length > 2 && args[2].startsWith("--testLatLon=")) {
					String val = args[2].substring(args[2].indexOf("=") + 1);
					latLon = val.split(";");
				}
				generateCountrySqlite(folder, sqliteFileName, skip, latLon);
		}
	}

	protected static void generateCountrySqlite(String folder, String database, boolean skip)
			throws SQLException, IOException, InterruptedException, XmlPullParserException {
		generateCountrySqlite(folder, database, skip, null);
	}

	protected static void generateCountrySqlite(String folder, String database, boolean skip, String[] testLatLon)
			throws SQLException, IOException, InterruptedException, XmlPullParserException {
		double lat = 0;
		double lon = 0;
		boolean test = testLatLon != null;
		if (test) {
			lat = Double.parseDouble(testLatLon[0]);
			lon = Double.parseDouble(testLatLon[1]);
		}
		String testRegionName = "";
		Connection conn = (Connection) DBDialect.SQLITE.getDatabaseConnection(database, log);
		OsmandRegions regs = new OsmandRegions();
		regs.prepareFile();
		Map<String, LinkedList<BinaryMapDataObject>> mapObjects = regs.cacheAllCountries();
		File rgns = new File(folder, "regions");
		rgns.mkdirs();
		Map<String, String> preferredRegionLanguages = new LinkedHashMap<>();
		QuadRect testRect = new QuadRect(lon, lat, lon, lat);
		for (String key : mapObjects.keySet()) {
			if (key == null) {
				continue;
			}
			WorldRegion wr = regs.getRegionDataByDownloadName(key);
			if (wr == null) {
				System.out.println("Missing language for world region '" + key + "'!");
			} else {
				String regionLang = wr.getParams().getRegionLang();
				preferredRegionLanguages.put(key.toLowerCase(), regionLang);
				if (test) {
					if (wr.containsBoundingBox(testRect)) {
						testRegionName = key;
					}
				}
			}
		}
		String query;
		if (test) {
			query = "SELECT '" + Algorithms.capitalizeFirstLetterAndLowercase(testRegionName) + "'";
		} else {
			query = "SELECT DISTINCT regionName FROM wiki_region";
		}
		ResultSet rs = conn.createStatement().executeQuery(query);
		while (rs.next()) {
			String lcRegionName = rs.getString(1);
			if(lcRegionName == null) {
				continue;
			}
			String regionName = Algorithms.capitalizeFirstLetterAndLowercase(lcRegionName);
			String preferredLang = preferredRegionLanguages.get(lcRegionName);
			if(preferredLang == null) {
				preferredLang = "";
			}
			LinkedList<BinaryMapDataObject> list = mapObjects.get(lcRegionName.toLowerCase());
			boolean hasWiki = false;
			if(list != null) {
				for(BinaryMapDataObject o : list) {
					Integer rl = o.getMapIndex().getRule("region_wiki", "yes");
					if(o.containsAdditionalType(rl)) {
						hasWiki = true;
						break;
					}
				}
			}
			if(!hasWiki) {
				System.out.println("Skip " + lcRegionName.toLowerCase() + " doesn't generate wiki");
				continue;
			}
			File osmGz = new File(rgns, regionName + "_" + IndexConstants.BINARY_MAP_VERSION + ".wiki.osm.gz");
			File obfFile = new File(rgns, regionName + "_" + IndexConstants.BINARY_MAP_VERSION + ".wiki.obf");
			if(obfFile.exists() && skip) {
				continue;
			}
			if(!skip) {
				osmGz.delete();
			}
			if(!osmGz.exists()) {
				System.out.println("Generate " + osmGz.getName());
				generateOsmFile(conn, rs.getString(1), preferredLang, osmGz, testLatLon);
			}
			obfFile.delete();
			generateObf(osmGz, obfFile);
		}
		conn.close();
	}

	private static void generateOsmFile(Connection conn, String regionName, String preferredLang, File osmGz,
	                                    String[] testLatLon) throws SQLException, IOException {
		String query;
		if (testLatLon == null) {
			query = "SELECT WC.id, WO.lat, WO.lon, WC.lang, WC.title, WC.zipContent FROM wiki_region WR"
					+ " INNER JOIN wiki_coords WO ON WR.id = WO.id "
					+ " INNER JOIN wiki_content WC ON WC.id = WR.id "
					+ " WHERE WR.regionName = '" + regionName + "' ORDER BY WC.id";
		} else {
			query = "SELECT WC.id, " + testLatLon[0] + ", " + testLatLon[1] + ", WC.lang, WC.title, WC.zipContent "
					+ " FROM wiki_content WC";
		}
		ResultSet rps = conn.createStatement().executeQuery(query);
		FileOutputStream out = new FileOutputStream(osmGz);
		GZIPOutputStream gzStream = new GZIPOutputStream(out);
		XmlSerializer serializer = new org.kxml2.io.KXmlSerializer();
		serializer.setOutput(gzStream, "UTF-8");
		serializer.startDocument("UTF-8", true);
		serializer.startTag(null, "osm");
		serializer.attribute(null, "version", "0.6");
		serializer.attribute(null, "generator", "OsmAnd");
		serializer.setFeature("http://xmlpull.org/v1/doc/features.html#indent-output", true);
		// indentation as 3 spaces
		// serializer.setProperty(
		// "http://xmlpull.org/v1/doc/properties.html#serializer-indentation", "   ");
		// // also set the line separator
		// serializer.setProperty(
		// "http://xmlpull.org/v1/doc/properties.html#serializer-line-separator", "\n");
		int cnt = 1;
		long prevOsmId = -1;
		StringBuilder content = new StringBuilder();
		String nameUnique = null;
		boolean preferredAdded = false;
		boolean nameAdded = false;
		while (rps.next()) {
			long osmId = -rps.getLong(1);
			double lat = rps.getDouble(2);
			double lon = rps.getDouble(3);
			long wikiId = rps.getLong(1);
			String wikiLang = rps.getString(4);
			String title = rps.getString(5);
			byte[] bytes = rps.getBytes(6);
			GZIPInputStream gzin = new GZIPInputStream(new ByteArrayInputStream(bytes));
			BufferedReader br = new BufferedReader(new InputStreamReader(gzin));
			content.setLength(0);
			String s;
			while ((s = br.readLine()) != null) {
				content.append(s);
			}
			String contentStr = content.toString();
			contentStr = contentStr.replace((char) 9, ' ');
			contentStr = contentStr.replace((char) 0, ' ');
			contentStr = contentStr.replace((char) 22, ' ');
			contentStr = contentStr.replace((char) 27, ' ');
			if(contentStr.trim().length() == 0) {
				continue;
			}
			if (osmId != prevOsmId) {
				if (prevOsmId != -1) {
					closeOsmWikiNode(serializer, nameUnique, nameAdded);
				}
				prevOsmId = osmId;
				nameAdded = false;
				nameUnique = null;
				preferredAdded = false;
				serializer.startTag(null, "node");
				serializer.attribute(null, "visible", "true");
				serializer.attribute(null, "id", (osmId) + "");
				serializer.attribute(null, "lat", lat + "");
				serializer.attribute(null, "lon", lon + "");

			}
			if (wikiLang.equals("en")) {
				nameAdded = true;
				addTag(serializer, "name", title);
				addTag(serializer, "wiki_id", wikiId + "");
				addTag(serializer, "content", contentStr);
				addTag(serializer, "wiki_lang:en", "yes");
			} else {
				addTag(serializer, "name:" + wikiLang, title);
				addTag(serializer, "wiki_id:" + wikiLang, wikiId + "");
				addTag(serializer, "wiki_lang:" + wikiLang, "yes");
				if (!preferredAdded) {
					nameUnique = title;
					preferredAdded = preferredLang.contains(wikiLang);
				}
				addTag(serializer, "content:" + wikiLang, contentStr);
			}
		}
		if (prevOsmId != -1) {
			closeOsmWikiNode(serializer, nameUnique, nameAdded);
		}
		serializer.endDocument();
		serializer.flush();
		gzStream.close();
		System.out.println("Processed " + cnt + " pois");
	}

	private static void closeOsmWikiNode(XmlSerializer serializer, String nameUnique, boolean nameAdded)
			throws IOException {
		if(!nameAdded && nameUnique != null) {
			addTag(serializer, "name", nameUnique);
		}
		if(nameAdded || nameUnique != null) {
			addTag(serializer, "osmwiki", "wiki_place");
		}
		serializer.endTag(null, "node");
	}

	private static void generateObf(File osmGz, File obf) throws IOException, SQLException, InterruptedException, XmlPullParserException {
		// be independent of previous results
		RTree.clearCache();
		IndexCreatorSettings settings = new IndexCreatorSettings();
		settings.indexMap = false;
		settings.indexAddress = false;
		settings.indexPOI = true;
		settings.indexTransport = false;
		settings.indexRouting = false;
		settings.poiZipLongStrings = true;
		
		IndexCreator creator = new IndexCreator(obf.getParentFile(), settings); //$NON-NLS-1$
		new File(obf.getParentFile(), IndexCreator.TEMP_NODES_DB).delete();
		creator.setMapFileName(obf.getName());
		creator.generateIndexes(osmGz,
				new ConsoleProgressImplementation(1), null, MapZooms.getDefault(),
				new MapRenderingTypesEncoder(obf.getName()), log);


	}

	private static void addTag(XmlSerializer serializer, String key, String value) throws IOException {
		serializer.startTag(null, "tag");
		serializer.attribute(null, "k", key);
		serializer.attribute(null, "v", value);
		serializer.endTag(null, "tag");
	}

	protected static void inspectWikiFile(String database) throws SQLException {
		Connection conn = DBDialect.SQLITE.getDatabaseConnection(database, log);
		ResultSet rs = conn.createStatement().executeQuery("SELECT COUNT(*) cnt, regionName  FROM wiki_region GROUP BY regionName ORDER BY cnt desc");
		System.out.println("POI by countries");
		while(rs.next()) {
			System.out.println(rs.getString(1) + " " + rs.getString(2));
		}
		System.out.println("POI by languages: ");
		rs = conn.createStatement().executeQuery("SELECT COUNT(*) cnt, lang  FROM wiki_content GROUP BY lang ORDER BY cnt desc");
		while(rs.next()) {
			System.out.println(rs.getString(1) + " " + rs.getString(2));
		}
		System.out.println();
		rs = conn.createStatement().executeQuery("SELECT id, title, lang  FROM wiki_content ORDER BY id ");
		int maxcnt = 100;
		while(rs.next() && maxcnt-- > 0) {
			System.out.println(rs.getString(1) + " " + rs.getString(2) + " " + rs.getString(3));
		}
	}

}
