package net.osmand.wiki;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.zip.GZIPOutputStream;

import org.junit.Test;

public class OsmWikidataLinksTest {

	// an item stored before its OSM object had a link (update-wikidata skips old items) gets it from the OSM extract
	@Test
	public void missingOsmLinkIsFilled() throws Exception {
		File dir = Files.createTempDirectory("osm-wiki").toFile();
		try (Writer w = new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(
				new File(dir, WikiDatabasePreparation.OSM_WIKI_FILE_PREFIX + "multipolygon.osm.gz"))), StandardCharsets.UTF_8)) {
			w.write("""
					<?xml version='1.0' encoding='UTF-8'?>
					<osm version="0.6">
					 <node id="1" lat="50.443" lon="30.502"/><node id="2" lat="50.443" lon="30.504"/>
					 <node id="3" lat="50.445" lon="30.504"/><node id="4" lat="50.445" lon="30.502"/>
					 <way id="10"><nd ref="1"/><nd ref="2"/><nd ref="3"/><nd ref="4"/><nd ref="1"/></way>
					 <relation id="13422065"><member type="way" ref="10" role="outer"/>
					  <tag k="type" v="boundary"/><tag k="boundary" v="protected_area"/><tag k="leisure" v="garden"/>
					  <tag k="name" v="Botanical garden"/><tag k="wikidata" v="Q894650"/>
					 </relation>
					</osm>
					""");
		}
		File db = new File(dir, "wikidata_osm.sqlitedb");
		try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + db.getAbsolutePath());
				Statement st = c.createStatement()) {
			st.execute("CREATE TABLE wiki_coords(id bigint PRIMARY KEY, originalId text, lat double, lon double, "
					+ "wlat double, wlon double, osmtype int, osmid bigint, poitype text, poisubtype text, labelsJson text)");
			st.execute("CREATE TABLE wiki_mapping(id bigint, lang text, title text, UNIQUE(id, lang))");
			st.execute("INSERT INTO wiki_coords VALUES(894650, 'Q894650', 50.4441, 30.5057, 50.4441, 30.5057, 0, 0, "
					+ "null, null, null)");
			st.execute("INSERT INTO wiki_coords VALUES(1, 'Q1', 1, 1, 1, 1, 2, 555, 'x', 'y', null)");
		}
		OsmCoordinatesByTag osm = new OsmCoordinatesByTag(new String[] { "wikipedia", "wikidata" },
				new String[] { "wikipedia:" }).parse(dir);
		WikiDatabasePreparation.createOSMWikidataTable(db, osm);
		try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + db.getAbsolutePath());
				Statement st = c.createStatement()) {
			ResultSet rs = st.executeQuery("SELECT osmtype, osmid FROM wiki_coords WHERE id = 894650");
			rs.next();
			assertEquals(3, rs.getInt(1)); // relation
			assertEquals(13422065, rs.getLong(2));
			rs = st.executeQuery("SELECT osmtype, osmid FROM wiki_coords WHERE id = 1");
			rs.next();
			assertEquals(2, rs.getInt(1)); // an existing link is kept
			assertEquals(555, rs.getLong(2));
		}
	}
}
