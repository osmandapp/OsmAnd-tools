package net.osmand.wiki.wikidata;

import net.osmand.PlatformUtil;
import net.osmand.data.LatLon;
import net.osmand.obf.preparation.DBDialect;
import org.apache.commons.logging.Log;
import org.xml.sax.Attributes;
import org.xml.sax.helpers.DefaultHandler;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static net.osmand.wiki.wikidata.OsmWikiMap.*;

class OsmWikiHandler extends DefaultHandler {

	private static final Log log = PlatformUtil.getLog(OsmWikiHandler.class);

	private final OsmWikiMap osmWikiMap;
	private final StringBuilder nameKey = new StringBuilder();
	private final StringBuilder wikidataValue = new StringBuilder();
	private final StringBuilder wikipediaValue = new StringBuilder();
	boolean parseWay;
	Way way;
	List<Way> allWays = new ArrayList<>();
	LatLon latlon;
	long id;
	Runtime runtime = Runtime.getRuntime();
    private List<String> filteredTags;
    private String filteredTagsValues;
    private Connection conn;
    private static String sqliteFile = "tag_parser.sqlite";
    private PreparedStatement relStatement;
    private PreparedStatement relMembersWayStatement;
    private PreparedStatement relMemberNodeStatement;
    private PreparedStatement wayStatement;
    private PreparedStatement nodeStatement;

	public OsmWikiHandler(OsmWikiMap osmWikiMap, List<String> filteredTags) {
		this.osmWikiMap = osmWikiMap;
        this.filteredTags = filteredTags;
        try {
            DBDialect dialect = DBDialect.SQLITE;
            File f = new File(sqliteFile);
            dialect.removeDatabase(f);
            conn = dialect.getDatabaseConnection(f.getAbsolutePath(), log);
            conn.createStatement().execute("CREATE table relations(id long, filtered_tags text)");
            conn.createStatement().execute("CREATE table rel_members(rel_id long, way_id long, node_id)");
            conn.createStatement().execute("CREATE table ways(id long, filtered_tags text, nodes text, filtered boolean)");
            conn.createStatement().execute("CREATE table nodes(id long, filtered_tags text, lat double, lon double, filtered boolean)");
            relStatement = conn.prepareStatement("INSERT INTO relation (id, filtered_tags) values (?, ?)");
            relMembersWayStatement = conn.prepareStatement("INSERT INTO rel_members (rel_id, way_id) values (?, ?)");
            relMemberNodeStatement = conn.prepareStatement("INSERT INTO rel_members (rel_id, node_id, ) values (?, ?)");
            wayStatement = conn.prepareStatement("INSERT INTO ways (id, filtered_tags, nodes, filtered) values (?, ?, ?, ?)");
            nodeStatement = conn.prepareStatement("INSERT INTO nodes (id, filtered_tags, lat, lon, filtered) values (?, ?, ?, ?, ?)");
        }  catch (SQLException e) {
            e.printStackTrace();
        }
	}

	@Override
	public void startElement(String uri, String localName, String qName, Attributes attributes) {
		if (qName.equalsIgnoreCase("node")) {
			/*osmWikiMap.nodeCount++;
			if (osmWikiMap.nodeCount % 1000000 == 0) {
				long usedMem1 = runtime.totalMemory() - runtime.freeMemory();
				log.info(String.format("Node %d memory used = %d", osmWikiMap.nodeCount, usedMem1 / MB));
			}
			String idStr = attributes.getValue("id");
			if (idStr != null) {
				id = Long.parseLong(idStr);
			}
			String lat = attributes.getValue("lat");
			String lon = attributes.getValue("lon");
			if (lat != null && lon != null) {
				latlon = new LatLon(Double.parseDouble(lat), Double.parseDouble(lon));
			}*/
		} else if (qName.equalsIgnoreCase("tag")) {
            String tag = attributes.getValue("k");
			/*if (attributes.getValue("k").equals("wikipedia")) {
				wikipediaValue.append(attributes.getValue("v"));
			} else if (attributes.getValue("k").equals("wikidata")) {
				wikidataValue.append(attributes.getValue("v"));
			}*/
            if (filteredTags.contains(tag)) {
                String val = attributes.getValue("v");
                filteredTagsValues += tag + "=" + val + "#";
            }
		} else if (qName.equalsIgnoreCase("way")) {
			/*osmWikiMap.wayCount++;
			if (osmWikiMap.wayCount % 100000 == 0) {
				long usedMem1 = runtime.totalMemory() - runtime.freeMemory();
				log.info(String.format("Way %d memory used = %d", osmWikiMap.wayCount, usedMem1 / MB));
			}
			parseWay = true;
			long id = 0;
			String idStr = attributes.getValue("id");
			if (idStr != null) {
				id = Long.parseLong(idStr);
			}
			way = new Way(id);*/
		} else if ((qName.equalsIgnoreCase("nd")) && parseWay) {
			/*long nodeId = Long.parseLong(attributes.getValue("ref"));
			Node node = osmWikiMap.mapNodes.get(nodeId);
			if (node != null) {
				way.nodes.add(node);
			} else {
				throw new IllegalArgumentException();
			}*/
		} else if (qName.equalsIgnoreCase("relation")) {
            String idStr = attributes.getValue("id");
            if (idStr != null) {
                id = Long.parseLong(idStr);
            }
        }
	}

	@Override
	public void endElement(String uri, String localName, String qName) {
		if (qName.equalsIgnoreCase("node")) {
			/*if (wikidataValue.length() > 0 || wikipediaValue.length() > 0) {
				nameKey.append(wikidataValue).append("#").append(wikipediaValue);
				osmWikiMap.wikiOsmCoordinates.put(nameKey.toString(), latlon);
				Node n = osmWikiMap.mapNodes.get(id);
				if (n == null) {
					osmWikiMap.mapNodes.put(id, new Node(latlon.getLatitude(), latlon.getLongitude(), id));
				}
				wikidataValue.setLength(0);
				wikipediaValue.setLength(0);
				nameKey.setLength(0);
			} else if (wikidataValue.length() == 0 && wikipediaValue.length() == 0) {
				Node n = osmWikiMap.mapNodes.get(id);
				if (n == null) {
					osmWikiMap.mapNodes.put(id, new Node(latlon.getLatitude(), latlon.getLongitude(), id));
				}
			}*/
		} else if (qName.equalsIgnoreCase("way")) {
			/*parseWay = false;
			if (wikidataValue.length() > 0 || wikipediaValue.length() > 0) {
				nameKey.append(wikidataValue).append("#").append(wikipediaValue);
				way.setWikiKey(nameKey.toString());
				allWays.add(way);
				wikidataValue.setLength(0);
				wikipediaValue.setLength(0);
				nameKey.setLength(0);
			}*/
		} else if (qName.equalsIgnoreCase("relation")) {
            if (filteredTagsValues.length() > 0) {
                try {
                    relStatement.setLong(1, id);
                    relStatement.setString(2, filteredTagsValues);
                } catch (SQLException e) {

                } finally {

                }
                filteredTagsValues = "";
            }
        }
	}
}
