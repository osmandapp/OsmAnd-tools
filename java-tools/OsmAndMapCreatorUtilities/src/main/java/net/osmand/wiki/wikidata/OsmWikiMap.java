package net.osmand.wiki.wikidata;

import net.osmand.PlatformUtil;
import net.osmand.data.LatLon;
import net.osmand.obf.preparation.DBDialect;
import org.apache.commons.logging.Log;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.*;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.zip.GZIPInputStream;

import static net.osmand.osm.edit.OsmMapUtils.getWeightCenterForWay;

public class OsmWikiMap {

	private static final Log log = PlatformUtil.getLog(OsmWikiMap.class);

	public static final int MB = 1 << 20;
	Map<String, LatLon> wikiOsmCoordinates = new HashMap<>();
	Map<Long, Node> mapNodes = new HashMap<>();
	int wayCount = 0;
	int nodeCount = 0;
    public static void main(String[] args) {
        File file = new File(args[0]);
        OsmWikiMap osmWikiMap = new OsmWikiMap();
        osmWikiMap.parse(file);
    }

	void parse(File wikiOsm) {
		SAXParserFactory factory = SAXParserFactory.newInstance();
        List<String> filteredTags = new ArrayList<>() {{
            add("wikipedia");
            add("wikidata");
        }};
		OsmWikiHandler osmWikiHandler = new OsmWikiHandler(this, filteredTags);
		try {
            long time = System.currentTimeMillis();
			SAXParser saxParser = factory.newSAXParser();
			GZIPInputStream is = new GZIPInputStream(new FileInputStream(wikiOsm));
			saxParser.parse(is, osmWikiHandler);
			is.close();
            long time2 = System.currentTimeMillis();
            System.out.println("Parsing time:" + (time2 - time) / 1000);
//			is = new GZIPInputStream(new FileInputStream(wikiOsm));
//			saxParser.parse(is, osmWikiHandler);
//			is.close();
//			addWaysCoordinates(osmWikiHandler);
		} catch (SAXException | ParserConfigurationException | IOException e) {
			throw new IllegalArgumentException("Error parsing osm_wiki.xml file", e);
		}

		Runtime runtime = Runtime.getRuntime();
		long usedMem1 = runtime.totalMemory() - runtime.freeMemory();
		log.info(String.format("Total parsed: nodes %d , ways %d", nodeCount, wayCount));
		log.info(String.format("Coordinates found: %d", wikiOsmCoordinates.size()));
		log.info(String.format("Unique nodes %d memory used = %d total memory= %d", mapNodes.size(), usedMem1 / MB,
				runtime.maxMemory() / MB));
	}

	private void addWaysCoordinates(OsmWikiHandler osmWikiHandler) {
		List<Way> wayList = osmWikiHandler.allWays;
		for (Way way : wayList) {
			net.osmand.osm.edit.Way osmWay = new net.osmand.osm.edit.Way(way.id);
			for (Node node : way.nodes) {
				net.osmand.osm.edit.Node osmNode = new net.osmand.osm.edit.Node(node.lat, node.lon, node.id);
				osmWay.addNode(osmNode);
			}
			LatLon latlon = getWeightCenterForWay(osmWay);
			wikiOsmCoordinates.put(way.getWikiKey(), latlon);
		}
	}

	public LatLon getCoordinates(String title, String lang, String articleTitle) {
		LatLon latLon = wikiOsmCoordinates.get(title + "#" + lang + ":" + articleTitle);
		if (latLon == null) {
			latLon = wikiOsmCoordinates.get("#" + lang + ":" + articleTitle);
		}
		return latLon;
	}

	public LatLon getCoordinates(String title) {
		return wikiOsmCoordinates.get(title);
	}

	public static class Way {
		long id;
		String wikiKey;
		List<Node> nodes = new ArrayList<>();

		public Way(long id) {
			this.id = id;
		}

		public List<Node> getNodes() {
			return nodes;
		}

		public void setWikiKey(String wikiKey) {
			this.wikiKey = wikiKey;
		}

		public String getWikiKey() {
			return wikiKey;
		}
	}

	public static class Node {
		long id;
		double lat;
		double lon;

		public Node(double lat, double lon, long id) {
			this.id = id;
			this.lat = lat;
			this.lon = lon;
		}
	}
}
