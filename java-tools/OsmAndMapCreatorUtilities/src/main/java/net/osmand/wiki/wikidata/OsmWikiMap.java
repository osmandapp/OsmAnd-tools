package net.osmand.wiki.wikidata;

import net.osmand.PlatformUtil;
import net.osmand.data.LatLon;
import org.apache.commons.logging.Log;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.*;
import java.util.*;
import java.util.zip.GZIPInputStream;

import static net.osmand.osm.edit.OsmMapUtils.getWeightCenterForWay;

public class OsmWikiMap {

	private static final Log log = PlatformUtil.getLog(OsmWikiMap.class);

	public static final int MB = 1 << 20;
	Map<String, LatLon> map = new HashMap<>();
	Map<Long, Node> mapNodes = new HashMap<>();
	int wayCount = 0;
	int nodeCount = 0;
	Runtime runtime = Runtime.getRuntime();
	OsmWikiWayHandler wayHandler = new OsmWikiWayHandler(this);

	void parse(File wikiOsm) {
		SAXParserFactory factory = SAXParserFactory.newInstance();
		OsmWikiHandler handler = new OsmWikiHandler();
		try {
			SAXParser saxParser = factory.newSAXParser();
			GZIPInputStream is = new GZIPInputStream(new FileInputStream(wikiOsm));
			saxParser.parse(is, handler);
			is.close();
			is = new GZIPInputStream(new FileInputStream(wikiOsm));
			saxParser.parse(is, wayHandler);
			is.close();
			addWaysCoordinates();
		} catch (SAXException | ParserConfigurationException | IOException e) {
			throw new IllegalArgumentException("Error parsing osm_wiki.xml file", e);
		}

		Runtime runtime = Runtime.getRuntime();
		long usedMem1 = runtime.totalMemory() - runtime.freeMemory();
		log.info(String.format("Total parsed: nodes %d , ways %d", nodeCount, wayCount));
		log.info(String.format("Coordinates found: %d", map.size()));
		log.info(String.format("Unique nodes %d memory used = %d total memory= %d", mapNodes.size(), usedMem1/MB,
				runtime.maxMemory()/MB));
	}

	private void addWaysCoordinates() {
		List<Way> wayList = wayHandler.allWays;
		for (Way way : wayList) {
			net.osmand.osm.edit.Way osmWay = new net.osmand.osm.edit.Way(way.id);
			for(Node node:way.nodes) {
				net.osmand.osm.edit.Node osmNode = new net.osmand.osm.edit.Node(node.lat, node.lon, node.id);
				osmWay.addNode(osmNode);
			}
			LatLon latlon = getWeightCenterForWay(osmWay);
			map.put(way.getWikiKey(), latlon);
		}
	}

	public LatLon getCoordinates(String title, String lang, String articleTitle) {
		LatLon latLon = map.get(title + "#" + lang + ":" + articleTitle);
		if (latLon == null) {
			latLon = map.get("#" + lang + ":" + articleTitle);
		}
		return latLon;
	}

	public LatLon getCoordinates(String title) {
		return map.get(title);
	}

	class OsmWikiHandler extends DefaultHandler {

		private final StringBuilder nameKey = new StringBuilder();
		private final StringBuilder wikidataValue = new StringBuilder();
		private final StringBuilder wikipediaValue = new StringBuilder();
		LatLon latlon;
		long id;

		@Override
		public void startElement(String uri, String localName, String qName, Attributes attributes) {
			if (qName.equalsIgnoreCase("node")) {
				nodeCount++;
				if (nodeCount % 1000000 == 0) {
					long usedMem1 = runtime.totalMemory() - runtime.freeMemory();
					log.info(String.format("Node %d memory used = %d",nodeCount, usedMem1/ MB));
				}
				String idStr = attributes.getValue("id");
				if(idStr != null) {
					id = Long.parseLong(idStr);
				}
				String lat = attributes.getValue("lat");
				String lon = attributes.getValue("lon");
				if (lat != null && lon != null) {
					latlon = new LatLon(Double.parseDouble(lat), Double.parseDouble(lon));
				}
			} else if (qName.equalsIgnoreCase("tag")) {
				if (attributes.getValue("k").equals("wikipedia")) {
					wikipediaValue.append(attributes.getValue("v"));
				} else if (attributes.getValue("k").equals("wikidata")) {
					wikidataValue.append(attributes.getValue("v"));
				}
			}
		}

		@Override
		public void endElement(String uri, String localName, String qName) {
			if (qName.equalsIgnoreCase("node") && (wikidataValue.length() > 0 || wikipediaValue.length() > 0)) {
				nameKey.append(wikidataValue).append("#").append(wikipediaValue);
				map.put(nameKey.toString(), latlon);
				Node n = mapNodes.get(id);
				if(n == null){
					mapNodes.put(id,new Node(latlon.getLatitude(), latlon.getLongitude(), id));
				}
				wikidataValue.setLength(0);
				wikipediaValue.setLength(0);
				nameKey.setLength(0);
			} else if (qName.equalsIgnoreCase("node") && (wikidataValue.length() == 0 && wikipediaValue.length() == 0)) {
				Node n = mapNodes.get(id);
				if(n == null){
					mapNodes.put(id, new Node(latlon.getLatitude(), latlon.getLongitude(), id));
				}
			}
		}
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
