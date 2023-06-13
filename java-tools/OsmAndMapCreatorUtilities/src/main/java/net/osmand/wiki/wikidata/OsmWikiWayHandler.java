package net.osmand.wiki.wikidata;

import net.osmand.PlatformUtil;
import org.apache.commons.logging.Log;
import org.xml.sax.Attributes;
import org.xml.sax.helpers.DefaultHandler;

import java.util.ArrayList;
import java.util.List;

import static net.osmand.wiki.wikidata.OsmWikiMap.*;
import static net.osmand.wiki.wikidata.OsmWikiMap.MB;

class OsmWikiWayHandler extends DefaultHandler {

	private static final Log log = PlatformUtil.getLog(OsmWikiWayHandler.class);

	private final OsmWikiMap osmWikiMap;
	private final StringBuilder nameKey = new StringBuilder();
	private final StringBuilder wikidataValue = new StringBuilder();
	private final StringBuilder wikipediaValue = new StringBuilder();
	boolean parseWay;
	Way way;
	List<Way> allWays = new ArrayList<>();
	Runtime runtime = Runtime.getRuntime();

	public OsmWikiWayHandler(OsmWikiMap osmWikiMap) {
		this.osmWikiMap = osmWikiMap;
	}

	@Override
	public void startElement(String uri, String localName, String qName, Attributes attributes) {
		if (qName.equalsIgnoreCase("way")) {
			parseWay = true;

			long id = Long.parseLong(attributes.getValue("id"));
			way = new Way(id);

			osmWikiMap.wayCount++;
			if (osmWikiMap.wayCount % 100000 == 0) {
				long usedMem1 = runtime.totalMemory() - runtime.freeMemory();
				log.info(String.format("Way %d memory used = %d", osmWikiMap.wayCount, usedMem1/MB));
			}
		} else if (qName.equalsIgnoreCase("tag") && parseWay) {
			if (attributes.getValue("k").equals("wikipedia")) {
				wikipediaValue.append(attributes.getValue("v"));
			} else if (attributes.getValue("k").equals("wikidata")) {
				wikidataValue.append(attributes.getValue("v"));
			}
		} else if ((qName.equalsIgnoreCase("nd")) && parseWay){
			long nodeId = Long.parseLong(attributes.getValue("ref"));
			Node node  = osmWikiMap.mapNodes.get(nodeId);
			if(node != null){
				way.nodes.add(node);
			} else {
				throw new IllegalArgumentException();
			}
		}
	}

	@Override
	public void endElement(String uri, String localName, String qName) {
		if (qName.equalsIgnoreCase("way")) {
			parseWay = false;
			if(wikidataValue.length() > 0 || wikipediaValue.length() > 0) {
				nameKey.append(wikidataValue).append("#").append(wikipediaValue);
				way.setWikiKey(nameKey.toString());
				allWays.add(way);
				wikidataValue.setLength(0);
				wikipediaValue.setLength(0);
				nameKey.setLength(0);
			}
		}
	}
}
