package net.osmand.wiki.wikidata;

import net.osmand.data.LatLon;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;

public class OsmWikiMap {

	Map<String, LatLon> map = new HashMap<>();

	void parse(File wikiOsm) {
		SAXParserFactory factory = SAXParserFactory.newInstance();
//		this.inputStream = stream;
//		this.progress = progress;
//		if(streamForProgress == null){
//			streamForProgress = inputStream;
//		}
//		this.streamForProgress = streamForProgress;
//		parseStarted = false;
//		this.entityInfo.clear();
//		if(progress != null){
//			progress.startWork(streamForProgress.available());
//		}

		OsmWikiHandler handler = new OsmWikiHandler();
		try {
			GZIPInputStream is = new GZIPInputStream(new FileInputStream(wikiOsm));
			SAXParser saxParser = factory.newSAXParser();
			saxParser.parse(is, handler);
		} catch (SAXException | ParserConfigurationException | IOException e) {
			throw new IllegalArgumentException("Error parsing osm_wiki.xml file", e);
		}

/*		if(progress != null){
			progress.finishTask();
		}*/
	}

	public LatLon getCoordinates(String title, String lang, String articleTitle) {
		LatLon  latLon = map.get(title + "#" + lang + ":" + articleTitle);
		if(latLon == null){
			latLon = map.get(lang + ":" + articleTitle);
		}
		return latLon;
	}

	public LatLon getCoordinates(String title) {
		return  map.get(title);
	}

	class OsmWikiHandler extends DefaultHandler {

		private final StringBuilder nameKey = new StringBuilder();
		private final StringBuilder wikidataValue = new StringBuilder();
		private final StringBuilder wikipediaValue = new StringBuilder();
		LatLon latlon;

		@Override
		public void startElement(String uri, String localName, String qName, Attributes attributes) {
			if (qName.equalsIgnoreCase("node")) {
				String lat = attributes.getValue("lat");
				String lon = attributes.getValue("lon");
				if (lat != null && lon != null) {
					latlon = new LatLon(Double.parseDouble(lat), Double.parseDouble(lon));
				}
			} else if (qName.equalsIgnoreCase("tag")) {
				if(attributes.getValue("k").equals("wikipedia")){
					wikipediaValue.append(attributes.getValue("v"));
				}else if(attributes.getValue("k").equals("wikidata")) {
					wikidataValue.append(attributes.getValue("v"));
				}
			}
		}

		@Override
		public void endElement(String uri, String localName, String qName) {
			if (qName.equalsIgnoreCase("node") && (wikidataValue.length()>0 || wikipediaValue.length()>0)) {
				nameKey.append(wikidataValue).append("#").append(wikipediaValue);
				OsmWikiMap.this.map.put(nameKey.toString(), latlon);
				wikidataValue.setLength(0);
				wikipediaValue.setLength(0);
				nameKey.setLength(0);
			}
		}
	}
}
