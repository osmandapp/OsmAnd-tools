package net.osmand.wiki.wikidata;

import net.osmand.IProgress;
import net.osmand.data.LatLon;
import net.osmand.osm.edit.Entity;
import net.osmand.osm.io.OsmBaseStorage;
import net.osmand.util.Algorithms;
import org.xmlpull.v1.XmlPullParserException;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;

public class OsmWikiMap {

	Map<String, LatLon> map = new HashMap<>();

	void parse(File wikiOsm) {
		OsmBaseStorage osmStorage = new OsmBaseStorage();
		final StringBuilder nameKey = new StringBuilder();
		try {
			InputStream is;
			if (wikiOsm.getName().endsWith(".gz")) {
				is = new GZIPInputStream(new FileInputStream(wikiOsm));
			} else {
				is = new FileInputStream(wikiOsm);
			}
			InputStream inputStream = new BufferedInputStream(is, 8192 * 4);
			osmStorage.parseOSM(inputStream, IProgress.EMPTY_PROGRESS);
		} catch (IOException e) {
			throw new IllegalArgumentException("Error parsing osm_wiki.xml file", e);
		} catch (XmlPullParserException e) {
			throw new RuntimeException(e);
		}
		for (Map.Entry<Entity.EntityId, Entity> e : osmStorage.getRegisteredEntities().entrySet()) {
			Entity entity = e.getValue();
			String wikipediaTag = entity.getTag("wikipedia");
			String wikidataTag = entity.getTag("wikidata");
			if (Algorithms.isEmpty(wikipediaTag) && Algorithms.isEmpty(wikidataTag)) {
				continue;
			}

			if (e.getKey().getType() == Entity.EntityType.WAY) {
				LatLon latlon = entity.getLatLon();
				if (!Algorithms.isEmpty(wikidataTag)) {
					nameKey.append(wikidataTag);
				}
				if (!Algorithms.isEmpty(wikipediaTag)) {
					nameKey.append("#").append(wikipediaTag);
				}
				OsmWikiMap.this.map.put(nameKey.toString(), latlon);
			} else if (e.getKey().getType() == Entity.EntityType.NODE) {
				LatLon latlon = entity.getLatLon();
				if (!Algorithms.isEmpty(wikidataTag)) {
					nameKey.append(wikidataTag);
				}
				if (!Algorithms.isEmpty(wikipediaTag)) {
					nameKey.append("#").append(wikipediaTag);
				}
				OsmWikiMap.this.map.put(nameKey.toString(), latlon);
			}
			nameKey.setLength(0);
		}
	}

	public LatLon getCoordinates(String title, String lang, String articleTitle) {
		LatLon  latLon = map.get(title + "#" + lang + ":" + articleTitle);
		if(latLon == null){
			latLon = map.get("#" + lang + ":" + articleTitle);
		}
		return latLon;
	}

	public LatLon getCoordinates(String title) {
		return  map.get(title);
	}
}
