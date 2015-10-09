package net.osmand.osm.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import net.osmand.IProgress;
import net.osmand.osm.edit.Entity;
import net.osmand.osm.edit.Node;
import net.osmand.osm.io.OsmBaseStorage;
import net.osmand.util.MapUtils;

import org.xmlpull.v1.XmlPullParserException;



public class InitiativeOsmIndexer {
	
	private static int CHARACTERS_TO_BUILD = 5;
	private static int TAG_VARIETY = 5;
	private static int ZOOM_SPLIT =  2;
	private static class TagDictionary {
		String tag;
		TreeMap<String, Frequency> values = new TreeMap<String, Frequency>();
		int freq = 0;
		int unique = 0;
		
		public TagDictionary(String tag) {
			this.tag = tag;
		}
		public void register(InitiativeTagValue k, Frequency ti) {
			parsePrefix(k.value, ti);
			freq += ti.freq;
			unique ++;
		}
		
		private void parsePrefix(String name, Frequency ti) {
			int prev = -1;
			for (int i = 0; i <= name.length(); i++) {
				if (i == name.length() || (!Character.isLetter(name.charAt(i)) && !Character.isDigit(name.charAt(i)) && name.charAt(i) != '\'')) {
					if (prev != -1) {
						String substr = name.substring(prev, i);
						String fullSubstr = substr;
						if (substr.length() > CHARACTERS_TO_BUILD) {
							substr = substr.substring(0, CHARACTERS_TO_BUILD);
						}
						String val = substr.toLowerCase();
						if(!values.containsKey(val)) {
							values.put(val, new Frequency());
						}
						values.get(val).uniqueFreq++;
						values.get(val).freq += ti.freq;
						values.get(val).values.add(fullSubstr);
//						if(!poiData.containsKey(val)){
//							poiData.put(val, new LinkedHashSet<PoiTileBox>());
//						}
//						poiData.get(val).add(data);
						prev = -1;
					}
				} else {
					if(prev == -1){
						prev = i;
					}
				}
			}
		}
		
	}
	
	private static class Frequency {
		int freq;
		int uniqueFreq;
		Set<String> values = new HashSet<String>(); 
	}
	
	private static class InitiativeTagValue {
		String tag;
		String value;
		
		@Override
		public String toString() {
			return tag+"/" + value;
		}
		
		public InitiativeTagValue(String tag, String value) {
			this.tag = tag;
			this.value = value;
		}
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((tag == null) ? 0 : tag.hashCode());
			result = prime * result + ((value == null) ? 0 : value.hashCode());
			return result;
		}
		
		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			InitiativeTagValue other = (InitiativeTagValue) obj;
			if (tag == null) {
				if (other.tag != null)
					return false;
			} else if (!tag.equals(other.tag))
				return false;
			if (value == null) {
				if (other.value != null)
					return false;
			} else if (!value.equals(other.value))
				return false;
			return true;
		}
		
	}
	
	/**
	 * @param args
	 * @throws IOException
	 * @throws XmlPullParserException
	 */
	public static void main(String[] args) throws IOException, XmlPullParserException {
		OsmBaseStorage bs = new OsmBaseStorage();
		bs.parseOSM(new FileInputStream("/Users/victorshcherb/osmand/temp/map.osm"),
				IProgress.EMPTY_PROGRESS);
		double minlat = Double.POSITIVE_INFINITY;
		double minlon = Double.POSITIVE_INFINITY;
		double maxlat = Double.NEGATIVE_INFINITY;
		double maxlon = Double.NEGATIVE_INFINITY;
		for(Entity es: bs.getRegisteredEntities().values()) {
			if(es instanceof Node) {
				minlon = Math.min(((Node) es).getLongitude(), minlon);
				maxlon = Math.max(((Node) es).getLongitude(), maxlon);
				minlat = Math.min(((Node) es).getLatitude(), minlat);
				maxlat = Math.max(((Node) es).getLatitude(), maxlat);
			}
		}
		Map<String, Frequency> possibleTags = new HashMap<String, Frequency>();
		Map<InitiativeTagValue, Frequency> possibleTagValues = new HashMap<InitiativeTagValue, Frequency>();
		for(Entity es: bs.getRegisteredEntities().values()) {
			if(!es.getTags().isEmpty()) {
				Iterator<Entry<String, String>> iterator = es.getTags().entrySet().iterator();
				while (iterator.hasNext()) {
					Entry<String, String> e = iterator.next();
					addValue(possibleTags, e.getKey());
					addValue(possibleTagValues, new InitiativeTagValue(e.getKey(), e.getValue()));
				}
			}
		}
		System.out.println("[BBOX]");
		int startZoom = 0;
		int tileX = 0;
		int tileY = 0;
		boolean equal = true; 
		while(equal) {
			int ty = (int) MapUtils.getTileNumberY(startZoom + 1, minlat);
			int my = (int) MapUtils.getTileNumberY(startZoom + 1, maxlat);
			int tx = (int) MapUtils.getTileNumberX(startZoom + 1, minlon);
			int mx = (int) MapUtils.getTileNumberX(startZoom + 1, maxlon);
			equal = ty == my && tx == mx;
			if(equal) {
				startZoom ++;
				tileX = tx;
				tileY = ty;
			}
		}
		MapUtils.getTileNumberX(startZoom, minlat);
		System.out.println("{tile:0 "+ 
				", zoom: " +startZoom +", tilex: "+ tileX +", tiley: " + tileY+
				", zoomsplit: " + ZOOM_SPLIT+
				", minlat: "+(float)minlat+", " +"minlon: " +(float)minlon +
				", maxlat: " +(float)maxlat +", maxlon: " + (float)maxlon +
				"}");
		int tileId = 0;
		int splitTileX = tileX << ZOOM_SPLIT;
		int splitTileY = tileY << ZOOM_SPLIT;
		for(int x = 0; x < MapUtils.getPowZoom(ZOOM_SPLIT); x++) {
			for (int y = 0; y < MapUtils.getPowZoom(ZOOM_SPLIT); y++) {
				tileId++;
				int tilex = (splitTileX + x);
				int tiley = (splitTileY + y);
				int zoom = (startZoom + ZOOM_SPLIT);
				System.out.println("{subtile: " + tileId + ", zoom: " + zoom + 
						", tilex:" + tilex + ", tiley:" + tiley
						+ ", minlat: " + (float)MapUtils.getLatitudeFromTile(zoom, tiley) 
						+ ", minlon: " + (float)MapUtils.getLongitudeFromTile(zoom, tilex)
						+ ", maxlat: " + (float)MapUtils.getLatitudeFromTile(zoom, tiley + 0.99999) 
						+ ", maxlon: " + (float)MapUtils.getLongitudeFromTile(zoom, tilex + 0.99999)
						+ "}");
			}
		}
		
		
		List<String> sortTags  = sortMap(possibleTags);
		List<InitiativeTagValue> sortTagValues  = sortByKey(possibleTagValues);
		System.out.println("\n[TAGS]");
		for(String k : sortTags) {
			System.out.println("{tag:'" + k + "', freq:'" + possibleTags.get(k).freq+"',"+
					"subtiles: {1,3,8,9}}");
		}
		System.out.println("\n\n[TAGSVALUES]");
		System.out.println("[[GROUPING_THRESHOLD "+TAG_VARIETY+"]]");
		Map<String, TagDictionary> mp = new LinkedHashMap<String, InitiativeOsmIndexer.TagDictionary>();
		String tg = null;
		for(InitiativeTagValue k : sortTagValues) {
			Frequency ti = possibleTagValues.get(k);
			if(ti.freq >= TAG_VARIETY) {
				System.out.println("{tag:'" + k.tag+ "', value:'"+k.value+"', freq:'" + possibleTagValues.get(k).freq+"', "+
						"subtiles: {1,3,8,14}}");
			} else {
				if(!mp.containsKey(k.tag)) {
					mp.put(k.tag, new TagDictionary(k.tag));
				}
				mp.get(k.tag).register(k, ti);
			}
			if(!k.tag.equals(tg) ){
				printTextTag(mp, tg);
			}
			tg = k.tag;
		}
		printTextTag(mp, tg);
		System.out.println("\n\n[TEXTTAGS]");
		for(TagDictionary td: mp.values()) {
			System.out.println("{texttag:'" + td.tag + "', unique:'" + td.unique + "', freq:'" + td.freq
					+ "', " + "subtiles: {4,15}}");
			for(String abbrev : td.values.keySet()) {
				Frequency fa = td.values.get(abbrev);
				String ab = fa.values.size() == 1? fa.values.iterator().next() : abbrev;
				System.out.println("{texttagvalue:'" + td.tag + "', "+
						"index:'" + ab + "', freq:'" + fa.freq+ "', "+
						"unique:'" + fa.uniqueFreq + "', "+
						"subtiles: {4,18}}");	
			}
		}
	}

	private static void printTextTag(Map<String, TagDictionary> mp, String tg) {
		if (mp.containsKey(tg)) {
			TagDictionary td = mp.get(tg);
			System.out.println("{texttag:'" + td.tag + "', unique:'" + td.unique + "', freq:'" + td.freq
					+ "', " + "subtiles: {4,15}}");
		}
	}

	private static <K> void addValue(Map<K, Frequency> map, K string) {
		if (!map.containsKey(string)) {
			map.put(string, new Frequency());
		}
		map.get(string).freq++;
	}

	private static <K> List<K> sortByKey(final Map<K, Frequency> mp) {
		final List<K> lst = new ArrayList<K>(mp.keySet());
		Collections.sort(lst, new Comparator<K>() {

			@Override
			public int compare(K o1, K o2) {
				return o1.toString().compareTo(o2.toString());
			}
		});
		return lst;
	}
	
	
	
	private static <K> List<K> sortMap(final Map<K, Frequency> possibleTags) {
		final List<K> lst = new ArrayList<K>(possibleTags.keySet());
		Collections.sort(lst, new Comparator<K>() {

			@Override
			public int compare(K o1, K o2) {
				Integer i1 = possibleTags.get(o1).freq;
				Integer i2 = possibleTags.get(o2).freq;
				return -i1.compareTo(i2);
			}
		});
		return lst;
	}
}