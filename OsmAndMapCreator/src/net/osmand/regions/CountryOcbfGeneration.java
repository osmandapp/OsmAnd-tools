package net.osmand.regions;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import net.osmand.PlatformUtil;
import net.osmand.util.Algorithms;

import org.xmlpull.v1.XmlPullParser;
import org.xmlpull.v1.XmlPullParserException;

public class CountryOcbfGeneration {

	public static void main(String[] args) throws XmlPullParserException, IOException {
		String repo =  "/Users/victorshcherb/osmand/repos/";
		if(args != null && args.length > 0) {
			repo = args[0];
		}
		String regionsXml = repo+"resources/countries-info/regions.xml";
		String targetObf = repo+"resources/countries-info/countries.reginfo";
		String targetOsmXml = repo+"resources/countries-info/countries.osm.bz2";
		String[] polygonFolders = new String[] {
				repo +"misc/osm-planet/polygons",
				repo +"misc/osm-planet/gislab-polygons",
				repo +"misc/osm-planet/geo-polygons",	
				repo +"misc/osm-planet/srtm-polygons"
		};
		String[] translations = new String[] {
				repo +"misc/osm-planet/osm-data/states_places.osm",
				repo +"misc/osm-planet/osm-data/states_regions.osm",
				repo +"misc/osm-planet/osm-data/countries_places.osm",
				repo +"misc/osm-planet/osm-data/countries_admin_level_2.osm"
		};
		new CountryOcbfGeneration().generate(regionsXml, polygonFolders,
				translations, targetObf, targetOsmXml);
	}
	
	private static class TranslateEntity {
		private Map<String, String> tm = new TreeMap<String, String>();

		public boolean isEmpty() {
			return tm.isEmpty();
		}
	}
	
	private static class CountryRegion {
		CountryRegion parent = null;
		List<CountryRegion> children = new ArrayList<CountryRegion>();
		String name;
		String downloadSuffix;
		String innerDownloadSuffix;
		String downloadPrefix;
		String innerDownloadPrefix;
		
		String boundary;
		String translate;
		
		
		public boolean map ;
		public boolean wiki;
		public boolean roads ;
		public boolean hillshade ;
		public boolean srtm ;
		
		public String getDownloadName() {
			String s = name;
			String p = getDownloadPrefix();
			if (p != null) {
				s = p + "_" + s;
			}
			String suf = getDownloadSuffix();
			if (s != null) {
				s = s + "_" + suf;
			}
			return Algorithms.capitalizeFirstLetterAndLowercase(s);
		}
		
		
		public String getInnerDownloadPrefix() {
			if(innerDownloadPrefix != null) {
				return innerDownloadPrefix;
			}
			return getDownloadPrefix();
		}
		
		public String getDownloadPrefix() {
			if(downloadPrefix == null && parent != null) {
				return parent.getInnerDownloadPrefix();
			}
			return downloadPrefix;
		}
		
		public String getInnerDownloadSuffix() {
			if(innerDownloadSuffix != null) {
				return innerDownloadSuffix;
			}
			return getDownloadSuffix();
		}
		
		public String getDownloadSuffix() {
			if(downloadSuffix == null && parent != null) {
				return parent.getInnerDownloadSuffix();
			}
			return downloadSuffix;
		}

		public void setInnerDownloadSuffix(String string) {
			if(string != null) {
				if("$name".equals(string)) {
					innerDownloadSuffix = name;
				} else {
					innerDownloadSuffix = string;
				}
			}
		}

		public void setDownloadPrefix(String string) {
			if(string != null) {
				if("$name".equals(string)) {
					downloadPrefix = name;
				} else {
					downloadPrefix = string;
				}
			}
		}

		public void setDownloadSuffix(String string) {
			if(string != null) {
				if("$name".equals(string)) {
					downloadSuffix = name;
				} else {
					downloadSuffix = string;
				}
			}
		}

		public void setInnerDownloadPrefix(String string) {
			if(string != null) {
				if("$name".equals(string)) {
					innerDownloadPrefix = name;
				} else {
					innerDownloadPrefix = string;
				}
			}
		}
	}
	
	private void scanTranslates(File file, Map<String, Set<TranslateEntity>> translates) throws XmlPullParserException, IOException {
		XmlPullParser parser = PlatformUtil.newXMLPullParser();
		parser.setInput(new FileReader(file));
		int tok;
		TranslateEntity te = null;
		while ((tok = parser.next()) != XmlPullParser.END_DOCUMENT) {
			if (tok == XmlPullParser.START_TAG) {
				String name = parser.getName();
				if (name.equals("way") || name.equals("node") || name.equals("relation")) {
					te = new TranslateEntity();
				} else if(name.equals("tag") && te != null) {
					Map<String, String> attrs = new LinkedHashMap<String, String>();
					for (int i = 0; i < parser.getAttributeCount(); i++) {
						attrs.put(parser.getAttributeName(i), parser.getAttributeValue(i));
					}
					te.tm.put(attrs.get("k"), attrs.get("v"));
				}
			} else if (tok == XmlPullParser.END_TAG) {
				String name = parser.getName();
				if (name.equals("way") || name.equals("node") || name.equals("relation")) {
					if(!te.isEmpty()) {
						Iterator<Entry<String, String>> it = te.tm.entrySet().iterator();
						while(it.hasNext()) {
							Entry<String, String> e = it.next();
							addTranslate(translates, te, e.getKey().toLowerCase() +"="+e.getValue().toLowerCase());
						}
					}
					te = null;
				}
			}
		}

		// TODO Auto-generated method stub
		
	}

	private void addTranslate(Map<String, Set<TranslateEntity>> translates, TranslateEntity te, String k) {
		if(!translates.containsKey(k)) {
			translates.put(k, new HashSet<CountryOcbfGeneration.TranslateEntity>());
		}
		translates.get(k).add(te);
	}

	private void generate(String regionsXml, String[] polygonFolders, 
			String[] translations, String targetObf, String targetOsmXml) throws XmlPullParserException, IOException {
		Map<String, File> polygonFiles = new LinkedHashMap<String, File>();
		for (String folder : polygonFolders) {
			scanPolygons(new File(folder), polygonFiles);
		}
		Map<String, Set<TranslateEntity>> translates = new TreeMap<String, Set<TranslateEntity>>();
		for (String t : translations) {
			scanTranslates(new File(t), translates);
		}
		XmlPullParser parser = PlatformUtil.newXMLPullParser();
		parser.setInput(new FileReader(regionsXml));
		int tok;
		CountryRegion global = new CountryRegion();
		List<CountryRegion> stack = new ArrayList<CountryOcbfGeneration.CountryRegion>();
		stack.add(global);
		CountryRegion current = global;
		while ((tok = parser.next()) != XmlPullParser.END_DOCUMENT) {
			if (tok == XmlPullParser.START_TAG) {
				String name = parser.getName();
				if (name.equals("region")) {
					Map<String, String> attrs = new LinkedHashMap<String, String>();
					for (int i = 0; i < parser.getAttributeCount(); i++) {
						attrs.put(parser.getAttributeName(i), parser.getAttributeValue(i));
					}
					CountryRegion cr = createRegion(current, attrs);
					stack.add(cr);
					current = cr;
				}
			} else if (tok == XmlPullParser.END_TAG) {
				String name = parser.getName();
				if (name.equals("region")) {
					stack.remove(stack.size() - 1);
					current = stack.get(stack.size() - 1);
				}
			}
		}
		createFile(global, translates, polygonFiles, targetObf, targetOsmXml);

	}

	

	private void createFile(CountryRegion global, Map<String, Set<TranslateEntity>> translates, Map<String, File> polygonFiles,
			String targetObf, String targetOsmXml) {
		for(CountryRegion r : global.children) {
			processRegion(r, translates, polygonFiles, targetObf, targetOsmXml, "");
		}
		
	}

	private void processRegion(CountryRegion r, Map<String, Set<TranslateEntity>> translates,
			Map<String, File> polygonFiles, String targetObf, String targetOsmXml, String indent) {
		String line = "key= " + r.name;
		if(r.map || r.roads || r.wiki || r.srtm || r.hillshade) {
			line += " download=" + r.getDownloadName();
			if(r.map) {
				line += " map=yes";
			}
			if(r.wiki) {
				line += " wiki=yes";
			}
			if(r.roads) {
				line += " roads=yes";
			}
			if(r.srtm) {
				line += " srtm=yes";
			}
			if(r.hillshade) {
				line += " hillshade=yes";
			}
		}
		if(r.translate == null) {
			line += " translate-no=" + Algorithms.capitalizeFirstLetterAndLowercase(r.name);
		} else if(r.translate.startsWith("=")) {
			line += " translate-assign=" + r.translate.substring(1);
		} else {
			String[] tags = r.translate.split(";");
			Set<TranslateEntity> set = null;
			for(String t : tags) {
				if(!t.contains("=")) {
					if(translates.containsKey("name="+t)) {
						t = "name=" +t;	
					} else if(translates.containsKey("name:en="+t)) {
						t = "name:en=" + t;
					}
				}
				if(set == null) {
					set = translates.get(t);
				} else {
					Set<TranslateEntity> st2 = translates.get(t);
					if(st2 != null) {
						set = new HashSet<TranslateEntity>(set);
						set.retainAll(st2);
					}
				}
			}
			if(set == null) {
				System.out.println("!!! Couldn't find translation name " + r.translate);
			} else if(set.size() > 1) {
				System.out.println("!!! More than 1 translation " + r.translate);
			} else {
				TranslateEntity nt = set.iterator().next();
				line += " translate-" + nt.tm.size() + "=" + nt.tm.get("name");
			}
		}
		if (r.boundary != null) {
			if (!polygonFiles.containsKey(r.boundary)) {
				System.out.println("!!! Missing boundary " + r.boundary);
			} else {
				line += " boundary="+polygonFiles.get(r.boundary).getName();
			}
		}
		System.out.println(indent + line);
		for(CountryRegion c : r.children) {
			processRegion(c, translates, polygonFiles, targetObf, targetOsmXml, indent + "  ");
		}		
	}

	private CountryRegion createRegion(CountryRegion parent, Map<String, String> attrs) {
		CountryRegion reg = new CountryRegion();
		reg.parent = parent;
		if(parent != null) {
			parent.children.add(reg);
		}
		String type = attrs.get("type");
		reg.name = attrs.get("name");
		reg.setDownloadSuffix(attrs.get("download_suffix"));
		reg.setDownloadPrefix(attrs.get("download_prefix"));
		reg.setInnerDownloadSuffix(attrs.get("inner_download_suffix"));
		reg.setInnerDownloadPrefix(attrs.get("inner_download_prefix"));
		if(attrs.containsKey("hillshade")) {
			reg.hillshade = Boolean.parseBoolean(attrs.get("hillshade"));
		} else {
			reg.hillshade = type == null || type.equals("hillshade"); 
		}
		if(attrs.containsKey("srtm")) {
			reg.srtm = Boolean.parseBoolean(attrs.get("srtm"));
		} else {
			reg.srtm = type == null || type.equals("srtm"); 
		}
		if(attrs.containsKey("map")) {
			reg.map = Boolean.parseBoolean(attrs.get("map"));
		} else {
			reg.map = type == null || type.equals("map"); 
		}
		if(attrs.containsKey("roads")) {
			reg.roads = Boolean.parseBoolean(attrs.get("roads"));
		} else {
			reg.roads = reg.map;
		}
		if(attrs.containsKey("wiki")) {
			reg.wiki = Boolean.parseBoolean(attrs.get("wiki"));
		} else {
			reg.wiki = reg.map;
		}
		if(attrs.containsKey("translate")) {
			reg.translate = attrs.get("translate");
			if(reg.translate.equals("no")) {
				reg.translate = null;
			}
		} else {
			reg.translate = reg.name;
		}
		if(attrs.containsKey("boundary")) {
			reg.boundary = attrs.get("boundary");
			if(reg.boundary.equals("no")) {
				reg.boundary = null;
			}
		} else {
			reg.boundary = reg.name;
		}
		return reg;
	}

	private void scanPolygons(File file, Map<String, File> polygonFiles) {
		if(file.isDirectory()) {
			for(File c : file.listFiles()) {
				if(c.isDirectory()) {
					scanPolygons(c, polygonFiles);
				} else if(c.getName().endsWith(".poly")) {
					String name = c.getName().substring(0, c.getName().length() - 5);
					if(!polygonFiles.containsKey(name)) {
						polygonFiles.put(name, c);
					} else {
						File rm = polygonFiles.get(name);
						System.out.println("Polygon duplicate -> " + rm.getParentFile().getName() + "/" + name + " and " + 
								c.getParentFile().getName() + "/" + name);
						polygonFiles.put(rm.getParentFile().getName() + "/" + name, rm);
						polygonFiles.put(c.getParentFile().getName() + "/" + name, c);
					}
				}
			}
		}
		
	}
}
