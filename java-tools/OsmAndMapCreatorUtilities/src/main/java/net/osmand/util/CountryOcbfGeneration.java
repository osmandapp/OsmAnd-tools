package net.osmand.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import net.osmand.PlatformUtil;
import net.osmand.binary.MapZooms;
import net.osmand.impl.ConsoleProgressImplementation;
import net.osmand.obf.preparation.IndexCreator;
import net.osmand.obf.preparation.IndexCreatorSettings;
import net.osmand.osm.MapRenderingTypesEncoder;

import org.apache.commons.logging.Log;
import org.xml.sax.SAXException;
import org.xmlpull.v1.XmlPullParser;
import org.xmlpull.v1.XmlPullParserException;
import org.xmlpull.v1.XmlSerializer;

public class CountryOcbfGeneration {
	private int OSM_ID=-1000;
	private static final Log log = PlatformUtil.getLog(CountryOcbfGeneration.class);

	public static void main(String[] args) throws XmlPullParserException, IOException, SAXException, SQLException, InterruptedException {
		String repo =  "/Users/ivan/OsmAnd/";
		if(args != null && args.length > 0) {
			repo = args[0];
		}
		new CountryOcbfGeneration().generate(repo);
	}


	public Map<String, File> getPolygons(String repo) {
		String[] polygonFolders = new String[] {
				repo +"misc/osm-planet/polygons",
//				repo +"misc/osm-planet/gislab-polygons",
				repo +"misc/osm-planet/geo-polygons",
				repo +"misc/osm-planet/srtm-polygons"
		};
		Map<String, File> polygonFiles = new LinkedHashMap<String, File>();
		for (String folder : polygonFolders) {
			scanPolygons(new File(folder), polygonFiles);
		}
		return polygonFiles;
	}

	public Map<String, Set<TranslateEntity>> getTranslates(String repo) throws XmlPullParserException, IOException {
		String[] translations = new String[] {
				repo +"misc/osm-planet/osm-data/states_places.osm",
				repo +"misc/osm-planet/osm-data/states_regions.osm",
				repo +"misc/osm-planet/osm-data/countries_places.osm",
				repo +"misc/osm-planet/osm-data/countries_admin_level_2.osm"
		};
		Map<String, Set<TranslateEntity>> translates = new TreeMap<String, Set<TranslateEntity>>();
		for (String t : translations) {
			scanTranslates(new File(t), translates);
		}
		return translates;
	}

	public CountryRegion parseDefaultOsmAndRegionStructure() throws XmlPullParserException, IOException {
		URL url = new URL( "https://raw.githubusercontent.com/osmandapp/OsmAnd-resources/master/countries-info/regions.xml");
		return parseRegionStructure(url.openStream());
	}


	public CountryRegion parseRegionStructure(InputStream repo) throws XmlPullParserException, IOException {

		XmlPullParser parser = PlatformUtil.newXMLPullParser();
		parser.setInput(new InputStreamReader(repo, "UTF-8"));
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
		repo.close();
		return global;
	}

	public static class TranslateEntity {
		private Map<String, String> tm = new TreeMap<String, String>();
		private String name;

		public TranslateEntity(String name) {
			this.name = name;
		}

		public boolean isEmpty() {
			return tm.isEmpty();
		}
	}

	public static class CountryRegion {
		CountryRegion parent = null;
		List<CountryRegion> children = new ArrayList<CountryRegion>();
		static final String[] tagsPropagate = new String[] {
			"lang", "left_hand_navigation", "metric", "road_signs", "maxspeed", "maxspeed_urban", "maxspeed_rural"
		};
		Map<String, String> additionalTags = new LinkedHashMap<String, String>();
		String name;
		String downloadSuffix;
		String innerDownloadSuffix;
		String downloadPrefix;
		String innerDownloadPrefix;

		public String boundary;
		public String translate;
		public String polyExtract;
		public boolean areaExtract;

		public boolean jointMap;
		public boolean jointRoads;
		public boolean map ;
		public boolean wiki;
		public boolean roads ;
		public boolean hillshade ;
		public boolean slope;
		public boolean heightmap;
		public boolean srtm ;

		public long timestampToUpdate;



		public CountryRegion getParent() {
			return parent;
		}

		public List<CountryRegion> getChildren() {
			return children;
		}

		public Iterator<CountryRegion> iterator() {
			final LinkedList<CountryRegion> stack = new LinkedList<CountryRegion>(children);
			return new Iterator<CountryRegion>() {


				@Override
				public boolean hasNext() {
					return !stack.isEmpty();
				}

				@Override
				public CountryRegion next() {
					CountryRegion reg = stack.pollFirst();
					stack.addAll(0, reg.children);
					return reg;
				}

				@Override
				public void remove() {
					throw new UnsupportedOperationException();
				}
			};
		}

		public String getPolyExtract() {
			if (!Algorithms.isEmpty(polyExtract) || parent == null) {
				return polyExtract;
			}
			return parent.getPolyExtract();
		}

		public String getSinglePolyExtract() {
			return polyExtract;
		}

		public String getAdditionalTag(String tg) {
			if(!Algorithms.isEmpty(additionalTags.get(tg)) || parent == null) {
				return additionalTags.get(tg);
			}
			return parent.getAdditionalTag(tg);
		}


		public String getFullName() {
			if(parent == null) {
				return name;
			} else {
				return parent.getFullName() + "_" + name;
			}
		}

		public String getDownloadName() {
			String s = name;
			String p = getDownloadPrefix();
			if (p != null && p.length() > 0) {
				s = p + "_" + s;
			}
			String suf = getDownloadSuffix();
			if (suf != null && suf.length() > 0) {
				s = s + "_" + suf;
			}
			return s;
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
			return downloadPrefix == null ? "" : downloadPrefix;
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
			return downloadSuffix == null ? "" : downloadSuffix;
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

		public boolean hasMapFiles() {
			if(map) {
				return true;
			}
			for(CountryRegion c : children) {
				if(c.hasMapFiles()) {
					return true;
				}
			}
			return false;
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
					te = new TranslateEntity(name);
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
						addTranslate(translates, te, "entity="+te.name);
						while(it.hasNext()) {
							Entry<String, String> e = it.next();
							addTranslate(translates, te, e.getKey().toLowerCase() +"="+e.getValue().toLowerCase());
						}
					}
					te = null;
				}
			}
		}
	}

	private void addTranslate(Map<String, Set<TranslateEntity>> translates, TranslateEntity te, String k) {
		if(!translates.containsKey(k)) {
			translates.put(k, new HashSet<CountryOcbfGeneration.TranslateEntity>());
		}
		translates.get(k).add(te);
	}

	private void generate(String repo) throws XmlPullParserException, IOException, SAXException, SQLException, InterruptedException {
		String targetObf = repo + "regions.ocbf";
		String targetOsmXml = repo + "regions.osm.xml";
		Map<String, Set<TranslateEntity>> translates = getTranslates(repo);
		Map<String, File> polygonFiles = getPolygons(repo);
		CountryRegion global = parseRegionStructureFromRepo(repo);
		createFile(global, translates, polygonFiles, targetObf, targetOsmXml);
	}


	public CountryRegion parseRegionStructureFromRepo(String repo) throws XmlPullParserException, IOException,
			FileNotFoundException {
		String regionsXml = repo + "/resources/countries-info/regions.xml";
		return parseRegionStructure(new FileInputStream(regionsXml));
	}



	private void createFile(CountryRegion global, Map<String, Set<TranslateEntity>> translates, Map<String, File> polygonFiles,
			String targetObf, String targetOsmXml) throws IOException, SQLException, InterruptedException, XmlPullParserException {
		File osm = new File(targetOsmXml);
		XmlSerializer serializer = new org.kxml2.io.KXmlSerializer();
		FileOutputStream fous = new FileOutputStream(osm);
		serializer.setOutput(fous, "UTF-8");
		serializer.startDocument("UTF-8", true);
		serializer.startTag(null, "osm");
		serializer.attribute(null, "version", "0.6");
		serializer.attribute(null, "generator", "OsmAnd");
		serializer.setFeature(
				"http://xmlpull.org/v1/doc/features.html#indent-output", true);
		for(CountryRegion r : global.children) {
			r.parent = null;
			processRegion(r, translates, polygonFiles, targetObf, targetOsmXml, "", serializer);
		}
		serializer.endDocument();
		serializer.flush();
		fous.close();

		IndexCreatorSettings settings = new IndexCreatorSettings();
		settings.indexMap = true;
		settings.indexAddress = false;
		settings.indexPOI = false;
		settings.indexTransport = false;
		settings.indexRouting = false;

		IndexCreator creator = new IndexCreator(new File(targetObf).getParentFile(), settings); //$NON-NLS-1$
		creator.setMapFileName(new File(targetObf).getName());
		MapZooms zooms = MapZooms.parseZooms("5-6");
        MapRenderingTypesEncoder encoder = new MapRenderingTypesEncoder("regions");
        encoder.addExternalAdditionalText("short_name", true);
        encoder.addExternalAdditionalText("alt_name", true);
        encoder.addExternalAdditionalText("name:abbreviation", false);
        creator.generateIndexes(osm,
				new ConsoleProgressImplementation(1), null, zooms,
				encoder, log);

	}

	private static void addTag(XmlSerializer serializer, String key, String value) throws IOException {
		serializer.startTag(null, "tag");
		serializer.attribute(null, "k", key);
		serializer.attribute(null, "v", value);
		serializer.endTag(null, "tag");
	}

	private void processRegion(CountryRegion r, Map<String, Set<TranslateEntity>> translates,
			Map<String, File> polygonFiles, String targetObf, String targetOsmXml, String indent, XmlSerializer serializer)
					throws IOException {
		if (r.areaExtract) {
			// skip helper regions
			return;
		}
		String line = "key=" + r.name;
		File boundary = null;
		if (r.boundary != null) {
			boundary = polygonFiles.get(r.boundary);
			if (boundary == null) {
				System.out.println("!!! Missing boundary " + r.boundary);
			} else {
				line += " boundary="+boundary.getName();
			}
		}
		List<List<String>> boundaryPoints = Collections.emptyList();
		if(boundary != null) {
			boundaryPoints = readBoundaryPoints(boundary, serializer);
		}
		if (boundaryPoints.size() > 0) {
			// find the biggest with points
			List<String> ls = boundaryPoints.get(0);
			for (int i = 0; i < boundaryPoints.size(); i++) {
				if(ls.size() < boundaryPoints.get(i).size()) {
					ls = boundaryPoints.get(i);
				}
			}
			for (int i = 0; i < boundaryPoints.size(); i++) {
				if(boundaryPoints.get(i) == ls) {
					continue;
				}
				writeWay(serializer, boundaryPoints.get(i));
				addTag(serializer, "osmand_region", "boundary");
				addTag(serializer, "key_name", r.name);
				addTag(serializer, "download_name", r.getDownloadName());
				addTag(serializer, "region_full_name", r.getFullName());
				serializer.endTag(null, "way");
			}
			writeWay(serializer, ls);
		} else {
			serializer.startTag(null, "node");
			serializer.attribute(null, "id", OSM_ID-- +"");
			serializer.attribute(null, "visible", "true");
			serializer.attribute(null, "lat", "0");
			serializer.attribute(null, "lon", "0");
		}
		addTag(serializer, "osmand_region", "yes");
		addTag(serializer, "key_name", r.name);
		addTag(serializer, "region_full_name", r.getFullName());
		if(r.parent != null) {
			addTag(serializer, "region_parent_name", r.parent.getFullName());
		}
		for(String tg : CountryRegion.tagsPropagate) {
			if(!Algorithms.isEmpty(r.getAdditionalTag(tg))) {
		 		addTag(serializer, "region_" + tg, r.getAdditionalTag(tg));
			}
		}
		if (r.map || r.roads || r.wiki || r.srtm || r.hillshade || r.slope || r.heightmap) {
			line += " download=" + r.getDownloadName();
			addTag(serializer, "download_name", r.getDownloadName());
			addTag(serializer, "region_prefix", r.getDownloadPrefix());
			addTag(serializer, "region_suffix", r.getDownloadSuffix()); // add exception for Russia for BW?
			if(r.map) {
				line += " map=yes";
				addTag(serializer, "region_map", "yes");
			}
			if(r.jointMap) {
				line += " join_map_files=yes";
				addTag(serializer, "region_join_map", "yes");
			}
			if (r.wiki) {
				line += " wiki=yes";
				addTag(serializer, "region_wiki", "yes");
			}
			if (r.roads) {
				line += " roads=yes";
				addTag(serializer, "region_roads", "yes");
			}
			if(r.jointRoads) {
				line += " join_road_files=yes";
				addTag(serializer, "region_join_roads", "yes");
			}
			if (r.srtm) {
				line += " srtm=yes";
				addTag(serializer, "region_srtm", "yes");
			}
			if (r.hillshade) {
				line += " hillshade=yes";
				addTag(serializer, "region_hillshade", "yes");
			}
			if (r.slope) {
				line += " slope=yes";
				addTag(serializer, "region_slope", "yes");
			}
			if (r.heightmap) {
				line += " heightmap=yes";
				addTag(serializer, "region_heightmap", "yes");
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
					if(set == null) {
						break;
					}
				} else {
					Set<TranslateEntity> st2 = translates.get(t);
					if(st2 != null) {
						set = new HashSet<TranslateEntity>(set);
						set.retainAll(st2);
					} else {
						set = null;
						break;
					}
				}
			}
			if(set == null || set.size() == 0) {
				System.out.println("!!! Couldn't find translation name " + r.translate);
			} else if(set.size() > 1) {
				System.out.println("!!! More than 1 translation " + r.translate);
			} else {
				TranslateEntity nt = set.iterator().next();
				line += " translate-" + nt.tm.size() + "=" + nt.tm.get("name");
				Iterator<Entry<String, String>> it = nt.tm.entrySet().iterator();
				while(it.hasNext()) {
					Entry<String, String> e = it.next();
					addTag(serializer, e.getKey(), e.getValue());
				}
			}
		}

		// COMMENT TO SEE ONLY WARNINGS
		System.out.println(indent + line);


		if(boundaryPoints.size() > 0) {
			serializer.endTag(null, "way");
		} else {
			serializer.endTag(null, "node");
		}


		for(CountryRegion c : r.children) {
			processRegion(c, translates, polygonFiles, targetObf, targetOsmXml, indent + "  ", serializer);
		}
	}

	private void writeWay(XmlSerializer serializer, List<String> ls) throws IOException {
		serializer.startTag(null, "way");
		serializer.attribute(null, "id", OSM_ID-- + "");
		serializer.attribute(null, "visible", "true");
		for (String bnd : ls) {
			serializer.startTag(null, "nd");
			serializer.attribute(null, "ref", bnd);
			serializer.endTag(null, "nd");
		}
	}

	private List<List<String>> readBoundaryPoints(File boundary, XmlSerializer serializer) throws IOException {
		List<List<String>> res = new ArrayList<List<String>>();
		List<String> l = new ArrayList<String>();
		BufferedReader br = new BufferedReader(new FileReader(boundary));
		br.readLine(); // name
		boolean newContour = true;
		String s;
		while ((s = br.readLine()) != null) {
			if (newContour) {
				// skip
				newContour = false;
				if(l.size()  >0) {
					res.add(l);
				}
				l = new ArrayList<String>();
			} else if (s.trim().length() == 0) {
			} else if (s.trim().equals("END")) {
				newContour = true;
			} else {
				s = s.trim();
				int i = s.indexOf(' ');
				if(i == -1) {
					i = s.indexOf('\t');
				}
				if(i == -1) {
					System.err.println("? " +s);
				}
				String lat = s.substring(i, s.length()).trim();
				String lon = s.substring(0, i).trim();
				serializer.startTag(null, "node");
				try {
					serializer.attribute(null, "lat", Double.parseDouble(lat)+"");
					serializer.attribute(null, "lon", Double.parseDouble(lon)+"");
				} catch (NumberFormatException e) {
					System.err.println(lat + " " + lon);
					e.printStackTrace();
				}
				long id = OSM_ID--;
				l.add(id + "");
				serializer.attribute(null, "id", id + "");
				serializer.endTag(null, "node");
			}
		}
		if(l.size()  >0) {
			res.add(l);
		}
		br.close();
		return res;
	}

	private CountryRegion createRegion(CountryRegion parent, Map<String, String> attrs) {
		CountryRegion reg = new CountryRegion();
		reg.parent = parent;
		if(parent != null) {
			parent.children.add(reg);
		}
		String type = attrs.get("type");
		reg.areaExtract = "area-extract".equals(type);
		reg.name = attrs.get("name");
		reg.setDownloadSuffix(attrs.get("download_suffix"));
		reg.setDownloadPrefix(attrs.get("download_prefix"));
		reg.setInnerDownloadSuffix(attrs.get("inner_download_suffix"));
		reg.setInnerDownloadPrefix(attrs.get("inner_download_prefix"));
		for (String tg : CountryRegion.tagsPropagate) {
			reg.additionalTags.put(tg, attrs.get(tg));
		}
		if (attrs.containsKey("hillshade")) {
			reg.hillshade = parseBoolean(attrs.get("hillshade"));
		} else {
			reg.hillshade = type == null || type.equals("hillshade");
		}
		if (attrs.containsKey("heightmap")) {
			reg.heightmap = parseBoolean(attrs.get("heightmap"));
		} else {
			reg.heightmap = type == null || type.equals("heightmap");
		}
		if (attrs.containsKey("slope")) {
			reg.slope = parseBoolean(attrs.get("slope"));
		} else {
			reg.slope = type == null || type.equals("slope");
		}
		if (attrs.containsKey("srtm")) {
			reg.srtm = parseBoolean(attrs.get("srtm"));
		} else {
			reg.srtm = type == null || type.equals("srtm");
		}
		if (attrs.containsKey("map")) {
			reg.map = parseBoolean(attrs.get("map"));
		} else {
			reg.map = type == null || type.equals("map");
		}
		if (attrs.containsKey("join_road_files")) {
			reg.jointRoads = parseBoolean(attrs.get("join_road_files"));
		}
		if (attrs.containsKey("join_map_files")) {
			reg.jointMap = parseBoolean(attrs.get("join_map_files"));
		}

		if (attrs.containsKey("roads")) {
			reg.roads = parseBoolean(attrs.get("roads"));
		} else {
			reg.roads = reg.map;
		}
		if (attrs.containsKey("wiki")) {
			reg.wiki = parseBoolean(attrs.get("wiki"));
		} else {
			reg.wiki = reg.map;
		}
		if (attrs.containsKey("poly_extract")) {
			reg.polyExtract = attrs.get("poly_extract");
		}
		if (attrs.containsKey("translate")) {
			reg.translate = attrs.get("translate").toLowerCase();
			if (reg.translate.equals("no")) {
				reg.translate = null;
			}
		} else {
			reg.translate = "entity=node;" + reg.name.toLowerCase();
		}
		if (attrs.containsKey("boundary")) {
			reg.boundary = attrs.get("boundary");
			if (reg.boundary.equals("no")) {
				reg.boundary = null;
			}
		} else {
			reg.boundary = reg.name;
		}
		return reg;
	}

	private boolean parseBoolean(String string) {
		return Boolean.parseBoolean(string) || "yes".equalsIgnoreCase(string);
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
						File rm = polygonFiles.remove(name);
						System.out.println("Polygon duplicate -> " + rm.getParentFile().getName() + "/" + name + " and " +
								c.getParentFile().getName() + "/" + name);
					}
					polygonFiles.put(c.getParentFile().getName() + "/" + name, c);
				}
			}
		}

	}
}
