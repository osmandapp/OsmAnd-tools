package net.osmand.osm.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import net.osmand.util.Algorithms;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class ParseSpecialPhrases {

	static Map<String, String > mapping =new LinkedHashMap<String, String>();
	private static void map(String a, String b) {
		mapping.put(a, b);
	}

	static {
		map("shop", "shop");
		map("craft", "service");
		map("emergency", "emergency");
		map("education", "education");
		map("religion", "tourism");
		map("transport", "transportation");
		map("democracy", "administrative");
		map("office", "office");
		map("tourism", "tourism");
		map("health", "healthcare");
		map("food", "food");
		map("sport_and_entertainment", "sport");
		map("service", "service");
		map("finance", "finance");
		map("infrastructure", "man_made");
		map("natural", "natural");
		map("culture", "leisure");

	}
	/**
	 * @param args
	 * @throws IOException
	 * @throws XMLStreamException
	 * @throws JSONException
	 */
	public static void main(String[] args) throws IOException, XMLStreamException, JSONException {
		InputStream is = ParseSpecialPhrases.class.getResourceAsStream("catalog.json");
		Map<String, PoiCategory> cats = parseJson(is, false);
		if (true) {
			File parentFolder = new File("/home/victor/projects/osmand/repo/resources/specialphrases");
			Map<String, Map<String, String>> map = parseSpecialPhrases(parentFolder);
			String output = "/home/victor/projects/osmand/repo/android/OsmAnd/res/values";
			processMap(map.values(), cats);
			writePhrasesXML(map, output);
		}

	}

	private static void processMap(Collection<Map<String, String>> values, Map<String, PoiCategory> cats) throws JSONException {
		Collection<PoiCategory> categories = cats.values();
		for(Map<String, String> m : values){
			for(PoiCategory pc : categories) {
				process(pc, m);
			}
		}

	}

	private static void process(PoiCategory pc, Map<String, String> m) throws JSONException {
		for(JSONObject o : pc.poiTypes){
			JSONObject tags = o.getJSONObject("tags");
			String vl = tags.getString((String) tags.keys().next());
			if(m.containsKey(vl)) {
				m.put(o.getString("name"), m.remove(vl));
			} else if(m.containsKey(o.getString("name"))) {
				System.out.println("- " + o.getString("name"));
			}
		}
		for(PoiCategory p : pc.subtypes) {
			process(p, m);
		}
	}

	private static class PoiCategory {
		public String name;
		public boolean top;
		public List<PoiCategory> subtypes = new ArrayList<ParseSpecialPhrases.PoiCategory>();
		public List<JSONObject> poiTypes = new ArrayList<JSONObject>();
	}


	private static Map<String, PoiCategory> parseJson(InputStream is, boolean write) throws IOException, JSONException, XMLStreamException {
		BufferedReader br = new BufferedReader(new InputStreamReader(is));
		StringBuilder bs = new StringBuilder();
		String s;
		while((s = br.readLine()) != null) {
			bs.append(s);
		}
		JSONArray fullList = new JSONArray(bs.toString());
		Map<String, PoiCategory> categories = new HashMap<String, PoiCategory>();
		for(int i = 0; i < fullList.length(); i++) {
			final JSONObject object = (JSONObject) fullList.get(i);
//			System.out.println(object);
			if(object.getJSONArray("parent").length() == 0) {
				PoiCategory pc = new PoiCategory();
				pc.top = true;
				pc.name = getCatName(object.getString("name"));
				if(categories.containsKey(pc.name)) {
					if(!pc.name.equals("service") && !pc.name.equals("tourism")) {
						throw new UnsupportedOperationException(pc.name);
					}
				} else {
					categories.put(pc.name, pc);
				}
			} else {
				JSONArray jsonArray = object.getJSONArray("parent");
				PoiCategory pc  = null;
				final String poiName = object.getString("name");
				if (!object.getBoolean("poi") ) {
					pc = new PoiCategory();
					pc.name = getCatName(object.getString("name"));
					if (categories.containsKey(pc.name)) {
						throw new UnsupportedOperationException(pc.name);
					}
					categories.put(pc.name, pc);
				}
				for (int j = 0; j < jsonArray.length(); j++) {
					String parentName = getCatName(jsonArray.get(j).toString());
					PoiCategory parentCategory = categories.get(parentName);
					if (parentCategory == null) {
						throw new UnsupportedOperationException(parentName);
					}

					if(pc != null){
						parentCategory.subtypes.add(pc);
					} else {
						parentCategory.poiTypes.add(object);
					}
				}
			}

		}
		String indent = "";
		for (PoiCategory c : categories.values()) {
			if(c.top) {
				System.out.println(c.name);
//				printCategory(indent, c);
			}
		}
		if(write) {
			writeCategories(categories, "/home/victor/projects/osmand/repo/resources/poi/poi_types.xml");
		}
		return categories;
	}

	private static String getCatName(String string) {
		if(mapping.containsKey(string)) {
			return mapping.get(string);
		}
		return string;
	}

	protected static void printCategory(String indent, PoiCategory c) {
		System.out.println(indent + "--------------------------");
		System.out.println(indent + c.name);
		for (JSONObject js : c.poiTypes) {
			System.out.println(indent + "\t" + js);
		}
		for(PoiCategory p : c.subtypes) {
			printCategory("\t"+indent, p);
		}
	}

	protected static void writePhrasesXML(Map<String, Map<String, String>> map, String output)
			throws FileNotFoundException, XMLStreamException {
		for(String ln : map.keySet()) {
			write(map.get(ln), output + (ln.equals("en") ? "" : ("-" + ln)) + "/phrases.xml");
		}
	}

	protected static Map<String, Map<String, String>> parseSpecialPhrases(File parentFolder) throws IOException {
		Map<String, Map<String, String> > map = new LinkedHashMap<String, Map<String, String> >();
		File[] lf = parentFolder.listFiles();
		for (File f : lf) {
			if (f.getName().startsWith("specialphrases_")) {
				String ls = f.getName().substring("specialphrases_".length(), "specialphrases_".length() + 2);
				Map<String, String> fs = loadFile(f);
				map.put(ls, fs);
			}
		}
		return map;
	}

	public static Map<String, String> loadFile(File f) throws IOException {
		TreeMap<String, String> m = new TreeMap<String, String>();
		// The InputStream opens the resourceId and sends it to the buffer
		InputStream is = null;
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(f));
			String readLine = null;

			// While the BufferedReader readLine is not null
			while ((readLine = br.readLine()) != null) {
				String[] arr = readLine.split(",");
				if (arr != null && arr.length == 2) {
					m.put(arr[0], arr[1]);
				}

			}

		} finally {
			Algorithms.closeStream(is);
			Algorithms.closeStream(br);
		}
		return m;
	}

	private static void writeCategories(Map<String, PoiCategory> fs, String fileName) throws FileNotFoundException, XMLStreamException, JSONException {
		XMLOutputFactory xof = XMLOutputFactory.newInstance();
		File fl = new File(fileName);
		fl.getParentFile().mkdirs();
        XMLStreamWriter streamWriter = xof.createXMLStreamWriter(new OutputStreamWriter(new FileOutputStream(fileName)));

		streamWriter.writeStartDocument();
		writeStartElement(streamWriter, "poi_types", "");
		for(PoiCategory cat: fs.values()) {
			if(!cat.top){
				continue;
			}
			writeStartElement(streamWriter, "poi_category", "\t");
			streamWriter.writeAttribute("name", cat.name);
			for(PoiCategory p : cat.subtypes) {
				writeStartElement(streamWriter, "poi_filter", "\t\t");
				streamWriter.writeAttribute("name", p.name);
				writeCategory(streamWriter, p, "\t\t\t");
				writeEndElement(streamWriter, "\n\t\t");
			}
			writeCategory(streamWriter, cat, "\t\t");
			writeEndElement(streamWriter, "\n\t");
		}
		writeEndElement(streamWriter, "\n"); // osm
		streamWriter.writeEndDocument();
		streamWriter.flush();
	}

	protected static void writeCategory(XMLStreamWriter streamWriter, PoiCategory cat, String ind) throws XMLStreamException,
			JSONException {
		for(JSONObject o : cat.poiTypes) {
			writeStartElement(streamWriter, "poi_type", ind);
			streamWriter.writeAttribute("name", o.getString("name"));
			JSONObject tags = o.getJSONObject("tags");
			if(tags.length() != 1 && tags.length() != 2) {
				throw new UnsupportedOperationException(o.toString());
			}
			streamWriter.writeAttribute("tag", tags.keys().next().toString());
			streamWriter.writeAttribute("value", tags.getString((String) tags.keys().next()));
			if(tags.length() == 2) {
				Iterator keys = tags.keys();
				keys.next();
				Object k = keys.next();
				streamWriter.writeAttribute("tag2", k.toString());
				streamWriter.writeAttribute("value2", tags.getString((String) k));
			}
			writeEndElement(streamWriter, "");
		}
	}

	private static void write(Map<String, String> fs, String fileName) throws FileNotFoundException, XMLStreamException {
		XMLOutputFactory xof = XMLOutputFactory.newInstance();
		File fl = new File(fileName);
		fl.getParentFile().mkdirs();
        XMLStreamWriter streamWriter = xof.createXMLStreamWriter(new OutputStreamWriter(new FileOutputStream(fileName)));

		streamWriter.writeStartDocument();
		writeStartElement(streamWriter, "resources", "");
		for(String key : fs.keySet()) {
			Object vl = fs.get(key);
			writeStartElement(streamWriter, "string", "\t");
			streamWriter.writeAttribute("name","poi_"+key);
			String value = vl.toString();
			value = value.replace("'", "\\'");
			streamWriter.writeCharacters(value);
			writeEndElement(streamWriter, "");
		}
		writeEndElement(streamWriter, ""); // osm
		streamWriter.writeEndDocument();
		streamWriter.flush();
	}

	private static void writeStartElement(XMLStreamWriter writer, String name, String indent) throws XMLStreamException{
		writer.writeCharacters("\n"+indent);
		writer.writeStartElement(name);
	}

	private static void writeEndElement(XMLStreamWriter writer, String indent) throws XMLStreamException{
		writer.writeCharacters(""+indent);
		writer.writeEndElement();
	}

}
