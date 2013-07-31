package net.osmand.map;

import gnu.trove.list.array.TIntArrayList;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import net.osmand.map.OsmandRegionInfo.OsmAndRegionInfo;
import net.osmand.map.OsmandRegionInfo.OsmAndRegions;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

public class RegionsRegistryConverter {
	static String COUNTRIES_FILE = "../../resources/countries-info/countries.xml";
	static String COUNTRIES_OPT_FILE = "../../resources/countries-info/opt-countries.xml";
	static String OUTPUT_BINARY_FILE = "../../resources/countries-info/"+RegionRegistry.fileName;
	
	public static List<RegionCountry> parseRegions(boolean withNoValidated) throws IllegalStateException, FileNotFoundException {
		InputStream is = new FileInputStream(COUNTRIES_FILE);
		try {
			SAXParser parser = SAXParserFactory.newInstance().newSAXParser();
			RegionsHandler h = new RegionsHandler(parser, withNoValidated);
			parser.parse(new InputSource(is), h);
			return h.getCountries();
		} catch (SAXException e) {
			throw new IllegalStateException(e);
		} catch (IOException e) {
			throw new IllegalStateException(e);
		} catch (ParserConfigurationException e) {
			throw new IllegalStateException(e);
		}
	}
	
	public static void main(String[] args) throws Exception {
		validate(true);
		optimizeBoxes();
		List<RegionCountry> countries = recreateReginfo();
		checkFileRead(countries);
		
		
//		makeFlat();
		
	}
	
	public static void validate(boolean overwrite) throws SAXException, IOException, ParserConfigurationException, TransformerException {
		List<RegionCountry> regCountries = parseRegions(true);
		InputStream is = new FileInputStream(COUNTRIES_FILE);
		DocumentBuilder docbuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
		Document doc = docbuilder.parse(is);
		Map<String, Element> elements = new LinkedHashMap<String, Element>();
		parseDomRegions(doc.getDocumentElement(), elements, "", "country");
		Map<String, Element> countries = new LinkedHashMap<String, Element>(elements);
		Iterator<Entry<String, Element>> it = countries.entrySet().iterator();
		while(it.hasNext()) {
			Entry<String, Element> e = it.next();
			parseDomRegions(e.getValue(), elements, e.getKey() +"#", "region");
		}
	
		for (RegionCountry rc : regCountries) {
			validateRegion(elements, rc, rc.name);
			for (RegionCountry r : rc.getSubRegions()) {
				String rgName = rc.name + "#" + r.name;
				validateRegion(elements, r, rgName);
			}
		}
		if (overwrite) {
			TransformerFactory transformerFactory = TransformerFactory.newInstance();
			Transformer transformer = transformerFactory.newTransformer();
			DOMSource source = new DOMSource(doc);
			StreamResult result = new StreamResult(new File(COUNTRIES_FILE));

			// Output to console for testing
			// StreamResult result = new StreamResult(System.out);

			transformer.transform(source, result);
		}
	}

	private static void validateRegion(Map<String, Element> elements, RegionCountry r, String rgName) {
		Element reg = elements.get(rgName);
		String bbox = reg.getAttribute("bbox");
		String b = r.left + " " + r.top + " " + r.right + " " + r.bottom;
		if(!bbox.equals(b)) {
			System.out.println("Region " + rgName);
			System.out.println("Validate bbox '" + bbox + "' != '" +b +"'");
			reg.setAttribute("bbox", b);
		}
		
		String size = reg.getAttribute("size");
		String sz = String.valueOf(r.getTileSize());
		if(!size.equals(sz)) {
			System.out.println("Region " + rgName);
			System.out.println("Validate size '" + size + "' != '" +sz +"'");
			reg.setAttribute("size", sz);
		}
		
		// format tiles
		Element tiles = null;
		NodeList ch = elements.get(rgName).getChildNodes();
		for(int i =0; i< ch.getLength(); i++) {
			if(ch.item(i).getNodeName().equals("tiles")) {
				tiles = (Element) ch.item(i);
				break;
			}
		}
		String ts = tiles.getTextContent();
		String tsz = r.serializeTilesArray();
		if(!ts.equals(tsz)) {
			System.out.println("Region " + rgName);
			System.out.println("Format tiles '" + ts + "' != '" +tsz +"'");
			tiles.setTextContent(tsz);
		}
	}
	
	public static void makeFlat() throws SAXException, IOException, ParserConfigurationException, TransformerException {
		List<RegionCountry> regCountries = parseRegions(true);
		InputStream is = new FileInputStream(COUNTRIES_OPT_FILE);
		DocumentBuilder docbuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
		Document doc = docbuilder.parse(is);
		Map<String, Element> elements = new LinkedHashMap<String, Element>();
		parseDomRegions(doc.getDocumentElement(), elements, "", "country");
		Map<String, Element> countries = new LinkedHashMap<String, Element>(elements);
		Iterator<Entry<String, Element>> it = countries.entrySet().iterator();
		while(it.hasNext()) {
			Entry<String, Element> e = it.next();
			parseDomRegions(e.getValue(), elements, e.getKey() +"#", "region");
		}
	
		for(RegionCountry rc : regCountries) {
			Element tiles = null;
			NodeList ch = elements.get(rc.name).getChildNodes();
			for (int i = 0; i < ch.getLength(); i++) {
				if (ch.item(i).getNodeName().equals("tiles")) {
					tiles = (Element) ch.item(i);
					tiles.setTextContent(rc.serializeFlatTilesArray());
					break;
				}
			}
			
			for (RegionCountry r : rc.getSubRegions()) {
				String rgName = rc.name + "#" + r.name;
				tiles = null;
				ch = elements.get(rgName).getChildNodes();
				for (int i = 0; i < ch.getLength(); i++) {
					if (ch.item(i).getNodeName().equals("tiles")) {
						tiles = (Element) ch.item(i);
						tiles.setTextContent(r.serializeFlatTilesArray());
						break;
					}
				}
			}
		}
		TransformerFactory transformerFactory = TransformerFactory.newInstance();
		Transformer transformer = transformerFactory.newTransformer();
		DOMSource source = new DOMSource(doc);
		StreamResult result = new StreamResult(new File(COUNTRIES_FILE));
 
		// Output to console for testing
//		 StreamResult result = new StreamResult(System.out);
 
		transformer.transform(source, result);
	}


	public static void optimizeBoxes() throws SAXException, IOException, ParserConfigurationException, TransformerException {
		List<RegionCountry> regCountries = parseRegions(true);
		InputStream is = new FileInputStream(COUNTRIES_FILE);
		DocumentBuilder docbuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
		Document doc = docbuilder.parse(is);
		Map<String, Element> elements = new LinkedHashMap<String, Element>();
		parseDomRegions(doc.getDocumentElement(), elements, "", "country");
		Map<String, Element> countries = new LinkedHashMap<String, Element>(elements);
		Iterator<Entry<String, Element>> it = countries.entrySet().iterator();
		while(it.hasNext()) {
			Entry<String, Element> e = it.next();
			parseDomRegions(e.getValue(), elements, e.getKey() +"#", "region");
		}
	
		for(RegionCountry rc : regCountries) {
			optimizeRegion(elements, rc, rc.name);
			for (RegionCountry r : rc.getSubRegions()) {
				String rgName = rc.name + "#" + r.name;
				optimizeRegion(elements, r, rgName);
			}
		}
		TransformerFactory transformerFactory = TransformerFactory.newInstance();
		Transformer transformer = transformerFactory.newTransformer();
		DOMSource source = new DOMSource(doc);
		StreamResult result = new StreamResult(new File(COUNTRIES_OPT_FILE));
 
		// Output to console for testing
		// StreamResult result = new StreamResult(System.out);
 
		transformer.transform(source, result);
	}

	private static void optimizeRegion(Map<String, Element> elements, RegionCountry r, String rgName) {
		boolean optimized = new AreaOptimizer().tryToCutBigSquareArea(r, false);
		boolean replace = optimized;
		while (optimized) {
			optimized = new AreaOptimizer().tryToCutBigSquareArea(r, false);
		}
		if (replace) {
			Element tiles = null;
			NodeList ch = elements.get(rgName).getChildNodes();
			for (int i = 0; i < ch.getLength(); i++) {
				if (ch.item(i).getNodeName().equals("tiles")) {
					tiles = (Element) ch.item(i);
					break;
				}
			}
//			System.out.println("-" + tiles.getTextContent());
//			System.out.println("+" + r.serializeTilesArray());
//			System.out.println("-----------------------------------\n");
			tiles.setTextContent(r.serializeTilesArray());
		}
	}

	private static void parseDomRegions(Node parent, Map<String, Element> elements, String parentName, String tag) {
		NodeList r = ((Element) parent).getElementsByTagName(tag);
		for(int i = 0; i< r.getLength(); i++) {
			Element item = (Element) r.item(i);
			String name = parentName + item.getAttribute("name");
			if(elements.containsKey(name)) {
				throw new IllegalStateException();
			} else {
				elements.put(name, item);
			}
		}
	}

	public static List<RegionCountry> recreateReginfo() throws FileNotFoundException, IOException {
		List<RegionCountry> countries = parseRegions(false);
		OsmAndRegions.Builder regions = OsmAndRegions.newBuilder();
		for (RegionCountry c : countries) {
			regions.addRegions(c.convert());
		}
		FileOutputStream out = new FileOutputStream(OUTPUT_BINARY_FILE);
		OsmAndRegionInfo.newBuilder().setRegionInfo(regions).build().writeTo(out);
		out.close();
		return countries;
	}


	public  static void checkFileRead(List<RegionCountry> originalcountries) throws IOException {
		long t = -System.currentTimeMillis();
		InputStream in = RegionRegistry.class.getResourceAsStream(RegionRegistry.fileName);
		OsmAndRegionInfo regInfo = OsmAndRegionInfo.newBuilder().mergeFrom(in).build();
		t += System.currentTimeMillis();
		for (int j = 0; j < regInfo.getRegionInfo().getRegionsCount(); j++) {
			RegionCountry.construct(regInfo.getRegionInfo().getRegions(j));
		}
		int subregions = 0;
		for(int i = 0; i < regInfo.getRegionInfo().getRegionsCount(); i++) {
			subregions += regInfo.getRegionInfo().getRegions(i).getSubregionsCount();
		}
		int subregionsOrig = 0;
		for(RegionCountry c : originalcountries) {
			subregionsOrig += c.getSubRegions().size();
		}
		System.out.println("Read countries " + regInfo.getRegionInfo().getRegionsCount() + " " + originalcountries.size());
		System.out.println("Read countries " + subregions + " " + subregionsOrig );
		System.out.println("Timing " + t);
	}
	
	
	private static class RegionsHandler extends DefaultHandler {
		
		private SAXParser parser;
		private String continentName;
		private RegionCountry current;
		private RegionCountry currentRegion;
		private List<RegionCountry> countries = new ArrayList<RegionCountry>();
		private StringBuilder txt = new StringBuilder();
		private boolean withNoValidated;

		public RegionsHandler(SAXParser parser, boolean withNoValidated) {
			this.parser = parser;
			this.withNoValidated = withNoValidated;
		}

		@Override
		public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
			String tagName = parser.isNamespaceAware() ? localName : qName;
			if(tagName.equals("continent")) {
				continentName = attributes.getValue("name");
			} else if(tagName.equals("country")) {
				RegionCountry c = new RegionCountry();
				c.continentName = continentName;
				c.name = attributes.getValue("name");
				c.subregionsVerified = !"no".equals(attributes.getValue("verified")) || withNoValidated;
				current = c;
				countries.add(c);
			} else if(tagName.equals("tiles")) {
				txt.setLength(0);
			} else if(tagName.equals("region")) {
				RegionCountry c = new RegionCountry();
				c.name = attributes.getValue("name");
				currentRegion = c;
				if(current.subregionsVerified) {
					current.addSubregion(c);
				}
			}
		}
		@Override
		public void characters(char[] ch, int start, int length) throws SAXException {
			txt.append(ch, start, length);
		}
		
		
		@Override
		public void endElement(String uri, String localName, String qName) throws SAXException {
			String tagName = parser.isNamespaceAware() ? localName : qName;
			if(tagName.equals("region")) {
				currentRegion = null;
			} else if(tagName.equals("tiles")) {
				String[] s = txt.toString().split(";");
				Pattern p = Pattern.compile("(-?\\d+)");
				RegionCountry a = currentRegion == null ? current : currentRegion;
				for (int i = 0; i < s.length; i++) {
					try {
						Matcher m = p.matcher(s[i]);
						if (s[i].contains("x")) {
							a.add(parseInt(m), parseInt(m), parseInt(m), parseInt(m));
						} else {
							a.add(parseInt(m), parseInt(m));
						}
					} catch (RuntimeException e) {
						System.err.println(a.name);
						throw e;
					}
				}
			}
		}

		private int parseInt(Matcher m) {
			m.find();
			int i = Integer.parseInt(m.group(1));
			return i;
		}
		
		public List<RegionCountry> getCountries() {
			return countries;
		}
	}
	
	private static class AreaOptimizer {
		
		private int findExtremumCoordinate(RegionCountry r, boolean min, boolean odd) {
			int i = odd ? 1 : 0;
			TIntArrayList ts = r.getSingleTiles();
			int init = ts.get(i);
			for (; i < ts.size(); i += 2) {
				if (min) {
					init = Math.min(ts.get(i), init);
				} else {
					init = Math.max(ts.get(i), init);
				}
			}
			return init;
		}
		
		// Algorithm is taken http://e-maxx.ru/algo/maximum_zero_submatrix
		// answer is int[4]
		private int findBiggestOneSubmatrix(int[][] a,int left, int right,  int top, int bottom, int[] answer) {
			int ans = 0;
			int[] d = new int[right - left];
			Arrays.fill(d, -1);
			int[] d1 = new int[right - left], d2 = new int[right - left];
			Stack<Integer> st = new Stack<Integer>();
			for (int i = top; i < bottom; ++i) {
				for (int j = left; j < right; ++j)
					if (a[i][j] == 0)
						d[j] = i;
				while (!st.empty())
					st.pop();
				for (int j = left; j < right; ++j) {
					while (!st.empty() && d[st.peek()] <= d[j])
						st.pop();
					d1[j] = st.empty() ? -1 : st.peek();
					st.push(j);
				}
				while (!st.empty())
					st.pop();
				for (int j = right - 1; j >= left; --j) {
					while (!st.empty() && d[st.peek()] <= d[j])
						st.pop();
					d2[j] = st.empty() ? right : st.peek();
					st.push(j);
				}
				for (int j = left; j < right; ++j) {
					int nans = Math.max(ans, (i - d[j]) * (d2[j] - d1[j] - 1));
					if (nans > ans) {
						ans = nans;
						answer[0] = d[j] + 1;
						answer[1] = d1[j] + 1;
						answer[2] = i;
						answer[3] = d2[j] - 1;
					}
				}
			}
			return ans;
		}
		
		
		public boolean tryToCutBigSquareArea(RegionCountry r, boolean verbose) {
			if(r.getSingleTiles().size() == 0) {
				return false;
			}
			int minX = findExtremumCoordinate(r, true, false);
			int maxX = findExtremumCoordinate(r, false, false);
			int minY = findExtremumCoordinate(r, true, true);
			int maxY = findExtremumCoordinate(r, false, true);
			int[][] areaMatrix = new int[maxY- minY + 1][maxX - minX + 1];
			TIntArrayList ts = r.getSingleTiles();
			for (int j = 0; j < ts.size(); j += 2) {
				int x = ts.get(j);
				int y = ts.get(j + 1);
				areaMatrix[maxY - y][x - minX] = 1;
			}
			int[] sub = new int[4];
			int a = findBiggestOneSubmatrix(areaMatrix, 0, maxX - minX + 1, 0, maxY - minY + 1, sub);
			if (a >= 4) {
				int xleft = sub[1]  + minX;
				int ytop = maxY - sub[0];
				int xright = sub[3]  + minX;
				int ybottom = maxY - sub[2] ;
				for (int x = xleft; x <= xright; x++) {
					for (int y = ytop; y >= ybottom; y--) {
						r.removeSingle(x, y);
					}
				}
				r.add(xleft, ytop, xright, ybottom);
				if (verbose) {
					System.out.println("-" + r.name);
					for (int t = 0; t < areaMatrix.length; t++) {
						 System.out.println(Arrays.toString(areaMatrix[t]));
					}
					System.out.println(xleft + " " + ytop + " x " + xright + " " + ybottom + " --- " + a);

					float c = (float) r.calcAllTiles().size() / (r.getBoxTiles().size() + r.getSingleTiles().size());
					System.out.println("Compression " + c);
				}
				return true;
			}
			return false;
		}
		
		
	}
}

