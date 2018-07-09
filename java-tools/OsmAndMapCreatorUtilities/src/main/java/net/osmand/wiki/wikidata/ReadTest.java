package net.osmand.wiki.wikidata;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.sql.SQLException;
import java.util.zip.GZIPInputStream;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.kxml2.io.KXmlParser;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;
import org.xmlpull.v1.XmlPullParser;
import org.xmlpull.v1.XmlPullParserException;

import net.osmand.data.LatLon;
import net.osmand.impl.FileProgressImplementation;
import net.osmand.map.OsmandRegions;

public class ReadTest {

	public static void main(String[] args) throws IOException, ParserConfigurationException, SAXException, SQLException, XmlPullParserException {
		FileProgressImplementation fprog = new FileProgressImplementation("", new File("/Users/victorshcherb/osmand/temp/wikidatawiki.xml.gz"));
		InputStream is = fprog.openFileInputStream();
		GZIPInputStream zis = new GZIPInputStream(is);
		
		SAXParser sx = SAXParserFactory.newInstance().newSAXParser();
		OsmandRegions reg = new OsmandRegions();
		reg.prepareFile();
		reg.cacheAllCountries();
		
		XmlPullParser ps = new KXmlParser();
		ps.setInput(zis, "UTF-8");
		int next;
		while ((next = ps.next()) != XmlPullParser.END_DOCUMENT) {
			if (next == XmlPullParser.START_TAG) {
				fprog.update();
				if (ps.getName().equals("trkpt")) {
				} 
			} else if(next == XmlPullParser.TEXT) {
			} else if (next == XmlPullParser.END_TAG) {
			}
		}
		
//		sx.parse(zis, new WikiDataHandler(sx, fprog, new File("test.sqlite"), reg));
//		sx.parse(zis, new DefaultHandler(){
//			@Override
//			public void endElement(String uri, String localName, String qName) throws SAXException {
//				super.endElement(uri, localName, qName);
//				fprog.update();
//			}
//		});
//		
//		simpleRead(fprog, zis);
	}

	private static void simpleRead(FileProgressImplementation fis, GZIPInputStream zis) throws IOException {
		byte[] buf = new byte[8192];
		int rd;
		while((rd = zis.read(buf)) != -1) {
			fis.update();
		}
	}
	
	private static InputSource getInputSource(InputStream zis) throws IOException {
		Reader reader = new InputStreamReader(zis,"UTF-8");
		InputSource is = new InputSource(reader);
		is.setEncoding("UTF-8");
		return is;
	}
}
