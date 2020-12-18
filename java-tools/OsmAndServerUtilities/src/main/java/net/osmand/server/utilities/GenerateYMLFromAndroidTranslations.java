package net.osmand.server.utilities;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;

import org.xmlpull.v1.XmlPullParser;
import org.xmlpull.v1.XmlPullParserException;

import net.osmand.PlatformUtil;

public class GenerateYMLFromAndroidTranslations {

	public static void main(String[] args) throws XmlPullParserException, IOException {
//		parse("/strings.xml");
//		parse("-de/strings.xml");
		parse("-ru/strings.xml");
	}

	private static void parse(String name) throws XmlPullParserException, IOException {
		XmlPullParser parser = PlatformUtil.newXMLPullParser();
		FileInputStream fis = new FileInputStream(new File("../../../android/OsmAnd/res/values" + name));
		parser.setInput(getUTF8Reader(fis));
		int tok;
		String key = "";
		StringBuilder vl = new StringBuilder();
		while ((tok = parser.next()) != XmlPullParser.END_DOCUMENT) {
			if (tok == XmlPullParser.START_TAG) {
				String tag = parser.getName();
				if ("string".equals(tag)) {
					key = parser.getAttributeValue("", "name");
				}
			} else if (tok == XmlPullParser.TEXT) {
				vl.append(parser.getText());
			} else if (tok == XmlPullParser.END_TAG) {
				String tag = parser.getName();
				if ("string".equals(tag)) {
					// replace("\"", "\\\"")
					System.out.println(key + ": \"" + processLine(vl) + "\"");
				}
				vl.setLength(0);
			}
		}
		fis.close();
	}

	private static String processLine(StringBuilder vl) {
		for (int i = 1; i < vl.length(); i++) {
			if (vl.charAt(i) == '"' && vl.charAt(i - 1) != '\\') {
				vl.insert(i, '\\');
			} else if (vl.charAt(i) == '\'' && vl.charAt(i - 1) == '\\') {
				vl.deleteCharAt(i - 1);
			} else if (vl.charAt(i) == 'n' && vl.charAt(i - 1) == '\\') {
				vl.deleteCharAt(i);
				vl.deleteCharAt(i -1);
				vl.insert(i - 1, ' ');
			}
		}
		
		return vl.toString().trim();
	}

	private static Reader getUTF8Reader(InputStream f) throws IOException {
		BufferedInputStream bis = new BufferedInputStream(f);
		assert bis.markSupported();
		bis.mark(3);
		boolean reset = true;
		byte[] t = new byte[3];
		bis.read(t);
		if (t[0] == ((byte) 0xef) && t[1] == ((byte) 0xbb) && t[2] == ((byte) 0xbf)) {
			reset = false;
		}
		if (reset) {
			bis.reset();
		}
		return new InputStreamReader(bis, "UTF-8");
	}
}
