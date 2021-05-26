package net.osmand.server.utilities;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.TreeSet;

import org.xmlpull.v1.XmlPullParser;
import org.xmlpull.v1.XmlPullParserException;

import net.osmand.PlatformUtil;

public class GenerateYMLFromAndroidTranslations {

	public static void main(String[] args) throws XmlPullParserException, IOException {
		convertAndroidTranslationsToYml("../../../android/OsmAnd/res/");
		convertIosTranslationsToYml("../../../ios/Resources/");
	}
	
	

	public static void convertAndroidTranslationsToYml(String path)
			throws FileNotFoundException, XmlPullParserException, IOException {
		File fs = new File(path);
		File outDir = new File("yml-translations");
		outDir.mkdir();
		for (File f : fs.listFiles()) {
			File str = new File(f, "strings.xml");
			if (str.exists() && f.getName().startsWith("values")) {
				FileOutputStream output = new FileOutputStream(new File(outDir, "android-" + f.getName() + ".yml"));
				parseXml(str, output);
				output.close();
			}
			File phr = new File(f, "phrases.xml");
			if (phr.exists()) {
				FileOutputStream output = new FileOutputStream(new File(outDir, "phrases-" + f.getName() + ".yml"));
				parseXml(phr, output);
				output.close();
			}
		}
	}
	
	public static void convertIosTranslationsToYml(String path)
			throws FileNotFoundException, XmlPullParserException, IOException {
		File fs = new File(path);
		File outDir = new File("yml-translations");
		outDir.mkdir();
		for (File f : fs.listFiles()) {
			File str = new File(f, "Localizable.strings");
			if (str.exists() && f.getName().endsWith(".lproj")) {
				String loc = f.getName().substring(0, f.getName().indexOf('.'));
				if (loc.equals("en")) {
					loc = "";
				} else {
					loc = "-" + loc;
				}
				FileOutputStream output = new FileOutputStream(new File(outDir, "ios-values" + loc + ".yml"));
				parseText(str, output, loc);
				output.close();
			}
		}
	}
	

	private static void parseText(File f, OutputStream out, String loc) throws XmlPullParserException, IOException {
		BufferedReader br = new BufferedReader(new FileReader(f, StandardCharsets.UTF_8));
		try {
			String line = "";
			String oline = "";
			Set<String> uniqueKeys = new TreeSet<>();
			int ln = 0;
			while ((line = br.readLine()) != null) {
				oline += line.trim();
				ln++;
				if (oline.endsWith(";")) {
					if (!oline.equals(";")) {
						try {
							int eq = oline.indexOf('=');
							String keyRaw = oline.substring(0, eq);
							String valueRaw = oline.substring(eq + 1);
							String key = keyRaw.substring(keyRaw.indexOf('\"') + 1, keyRaw.lastIndexOf('\"'));
							if (!uniqueKeys.contains(key)) {
								uniqueKeys.add(key);
								StringBuilder vl = new StringBuilder(
										valueRaw.substring(valueRaw.indexOf('\"') + 1, valueRaw.lastIndexOf('\"')));
								out.write((key + ": \"" + processLine(vl) + "\"\n").getBytes());
							}
						} catch (RuntimeException e) {
							throw new IllegalArgumentException(String.format("Parsing line '%s' of %s:%d crashed.", 
									oline, loc + "-" + f.getName(), ln), e);
						}
					}
					oline = "";
				} else {
					oline += "\n";
				}
			}
		} finally {
			br.close();
		}
	}
	


	private static void parseXml(File f, OutputStream out) throws XmlPullParserException, IOException {
		XmlPullParser parser = PlatformUtil.newXMLPullParser();
		FileInputStream fis = new FileInputStream(f);
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
					out.write((key + ": \"" + processLine(vl) + "\"\n").getBytes());
				}
				vl.setLength(0);
			}
		}
		fis.close();
	}
	

	private static String processLine(StringBuilder vl) {
		for (int i = 1; i < vl.length(); i++) {
			if (vl.charAt(i) == '“') {
				vl.setCharAt(i, '"');
			}
			if (vl.charAt(i) == '"' && vl.charAt(i - 1) != '\\') {
				vl.insert(i, '\\');
			} else if (vl.charAt(i) == '\'' && vl.charAt(i - 1) == '\\') {
				vl.deleteCharAt(i - 1);
			} else if (vl.charAt(i) == '?' && vl.charAt(i - 1) == '\\') {
				vl.deleteCharAt(i - 1);
			} else if (vl.charAt(i) == 't' && vl.charAt(i - 1) == '\\') {
				vl.deleteCharAt(i);
				vl.deleteCharAt(i - 1);
				vl.insert(i - 1, '\t');
			} else if (vl.charAt(i) == 'n' && vl.charAt(i - 1) == '\\') {
				vl.deleteCharAt(i);
				vl.deleteCharAt(i - 1);
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
