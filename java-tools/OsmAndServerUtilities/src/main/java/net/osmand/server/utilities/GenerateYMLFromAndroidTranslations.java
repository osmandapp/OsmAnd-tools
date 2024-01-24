package net.osmand.server.utilities;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.google.gson.*;
import com.google.gson.stream.JsonReader;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xmlpull.v1.XmlPullParser;
import org.xmlpull.v1.XmlPullParserException;

import net.osmand.PlatformUtil;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

public class GenerateYMLFromAndroidTranslations {
	
	private static final String DEFAULT_LOCALE = "en";

	public static void main(String[] args) {
		// convertAndroidTranslationsToYml("../../../android/OsmAnd/res/");
		//convertIosTranslationsToYml("../../../ios/Resources/Localizations");
		convertAndroidTranslationsToJSON("../../../osmand/web/map/src/resources/translations", "../../../osmand/android/OsmAnd/res");
	}
	
	public static void convertAndroidTranslationsToYml(String path) throws XmlPullParserException, IOException {
		File fs = new File(path);
		File outDir = new File("yml-translations");
		outDir.mkdir();
		for (File f : Objects.requireNonNull(fs.listFiles())) {
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
    
    /**
     * Converts and updates Android XML translations to JSON format for web use.
     * It syncs English translations and then updates other languages based on these changes.
     *
     * @param webPath The directory path for saving web translations.
     * @param andPath The source directory path for Android XML translations.
     */
    public static void convertAndroidTranslationsToJSON(String webPath, String andPath) {
	    Map<String, JsonObject> webTranslations = getTranslationsFromJSON(webPath);
	    Map<String, JsonObject> andTranslations = getTranslationsFromXml(andPath);
	    
	    JsonObject enWebTranslations = webTranslations.getOrDefault(DEFAULT_LOCALE, new JsonObject());
	    JsonObject enAndTranslations = andTranslations.getOrDefault(DEFAULT_LOCALE, new JsonObject());
	    
	    // Update English translations in web from Android
	    enAndTranslations.entrySet().forEach(entry -> {
		    String key = entry.getKey();
		    JsonElement value = entry.getValue();
		    if ((key.contains("rendering_attr_")
				    || key.contains("routeInfo_")
				    || key.startsWith("lang_")
				    || key.startsWith("poi_"))
				    && (!enWebTranslations.has(key) || !enWebTranslations.get(key).equals(value))) {
			    enWebTranslations.add(key, value);
		    }
	    });
	    webTranslations.put(DEFAULT_LOCALE, enWebTranslations);
	    
	    // Update or create other languages based on updated English translations
	    andTranslations.forEach((lang, andLangTranslations) -> {
		    JsonObject webLangTranslations = webTranslations.getOrDefault(lang, new JsonObject());
		    enWebTranslations.keySet().forEach(key -> {
			    if (andLangTranslations.has(key)) {
				    JsonElement newValue = andLangTranslations.get(key);
				    if (!webLangTranslations.has(key) || !webLangTranslations.get(key).equals(newValue)) {
					    webLangTranslations.add(key, newValue);
				    }
			    }
		    });
		    webTranslations.put(lang, webLangTranslations);
	    });
	    
	    saveTranslationsToFile(webTranslations, webPath);
    }
    
    /**
     * Saves translated JSON objects into corresponding language directories as 'translation.json'.
     * Ensures the target directories are created if they don't exist and sorts the languages alphabetically.
     *
     * @param translations The map containing language codes and their corresponding translations.
     * @param path         The target directory path to save the translations.
     */
    private static void saveTranslationsToFile(Map<String, JsonObject> translations, String path) {
        File webDir = new File(path);
        if (!webDir.mkdirs() && !webDir.isDirectory()) {
            throw new IllegalStateException("Failed to create directory at path: " + path);
        }
        
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
	    List<String> languages = new ArrayList<>();
        translations.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(entry -> {
                    String lang = entry.getKey();
	                languages.add(lang);
                    JsonObject langTranslations = entry.getValue();
                    File langDir = new File(webDir, lang);
                    if (!langDir.mkdirs() && !langDir.isDirectory()) {
                        throw new IllegalStateException("Failed to create language directory: " + langDir.getPath());
                    }
                    File translationFile = new File(langDir, "translation.json");
                    
                    try (FileWriter writer = new FileWriter(translationFile)) {
                        gson.toJson(langTranslations, writer);
                    } catch (IOException e) {
                        throw new RuntimeException("Error writing to file: " + translationFile.getPath(), e);
                    }
                });
	    
	    File langListFile = new File(webDir, "supportedLanguages.json");
	    try (FileWriter writer = new FileWriter(langListFile)) {
		    gson.toJson(languages, writer);
	    } catch (IOException e) {
		    throw new RuntimeException("Error writing to file: " + langListFile.getPath(), e);
	    }
    }
    
    /**
     * Retrieves JSON translations from a specified directory, organizing them by language.
     * Skips directories that don't contain a 'translation.json' file.
     *
     * @param path The path to the directory containing the translation files.
     * @return A map of language codes to their corresponding JSON translations.
     */
    private static Map<String, JsonObject> getTranslationsFromJSON(String path) {
        File webDir = new File(path);
        
        if (!webDir.exists() || !webDir.isDirectory()) {
            return new HashMap<>();
        }
        
        File[] languageDirectories = webDir.listFiles();
        if (languageDirectories == null) {
            throw new RuntimeException("Failed to list files in directory: " + path);
        }
        
        return Arrays.stream(languageDirectories)
                .filter(File::isDirectory)
                .collect(Collectors.toMap(
                        File::getName,
                        langDir -> {
                            File translationFile = new File(langDir, "translation.json");
                            if (translationFile.exists()) {
                                return readJsonFile(translationFile);
                            } else {
                                return new JsonObject();
                            }
                        }
                ));
    }
    
    /**
     * Extracts XML translations from a specified directory, converting them into JSON format.
     * Organizes translations by language code derived from directory names.
     *
     * @param path The path to the directory containing XML translation files.
     * @return A map of language codes to their corresponding JSON translations.
     */
    private static Map<String, JsonObject> getTranslationsFromXml(String path) {
        File baseDir = new File(path);
        Map<String, JsonObject> translations = new HashMap<>();
        
        if (baseDir.exists() && baseDir.isDirectory()) {
            File[] languageDirectories = baseDir.listFiles();
            if (languageDirectories == null) {
                throw new RuntimeException("Failed to list files in directory: " + path);
            }
            
            Arrays.stream(languageDirectories)
                    .filter(File::isDirectory)
                    .filter(langDir -> langDir.getName().startsWith("values"))
                    .forEach(langDir -> {
                        String langCode = langDir.getName().substring("values".length());
                        langCode = langCode.startsWith("-") ? langCode.substring(1) : DEFAULT_LOCALE;
                        
                        JsonObject translation = new JsonObject();
                        File[] files = {new File(langDir, "strings.xml"), new File(langDir, "phrases.xml")};
                        
                        Stream.of(files)
                                .filter(File::exists)
                                .forEach(file -> {
                                    Map<String, String> fileTranslations = parseXml(file);
                                    fileTranslations.forEach(translation::addProperty);
                                });
                        
                        translations.put(langCode, translation);
                    });
        }
        
        return translations;
    }
    
    
    /**
     * Reads a JSON file and converts it to a JsonObject.
     *
     * @param file The file to read from.
     * @return A JsonObject representation of the file content.
     */
    private static JsonObject readJsonFile(File file) {
        JsonParser parser = new JsonParser();
        try (JsonReader reader = new JsonReader(new FileReader(file))) {
            return parser.parse(reader).getAsJsonObject();
        } catch (Exception e) {
            throw new RuntimeException("Error reading JSON from file: " + file.getAbsolutePath(), e);
        }
    }
    
    
    /**
     * Parses an XML file and extracts elements into a map.
     * The map's keys are the 'name' attributes of the 'string' elements, and the values are their text contents.
     *
     * @param file The XML file to parse.
     * @return A map of string names and their corresponding values.
     */
    private static Map<String, String> parseXml(File file) {
        Map<String, String> translations = new HashMap<>();
        try {
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newDefaultInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            Document doc = dBuilder.parse(file);
            doc.getDocumentElement().normalize();
            
            NodeList nodeList = doc.getElementsByTagName("string");
            IntStream.range(0, nodeList.getLength()).mapToObj(nodeList::item)
                    .filter(node -> node.getNodeType() == Node.ELEMENT_NODE)
                    .forEach(node -> {
                        Element element = (Element) node;
                        translations.put(element.getAttribute("name"), element.getTextContent());
                    });
        } catch (Exception e) {
            throw new RuntimeException("Error parsing XML file: " + file.getAbsolutePath(), e);
        }
        return translations;
    }
	
	public static void convertIosTranslationsToYml(String path) throws XmlPullParserException, IOException {
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
				if (oline.endsWith(";") && oline.substring(0, oline.length() - 1).trim().endsWith("\"")) {
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
			if (vl.charAt(i) == '”') {
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
