package net.osmand.server.utilities;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.Map.Entry;
import java.util.function.Predicate;
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

public class GenerateWebTranslations {
	
	private static final String DEFAULT_LOCALE = "en";

	public static void main(String[] args) throws IOException {
		generateTranslations("../../../web/map/src/resources/translations/gen-translations-config.json", "../../../");
		
	}
	
	/**
     * Converts and updates Android XML translations to JSON format for web use.
     * It syncs English translations and then updates other languages based on these changes.
     *
     * @param webPath The directory path for saving web translations.
     * @param andPath The source directory path for Android XML translations.
	 * @throws IOException 
     */
    public static void generateTranslations(String configPath, String rootPath) throws IOException {
    	File configFolder = new File(configPath).getParentFile();
    	JsonObject config = readJsonFile(new File(configPath));
		String[] lst = {"android", "ios"}; 
		for(String keyConfig : lst) {
			if (!config.has(keyConfig)) {
				continue;
			}
			JsonObject osConfig = config.get(keyConfig).getAsJsonObject();
			List<Predicate<String>> filters;
			if (osConfig.has("filters")) {
				filters = new ArrayList<>();
				JsonArray flts = osConfig.getAsJsonArray("filters");
				for (int i = 0; i < flts.size(); i++) {
					JsonObject fl = flts.get(i).getAsJsonObject();
					if (fl.get("type").getAsString().equals("contains")) {
						filters.add(s -> s.contains(fl.get("filter").getAsString()));
					} else if (fl.get("type").getAsString().equals("startsWith")) {
						filters.add(s -> s.startsWith(fl.get("filter").getAsString()));
					} else {
						throw new UnsupportedOperationException(fl + "");
					}
				}
			} else {
				filters = null;
			}
			String defaultJsonPath = osConfig.get("default_translate_json").getAsString();
			String translateJsonPatternPath = osConfig.get("translate_json_pattern").getAsString();
			Set<String> languages = new TreeSet<String>();
			JsonArray langs = osConfig.get("languages").getAsJsonArray();
			for (int i = 0; i < langs.size(); i++) {
				languages.add(langs.get(i).getAsString());
			}

			Map<String, JsonObject> osTranslations;
			if (keyConfig.equals("android")) {
				osTranslations = getTranslationsFromAndroidXml(new File(rootPath, "android/OsmAnd/res"), languages);
			} else if (keyConfig.equals("ios")) {
				osTranslations = getTranslationsFromiOS(new File(rootPath, "ios/Resources/Localizations/"), languages);
			} else {
				throw new UnsupportedOperationException();
			}
			languages.remove("*");
			Map<String, JsonObject> webTranslations = getTranslationsFromJSON(configFolder, osTranslations.keySet(),
					defaultJsonPath, translateJsonPatternPath);

			JsonObject enWebTranslations = webTranslations.getOrDefault(DEFAULT_LOCALE, new JsonObject());
			JsonObject enAndTranslations = osTranslations.getOrDefault(DEFAULT_LOCALE, new JsonObject());

			// Update English translations in web from Android
			enAndTranslations.entrySet().forEach(entry -> {
				String key = entry.getKey();
				JsonElement value = entry.getValue();
				boolean include = filters == null || enWebTranslations.has(key);
				if (!include) {
					for (Predicate<String> p : filters) {
						if (p.test(key)) {
							include = true;
							break;
						}
					}
				}
				if (include && (!enWebTranslations.has(key) || !enWebTranslations.get(key).equals(value))) {
					enWebTranslations.add(key, value);
				} else if (!include) {
					enWebTranslations.remove(key);
				}
			});
			webTranslations.put(DEFAULT_LOCALE, enWebTranslations);

			// Update or create other languages based on updated English translations
			osTranslations.forEach((lang, andLangTranslations) -> {
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
			File langListFile = null;
			if(osConfig.has("supportedLanguagesFile")) {
				String supportedLanguagesFile = osConfig.get("supportedLanguagesFile").getAsString();
				langListFile = new File(configFolder, supportedLanguagesFile);	
			}
			saveTranslationsToFile(webTranslations, configFolder, 
					defaultJsonPath, translateJsonPatternPath, langListFile);
			 
		}
    }
    
    
    /**
     * Saves translated JSON objects into corresponding language directories as 'translation.json'.
     * Ensures the target directories are created if they don't exist and sorts the languages alphabetically.
     *
     * @param translations The map containing language codes and their corresponding translations.
     * @param translateJsonPatternPath 
     * @param defaultJsonPath 
     * @param configFolder         The target directory path to save the translations.
     */
	private static void saveTranslationsToFile(Map<String, JsonObject> translations, File configDir, 
			String defaultJsonPath, String translateJsonPatternPath, File langListFile) {
		if (!configDir.mkdirs() && !configDir.isDirectory()) {
			throw new IllegalStateException("Failed to create directory at path: " + configDir.getAbsolutePath());
		}

		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		List<String> languages = new ArrayList<>();
		translations.entrySet().stream().sorted(Map.Entry.comparingByKey()).forEach(entry -> {
			String lang = entry.getKey();
			languages.add(lang);
			JsonObject langTranslations = entry.getValue();
			File translationFile;
			if (lang.equals(DEFAULT_LOCALE)) {
				translationFile = new File(configDir, defaultJsonPath);
			} else {
				translationFile = new File(configDir, String.format(translateJsonPatternPath, lang));
			}
			File langDir = translationFile.getParentFile();
			if (!langDir.mkdirs() && !langDir.isDirectory()) {
				throw new IllegalStateException("Failed to create language directory: " + langDir.getPath());
			}


			try (FileWriter writer = new FileWriter(translationFile)) {
				gson.toJson(langTranslations, writer);
			} catch (IOException e) {
				throw new RuntimeException("Error writing to file: " + translationFile.getPath(), e);
			}
		});
		if (langListFile != null) {
			try (FileWriter writer = new FileWriter(langListFile)) {
				gson.toJson(languages, writer);
			} catch (IOException e) {
				throw new RuntimeException("Error writing to file: " + langListFile.getPath(), e);
			}
		}
	}
    
    /**
     * Retrieves JSON translations from a specified directory, organizing them by language.
     * Skips directories that don't contain a 'translation.json' file.
     * @param langs 
     * @param translateJsonPatternPath 
     * @param defaultJsonPath 
     *
     * @param path The path to the directory containing the translation files.
     * @return A map of language codes to their corresponding JSON translations.
     */
    private static Map<String, JsonObject> getTranslationsFromJSON(File configDir, 
    		Set<String> langs, String defaultJsonPath, String translateJsonPatternPath) {
        if (!configDir.exists() || !configDir.isDirectory()) {
            return new HashMap<>();
        }
        
        File[] languageDirectories = configDir.listFiles();
        if (languageDirectories == null) {
            throw new RuntimeException("Failed to list files in directory: " + configDir.getAbsolutePath());
        }
        Map<String, JsonObject> mp = new LinkedHashMap<String, JsonObject>();
		for (String lang : langs) {
			File translationFile;
			if (lang.equals(DEFAULT_LOCALE)) {
				translationFile = new File(configDir, defaultJsonPath);
			} else {
				translationFile = new File(configDir, String.format(translateJsonPatternPath, lang));
			}
			mp.put(lang, translationFile.exists() ? readJsonFile(translationFile) : new JsonObject());
		}
        return mp;
    }
    
    /**
     * Extracts XML translations from a specified directory, converting them into JSON format.
     * Organizes translations by language code derived from directory names.
     *
     * @param path The path to the directory containing XML translation files.
     * @param languages 
     * @return A map of language codes to their corresponding JSON translations.
     */
    private static Map<String, JsonObject> getTranslationsFromAndroidXml(File baseDir, Set<String> languages) {
        Map<String, JsonObject> translations = new HashMap<>();
        
        if (baseDir.exists() && baseDir.isDirectory()) {
            File[] languageDirectories = baseDir.listFiles();
            if (languageDirectories == null) {
                throw new RuntimeException("Failed to list files in directory: " + baseDir.getAbsolutePath());
            }
            
            Arrays.stream(languageDirectories)
                    .filter(File::isDirectory)
                    .filter(langDir -> langDir.getName().startsWith("values"))
                    .filter(langDir -> new File(langDir, "strings.xml").exists())
                    .forEach(langDir -> {
                        String langCode = langDir.getName().substring("values".length());
                        
                        langCode = langCode.startsWith("-") ? langCode.substring(1) : DEFAULT_LOCALE;
						if (languages.contains("*")) {
							languages.add(langCode);
						} else if (!languages.contains(langCode) && !DEFAULT_LOCALE.equals(langCode)) {
							return;
						}
                        
                        JsonObject translation = new JsonObject();
						File[] files = { new File(langDir, "strings.xml"), new File(langDir, "phrases.xml") };
                        
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
	
	public static Map<String, JsonObject> getTranslationsFromiOS(File fs, Set<String> languages) throws IOException {
		Map<String, JsonObject> osTranslations = new LinkedHashMap<String, JsonObject>();
		for (File f : fs.listFiles()) {
			File str = new File(f, "Localizable.strings");
			if (str.exists() && f.getName().endsWith(".lproj")) {
				String langCode = f.getName().substring(0, f.getName().indexOf('.'));
				if (languages.contains("*")) {
					languages.add(langCode);
				} else if (!languages.contains(langCode) && !DEFAULT_LOCALE.equals(langCode)) {
					continue;
				}
				String loc = f.getName().substring(0, f.getName().indexOf('.'));
				if (loc.equals("en")) {
					loc = "";
				} else {
					loc = "-" + loc;
				}
				Map<String, String> dict = parseIOSProj(str);
				JsonObject translation = new JsonObject();
				Iterator<Entry<String, String>> it = dict.entrySet().iterator();
				while (it.hasNext()) {
					Entry<String, String> e = it.next();
					translation.addProperty(e.getKey(), e.getValue());
				}
				osTranslations.put(langCode, translation);
			}
		}
		return osTranslations;
	}
	

	private static Map<String, String> parseIOSProj(File f) throws IOException {
		Map<String, String> translations = new TreeMap<>(); 
		BufferedReader br = new BufferedReader(new FileReader(f, StandardCharsets.UTF_8));
		try {
			String line = "";
			String oline = "";
			int ln = 0;
			while ((line = br.readLine()) != null) {
				oline += line.trim();
				ln++;
				if(oline.startsWith("//")) {
					continue;
				} else if (oline.endsWith(";") && oline.substring(0, oline.length() - 1).trim().endsWith("\"")) {
					if (!oline.equals(";")) {
						try {
							int eq = oline.indexOf('=');
							String keyRaw = oline.substring(0, eq);
							String valueRaw = oline.substring(eq + 1);
							String key = keyRaw.substring(keyRaw.indexOf('\"') + 1, keyRaw.lastIndexOf('\"'));
							String value = valueRaw.substring(valueRaw.indexOf('\"') + 1, valueRaw.lastIndexOf('\"'));
							if (!translations.containsKey(key)) {
								translations.put(key, value);
							}
						} catch (RuntimeException e) {
							throw new IllegalArgumentException(String.format("Parsing line '%s' of %s:%d crashed.",
									oline, f.getParentFile().getName() + "/" + f.getName(), ln), e);
						}
					}
					oline = "";
				} else {
					oline += "\n";
				}
			}
			return translations;
		} finally {
			br.close();
		}
	}
	


	protected static Map<String, String> parseAndroidXml(File f) throws XmlPullParserException, IOException {
		Map<String, String> mp = new LinkedHashMap<>();
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
					mp.put(key, vl.toString());
				}
				vl.setLength(0);
			}
		}
		fis.close();
		return mp;
	}
	

	protected static String processLine(StringBuilder vl) {
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
