package net.osmand.server.utilities;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.reflect.TypeToken;
import net.osmand.render.RenderingRule;
import net.osmand.render.RenderingRuleProperty;
import net.osmand.render.RenderingRuleSearchRequest;
import net.osmand.render.RenderingRulesStorage;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;
import org.xmlpull.v1.XmlPullParserException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static net.osmand.NativeJavaRendering.parseStorage;

/**
 * The StyleDataExtractor class is responsible for parsing XML files that define rendering styles.
 * It extracts relevant attributes and rules from the XML and organizes them into a structured format.
 */
public class StyleDataExtractor {
    
    private static final String STYLE_RULES_RESULT_JSON = "styleRulesResult.json";
    
    public static void main(String[] args) throws IOException, ParserConfigurationException, SAXException {
        parseStylesXml("../../../osmand/web/map/src/resources/mapStyles/styles.json", "../../../osmand/web/map/src/resources/mapStyles/attributes.json");
       // parsePoiStylesXml("../../../osmand/resources/poi/poi_types.xml", "../../../osmand/web/map/src/resources/generated/poi-types.json");
    }
    
    /**
     * Parses XML files for styles and attributes, and writes the results to 'styleRulesResult.json'.
     * For each style, it retrieves the associated rendering rules for the given attributes, collects all related rules,
     * and maps each rule's properties to their corresponding values.
     *
     * @param stylesPath     The path to the JSON file containing style names.
     * @param attributesPath The path to the JSON file containing attribute names.
     * @throws IOException If an error occurs during file reading, JSON parsing, or writing to the output file.
     */
    public static void parseStylesXml(String stylesPath, String attributesPath) throws IOException {
        if (stylesPath == null || attributesPath == null) {
            return;
        }
        List<String> styles = parseFile(stylesPath);

        Gson gson = new Gson();
        List<String> regularAttributes = new ArrayList<>();
        List<String> publicTransportAttributes = new ArrayList<>();
        
        try (FileReader reader = new FileReader(attributesPath)) {
            JsonElement jsonElement = gson.fromJson(reader, JsonElement.class);
            if (jsonElement != null) {
                if (jsonElement.isJsonObject()) {
                    com.google.gson.JsonObject jsonObj = jsonElement.getAsJsonObject();
                    if (jsonObj.has("regular")) {
                        Type listType = new TypeToken<List<String>>() {}.getType();
                        regularAttributes = gson.fromJson(jsonObj.get("regular"), listType);
                    }
                    if (jsonObj.has("publictransport")) {
                        Type listType = new TypeToken<List<String>>() {}.getType();
                        publicTransportAttributes = gson.fromJson(jsonObj.get("publictransport"), listType);
                    }
                }
            }
        } catch (Exception e) {
            throw new IOException("Failed to parse attributes JSON file at " + attributesPath, e);
        }
        
        if (styles.isEmpty() || (regularAttributes.isEmpty() && publicTransportAttributes.isEmpty())) {
            return;
        }
        
        Map<String, Object> result = new HashMap<>();
        for (String style : styles) {
            try {
                RenderingRulesStorage storage = parseStorage(style);
                List<String> attributesToParse = style.endsWith("publictransportroutes.addon.render.xml") 
                    ? publicTransportAttributes 
                    : regularAttributes;
                
                Map<String, List<Map<String, String>>> attributesRes = parseAttributes(storage, attributesToParse);
                
                if (!attributesRes.isEmpty()) {
                    result.put(style, attributesRes);
                }
            } catch (XmlPullParserException | IOException | SAXException e) {
                throw new RuntimeException("Error parsing storage", e);
            }
        }
        
        Gson gsonPretty = new GsonBuilder().setPrettyPrinting().disableHtmlEscaping().create();
        String jsonResult = gsonPretty.toJson(result);
        String outputFilePath = Paths.get(new File(stylesPath).getParent(), STYLE_RULES_RESULT_JSON).toString();
        Files.write(Paths.get(outputFilePath), jsonResult.getBytes());
    }
    
    /**
     * Reads a JSON file and returns a list of strings.
     * The JSON file should contain an array of strings.
     *
     * @param filePath The path to the JSON file.
     * @return A list of strings extracted from the JSON file.
     * @throws IOException If an error occurs during file reading or JSON parsing.
     */
    public static List<String> parseFile(String filePath) throws IOException {
        Gson gson = new Gson();
        Type listType = new TypeToken<List<String>>() {}.getType();
        List<String> styles;
        
        try (FileReader reader = new FileReader(filePath)) {
            styles = gson.fromJson(reader, listType);
        } catch (Exception e) {
            throw new IOException("Failed to parse JSON file at " + filePath, e);
        }
        return styles;
    }
    
    /**
     * Extracts and organizes attribute data for a given style from the rendering storage.
     *
     * @param storage    The RenderingRulesStorage object that contains the rendering rules.
     * @param attributes List of attribute names to be extracted.
     * @return A map where each key is an attribute name and the value is a list of maps.
     * Each map in the list represents a rendering rule's properties and their corresponding values.
     */
    private static Map<String, List<Map<String, String>>> parseAttributes(RenderingRulesStorage storage, List<String> attributes) {
        return attributes.stream()
                .map(attribute -> new AbstractMap.SimpleEntry<>(attribute, parseRules(storage, attribute)))
                .filter(entry -> entry.getValue() != null && !entry.getValue().isEmpty())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
    
    /**
     * Processes the rendering rules for a specific attribute from the provided storage.
     *
     * @param storage   The RenderingRulesStorage object that contains the rendering rules.
     * @param attribute The attribute for which the rules are to be processed.
     * @return A list of maps, where each map represents a rendering rule's properties and their corresponding values.
     * Returns an empty list if no rule is associated with the attribute.
     */
    private static List<Map<String, String>> parseRules(RenderingRulesStorage storage, String attribute) {
        RenderingRule rule = storage.getRenderingAttributeRule(attribute);
        if (rule != null) {
            List<RenderingRule> allRules = getRules(rule, new ArrayList<>());
            // Define preferred order for keys: attrStringValue first, then attrColorValue, then tag, value, etc.
            List<String> preferredOrder = Arrays.asList("attrStringValue", "attrColorValue", "tag", "value", "additional");
            
            List<Map<String, String>> rulesList = allRules.stream()
                    .map(renderingRule -> {
                        RenderingRuleSearchRequest searchRequest = new RenderingRuleSearchRequest(storage);
                        searchRequest.loadOutputProperties(renderingRule, true);
                        Map<String, String> ruleMap = Arrays.stream(renderingRule.getProperties())
                                .map(prop -> new AbstractMap.SimpleEntry<>(prop.getAttrName(), getProperty(prop, prop.getAttrName(), searchRequest, renderingRule)))
                                .filter(entry -> entry.getValue() != null)
                                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (v1, v2) -> v1, LinkedHashMap::new));
                        
                        // Reorder keys: put preferred keys first, then others in alphabetical order
                        LinkedHashMap<String, String> orderedMap = new LinkedHashMap<>();
                        // Add preferred keys in order
                        for (String key : preferredOrder) {
                            if (ruleMap.containsKey(key)) {
                                orderedMap.put(key, ruleMap.get(key));
                            }
                        }
                        // Add remaining keys in alphabetical order
                        ruleMap.entrySet().stream()
                                .filter(e -> !preferredOrder.contains(e.getKey()))
                                .sorted(Map.Entry.comparingByKey())
                                .forEach(e -> orderedMap.put(e.getKey(), e.getValue()));
                        
                        return orderedMap;
                    })
                    .filter(map -> !map.isEmpty())
                    .collect(Collectors.toList());
            
            // Remove duplicates: keep first occurrence, remove rest
            // Compare all fields except attrColorValue to identify duplicates
            // (same rule with different colors should be considered duplicate)
            Set<String> seen = new HashSet<>();
            List<Map<String, String>> uniqueRules = new ArrayList<>();
            
            for (Map<String, String> ruleMap : rulesList) {
                // Create comparison key from all fields except attrColorValue
                Map<String, String> comparisonMap = new TreeMap<>(ruleMap);
                comparisonMap.remove("attrColorValue"); // Exclude color from comparison
                
                String normalizedKey = comparisonMap.entrySet().stream()
                        .map(e -> e.getKey() + "=" + e.getValue())
                        .collect(Collectors.joining("|"));
                
                if (!seen.contains(normalizedKey)) {
                    seen.add(normalizedKey);
                    uniqueRules.add(new LinkedHashMap<>(ruleMap)); // Keep original order
                }
            }
            
            return uniqueRules;
        }
        return Collections.emptyList();
    }
    
    /**
     * Extracts the value of a given property from a rendering rule.
     *
     * @param prop          The rendering rule property.
     * @param name          The name of the property.
     * @param searchRequest The search request object for the rendering rule.
     * @param renderingRule The rendering rule from which the property value is extracted.
     * @return The value of the property as a String, or null if not found.
     */
    private static String getProperty(RenderingRuleProperty prop, String name, RenderingRuleSearchRequest searchRequest, RenderingRule renderingRule) {
        String value = null;
        if (prop.isString()) {
            value = searchRequest.getStringPropertyValue(prop);
            if (value == null) {
                value = renderingRule.getStringPropertyValue(name);
            }
        } else if (prop.isFloat()) {
            float f = searchRequest.getFloatPropertyValue(prop);
            value = f != 0 ? String.valueOf(f) : null;
            if (value == null) {
                f = renderingRule.getFloatPropertyValue(name);
                value = f != 0 ? String.valueOf(f) : null;
            }
        } else if (prop.isColor()) {
            value = searchRequest.getColorStringPropertyValue(prop);
            if (value == null) {
                value = renderingRule.getColorPropertyValue(name);
            }
        } else if (prop.isIntParse()) {
            int i = searchRequest.getIntPropertyValue(prop);
            value = i != -1 ? String.valueOf(i) : null;
            if (value == null) {
                i = renderingRule.getIntPropertyValue(name);
                value = i != -1 ? String.valueOf(i) : null;
            }
        }
        return value;
    }
    
    /**
     * Recursively collects all rendering rules associated with a given rule.
     * The method traverses the rule hierarchy, including the provided rule, its 'if-else' children, and 'if' children.
     *
     * @param rule     The initial rendering rule from which to start collecting.
     * @param allRules A list that accumulates all discovered rendering rules.
     * @return A list of all rendering rules related to the initial rule.
     */
    private static List<RenderingRule> getRules(RenderingRule rule, List<RenderingRule> allRules) {
        allRules.add(rule);
        Stream.concat(rule.getIfElseChildren().stream(), rule.getIfChildren().stream())
                .forEach(r -> getRules(r, allRules));
        return allRules;
    }

    public static void parsePoiStylesXml(String xmlPath, String jsonPath) throws IOException, SAXException, ParserConfigurationException {
        File xmlFile = new File(xmlPath);
        if (!xmlFile.exists()) {
            throw new IOException("File not found: " + xmlPath);
        }
        List<Map<String, String>> poiTypes = new ArrayList<>();
        SAXParserFactory factory = SAXParserFactory.newInstance();
        SAXParser saxParser = factory.newSAXParser();

        DefaultHandler handler = new DefaultHandler() {
            private Map<String, String> currentPoiType;

            @Override
            public void startElement(String uri, String localName, String qName, Attributes attributes) {
                if ("poi_type".equals(qName)) {
                    currentPoiType = new LinkedHashMap<>();

                    String name = attributes.getValue("name");
                    String tag = attributes.getValue("tag");
                    String value = attributes.getValue("value");

                    if (name != null) currentPoiType.put("name", name);
                    if (tag != null) currentPoiType.put("tag", tag);
                    if (value != null) currentPoiType.put("value", value);
                }
            }

            @Override
            public void endElement(String uri, String localName, String qName) {
                if ("poi_type".equals(qName) && currentPoiType != null) {
                    poiTypes.add(currentPoiType);
                    currentPoiType = null;
                }
            }
        };

        saxParser.parse(xmlFile, handler);

        Gson gson = new GsonBuilder().setPrettyPrinting().disableHtmlEscaping().create();
        String jsonResult = gson.toJson(poiTypes);
        Files.write(Paths.get(jsonPath), jsonResult.getBytes());
    }
}
