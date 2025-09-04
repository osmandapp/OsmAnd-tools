package net.osmand.render;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.zip.GZIPInputStream;

import org.apache.commons.io.FileUtils;
import org.jetbrains.annotations.NotNull;
import org.xmlpull.v1.XmlPullParser;

import net.osmand.PlatformUtil;
import net.osmand.util.Algorithms;

// Script launches from MainUtilities with CLI Arguments: generate-maplegend-svg
// Also you need to add to environment variables a path to all OsmAnd repositories. Like this:
// repo_dir=/Users/nnngrach/OsmAnd/

public class SvgMapLegendGenerator {

	public static final String LEGEND_COMPONENT_MAP_JS = "/web/main/src/components/docs/legend/componentMap.js";
	public static final String LEGEND_LEGEND_SECTIONS_JS = "/web/main/src/components/docs/legend/legendSections.js";
	static int defaultZoomLevel = 19; // Most of the icons on this zoom are visible
	static int canvasWidth = 300;
	static int canvasHeight = 40;
	static int defaultShieldSize = 40;
	static int defaultIconSize = 24;

	public static void main(String[] args) throws Exception {
		generate(System.getenv("repo_dir"), "default");
	}

	private static String getStylePath(String styleName, String repositoriesPath) {
		String[] allStylesNames = new String[]{"default", "desert", "LightRS", "mapnik", "nautical", "offroad",
				"osm-carto", "regions", "skimap", "snowmobile", "standalone-template", "Topo-map-assimilation", "topo",
				"Touring-view_(more-contrast-and-details)", "UniRS", "weather.addon"};
		if (Algorithms.isEmpty(styleName) || !Arrays.asList(allStylesNames).contains(styleName)) {
			styleName = "default";
		}
		return repositoriesPath + "/resources/rendering_styles/" + styleName + ".render.xml";
	}

	public static void generate(String repositoriesPath, String rendererStyle) throws Exception {
		System.out.println("\n\n\n");
		System.out.println("OK: SvgMapLegendGenerator - Start script");
		try {

			if (Algorithms.isEmpty(repositoriesPath)) {
				throw new Exception("ERROR: SvgMapLegendGenerator - 'repo_dir' env parameter is empty");
			}

			// 1 Read XML config file
			String configPath = repositoriesPath + "/resources/rendering_styles/map-legend/" + rendererStyle
					+ ".legend.xml";
			ArrayList<GroupDTO> configGroups = parseXmlConfig(configPath);
			if (!Algorithms.isEmpty(configGroups)) {
				System.out.println("OK: SvgMapLegendGenerator - Config file reading DONE");
			} else {
				throw new Exception("ERROR: SvgMapLegendGenerator - configGroups is empty");
			}

			// 2 Get styles for each icon.
			String path = getStylePath(rendererStyle, repositoriesPath);
			RenderingRulesStorage storage = RenderingRulesStorage.getTestStorageForStyle(path);
			for (GroupDTO group : configGroups) {
				if (group.spriteSheet) {
					continue;
				}
				for (IconDTO icon : group.icons) {
					Map<String, String> dayStyle = getAmenityIconStyle(icon.tag,
							icon.value, icon.tag2, icon.value2, false, icon.zoom, storage);
					Map<String, String> nightStyle = getAmenityIconStyle(icon.tag,
							icon.value, icon.tag2, icon.value2, false, icon.zoom, storage);
					if (!Algorithms.isEmpty(dayStyle) && !Algorithms.isEmpty(dayStyle)
							&& !Algorithms.isEmpty(dayStyle.get("iconName"))) {
						icon.iconName = dayStyle.get("iconName");
						icon.shieldNameDay = dayStyle.get("shieldName");
						icon.shieldNameNight = nightStyle.get("shieldName");
						icon.iconTargetFileName = icon.tag + "_" + icon.value;
						if (!Algorithms.isEmpty(icon.tag2)) {
							icon.iconTargetFileName += "_" + icon.tag2 + "_" + icon.value2;
						}
						icon.iconTargetFileName = icon.iconTargetFileName.replace(':', '_');
						icon.styleIconSize = Float.parseFloat(dayStyle.get("iconSize"));
					} else {
						throw new Exception(String.format(
								"ERROR: SvgMapLegendGenerator - style collecting invalid result for  '%s':'%s'  '%s':'%s'",
								icon.tag, icon.value, icon.tag2, icon.value2));
					}
				}
			}
			System.out.println("OK: SvgMapLegendGenerator - styles collecting DONE");

			// 3 Generate SVG files
			for (GroupDTO group : configGroups) {
				String resultIconsFolderPath = System.getenv("repo_dir") + "/web/main/static/img/legend/osmand/";
				if (group.spriteSheet) {
					//TODO  copy sprite file
					continue;
				}

				// Create directory is it needed. Clean files inside.
				String resultFolderPath = resultIconsFolderPath + group.folderName;
				try {
					Files.createDirectories(Paths.get(resultFolderPath));
					FileUtils.cleanDirectory(new File(resultFolderPath));
				} catch (Exception e) {
					throw new Exception(
							"ERROR: SvgMapLegendGenerator - failed to create or clean directory: " + resultFolderPath, e);
				}

				for (IconDTO icon : group.icons) {
					try {
						String contentDay = SvgGenerator.generate(icon.iconName, icon.iconSize, icon.shieldNameDay,
								icon.shieldSize, null, 0.f);
						SvgGenerator.createSvgFile(group, icon, contentDay, true, resultIconsFolderPath);
						String contentNight = SvgGenerator.generate(icon.iconName, icon.iconSize, icon.shieldNameNight,
								icon.shieldSize, null, 0.f);
						SvgGenerator.createSvgFile(group, icon, contentNight, false, resultIconsFolderPath);
					} catch (Exception e) {
						throw new Exception(String.format(
								"ERROR: SvgMapLegendGenerator - failed generate icon content for '%s' - '%s' ",
								group.groupName, icon.name), e);
					}
				}
			}
			System.out.println("OK: SvgMapLegendGenerator - svg files saving DONE");

			// 4 Generate React components with generated icons
			ReactComponentsGenerator.generate(configGroups);

			System.out.println("SUCCESS: SvgMapLegendGenerator - script finished successful!");
		} catch (Exception e) {
			throw new Exception("FATAL ERROR: SvgMapLegendGenerator - script failed with error:", e);
		}
	}

	public static Map<String, String> getAmenityIconStyle(String tag, String value, String tag2, String value2,
	                                                      boolean nightMode, int zoom, RenderingRulesStorage storage) {

		RenderingRuleSearchRequest searchRequest = new RenderingRuleSearchRequest(storage);

		searchRequest.setStringFilter(storage.PROPS.R_TAG, tag);
		searchRequest.setStringFilter(storage.PROPS.R_VALUE, value);

		if (!Algorithms.isEmpty(tag2) && !Algorithms.isEmpty(value2)) {
			searchRequest.setStringFilter(storage.PROPS.R_ADDITIONAL, tag2 + "=" + value2);
		}

		searchRequest.setBooleanFilter(storage.PROPS.R_NIGHT_MODE, nightMode);
		searchRequest.setIntFilter(storage.PROPS.R_MINZOOM, zoom);
		searchRequest.setIntFilter(storage.PROPS.R_MAXZOOM, zoom);
		for (RenderingRuleProperty customProp : storage.PROPS.getCustomRules()) {
			if (customProp.isBoolean()) {
				searchRequest.setBooleanFilter(customProp, false);
			} else {
				searchRequest.setStringFilter(customProp, "");
			}
		}

		searchRequest.search(RenderingRulesStorage.POINT_RULES);

		Map<String, String> result = new HashMap<>();
		result.put("iconName", searchRequest.getStringPropertyValue(searchRequest.ALL.R_ICON));
		result.put("shieldName", searchRequest.getStringPropertyValue(searchRequest.ALL.R_SHIELD));
		result.put("iconSize", String.valueOf(searchRequest.getFloatPropertyValue(searchRequest.ALL.R_ICON_VISIBLE_SIZE, -1)));
		// System.out.println("RendererRulesStorageAmenityFetcher result: " + result);
		return result;
	}

	private static class GroupDTO {
		String groupName = "";
		String folderName = "";
		boolean spriteSheet = false;
		ArrayList<IconDTO> icons = new ArrayList<>();
	}

	private static class IconDTO {
		public String iconTargetFileName;
		String name = null;
		String id = null;
		String tag = null;
		String value = null;
		String tag2 = null;
		String value2 = null;
		int iconSize = defaultIconSize; // optional value
		int shieldSize = defaultShieldSize; // optional value
		int zoom = defaultZoomLevel; // optional value

		String iconName = null;
		String shieldNameNight = null;
		String shieldNameDay = null;
		float styleIconSize = -1;
	}

	private static ArrayList<GroupDTO> parseXmlConfig(String filePath) throws Exception {
		try {
			File file = new File(filePath);
			XmlPullParser parser = PlatformUtil.newXMLPullParser();
			InputStream fis = new FileInputStream(file);
			if (file.getName().endsWith(".gz")) {
				fis = new GZIPInputStream(fis);
			}
			parser.setInput(fis, "UTF-8");
			int next;

			ArrayList<GroupDTO> resultGroups = new ArrayList<>();
			GroupDTO tempGroup = new GroupDTO();
			IconDTO tempIcon = new IconDTO();

			while ((next = parser.next()) != XmlPullParser.END_DOCUMENT) {
				if (next == XmlPullParser.END_TAG) {
					String name = parser.getName();
					if (name.equals("icon")) {
						tempGroup.icons.add(tempIcon);
						tempIcon = new IconDTO();
					} else if (name.equals("group")) {
						resultGroups.add(tempGroup);
						tempGroup = new GroupDTO();
					}
				} else if (next == XmlPullParser.START_TAG) {
					String name = parser.getName();
					if (name.equals("group")) {
						tempGroup.groupName = parser.getAttributeValue("", "name");
						String foldername = parser.getAttributeValue("", "foldername");
						if (foldername == null) {
							foldername = parser.getAttributeValue("", "sprite");
							if (foldername != null) {
								tempGroup.spriteSheet = true;
							}
						}
						tempGroup.folderName = foldername;

						if (Algorithms.isEmpty(tempGroup.groupName) || Algorithms.isEmpty(tempGroup.folderName)) {
							throw new Exception("ERROR: parseXmlConfig() - group fields invalid at map-legend.xml line "
									+ parser.getLineNumber());
						}
					} else if (name.equals("icon")) {
						tempIcon.name = parser.getAttributeValue("", "name");
						if (tempGroup.spriteSheet) {
							tempIcon.id = parser.getAttributeValue("", "id");
							if (Algorithms.isEmpty(tempIcon.name) || Algorithms.isEmpty(tempIcon.id)) {
								throw new Exception("ERROR: parseXmlConfig() - icon fields invalid at map-legend.xml line "
										+ parser.getLineNumber());
							}
						} else {
							tempIcon.tag = parser.getAttributeValue("", "tag");
							tempIcon.value = parser.getAttributeValue("", "value");
							tempIcon.tag2 = parser.getAttributeValue("", "tag2");
							tempIcon.value2 = parser.getAttributeValue("", "value2");

							String numberString = parser.getAttributeValue("", "icon_size");
							if (!Algorithms.isEmpty(numberString)) {
								tempIcon.iconSize = Integer.parseInt(numberString);
							}

							numberString = parser.getAttributeValue("", "shield_size");
							if (!Algorithms.isEmpty(numberString)) {
								tempIcon.shieldSize = Integer.parseInt(numberString);
							}

							numberString = parser.getAttributeValue("", "zoom");
							if (!Algorithms.isEmpty(numberString)) {
								tempIcon.zoom = Integer.parseInt(numberString);
							}

							if (Algorithms.isEmpty(tempIcon.name) || Algorithms.isEmpty(tempIcon.tag)
									|| Algorithms.isEmpty(tempIcon.value)) {
								throw new Exception("ERROR: parseXmlConfig() - icon fields invalid at map-legend.xml line "
										+ parser.getLineNumber());
							}
						}
					}
				}
			}
			return resultGroups;
		} catch (Exception e) {
			throw new Exception("ERROR: parseConfig() failed to parse file " + filePath, e);
		}
	}

	private static class SvgGenerator {

		public static String generate(String iconName, int iconSize, String shieldName, int shieldSize,
		                              String backgroundColor, float backgroundOpacity) throws Exception {

			String additionalIconHeaderTags = getSvgIconAdditionalHeaderTags(iconName);
			String content = String.format(
					"<svg width=\"%d\" height=\"%d\" viewBox=\"0 0 %d %d\" fill=\"none\" xmlns=\"http://www.w3.org/2000/svg\" %s>\n",
					canvasWidth, canvasHeight, canvasWidth, canvasHeight, additionalIconHeaderTags);

			if (!Algorithms.isEmpty(backgroundColor)) {
				content += geBackgroundRect(backgroundColor, backgroundOpacity);
			}
			if (!Algorithms.isEmpty(shieldName)) {
				content += getShield(shieldName, shieldSize);
			}
			if (!Algorithms.isEmpty(iconName)) {
				content += getIcon(iconName, iconSize);
			}
			content += "</svg>";
			return content;
		}

		private static String getIconPath(String iconName) throws Exception {
			String basePath = System.getenv("repo_dir") + "/resources/rendering_styles/style-icons/map-icons-svg/";
			String filePath = basePath + "mx_" + iconName + ".svg";

			File file = new File(filePath);
			if (file.exists()) {
				return filePath;
			}
			String coloredIconFilePath = basePath + "c_mx_" + iconName + ".svg";
			file = new File(coloredIconFilePath);
			if (file.exists()) {
				return coloredIconFilePath;
			} else {
				throw new Exception("File not found " + filePath);
			}
		}

		private static String geBackgroundRect(String color, float opacity) {
			return String.format("<rect width=\"%d\" height=\"%d\" fill=\"%s\" fill-opacity=\"%f\"/>\n", canvasWidth,
					canvasHeight, color, opacity);
		}

		private static String getShield(String shieldName, int shieldSize) throws Exception {
			String shieldsFolder = System.getenv("repo_dir") + "/resources/icons/svg/shields/";
			String filePath = shieldsFolder + "h_" + shieldName + ".svg";
			SvgDTO shieldSvg = parseSvg(filePath);
			return moveToCenterAndResize(shieldSvg, shieldSize);
		}

		private static String getIcon(String iconName, int iconSize) throws Exception {
			String filePath = getIconPath(iconName);
			SvgDTO iconSvg = parseSvg(filePath);
			// change clip_id from default "clip0", "clip1"... to "clip1000", "clip1001"...
			// for not overwriting shield's clip_id data
			iconSvg.content = iconSvg.content.replace("clip", "clip100");
			return moveToCenterAndResize(iconSvg, iconSize);
		}

		private static SvgDTO parseSvg(String filePath) throws Exception {
			try {
				SvgDTO parsedSvg = new SvgDTO();
				File file = new File(filePath);
				XmlPullParser parser = PlatformUtil.newXMLPullParser();
				InputStream fis = new FileInputStream(file);
				if (file.getName().endsWith(".gz")) {
					fis = new GZIPInputStream(fis);
				}
				parser.setInput(fis, "UTF-8");
				int next;
				while ((next = parser.next()) != XmlPullParser.END_DOCUMENT) {
					if (next == XmlPullParser.START_TAG) {
						String name = parser.getName();
						if (name.equals("svg")) {
							parsedSvg.width = Double.parseDouble(parser.getAttributeValue("", "width"));
							parsedSvg.height = Double.parseDouble(parser.getAttributeValue("", "height"));
							String viewBoxStr = parser.getAttributeValue(null, "viewBox");
							if (viewBoxStr != null) {
								String[] parts = viewBoxStr.split(" ");
								for (int i = 0; i < parts.length; i++) {
									parsedSvg.viewBox[i] = Double.parseDouble(parts[i]);
								}
							}
						}
					}
				}

				parsedSvg.content = getSvgInnerContent(filePath);
				return parsedSvg;
			} catch (Exception e) {
				throw new Exception("ERROR: parseSvg() failed to parse file " + filePath, e);
			}
		}

		private static String getSvgIconAdditionalHeaderTags(String iconName) throws Exception {
			String filePath = getIconPath(iconName);
			try {
				String firstTagStart = "<svg";
				String firstTagEnd = ">";
				String svgHeaderContent = Files.readString(Path.of(filePath));
				int trimIndexStart = svgHeaderContent.indexOf(firstTagStart) + firstTagStart.length();
				int trimIndexEnd = svgHeaderContent.indexOf(firstTagEnd, trimIndexStart);
				svgHeaderContent = svgHeaderContent.substring(trimIndexStart, trimIndexEnd);
				svgHeaderContent = svgHeaderContent.replace("\n", " ");
				svgHeaderContent = svgHeaderContent.replace("  ", " ");

				String filteredAdditionalTags;
				String[] parameters = svgHeaderContent.split("\" ");
				StringBuilder filteredAdditionalTagsBuilder = new StringBuilder();
				for (String parameter : parameters) {
					if (!parameter.contains("width=") && !parameter.contains("height=")
							&& !parameter.contains("viewBox=") && !parameter.contains("fill=")
							&& !parameter.contains("xmlns=")) {
						filteredAdditionalTagsBuilder.append(parameter).append("\" ");
					}
				}
				filteredAdditionalTags = filteredAdditionalTagsBuilder.toString();

				filteredAdditionalTags = filteredAdditionalTags.trim();
				if (filteredAdditionalTags.endsWith("\"\"")) {
					filteredAdditionalTags = filteredAdditionalTags.substring(0, filteredAdditionalTags.length() - 1);
				}
				return filteredAdditionalTags;
			} catch (Exception e) {
				throw new Exception("ERROR: getSvgIconAdditionalHeaderTags() failed to process file " + filePath, e);
			}
		}

		private static String getSvgInnerContent(String filePath) throws Exception {
			try {
				String firstTagStart = "<svg";
				String firstTagEnd = ">";
				String secondTag = "</svg>";
				String svgInnerContent = Files.readString(Path.of(filePath));
				int trimIndex = svgInnerContent.indexOf(firstTagStart);
				trimIndex = svgInnerContent.indexOf(firstTagEnd, trimIndex);
				svgInnerContent = svgInnerContent.substring(trimIndex + firstTagEnd.length());
				return svgInnerContent.replace(secondTag, "");
			} catch (Exception e) {
				throw new Exception("ERROR: getSvgInnerContent() failed to process file " + filePath, e);
			}
		}

		/**
		 * Using viewBox for scaling calculations instead of the width and height attributes
		 * ensures that the SVG content is scaled based on its intended visual representation. This approach
		 * preserves the aspect ratio and relative positioning within the SVG, making it adaptable and
		 * consistent across different container sizes, enhancing flexibility and scalability.
		 */
		private static String moveToCenterAndResize(SvgDTO svg, int newSize) {
			double originalMaxSize = Math.max(svg.viewBox[2], svg.viewBox[3]);
			double rescalingRatio = newSize / originalMaxSize;
			double xOffset = (double) canvasWidth / 2 - (double) newSize / 2;
			double yOffset = (double) canvasHeight / 2 - (double) newSize / 2;
			String resultContent = String.format(Locale.US, "%n<g transform=\"translate(%f, %f) scale(%f %f) \"> %n",
					xOffset, yOffset, rescalingRatio, rescalingRatio);
			resultContent += svg.content + "</g>%n%n";

			return resultContent;
		}

		public static void createSvgFile(GroupDTO group, IconDTO icon, String content, boolean isDay,
		                                 String resultIconsFolderPath) throws Exception {
			String postfix = isDay ? "_day.svg" : "_night.svg";
			String path = resultIconsFolderPath + group.folderName + "/" + group.folderName + "_" + icon.iconTargetFileName
					+ postfix;
			try {
				FileWriter writer = new FileWriter(path);
				writer.write(content);
				writer.close();
			} catch (Exception e) {
				throw new Exception("ERROR: createSvgFile() - failed to write file: " + path, e);
			}
		}

		private static class SvgDTO {
			String content = null;
			double width = -1;
			double height = -1;
			double[] viewBox = new double[4]; // [minX, minY, width, height]
		}
	}

	private static class ReactComponentsGenerator {
		public static void generate(ArrayList<GroupDTO> groups) throws Exception {
			Map<String, GroupDTO> components = new LinkedHashMap<>();
			for (GroupDTO group : groups) {
				StringBuilder content;
				if (group.spriteSheet) {
					content = createSpriteSheetGroupContent(group);
				} else {
					content = createGroupContent(group);
				}
				String componentName = "LegendItemAutogenerated" + Algorithms.capitalizeFirstLetter(group.folderName);
				String path = System.getenv("repo_dir")
						+ "/web/main/src/components/docs/autogenerated/" + componentName + ".js";
				try {
					FileWriter writer = new FileWriter(path);
					writer.write(content.toString());
					writer.close();
					components.put(componentName, group);
				} catch (Exception e) {
					throw new Exception("ERROR: ReactComponentsGenerator - file saving error for " + path, e);
				}
				createLegendAndComponentsFiles(components);
			}
		}

		private static void createLegendAndComponentsFiles(Map<String, GroupDTO> components) throws Exception {
			StringBuilder componentMapContent = new StringBuilder("""
					import React from 'react';
					
					// This code was automatically generated
					// with Java-tools SvgMapLegendGenerator
					
					export const componentMap = {
					""");
			StringBuilder legendSectionsContent = new StringBuilder("""
					/**
					 * This code was automatically generated
					 * with Java-tools SvgMapLegendGenerator
					 *
					 * Data for the CONTENT of legend sections.
					 * Headings are defined directly in the MD file for Docusaurus's Table of Contents.
					 */
					
					export const legendContentData = {
					""");
			for (Map.Entry<String, GroupDTO> component : components.entrySet()) {
				String componentName = component.getKey();
				componentMapContent
						.append("  %s: React.lazy(() => import('@site/src/components/docs/autogenerated/%s.js')),\n"
								.formatted(componentName, componentName));
				int height = 70 + (component.getValue().icons.size() + 2) / 3 * 111;
				String sectionId = component.getValue().folderName;
				legendSectionsContent
						.append("  '%s': { component: '%s', height: %d },\n"
								.formatted(sectionId, componentName, height));
			}
			componentMapContent.append("};");
			legendSectionsContent.append("};");
			String componentMapPath = System.getenv("repo_dir") + LEGEND_COMPONENT_MAP_JS;
			String legendSectionsPath = System.getenv("repo_dir") + LEGEND_LEGEND_SECTIONS_JS;
			try {
				FileWriter writer = new FileWriter(componentMapPath);
				writer.write(componentMapContent.toString());
				writer.close();
				writer = new FileWriter(legendSectionsPath);
				writer.write(legendSectionsContent.toString());
				writer.close();
			} catch (Exception e) {
				throw new Exception("ERROR: createLegendAndComponentsFiles - file saving error ", e);
			}
		}

		private static @NotNull StringBuilder createSpriteSheetGroupContent(GroupDTO group) {

			String svgPath = group.folderName;
			StringBuilder content = new StringBuilder("""
					import LegendItemWithProcessing from "../LegendItemWithProcessing";
					
					// This code was automatically generated
					// with Java-tools SvgMapLegendGenerator
					
					export default function Render() {
					  return LegendItemWithProcessing({
					    svgPath: '/img/map-legend/osmand-%s',
					    svgParts: {
					""".formatted(svgPath));
			for (IconDTO icon : group.icons) {
				content.append(String.format("      '%s' : '%s',\n", icon.id, icon.name));
			}
			content.append("""
					    }});
					}
					""");
			return content;
		}

		private static @NotNull StringBuilder createGroupContent(GroupDTO group) {
			StringBuilder content = new StringBuilder("""
					import LegendItem from "../LegendItem";
					
					// This code was automatically generated
					// with Java-tools SvgMapLegendGenerator
					
					export default function Render() {
					    return LegendItem({itemsMap: {
					""");
			for (IconDTO icon : group.icons) {
				String iconName = group.folderName + "_" + icon.iconTargetFileName;
				content.append(String.format("        '%s' : '%s/%s',\n", icon.name, group.folderName, iconName));
			}
			content.append("""
					    }});
					}
					""");
			return content;
		}
	}
}