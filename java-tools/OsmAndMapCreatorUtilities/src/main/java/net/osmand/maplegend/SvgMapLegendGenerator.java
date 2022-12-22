package net.osmand.maplegend;
import net.osmand.PlatformUtil;
import net.osmand.render.RendererRulesStorageAmenityFetcher;
import net.osmand.util.Algorithms;
import org.xmlpull.v1.XmlPullParser;
import org.xmlpull.v1.XmlPullParserException;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Map;
import java.util.zip.GZIPInputStream;

public class SvgMapLegendGenerator {

    static String configPath = "/Users/nnngrach/Documents/Projects/Coding/OsmAnd/tools/java-tools/OsmAndMapCreatorUtilities/src/main/java/net/osmand/maplegend/config.xml";
    static String resultsFolderPath = "/Users/nnngrach/Documents/Projects/Coding/OsmAnd/web/main/static/img/legend/osmand/";

    static String shieldsFolder = "/Users/nnngrach/Documents/Projects/Coding/OsmAnd/resources/icons/svg/shields/";
    static String iconsFolder = "/Users/nnngrach/Documents/Projects/Coding/OsmAnd/resources/rendering_styles/style-icons/poi-icons-svg/";
    static int canvasWidth = 300;
    static int canvasHeight = 35;
    static double shieldSize = 20;
    static double iconSize = 12;

    public static void main(String[] args) throws Exception{
        // 1 Read Config file
        System.out.println("SvgMapLegendGenerator: start script");
        ArrayList<GroupDTO> configGroups = parseConfig(configPath);
        System.out.println("SvgMapLegendGenerator: config reading DONE");

        // 2 Get styles for each icon.
        String rendererName = (args.length > 1 && !Algorithms.isEmpty(args[1])) ? args[1] : "default";
        for (GroupDTO group : configGroups) {
            for (IconDTO icon : group.icons) {
                String[] argumentsDay = {rendererName, icon.tag, icon.value, icon.tag2, icon.value2, "false"};
                String[] argumentsNight = {rendererName, icon.tag, icon.value, icon.tag2, icon.value2, "true"};
                Map<String, String> dayStyle = RendererRulesStorageAmenityFetcher.main(argumentsDay);
                Map<String, String> nightStyle = RendererRulesStorageAmenityFetcher.main(argumentsNight);

                icon.iconSize = Float.valueOf(dayStyle.get("iconSize"));
                icon.iconName = dayStyle.get("iconName");
                icon.shieldNameDay = dayStyle.get("shieldName");
                icon.shieldNameNight = nightStyle.get("shieldName");
            }
        }
        System.out.println("SvgMapLegendGenerator: styles collecting DONE");

        // 3 Generate SVG files
        for (GroupDTO group : configGroups) {
            Files.createDirectories(Paths.get(resultsFolderPath + group.folderName));
            for (IconDTO icon : group.icons) {

                if (Algorithms.isEmpty(icon.iconName) || Algorithms.isEmpty(icon.shieldNameDay) || Algorithms.isEmpty(icon.shieldNameNight)) {
                    System.out.println("SvgMapLegendGenerator: Error - invalid parameters for icon " + icon.name);
                    continue;
                }

                String contentDay = SvgGenerator.generate(icon.iconName, icon.shieldNameDay, null, 0.f);
                createSvgFile(group, icon, contentDay, true);
                String contentNight = SvgGenerator.generate(icon.iconName, icon.shieldNameNight, null, 0.f);
                createSvgFile(group, icon, contentNight, false);
            }
        }
        System.out.println("SvgMapLegendGenerator: svg files saving DONE");

        // 4 Generate JS code fragments to copy
        HtmlGenerator.generate(configGroups);
    }

    private static boolean createSvgFile(GroupDTO group, IconDTO icon, String content, boolean isDay) {
        try {
            String postfix = isDay ? "_day.svg" : "_night.svg";
            FileWriter writer = new FileWriter(resultsFolderPath + group.folderName + "/" + group.folderName + "_" + icon.iconName + postfix);
            writer.write(content);
            writer.close();
        } catch (IOException e) {
            System.out.println("SvgMapLegendGenerator: svg file saving error for " + icon.tag + "=" + icon.value);
            e.printStackTrace();
            return false;
        }
        return true;
    }

    private static class GroupDTO {
        String groupName = "";
        String folderName = "";
        ArrayList<IconDTO> icons= new ArrayList<IconDTO>();
    }

    private static class IconDTO {
        String name = null;
        String tag = null;
        String value = null;
        String tag2 = null;
        String value2 = null;

        String iconName = null;
        String shieldNameNight = null;
        String shieldNameDay = null;
        float iconSize = -1;
    }

    private static ArrayList<GroupDTO> parseConfig(String filePath) throws XmlPullParserException, IOException {
        File file = new File(filePath);
        XmlPullParser parser = PlatformUtil.newXMLPullParser();
        InputStream fis = new FileInputStream(file);
        if (file.getName().endsWith(".gz")) {
            fis = new GZIPInputStream(fis);
        }
        parser.setInput(fis, "UTF-8");
        int next;

        ArrayList<GroupDTO> resultGroups= new ArrayList<GroupDTO>();
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
                    tempGroup.folderName = parser.getAttributeValue("", "foldername");
                } else if (name.equals("icon")) {
                    tempIcon.name = parser.getAttributeValue("", "name");
                    tempIcon.tag = parser.getAttributeValue("", "tag");
                    tempIcon.value = parser.getAttributeValue("", "value");
                    tempIcon.tag2 = parser.getAttributeValue("", "tag2");
                    tempIcon.value2 = parser.getAttributeValue("", "value2");
                }
            }
        }
        return resultGroups;
    }


    private static class HtmlGenerator {
        public static void generate(ArrayList<GroupDTO> groups) {
            for (GroupDTO group : groups) {
                String content = "### " + group.groupName + "\n\n";
                content += "<LegendItem itemsMap={{\n";
                String folderName = resultsFolderPath + group.folderName ;

                for (IconDTO icon : group.icons) {
                    String iconName = group.folderName + "_" + icon.iconName.replace("_day", "");
                    content += String.format("        '%s' : '%s/%s',\n", icon.name, group.folderName, iconName);
                }
                content += "        }}>\n";
                content += "</LegendItem>\n\n";

                try {
                    FileWriter writer = new FileWriter(folderName + "/autogenerated.txt");
                    writer.write(content);
                    writer.close();
                } catch (IOException e) {
                    System.out.println("HtmlGenerator: file saving error for " + folderName);
                    e.printStackTrace();
                }
            }
        }
    }

    private static class SvgGenerator {

        public static String generate(String iconName, String shieldName, String backgroundColor, float backgroundOpacity) {
            try {
                String content = String.format("<svg width=\"%d\" height=\"%d\" viewBox=\"0 0 %d %d\" fill=\"none\" xmlns=\"http://www.w3.org/2000/svg\">\n",
                        canvasWidth, canvasHeight, canvasWidth, canvasHeight);
                if (!Algorithms.isEmpty(backgroundColor)) {
                    content += geBackgroundRect(backgroundColor, backgroundOpacity);
                }
                if (!Algorithms.isEmpty(shieldName)) {
                    content += getShield(shieldName);
                }
                if (!Algorithms.isEmpty(iconName)) {
                    content += getIcon(iconName);
                }
                content += "</svg>";
                return content;
            } catch (Exception e) {
                return "";
            }
        }

        private static String geBackgroundRect(String color, float opacity) {
            return String.format("<rect width=\"%d\" height=\"%d\" fill=\"%s\" fill-opacity=\"%f\"/>\n", canvasWidth, canvasHeight, color, opacity);
        }

        private static String getShield(String shieldName) throws IOException, XmlPullParserException {
            String filePath = shieldsFolder + "h_" + shieldName + ".svg";
            SvgDTO shieldSvg = parseSvg(filePath);
            return moveToCenterAndResize(shieldSvg, shieldSize);
        }

        private static String getIcon(String iconName) throws IOException, XmlPullParserException {
            String filePath = iconsFolder + "mx_" + iconName + ".svg";
            SvgDTO iconSvg = parseSvg(filePath);
            return moveToCenterAndResize(iconSvg, iconSize);
        }

        private static String moveToCenterAndResize(SvgDTO svg, Double newSize) {
            double rescalingRatio = newSize / svg.width;
            double xOffset = canvasWidth / 2 - newSize / 2;
            double yOffset = canvasHeight / 2 - newSize / 2;
            String resultContent = String.format("\n<g transform=\"translate(%f, %f) scale(%f %f) \"> \n", xOffset, yOffset, rescalingRatio, rescalingRatio);
            resultContent += svg.content + "</g>\n\n" ;
            return resultContent;
        }

        private static SvgDTO parseSvg(String filePath) throws XmlPullParserException, IOException {
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
                    }
                }
            }

            parsedSvg.content = getSvgInnerContent(filePath);
            return parsedSvg;
        }

        private static String getSvgInnerContent(String filePath) throws IOException {
            String firstTagStart = "<svg";
            String firstTagEnd = ">";
            String secondTag = "</svg>";
            String svgInnerContent = Files.readString(Path.of(filePath));
            int trimIndex = svgInnerContent.indexOf(firstTagStart);
            trimIndex = svgInnerContent.indexOf(firstTagEnd, trimIndex);
            svgInnerContent = svgInnerContent.substring(trimIndex + firstTagEnd.length());
            return svgInnerContent.replace(secondTag, "");
        }

        private static class SvgDTO {
            String content = null;
            double width = -1;
            double height = -1;
        }
    }

}
