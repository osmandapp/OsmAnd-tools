package net.osmand.render;

import com.google.gson.*;
import com.google.gson.stream.JsonReader;
import net.osmand.data.LatLon;
import net.osmand.util.MapUtils;


import java.io.*;
import java.util.*;

public class OsmAndTestOpenGLRenderer {

    CummulativeException cummulativeException;
    private Map<Integer, Integer> DISTANCES_TABLE;
    private final int DISTANCE_ZOOM = 15;
    private final int MAX_ZOOM = 21;
    private final int MAX_DISTANCE_IN_METERS = 300;//for 15 zoom


    public static void main(String[] args) {
        String testFile = null;
        String jsonDir = null
        for (String a : args) {
            if (a.startsWith("--test=")) {
                testFile = a.substring("--test=".length());
            } else if (a.startsWith("--jsonDir")) {
                jsonDir = a.substring("--jsonDir=".length());
            }
        }
        if (testFile == null || jsonDir == null) {
            System.out.println("--test=path_to_test.json --jsonDir=path_to_render_results_json_dir");
            return;
        }
        File jsonDirectory = new File(jsonDir);
        if (!jsonDirectory.isDirectory()) {
            System.out.println("--jsonDir= must be a valid directory");
        }
        OsmAndTestOpenGLRenderer test = new OsmAndTestOpenGLRenderer();
        try {
            test.initDistanceTable();
            test.test(new File(testFile), jsonDirectory);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void initDistanceTable() {
        if (DISTANCES_TABLE == null) {
            DISTANCES_TABLE = new HashMap<>();
            for (int i = 0; i <= MAX_ZOOM; i++) {
                double coef = Math.pow(2, DISTANCE_ZOOM - i);
                DISTANCES_TABLE.put(i, (int) (coef * MAX_DISTANCE_IN_METERS));
            }
        }
    }

    public static void generateSh(String[] args) {
        String shFile = null;
        String testFile = null;
        for (String a : args) {
            if (a.startsWith("--sh=")) {
                shFile = a.substring("--sh=".length());
            } else if (a.startsWith("--test=")) {
                testFile = a.substring("--test=".length());
            }
        }
        if (shFile == null || testFile == null) {
            System.out.println("--test=path_to_test.json --sh=path_to_store_sh");
            return;
        }
        OsmAndTestOpenGLRenderer test = new OsmAndTestOpenGLRenderer();
        try {
            test.generateEyepieceSh(new File(testFile), new File(shFile));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void generateEyepieceSh(File testResoure, File eyepieceSh) throws IOException {
        Gson gson = new Gson();
        JsonArray arr = gson.fromJson(new JsonReader(new FileReader(testResoure)), JsonArray.class);
        StringBuilder builder = new StringBuilder();
        builder.append("#!/bin/bash -xe" + "\n");
        for (int i = 0; i < arr.size(); i++) {
            EyepieceParams params = new EyepieceParams();
            JsonObject o = (JsonObject) arr.get(i);
            JsonObject center = o.getAsJsonObject("center");
            assert(center != null);
            params.latitude = center.getAsJsonPrimitive("latitude").getAsDouble();
            params.longitude = center.getAsJsonPrimitive("longitude").getAsDouble();

            parseVisibilityZoom(o.getAsJsonArray("icons"), params);
            parseVisibilityZoom(o.getAsJsonArray("textOnPath"), params);
            parseVisibilityZoom(o.getAsJsonArray("text"), params);

            JsonPrimitive eyepieceParams = o.getAsJsonPrimitive("eyepieceParams");
            if (eyepieceParams != null) {
                params.commandParams = eyepieceParams.getAsString();
            }

            JsonPrimitive testName = o.getAsJsonPrimitive("testName");
            assert(testName != null);
            params.testName = testName.getAsString();
            builder.append(params.getCommand() + "\n");
        }
        FileWriter writer = new FileWriter(eyepieceSh);
        writer.write(builder.toString());
        writer.close();
    }

    private void test(File testResoure, File renderedJsonDir) throws FileNotFoundException {
        initDistanceTable();
        cummulativeException = new CummulativeException();
        Gson gson = new Gson();
        JsonArray arr = gson.fromJson(new JsonReader(new FileReader(testResoure)), JsonArray.class);
        for (int i = 0; i < arr.size(); i++) {
            JsonObject o = (JsonObject) arr.get(i);
            JsonPrimitive testNamePrimitive = o.getAsJsonPrimitive("testName");
            assert(testNamePrimitive != null);
            String testName = testNamePrimitive.getAsString();
            cummulativeException.setCurrentTestName(testName);
            List<RenderedInfo> renderedInfo = parseRenderedJsonForMap(renderedJsonDir, testName);
            if (renderedInfo.size() == 0) {
                throw new RuntimeException("File(s) is empty for test:" + testName);
            }
            List<TestInfo> testInfo = parseTestJson(o);
            compareTestAndRenderedInfo(testInfo, renderedInfo);
        }
        if (cummulativeException.hasExceptions()) {
            throw new IllegalStateException(cummulativeException.getFormatExceptions());
        }
    }

    private void compareTestAndRenderedInfo(List<TestInfo> testInfo, List<RenderedInfo> renderedInfo) {
        for (TestInfo t : testInfo) {
            checkOsmIdAndText(renderedInfo, t);
        }
    }

    private void checkOsmIdAndText(List<RenderedInfo> renderedInfo, TestInfo testInfo) {
        HashSet<Integer> checkedZooms = testInfo.visibleZooms;
        checkedZooms.addAll(testInfo.inVisibleZooms);
        for (RenderedInfo info : renderedInfo) {
            int zoom = info.zoom;
            if (!checkedZooms.contains(zoom)) {
                continue;
            }
            if (info.id == testInfo.id && testInfo.visibleZooms.contains(zoom)) {
                checkedZooms.remove(zoom);
                continue;
            }
            if (info.id == testInfo.id && testInfo.inVisibleZooms.contains(zoom)) {
                cummulativeException.addException("osmId:" + testInfo.id + " must be not visible on zoom:" + zoom);
                checkedZooms.remove(zoom);
                continue;
            }
            if (testInfo.type == RenderedType.TEXT_ON_LINE || testInfo.type == RenderedType.TEXT_ON_POINT) {
                if (info.content != null && info.content.equals(testInfo.text)) {
                    LatLon c = null;
                    LatLon c2 = null;
                    if (testInfo.center != null) {
                        c = testInfo.center;
                    } else if (testInfo.startPoint != null && testInfo.endPoint != null) {
                        c = MapUtils.calculateMidPoint(testInfo.startPoint, testInfo.endPoint);
                    }
                    if (info.startPoint != null && info.endPoint != null) {
                        c2 = MapUtils.calculateMidPoint(info.startPoint, info.endPoint);
                    } else if (info.center != null) {
                        c2 = info.center;
                    }
                    if (c != null && c2 != null) {
                        double dist = MapUtils.getDistance(c, c2);
                        if (dist <= DISTANCES_TABLE.get(zoom)) {
                            if (testInfo.inVisibleZooms.contains(zoom)) {
                                cummulativeException.addException("text:" + testInfo.text + " must be not visible on zoom:" + zoom);
                            }
                        } else {
                            cummulativeException.addException("text:" + testInfo.text + " is visible on zoom:" + zoom +
                                    ", but too far from test location. Found location " + info.id + " " + c2.getLatitude() + " " + c2.getLongitude() +
                                    ". Distance " + (int) dist + " meters");
                        }
                        checkedZooms.remove(zoom);
                    }
                }
            }
            if (checkedZooms.size() == 0) {
                break;
            }
        }
        checkedZooms.removeAll(testInfo.inVisibleZooms);
        if (checkedZooms.size() > 0) {
            String name = testInfo.text != null ? " name:\"" + testInfo.text + "\"" : "";
            cummulativeException.addException("osmId:" + testInfo.id + name + " must be visible on zooms:" + checkedZooms.toString());
        }
    }

    private List<TestInfo> parseTestJson(JsonObject testJsonObj) {
        List<TestInfo> result = new ArrayList<>();
        parseTestJsonArr(testJsonObj.getAsJsonArray("icons"), result, RenderedType.ICON);
        parseTestJsonArr(testJsonObj.getAsJsonArray("text"), result, RenderedType.TEXT_ON_POINT);
        parseTestJsonArr(testJsonObj.getAsJsonArray("textOnPath"), result, RenderedType.TEXT_ON_LINE);
        return result;
    }

    private void parseTestJsonArr(JsonArray arr, List<TestInfo> result, RenderedType type) {
        if (arr == null) {
            return;
        }
        for (int i = 0; i < arr.size(); i++) {
            TestInfo testInfo = new TestInfo();
            JsonObject obj = (JsonObject) arr.get(i);
            if (obj.getAsJsonPrimitive("osmId") == null) {
                throw new RuntimeException("osmId not found");
            }
            if (obj.getAsJsonObject("visibilityZoom") == null) {
                throw new RuntimeException("visibilityZoom not found");
            }

            try {
                testInfo.id = obj.getAsJsonPrimitive("osmId").getAsLong();
            } catch (NumberFormatException e) {
                throw new RuntimeException("osmId is empty");
            }
            JsonObject zooms = obj.getAsJsonObject("visibilityZoom");
            Set<Map.Entry<String, JsonElement>> set = zooms.entrySet();
            for (Map.Entry<String, JsonElement> s : set) {
                int z = Integer.parseInt(s.getKey());
                String v = s.getValue().getAsString();
                boolean visible = "true".equals(v) || "yes".equals(v);
                if (visible) {
                    testInfo.addVisibleZoom(z);
                } else {
                    testInfo.addInVisibleZoom(z);
                }
            }

            JsonPrimitive lat = obj.getAsJsonPrimitive("latitude");
            JsonPrimitive lon = obj.getAsJsonPrimitive("longitude");
            if (lat != null && lon != null) {
                testInfo.center = new LatLon(lat.getAsDouble(), lon.getAsDouble());
            }

            lat = obj.getAsJsonPrimitive("lat");
            lon = obj.getAsJsonPrimitive("lon");
            if (lat != null && lon != null) {
                testInfo.center = new LatLon(lat.getAsDouble(), lon.getAsDouble());
            }

            JsonPrimitive name = obj.getAsJsonPrimitive("name");
            if (name != null) {
                testInfo.text = name.getAsString();
            }

            JsonObject startPoint = obj.getAsJsonObject("startPoint");
            JsonObject endPoint = obj.getAsJsonObject("endPoint");
            if (startPoint != null && endPoint != null) {
                lat = startPoint.getAsJsonPrimitive("latitude");
                lon = startPoint.getAsJsonPrimitive("longitude");
                assert (lat != null && lon != null);
                testInfo.startPoint = new LatLon(lat.getAsDouble(), lon.getAsDouble());
                lat = endPoint.getAsJsonPrimitive("latitude");
                lon = endPoint.getAsJsonPrimitive("longitude");
                assert (lat != null && lon != null);
                testInfo.endPoint = new LatLon(lat.getAsDouble(), lon.getAsDouble());
            }

            testInfo.type = type;

            result.add(testInfo);
        }
    }

    private List<RenderedInfo> parseRenderedJsonForMap(File jsonDir, String testName) throws FileNotFoundException {
        assert(jsonDir.isDirectory());
        final String mapName = getMapName(testName);
        File[] jsonFiles = jsonDir.listFiles(pathname -> pathname.getName().endsWith(".json") && pathname.getName().startsWith(mapName));
        if (jsonFiles.length == 0) {
            throw new RuntimeException("File(s) not found:" + mapName + "000x.json for test:" + testName);
        }
        List<RenderedInfo> renderedInfo = new ArrayList<>();
        for (File f : jsonFiles) {
            Gson gson = new Gson();
            JsonArray arr = gson.fromJson(new JsonReader(new FileReader(f)), JsonArray.class);
            renderedInfo.addAll(parseRenderedJson(arr));
        }
        return renderedInfo;
    }

    private List<RenderedInfo> parseRenderedJson(JsonArray arr) {
        if (arr == null) {
            return null;
        }
        List<RenderedInfo> info = new ArrayList();
        for (int i = 0; i < arr.size(); i++) {
            RenderedInfo ri = new RenderedInfo();
            JsonObject obj = (JsonObject) arr.get(i);
            String cl = obj.getAsJsonPrimitive("class").getAsString();
            String type = obj.getAsJsonPrimitive("type").getAsString();
            ri.setType(cl, type);
            ri.zoom = obj.getAsJsonPrimitive("zoom").getAsInt();
            ri.id = obj.getAsJsonPrimitive("id").getAsLong();
            double lat = obj.getAsJsonPrimitive("lat").getAsDouble();
            double lon = obj.getAsJsonPrimitive("lon").getAsDouble();
            ri.center = new LatLon(lat, lon);

            JsonObject start = obj.getAsJsonObject("startPoint");
            JsonObject end = obj.getAsJsonObject("endPoint");
            if (start != null && end != null) {
                lat = start.getAsJsonPrimitive("lat").getAsDouble();
                lon = start.getAsJsonPrimitive("lon").getAsDouble();
                ri.startPoint = new LatLon(lat, lon);
                lat = end.getAsJsonPrimitive("lat").getAsDouble();
                lon = end.getAsJsonPrimitive("lon").getAsDouble();
                ri.endPoint = new LatLon(lat, lon);
            }
            JsonPrimitive content = obj.getAsJsonPrimitive("content");
            if (content != null) {
                ri.content = content.getAsString();
            }
            info.add(ri);
        }
        return info;
    }

    private void parseVisibilityZoom(JsonArray arr, EyepieceParams params) {
        if (arr == null) {
            return;
        }
        for (int i = 0; i < arr.size(); i++) {
            JsonObject obj = (JsonObject) arr.get(i);
            JsonObject zooms = obj.getAsJsonObject("visibilityZoom");
            Set<Map.Entry<String, JsonElement>> set = zooms.entrySet();
            for (Map.Entry<String, JsonElement> s : set) {
                int z = Integer.parseInt(s.getKey());
                params.registerZoom(z);
            }
        }
    }

    private class RenderedInfo {
        RenderedType type;
        String content;
        long id;
        LatLon center;
        LatLon startPoint;
        LatLon endPoint;
        int zoom;
        public void setType(String cl, String type) {
            if (cl.equals("icon")) {
                this.type = RenderedType.ICON;
            } else if (cl.equals("caption")) {
                if (type.equals("billboard")) {
                    this.type = RenderedType.TEXT_ON_POINT;
                } else {
                    this.type = RenderedType.TEXT_ON_LINE;
                }
            }
        }
    }

    private class TestInfo {
        long id;
        LatLon center;
        LatLon startPoint;
        LatLon endPoint;
        HashSet<Integer> visibleZooms = new HashSet<>();
        HashSet<Integer> inVisibleZooms = new HashSet<>();
        String text;
        RenderedType type;
        void addVisibleZoom(int zoom) {
            visibleZooms.add(zoom);
        }
        void addInVisibleZoom(int zoom) {
            inVisibleZooms.add(zoom);
        }
    }

    private enum RenderedType {
        ICON,
        TEXT_ON_POINT,
        TEXT_ON_LINE
    }

    private class EyepieceParams {
        String testName = "";
        int minZoom = 21;
        int maxZoom = 0;
        double latitude;
        double longitude;
        String commandParams = "";

        void registerZoom(int zoom) {
            minZoom = Math.min(minZoom, zoom);
            maxZoom = Math.max(maxZoom, zoom);
        }

        String getCommand() {
            assert(minZoom < maxZoom);
            StringBuilder builder = new StringBuilder();
            builder.append("./eyepiece_standalone -verbose ");
            if (!commandParams.contains("-obfsPath")) {
                builder.append("-obfsPath=./maps ");
            }
            if (!commandParams.contains("-geotiffPath")) {
                builder.append("-geotiffPath=./geotiffs ");
            }
            if (!commandParams.contains("-cachePath")) {
                builder.append("-cachePath=./cache ");
            }
            if (!commandParams.contains("-outputRasterWidth")) {
                builder.append("-outputRasterWidth=1024 ");
            }
            if (!commandParams.contains("-outputRasterHeight")) {
                builder.append("-outputRasterHeight=768 ");
            }
            if (!commandParams.contains("-outputImageFilename")) {
                builder.append("-outputImageFilename=./mapimage/" + getMapName(testName) + " ");
            }
            if (!commandParams.contains("-outputJSONFilename")) {
                builder.append("-outputJSONFilename=./mapdata/" + getMapName(testName) + " ");
            }
            if (!commandParams.contains("-latLon") && !commandParams.contains("-endLatLon")) {
                builder.append("-latLon=" + latitude + ":" + longitude + " ");
            }
            if (!commandParams.contains("-zoom") && !commandParams.contains("-endZoom")) {
                builder.append("-zoom=" + minZoom + " -endZoom=" + maxZoom + " ");
            }
            if (!commandParams.contains("-frames")) {
                int frames = maxZoom - minZoom + 1;
                builder.append("-frames=" + frames +  " ");
            }
            builder.append(commandParams);
            return builder.toString();
        }
    }

    private static String getMapName(String testName) {
        String shortName = testName.substring(0, Math.min(testName.length(), 10));
        return shortName.replaceAll("[^a-zA-Z0-9\\.\\-]", "_");
    }

    private class CummulativeException {
        Map<String, List<String>> exceptions;
        String currentTestName = "";

        void addException(String e) {
            if (exceptions == null) {
                exceptions = new HashMap<>();
            }
            if (currentTestName.isEmpty()) {
                throw new RuntimeException("Set test name");
            }
            if (exceptions.get(currentTestName) == null) {
                exceptions.put(currentTestName, new ArrayList<>());
            }
            List<String> explist = exceptions.get(currentTestName);
            explist.add(e);
        }

        void setCurrentTestName(String testName) {
            currentTestName = testName;
        }

        boolean hasExceptions() {
            return exceptions.size() > 0;
        }

        String getFormatExceptions() {
            String res = "\n";
            for (Map.Entry<String, List<String>> entry : exceptions.entrySet()) {
                res += ">>>>" + entry.getKey() + "\n";
                for (String s : entry.getValue()) {
                    res += "\t\t" + s + "\n";
                }
                res += "\n";
            }
            return res;
        }
    }
}
