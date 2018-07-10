package net.osmand.server.controllers.pub;


import net.osmand.server.index.Index;
import net.osmand.server.index.IndexBuilder;
import net.osmand.server.index.IndexNames;
import org.apache.commons.collections4.map.HashedMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.kxml2.io.KXmlParser;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.xmlpull.v1.XmlPullParser;
import org.xmlpull.v1.XmlPullParserException;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Controller
public class ApiController {

    private static final Log LOGGER = LogFactory.getLog(ApiController.class);

    private static final String PATH_TO_FILE = "/home/yevhenii/osmand/repos/tools/java-tools/OsmAndServer/src/main/resources/indexes.xml";

    private Map<String, List<Index>> initIndexMap() {
        Map<String, List<Index>> indexMap = new HashedMap<>();
        for (String indexName : IndexNames.getNames()) {
            indexMap.put(indexName, new ArrayList<>());
        }
        return indexMap;
    }


    private Map<String, List<Index>> getIndexesFromFile(String filename) throws IOException, XmlPullParserException {
        try (FileInputStream fis = new FileInputStream(filename)) {
            Map<String, List<Index>> indexMap = initIndexMap();
            IndexBuilder indexBuilder = new IndexBuilder();
            XmlPullParser xpp = new KXmlParser();
            xpp.setInput(new InputStreamReader(fis));
            int eventType = xpp.getEventType();
            while (eventType != XmlPullParser.END_DOCUMENT) {
                if (eventType == XmlPullParser.START_TAG) {
                    indexBuilder.setType(xpp.getAttributeValue("", "type"));
                    indexBuilder.setContainerSize(xpp.getAttributeValue("", "containerSize"));
                    indexBuilder.setContentSize(xpp.getAttributeValue("", "contentSize"));
                    indexBuilder.setTimestamp(xpp.getAttributeValue("", "timestamp"));
                    indexBuilder.setDate(xpp.getAttributeValue("", "date"));
                    indexBuilder.setSize(xpp.getAttributeValue("", "size"));
                    indexBuilder.setTargetsize(xpp.getAttributeValue("", "targetsize"));
                    indexBuilder.setName(xpp.getAttributeValue("", "name"));
                    indexBuilder.setDescription(xpp.getAttributeValue("", "description"));
                } else if (eventType == XmlPullParser.END_TAG) {
                    String elemName = xpp.getName();
                    if (IndexNames.getNames().contains(elemName)) {
                        indexMap.get(elemName).add(indexBuilder.createIndex());
                    }
                }
                eventType = xpp.next();
            }
            return indexMap;
        }
    }

    @RequestMapping(value = "indexes", method = RequestMethod.GET)
    public String indexes(Model model) throws IOException, XmlPullParserException {
        Map<String, List<Index>> indexMap = getIndexesFromFile(PATH_TO_FILE);
        model.addAttribute(IndexNames.REGION, indexMap.get(IndexNames.REGION));
        model.addAttribute(IndexNames.ROAD_REGION, indexMap.get(IndexNames.ROAD_REGION));
        model.addAttribute(IndexNames.SRTMCOUNTRY, indexMap.get(IndexNames.SRTMCOUNTRY));
        model.addAttribute(IndexNames.WIKI, indexMap.get(IndexNames.WIKI));
        model.addAttribute(IndexNames.WIKIVOYAGE, indexMap.get(IndexNames.WIKIVOYAGE));
        return "pub/indexes";
    }
}
