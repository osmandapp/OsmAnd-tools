package net.osmand.server.mapillary.services;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import net.osmand.binary.VectorTile;
import net.osmand.server.mapillary.CameraPlace;
import net.osmand.server.mapillary.wikidata.Query;
import net.osmand.server.mapillary.wikidata.WikiBatch;
import net.osmand.server.mapillary.wikidata.WikiPage;
import org.apache.commons.collections4.map.HashedMap;
import org.geojson.Feature;
import org.geojson.FeatureCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestTemplate;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.MemoryHandler;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.junit.Assert.*;
import static org.springframework.test.web.client.ExpectedCount.manyTimes;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

@RunWith(SpringRunner.class)
@AutoConfigureMockMvc
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class ImageServiceTest {

    private static final String WIKI_URL = "https://commons.wikimedia.org/w/api.php?format=json&" +
            "formatversion=2&action=query&prop=imageinfo&iiprop=timestamp|user|url&iiurlwidth=576&titles=File:Parker_Solar_Probe.jpg";

    private static final String WIKI_RESPONSE = "{\n" +
            "    \"batchcomplete\": true,\n" +
            "    \"query\": {\n" +
            "        \"normalized\": [\n" +
            "            {\n" +
            "                \"fromencoded\": false,\n" +
            "                \"from\": \"File:Parker_Solar_Probe.jpg\",\n" +
            "                \"to\": \"File:Parker Solar Probe.jpg\"\n" +
            "            }\n" +
            "        ],\n" +
            "        \"pages\": [\n" +
            "            {\n" +
            "                \"pageid\": 69306218,\n" +
            "                \"ns\": 6,\n" +
            "                \"title\": \"File:Parker Solar Probe.jpg\",\n" +
            "                \"imagerepository\": \"local\",\n" +
            "                \"imageinfo\": [\n" +
            "                    {\n" +
            "                        \"timestamp\": \"2018-05-20T14:15:35Z\",\n" +
            "                        \"user\": \"Sitak87\",\n" +
            "                        \"thumburl\": \"https://upload.wikimedia.org/wikipedia/commons/thumb/1/1c/Parker_Solar_Probe.jpg/576px-Parker_Solar_Probe.jpg\",\n" +
            "                        \"thumbwidth\": 576,\n" +
            "                        \"thumbheight\": 399,\n" +
            "                        \"url\": \"https://upload.wikimedia.org/wikipedia/commons/1/1c/Parker_Solar_Probe.jpg\",\n" +
            "                        \"descriptionurl\": \"https://commons.wikimedia.org/wiki/File:Parker_Solar_Probe.jpg\",\n" +
            "                        \"descriptionshorturl\": \"https://commons.wikimedia.org/w/index.php?curid=69306218\"\n" +
            "                    }\n" +
            "                ]\n" +
            "            }\n" +
            "        ]\n" +
            "    }\n" +
            "}";

    private static final double lon = 30.506424700000025;
    private static final double lat = 50.436434399999996;

    private static final String MAPILLARY_URL = "https://a.mapillary.com/v3/images/?closeto=30.506424700000025,50.436434399999996&radius=50&client_id=LXJVNHlDOGdMSVgxZG5mVzlHQ3ZqQTo0NjE5OWRiN2EzNTFkNDg4";

    private static final String MAPILLARY_RESPONSE = "{\n" +
            "    \"type\": \"FeatureCollection\",\n" +
            "    \"features\": [\n" +
            "        {\n" +
            "            \"type\": \"Feature\",\n" +
            "            \"properties\": {\n" +
            "                \"ca\": 6.0383299999999736,\n" +
            "                \"camera_make\": \"Apple\",\n" +
            "                \"camera_model\": \"iPhone7,2\",\n" +
            "                \"captured_at\": \"2016-10-08T12:14:09.963Z\",\n" +
            "                \"key\": \"dbUkRnV8zubmYUfLayGgXw\",\n" +
            "                \"pano\": false,\n" +
            "                \"user_key\": \"0U4O8yII202S84EnmD0-sw\",\n" +
            "                \"username\": \"kuka\"\n" +
            "            },\n" +
            "            \"geometry\": {\n" +
            "                \"type\": \"Point\",\n" +
            "                \"coordinates\": [\n" +
            "                    30.506477,\n" +
            "                    50.436314\n" +
            "                ]\n" +
            "            }\n" +
            "        },\n" +
            "        {\n" +
            "            \"type\": \"Feature\",\n" +
            "            \"properties\": {\n" +
            "                \"ca\": 42.4436,\n" +
            "                \"camera_make\": \"Apple\",\n" +
            "                \"camera_model\": \"iPhone7,2\",\n" +
            "                \"captured_at\": \"2016-10-08T12:14:05.899Z\",\n" +
            "                \"key\": \"W2dLdVFPnrQY-U7AO7k06Q\",\n" +
            "                \"pano\": false,\n" +
            "                \"user_key\": \"0U4O8yII202S84EnmD0-sw\",\n" +
            "                \"username\": \"kuka\"\n" +
            "            },\n" +
            "            \"geometry\": {\n" +
            "                \"type\": \"Point\",\n" +
            "                \"coordinates\": [\n" +
            "                    30.506469,\n" +
            "                    50.436294\n" +
            "                ]\n" +
            "            }\n" +
            "        }\n" +
            "    ]\n" +
            "}";

    @Autowired
    private ImageService service;

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(wireMockConfig().port(8089));

    private WikiBatch prepareBatchWithFile() {
        WikiPage wp = new WikiPage();
        wp.setTitle("File:Parker Solar Probe.jpg");

        Query query = new Query();
        List<WikiPage> wikiPages = new ArrayList<WikiPage>();
        wikiPages.add(wp);
        query.setPages(wikiPages);

        WikiBatch wikiBatch = new WikiBatch();
        wikiBatch.setQuery(query);

        return wikiBatch;
    }

    private WikiBatch prepareBatchWithoutFile() {
        WikiPage wp = new WikiPage();
        wp.setTitle("Parker Solar Probe.jpg");
        Query query = new Query();
        List<WikiPage> wikiPages = new ArrayList<WikiPage>();
        wikiPages.add(wp);
        query.setPages(wikiPages);

        WikiBatch wikiBatch = new WikiBatch();
        wikiBatch.setQuery(query);

        return wikiBatch;
    }


    @Test
    public void testWikimediaUrlPhoto() {
        stubFor(get(urlEqualTo(WIKI_URL))
                .willReturn(aResponse()
                    .withStatus(200)
                    .withBody(WIKI_RESPONSE)));

        CameraPlace cp = service.processWikimediaData(0d, 0d, "osmImage");
        assertEquals("url-photo", cp.getType());
        assertEquals("osmImage", cp.getImageUrl());
        assertEquals("osmImage", cp.getUrl());
        assertEquals(0d, cp.getLat(), 1d);
        assertEquals(0d, cp.getLon(), 1d);

        CameraPlace cpNull1 = service.processWikimediaData(0d, 0d, "");
        assertNull(cpNull1);

        CameraPlace cpNull2 = service.processWikimediaData(0d, 0d, null);
        assertNull(cpNull2);

        CameraPlace cp2 = service.processWikimediaData(10d, 0d, "File:Parker Solar Probe.jpg");
        assertEquals("wikimedia-photo", cp2.getType());
        assertEquals("2018-05-20T14:15:35Z", cp2.getTimestamp());
        assertEquals("File:Parker Solar Probe.jpg", cp2.getKey());
        assertEquals("Parker Solar Probe.jpg", cp2.getTitle());
        assertEquals("https://upload.wikimedia.org/wikipedia/commons/thumb/1/1c/Parker_Solar_Probe.jpg/576px-Parker_Solar_Probe.jpg", cp2.getImageUrl());
        assertEquals("https://upload.wikimedia.org/wikipedia/commons/1/1c/Parker_Solar_Probe.jpg", cp2.getImageHiresUrl());
        assertEquals("https://commons.wikimedia.org/wiki/File:Parker_Solar_Probe.jpg", cp2.getUrl());
        assertEquals("Sitak87", cp2.getUsername());
        assertEquals(10, cp2.getLat(), 0);
        assertEquals(0, cp2.getLon(), 0);
        assertFalse(cp2.isExternalLink());

    }

    @Test
    public void testWikimediaUrl() throws Exception {
        Method m = ImageService.class.getDeclaredMethod("isWikimediaUrl", String.class);
        m.setAccessible(true);
        String osmImage = null;

        boolean b = (Boolean) m.invoke(service, osmImage);
        assertFalse(b);

        b = (Boolean) m.invoke(service, "Parker Solar Probe.jpg");
        assertFalse(b);

        b = (Boolean) m.invoke(service, "File:Parker Solar Probe.jpg");
        assertTrue(b);

        b = (Boolean) m.invoke(service, "https://en.wikipedia.org/wiki/File:Bret_08-22-1999_1431Z.png");
        assertTrue(b);
    }

    @Test
    public void testGetFilename() throws Exception {
        Method m = ImageService.class.getDeclaredMethod("getFilename", String.class);
        m.setAccessible(true);

        String r = (String) m.invoke(service, "File:Parker Solar Probe.jpg");
        assertEquals("File:Parker Solar Probe.jpg", r);

        String r2 = (String) m.invoke(service, "https://en.wikipedia.org/wiki/File:Amy_Adams_(29708985502)_(cropped).jpg");
        assertEquals("File:Amy_Adams_(29708985502)_(cropped).jpg", r2);
    }

    @Test
    public void testParseTitle() throws Exception {
        Method m = ImageService.class.getDeclaredMethod("parseTitle", WikiBatch.class);
        m.setAccessible(true);
        String title = (String) m.invoke(service, prepareBatchWithFile());
        assertEquals("Parker Solar Probe.jpg", title);

        title = (String) m.invoke(service, prepareBatchWithoutFile());
        assertEquals("Parker Solar Probe.jpg", title);
    }

    @Test
    public void testAngelDiff() throws Exception {
        Method m = ImageService.class.getDeclaredMethod("angleDiff", double.class, double.class);
        m.setAccessible(true);

        boolean r1 = (Boolean) m.invoke(service, (-10-359), 5);
        assertFalse(r1);

        boolean r2 = (Boolean) m.invoke(service, (-10-359), 10);
        assertTrue(r2);

        boolean r3 = (Boolean) m.invoke(service, (10+359), 5);
        assertFalse(r3);

        boolean r4 = (Boolean) m.invoke(service, (10+359), 10);
        assertTrue(r4);

        boolean r5 = (Boolean) m.invoke(service, (-10-359), 9);
        assertFalse(r5);
    }

    @Test
    public void testSplitByAngel() throws Exception {
        String RESULT_MAP_ARR = "arr";
        String RESULT_MAP_HALFVISARR = "halfvisarr";

        Map<String, List<CameraPlace>> result = new HashedMap<>();
        result.put(RESULT_MAP_ARR, new ArrayList<>());
        result.put(RESULT_MAP_HALFVISARR, new ArrayList<>());

        CameraPlace cp1 = new CameraPlace.CameraPlaceBuilder().setBearing(10).setCa(10).setKey("1").build();
        CameraPlace cp2 = new CameraPlace.CameraPlaceBuilder().setBearing(20).setCa(10).setKey("2").build();

        CameraPlace cp3 = new CameraPlace.CameraPlaceBuilder().setBearing(20).setCa(10).setKey("3").build();
        CameraPlace cp4 = new CameraPlace.CameraPlaceBuilder().setBearing(40).setCa(10).setKey("4").build();

        CameraPlace cp5 = new CameraPlace.CameraPlaceBuilder().setBearing(30).setKey("5").build();

        CameraPlace cp6 = new CameraPlace.CameraPlaceBuilder().setBearing(68).setKey("6").setCa(5).build();

        Method m = ImageService.class.getDeclaredMethod("splitCameraPlaceByAngel", CameraPlace.class, Map.class);
        m.setAccessible(true);

        m.invoke(service, cp1, result);
        m.invoke(service, cp2, result);
        m.invoke(service, cp3, result);
        m.invoke(service, cp4, result);
        m.invoke(service, cp5, result);
        m.invoke(service, cp6, result);

        List<CameraPlace> arr = result.get(RESULT_MAP_ARR);
        assertTrue(arr.size() > 0);
        assertEquals("1", arr.get(0).getKey());
        assertEquals("2", arr.get(1).getKey());
        assertEquals("3", arr.get(2).getKey());

        List<CameraPlace> halfvisarr = result.get(RESULT_MAP_HALFVISARR);
        assertTrue(halfvisarr.size() > 0);

        assertEquals("4", halfvisarr.get(0).getKey());
        assertEquals("5", halfvisarr.get(1).getKey());
    }

    @Test
    public void testParseFeature() throws Exception {
        RestTemplate restTemplate = new RestTemplate();
        MockRestServiceServer server = MockRestServiceServer.bindTo(restTemplate).build();

        server.expect(manyTimes(), requestTo(MAPILLARY_URL)).andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess(MAPILLARY_RESPONSE, MediaType.APPLICATION_JSON));

        List<Feature> features = restTemplate.getForObject(MAPILLARY_URL, FeatureCollection.class).getFeatures();
        assertEquals(2, features.size());

        Method m = ImageService.class.getDeclaredMethod("parseFeature", Feature.class, double.class, double.class);
        m.setAccessible(true);
        Feature feature = features.get(0);
        CameraPlace cp = (CameraPlace) m.invoke(service, feature, 50.43644209158319, 30.506541134669305);

        assertEquals(6.0383299999999736, cp.getCa(), 0.1);
        assertEquals("2016-10-08T12:14:09.963Z", cp.getTimestamp());
        assertEquals("dbUkRnV8zubmYUfLayGgXw", cp.getKey());
        assertEquals("kuka", cp.getUsername());
        assertEquals(50.436314, cp.getLat(), 0.1);
        assertEquals(30.506477, cp.getLon(), 0.1);
    }

    @Test
    public void testIsPrimaryKey() throws Exception {
        Method m = ImageService.class.getDeclaredMethod("isPrimaryCameraPlace", CameraPlace.class, String.class);
        m.setAccessible(true);

        CameraPlace notPrimaryCameraPlace = new CameraPlace.CameraPlaceBuilder().setKey("notprimary").build();
        CameraPlace primaryCameraPlace = new CameraPlace.CameraPlaceBuilder().setKey("primary").build();
        CameraPlace nullKeyCameraPlace = new CameraPlace.CameraPlaceBuilder().build();

        assertFalse((Boolean) m.invoke(service, notPrimaryCameraPlace, "primary"));
        assertFalse((Boolean) m.invoke(service, nullKeyCameraPlace, "primary"));
        assertFalse((Boolean) m.invoke(service, primaryCameraPlace, null));
        assertFalse((Boolean) m.invoke(service, null, "primary"));
        assertTrue((Boolean) m.invoke(service, primaryCameraPlace, "primary"));
    }
}
