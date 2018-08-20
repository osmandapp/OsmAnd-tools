package net.osmand.server.mapillary.services;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import net.osmand.server.mapillary.CameraPlace;
import net.osmand.server.mapillary.wikidata.Query;
import net.osmand.server.mapillary.wikidata.WikiBatch;
import net.osmand.server.mapillary.wikidata.WikiPage;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.lang.reflect.Method;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.junit.Assert.*;

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

    @Autowired
    private ImageService service;

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(wireMockConfig().port(8089));

    private WikiBatch prepareBatch() {
        WikiPage wp = new WikiPage();
        wp.setTitle("File:Parker Solar Probe.jpg");

        Query query = new Query();
        query.getPages().add(wp);

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

        CameraPlace cp2 = service.processWikimediaData(0d, 0d, "File:Parker Solar Probe.jpg");

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
}
