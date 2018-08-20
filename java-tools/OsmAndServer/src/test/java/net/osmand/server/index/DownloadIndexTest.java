package net.osmand.server.index;

import crosby.binary.Osmformat;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.FileSystemResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

import java.io.File;

import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
public class DownloadIndexTest {

    private final String host = "localhost:8080";

    @Autowired
    private MockMvc mvc;

    private void testExistence(String folder, String filename, String type) throws Exception {
        FileSystemResource file = new FileSystemResource(
                new File("/var/www-download/" + folder + filename));

        mvc.perform(MockMvcRequestBuilders.get(
                "http://" + host + "/download?" + type +"=yes" + "&file=" + file.getFilename())
                .accept(MediaType.ALL))
                //.header(HttpHeaders.HOST, "download.osmand.net"))
                .andExpect(status().isOk())
                .andExpect(header().longValue(HttpHeaders.CONTENT_LENGTH, file.contentLength()));
    }

    @Test
    public void testCheckRangeRequests() throws Exception {
        FileSystemResource file = new FileSystemResource(
                new File("/var/www-download/indexes/Ukraine_kiev_europe_2.obf.zip"));
        mvc.perform(MockMvcRequestBuilders.head(
                "http://" + host + "/download.php?standard=yes&file=" + file.getFilename())
                .accept(MediaType.ALL))
                .andExpect(status().isOk())
                .andExpect(header().string(HttpHeaders.ACCEPT_RANGES, "bytes"))
                .andExpect(header().longValue(HttpHeaders.CONTENT_LENGTH, file.contentLength()));
    }

    @Test
    public void testCheckRangeRequestsFileNotFound() throws Exception {
        mvc.perform(MockMvcRequestBuilders.head(
                "http://" + host + "/download.php?standard=yes&file=1234Albania_europe_2.obf.zip")
                .accept(MediaType.ALL))
                .andExpect(status().isNotFound());
    }

    @Test
    public void testDownloadEntire() throws Exception {
        FileSystemResource file = new FileSystemResource(
                new File("/var/www-download/indexes/Ukraine_kiev_europe_2.obf.zip"));
        int size = (int)file.contentLength();
        byte[] body = new byte[size];
        file.getInputStream().read(body);

        mvc.perform(MockMvcRequestBuilders.get(
                "http://" + host + "/download?standard=yes&file=" + file.getFilename())
                .accept(MediaType.ALL))
                //.header(HttpHeaders.HOST, "download.osmand.net"))
                .andExpect(status().isOk())
                .andExpect(header().longValue(HttpHeaders.CONTENT_LENGTH, file.contentLength()))
                .andExpect(content().bytes(body));
    }

    @Test
    public void testDownloadEntireWithOutPhp() throws Exception {
        FileSystemResource file = new FileSystemResource(
                new File("/var/www-download/indexes/Ukraine_kiev_europe_2.obf.zip"));
        int size = (int)file.contentLength();
        byte[] body = new byte[size];
        file.getInputStream().read(body);

        mvc.perform(MockMvcRequestBuilders.get(
                "http://" + host + "/download?standard=yes&file=" + file.getFilename())
                .accept(MediaType.ALL))
                //.header(HttpHeaders.HOST, "download.osmand.net"))
                .andExpect(status().isOk())
                .andExpect(header().longValue(HttpHeaders.CONTENT_LENGTH, file.contentLength()))
                .andExpect(content().bytes(body));
    }

    @Test
    public void testRegionExistence() throws Exception {
        testExistence("indexes/", "Ukraine_kiev_europe_2.obf.zip", "standard");
    }

    @Test
    public void testVoiceExistence() throws Exception {
        testExistence("indexes/", "ru_0.voice.zip", "standard");
    }

    @Test
    public void testDepthExistence() throws Exception {
        testExistence("indexes/inapp/depth/", "Depth_netherlands_2.obf.zip", "inapp");
    }

    @Test
    public void testFontsExistence() throws Exception {
        testExistence("indexes/fonts/", "NotoSans-Korean.otf.zip", "fonts");
    }

    @Test
    public void testAosmcExistence() throws Exception {
        testExistence("aosmc/albania_europe/", "Albania_europe_18_07_00.obf.gz", "aosmc");
    }

    @Test
    public void testRoadExistence() throws Exception {
        testExistence("road-indexes/", "Luxembourg_europe_2.road.obf.zip", "road");
    }

    @Test
    public void testSrtmCountriesExistence() throws Exception {
        testExistence("srtm-countries/", "Slovenia_europe_2.srtm.obf.zip", "srtmcountry");
    }

    @Test
    public void testWikiExistence() throws Exception {
        testExistence("wiki/", "Luxembourg_europe_2.wiki.obf.zip", "wiki");
    }

    @Test
    public void testWikivoyagePartialDownload() throws Exception {
        FileSystemResource file = new FileSystemResource(
                new File("/var/www-download/wikivoyage/World_wikivoyage.sqlite"));
        int size = 1024;
        byte[] body = new byte[size];
        file.getInputStream().read(body);
        mvc.perform(MockMvcRequestBuilders.get(
                "http://" + host + "/download?wikivoyage=yes&file=" + file.getFilename())
                .accept(MediaType.ALL)
                .header(HttpHeaders.RANGE, "bytes=0-1023"))
                .andExpect(status().isPartialContent())
                .andExpect(content().bytes(body));
    }

    @Test
    public void testHillshadeExistence() throws Exception {
        testExistence("hillshade/", "Hillshade_Andorra_europe.sqlitedb", "hillshade");
    }
}