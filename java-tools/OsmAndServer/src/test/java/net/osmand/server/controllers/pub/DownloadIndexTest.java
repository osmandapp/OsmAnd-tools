package net.osmand.server.controllers.pub;

import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.redirectedUrl;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.request;

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


@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
public class DownloadIndexControllerTest {

    private final String host = "builder.osmand.net";

    @Autowired
    private MockMvc mvc;

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
                "http://" + host + "/download.php?standard=yes&file=Albania_europe_2.obf.zip")
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
                "http://" + host + "/download.php?standard=yes&file=" + file.getFilename())
                .accept(MediaType.ALL)
                .header(HttpHeaders.HOST, "download.osmand.net"))
                .andExpect(status().isOk())
                .andExpect(header().longValue(HttpHeaders.CONTENT_LENGTH, file.contentLength()))
                .andExpect(content().bytes(body));
    }





}
