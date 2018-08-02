package net.osmand.server.index;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.core.io.AbstractResource;
import org.springframework.core.io.Resource;
import org.springframework.http.ContentDisposition;
import org.springframework.http.HttpHeaders;
import org.springframework.web.servlet.resource.AbstractResourceResolver;
import org.springframework.web.servlet.resource.HttpResource;
import org.springframework.web.servlet.resource.ResourceResolverChain;

import javax.servlet.http.HttpServletRequest;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.nio.channels.ReadableByteChannel;
import java.util.List;

public class IndexResourceResolver extends AbstractResourceResolver {
    private static final Log LOGGER = LogFactory.getLog(IndexResourceResolver.class);
    /*
		DATE_AND_EXT_STR_LEN = "_18_06_02.obf.gz".length()
	 */
    private static final int DATE_AND_EXT_STR_LEN = 16;

    private boolean checkParameter(HttpServletRequest request, String parameter) {
        return request.getParameter(parameter) != null;
    }

    @Override
    protected Resource resolveResourceInternal(HttpServletRequest request, String requestPath,
                                               List<? extends Resource> locations, ResourceResolverChain chain) {
        Resource resource = chain.resolveResource(request, requestPath, locations);
        if (request == null) {
            return resource;
        }
        String file = request.getParameter("file");

        if (checkParameter(request, "srtm")) {
            file = "srtm" + File.separator + file;
        }
        if (checkParameter(request, "srtmcountry")) {
            file = "srtm-countries" + File.separator + file;
        }
        if (checkParameter(request, "road")) {
            file = "road-indexes" + File.separator + file;
        }
        if (checkParameter(request,"osmc")) {
            String folder = file.substring(0, file.length() - DATE_AND_EXT_STR_LEN).toLowerCase();
            file = "osmc" + File.separator + folder + File.separator + file;
        }
        if (checkParameter(request, "aosmc")) {
            String folder = file.substring(0, file.length() - DATE_AND_EXT_STR_LEN).toLowerCase();
            file = "aosmc" + File.separator + folder + File.separator + file;
        }
        if (checkParameter(request,"wiki")) {
            file = "wiki" + File.separator + file;
        }
        if (checkParameter(request, "hillshade")) {
            file = "hillshade" + File.separator + file;
        }
        if (checkParameter(request, "inapp")) {
            file = "indexes/inapp/depth" + File.separator + file;
        }
        if (checkParameter(request, "wikivoyage")) {
            file = "wikivoyage" + File.separator + file;
        }
        if (checkParameter(request, "fonts")) {
            file = "indexes/fonts" + File.separator + file;
        }
        if (checkParameter(request, "standard")) {
            file = "indexes" + File.separator + file;
        }
        for (Resource location : locations) {
            try {
                Resource indexResource = new IndexResource(location.createRelative(file));
                if (indexResource.exists()) {
                    return indexResource;
                }
            } catch (IOException ex) {
                LOGGER.error(ex.getMessage(), ex);
            }
        }
        return resource;
    }

    @Override
    protected String resolveUrlPathInternal(String resourceUrlPath, List<? extends Resource> locations, ResourceResolverChain chain) {
        return chain.resolveUrlPath(resourceUrlPath, locations);
    }

    static class IndexResource extends AbstractResource implements HttpResource {

        private final Resource resource;

        public IndexResource(Resource resource) {
            this.resource = resource;
        }

        @Override
        public HttpHeaders getResponseHeaders() {
            HttpHeaders headers;
            if (resource instanceof HttpResource) {
                headers = ((HttpResource) resource).getResponseHeaders();
            } else {
                headers = new HttpHeaders();
                headers.setContentDisposition(ContentDisposition.builder(
                        "attachment").filename(resource.getFilename()).build());
            }
            return headers;
        }

        @Override
        public String getDescription() {
            return resource.getDescription();
        }

        @Override
        public InputStream getInputStream() throws IOException {
            return resource.getInputStream();
        }

        @Override
        public boolean exists() {
            return resource.exists();
        }

        @Override
        public boolean isReadable() {
            return resource.isReadable();
        }

        @Override
        public boolean isOpen() {
            return resource.isOpen();
        }

        @Override
        public boolean isFile() {
            return resource.isFile();
        }

        @Override
        public URL getURL() throws IOException {
            return resource.getURL();
        }

        @Override
        public URI getURI() throws IOException {
            return resource.getURI();
        }

        @Override
        public File getFile() throws IOException {
            return resource.getFile();
        }

        @Override
        public ReadableByteChannel readableChannel() throws IOException {
            return resource.readableChannel();
        }

        @Override
        public long contentLength() throws IOException {
            return resource.contentLength();
        }

        @Override
        public long lastModified() throws IOException {
            return resource.lastModified();
        }

        @Override
        public Resource createRelative(String relativePath) throws IOException {
            return resource.createRelative(relativePath);
        }

        @Override
        public String getFilename() {
            return resource.getFilename();
        }
    }
}
