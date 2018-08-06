package net.osmand.server.index;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.core.io.AbstractResource;
import org.springframework.core.io.Resource;
import org.springframework.http.ContentDisposition;
import org.springframework.http.HttpHeaders;
import org.springframework.lang.Nullable;
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
    private static final Log LOG = LogFactory.getLog(IndexResourceResolver.class);
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
        if (request == null) {
            LOG.error("Request is null. Can't resolve resource");
            return null;
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
        Resource resource = null;
        for (Resource location : locations) {
            try {
                resource = new IndexResource(location.createRelative(file));
                if (resource.exists()) {
                    return resource;
                }
            } catch (IOException ex) {
                LOG.error(ex.getMessage(), ex);
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
        @Nullable
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
        @Nullable
        public URL getURL() throws IOException {
            return resource.getURL();
        }

        @Override
        @Nullable
        public URI getURI() throws IOException {
            return resource.getURI();
        }

        @Override
        @Nullable
        public File getFile() throws IOException {
            return resource.getFile();
        }

        @Override
        @Nullable
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
        @Nullable
        public Resource createRelative(String relativePath) throws IOException {
            return resource.createRelative(relativePath);
        }

        @Override
        public String getFilename() {
            return resource.getFilename();
        }
    }
}
