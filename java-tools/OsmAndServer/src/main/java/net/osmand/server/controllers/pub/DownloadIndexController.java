package net.osmand.server.controllers.pub;

import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpRange;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.util.MimeTypeUtils;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.util.List;

@Controller
public class DownloadIndexController {

    private static final int BUFFER_SIZE = 4096;

    private final File rootDir = new File("/var/www-download/");

    private Resource getFileAsResource(String dir, String filename) throws FileNotFoundException {
        File file = new File(rootDir,dir + filename);
        System.out.println("File " + file.getAbsolutePath());
        if (file.exists()) {
            return new FileSystemResource(file);
        }
        throw new FileNotFoundException("File: " + filename + " not found");

    }

    private void writePartially(Resource res, HttpHeaders headers, HttpServletResponse resp) throws IOException {
        long contentLength = res.contentLength();
        List<HttpRange> ranges;
        try {
            ranges = headers.getRange();
        } catch (IllegalArgumentException ex) {
            System.out.println("Error" + ex.getMessage());
            resp.addHeader("Content-Range", "bytes */" + contentLength);
            resp.sendError(HttpServletResponse.SC_REQUESTED_RANGE_NOT_SATISFIABLE);
            return;
        }
        resp.setStatus(HttpServletResponse.SC_PARTIAL_CONTENT);
        resp.addHeader(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + res.getFilename() + "\"");
        if (ranges.size() == 1) {
            HttpRange range = ranges.get(0);
            long start = range.getRangeStart(contentLength);
            long end = range.getRangeEnd(contentLength);
            long rangeLength = end - start + 1;

            resp.setContentLengthLong(rangeLength);
            resp.setContentType(MediaType.APPLICATION_OCTET_STREAM_VALUE);
            resp.setHeader(HttpHeaders.ACCEPT_RANGES, "bytes");
            resp.addHeader(HttpHeaders.CONTENT_RANGE, String.format("bytes %d-%d/%d", start, end, rangeLength));

            try(InputStream in = res.getInputStream()) {
                copyRange(in, resp.getOutputStream(), start, end);
            }
        } else {
            String boundaryString = MimeTypeUtils.generateMultipartBoundaryString();
            resp.setContentType("multipart/byteranges; boundary=" + boundaryString);
            ServletOutputStream out = resp.getOutputStream();
            for (HttpRange range : ranges) {
                long start = range.getRangeStart(contentLength);
                long end = range.getRangeEnd(contentLength);
                InputStream in = res.getInputStream();

                out.println();
                out.println("--" + boundaryString);
                out.println("Content-Type: " + MediaType.APPLICATION_OCTET_STREAM_VALUE);
                out.println("Content-Range: bytes " + start + "-" + end + "/" + contentLength);
                out.println();
                copyRange(in, out, start, end);
            }
            out.println();
            out.print("--" + boundaryString + "--");
        }
    }



    private void copyRange(InputStream in, OutputStream out, long start, long end) throws IOException {
        long skipped = in.skip(start);
        if (skipped < start) {
            throw new IOException("Skipped only " + skipped + " bytes out of " + start + " required.");
        }

        long rangeToCopy = end - start + 1;

        byte[] buf = new byte[BUFFER_SIZE];
        while (rangeToCopy > 0) {
            int read = in.read(buf);
            if (read <= rangeToCopy) {
                out.write(buf, 0, read);
                rangeToCopy -= read;
            } else {
                out.write(buf, 0, (int) rangeToCopy);
                rangeToCopy = 0;
            }
            if (read < buf.length) {
                break;
            }
        }

    }

    @RequestMapping("/download.php")
    @ResponseBody
    public void downloadRegion(@RequestParam("file") String file,
                             @RequestParam("standard") String standard,
                             @RequestHeader HttpHeaders headers,
                             HttpServletResponse resp) throws IOException {
        if (file.equals("World_basemap_2.obf.zip")) {
            Resource res = getFileAsResource("indexes/", file);
            System.out.println(res);
            writePartially(res, headers, resp);
            return;
        }
        writePartially(getFileAsResource("indexes/", file), headers, resp);
    }



}
