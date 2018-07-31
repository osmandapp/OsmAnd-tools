package net.osmand.server.controllers.pub;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpRange;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.util.MimeTypeUtils;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.*;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

@Controller
public class DownloadIndexController {
	private static final Log LOGGER = LogFactory.getLog(DownloadIndexController.class);

	private static final int BUFFER_SIZE = 4096;

	private static final String DOWNLOAD_SERVER = "download.osmand.net";

	private static final int MAIN_SERVERS_LOAD = 60;
	private static final List<String> HELP_SERVERS = Arrays.asList("dl4.osmand.net");
	private static final List<String> MAIN_SERVERS = Arrays.asList("dl6.osmand.net");

	private final File rootDir = new File("/var/www-download/");

	private Resource getFileAsResource(String dir, String filename) throws FileNotFoundException {
		File file = new File(rootDir, dir + File.separator + filename);
		if (file.exists()) {
			return new FileSystemResource(file);
		}
		String msg = "File: " + filename + " not found";
		LOGGER.error(msg);
		throw new FileNotFoundException(msg);

	}

	private void writeEntire(Resource res, HttpHeaders headers, HttpServletResponse resp) throws IOException {
		long contentLength = res.contentLength();
		resp.setStatus(HttpServletResponse.SC_OK);
		resp.addHeader(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + res.getFilename() + "\"");
		resp.setContentLengthLong(contentLength);
		resp.setContentType(MediaType.APPLICATION_OCTET_STREAM_VALUE);
		try (InputStream in = res.getInputStream()) {
			copyRange(in, resp.getOutputStream(), 0, contentLength);
		}
	}

	private void writePartially(Resource res, HttpHeaders headers, HttpServletResponse resp) throws IOException {
		long contentLength = res.contentLength();
		List<HttpRange> ranges;
		try {
			ranges = headers.getRange();
		} catch (IllegalArgumentException ex) {
			resp.addHeader("Content-Range", "bytes */" + contentLength);
			resp.sendError(HttpServletResponse.SC_REQUESTED_RANGE_NOT_SATISFIABLE);
			LOGGER.error(ex.getMessage(), ex);
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

			try (InputStream in = res.getInputStream()) {
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

	private void handleDownload(Resource res, HttpHeaders headers, HttpServletResponse resp) throws IOException {
		if (headers.containsKey(HttpHeaders.RANGE)) {
			writePartially(res, headers, resp);
		} else {
			writeEntire(res, headers, resp);
		}
	}

	private String getFileOrThrow(MultiValueMap<String, String> params) {
		if (params.containsKey("file")) {
			return params.getFirst("file");
		}
		String msg = "File parameter is missing.";
		LOGGER.error(msg);
		throw new IllegalArgumentException(msg);
	}

	private Resource findFileResource(MultiValueMap<String, String> params) throws IOException {
		String filename = getFileOrThrow(params);
		if (params.containsKey("srtm")) {
			return getFileAsResource("srtm", filename);
		}
		if (params.containsKey("srtmcountry")) {
			return getFileAsResource("srtm-countries", filename);
		}
		if (params.containsKey("road")) {
			return getFileAsResource("road-indexes", filename);
		}
		if (params.containsKey("osmc")) {
			throw new FileNotFoundException("Osmc not implemented");
		}
		if (params.containsKey("aosmc")) {
			throw new FileNotFoundException("Aosmc not implemeted");
		}
		if (params.containsKey("wiki")) {
			return getFileAsResource("wiki", filename);
		}
		if (params.containsKey("hillshade")) {
			return getFileAsResource("hillshade", filename);
		}
		if (params.containsKey("inapp")) {
			return getFileAsResource("indexes/inapp", filename);
		}
		if (params.containsKey("wikivoyage")) {
			return getFileAsResource("wiki", filename);
		}
		if (params.containsKey("fonts")) {
			return getFileAsResource("indexes/fonts", filename);
		}
		if (params.containsKey("standard")) {
			return getFileAsResource("indexes", filename);
		}
		String msg = "Requested resource is missing or request is incorrect.\nRequest parameters: " + params;
		throw new FileNotFoundException(msg);

	}

	private boolean isContainAndEqual(String param, String equalTo, MultiValueMap<String, String> params) {
		return params.containsKey(param) && params.getFirst(param) != null && !params.getFirst(param).isEmpty()
				&& params.getFirst(param).equalsIgnoreCase(equalTo);
	}

	private boolean isContainAndEqual(String param, MultiValueMap<String, String> params) {
		return isContainAndEqual(param, "yes", params);
	}

	private boolean computeSimpleCondition(MultiValueMap<String, String> params) {
		return isContainAndEqual("wiki", params)
				|| isContainAndEqual("standard", params)
				|| isContainAndEqual("road", params)
				|| isContainAndEqual("wikivoyage", params);
	}

	private boolean computeStayHereCondition(MultiValueMap<String, String> params) {
		return isContainAndEqual("osmc", params)
				|| isContainAndEqual("aosmc", params)
				|| isContainAndEqual("fonts", params)
				|| isContainAndEqual("inapp", params);
	}

	@RequestMapping(value = "/download.php", method = RequestMethod.GET)
	@ResponseBody
	public void downloadIndex(@RequestParam MultiValueMap<String, String> params,
							  @RequestHeader HttpHeaders headers,
							  HttpServletRequest req,
							  HttpServletResponse resp) throws IOException {
		String hostName = headers.getHost().getHostName();
		boolean self = isContainAndEqual("self", "true", params);
		if (hostName.equals(DOWNLOAD_SERVER) && self) {
			Resource res = findFileResource(params);
			handleDownload(res, headers, resp);
			return;
		}

		if (hostName.equals(DOWNLOAD_SERVER)) {
			ThreadLocalRandom tlr = ThreadLocalRandom.current();
			int random = tlr.nextInt(100);
			boolean isSimple = computeSimpleCondition(params);
			if (computeStayHereCondition(params)) {
				handleDownload(findFileResource(params), headers, resp);
			} else if (HELP_SERVERS.size() > 0 && isSimple && random < (100 - MAIN_SERVERS_LOAD)) {
				String host = HELP_SERVERS.get(random % HELP_SERVERS.size());
				resp.setStatus(HttpServletResponse.SC_FOUND);
				resp.setHeader(HttpHeaders.LOCATION, "http://" + host + "/download.php?" + req.getQueryString());
			} else if (MAIN_SERVERS.size() > 0) {
				String host = MAIN_SERVERS.get(random % MAIN_SERVERS.size());
				resp.setStatus(HttpServletResponse.SC_FOUND);
				resp.setHeader(HttpHeaders.LOCATION, "http://" + host + "/download.php?" + req.getQueryString());
			} else {
				handleDownload(findFileResource(params), headers, resp);
			}
			return;
		}
		Resource res = findFileResource(params);
		handleDownload(res, headers, resp);
	}

	@RequestMapping(value = "/download.php", method = RequestMethod.HEAD)
	public HttpHeaders checkRangeRequests(@RequestParam MultiValueMap<String, String> params) throws IOException {
		HttpHeaders headers = new HttpHeaders();
		Resource resource = findFileResource(params);
		headers.add(HttpHeaders.ACCEPT_RANGES, "bytes");
		headers.setContentLength(resource.contentLength());
		return headers;
	}
}
