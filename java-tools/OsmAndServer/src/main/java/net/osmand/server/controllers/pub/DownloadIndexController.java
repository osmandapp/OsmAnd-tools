package net.osmand.server.controllers.pub;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpRange;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.util.MimeTypeUtils;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import net.osmand.server.api.services.DownloadIndexesService;
import net.osmand.server.api.services.DownloadIndexesService.DownloadServerLoadBalancer;
import net.osmand.server.api.services.DownloadIndexesService.DownloadServerSpecialty;

@Controller
public class DownloadIndexController {
	private static final Log LOGGER = LogFactory.getLog(DownloadIndexController.class);

	private static final int BUFFER_SIZE = 4096;
	
	@Autowired
	private DownloadIndexesService downloadService;
	
	@Value("${osmand.files.location}")
	private String filesPath;


	/*
		DATE_AND_EXT_STR_LEN = "_18_06_02.obf.gz".length()
	 */
	private static final int DATE_AND_EXT_STR_LEN = 16;


	private Resource getFileAsResource(String dir, String filename) throws FileNotFoundException {
		File file = new File(new File(filesPath, dir), filename);
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

	private Resource findFileResource(MultiValueMap<String, String> params) throws FileNotFoundException {
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
		if (params.containsKey("aosmc") || params.containsKey("osmc")) {
			String folder = filename.substring(0, filename.length() - DATE_AND_EXT_STR_LEN).toLowerCase();
			return getFileAsResource("aosmc" + File.separator + folder, filename);
		}
		if (params.containsKey("wiki")) {
			return getFileAsResource("wiki", filename);
		}
		if (params.containsKey("hillshade")) {
			return getFileAsResource("hillshade", filename);
		}
		if (params.containsKey("slope")) {
			return getFileAsResource("slope", filename);
		}
		if (params.containsKey("heightmap")) {
			return getFileAsResource("heightmap", filename);
		}
		if (params.containsKey("depth")) {
			return getFileAsResource("depth", filename);
		}
		if (params.containsKey("inapp")) {
			String type = params.getFirst("inapp");
			return getFileAsResource("indexes/inapp/"+type, filename);
		}
		if (params.containsKey("wikivoyage")) {
			return getFileAsResource("wikivoyage", filename);
		}
		if (params.containsKey("fonts")) {
			return getFileAsResource("indexes/fonts", filename);
		}
		Resource res = getFileAsResource("indexes", filename);
		if (res.exists()) {
			return getFileAsResource("indexes", filename);
		}
		String msg = "Requested resource is missing or request is incorrect.\nRequest parameters: " + params;
		LOGGER.error(msg);
		throw new FileNotFoundException(msg);
	}

	private boolean isContainAndEqual(String param, String equalTo, MultiValueMap<String, String> params) {
		return params.containsKey(param) && params.getFirst(param) != null && !params.getFirst(param).isEmpty()
				&& params.getFirst(param).equalsIgnoreCase(equalTo);
	}

	private boolean isContainAndEqual(String param, MultiValueMap<String, String> params) {
		return isContainAndEqual(param, "yes", params);
	}

	
	@RequestMapping(value = {"/download.php", "/download"}, method = RequestMethod.GET)
	@ResponseBody
	public void downloadIndex(@RequestParam MultiValueMap<String, String> params,
							  @RequestHeader HttpHeaders headers,
							  HttpServletRequest req,
							  HttpServletResponse resp) throws IOException {
		DownloadServerLoadBalancer servers = downloadService.getSettings();
		InetSocketAddress inetHost = headers.getHost();
		if (inetHost == null) {
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST, "Invalid host name");
			return;
		}
		String remoteAddr = req.getRemoteAddr();
		Enumeration<String> hs = req.getHeaders("X-Forwarded-For");
		if (hs != null && hs.hasMoreElements()) {
			remoteAddr = hs.nextElement();
		}
		boolean self = isContainAndEqual("self", "true", params) ;
		String proto = headers.getFirst("X-Forwarded-Proto");
		if(proto == null) {
			proto = req.getScheme();
		}
		if (!self) {
			String extraParam = "";
			if (params.containsKey("aosmc") || params.containsKey("osmc")) {
				String filename = params.getFirst("file");
				if (filename != null) {
					extraParam = "&region=" + filename.substring(0, filename.length() - DATE_AND_EXT_STR_LEN).toLowerCase();
				}
			}
			String host = null;
			for (DownloadServerSpecialty dss : DownloadServerSpecialty.values()) {
				for (String httpParam : dss.httpParams) {
					if (isContainAndEqual(httpParam, params)) {
						host = servers.getServer(dss, remoteAddr);
						if (host != null) {
							break;
						}
					}
				}
			}
			if (host == null) {
				host = servers.getServer(DownloadServerSpecialty.MAIN, remoteAddr);
			}
			if (host != null) {
				resp.setStatus(HttpServletResponse.SC_FOUND);
				resp.setHeader(HttpHeaders.LOCATION, proto + "://" + host + "/download?" + req.getQueryString() + extraParam);
			} else {
				self = true;
			}
		}
		if (self) {
			handleDownload(findFileResource(params), headers, resp);
		}

	}

	@RequestMapping(value = {"/download.php", "/download"}, method = RequestMethod.HEAD)
	public void checkRangeRequests(@RequestParam MultiValueMap<String, String> params, HttpServletResponse resp)
			throws IOException {
		try {
			long externalUrlSize = getExternalUrlSize(params);
			if (externalUrlSize >= 0) {
				resp.setStatus(HttpServletResponse.SC_OK);
				resp.setHeader(HttpHeaders.ACCEPT_RANGES, "bytes");
				resp.setContentLengthLong(externalUrlSize);
			} else {
				Resource resource = findFileResource(params);
				resp.setStatus(HttpServletResponse.SC_OK);
				resp.setHeader(HttpHeaders.ACCEPT_RANGES, "bytes");
				resp.setContentLengthLong(resource.contentLength());
			}
		} catch (FileNotFoundException ex) {
			LOGGER.error(ex.getMessage(), ex);
			resp.sendError(HttpServletResponse.SC_NOT_FOUND, ex.getMessage());
		}
	}

	long getExternalUrlSize(MultiValueMap<String, String> params) {
		String filename = getFileOrThrow(params);
		String directory = null;
		Map<String, String> dirMap = new HashMap<>() {
			{
				put("srtm", "srtm");
				put("srtmcountry", "srtm-countries");
				put("road", "road-indexes");
				String folder = filename.substring(0, filename.length() - DATE_AND_EXT_STR_LEN).toLowerCase();
				put("aosmc", "aosmc" + File.separator + folder);
				put("wiki", "wiki");
				put("hillshade", "hillshade");
				put("slope", "slope");
				put("heightmap", "heightmap");
				put("depth", "depth");
				String type = params.getFirst("inapp");
				put("inapp", "indexes/inapp/"+type);
				put("fonts", "indexes/fonts");
				//put("standard", "indexes");
			}
		};
		for (Map.Entry<String, String> entry : dirMap.entrySet()) {
			if (params.containsKey(entry.getKey())) {
				directory = entry.getValue();
				break;
			}
		}
		if (directory != null) {
			File subFolder = new File(filesPath, directory);
			File[] files = subFolder.listFiles();
			if (files != null && files.length > 0) {
				DownloadIndexesService downloadIndexesService = new DownloadIndexesService();
				DownloadIndexesService.ExternalSource[] externalSources = downloadIndexesService.getExternalSources(files, directory);
				if (externalSources != null) {
					for (DownloadIndexesService.ExternalSource source : externalSources) {
						if (source.getName().equals(filename)) {
							return source.getSize();
						}
					}
				}
			}
		}
		return -1;
	}
	
}
