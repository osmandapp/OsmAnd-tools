package net.osmand.server.controllers.pub;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

import jakarta.servlet.http.HttpServletResponse;

import net.osmand.PlatformUtil;
import net.osmand.server.api.services.BuildDeltaService;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.xmlpull.v1.XmlSerializer;


@Controller
public class BuildsController {

	protected static final Log LOGGER = LogFactory.getLog(BuildsController.class);

	private static final String INDEX_FILE = "builds.xml";

	private static final long DELAY_TO_REBUILD = 15 * 1000;
	private final SimpleDateFormat FMT = new SimpleDateFormat("dd.MM.yyyy");

	@Autowired
	private BuildDeltaService deltaService;

	@Value("${osmand.files.location}")
	private String ROOT_FOLDER;

	@Value("${osmand.gen.location}")
	private String pathToGenFiles;

	private synchronized void updateBuilds(File output) throws IOException {
		File allBuilds = new File(ROOT_FOLDER, "night-builds");
		File latestBuilds = new File(ROOT_FOLDER, "latest-night-build");
		XmlSerializer serializer = PlatformUtil.newSerializer();
		serializer.setOutput(new FileWriter(output));
		serializer.setFeature("http://xmlpull.org/v1/doc/features.html#indent-output", true);
		long tm = System.currentTimeMillis();
		serializer.startDocument("UTF-8", true);
		serializer.startTag(null, "builds");
		serializer.attribute(null, "version", "1.1");
		serializer.attribute(null, "incremental", "true");
		addBuilds(latestBuilds, serializer);
		try {
			addNightBuilds(allBuilds, serializer);
		} catch (Exception e) {
			e.printStackTrace();
		}
		serializer.startTag(null, "time");
		serializer.attribute(null, "gentime", String.format("%.1f", (System.currentTimeMillis() - tm) / 1000.0));
		SimpleDateFormat fmt = new SimpleDateFormat("dd.MM.yyyy HH:mm:ss");
		serializer.attribute(null, "time", fmt.format(new Date()));
		serializer.endTag(null, "time");
		serializer.endDocument();
		serializer.flush();
	}

	private void addBuilds(File buildsFolder, XmlSerializer serializer) throws IOException {
		File[] listFiles = !buildsFolder.exists() ? null : buildsFolder.listFiles();
		if (listFiles != null) {
			Arrays.sort(listFiles, (File o1, File o2) -> -Long.compare(o1.lastModified(), o2.lastModified()));
			for (File f : listFiles) {
				serialize(serializer, buildsFolder.getName(), f.getName(), f.length(), f.lastModified());
			}
		}
	}

	public void addNightBuilds(File buildsFolder, XmlSerializer serializer) throws Exception {
		for (BuildDeltaService.BuildFile file : deltaService.getNightBuilds()) {
			serialize(serializer, buildsFolder.getName(), file.name, file.size, file.mtime.getTime());
		}
	}

	private void serialize(XmlSerializer serializer, String buildsFolderName, String fileName, long length,
	                       long lastModified) throws IOException {

		String nm = fileName.toLowerCase();
		if ((!nm.contains("osmand-") && !nm.contains("osmandcore-")) || nm.endsWith(".bar")) {
			return;
		}
		String type = "OsmAnd";
		String tag = nm;
		if (tag.startsWith("osmand-")) {
			tag = tag.substring("osmand-".length());
		}
		if (tag.contains("-nb-")) {
			tag = tag.substring(0, tag.indexOf("-nb-"));
		}
		if (tag.contains("-gnb-")) {
			tag = tag.substring(0, tag.indexOf("-gnb-"));
		}
		if (tag.contains(".")) {
			tag = tag.substring(0, tag.indexOf("."));
		}
		serializer.startTag(null, "build");
		serializer.attribute(null, "size", String.format("%.1f", length / (1024.0 * 1024.0)));
		serializer.attribute(null, "date", FMT.format(new Date(lastModified)));
		serializer.attribute(null, "timestamp", lastModified + "");
		serializer.attribute(null, "type", type);
		serializer.attribute(null, "tag", tag);
		serializer.attribute(null, "path", buildsFolderName + "/" + fileName);
		serializer.endTag(null, "build");
	}

	@RequestMapping(path = {"builds.xml"}, produces = {"application/xml"})
	@ResponseBody
	public FileSystemResource buildsXml(@RequestParam(required = false) boolean update,
	                                    @RequestParam(required = false) boolean refresh) throws IOException {
		File output = new File(pathToGenFiles, INDEX_FILE);
		return new FileSystemResource(output);
	}

	@RequestMapping(path = {"builds.php", "builds"})
	@ResponseBody
	public ResponseEntity<Resource> indexesPhp(@RequestParam(defaultValue = "", required = false)
			                                           String gzip, HttpServletResponse resp) throws IOException {
		File output = new File(pathToGenFiles, INDEX_FILE);
		if (System.currentTimeMillis() - output.lastModified() > DELAY_TO_REBUILD) {
			updateBuilds(output);
		}
		HttpHeaders headers = new HttpHeaders();
		headers.add(HttpHeaders.CONTENT_DISPOSITION, String.format("attachment; filename=\"%s\"", output.getName()));
		headers.add(HttpHeaders.CONTENT_TYPE, "application/xml");
		return ResponseEntity.ok().headers(headers).body(new FileSystemResource(output));
	}

}
