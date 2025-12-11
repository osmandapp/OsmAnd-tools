package net.osmand.server.controllers.pub;

import net.osmand.server.api.services.DownloadIndex;
import net.osmand.server.api.services.DownloadIndexDocument;
import net.osmand.server.api.services.DownloadIndexesService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import jakarta.servlet.http.HttpServletResponse;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Controller
public class IndexController {

    private static final Log LOGGER = LogFactory.getLog(IndexController.class);

    private final SimpleDateFormat dateFormat = new SimpleDateFormat("dd.MM.yyyy");

    @Value("${osmand.files.location}")
    private String filesLocation;

    @Autowired
    private DownloadIndexesService downloadIndexes;


    private <T,R extends Comparable<? super R>> Comparator<T> compareBy(Function<T,R> fun) {
        return Comparator.comparing(fun);
    }

    private List<DownloadIndex> sortUsingComparatorAndDirection(List<DownloadIndex> list, Comparator<DownloadIndex> comparator, boolean asc) {
        if (asc) {
            return list.stream().sorted(comparator).collect(Collectors.toList());
        } else {
            return list.stream().sorted(comparator.reversed()).collect(Collectors.toList());
        }
    }

    private String getFileSizeInMBFormatted(File file) {
        double size = file.length() / (1024.0 * 1024.0);
        if (size < 0.05) {
            size = 0.1;
        }
        return String.format("%.1f", size);
    }

    private synchronized String formatDate(File file) {
        return dateFormat.format(new Date(file.lastModified()));
    }

    private boolean isUpdateAvailable(long fileTimestamp, long timestamp) {
        return fileTimestamp > timestamp;
    }

    private String getFileUpdateDate(File file) {
        String fname = file.getName();
        int len = fname.length();
        return fname.substring(len - 15, len - 7);
    }

    private void writeAttributes(XMLStreamWriter writer, File file, long timestamp) throws XMLStreamException {
        String size = getFileSizeInMBFormatted(file);
        long containerSize = file.length();
        long contentSize = 2 * containerSize;
        long fileTimestamp = file.lastModified();
        if (isUpdateAvailable(fileTimestamp, timestamp)) {
            writer.writeEmptyElement("update");
        } else {
            writer.writeEmptyElement("outdate");
        }
        writer.writeAttribute("updateDate", getFileUpdateDate(file));
        writer.writeAttribute("containerSize", String.valueOf(containerSize));
        writer.writeAttribute("contentSize", String.valueOf(contentSize));
        writer.writeAttribute("timestamp", String.valueOf(fileTimestamp));
        writer.writeAttribute("date", formatDate(file));
        writer.writeAttribute("size", size);
        writer.writeAttribute("name", file.getName());
    }

    private void close(XMLStreamWriter writer) {
        try {
            writer.close();
        } catch (XMLStreamException ex) {
            LOGGER.error(ex.getMessage(), ex);
        }
    }

    private void close(StringWriter writer) {
        try {
            writer.close();
        } catch (IOException ex) {
            LOGGER.error(ex.getMessage(), ex);
        }
    }

	@RequestMapping(path = { "indexes.xml"}, produces = {"application/xml"})
	@ResponseBody
    public FileSystemResource indexesXml(@RequestParam(required=false) boolean update, 
			@RequestParam(required = false) boolean refresh) throws IOException {
		File fl = downloadIndexes.getIndexesXml(refresh || update, false);
		return new FileSystemResource(fl);
	}
	
	@RequestMapping(path = { "get_indexes.php", "get_indexes"})
	@ResponseBody
	public ResponseEntity<Resource> indexesPhp(@RequestParam(required = false) String gzip, HttpServletResponse resp)
			throws IOException {
		boolean gz = gzip != null;
		File fl = downloadIndexes.getIndexesXml(false, gz);
        HttpHeaders headers = new HttpHeaders();
        headers.add(HttpHeaders.CONTENT_DISPOSITION, String.format("attachment; filename=\"%s\"", fl.getName()));
        if (gz) {
            headers.add(HttpHeaders.CONTENT_TYPE, "application/gzip");
        } else {
            headers.add(HttpHeaders.CONTENT_TYPE, "application/xml");
        }
		return  ResponseEntity.ok().headers(headers).body(new FileSystemResource(fl));
	}
    
    @RequestMapping(value = "admin/indexes")
    public String indexes(@RequestParam(required=false) boolean update,
    		@RequestParam(required=false)  boolean refresh, Model model) throws IOException {
    	// keep this step
    	DownloadIndexDocument doc = downloadIndexes.getIndexesDocument(refresh || update, false);
		model.addAttribute("region", doc.getMaps());
		model.addAttribute("road_region", doc.getRoadMaps());
		List<DownloadIndex> srtms = new ArrayList<>(doc.getSrtmMaps());
		srtms.addAll(doc.getSrtmFeetMaps());
		doc.sortMaps(srtms);
		model.addAttribute("srtmcountry", srtms);
		model.addAttribute("wiki", doc.getWikimaps());
		model.addAttribute("travel", doc.getTravelGuides());
		model.addAttribute("hillshade", doc.getHillshade());
		model.addAttribute("depth", doc.getDepths());
		model.addAttribute("slope", doc.getSlope());
		model.addAttribute("heightmap", doc.getHeightmap());
		model.addAttribute("weather", doc.getWeather());
        return "admin/indexes";
    }

    @RequestMapping(value = {"list", "list.php"})
    public String list(@RequestParam(required = false) String sortby,
                          @RequestParam(required = false) boolean asc,
                          Model model) throws IOException {
    	DownloadIndexDocument doc = downloadIndexes.getIndexesDocument(false, false);
        List<DownloadIndex> regions = doc.getMaps();
        if (sortby != null && sortby.equals("name")) {
            regions = sortUsingComparatorAndDirection(regions, compareBy(DownloadIndex::getName), asc);
            asc = !asc;
        }

        if (sortby != null && sortby.equals("date")) {
            regions = sortUsingComparatorAndDirection(regions, compareBy(DownloadIndex::getDate), asc);
            asc = !asc;
        }

        if (sortby != null && sortby.equals("size")) {
            regions = sortUsingComparatorAndDirection(regions, compareBy(DownloadIndex::getSize), asc);
            asc = !asc;
        }

        if (sortby != null && sortby.equals("descr")) {
            regions = sortUsingComparatorAndDirection(regions, compareBy(DownloadIndex::getDescription), asc);
            asc = !asc;
        }

        model.addAttribute("regions", regions);
        model.addAttribute("asc", asc);
        return "pub/list";
    }

    @GetMapping(path = {"check_live", "check_live.php"}, produces = MediaType.APPLICATION_XML_VALUE)
    @ResponseBody
    public ResponseEntity<String> checkLive(@RequestParam("file") String file,
                                            @RequestParam("timestamp") Long timestamp) {
        if (timestamp == null || file == null || file.isEmpty() || file.contains("/")) {
            return ResponseEntity.badRequest().body("BadRequest");
        }
        XMLOutputFactory output = XMLOutputFactory.newInstance();
        XMLStreamWriter xmlWriter = null;
        StringWriter stringWriter = null;
        try {
            stringWriter = new StringWriter();
            xmlWriter = output.createXMLStreamWriter(stringWriter);
            xmlWriter.writeStartDocument();
            xmlWriter.writeStartElement("updates");
            xmlWriter.writeAttribute("file", file);
            xmlWriter.writeAttribute("timestamp", String.valueOf(timestamp));
            File dir = new File(filesLocation, "aosmc/"+file.toLowerCase());
            if (dir.isDirectory()) {
                Stream<File> files = Arrays.stream(dir.listFiles());
                List<File> sortedFiles = files.sorted(Comparator.comparing(File::getName)).collect(Collectors.toList());
                for (File mapFile : sortedFiles) {
                    String filename = mapFile.getName().toLowerCase();
                    if (filename.startsWith(file.toLowerCase())) {
                        writeAttributes(xmlWriter, mapFile, timestamp);
                    }
                }
            }
            xmlWriter.writeEndElement();
            xmlWriter.writeEndDocument();
            xmlWriter.flush();
        } catch (XMLStreamException ex) {
            LOGGER.error("Error within checklive: " + ex.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Can't process request");
        }
        finally {
            if (xmlWriter != null) {
                close(xmlWriter);
            }
            if (stringWriter != null) {
                close(stringWriter);
            }
        }
        return ResponseEntity.ok(stringWriter.getBuffer().toString());
    }
}