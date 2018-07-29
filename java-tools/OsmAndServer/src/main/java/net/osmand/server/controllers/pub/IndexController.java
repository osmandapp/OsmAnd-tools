package net.osmand.server.controllers.pub;


import java.io.*;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.sun.xml.internal.ws.developer.MemberSubmissionAddressing;
import net.osmand.bitcoinsender.model.Input;
import net.osmand.server.index.DownloadIndex;
import net.osmand.server.index.DownloadIndexDocument;
import net.osmand.server.index.DownloadIndexesService;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.WritableResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpRange;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.util.MimeType;
import org.springframework.util.MimeTypeUtils;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

@Controller
public class IndexController {

    private static final Log LOGGER = LogFactory.getLog(IndexController.class);

    @Autowired
    private DownloadIndexesService downloadIndexes;

    private DownloadIndexDocument unmarshallIndexes(File fl) throws IOException {
		DownloadIndexDocument doc;
		try {
			JAXBContext jc = JAXBContext.newInstance(DownloadIndexDocument.class);
			Unmarshaller unmarshaller = jc.createUnmarshaller();
			return (DownloadIndexDocument) unmarshaller.unmarshal(fl);
		} catch (JAXBException ex) {
			LOGGER.error(ex.getMessage(), ex);
			throw new IOException(ex);
		}
	}

    private <T,R extends Comparable<? super R>> Comparator<T> compareBy(Function<T,R> fun) {
        return Comparator.comparing(fun);
    }

    private List<DownloadIndex> sortUsingComparatorAndDirection(List<DownloadIndex> list, Comparator<DownloadIndex> comparator, boolean asc) {
        if (asc) {
            return list.stream().sorted().collect(Collectors.toList());
        } else {
            return list.stream().sorted(comparator.reversed()).collect(Collectors.toList());
        }
    }

	@RequestMapping(path = { "indexes.xml"}, produces = {"application/xml"})
	@ResponseBody
    public FileSystemResource indexesXml(@RequestParam(required=false) boolean update, 
    		@RequestParam(required=false) boolean refresh) throws IOException {
    	File fl = downloadIndexes.getIndexesXml(refresh || update, false);
        return new FileSystemResource(fl); 
    }
	
	@RequestMapping(path = { "get_indexes.php", "get_indexes"})
	@ResponseBody
    public ResponseEntity<Resource> indexesPhp(@RequestParam(defaultValue="", required=false)
    String gzip, HttpServletResponse resp) throws IOException {
		boolean gz = gzip != null && !gzip.isEmpty() && gzip.equals("true");
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
    
    @RequestMapping(value = {"indexes.php", "indexes"})
    public String indexesPhp(@RequestParam(required=false) boolean update,
    		@RequestParam(required=false)  boolean refresh, Model model) throws IOException {
    	// keep this step
    	File fl = downloadIndexes.getIndexesXml(refresh || update, false);
    	// TODO print table
    	// possible algorithm
    	// 1. read from xml into DownloadIndex Map, Map<DownloadType, List<DownloadIndex> >
    	// 2. print each section
		DownloadIndexDocument doc = unmarshallIndexes(fl);
		model.addAttribute("region", doc.getMaps());
		model.addAttribute("road_region", doc.getRoadMaps());
		model.addAttribute("srtmcountry", doc.getSrtmMaps());
		model.addAttribute("wiki", doc.getWikimaps());
		model.addAttribute("wikivoyage", doc.getWikivoyages());
        return "pub/indexes";
    }

    @RequestMapping(value = {"list", "list.php"})
    public String listPhp(@RequestParam(required = false) String sortby,
                          @RequestParam(required = false) boolean asc,
                          Model model) throws IOException {
        File fl = null;
        if (sortby == null || sortby.isEmpty()) {
            // Update at first load
            fl = downloadIndexes.getIndexesXml(true, false);
        } else {
            // Do not update when filtering
            fl = downloadIndexes.getIndexesXml(false, false);
        }
        DownloadIndexDocument doc = unmarshallIndexes(fl);
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
}