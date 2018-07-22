package net.osmand.server.controllers.pub;


import java.io.File;
import java.io.IOException;

import net.osmand.server.index.DownloadIndexDocument;
import net.osmand.server.index.DownloadIndexesService;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.FileSystemResource;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

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

	@RequestMapping(path = { "indexes.xml", "indexes" }, produces = {"application/xml"})
	@ResponseBody
    public FileSystemResource indexesXml(@RequestParam(required=false) boolean update, 
    		@RequestParam(required=false) boolean refresh) throws IOException {
    	File fl = downloadIndexes.getIndexesXml(refresh || update, false);
        return new FileSystemResource(fl); 
    }
	
	@RequestMapping(path = { "get_indexes.php", "get_indexes"})
	@ResponseBody
    public FileSystemResource indexesPhp(@RequestParam(defaultValue="", required=false)
    String gzip) throws IOException {
		boolean gz = gzip != null && !gzip.isEmpty();
		File fl = downloadIndexes.getIndexesXml(false, gz);
		return new FileSystemResource(fl); 
	}
    
    @RequestMapping(value = "indexes.php")
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
}
