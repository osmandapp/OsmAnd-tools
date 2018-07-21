package net.osmand.server.controllers.pub;


import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;

import net.osmand.server.index.DownloadIndex;
import net.osmand.server.index.DownloadIndexesService;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.kxml2.io.KXmlParser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.FileSystemResource;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.xmlpull.v1.XmlPullParser;
import org.xmlpull.v1.XmlPullParserException;

@Controller
public class IndexController {

    private static final Log LOGGER = LogFactory.getLog(IndexController.class);

    @Autowired
    private DownloadIndexesService downloadIndexes;

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
    
    @RequestMapping("indexes.php")
    @ResponseBody
    public FileSystemResource indexesPhp(@RequestParam(required=false) boolean update, 
    		@RequestParam(required=false)  boolean refresh) throws IOException {
    	// keep this step
    	File fl = downloadIndexes.getIndexesXml(refresh || update, false);
    	// TODO print table
    	// possible algorithm
    	// 1. read from xml into DownloadIndex Map, Map<DownloadType, List<DownloadIndex> >
    	// 2. print each section
        return new FileSystemResource(fl); 
    }
}
