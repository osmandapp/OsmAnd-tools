package net.osmand.server.controllers.pub;


import java.io.File;
import java.io.IOException;

import net.osmand.server.index.DownloadIndexesService;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.FileSystemResource;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

@Controller
public class IndexController {

    private static final Log LOGGER = LogFactory.getLog(IndexController.class);

    @Autowired
    private DownloadIndexesService downloadIndexes;

	@RequestMapping(path = { "indexes.xml", "indexes" })
    public FileSystemResource indexesXml(@RequestParam boolean update, 
    		@RequestParam boolean refresh) throws IOException {
    	File fl = downloadIndexes.getIndexesXml(refresh || update);
        return new FileSystemResource(fl); 
    }
	
	@RequestMapping(path = { "get_indexes.php", "get_indexes"})
    public FileSystemResource indexesPhp(@RequestParam String gzip) throws IOException {
		File fl = downloadIndexes.getIndexesXml(false);
		if(gzip != null && !gzip.isEmpty()) {
			return new FileSystemResource(fl.getAbsolutePath() + ".gz"); 
		}
		return new FileSystemResource(fl); 
	}
    
    @RequestMapping("indexes.php")
    public FileSystemResource indexesPhp(@RequestParam boolean update, 
    		@RequestParam boolean refresh) throws IOException {
    	// TODO print table
    	File fl = downloadIndexes.getIndexesXml(refresh || update);
        return new FileSystemResource(fl); 
    }
}
