package net.osmand.server.controllers.pub;

import java.io.File;
import java.io.IOException;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.FileSystemResource;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.thymeleaf.context.IContext;
import org.thymeleaf.context.WebContext;
import org.thymeleaf.spring5.SpringTemplateEngine;

@Controller
public class WebController {
    private static final Log LOGGER = LogFactory.getLog(WebController.class);

    @Value("${web.location}")
    private String websiteLocation;
    

    @Autowired
    private SpringTemplateEngine templateEngine;


    // TOP LEVEL API (redirects and static files) 
    @RequestMapping(path = { "tile_sources.php", "tile_sources.xml", "tile_sources"}, produces = {"application/xml"})
	@ResponseBody
    public FileSystemResource tileSourcesXml(@RequestParam(required=false) boolean update, 
    		@RequestParam(required=false) boolean refresh) throws IOException {
        return new FileSystemResource(new File(websiteLocation, "tile_sources.xml")); 
    }
    
    @RequestMapping(path = { "go" })
    public void webLocation(HttpServletResponse response, HttpServletRequest request) {
        response.setHeader("Location", "go.html?" + request.getQueryString());
        response.setStatus(302); 
    }
    
    @RequestMapping(path = { "travel" })
    public void travel(HttpServletResponse response, @RequestParam(required=false) String title, 
    		@RequestParam(required=false) String lang) {
        response.setHeader("Location",  "https://"+lang+".wikivoyage.org/wiki/"+title);
        response.setStatus(302); 
    }
    
    public void clearCaches() {
    	templateEngine.clearTemplateCache();
    }
    // WEBSITE
    @RequestMapping(path = { "/apps", "/apps.html" })
    public String apps(HttpServletResponse response) {
    	// TODO generate static 
        return "pub/apps.html"; 
    }
    
    @RequestMapping(path = { "/", "/index.html", "/index" })
    public String index(HttpServletRequest request, HttpServletResponse response) {
    	// TODO generate static 
    	final IContext ctx = new WebContext(request, response, request.getServletContext());

    	String test = templateEngine.process("pub/dvr.html", ctx);
    	System.out.println(test);    	
        return "pub/index.html";
    }
    
    @RequestMapping(path = { "/build_it", "/build_it.html" })
    public String buildIt(HttpServletResponse response) {
    	// TODO generate static 
        return "pub/build_it.html"; 
    }
    
    @RequestMapping(path = { "/dvr", "/dvr.html"  })
    public String dvr(HttpServletResponse response) {
    	// TODO generate static 
        return "pub/dvr.html"; 
    }
    
    @RequestMapping(path = { "/osm_live", "/osm_live.html"  })
    public String osmlive(HttpServletResponse response) {
    	// TODO generate static 
        return "pub/osm_live.html"; 
    }
    
    @RequestMapping(path = { "/downloads", "/downloads.html"  })
    public String downloads(HttpServletResponse response) {
    	// TODO generate static 
        return "pub/downloads.html"; 
    }
    
    @RequestMapping(path = { "/features/{articleId}" })
    public String featuresSpecific(HttpServletResponse response, @PathVariable(required=false) String articleId,
    		Model model) {
    	// TODO generate static 
    	model.addAttribute("article",articleId);
        return "pub/features.html"; 
    }
    
    @RequestMapping(path = { "/features", "/features.html"  })
    public String features(HttpServletResponse response, @RequestParam(required=false) String id,
    		Model model) {
    	if(id != null && !id.equals("main")) {
			response.setHeader("Location", "/features/" + id);
            response.setStatus(301); 
            return null;
    	}
    	// TODO generate static 
		model.addAttribute("article", "main");
        return "pub/features.html"; 
    }
    
    
    @RequestMapping(path = { "/help-online/{articleId}" })
    public String helpSpecific(HttpServletResponse response, @PathVariable(required=false) String articleId,
    		Model model) {
    	// TODO generate static 
    	model.addAttribute("article",articleId);
        return "pub/help-online.html"; 
    }
    
    @RequestMapping(path = { "/help-online", "/help-online.html"  })
    public String help(HttpServletResponse response, @RequestParam(required=false) String id,
    		Model model) {
    	if(id != null) {
			response.setHeader("Location", "/help-online/" + id);
            response.setStatus(301); 
            return null;
    	}
    	// TODO generate static 
		model.addAttribute("article", "faq");
        return "pub/help-online.html"; 
    }
    

    @RequestMapping(path = { "/blog", "/blog.html"  })
    public String blog(HttpServletResponse response, Model model, @RequestParam(required=false) String id) {
    	if(id != null) {
			response.setHeader("Location", "/blog/" + id);
            response.setStatus(301); 
            return null;
    	}
    	// TODO generate static 
    	// TODO evaluate
        model.addAttribute("article", "osmand-3-1-released");
        return "pub/blog.html";
    }
    
    @RequestMapping(path = { "/blog/{articleId}" })
    public String blogSpecific(HttpServletResponse response, @PathVariable(required=false) String articleId,
    		Model model) {
    	// TODO generate static 
    	model.addAttribute("article",articleId);
        return "pub/blog.html"; 
    }
}