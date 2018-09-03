package net.osmand.server.controllers.pub;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import net.osmand.PlatformUtil;

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
import org.xmlpull.v1.XmlPullParser;

@Controller
public class WebController {
    private static final Log LOGGER = LogFactory.getLog(WebController.class);

	private static final int LATEST_ARTICLES_MAIN = 10;
	private static final int LATEST_ARTICLES_OTHER = 20;
	private static final int LATEST_ARTICLES_RSS = 15;

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
    
    public static class BlogArticle {
    	public String url;
    	public String id;
    	public String shortTitle;
    	public String title;
    	public Date pubdate;
    	public StringBuilder content = new StringBuilder();
    	public String dateRSS;
    }
    
    private List<BlogArticle> getBlogArticles() {
    	// TODO cache
		File folder = new File(websiteLocation, "blog_articles");
    	File[] files = folder.listFiles();
    	List<BlogArticle> blogs = new ArrayList<WebController.BlogArticle>();
    	Pattern pt = Pattern.compile(" (\\w*)=\\\"([^\\\"]*)\\\"");
    	SimpleDateFormat dateFormat = new SimpleDateFormat("dd-MM-yyyy HH:mm");
    	SimpleDateFormat gmtDateFormat = new SimpleDateFormat("dd MMM yyyy HH:mm:ss zzz", Locale.ENGLISH);
    	gmtDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
    	if(files != null) {
    		for(File f : files) {
    			
    			if(f.getName().endsWith(".html") && !f.getName().equals("osmand_videos.html")) {
    				String id = f.getName().substring(0, f.getName().length() - ".html".length());
    				try {
						BlogArticle ba = new BlogArticle();
						BufferedReader fr = new BufferedReader(new FileReader(f));
						String meta = fr.readLine();
						String header;
						if(!meta.contains("<meta")) {
							header = meta;
							// now we don't need not pubdate articles
							continue;
						} else {
							header = fr.readLine();
							ba.content.append(header);
							String line;
							while((line = fr.readLine()) != null) {
								ba.content.append(line); 
							}
						}
						header = header.substring(header.indexOf(">") + 1);
						header = header.substring(0, header.indexOf("</"));
						ba.title = header;
						ba.url = "/blog/"+id;
						ba.id = id;
						Matcher matcher = pt.matcher(meta);
						Map<String, String> params = new LinkedHashMap<>();
						while(matcher.find()) {
							params.put(matcher.group(1), matcher.group(2));
						}
						if(params.containsKey("title")) {
							ba.shortTitle = params.get("title");
						} else {
							ba.shortTitle = header;
						}
						if(params.containsKey("pubdate")) {
							ba.pubdate = dateFormat.parse(params.get("pubdate"));
							ba.dateRSS = gmtDateFormat.format(ba.pubdate);
							if(ba.pubdate != null) {
								blogs.add(ba);
							}
						}
						
						fr.close();
					} catch (Exception e) {
						LOGGER.error("Error reading blog file " + f.getName()  + " " + e.getMessage());
					}
    			}
    		}
    	}
    	blogs.sort(new Comparator<BlogArticle>() {

			@Override
			public int compare(BlogArticle o1, BlogArticle o2) {
				long l1 = o1.pubdate == null ? 0 : o1.pubdate.getTime();
				long l2 = o2.pubdate == null ? 0 : o2.pubdate.getTime();
				return -Long.compare(l1, l2);
			}
		});
		return blogs;
	}

    @RequestMapping(path = { "/blog", "/blog.html"  })
    public String blog(HttpServletResponse response, Model model, @RequestParam(required=false) String id) {
    	if(id != null) {
			response.setHeader("Location", "/blog/" + id);
            response.setStatus(301); 
            return null;
    	}
    	// TODO generate static 
    	List<BlogArticle> blogs = getBlogArticles();
    	if(blogs.size() > LATEST_ARTICLES_MAIN) {
    		blogs = blogs.subList(0, LATEST_ARTICLES_MAIN);
    	}
    	model.addAttribute("articles", blogs);
        model.addAttribute("article", blogs.get(0).id);
        return "pub/blog.html";
    }
    
    @RequestMapping(path = { "/rss", "/rss.xml"  }, produces = "application/rss+xml")
    public String rss(HttpServletRequest request, HttpServletResponse response,  
    		Model model, @RequestParam(required=false) String id) {
    	if(id != null) {
			response.setHeader("Location", "/blog/" + id);
            response.setStatus(301); 
            return null;
    	}
    	// TODO generate static 
    	final IContext ctx = new WebContext(request, response, request.getServletContext());
    	String cssItem = templateEngine.process("pub/rss_item.css.html", ctx);
    	List<BlogArticle> blogs = getBlogArticles();
    	if(blogs.size() > LATEST_ARTICLES_RSS) {
    		blogs = blogs.subList(0, LATEST_ARTICLES_RSS);
    	}
    	for(BlogArticle b : blogs) {
    		String cont = cssItem + b.content.toString();
    		cont = cont.replace("src=\"/images/", "src=\"https://osmand.net/images/");
    		cont = cutTags(cont, "script");
    		cont = cutTags(cont, "iframe");
    		b.content = new StringBuilder(cont);
    	}
    	model.addAttribute("articles", blogs);

        return "pub/rss.xml";
    }

	private String cutTags(String cont, String tag) {
		boolean changed = true;
		while(changed) {
			changed = false;
			int i = cont.indexOf("<"+tag);
			int e = cont.indexOf("</"+tag+">");
			if(i != -1 && e != -1) {
				cont = cont.substring(0, i) + cont.substring(e + tag.length() + 3);
				changed = true;
			}
		}
		return cont;
	}
    
    @RequestMapping(path = { "/blog/{articleId}" })
    public String blogSpecific(HttpServletResponse response, @PathVariable(required=false) String articleId,
    		Model model) {
    	// TODO generate static
    	List<BlogArticle> blogs = getBlogArticles();
    	if(blogs.size() > LATEST_ARTICLES_OTHER) {
    		blogs = blogs.subList(0, LATEST_ARTICLES_OTHER);
    	}
    	model.addAttribute("articles",blogs);
    	model.addAttribute("article", articleId);
        return "pub/blog.html"; 
    }

	
}