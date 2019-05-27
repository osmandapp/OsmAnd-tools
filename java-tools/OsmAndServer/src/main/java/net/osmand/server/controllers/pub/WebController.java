package net.osmand.server.controllers.pub;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import net.osmand.server.api.services.PollsService;
import net.osmand.server.api.services.PollsService.PollQuestion;

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
@RequestMapping(path = {"", "ru", "de"  })
public class WebController {
    private static final Log LOGGER = LogFactory.getLog(WebController.class);
    
	public static final String[] SUPPORTED_LOCALES = new String[] { "", "ru", "de"  };

	private static final int LATEST_ARTICLES_MAIN = 10;
	private static final int LATEST_ARTICLES_OTHER = 20;
	private static final int LATEST_ARTICLES_RSS = 15;
	
	private Random random = new Random();

    @Value("${web.location}")
    private String websiteLocation;
    
    @Value("${gen.location}")
    private String genLocation;
    

    @Autowired
    private SpringTemplateEngine templateEngine;
    
    @Autowired
    private PollsService pollService;
    
    private List<BlogArticle> cacheBlogArticles = null;
    
    private ConcurrentHashMap<String, GeneratedResource> staticResources = new ConcurrentHashMap<>();
    
    public static class GeneratedResource {
    	public FileSystemResource staticResource;
    	public String template;
    	public String targetFile;
    }
    
    public WebController() {
    }

    
    // TOP LEVEL API (redirects and static files) 
    @RequestMapping(path = { "tile_sources.php", "tile_sources.xml", "tile_sources"}, produces = {"application/xml"})
	@ResponseBody
    public FileSystemResource tileSourcesXml(@RequestParam(required=false) boolean update, 
    		@RequestParam(required=false) boolean refresh) throws IOException {
        return new FileSystemResource(new File(websiteLocation, "tile_sources.xml")); 
    }
    
    @RequestMapping(path = { "go" })
    public void webLocation(HttpServletRequest request, HttpServletResponse response) {
        response.setHeader("Location", "go.html?" + request.getQueryString());
        response.setStatus(302); 
    }
    
    @RequestMapping(path = { "travel" })
    public void travel(HttpServletResponse response, @RequestParam(required=false) String title, 
    		@RequestParam(required=false) String lang) {
        response.setHeader("Location",  "https://"+lang+".wikivoyage.org/wiki/"+title);
        response.setStatus(302); 
    }
    
    public void reloadConfigs(List<String> errors) {
    	templateEngine.clearTemplateCache();
    	cacheBlogArticles = null;
    	pollService.reloadConfigs(errors);
    	staticResources.clear();
    }
    
    private FileSystemResource generateStaticResource(String template, String file, HttpServletRequest request,
			HttpServletResponse response) {
    	return generateStaticResource(template, file, request, response, null);
    }
    
    private FileSystemResource generateStaticResource(String template, String file, HttpServletRequest request,
			HttpServletResponse response, Model model) {
    	try {
    		boolean enableTranslation = request.getParameter("tr") != null;
    		String localePath = "";
    		Locale locale = null;
    		String pth = request.getRequestURI();
    		if(enableTranslation) {
    			file = "tr_"+ file;
    		}
    		for(String loc : SUPPORTED_LOCALES) {
    			if(pth.startsWith("/" + loc +"/")) {
    				file = loc +"_" + file;
    				localePath = loc + "/";
    				locale = new Locale(loc);
    				break;
    			}
    		}
    		
			GeneratedResource gr = staticResources.get(file);
			if(gr == null) {
				Map<String, Object> variables = new TreeMap<String, Object>();
				if(model != null) {
					variables.putAll(model.asMap());
				}
				variables.put("translation", enableTranslation);
				variables.put("locale_path", localePath);
				gr = new GeneratedResource();
				File targetFile = new File(genLocation, file);
				gr.staticResource = new FileSystemResource(targetFile);
				gr.template = template;
				final IContext ctx = new WebContext(request, response, 
						request.getServletContext(), locale, variables);
				String content = templateEngine.process(template, ctx);
				writeToFileSync(targetFile, content);
				staticResources.put(file, gr);
			}
			return gr.staticResource;
		} catch (Exception e) {
			LOGGER.error(String.format("Error generating template %s to %s", template, file, e.getMessage()));
			throw new RuntimeException(e);
		}
	}

	private synchronized void writeToFileSync(File targetFile, String content) throws IOException {
		targetFile.getParentFile().mkdirs();
		FileWriter fw = new FileWriter(targetFile);
		fw.write(content);
		fw.close();
	}
    
    // WEBSITE
    @RequestMapping(path = { "/apps", "/apps.html" })
    @ResponseBody
    public FileSystemResource apps(HttpServletRequest request, HttpServletResponse response) {
        return generateStaticResource("pub/apps.html", "apps.html", request, response); 
    }
    
    

	@RequestMapping(path = { "/", "/index.html", "/index" })
	@ResponseBody
    public FileSystemResource index(HttpServletRequest request, HttpServletResponse response, Model model) {
		Map<String, PollQuestion> polls = pollService.getPollsConfig(false);
		List<String> keys = new ArrayList<String>();
		for(String web : polls.keySet()) {
			if(web.startsWith("website")) {
				keys.add(web);
			}
		}
		int v = 0; 
		if(keys.size() > 0) {
			v = random.nextInt(keys.size());
			model.addAttribute("poll", polls.get(keys.get(v)));
		} 
		return generateStaticResource("pub/index.html", "index_" + v + ".html", request, response, model);
    }

	@RequestMapping(path = { "/android-poll.html" })
	@ResponseBody
    public FileSystemResource androidPoll(HttpServletRequest request, HttpServletResponse response, Model model) {
		Map<String, PollQuestion> polls = pollService.getPollsConfig(false);
		model.addAttribute("poll", polls.get("android"));
        return generateStaticResource("pub/mobile-poll.html", "android-poll.html", request, response, model);
    }

	@RequestMapping(path = { "/ios-poll.html" })
	@ResponseBody
    public FileSystemResource iosPoll(HttpServletRequest request, HttpServletResponse response, Model model) {
		Map<String, PollQuestion> polls = pollService.getPollsConfig(false);
		model.addAttribute("poll", polls.get("ios"));
        return generateStaticResource("pub/mobile-poll.html", "ios-poll.html", request, response, model);
    }
	
    @RequestMapping(path = { "/build_it", "/build_it.html" })
    @ResponseBody
    public FileSystemResource buildIt(HttpServletRequest request, HttpServletResponse response) {
        return generateStaticResource("pub/build_it.html", "build_it.html", request, response);
    }
    
    @RequestMapping(path = { "/dvr", "/dvr.html"  })
    @ResponseBody
    public FileSystemResource dvr(HttpServletRequest request, HttpServletResponse response) {
        return generateStaticResource("pub/dvr.html", "dvr.html", request, response);
    }
    
    @RequestMapping(path = { "/osm_live", "/osm_live.html"  })
    @ResponseBody
    public FileSystemResource osmlive(HttpServletRequest request, HttpServletResponse response) {
        return generateStaticResource("pub/osm_live.html", "osm_live.html", request, response);
    }
    
    @RequestMapping(path = { "/giveaway", "/giveaway.html"  })
    @ResponseBody
    public FileSystemResource giveaway(HttpServletRequest request, HttpServletResponse response) {
        return generateStaticResource("pub/giveaway.html", "giveaway.html", request, response);
    }
    
    @RequestMapping(path = { "/downloads", "/downloads.html"  })
    @ResponseBody
    public FileSystemResource downloads(HttpServletRequest request, HttpServletResponse response) {
        return generateStaticResource("pub/downloads.html", "downloads.html", request, response);
    }
    
    @RequestMapping(path = { "/features/{articleId}" })
    @ResponseBody
    public FileSystemResource featuresSpecific(HttpServletRequest request, HttpServletResponse response, @PathVariable(required=false) String articleId,
    		Model model) {
    	model.addAttribute("article",articleId);
    	model.addAttribute("article_template", getLocaleTemplate("feature_articles/", request, articleId));
    	return generateStaticResource("pub/features.html", "features/"+articleId+".html", request, response, model);
    }
    
    @RequestMapping(path = { "/features", "/features.html"  })
    @ResponseBody
    public FileSystemResource features(HttpServletRequest request, HttpServletResponse response, @RequestParam(required=false) String id,
    		Model model) {
    	if(id != null && !id.equals("main")) {
			response.setHeader("Location", "/features/" + id);
            response.setStatus(301); 
            return null;
    	}
		model.addAttribute("article", "main");
		model.addAttribute("article_template", getLocaleTemplate("feature_articles/", request, "main"));
		return generateStaticResource("pub/features.html", "features.html", request, response, model);
    }
    
    
    @RequestMapping(path = { "/help-online/{articleId}" })
    @ResponseBody
	public FileSystemResource helpSpecific(HttpServletRequest request, HttpServletResponse response,
			@PathVariable(required = false) String articleId, Model model) {
		model.addAttribute("article", articleId);
		return generateStaticResource("pub/help-online.html", "help-online/" + articleId + ".html", request, response,
				model);
	}
    
    @RequestMapping(path = { "/help-online", "/help-online.html"  })
    @ResponseBody
    public FileSystemResource help(HttpServletRequest request, HttpServletResponse response, @RequestParam(required=false) String id,
    		Model model) {
    	if(id != null) {
			response.setHeader("Location", "/help-online/" + id);
            response.setStatus(301); 
            return null;
    	}
		model.addAttribute("article", "faq");
		return generateStaticResource("pub/help-online.html", "help-online.html", request, response, model);
    }

    @RequestMapping(path = { "/blog", "/blog.html"  })
    @ResponseBody
    public FileSystemResource blog(HttpServletRequest request, HttpServletResponse response, Model model, @RequestParam(required=false) String id) {
    	if(id != null) {
			response.setHeader("Location", "/blog/" + id);
            response.setStatus(301); 
            return null;
    	}
    	List<BlogArticle> blogs = getBlogArticles(request, response);
    	if(blogs.size() > LATEST_ARTICLES_MAIN) {
    		blogs = blogs.subList(0, LATEST_ARTICLES_MAIN);
    	}
    	model.addAttribute("articles", blogs);
        model.addAttribute("article", blogs.get(0).id);
        model.addAttribute("article_template", getLocaleTemplate("blog_articles/", request, blogs.get(0).id));
        
        return generateStaticResource("pub/blog.html", "blog.html", request, response, model);
    }
    
    public String getLocaleFromRequest(HttpServletRequest request) {
    	String pth = request.getRequestURI();
		for(String loc : SUPPORTED_LOCALES) {
			if(pth.startsWith("/" + loc +"/")) {
				return loc;
			}
		}
		return "";
    	
    }
    
	private String getLocaleTemplate(String resourcePath, HttpServletRequest request, String id) {
		String l = getLocaleFromRequest(request);
		if (l.length() > 0) {
			String fullPath = resourcePath + l + "/" + id;
			if (new File(websiteLocation, fullPath).exists() || new File(websiteLocation, fullPath + ".html").exists()) {
				return fullPath;
			}
		}
		return resourcePath + id;
	}


	@RequestMapping(path = { "/blog/{articleId}" })
    @ResponseBody
    public FileSystemResource blogSpecific(HttpServletRequest request, HttpServletResponse response, @PathVariable(required=false) String articleId,
    		Model model) {
    	List<BlogArticle> blogs = getBlogArticles(request, response);
    	if(blogs.size() > LATEST_ARTICLES_OTHER) {
    		blogs = blogs.subList(0, LATEST_ARTICLES_OTHER);
    	}
    	model.addAttribute("articles",blogs);
    	model.addAttribute("article", articleId);
    	model.addAttribute("article_template", getLocaleTemplate("blog_articles/", request, articleId));
    	return generateStaticResource("pub/blog.html", "blog/"+articleId+".html", request, response, model);
    }
    
    @RequestMapping(path = { "/rss", "/rss.xml"  }, produces = "application/rss+xml")
    @ResponseBody
    public FileSystemResource rss(HttpServletRequest request, HttpServletResponse response,  
    		Model model, @RequestParam(required=false) String id) {
    	if(id != null) {
			response.setHeader("Location", "/blog/" + id);
            response.setStatus(301); 
            return null;
    	}
    	List<BlogArticle> blogs = getBlogArticles(request, response);
    	if(blogs.size() > LATEST_ARTICLES_RSS) {
    		blogs = blogs.subList(0, LATEST_ARTICLES_RSS);
    	}
    	
    	model.addAttribute("articles", blogs);
    	return generateStaticResource("pub/rss.xml", "rss.xml", request, response, model);
    }

	


    // BLOG articles
    
    public static class BlogArticle {
    	public String url;
    	public String id;
    	public String shortTitle;
    	public String title;
    	public Date pubdate;
    	// rss content
    	public StringBuilder content = new StringBuilder();
    	public String dateRSS;
    	
    	public Map<String, BlogArticle> translations = Collections.emptyMap();
    }
    
    public List<BlogArticle> getBlogArticles(HttpServletRequest request, HttpServletResponse response) {
    	List<BlogArticle> articles = this.cacheBlogArticles;
    	if(articles == null) {
    		articles = loadBlogArticles(request, response);
    	}
    	String locale = getLocaleFromRequest(request);
		if (locale.length() > 0) {
			ArrayList<BlogArticle> res = new ArrayList<WebController.BlogArticle>();
			for (BlogArticle b : articles) {
				if (b.translations.containsKey(locale)) {
					res.add(b.translations.get(locale));
				} else {
					res.add(b);
				}
			}
			return res;
		}
		return articles;
    }
    

    private synchronized List<BlogArticle> loadBlogArticles(HttpServletRequest request, HttpServletResponse response) {
    	List<BlogArticle> blogs = this.cacheBlogArticles;
    	// double check that it is not retrieved yet
    	if(blogs != null) {
    		return blogs;
    	}
    	blogs = new ArrayList<WebController.BlogArticle>();
		File folder = new File(websiteLocation, "blog_articles");
    	loadArticles(request, response, blogs, folder);
    	Map<String, BlogArticle> articleById = new TreeMap<String, WebController.BlogArticle>(); 
    	for(BlogArticle b : blogs) {
    		articleById.put(b.id, b);
    	}
    	for(String loc : SUPPORTED_LOCALES) {
    		if(!loc.equals("")) {
    			List<BlogArticle> localeBlogs = new ArrayList<WebController.BlogArticle>();
    			loadArticles(request, response, localeBlogs, new File(folder, loc));
    			for(BlogArticle b : localeBlogs) {
    				BlogArticle original = articleById.get(b.id);
    				if(original != null) {
    					if(original.translations.size() == 0) {
    						original.translations = new TreeMap<String, WebController.BlogArticle>();
    					}
    					original.translations.put(loc, b);
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
    	this.cacheBlogArticles = blogs;
		return blogs;
	}


	private void loadArticles(HttpServletRequest request, HttpServletResponse response, List<BlogArticle> blogs,
			File folder) {
		if(!folder.exists()) {
			return;
		}
		Pattern pt = Pattern.compile(" (\\w*)=\\\"([^\\\"]*)\\\"");
    	final IContext ctx = new WebContext(request, response, request.getServletContext());
    	String cssItem = templateEngine.process("pub/rss_item.css.html", ctx);
    	
    	File[] files = folder.listFiles();
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
				    	String rssContent = cssItem + ba.content.toString();
				    	rssContent = rssContent.replace("src=\"/images/", "src=\"https://osmand.net/images/");
				    	rssContent = cutTags(rssContent, "script");
				    	rssContent = cutTags(rssContent, "iframe");
				    	ba.content = new StringBuilder(rssContent);
				    	
						header = header.substring(header.indexOf(">") + 1);
						header = header.substring(0, header.indexOf("</"));
						ba.title = header;
						ba.url = "blog/" + id;
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
}
