package net.osmand.server.controllers.pub;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import net.osmand.server.api.repo.DataMissingSearchRepository;
import net.osmand.server.api.repo.DataMissingSearchRepository.DataMissingSearchFeedback;
import net.osmand.server.api.repo.EmailSupportSurveyRepository;
import net.osmand.server.api.repo.EmailSupportSurveyRepository.EmailSupportSurveyFeedback;
import net.osmand.server.api.repo.EmailUnsubscribedRepository;
import net.osmand.server.api.repo.EmailUnsubscribedRepository.EmailUnsubscribed;
import net.osmand.server.api.services.IpLocationService;
import net.osmand.server.api.services.MotdService;
import net.osmand.server.api.services.MotdService.MessageParams;
import net.osmand.server.api.services.PlacesService;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.FileSystemResource;
import org.springframework.http.HttpHeaders;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.util.Base64Utils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.util.UriComponentsBuilder;

import java.io.IOException;
import java.net.URLDecoder;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map.Entry;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.sql.DataSource;

@Controller
@RequestMapping("/api")
public class ApiController {
    protected static final Log LOGGER = LogFactory.getLog(ApiController.class);

    private static final String PROC_FILE = ".proc_timestamp";

    @Value("${files.location}")
    private String filesLocation;
    
    @Value("${web.location}")
    private String websiteLocation;

	@Autowired
	private DataSource dataSource;

    @Autowired
    PlacesService placesService;
    
    @Autowired
    MotdService motdService;

    @Autowired 
    EmailSupportSurveyRepository surveyRepo;
    
    @Autowired 
    EmailUnsubscribedRepository unsubscribedRepo;
    
    @Autowired 
    DataMissingSearchRepository dataMissingSearch;
    
    @Autowired
	private IpLocationService locationService;

	private ObjectMapper jsonMapper;

    
    private ApiController() {
    	ObjectMapper mapper = new ObjectMapper();
    	this.jsonMapper = mapper;
    }
    

    @GetMapping(path = {"/osmlive_status.php", "/osmlive_status"}, produces = "text/html;charset=UTF-8")
    @ResponseBody
    public FileSystemResource osmLiveStatus() throws IOException  {
        String procFile = filesLocation.concat(PROC_FILE);
        FileSystemResource fsr = new FileSystemResource(procFile);
        return fsr;
    }
    
    @GetMapping(path = {"/geo-ip"}, produces = "application/json")
    @ResponseBody
    public String findGeoIP(HttpServletRequest request, @RequestParam(required = false) String ip) throws 
    	JsonParseException, JsonMappingException, IOException{
    	String remoteAddr = request.getRemoteAddr();
    	Enumeration<String> hs = request.getHeaders("X-Forwarded-For");
        if (hs != null && hs.hasMoreElements()) {
            remoteAddr = hs.nextElement();
        }
        if(ip != null && ip.length() > 0) {
        	remoteAddr = ip;
        }
        return locationService.getLocationAsJson(remoteAddr);
    }
    
    public static String extractFirstDoubleNumber(String s) {
		String d = "";
		boolean dot = true;
		for (int k = 0; k < s.length(); k++) {
			char charAt = s.charAt(k);
			if(k == 0 && charAt =='-') {
				d += charAt;
			} else if(charAt == '.' && dot) {
				d += charAt;
				dot = false;
			} else if ((charAt >= '0' && charAt <= '9') ) {
				d += charAt;
			} else {
				break;
			}
		}
		return d;
	}
    
    @PostMapping(path = {"/missing_search"}, produces = "application/json")
    @ResponseBody
    public String missingSearch(HttpServletRequest request, @RequestParam(required = false) String query,
            @RequestParam(required =false) String location) {
    	String remoteAddr = request.getRemoteAddr();
    	Enumeration<String> hs = request.getHeaders("X-Forwarded-For");
        if (hs != null && hs.hasMoreElements()) {
            remoteAddr = hs.nextElement();
        }
        if(query == null && location == null && request.getParameterMap().size() == 1) {
        	Entry<String, String[]> e = request.getParameterMap().entrySet().iterator().next();
        	query = e.getKey();
        	if(e.getValue() != null && e.getValue().length > 0 && e.getValue()[0] != null) { 
        		location = e.getValue()[0].toLowerCase();
        		String lat = null, lon = null;
        		try {
					int i = location.indexOf("latitude=");
					if(i >= 0) {
						lat = ((float)Double.parseDouble(extractFirstDoubleNumber(
								location.substring(i + "latitude=".length())))) + "";
					}
					i = location.indexOf("longitude=");
					if(i >= 0) {
						lon = ((float)Double.parseDouble(extractFirstDoubleNumber(
								location.substring(i + "longitude=".length())))) + "";
					}
				} catch (NumberFormatException e1) {
				}
				if (lat != null && lon != null) {
					location = lat + "," + lon;
				} else {
					location = null;
				}
        	}
        }
        if(query != null) {
        	DataMissingSearchFeedback feedback = new DataMissingSearchFeedback();
        	feedback.ip = remoteAddr;
        	feedback.timestamp = new Date();
        	feedback.search = query;
        	feedback.location = location;
        	dataMissingSearch.save(feedback);
        }
    	
        return "{'status':'OK'}";
    }
    
    
    @GetMapping(path = {"/cm_place.php", "/cm_place"})
	public void getCmPlace(@RequestParam("lat") double lat, @RequestParam("lon") double lon,
			@RequestHeader HttpHeaders headers, HttpServletRequest request, HttpServletResponse response)
			throws JsonProcessingException {
		placesService.processPlacesAround(headers, request, response, jsonMapper, lat, lon);
	}

    @GetMapping(path = {"/mapillary/get_photo.php", "/mapillary/get_photo"})
    @ResponseBody
    public void getPhoto(@RequestParam("photo_id") String photoId,
                         @RequestParam(value = "hires", required = false) boolean hires,
                         HttpServletResponse resp) throws IOException {
        String hiresThumb = "thumb-1024.jpg";
        String thumb = "thumb-640.jpg";
        String cloudFrontUriTemplate = "https://d1cuyjsrcm0gby.cloudfront.net/{photoId}/{thumb}?origin=osmand";
        UriComponentsBuilder uriBuilder = UriComponentsBuilder.fromHttpUrl(cloudFrontUriTemplate);
        resp.setContentType("image/jpeg");
        if (hires) {
            resp.sendRedirect(uriBuilder.buildAndExpand(photoId, hiresThumb).toString());
        } else {
            resp.sendRedirect(uriBuilder.buildAndExpand(photoId, thumb).toString());
        }
    }

    @GetMapping(path = {"/mapillary/photo-viewer.php", "/mapillary/photo-viewer"})
    public String getPhotoViewer(@RequestParam("photo_id") String photoId, Model model) {
        model.addAttribute("photoId", photoId);
        return "mapillary/photo-viewer";
    }

	@GetMapping(path = { "/subscriptions/active" })
	@ResponseBody
	public FileSystemResource getActiveSubscriptions(@RequestParam(required = false) String version,
			@RequestParam(required = false) String androidPackage, 
			@RequestParam(required = false) Integer nd,
			@RequestParam(required = false) Integer ns, 
			@RequestParam(required = false) String lang,
			@RequestParam(required = false) String os, 
			@RequestParam(required = false) String aid,
			@RequestHeader HttpHeaders headers, HttpServletRequest request) throws IOException, ParseException {
	MessageParams params = new MessageParams();
		params.hostAddress = request.getRemoteAddr();
		if (headers.getFirst("X-Forwarded-For") != null) {
			params.hostAddress = headers.getFirst("X-Forwarded-For");
		}
		params.os = os;
		params.lang = lang;
		params.numberOfDays = nd;
		params.numberOfStarts = ns;
		params.appVersion = "";
		params.appPackage = androidPackage;
		if (version != null) {
			int i = version.indexOf(" ");
			if (i >= 0) {
				params.appVersion = version.substring(0, i);
				params.version = version.substring(i + 1);
			}
		}

		String file = motdService.getSubscriptions(params);
		FileSystemResource fsr = new FileSystemResource(file);
		return fsr;
	}
    
    @GetMapping(path = {"/motd", "/motd.php"})
    @ResponseBody
    public String getMessage(@RequestParam(required = false) String version,
                             @RequestParam(required = false) Integer nd,
                             @RequestParam(required = false) Integer ns,
                             @RequestParam(required = false) String lang,
                             @RequestParam(required = false) String os,
                             @RequestParam(required = false) String aid,
                             @RequestParam(required = false) String discount,
                             @RequestHeader HttpHeaders headers,
                             HttpServletRequest request) throws IOException, ParseException {
    	MessageParams params = new MessageParams();
    	params.hostAddress = request.getRemoteAddr();
        if (headers.getFirst("X-Forwarded-For") != null) {
        	params.hostAddress = headers.getFirst("X-Forwarded-For");
        }
		params.numberOfDays = nd;
		params.numberOfStarts = ns;
		params.appVersion = "";
		params.os = os;
		params.lang = lang;
		if (version != null) {
			int i = version.indexOf(" ");
			if (i >= 0) {
				params.appVersion = version.substring(0, i);
				params.version = version.substring(i + 1);
			}
		}
		
        HashMap<String,Object> body = motdService.getMessage(params);
        if (body != null) {
            return jsonMapper.writeValueAsString(body);
        }
        return "{}";
    }
    
    
    @GetMapping(path = {"/email/support_survey"})
    public String emailSupportSurvey(@RequestHeader HttpHeaders headers,
            HttpServletRequest request, @RequestParam(required=false) String response, Model model) throws IOException  {
    	String remoteAddr = request.getRemoteAddr();
    	Enumeration<String> hs = request.getHeaders("X-Forwarded-For");
        if (hs != null && hs.hasMoreElements()) {
            remoteAddr = hs.nextElement();
        }
        if(response == null) {
        	response = "good";
        } else {
        	EmailSupportSurveyFeedback feedback = new EmailSupportSurveyRepository.EmailSupportSurveyFeedback();
        	feedback.ip = remoteAddr;
        	feedback.timestamp = new Date();
        	feedback.response = response;
        	surveyRepo.save(feedback);
        }
        model.addAttribute("response", response); 
    	return "pub/email/survey";
    }
    
    @GetMapping(path = {"/email/unsubscribe"}, produces = "text/html;charset=UTF-8")
    public String emailUnsubscribe(@RequestParam(required=true) String id, @RequestParam(required=false) String group) throws IOException  {
		String email = new String(Base64Utils.decodeFromString(URLDecoder.decode(id, "UTF-8")));
    	EmailUnsubscribed ent = new EmailUnsubscribedRepository.EmailUnsubscribed();
    	ent.timestamp = new Date();
    	if(group == null) {
    		group = "all";
    	}
    	ent.channel = group;
    	ent.email = email;
    	unsubscribedRepo.save(ent);
    	return "pub/email/unsubscribe";
    	
    }
    
    @GetMapping(path = {"/email/subscribe"}, produces = "text/html;charset=UTF-8")
    public String emailSubscribe(@RequestParam(required=true) String id, @RequestParam(required=false) String group) throws IOException  {
		String email = new String(Base64Utils.decodeFromString(URLDecoder.decode(id, "UTF-8")));
    	unsubscribedRepo.deleteAllByEmail(email);
    	return "pub/email/subscribe";
    }

	@PostMapping(path = {"/submit_analytics"}, consumes = {"multipart/form-data"})
	@ResponseBody
	public String submitAnalytics(HttpServletRequest request,
								  @RequestParam() Long startDate,
								  @RequestParam() Long finishDate,
								  @RequestParam() Integer nd,
								  @RequestParam() Integer ns,
								  @RequestParam() String aid,
								  @RequestParam() String version,
								  @RequestParam() String lang,
								  @RequestParam() MultipartFile file) throws IOException, SQLException {
		String remoteAddr = request.getRemoteAddr();
		Enumeration<String> hs = request.getHeaders("X-Forwarded-For");
		if (hs != null && hs.hasMoreElements()) {
			remoteAddr = hs.nextElement();
		}
		if (!file.isEmpty()) {
			Connection conn = DataSourceUtils.getConnection(dataSource);
			try {
				PreparedStatement p = conn.prepareStatement(
						"insert into analytics " +
								"(ip, date, aid, nd, ns, version, lang, start_date, finish_date, data) " +
								"values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
				p.setString(1, remoteAddr);
				p.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
				p.setString(3, aid);
				p.setInt(4, nd);
				p.setInt(5, ns);
				p.setString(6, version);
				p.setString(7, lang);
				p.setTimestamp(8, new Timestamp(startDate));
				p.setTimestamp(9, new Timestamp(finishDate));
				p.setBinaryStream(10, file.getInputStream());
				p.executeUpdate();
			} finally {
				DataSourceUtils.releaseConnection(conn, dataSource);
			}
		} else {
			throw new IllegalArgumentException("File is empty");
		}
		return "OK";
	}
}
