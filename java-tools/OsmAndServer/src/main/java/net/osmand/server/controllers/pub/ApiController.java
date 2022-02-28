package net.osmand.server.controllers.pub;

import java.io.File;
import java.io.IOException;
import java.net.URLDecoder;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.sql.DataSource;

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

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import net.osmand.server.api.repo.DataMissingSearchRepository;
import net.osmand.server.api.repo.DataMissingSearchRepository.DataMissingSearchFeedback;
import net.osmand.server.api.repo.DeviceSubscriptionsRepository;
import net.osmand.server.api.repo.DeviceSubscriptionsRepository.SupporterDeviceSubscription;
import net.osmand.server.api.repo.EmailSupportSurveyRepository;
import net.osmand.server.api.repo.EmailSupportSurveyRepository.EmailSupportSurveyFeedback;
import net.osmand.server.api.repo.EmailUnsubscribedRepository;
import net.osmand.server.api.repo.EmailUnsubscribedRepository.EmailUnsubscribed;
import net.osmand.server.api.repo.SupportersRepository;
import net.osmand.server.api.repo.SupportersRepository.Supporter;
import net.osmand.server.api.services.CameraPlace;
import net.osmand.server.api.services.IpLocationService;
import net.osmand.server.api.services.MotdService;
import net.osmand.server.api.services.MotdService.MessageParams;
import net.osmand.server.api.services.PlacesService;
import net.osmand.util.Algorithms;
import net.osmand.server.monitor.OsmAndServerMonitorTasks;

@Controller
@RequestMapping("/api")
public class ApiController {
    protected static final Log LOGGER = LogFactory.getLog(ApiController.class);

    private static final String PROC_FILE = ".proc_timestamp";

    @Value("${osmand.files.location}")
    private String filesLocation;
    
    @Value("${osmand.web.location}")
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
	IpLocationService locationService;

	@Autowired
	DeviceSubscriptionsRepository subscriptionsRepository;
	
	@Autowired
	SupportersRepository supportersRepository;

	@Autowired
	private OsmAndServerMonitorTasks monitoring;

	private ObjectMapper jsonMapper;

    
    private ApiController() {
    	ObjectMapper mapper = new ObjectMapper();
    	this.jsonMapper = mapper;
    }
    

    @GetMapping(path = {"/osmlive_status.php", "/osmlive_status"}, produces = "text/html;charset=UTF-8")
    @ResponseBody
    public FileSystemResource osmLiveStatus() throws IOException  {
        FileSystemResource fsr = new FileSystemResource(new File(filesLocation, PROC_FILE));
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
    	
        return "{\"status\":\"OK\"}";
    }
    
	@GetMapping(path = { "/check_download" }, produces = "text/html;charset=UTF-8")
	@ResponseBody
	public String checkDownload(@RequestParam(value = "file_name", required = false) String fn,
			@RequestParam(value = "file_size", required = false) String sz) throws IOException {
		return "{\"status\":\"OK\"}";
	}
    
	@GetMapping(path = { "/status_server" }, produces = "text/html;charset=UTF-8")
	@ResponseBody
	public String statusServer() throws IOException {
		String refreshAll = monitoring.refreshAll();
		return "<pre>" + refreshAll + "</pre>";
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
        CameraPlace cp = new CameraPlace();
        cp.setKey(photoId);
        placesService.initMapillaryImageUrl(cp);
		if (hires) {
			if (!Algorithms.isEmpty(cp.getImageHiresUrl())) {
				resp.sendRedirect(cp.getImageHiresUrl());
			}
		} else {
			if (!Algorithms.isEmpty(cp.getImageUrl())) {
				resp.sendRedirect(cp.getImageUrl());
			}
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
			} else {
				params.version = version;
			}
		}

		File file = motdService.getSubscriptions(params);
		FileSystemResource fsr = new FileSystemResource(file);
		return fsr;
	}

	@GetMapping(path = { "/subscriptions/get" })
	@ResponseBody
	public String getSubscriptions(
			@RequestParam(required = false) String userId,
			@RequestParam(required = false) String userToken,
			@RequestParam(required = false) String orderId,
			@RequestHeader HttpHeaders headers, HttpServletRequest request) throws IOException, ParseException {
		MessageParams params = new MessageParams();
		params.hostAddress = request.getRemoteAddr();
		if (headers.getFirst("X-Forwarded-For") != null) {
			params.hostAddress = headers.getFirst("X-Forwarded-For");
		}
		if (!Algorithms.isEmpty(userId) && !Algorithms.isEmpty(userToken) && Algorithms.isEmpty(orderId)) {
			Optional<Supporter> sup = supportersRepository.findByUserId(Long.parseLong(userId));
			if (sup.isPresent()) {
				Supporter s = sup.get();
				if (userToken.equals(s.token)) {
					orderId = sup.get().orderId;
				}
			}
		}
		if (!Algorithms.isEmpty(orderId)) {
			List<SupporterDeviceSubscription> subscriptions = subscriptionsRepository.findByOrderId(orderId);
			List<Object> res = new ArrayList<>();
			for (SupporterDeviceSubscription sub : subscriptions) {
				Map<String, String> subMap = new HashMap<>();
				subMap.put("sku", sub.sku);
				if (sub.valid != null) {
					subMap.put("valid", sub.valid.toString());
				}
				String state = "undefined";
				if (sub.starttime != null) {
					subMap.put("start_time", String.valueOf(sub.starttime.getTime()));
				}
				if (sub.expiretime != null) {
					long expireTimeMs = sub.expiretime.getTime();
					int paymentState = sub.paymentstate == null ? 1 : sub.paymentstate.intValue();
					boolean autoRenewing = sub.autorenewing == null ? false : sub.autorenewing.booleanValue();
					if (expireTimeMs > System.currentTimeMillis()) {
						if (paymentState == 1 && autoRenewing) {
							state = "active";
						} else if (paymentState == 1 && !autoRenewing) {
							state = "cancelled";
						} else if (paymentState == 0 && autoRenewing) {
							state = "in_grace_period";
						}
					} else {
						if (paymentState == 0 && autoRenewing) {
							state = "on_hold";
						} else if (paymentState == 1 && autoRenewing) {
							state = "paused";
						} else if (paymentState == 1 && !autoRenewing) {
							state = "expired";
						}
					}
					subMap.put("expire_time", String.valueOf(expireTimeMs));
				}
				subMap.put("state", state);
				res.add(subMap);
			}
			return jsonMapper.writeValueAsString(res);
		}
		return "{}";
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
			} else {
				params.version = version;
			}
		}
		
        HashMap<String, Object> body = motdService.getMessage(params);
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
    public String emailUnsubscribe(@RequestParam(required=false) String id, 
    		@RequestParam(required=false) String email, @RequestParam(required=false) String group) throws IOException  {
    	if(Algorithms.isEmpty(email)) {
    		if(Algorithms.isEmpty(id)) {
    			throw new IllegalArgumentException("Missing email parameter");
    		}
    		email = new String(Base64Utils.decodeFromString(URLDecoder.decode(id, "UTF-8")));
    	}
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
    public String emailSubscribe(@RequestParam(required=false) String id, 
    		@RequestParam(required=false) String email, @RequestParam(required=false) String group) throws IOException  {
		if (Algorithms.isEmpty(email)) {
			if (Algorithms.isEmpty(id)) {
				throw new IllegalArgumentException("Missing email parameter");
			}
			email = new String(Base64Utils.decodeFromString(URLDecoder.decode(id, "UTF-8")));
		}
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
