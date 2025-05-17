package net.osmand.server.controllers.pub;

import java.io.File;
import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import javax.sql.DataSource;

import net.osmand.server.api.repo.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.FileSystemResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import java.util.Base64;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.multipart.MultipartFile;

import com.google.gson.Gson;

import net.osmand.map.OsmandRegions;
import net.osmand.server.api.repo.DataMissingSearchRepository.DataMissingSearchFeedback;
import net.osmand.server.api.repo.DeviceSubscriptionsRepository.SupporterDeviceSubscription;
import net.osmand.server.api.repo.EmailSupportSurveyRepository.EmailSupportSurveyFeedback;
import net.osmand.server.api.repo.EmailUnsubscribedRepository.EmailUnsubscribed;
import net.osmand.server.api.repo.LotteryUsersRepository.LotteryUser;
import net.osmand.server.api.repo.SupportersRepository.Supporter;
import net.osmand.server.api.services.CameraPlace;
import net.osmand.server.api.services.IpLocationService;
import net.osmand.server.api.services.LotteryPlayService;
import net.osmand.server.api.services.LotteryPlayService.LotteryResult;
import net.osmand.server.api.services.MotdService;
import net.osmand.server.api.services.MotdService.MessageParams;
import net.osmand.server.api.services.PlacesService;
import net.osmand.server.api.services.PluginsService;
import net.osmand.server.api.services.PollsService;
import net.osmand.server.api.services.PollsService.PollQuestion;
import net.osmand.server.api.services.PromoService;
import net.osmand.server.api.services.WikiService;
import net.osmand.server.monitor.OsmAndServerMonitorTasks;
import net.osmand.util.Algorithms;

import static net.osmand.server.api.repo.DeviceInAppPurchasesRepository.*;
import static net.osmand.server.api.repo.CloudUserDevicesRepository.*;
import static net.osmand.server.api.repo.CloudUsersRepository.*;

@Controller
@RequestMapping("/api")
public class ApiController {
    protected static final Log LOGGER = LogFactory.getLog(ApiController.class);

    private static final String PROC_FILE = ".proc_timestamp";

    @Value("${osmand.files.location}")
    private String filesLocation;


	@Autowired
	private DataSource dataSource;

    @Autowired
    PlacesService placesService;

	@Autowired
	WikiService wikiService;

    @Autowired
    MotdService motdService;

    @Autowired
    PollsService pollsService;

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
    private DeviceInAppPurchasesRepository iapsRepository;

    @Autowired
    private CloudUsersRepository usersRepository;

    @Autowired
    private CloudUserDevicesRepository userDevicesRepository;

    @Autowired
	OsmAndServerMonitorTasks monitoring;

	@Autowired
	LotteryPlayService lotteryPlayService;

	@Autowired
	PluginsService pluginsService;

	@Autowired
	PromoService promoService;

	Gson gson = new Gson();

	OsmandRegions osmandRegions;

    @GetMapping(path = {"/osmlive_status.php", "/osmlive_status"}, produces = "text/html;charset=UTF-8")
    @ResponseBody
    public FileSystemResource osmLiveStatus() throws IOException  {
        FileSystemResource fsr = new FileSystemResource(new File(filesLocation, PROC_FILE));
        return fsr;
    }

    @GetMapping(path = {"/geo-ip"}, produces = "application/json")
    @ResponseBody
    public String findGeoIP(HttpServletRequest request, @RequestParam(required = false) String ip) throws IOException{
		String remoteAddr = request.getRemoteAddr();
		Enumeration<String> hs = request.getHeaders("X-Forwarded-For");
		if (hs != null && hs.hasMoreElements()) {
			remoteAddr = hs.nextElement();
		}
		if (ip != null && ip.length() > 0) {
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

	private String pollResult(PollQuestion pq) {
		Map<String, Integer> res = new LinkedHashMap<String, Integer>();
		if (pq != null) {
			for (int i = 0; i < pq.votes.size(); i++) {
				res.put(pq.id + "_" + i, pq.votes.get(i));
			}
		}
		return gson.toJson(res);
	}

	@PostMapping(path = { "/poll-submit" }, produces = "application/json")
	@ResponseBody
	public String pollSubmit(HttpServletRequest request, @RequestParam(required = true) String pollId, @RequestParam(required = true) String ansId) {
		String remoteAddr = request.getRemoteAddr();
    	Enumeration<String> hs = request.getHeaders("X-Forwarded-For");
        if (hs != null && hs.hasMoreElements()) {
            remoteAddr = hs.nextElement();
        }
		PollQuestion pq = pollsService.getPollById(pollId);
		if (pq != null) {
			pollsService.submitVote(remoteAddr, pq, Integer.parseInt(ansId));
		}
		return pollResult(pq);
	}


	@GetMapping(path = {"/regions-by-latlon"})
	@ResponseBody
	public String getRegionsByLatlon(@RequestParam("lat") double lat, @RequestParam("lon") double lon) throws IOException {
		List<String> regions = new ArrayList<String>();
		if(osmandRegions == null) {
			osmandRegions = new OsmandRegions();
			osmandRegions.prepareFile();
		}
		regions = osmandRegions.getRegionsToDownload(lat, lon, regions);
		return gson.toJson(Map.of("regions", regions));
	}

	@GetMapping(path = { "/poll-results" }, produces = "application/json")
	@ResponseBody
	public String pollResults(@RequestParam(required = true) String pollId) {
		return pollResult(pollsService.getPollById(pollId));
	}

	@GetMapping(path = { "/latest-poll" }, produces = "application/json")
	@ResponseBody
	public String pollDetails(@RequestParam(required = false, defaultValue = "website" ) String channel,
			@RequestParam(required = false) String pollId ) {
		if(pollId != null) {
			return gson.toJson(pollsService.getPollByIdResult(pollId));
		}
		return gson.toJson(pollsService.getPoll(channel));
	}


	@RequestMapping(path = { "/purchase-complete" }, produces = "application/json")
	@ResponseBody
	public String purchaseComplete(@RequestParam(required = false) String purchaseId,
			@RequestParam(required = false) String os, @RequestParam(required = false) String purchaseType) {
		// here we could validate purchase in fiture
		return "{\"status\":\"OK\"}";
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

    @GetMapping(path = {"/cm_place", "/cm_place"})
	public void getCmPlace(@RequestParam("lat") double lat, @RequestParam("lon") double lon,
			@RequestHeader HttpHeaders headers, HttpServletRequest request, HttpServletResponse response) {
		placesService.processPlacesAround(headers, request, response, gson, lat, lon);
	}

    // /api/wiki_place?category=Sora%20(Italy)
    // /api/wiki_place?article=Q51838
    @GetMapping(path = {"/wiki_place"})
    @ResponseBody
    public String geWikiPlace(@RequestParam(required = false) String article,
                              @RequestParam(required = false) String category,
                              @RequestParam(required = false) String wiki,
                              @RequestParam(required = false, defaultValue = "false") boolean addMetaData) {
	    if (addMetaData) {
		    Set<Map<String, Object>> imagesWithDetails = wikiService.processWikiImagesWithDetails(article, category, wiki);
		    return gson.toJson(Collections.singletonMap("features-v2", imagesWithDetails));
	    }
	    Set<String> images = wikiService.processWikiImages(article, category, wiki);
	    return gson.toJson(Collections.singletonMap("features", images));
    }

    @GetMapping(path = {"/mapillary/get_photo"})
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

    @GetMapping(path = {"/mapillary/photo-viewer"})
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
            @RequestParam(required = false) String deviceId,
            @RequestParam(required = false) String accessToken,
			@RequestParam(required = false) String orderId,
			@RequestHeader HttpHeaders headers, HttpServletRequest request) throws IOException, ParseException {
		MessageParams params = new MessageParams();
		params.hostAddress = request.getRemoteAddr();
		if (headers.getFirst("X-Forwarded-For") != null) {
			params.hostAddress = headers.getFirst("X-Forwarded-For");
		}
		if (!Algorithms.isEmpty(userId) && !Algorithms.isEmpty(userToken) && Algorithms.isEmpty(orderId)) {
            Supporter sup = resolveSupporter(userId, userToken);
			if (sup != null) {
                orderId = sup.orderId;
			}
		}
		if (!Algorithms.isEmpty(deviceId) && !Algorithms.isEmpty(accessToken) && Algorithms.isEmpty(orderId)) {
            CloudUser pu = resolveUser(deviceId, accessToken);
			if (pu != null) {
                orderId = pu.orderid;
			}
		}
		if (!Algorithms.isEmpty(orderId)) {
			List<SupporterDeviceSubscription> subscriptions = subscriptionsRepository.findByOrderId(orderId);
			List<Object> res = new ArrayList<>();
			for (SupporterDeviceSubscription sub : subscriptions) {
				Map<String, String> subMap = getSubscriptionInfo(sub);
				if (!subMap.isEmpty()) {
					res.add(subMap);
				}
			}
			return gson.toJson(res);
		}
		return "{}";
	}

	@NotNull
	private static Map<String, String> getSubscriptionInfo(SupporterDeviceSubscription sub) {
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
		return subMap;
	}

	@GetMapping(path = {"/subscriptions/getAll"})
	@ResponseBody
	public String getAllActiveSubscriptions(
			@RequestParam(required = false) String deviceId,
			@RequestParam(required = false) String accessToken,
			@RequestHeader HttpHeaders headers, HttpServletRequest request) {
		MessageParams params = new MessageParams();
		params.hostAddress = request.getRemoteAddr();
		if (headers.getFirst("X-Forwarded-For") != null) {
			params.hostAddress = headers.getFirst("X-Forwarded-For");
		}
		CloudUser pu = resolveUser(deviceId, accessToken);
		if (pu != null) {
			List<SupporterDeviceSubscription> subscriptions = subscriptionsRepository.findAllByUserId(pu.id);
			List<SupporterDeviceSubscription> activeSubscriptions = new ArrayList<>();
			subscriptions.stream()
					.filter(s -> s.valid && s.expiretime != null && s.expiretime.getTime() > System.currentTimeMillis())
					.forEach(activeSubscriptions::add);

			List<Object> res = new ArrayList<>();
			for (SupporterDeviceSubscription sub : activeSubscriptions) {
				Map<String, String> subMap = getSubscriptionInfo(sub);
				if (!subMap.isEmpty()) {
					res.add(subMap);
				}
			}
			return gson.toJson(res);
		}
		return "{}";
	}

    @GetMapping(path = { "/inapps/get" })
    @ResponseBody
    public String getInAppPurchases(
            @RequestParam(required = false) String userId,
            @RequestParam(required = false) String userToken,
            @RequestParam(required = false) String deviceId,
            @RequestParam(required = false) String accessToken,
            @RequestHeader HttpHeaders headers, HttpServletRequest request) {

        CloudUser pu = resolveUser(deviceId, accessToken);
        Supporter sup = resolveSupporter(userId, userToken);
        if (pu == null && sup == null) {
            return "{}";
        }

        List<SupporterDeviceInAppPurchase> validIaps = pu != null
                ? iapsRepository.findByUserIdAndValidTrue(pu.id)
                : iapsRepository.findBySupporterIdAndValidTrue(sup.userId);

        List<Map<String, String>> iapResults = new ArrayList<>();
        for (SupporterDeviceInAppPurchase iap : validIaps) {
            Map<String, String> iapMap = new HashMap<>();
            iapMap.put("sku", iap.sku);
            if (iap.platform != null) {
                iapMap.put("platform", iap.platform);
            }
            if (iap.purchaseTime != null) {
                iapMap.put("purchaseTime", String.valueOf(iap.purchaseTime.getTime()));
            }
            iapResults.add(iapMap);
        }
        return gson.toJson(iapResults);
    }

    private Supporter resolveSupporter(String userId, String userToken) {
        if (!Algorithms.isEmpty(userId) && !Algorithms.isEmpty(userToken)) {
            Optional<Supporter> sup = supportersRepository.findByUserId(Long.parseLong(userId));
            Supporter supporter = sup.orElse(null);
            if (supporter != null && userToken.equals(sup.get().token)) {
                return supporter;
            }
        }
        return null;
    }

    private CloudUser resolveUser(String deviceId, String accessToken) {
        if (Algorithms.isEmpty(deviceId) || Algorithms.isEmpty(accessToken)) {
            return null;
        }
        int devId;
        try {
            devId = Integer.parseInt(deviceId);
        } catch (NumberFormatException e) {
            return null;
        }
        CloudUserDevice device = userDevicesRepository.findById(devId);
        if (device != null && Algorithms.stringsEqual(device.accesstoken, accessToken)) {
            return usersRepository.findById(device.userid);
        }
        return null;
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

        Map<String, Object> body = motdService.getMessage(params);
        if (body != null) {
            return gson.toJson(body);
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
    		@RequestParam(required=false) String email, @RequestParam(required=false) String group) {
    	if(Algorithms.isEmpty(email)) {
    		if(Algorithms.isEmpty(id)) {
    			throw new IllegalArgumentException("Missing email parameter");
    		}
    		email = new String(Base64.getDecoder().decode(URLDecoder.decode(id, StandardCharsets.UTF_8)));
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
    		@RequestParam(required=false) String email, @RequestParam(required=false) String group) {
		if (Algorithms.isEmpty(email)) {
			if (Algorithms.isEmpty(id)) {
				throw new IllegalArgumentException("Missing email parameter");
			}
			email = new String(Base64.getDecoder().decode(URLDecoder.decode(id, StandardCharsets.UTF_8)));
		}
    	unsubscribedRepo.deleteAllByEmailIgnoreCase(email);
    	return "pub/email/subscribe";
    }

	@PostMapping(path = {"/submit_analytics"}, consumes = {"multipart/form-data"})
	@ResponseBody
	public String submitAnalytics(HttpServletRequest request,
								  @RequestParam() Long startDate,
								  @RequestParam() Long finishDate,
								  @RequestParam() Integer nd,
								  @RequestParam() Integer ns,
								  @RequestParam(required = false) String aid,
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

	/// LOTTERY
	@GetMapping(path = {"/giveaway-series"}, produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public String series(HttpServletRequest request) throws IOException {
		return gson.toJson(lotteryPlayService.series());
	}


	@PostMapping(path = {"/giveaway-subscribe" }, produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public String registerEmail(HttpServletRequest request, @RequestParam(required=false) String aid,
			@RequestParam(required = true) String email, @RequestParam(required = true) String os) {
		return gson.toJson(lotteryPlayService.subscribeToGiveaways(request, aid, email, os));
	}

	@GetMapping(path = {"/giveaway"}, produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public String index(HttpServletRequest request, @RequestParam(required = false) String email,
			@RequestParam(required = true) String series) throws IOException {
		LotteryResult res = new LotteryResult();
		res.series = series;
		res.message = "";
		if (!Algorithms.isEmpty(email)) {
			String remoteAddr = request.getRemoteAddr();
	    	Enumeration<String> hs = request.getHeaders("X-Forwarded-For");
	        if (hs != null && hs.hasMoreElements()) {
	            remoteAddr = hs.nextElement();
	        }
			LotteryUser user = lotteryPlayService.participate(remoteAddr, email, series);
			if (user != null) {
				res.user = user.hashcode;
				res.message = user.message;
			}
		}
		lotteryPlayService.fillSeriesDetails(res);
		return gson.toJson(res);
	}


	@PostMapping(path = {"/giveaway-run"}, produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
	public String runLottery(@RequestParam(required = true) String latestHash) throws IOException {
		return gson.toJson(lotteryPlayService.runLottery(latestHash));
    }

	@PostMapping(path = {"/promo-add-user"})
	public ResponseEntity<String> addUser(@RequestParam String promoKey,
	                                      @RequestParam String userEmail) {
		synchronized (this) {
			return promoService.addUser(promoKey, userEmail);
		}
	}


	@GetMapping(path = {"/plugins/list"}, produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public String pluginsList(@RequestParam(required = false) String version,
			@RequestParam(required = false) boolean nightly, @RequestParam(required = false) String os) throws IOException {
		return gson.toJson(pluginsService.getPluginsInfo(os, version, nightly));
	}
}
