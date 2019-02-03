package net.osmand.server.controllers.pub;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonObject;

import net.osmand.server.api.repo.MapUserRepository;
import net.osmand.server.api.repo.MapUserRepository.MapUser;
import net.osmand.server.api.repo.OsmRecipientsRepository;
import net.osmand.server.api.repo.OsmRecipientsRepository.OsmRecipient;
import net.osmand.server.api.repo.SupportersDeviceSubscriptionRepository;
import net.osmand.server.api.repo.SupportersDeviceSubscriptionRepository.SupporterDeviceSubscription;
import net.osmand.server.api.repo.SupportersDeviceSubscriptionRepository.SupporterDeviceSubscriptionPrimaryKey;
import net.osmand.server.api.repo.SupportersRepository;
import net.osmand.server.api.repo.SupportersRepository.Supporter;
import net.osmand.server.api.services.ReceiptValidationService;
import net.osmand.server.api.services.ReceiptValidationService.InAppReceipt;
import net.osmand.server.utils.BTCAddrValidator;
import net.osmand.util.Algorithms;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

import java.util.Arrays;
import java.util.Base64;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

import javax.servlet.http.HttpServletRequest;

import static net.osmand.server.api.services.ReceiptValidationService.NO_SUBSCRIPTIONS_FOUND_STATUS;
import static net.osmand.server.api.services.ReceiptValidationService.USER_NOT_FOUND_STATUS;

@RestController
@RequestMapping("/subscription")
public class SubscriptionController {
    private static final Log LOGGER = LogFactory.getLog(SubscriptionController.class);

    private static final int TIMEOUT = 20000;

    @Autowired
    private SupportersRepository supportersRepository;
    
    @Autowired
    private MapUserRepository mapUserRepository;
    
    @Autowired
    private OsmRecipientsRepository osmRecipientsRepository;
    
    @Autowired
    private SupportersDeviceSubscriptionRepository subscriptionsRepository;
    
    @Autowired
    private ReceiptValidationService validationService;
    
    @Autowired
    private StringRedisTemplate redisTemplate;
    
    private ObjectMapper jsonMapper = new ObjectMapper();


    private final RestTemplate restTemplate;

    @Autowired
    public SubscriptionController(RestTemplateBuilder builder) {
        this.restTemplate = builder.setConnectTimeout(TIMEOUT).setReadTimeout(TIMEOUT).build();
    }

    private String encodeCredentialsToBase64(String userName, String password) {
        Base64.Encoder encoder = Base64.getMimeEncoder();
        byte[] credentials = userName.concat(":").concat(password).getBytes();
        return encoder.encodeToString(credentials);
    }

    private String processOsmUsername(String userName) {
        int ind = userName.indexOf('\'');
        if (ind > -1) {
            return userName.substring(0, ind).substring(ind + 1);
        }
        return userName;
    }

    private HttpHeaders buildHeaders(String credentials) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);
        headers.set(HttpHeaders.AUTHORIZATION, String.format("Basic %s", credentials));
        return headers;
    }

    private void authenticateUser(String credentials) {
        HttpHeaders headers = buildHeaders(credentials);
        makeRequest(headers);
    }

    private void makeRequest(HttpHeaders headers) {
        HttpEntity<String> requestEntity = new HttpEntity<>(null, headers);
        restTemplate.exchange("https://api.openstreetmap.org/api/0.6/user/details",
                HttpMethod.GET, requestEntity, String.class);
    }

    private ResponseEntity<String> ok(String body, Object... args) {
		return ResponseEntity.ok().body(String.format(body, args));
	}
    
    private ResponseEntity<String> error(String txt) {
    	// clients don't accept error requests (neither mobile, neither http)
    	return ResponseEntity.ok().body(String.format("{\"error\": \"%s.\"}", txt.replace('"', '\'')));
	}
    
    @PostMapping(path = {"/register_email", "/register_email.php"},
            consumes = MediaType.APPLICATION_FORM_URLENCODED_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> registerEmail(HttpServletRequest request) {
        MapUser mapUser = new MapUser();
        mapUser.aid = request.getParameter("aid");
        String remoteAddr = request.getRemoteAddr();
    	Enumeration<String> hs = request.getHeaders("X-Forwarded-For");
        if (hs != null && hs.hasMoreElements()) {
            remoteAddr = hs.nextElement();
        }
        if(Algorithms.isEmpty(mapUser.aid)) {
        	mapUser.aid = remoteAddr;
        }
        mapUser.email = request.getParameter("email");
        if(!validateEmail(mapUser.email)) {
        	throw new IllegalStateException(String.format("Email '%s' is not valid.", mapUser.email));
        }
        mapUser.os = request.getParameter("os");
        mapUser.updateTime = new Date();
        mapUser = mapUserRepository.save(mapUser);
        return ok("{\"email\": \"%s\", \"time\": \"%d\"}", mapUser.email, mapUser.updateTime.getTime());
    }
    
	private boolean validateEmail(String email) {
		if(Algorithms.isEmpty(email)) {
			return false;
		}
		if(!email.contains("@")) {
			return false;
		}
		return true;
	}

	private String userInfoAsJson(Supporter s) {
		String response = String.format(
				"{\"userid\": \"%d\", \"token\": \"%s\", \"visibleName\": \"%s\", \"email\": \"%s\", "
						+ "\"preferredCountry\": \"%s\"}", 
				s.userId, s.token, s.visibleName, s.userEmail, s.preferredRegion);
		return response;
	}

	private Map<String, String> userInfoAsMap(Supporter s) {
    	Map<String, String> res = new HashMap<>();
    	res.put("userid", "" + s.userId);
		res.put("token", s.token);
		res.put("visibleName", s.visibleName);
		res.put("email", s.userEmail);
		res.put("preferredCountry", s.preferredRegion);
		return res;
	}

	private String userShortInfoAsJson(Supporter s) {
		String response = String.format(
				"{\"userid\": \"%d\", \"visibleName\": \"%s\",\"preferredCountry\": \"%s\"}", 
				s.userId, s.visibleName, s.preferredRegion);
		return response;
	}

    

	@PostMapping(path = {"/register", "/register.php"},
        consumes =  MediaType.APPLICATION_FORM_URLENCODED_VALUE,
        produces =  MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> register(HttpServletRequest request) {
        String email = request.getParameter("email");
        String visibleName = request.getParameter("visibleName");
        String preferredCountry = request.getParameter("preferredCountry");
        // no email validation cause client doesn't provide it 99%
        // avoid all nulls, empty, none
        boolean emailValid = email != null && email.contains("@");
        if(emailValid) {
        	Optional<Supporter> optionalSupporter = supportersRepository.findByUserEmail(email);
        	if (optionalSupporter.isPresent()) {
        		Supporter supporter = optionalSupporter.get();
        		return ResponseEntity.ok(userInfoAsJson(supporter));
        	}
        }
        ThreadLocalRandom tlr = ThreadLocalRandom.current();
        int token = tlr.nextInt(100000, 1000000);
        Supporter supporter = new Supporter();
        supporter.userId = null;
        supporter.os = request.getParameter("os");
        supporter.token = String.valueOf(token);
        supporter.visibleName = visibleName;
        supporter.userEmail = email;
        supporter.preferredRegion = preferredCountry;
        supporter = supportersRepository.saveAndFlush(supporter);
        return ResponseEntity.ok(userInfoAsJson(supporter));
    }

    @PostMapping(path = {"/update", "/update.php"},
            consumes =  MediaType.APPLICATION_FORM_URLENCODED_VALUE,
            produces =  MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> update(HttpServletRequest request) {
    	String userId = request.getParameter("userid");
    	if (isEmpty(userId)) {
        	return error("Please validate user id.");
        }
    	String token = request.getParameter("token");
    	if (isEmpty(token)) {
        	return error("Token is not present: fix will be in OsmAnd 3.2");
        }
        Optional<Supporter> sup = supportersRepository.findById(Long.parseLong(userId));
        if(!sup.isPresent() ) {
        	return error("Couldn't find your user id: " + userId);
        }
        Supporter supporter = sup.get();
        if(!token.equals(supporter.token)) {
        	return error("Couldn't validate the token: " + token);
        }
        supporter.visibleName = request.getParameter("visibleName");
        supporter.userEmail = request.getParameter("email");
        supporter.preferredRegion = request.getParameter("preferredCountry");
        supporter = supportersRepository.save(supporter);
        return ResponseEntity.ok(userInfoAsJson(supporter));
    }

    @PostMapping(path = {"/register_osm", "/register_osm.php"},
            consumes =  MediaType.APPLICATION_FORM_URLENCODED_VALUE,
            produces =  MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> registerOsm(HttpServletRequest request) {
        String bitcoinAddress = request.getParameter("bitcoin_addr");
        // empty means deleted (keep it)
        if (bitcoinAddress != null && bitcoinAddress.length() > 0 && 
        		!BTCAddrValidator.validate(bitcoinAddress) ) {
        	return error("Please validate bitcoin address.");
        }
        String osmUser = request.getParameter("osm_usr");
        if (isEmpty(osmUser)) {
        	return error("Please validate osm user.");
        }
        String osmPassword = request.getParameter("osm_pwd");
        if (isEmpty(osmPassword)) {
        	return error("Please validate osm pwd.");
        }
        String email = request.getParameter("email");
        if (isEmpty(email) || !email.contains("@")) {
        	return error("Please validate your email address.");
        }
        String username = processOsmUsername(osmUser);
        String credentials = encodeCredentialsToBase64(username, osmPassword);
        try {
            authenticateUser(credentials);
        } catch (Exception ex) {
            return error("Please validate osm user/pwd (couldn't authenticate): " + ex.getMessage());
        }
        OsmRecipient recipient = new OsmRecipient();
        recipient.osmId = osmUser;
        recipient.email = email;
        recipient.bitcoinAddress = bitcoinAddress;
        recipient.updateTime = new Date();
        recipient = osmRecipientsRepository.save(recipient);
        String response = String.format("{\"osm_user\": \"%s\", \"bitcoin_addr\": \"%s\", \"time\": \"%s\"}",
                recipient.osmId, recipient.bitcoinAddress, recipient.updateTime.getTime()+"");
        return ResponseEntity.ok(response);
    }

    private boolean isEmpty(String s) {
		return s == null || s.length() == 0;
	}
    
	@PostMapping(path = { "/ios-receipt-validate" })
	public ResponseEntity<String> validateIos(HttpServletRequest request) throws Exception {
		String receipt = request.getParameter("receipt");
		JsonObject receiptObj = validationService.loadReceiptJsonObject(receipt, false);
		if (receiptObj != null) {
			String userId = request.getParameter("userid");
			long uId = -1;
			Map<String, Object> result = new HashMap<>();
			Map<String, InAppReceipt> inAppReceipts = validationService.loadInAppReceipts(receiptObj);
			if (inAppReceipts != null) {
				if (inAppReceipts.size() == 0) {
					result.put("result", false);
					result.put("status", NO_SUBSCRIPTIONS_FOUND_STATUS);
					return ResponseEntity.ok(jsonMapper.writeValueAsString(result));
				} else {
					if (Algorithms.isEmpty(userId)) {
						Supporter s = restoreUserIdByPurchaseToken(result, inAppReceipts);
						if (s != null) {
							uId = s.userId;
						}
					} else {
						uId = Long.valueOf(userId);
					}

					if (uId == -1) {
						result.put("result", false);
						result.put("status", USER_NOT_FOUND_STATUS);
						return ResponseEntity.ok(jsonMapper.writeValueAsString(result));
					} else {
						// update existing subscription payload
						for (InAppReceipt r : inAppReceipts.values()) {
							if (r.isSubscription()) {
								Optional<SupporterDeviceSubscription> subscription =
										subscriptionsRepository.findTopByUserIdAndSkuOrderByTimestampDesc(uId, r.getProductId());
								if (subscription.isPresent()) {
									SupporterDeviceSubscription s = subscription.get();
									s.payload = receipt;
									subscriptionsRepository.saveAndFlush(s);
								}
							}
						}
					}

					Map<String, Object> validationResult = validationService.validateReceipt(receiptObj);
					result.putAll(validationResult);

					return ResponseEntity.ok(jsonMapper.writeValueAsString(result));
				}
			}
		}
		return error("Cannot load receipt.");
	}

	private Supporter restoreUserIdByPurchaseToken(Map<String, Object> result, Map<String, InAppReceipt> inAppReceipts) {
		Optional<SupporterDeviceSubscription> subscriptionOpt = subscriptionsRepository
				.findByPurchaseTokenIn(inAppReceipts.keySet());
		if (subscriptionOpt.isPresent()) {
			SupporterDeviceSubscription subscription = subscriptionOpt.get();
			Optional<Supporter> supporter = supportersRepository.findById(subscription.userId);
			if (supporter.isPresent()) {
				Supporter s = supporter.get();
				result.put("user", userInfoAsMap(s));
				return s;
			}
		}
		return null;
	}

	@PostMapping(path = {"/purchased", "/purchased.php"})
	public ResponseEntity<String> purchased(HttpServletRequest request) {
		SupporterUser suser = new SupporterUser();
		ResponseEntity<String> error = validateUserId(request, suser);
		if (error != null) {
			return error;
		}
		SupporterDeviceSubscription subscr = new SupporterDeviceSubscription();
		subscr.userId = suser.supporter.userId;
		subscr.sku = request.getParameter("sku");
		String payload = request.getParameter("payload");
		if (payload != null) {
			subscr.payload = payload;
		}
		subscr.purchaseToken = request.getParameter("purchaseToken");
		subscr.timestamp = new Date();
		Optional<SupporterDeviceSubscription> subscrOpt = subscriptionsRepository.findById(
						new SupporterDeviceSubscriptionPrimaryKey(subscr.userId, subscr.sku, subscr.purchaseToken));
		if (subscrOpt.isPresent()) {
			if (subscr.payload != null) {
				SupporterDeviceSubscription deviceSubscription = subscrOpt.get();
				deviceSubscription.payload = subscr.payload;
				subscriptionsRepository.save(deviceSubscription);
			}
			return ResponseEntity.ok("{ \"res\" : \"OK\" }");
		}
		subscriptionsRepository.save(subscr);
		return ResponseEntity.ok(!suser.tokenValid ? userShortInfoAsJson(suser.supporter)
				: userInfoAsJson(suser.supporter));
	}
	
	private class SupporterUser {
		Supporter supporter = null;
		boolean tokenValid = true;
	}

	private ResponseEntity<String> validateUserId(HttpServletRequest request, SupporterUser suser) {
		String userId = request.getParameter("userid");

		if (isEmpty(userId)) {
			return error("Please validate user id.");
		}
		String token = request.getParameter("token");
		if (isEmpty(token)) {
			suser.tokenValid = false;
			LOGGER.warn("Token was not provided: " + toString(request.getParameterMap()));
			// return error("Token is not present: fix will be in OsmAnd 3.2");
		}
		Optional<Supporter> sup = supportersRepository.findById(Long.parseLong(userId));
		if (!sup.isPresent()) {
			return error("Couldn't find your user id: " + userId);
		}
		Supporter supporter = sup.get();

		if (token != null && !token.equals(supporter.token)) {
			suser.tokenValid = false;
			LOGGER.warn("Token failed validation: " + toString(request.getParameterMap()));
			// return error("Couldn't validate the token: " + token);
		}
		suser.supporter = supporter;
		return null;
	}

	private String toString(Map<String, String[]> parameterMap) {
		StringBuilder bld = new StringBuilder();
		for(String s : parameterMap.keySet()) {
			bld.append(" ").append(s).append("=").append(Arrays.toString(parameterMap.get(s)));
		}
		return bld.toString();
	}
}
