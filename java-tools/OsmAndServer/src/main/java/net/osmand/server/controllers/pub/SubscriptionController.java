package net.osmand.server.controllers.pub;

import java.util.Base64;
import java.util.Date;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

import javax.servlet.http.HttpServletRequest;

import net.osmand.server.api.repo.MapUserRepository;
import net.osmand.server.api.repo.MapUserRepository.MapUser;
import net.osmand.server.api.repo.OsmRecipientsRepository;
import net.osmand.server.api.repo.OsmRecipientsRepository.OsmRecipient;
import net.osmand.server.api.repo.SupportersDeviceSubscriptionRepository;
import net.osmand.server.api.repo.SupportersDeviceSubscriptionRepository.SupporterDeviceSubscription;
import net.osmand.server.api.repo.SupportersRepository;
import net.osmand.server.api.repo.SupportersRepository.Supporter;
import net.osmand.server.utils.BTCAddrValidator;

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
    private SupportersDeviceSubscriptionRepository supportersDeviceSubscriptionRepository;
    
    @Autowired
    private StringRedisTemplate redisTemplate;

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
        long updateTime = System.currentTimeMillis();
        MapUser mapUser = new MapUser();
        mapUser.aid = request.getParameter("aid");
        mapUser.email = request.getParameter("email");
        mapUser.updateTime = updateTime;
        mapUser = mapUserRepository.save(mapUser);
        return ok("{\"email\": \"%s\", \"time\": \"%d\"}", mapUser.email, mapUser.updateTime);
    }
    
	private String userInfoAsJson(Supporter s) {
		String response = String.format(
				"{\"userid\": \"%d\", \"token\": \"%s\", \"visibleName\": \"%s\", \"email\": \"%s\", "
						+ "\"preferredCountry\": \"%s\"}", 
				s.userId, s.token, s.visibleName, s.userEmail, s.preferredRegion);
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
        if (!BTCAddrValidator.validate(bitcoinAddress)) {
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
        long registerTimestamp = System.currentTimeMillis();
        OsmRecipient recipient = new OsmRecipient();
        recipient.osmId = osmUser;
        recipient.email = email;
        recipient.bitcoinAddress = bitcoinAddress;
        recipient.updateTime = registerTimestamp;
        recipient = osmRecipientsRepository.save(recipient);
        String response = String.format("{\"osm_user\": \"%s\", \"bitcoin_addr\": \"%s\", \"time\": \"%s\"}",
                recipient.osmId, recipient.bitcoinAddress, recipient.updateTime +"");
        return ResponseEntity.ok(response);
    }

    private boolean isEmpty(String s) {
		return s == null || s.length() == 0;
	}

	@PostMapping(path = {"/purchased", "/purchased.php"})
    public ResponseEntity<String> purchased(HttpServletRequest request) {
		String userId = request.getParameter("userid");
    	if (isEmpty(userId)) {
        	return error("Please validate user id.");
        }
    	String token = request.getParameter("token");
    	if (isEmpty(token)) {
    		LOGGER.warn("Token was not provided: " + request.getParameterMap());
//        	return error("Token is not present: fix will be in OsmAnd 3.2");
        }
        Optional<Supporter> sup = supportersRepository.findById(Long.parseLong(userId));
        if(!sup.isPresent() ) {
        	return error("Couldn't find your user id: " + userId);
        }
        Supporter supporter = sup.get();
        if(token != null && !token.equals(supporter.token)) {
    		LOGGER.warn("Token failed validation: " + request.getParameterMap());
//        	return error("Couldn't validate the token: " + token);
        }
        SupporterDeviceSubscription subscr = new SupporterDeviceSubscription();
        subscr.userId = supporter.userId.toString();
        subscr.sku = request.getParameter("sku");
        subscr.purchaseToken = request.getParameter("purchaseToken");
        subscr.timestamp = new Date();
        supportersDeviceSubscriptionRepository.save(subscr);
        return ResponseEntity.ok(userInfoAsJson(supporter));
    }
}
