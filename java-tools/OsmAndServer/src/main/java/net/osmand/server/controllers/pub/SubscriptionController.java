package net.osmand.server.controllers.pub;

import com.fasterxml.jackson.databind.ObjectMapper;
import net.osmand.server.api.repo.MapUserRepository;
import net.osmand.server.api.repo.MapUserRepository.MapUser;
import net.osmand.server.api.repo.OsmRecipientsRepository;
import net.osmand.server.api.repo.OsmRecipientsRepository.OsmRecipient;
import net.osmand.server.api.repo.SupportersDeviceSubscriptionRepository;
import net.osmand.server.api.repo.SupportersRepository;
import net.osmand.server.api.repo.SupportersRepository.Supporter;
import net.osmand.server.utils.BTCAddrValidator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.hibernate.validator.internal.constraintvalidators.bv.EmailValidator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.*;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import javax.servlet.http.HttpServletRequest;
import java.util.Base64;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

@RestController
@RequestMapping("/subscription")
public class SubscriptionController {
    private static final Log LOGGER = LogFactory.getLog(SubscriptionController.class);

    private static final String REDIS_KEY_INVALID_TOKEN = "invalid_token";

    private static final int CONNECTION_POOL_SIZE = 5;
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
    @Autowired
    private ObjectMapper mapper;

    private final RestTemplate restTemplate;

    @Autowired
    public SubscriptionController(RestTemplateBuilder builder) {
        this.restTemplate = builder.requestFactory(
                () -> new HttpComponentsClientHttpRequestFactory(buildHttpClient()))
                .setConnectTimeout(TIMEOUT)
                .setReadTimeout(TIMEOUT)
                .build();
    }

    private HttpClient buildHttpClient() {
        PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
        connectionManager.setDefaultMaxPerRoute(CONNECTION_POOL_SIZE);
        connectionManager.setMaxTotal(CONNECTION_POOL_SIZE);
        return HttpClients.custom().setConnectionManager(connectionManager).build();
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

    @PostMapping(path = {"/register_email", "/register_email.php"},
            consumes = MediaType.APPLICATION_FORM_URLENCODED_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> registerEmail(HttpServletRequest request) {
        long updateTime = System.currentTimeMillis();
        MapUser mapUser = new MapUser();
        mapUser.aid = request.getParameter("aid");
        mapUser.email = request.getParameter("email");
        mapUser.updateTime = updateTime;
        mapUser = mapUserRepository.saveAndFlush(mapUser);
        return ResponseEntity.ok().body(String.format("{\"email\": \"%s\", \"time\": \"%d\"}", mapUser.email,
                mapUser.updateTime));
    }

    @PostMapping(path = {"/register", "/register.php"},
        consumes =  MediaType.APPLICATION_FORM_URLENCODED_VALUE,
        produces =  MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> register(HttpServletRequest request) {
        String email = request.getParameter("email");
        String visibleName = request.getParameter("visibleName");
        String preferredCountry = request.getParameter("preferredCountry");
        EmailValidator emailValidator = new EmailValidator();
        if (!emailValidator.isValid(email, null)) {
            return ResponseEntity.badRequest().body("{\"error\": \"Please validate email address.\"}");
        }
        Optional<Supporter> optionalSupporter = supportersRepository.findByUserEmail(email);
        if (optionalSupporter.isPresent()) {
            Supporter supporter = optionalSupporter.get();
            String response = String.format(
                    "{\"userid\": \"%d\", \"token\": \"%s\", \"visibleName\": \"%s\", \"email\": \"%s\", " +
                            "\"preferredCountry\": \"%s\"}",
                    supporter.userId, supporter.token, supporter.visibleName, supporter.userEmail, supporter.preferredRegion);
            return ResponseEntity.ok(response);
        }
        ThreadLocalRandom tlr = ThreadLocalRandom.current();
        int token = tlr.nextInt(100000, 1000000);
        Supporter supporter = new Supporter();
        supporter.userId = 0L;
        supporter.token = String.valueOf(token);
        supporter.visibleName = visibleName;
        supporter.userEmail = email;
        supporter.preferredRegion = preferredCountry;
        supporter.disabled = 0;
        supporter = supportersRepository.saveAndFlush(supporter);
        String response = String.format(
                "{\"userid\": \"%d\", \"token\": \"%s\", \"visibleName\": \"%s\", \"email\": \"%s\", " +
                        "\"preferredCountry\": \"%s\"}",
                supporter.userId, supporter.token, supporter.visibleName, supporter.userEmail, supporter.preferredRegion);
        return ResponseEntity.ok(response);
    }

    @PostMapping(path = {"/update", "/update.php"},
            consumes =  MediaType.APPLICATION_FORM_URLENCODED_VALUE,
            produces =  MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> update(HttpServletRequest request) {
        Supporter supporter = new Supporter();
        supporter.userId = Long.parseLong(request.getParameter("userid"));
        supporter.token = request.getParameter("token");
        supporter.visibleName = request.getParameter("visibleName");
        supporter.userEmail = request.getParameter("email");
        supporter.preferredRegion = request.getParameter("preferredCountry");
        supporter = supportersRepository.saveAndFlush(supporter);
        return ResponseEntity.ok(String.format("{\"userid\": \"%d\", \"token\": \"%s\", \"visibleName\": \"%s\", " +
                "\"email\": \"%s\", \"preferredCountry\": \"%s\"}", supporter.userId, supporter.token,
                supporter.visibleName, supporter.userEmail, supporter.preferredRegion));
    }

    @PostMapping(path = {"/register_osm", "/register_osm.php"},
            consumes =  MediaType.APPLICATION_FORM_URLENCODED_VALUE,
            produces =  MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> registerOsm(HttpServletRequest request) {
        String bitcoinAddress = request.getParameter("bitcoin_addr");
        if (!bitcoinAddress.startsWith("3") && !BTCAddrValidator.validate(bitcoinAddress)) {
            return ResponseEntity.badRequest().body("{\"error\": \"Please validate bitcoin address.\"}");
        }
        String osmUser = request.getParameter("osm_usr");
        String osmPassword = request.getParameter("osm_pwd");
        String email = request.getParameter("email");
        String username = processOsmUsername(osmUser);
        String credentials = encodeCredentialsToBase64(username, osmPassword);
        try {
            authenticateUser(credentials);
        } catch (RestClientException ex) {
            LOGGER.error(ex.getMessage(), ex);
            return ResponseEntity.badRequest().body("{\"error\": \"Couldn't authenticate on osm server\"}");
        }
        long registerTimestamp = System.currentTimeMillis();
        OsmRecipient recipient = new OsmRecipient();
        recipient.osmId = osmUser;
        recipient.email = email;
        recipient.bitcoinAddress = bitcoinAddress;
        recipient.updateTime = registerTimestamp;
        recipient = osmRecipientsRepository.saveAndFlush(recipient);
        String response = String.format("{\"osm_user\": \"%s\", \"bitcoin_addr\": \"%s\", \"time\": \"%d\"}",
                recipient.osmId, recipient.bitcoinAddress, recipient.updateTime);
        return ResponseEntity.ok(response);
    }

    @PostMapping(path = {"/purchased", "/purchased.php"})
    public ResponseEntity<String> purchased(HttpServletRequest request) {
        long userId = Long.parseLong(request.getParameter("userid"));
        Optional<Supporter> supporterOptional = supportersRepository.findById(userId);
        if (!supporterOptional.isPresent()) {
            return ResponseEntity.badRequest().body("{\"error\": \"User not found with given id.\"}");
        }
        Supporter supporter = supporterOptional.get();
        String token = request.getParameter("token");
        if (!supporter.token.equals(token)) {
            redisTemplate.opsForZSet().add(REDIS_KEY_INVALID_TOKEN, token, token.hashCode());
            return ResponseEntity.badRequest().body("{\"error\": \"Wrong token.\"}");
        }
        String purchaseToken = request.getParameter("purchaseToken");
        if (purchaseToken == null || purchaseToken.isEmpty()) {
            return ResponseEntity.badRequest().body("{\"error\": \"Purchase token is not specified.\"}");
        }
        String sku = request.getParameter("sku");
        if (sku == null || sku.isEmpty()) {
            return ResponseEntity.badRequest().body("{\"error\": \"Subscription id is not specified.\"}");
        }
        long timestamp = System.currentTimeMillis();
        supportersDeviceSubscriptionRepository.createSupporterDeviceSubscriptionIfNotExists(
                userId, sku, purchaseToken, timestamp);
        String response = String.format(
                "{\"status\": \"OK\", \"visibleName\": \"%s\", \"email\": \"%s\", \"preferredCountry\": \"%s\"}",
                supporter.visibleName, supporter.userEmail, supporter.preferredRegion);
        return ResponseEntity.ok(response);
    }
}
