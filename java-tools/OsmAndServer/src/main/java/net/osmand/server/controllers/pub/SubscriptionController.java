package net.osmand.server.controllers.pub;

import net.osmand.server.api.repo.*;
import net.osmand.server.api.repo.MapUserRepository.MapUser;
import net.osmand.server.api.repo.OsmRecipientsRepository.OsmRecipient;
import net.osmand.server.api.repo.SupportersRepository.Supporter;
import net.osmand.server.utils.BTCAddrValidator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.*;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;

import java.sql.Timestamp;
import java.util.Base64;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

@RestController
@RequestMapping("/subscription")
public class SubscriptionController {
    private static final Log LOGGER = LogFactory.getLog(SubscriptionController.class);

    private static final String ERROR_MESSAGE_TEMPLATE = "{\"error\": \"%s is not specified\"}";

    private static final String REDIS_KEY_INVALID_TOKEN = "invalid_token";

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

    public SubscriptionController(RestTemplateBuilder builder) {
        this.restTemplate = builder.requestFactory(HttpComponentsClientHttpRequestFactory.class).build();
    }

    private void checkParameter(String paramName, String paramValue) {
        if (paramValue.isEmpty()) {
            throw new MissingRequestParameterException(paramName);
        }
    }

    private void validateBitcoinAddress(String bitcoinAddress) {
        if (!BTCAddrValidator.validate(bitcoinAddress)) {
            throw new BitcoinAddressInvalidException("Address is invalid");
        }
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

    private void validateSupporterToken(Supporter supporter, String token) {
        if (!supporter.token.equals(token)) {
            throw new InvalidTokenException("Token is invalid.", token);
        }
    }

    @PostMapping(path = {"/register_email", "/register_email.php"},
            consumes = MediaType.APPLICATION_FORM_URLENCODED_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<MapUser> registerEmail(@RequestParam("aid") String aid,
                                                 @RequestParam("email") String email) {
        checkParameter("aid", aid);
        checkParameter("E-mail", email);
        long updateTime = System.currentTimeMillis();
        MapUser mapUser = new MapUser();
        mapUser.aid = aid;
        mapUser.email = email;
        mapUser.updateTime = updateTime;
        mapUser = mapUserRepository.saveAndFlush(mapUser);
        return ResponseEntity.ok(mapUser);
    }

    @PostMapping(path = {"/register", "/register.php"},
        consumes =  MediaType.APPLICATION_FORM_URLENCODED_VALUE,
        produces =  MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Supporter> register(@RequestParam("visibleName") String visibleName,
                                              @RequestParam("email") String email,
                                              @RequestParam("preferredCountry") String preferredCountry) {
        checkParameter("Visible Name", visibleName);
        checkParameter("E-mail", email);
        checkParameter("Preferred Country", preferredCountry);
        Optional<Supporter> optionalSupporter = supportersRepository.findByUserEmail(email);
        if (optionalSupporter.isPresent()) {
            return ResponseEntity.ok(optionalSupporter.get());
        }
        ThreadLocalRandom tlr = ThreadLocalRandom.current();
        int token = tlr.nextInt(100000, 1000000);
        Supporter supporter = new Supporter();
        supporter.userId = 0L;
        supporter.token = String.valueOf(token);
        supporter.visibleName = visibleName;
        supporter.userEmail = email;
        supporter.preferedRegion = preferredCountry;
        supporter.disabled = 0;
        supporter = supportersRepository.saveAndFlush(supporter);
        return ResponseEntity.ok(supporter);
    }

    @PostMapping(path = {"/update", "/update.php"},
            consumes =  MediaType.APPLICATION_FORM_URLENCODED_VALUE,
            produces =  MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Supporter> update(@RequestParam("visibleName") String visibleName,
                                            @RequestParam("email") String email,
                                            @RequestParam("token") String token,
                                            @RequestParam("preferredCountry") String preferredCountry,
                                            @RequestParam("userid") Long userid) {
        checkParameter("Visible Name", visibleName);
        checkParameter("E-mail", email);
        checkParameter("Token", token);
        checkParameter("Preferred Country", preferredCountry);
        Supporter supporter = new Supporter();
        supporter.userId = userid;
        supporter.token = token;
        supporter.visibleName = visibleName;
        supporter.userEmail = email;
        supporter.preferedRegion = preferredCountry;
        supporter.disabled = 0;
        supporter = supportersRepository.saveAndFlush(supporter);
        return ResponseEntity.ok(supporter);
    }

    @PostMapping(path = {"/register_osm", "/register_osm.php"},
            consumes =  MediaType.APPLICATION_FORM_URLENCODED_VALUE,
            produces =  MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<OsmRecipient> registerOsm(@RequestParam("bitcoin_addr") String bitcoinAddress,
                                                    @RequestParam("osm_usr") String osmUser,
                                                    @RequestParam("osm_pwd") String osmPassword,
                                                    @RequestParam("email") String email) {
        checkParameter("Bitcoin address", bitcoinAddress);
        validateBitcoinAddress(bitcoinAddress);
        checkParameter("E-mail", email);
        checkParameter("Osm user", osmUser);
        checkParameter("Osm password", osmPassword);
        String username = processOsmUsername(osmUser);
        String credentials = encodeCredentialsToBase64(username, osmPassword);
        authenticateUser(credentials);
        long registerTimestamp = System.currentTimeMillis();
        OsmRecipient recipient = new OsmRecipient();
        recipient.osmId = osmUser;
        recipient.email = email;
        recipient.bitcoinAddress = bitcoinAddress;
        recipient.updateTime = registerTimestamp;
        recipient = osmRecipientsRepository.saveAndFlush(recipient);
        return ResponseEntity.ok(recipient);
    }

    @PostMapping(path = {"/purchased", "/purchased.php"})
    public ResponseEntity<Supporter> purchased(@RequestParam("userid") Long userId,
                                               @RequestParam("token") String token,
                                               @RequestParam("sku") String sku,
                                               @RequestParam("purchaseToken") String purchaseToken) {
        checkParameter("token", token);
        checkParameter("sku", sku);
        checkParameter("purchase token", purchaseToken);
        Optional<Supporter> supporterOptional = supportersRepository.findById(userId);
        if (!supporterOptional.isPresent()) {
            throw new SupporterNotFoundException("User not found with given id : " + userId);
        }
        Supporter supporter = supporterOptional.get();
        validateSupporterToken(supporter, token);
        long timestamp = System.currentTimeMillis();
        supportersDeviceSubscriptionRepository.createSupporterDeviceSubscriptionIfNotExists(
                userId, sku, purchaseToken, new Timestamp(timestamp));
        return ResponseEntity.ok(supporter);
    }

    @ExceptionHandler(MissingRequestParameterException.class)
    public ResponseEntity<String> missingParameterHandler(MissingRequestParameterException ex) {
        LOGGER.error(ex.getMessage(), ex);
        return ResponseEntity.badRequest().body(String.format(ERROR_MESSAGE_TEMPLATE, ex.getMessage()));
    }

    @ExceptionHandler(BitcoinAddressInvalidException.class)
    public ResponseEntity<String> bitcoinAddressInvalidHandler(BitcoinAddressInvalidException ex) {
        LOGGER.error(ex.getMessage(), ex);
        return ResponseEntity.badRequest().body("{\"error\": \"%s\"}");
    }

    @ExceptionHandler(SupporterNotFoundException.class)
    public ResponseEntity<String> supporterNotFoundHandler(SupporterNotFoundException ex) {
        LOGGER.error(ex.getMessage(), ex);
        return ResponseEntity.status(HttpStatus.NOT_FOUND).body("{\"error\": \"User not found with given userid.\"}");
    }

    @ExceptionHandler(InvalidTokenException.class)
    public ResponseEntity<String> invalidTokenHandler(InvalidTokenException ex) {
        LOGGER.error(ex.getMessage(), ex);
        String token = ex.getToken();
        redisTemplate.opsForZSet().add(REDIS_KEY_INVALID_TOKEN, token, token.hashCode());
        return ResponseEntity.badRequest().body(String.format("{\"error\": \"%s\"}", ex.getToken()));
    }

    private static class MissingRequestParameterException extends RuntimeException {
        MissingRequestParameterException(String s) {
            super(s);
        }
    }

    private static class BitcoinAddressInvalidException extends RuntimeException {
        BitcoinAddressInvalidException(String s) {
            super(s);
        }
    }

    private static class SupporterNotFoundException extends RuntimeException {
        SupporterNotFoundException(String s) {
            super(s);
        }
    }

    private static class InvalidTokenException extends RuntimeException {

        private final String token;

        InvalidTokenException(String s, String token) {
            super(s);
            this.token = token;
        }

        String getToken() {
            return token;
        }
    }
}
