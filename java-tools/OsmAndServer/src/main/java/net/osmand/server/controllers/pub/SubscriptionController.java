package net.osmand.server.controllers.pub;

import net.osmand.server.api.repo.MapUserRepository;
import net.osmand.server.api.repo.MapUserRepository.MapUser;
import net.osmand.server.api.repo.OsmRecipientsRepository;
import net.osmand.server.api.repo.OsmRecipientsRepository.OsmRecipient;
import net.osmand.server.api.repo.SupporterSubscriptionRepository;
import net.osmand.server.api.repo.SupportersRepository;
import net.osmand.server.api.repo.SupportersRepository.Supporter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.http.*;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import java.util.Base64;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

@RestController
@RequestMapping("/subscription")
public class SubscriptionController {
    private static final Log LOGGER = LogFactory.getLog(SubscriptionController.class);

    private static final String ERROR_MESSAGE_TEMPLATE = "{\"error\": \"%s is not specified\"}";

    @Autowired
    private SupportersRepository supportersRepository;
    @Autowired
    private SupporterSubscriptionRepository supporterSubscriptionRepository;
    @Autowired
    private MapUserRepository mapUserRepository;
    @Autowired
    private OsmRecipientsRepository osmRecipientsRepository;

    private final RestTemplate restTemplate;

    public SubscriptionController(RestTemplateBuilder builder) {
        this.restTemplate = builder.requestFactory(HttpComponentsClientHttpRequestFactory.class).build();
    }

    private void checkParameter(String paramName, String paramValue) {
        if (paramValue.isEmpty()) {
            throw new MissingRequestParameterException(paramName);
        }
    }

    private String encodeCredentialsToBase64(String userName, String password) {
        Base64.Encoder encoder = Base64.getMimeEncoder();
        byte[] credentials = userName.concat(":").concat(password).getBytes();
        return encoder.encodeToString(credentials);
    }

    private String processOsmUsername(String userName) {
        int ind = userName.indexOf("\'");
        if (ind > -1) {
            return userName.substring(0, ind).substring(ind + 1);
        }
        return userName;
    }

    private HttpHeaders buildHeaders(String credentials) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);
        headers.set(HttpHeaders.AUTHORIZATION, String.format("Basic: %s", credentials));
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
    public ResponseEntity<MapUser> registerEmail(@RequestParam("aid") String aid,
                                                                   @RequestParam("email") String email) {
        checkParameter("aid", aid);
        checkParameter("E-mail", email);
        long timestamp = System.currentTimeMillis();
        MapUser mapUser = new MapUser();
        mapUser.setAid(aid);
        mapUser.setEmail(email);
        mapUser.setUpdateTime(timestamp);
        mapUser = mapUserRepository.save(mapUser);
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
            Supporter supporter = optionalSupporter.get();
            return ResponseEntity.ok(supporter);
        }
        ThreadLocalRandom tlr = ThreadLocalRandom.current();
        int token = tlr.nextInt(100000, 1000000);
        Supporter supporter = new Supporter(0L , String.valueOf(token),
                visibleName, email, preferredCountry, 0);
        supporter = supportersRepository.saveAndFlush(supporter);
        return ResponseEntity.ok(supporter);
    }

    @PostMapping(path = {"/update", "/update.php"})
    public ResponseEntity<Supporter> update(@RequestParam("visibleName") String visibleName,
                                            @RequestParam("email") String email,
                                            @RequestParam("token") String token,
                                            @RequestParam("preferredCountry") String preferredCountry,
                                            @RequestParam("userid") Long userid) {
        checkParameter("Visible Name", visibleName);
        checkParameter("E-mail", email);
        checkParameter("Token", token);
        checkParameter("Preferred Country", preferredCountry);
        Supporter supporter = new Supporter(userid, token, visibleName, email, preferredCountry, 0);
        supporter = supportersRepository.saveAndFlush(supporter);
        return ResponseEntity.ok(supporter);
    }

    public ResponseEntity<OsmRecipient> registerOsm(@RequestParam("bitcoin_addr") String bitcoinAddress,
                                                    @RequestParam("osm_usr") String osmUser,
                                                    @RequestParam("osm_pwd") String osmPassword,
                                                    @RequestParam("email") String email) {
        checkParameter("Bitcoin address", bitcoinAddress);
        checkParameter("E-mail", email);
        checkParameter("Osm user", osmUser);
        checkParameter("Osm password", osmPassword);
        String username = processOsmUsername(osmUser);
        String credentials = encodeCredentialsToBase64(username, osmPassword);
        authenticateUser(credentials);
        long registerTimestamp = System.currentTimeMillis();
        OsmRecipient recipient = new OsmRecipient(osmUser, email, bitcoinAddress, registerTimestamp);
        recipient = osmRecipientsRepository.save(recipient);
        return ResponseEntity.ok(recipient);
    }
    
    @ExceptionHandler(MissingRequestParameterException.class)
    public ResponseEntity<String> missingParameterHandler(MissingRequestParameterException ex) {
        LOGGER.error(ex.getMessage(), ex);
        return ResponseEntity.badRequest().body(String.format(ERROR_MESSAGE_TEMPLATE, ex.getMessage()));
    }

    private static class MissingRequestParameterException extends RuntimeException {
        public MissingRequestParameterException(String s) {
            super(s);
        }
    }
}
