package net.osmand.server.controllers.pub;

import net.osmand.server.api.repo.MapUserRepository;
import net.osmand.server.api.repo.SupporterSubscriptionRepository;
import net.osmand.server.api.repo.SupportersRepository;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

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

    private void checkParameter(String paramName, String paramValue) {
        if (paramValue.isEmpty()) {
            throw new MissingRequestParameterException(paramName);
        }
    }

    @PostMapping(path = {"/register_email", "/register_email.php"},
            consumes = MediaType.APPLICATION_FORM_URLENCODED_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<MapUserRepository.MapUser> registerEmail(@RequestParam("aid") String aid,
                                                                   @RequestParam("email") String email) {
        checkParameter("aid", aid);
        checkParameter("E-mail", email);
        long timestamp = System.currentTimeMillis();
        MapUserRepository.MapUser mapUser = new MapUserRepository.MapUser();
        mapUser.setAid(aid);
        mapUser.setEmail(email);
        mapUser.setUpdateTime(timestamp);
        mapUser = mapUserRepository.save(mapUser);
        return ResponseEntity.ok(mapUser);
    }

    @PostMapping(path = {"/register", "/update.php"},
        consumes =  MediaType.APPLICATION_FORM_URLENCODED_VALUE,
        produces =  MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<SupportersRepository.Supporter> register(@RequestParam("visibleName") String visibleName,
                                                                   @RequestParam("email") String email,
                                                                   @RequestParam("preferredCountry") String preferredCountry) {
        checkParameter("Visible Name", visibleName);
        checkParameter("E-mail", email);
        checkParameter("Preferred Country", preferredCountry);
        if (preferredCountry.isEmpty()) {
            throw new MissingRequestParameterException("Preferred country");
        }
        Optional<SupportersRepository.Supporter> optionalSupporter = supportersRepository.findByUserEmail(email);
        if (optionalSupporter.isPresent()) {
            SupportersRepository.Supporter supporter = optionalSupporter.get();
            return ResponseEntity.ok(supporter);
        }
        ThreadLocalRandom tlr = ThreadLocalRandom.current();
        int token = tlr.nextInt(100000, 1000000);
        SupportersRepository.Supporter supporter = new SupportersRepository.Supporter(String.valueOf(token),
                visibleName, email, preferredCountry, 0);
        supporter = supportersRepository.save(supporter);
        return ResponseEntity.ok(supporter);
    }

    @PostMapping(path = {"/update", "/update.php"})
    public ResponseEntity<SupportersRepository.Supporter> update(@RequestParam("visibleName") String visibleName,
                                                                 @RequestParam("email") String email,
                                                                 @RequestParam("token") String token,
                                                                 @RequestParam("preferredCountry") String preferredCountry,
                                                                 @RequestParam("userid") Long userid) {
        checkParameter("Visible Name", visibleName);
        checkParameter("E-mail", email);
        checkParameter("Token", token);
        checkParameter("Preferred Country", preferredCountry);
        SupportersRepository.Supporter supporter =
                new SupportersRepository.Supporter(token, visibleName, email, preferredCountry, 0);
        supporter = supportersRepository.save(supporter);
        System.out.println("Supporter UserId after save = " + supporter.getUserId() + "\nUserId from request = " + userid);
        return ResponseEntity.ok(supporter);
    }

    @ExceptionHandler(MissingRequestParameterException.class)
    public ResponseEntity<String> missingParameterHandler(RuntimeException ex) {
        LOGGER.error(ex.getMessage(), ex);
        return ResponseEntity.badRequest().body(String.format(ERROR_MESSAGE_TEMPLATE, ex.getMessage()));
    }

    private static class MissingRequestParameterException extends RuntimeException {
        public MissingRequestParameterException(String s) {
            super(s);
        }
    }
}
