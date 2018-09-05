package net.osmand.server.controllers.pub;

import net.osmand.server.api.services.SubscriptionService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/subscription")
public class SubscriptionController {

    private static final String ERROR_MESSAGE_TEMPLATE = "{\"error\": \"%s is not specified\"}";

    private final SubscriptionService subscriptionService;

    @Autowired
    public SubscriptionController(SubscriptionService subscriptionService) {
        this.subscriptionService = subscriptionService;
    }

    @PostMapping(path = "/purchased",
            consumes = MediaType.APPLICATION_FORM_URLENCODED_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> purchased(@RequestParam("userid") String userId,
                                            @RequestParam("purchaseToken") String purchaseToken,
                                            @RequestParam("sku") String sku) {
        if (userId.isEmpty()) {
            return ResponseEntity.badRequest().body(String.format(ERROR_MESSAGE_TEMPLATE, "User id"));
        }
        if (purchaseToken.isEmpty()) {
            return ResponseEntity.badRequest().body(String.format(ERROR_MESSAGE_TEMPLATE, "Purchase token"));
        }
        if (sku.isEmpty()) {
            return ResponseEntity.badRequest().body(String.format(ERROR_MESSAGE_TEMPLATE, "Subscription id"));
        }
        return subscriptionService.purchase(userId, purchaseToken, sku);
    }

    @PostMapping(path = "/register_email",
            consumes = MediaType.APPLICATION_FORM_URLENCODED_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> registerEmail(@RequestParam("aid") String aid,
                                                @RequestParam("email") String email) {
        if (aid.isEmpty()) {
            return ResponseEntity.badRequest().body(String.format(ERROR_MESSAGE_TEMPLATE, "aid"));
        }
        if (email.isEmpty()) {
            return ResponseEntity.badRequest().body(String.format(ERROR_MESSAGE_TEMPLATE, "Email"));
        }
        return subscriptionService.registerEmail(aid, email);
    }
}
