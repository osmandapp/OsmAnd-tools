package net.osmand.server.controllers.pub;

import net.osmand.server.api.services.PromoService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

@RequestMapping("/promo")
@Controller
public class PromoController {
    
    @Autowired
    PromoService promoService;
    
    @PostMapping(path = {"/add-user"})
    public ResponseEntity<String> addUser(@RequestParam String name,
                                          @RequestParam String email) {
        return promoService.addUser(name, email);
    }
}
