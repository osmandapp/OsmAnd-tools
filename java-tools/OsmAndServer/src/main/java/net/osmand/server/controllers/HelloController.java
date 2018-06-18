package net.osmand.server.controllers;

import net.osmand.MapCreatorVersion;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class HelloController {

    @RequestMapping("/hello")
    public String index() {
        return "Greetings from: " + MapCreatorVersion.APP_MAP_CREATOR_FULL_NAME+ " ";
    }

}