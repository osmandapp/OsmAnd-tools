package net.osmand.server.controllers.pub;

import net.osmand.server.services.images.CameraPlace;
import net.osmand.server.services.images.CameraPlaceCollection;
import net.osmand.server.services.images.ImageService;

import net.osmand.server.services.motd.MotdService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.FileSystemResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.util.UriComponentsBuilder;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.stream.Collectors;

@Controller
@RequestMapping("/api")
public class ApiController {
    private static final Log LOGGER = LogFactory.getLog(ApiController.class);

    private static final String RESULT_MAP_ARR = "arr";
    private static final String RESULT_MAP_HALFVISARR = "halfvisarr";

    @Value("${osmlive.status}")
    private String procFile;

    private final ImageService imageService;
    private final MotdService motdService;

    @Autowired
    public ApiController(ImageService imageService, MotdService motdService) {
        this.imageService = imageService;
        this.motdService = motdService;
    }

    private List<CameraPlace> sortByDistance(List<CameraPlace> arr) {
        return arr.stream().sorted(Comparator.comparing(CameraPlace::getDistance)).collect(Collectors.toList());
    }

    private CameraPlace createEmptyCameraPlaceWithTypeOnly(String type) {
        CameraPlace.CameraPlaceBuilder builder = new CameraPlace.CameraPlaceBuilder();
        builder.setType(type);
        return builder.build();
    }

    @GetMapping(path = {"/osmlive_status.php", "/osmlive_status"}, produces = "text/html;charset=UTF-8")
    @ResponseBody
    public String osmLiveStatus() throws IOException  {
        FileSystemResource fsr = new FileSystemResource(procFile);
        if (fsr.exists()) {
            BufferedReader br = new BufferedReader(new InputStreamReader(fsr.getInputStream()));
            return br.readLine();
        }
        LOGGER.error("proc file not found at " + procFile);
        throw new RuntimeException("File not found at " + procFile);
    }

    @GetMapping(path = {"/cm_place.php", "/cm_place"})
    @ResponseBody
    public CameraPlaceCollection getCmPlace(@RequestParam("lat") double lat,
                                            @RequestParam("lon") double lon,
                                            @RequestParam(value = "myLocation", required = false) String myLocation,
                                            @RequestParam(value = "app", required = false) String app,
                                            @RequestParam(value = "lang", required = false) String lang,
                                            @RequestParam(value = "osm_image", required = false) String osmImage,
                                            @RequestParam(value = "osm_mapillary_key", required = false) String osmMapillaryKey,
                                            @RequestHeader HttpHeaders headers,
                                            HttpServletRequest request) {
        InetSocketAddress inetAddress = headers.getHost();
        String host = inetAddress.getHostName();
        String proto = request.getScheme();
        String forwardedHost = headers.getFirst("X-Forwarded-Host");
        String forwardedProto = headers.getFirst("X-Forwarded-Proto");
        if (forwardedHost != null) {
            host = forwardedHost;
        }
        if (forwardedProto != null) {
            proto = forwardedProto;
        }
        if (host == null) {
            LOGGER.error("Bad request. Host is null");
            return new CameraPlaceCollection();
        }
        Map<String, List<CameraPlace>> result = new HashMap<>();

        List<CameraPlace> arr = new ArrayList<>();
        List<CameraPlace> halfvisarr = new ArrayList<>();

        result.put(RESULT_MAP_ARR, arr);
        result.put(RESULT_MAP_HALFVISARR, halfvisarr);

        CameraPlace wikimediaPrimaryCameraPlace = imageService.processWikimediaData(lat, lon, osmImage);
        CameraPlace mapillaryPrimaryCameraPlace = imageService.processMapillaryData(lat, lon, osmMapillaryKey, result,
                host, proto);
        if (arr.isEmpty()) {
            arr.addAll(halfvisarr);
        }
        arr = sortByDistance(arr);
        if (wikimediaPrimaryCameraPlace != null) {
            arr.add(0, wikimediaPrimaryCameraPlace);
        }
        if (mapillaryPrimaryCameraPlace != null) {
            arr.add(0, mapillaryPrimaryCameraPlace);
        }
        if (!arr.isEmpty()) {
            arr.add(createEmptyCameraPlaceWithTypeOnly("mapillary-contribute"));
        }
        return new CameraPlaceCollection(arr);
    }

    @GetMapping(path = {"/mapillary/get_photo.php", "/mapillary/get_photo"})
    @ResponseBody
    public void getPhoto(@RequestParam("photo_id") String photoId,
                         @RequestParam(value = "hires", required = false) boolean hires,
                         HttpServletResponse resp) throws IOException {
        String hiresThumb = "thumb-1024.jpg";
        String thumb = "thumb-640.jpg";
        String cloudFrontUriTemplate = "https://d1cuyjsrcm0gby.cloudfront.net/{photoId}/{thumb}?origin=osmand";
        UriComponentsBuilder uriBuilder = UriComponentsBuilder.fromHttpUrl(cloudFrontUriTemplate);
        resp.setContentType("image/jpeg");
        if (hires) {
            resp.sendRedirect(uriBuilder.buildAndExpand(photoId, hiresThumb).toString());
        } else {
            resp.sendRedirect(uriBuilder.buildAndExpand(photoId, thumb).toString());
        }
    }

    @GetMapping(path = {"/mapillary/photo-viewer.php", "/mapillary/photo-viewer"})
    public String getPhotoViewer(@RequestParam("photo_id") String photoId, Model model) {
        model.addAttribute("photoId", photoId);
        return "mapillary/photo-viewer";
    }

    @GetMapping(path = {"/motd", "/motd.php"})
    @ResponseBody
    public ResponseEntity<String> getMessage(@RequestParam(required = false) String version,
                             @RequestParam(required = false) Integer nd,
                             @RequestParam(required = false) Integer ns,
                             @RequestParam(required = false) String lang,
                             @RequestParam(required = false) String os,
                             @RequestParam(required = false) String aid,
                             @RequestParam(required = false) String discount,
                             @RequestHeader HttpHeaders headers) throws IOException {
        String body = motdService.getMessage(version, os, headers);
        if (body != null) {
            return ResponseEntity.ok(body);
        }
        return ResponseEntity.noContent().build();
    }

    @PutMapping(path = {"/motd/update"})
    @ResponseBody
    public String updateMotdSettings() {
        return motdService.updateSettings();
    }

    @ResponseStatus(HttpStatus.I_AM_A_TEAPOT)
    @ExceptionHandler(MotdService.CannotUpdateMotdSettingsException.class)
    @ResponseBody
    public String handleUpdateMotdSettingsFailed(Exception ex) {
        return ex.getMessage();
    }
}