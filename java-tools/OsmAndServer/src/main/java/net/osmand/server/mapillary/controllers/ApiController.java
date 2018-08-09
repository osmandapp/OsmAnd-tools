package net.osmand.server.mapillary.controllers;

import net.osmand.server.mapillary.CameraPlace;
import net.osmand.server.mapillary.CameraPlaceHolder;
import net.osmand.server.mapillary.services.ImageService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.util.UriComponentsBuilder;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api")
public class ApiController {

    private static final String PROC_FILE = "/var/www-download/.proc_timestamp";

    private final ImageService imageService;

    @Autowired
    public ApiController(ImageService imageService) {
        this.imageService = imageService;
    }

    private List<CameraPlace> sortByDistance(List<CameraPlace> arr) {
        return arr.stream().sorted(Comparator.comparing(CameraPlace::getDistance)).collect(Collectors.toList());
    }

    @GetMapping(path = {"/osmlive_status.php", "/osmlive_status"}, headers = {"Content-Type: text/html; charset=UTF-8"})
    public Resource osmLiveStatus() {
        FileSystemResource fsr = new FileSystemResource(PROC_FILE);
        if (fsr.exists()) {
            return fsr;
        }
        throw new RuntimeException("File not found");
    }

    @GetMapping(path = {"/cm_place", "/cm_place.php"})
    public CameraPlaceHolder getCmPlace(@RequestParam("lat") double lat,
                                        @RequestParam("lon") double lon,
                                        @RequestParam(value = "myLocation", required = false) String myLocation,
                                        @RequestParam(value = "app", required = false) String app,
                                        @RequestParam(value = "lang", required = false) String lang,
                                        @RequestParam(value = "osm_image", required = false) String osmImage,
                                        @RequestParam(value = "osm_mapillary_key", required = false) String osmMapillaryKey) {

        Map<String, List<CameraPlace>> result = new HashMap<>();

        List<CameraPlace> arr = new ArrayList<>();
        List<CameraPlace> halfvisarr = new ArrayList<>();

        result.put("arr", arr);
        result.put("halfvisarr", halfvisarr);

        CameraPlace wikimediaPrimaryCameraPlace = imageService.processWikimediaData(lat, lon, osmImage);
        CameraPlace mapillaryPrimaryCameraPlace = imageService.processMapillaryData(lat, lon, osmMapillaryKey, result);

        arr = sortByDistance(arr);
        halfvisarr = sortByDistance(halfvisarr);

        if (arr.isEmpty()) {
            arr.addAll(halfvisarr);
        }

        if (wikimediaPrimaryCameraPlace != null) {
            arr.add(0, wikimediaPrimaryCameraPlace);
        }

        if (mapillaryPrimaryCameraPlace != null) {
            arr.add(0, mapillaryPrimaryCameraPlace);
        }
        return new CameraPlaceHolder(arr);
    }

    @GetMapping(path = {"/get_photo.php"})
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
}


















