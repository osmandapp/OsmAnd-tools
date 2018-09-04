package net.osmand.server.controllers.pub;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.URL;
import java.net.URLConnection;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import net.osmand.server.services.api.CameraPlace;
import net.osmand.server.services.api.ImageService;
import net.osmand.server.services.api.MotdMessage;
import net.osmand.server.services.api.MotdService;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.FileSystemResource;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.util.UriComponentsBuilder;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@Controller
@RequestMapping("/api")
public class ApiController {
    private static final Log LOGGER = LogFactory.getLog(ApiController.class);

    private static final String RESULT_MAP_ARR = "arr";
    private static final String RESULT_MAP_HALFVISARR = "halfvisarr";
    private static final String PROC_FILE = ".proc_timestamp";

    @Value("${files.location}")
    private String filesLocation;
    
    @Value("${geoip.url}")
    private String geoipURL;

    @Autowired
    private ImageService imageService;
    
    @Autowired
    private MotdService motdService;

	private ObjectMapper jsonMapper;
    
    private ApiController() {
    	 ObjectMapper mapper = new ObjectMapper();
    	 this.jsonMapper = mapper;
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
        String procFile = filesLocation.concat(PROC_FILE);
        FileSystemResource fsr = new FileSystemResource(procFile);
        if (fsr.exists()) {
            BufferedReader br = new BufferedReader(new InputStreamReader(fsr.getInputStream()));
            return br.readLine();
        }
        LOGGER.error("proc file not found at " + procFile);
        throw new RuntimeException("File not found at " + procFile);
    }
    
    @GetMapping(path = {"/geo-ip"}, produces = "application/json")
    @ResponseBody
    public String findGeoIP(HttpServletRequest request) throws JsonParseException, JsonMappingException, IOException{
    	String remoteAddr = request.getRemoteAddr();
    	Enumeration<String> hs = request.getHeaders("X-Forwarded-For");
        if (hs != null && hs.hasMoreElements()) {
            remoteAddr = hs.nextElement();
        }
        URLConnection conn = new URL(geoipURL + remoteAddr).openConnection();
        TypeReference<HashMap<String,Object>> typeRef = new TypeReference<HashMap<String,Object>>() {};
        HashMap<String,Object> value = jsonMapper.readValue(conn.getInputStream(), typeRef);
        conn.getInputStream().close();
        if(value.containsKey("lat") && !value.containsKey("latitude")) {
        	value.put("latitude", value.get("lat"));
        } else if(!value.containsKey("lat") && value.containsKey("latitude")) {
        	value.put("lat", value.get("latitude"));
        }
        if(value.containsKey("lon") && !value.containsKey("longitude")) {
        	value.put("longitude", value.get("lon"));
        } else if(!value.containsKey("lon") && value.containsKey("longitude")) {
        	value.put("lon", value.get("longitude"));
        }
        
        return jsonMapper.writeValueAsString(value);
    }
    
    
    
    @GetMapping(path = {"/cm_place.php", "/cm_place"})
    @ResponseBody
    public String getCmPlace(@RequestParam("lat") double lat,
                                            @RequestParam("lon") double lon,
                                            @RequestParam(value = "mloc", required = false) String mloc,
                                            @RequestParam(value = "app", required = false) String app,
                                            @RequestParam(value = "lang", required = false) String lang,
                                            @RequestParam(value = "osm_image", required = false) String osmImage,
                                            @RequestParam(value = "osm_mapillary_key", required = false) String osmMapillaryKey,
                                            @RequestHeader HttpHeaders headers,
                                            HttpServletRequest request) throws JsonProcessingException {
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
			return "{" + jsonMapper.writeValueAsString("features") + ":[]}";
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
        return "{"+jsonMapper.writeValueAsString("features")+
        			":"+jsonMapper.writeValueAsString(arr)+"}";
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
    public String getMessage(@RequestParam(required = false) String version,
                             @RequestParam(required = false) Integer nd,
                             @RequestParam(required = false) Integer ns,
                             @RequestParam(required = false) String lang,
                             @RequestParam(required = false) String os,
                             @RequestParam(required = false) String aid,
                             @RequestParam(required = false) String discount,
                             @RequestHeader HttpHeaders headers,
                             HttpServletRequest request) throws IOException, ParseException {
    	String remoteAddr = request.getRemoteAddr();
        if (headers.getFirst("X-Forwarded-For") != null) {
            remoteAddr = headers.getFirst("X-Forwarded-For");
        }
        MotdMessage body = motdService.getMessage(version, os, remoteAddr);
        if (body != null) {
            return jsonMapper.writeValueAsString(body);
        }
        return "{}";
    }
}