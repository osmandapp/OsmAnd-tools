package net.osmand.server.controllers.pub;


import java.io.IOException;
import java.sql.SQLException;
import java.util.*;

import net.osmand.server.api.repo.CloudUserDevicesRepository;
import net.osmand.server.utils.MultiPlatform;
import net.osmand.shared.wiki.WikiHelper;
import net.osmand.shared.wiki.WikiImage;
import net.osmand.shared.wiki.WikiMetadata;
import net.osmand.util.Algorithms;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import com.google.gson.Gson;

import net.osmand.data.LatLon;
import net.osmand.server.api.services.OsmAndMapsService;
import net.osmand.server.api.services.SearchService;
import net.osmand.server.api.services.WikiService;
import net.osmand.server.controllers.pub.GeojsonClasses.FeatureCollection;
import org.xmlpull.v1.XmlPullParserException;

import static net.osmand.server.controllers.pub.GeojsonClasses.*;
@Controller
@RequestMapping({"/search", "/routing/search"})
public class SearchController {
    
    protected static final Log LOGGER = LogFactory.getLog(SearchController.class);
    Gson gson = new Gson();
    
    @Autowired
    OsmAndMapsService osmAndMapsService;
    
    @Autowired
    WikiService wikiService;
    
    @Autowired
    SearchService searchService;
    
    @Autowired
    protected CloudUserDevicesRepository devicesRepository;
    
    private CloudUserDevicesRepository.CloudUserDevice checkToken(int deviceId, String accessToken) {
        CloudUserDevicesRepository.CloudUserDevice d = devicesRepository.findById(deviceId);
        if (d != null && Algorithms.stringsEqual(d.accesstoken, accessToken)) {
            return d;
        }
        return null;
    }
    
    @RequestMapping(path = "/search", produces = {MediaType.APPLICATION_JSON_VALUE})
    public ResponseEntity<String> search(@RequestParam double lat,
                                         @RequestParam double lon,
                                         @RequestParam String text,
                                         @RequestParam String locale,
                                         @RequestParam (required = false) String northWest,
                                         @RequestParam (required = false) String southEast,
                                         @RequestParam(required = false) Boolean baseSearch) throws IOException {
        if (!osmAndMapsService.validateAndInitConfig()) {
            return osmAndMapsService.errorConfig();
        }
        List<Feature> features = searchService.search(new SearchService.SearchContext(lat, lon, text, locale,
		        baseSearch != null && baseSearch, null, northWest, southEast));
        return ResponseEntity.ok(gson.toJson(new FeatureCollection(features.toArray(new Feature[0]))));
    }
    
    @RequestMapping(path = {"/search-poi"}, produces = "application/json")
    @ResponseBody
    public ResponseEntity<String> searchPoi(@RequestBody SearchService.PoiSearchData searchData,
                                            @RequestParam String locale,
                                            @RequestParam double lat,
                                            @RequestParam double lon,
                                            @RequestParam(required = false) Boolean baseSearch) throws IOException {
        SearchService.PoiSearchResult poiSearchResult = searchService.searchPoi(searchData, locale, new LatLon(lat, lon), baseSearch != null && baseSearch);
        return ResponseEntity.ok(gson.toJson(poiSearchResult));
    }

    @RequestMapping(path = {"/get-poi"}, produces = "application/json")
    @ResponseBody
    public ResponseEntity<String> getPoi(@RequestParam String type,
                                         @RequestParam String pin,
                                         @RequestParam (required = false) String name,
                                         @RequestParam (required = false) Long osmId,
                                         @RequestParam (required = false) Long wikidataId) throws IOException {
        Feature poiSearchResult;

        if (pin == null || pin.isEmpty()) {
            return ResponseEntity.badRequest().body("Missing or empty 'pin' parameter");
        }

        String[] latlon = pin.split(",");
        if (latlon.length != 2) {
            return ResponseEntity.badRequest().body("Invalid 'pin' format, expected 'lat,lon'");
        }
        double lat, lng;
        try {
            lat = Double.parseDouble(latlon[0]);
            lng = Double.parseDouble(latlon[1]);
        } catch (NumberFormatException e) {
            return ResponseEntity.badRequest().body("Invalid 'pin' coordinates, expected numeric values for 'lat,lon'");
        }

        poiSearchResult = searchService.getPoiResultByShareLink(type, new LatLon(lat, lng), name, osmId, wikidataId);

        return ResponseEntity.ok(gson.toJson(poiSearchResult));
    }
    
    @GetMapping(path = {"/get-poi-categories"}, produces = "application/json")
    @ResponseBody
    public ResponseEntity<String> getPoiCategories(@RequestParam String locale) {
        Map<String, List<String>> categoriesNames = searchService.searchPoiCategories(locale);
        if (categoriesNames != null) {
            return ResponseEntity.ok(gson.toJson(categoriesNames));
        } else {
            return ResponseEntity.badRequest().body("Error get poi categories!");
        }
    }
    
    @GetMapping(path = {"/get-top-filters"}, produces = "application/json")
    @ResponseBody
    public ResponseEntity<String> getTopFilters() {
        List<String> filters = searchService.getTopFilters();
        if (filters != null) {
            return ResponseEntity.ok(gson.toJson(filters));
        } else {
            return ResponseEntity.badRequest().body("Error get top poi filters!");
        }
    }
    
    @GetMapping(path = {"/search-poi-categories"}, produces = "application/json")
    @ResponseBody
    public ResponseEntity<String> searchPoiCategories(@RequestParam String search,
                                                      @RequestParam String locale) {
        Map<String, Map<String, String>> res = searchService.searchPoiCategories(search, locale);
        return ResponseEntity.ok(gson.toJson(res));
    }
    
    @GetMapping(path = {"/get-poi-address"}, produces = "application/json")
    @ResponseBody
    public ResponseEntity<String> getPoiAddress(@RequestParam double lat, @RequestParam double lon) throws IOException, InterruptedException {
        String address = searchService.getPoiAddress(new LatLon(lat, lon));
        return ResponseEntity.ok(gson.toJson(address));
    }

    @MultiPlatform
    @GetMapping(path = {"/get-wiki-data"}, produces = "application/json")
    @ResponseBody
    public ResponseEntity<String> getWikiData(@RequestParam String northWest,
                                              @RequestParam String southEast,
                                              @RequestParam String lang,
                                              @RequestParam (required = false) Integer zoom,
                                              @RequestParam Set<String> filters) {
        FeatureCollection collection = wikiService.getWikidataData(northWest, southEast, lang, filters, zoom);
        return ResponseEntity.ok(gson.toJson(collection));
    }
    
    @GetMapping(path = {"/get-wiki-images"}, produces = "application/json")
    @ResponseBody
    public ResponseEntity<String> getWikiImages(@RequestParam String northWest, @RequestParam String southEast) {
        FeatureCollection collection = wikiService.getImages(northWest, southEast);
        return ResponseEntity.ok(gson.toJson(collection));
    }

    @MultiPlatform
    @RequestMapping(path = {"/parse-image-info"}, produces = "application/json")
    @ResponseBody
    public ResponseEntity<String> parseImageInfo(@RequestBody(required = false) String data,
                                                 @RequestParam(required = false) String imageTitle,
                                                 @RequestParam(required = false) Long pageId,
                                                 @RequestParam(required = false) String lang) throws IOException, SQLException {
        if (imageTitle == null && data == null) {
            return ResponseEntity.badRequest().body("Required imageTitle or data!");
        }

        if (imageTitle == null) {
            // old parsing
            Map<String, String> info = wikiService.parseImageInfo(data);
            return ResponseEntity.ok(gson.toJson(info));
        }
        // ignore data for new parsing
        data = wikiService.getWikiRawDataFromCache(imageTitle, pageId);

        if (data == null) {
            data = wikiService.parseRawImageInfo(imageTitle);
            if (data != null) {
                wikiService.saveWikiRawDataToCache(imageTitle, pageId, data);
            }
        }
        if (data == null) {
            return ResponseEntity.badRequest().body("Error get image info!");
        }
        Map<String, String> info = wikiService.parseImageInfo(data, imageTitle, lang);
        return ResponseEntity.ok(gson.toJson(info));
    }
    
    public record WikiImageInfo(String title, Long pageId, String data) {
    }
    
    @MultiPlatform
    @RequestMapping(path = {"/parse-images-list-info"}, produces = "application/json")
    @ResponseBody
    public ResponseEntity<String> parseImagesListInfo(@RequestBody List<WikiImageInfo> data,
                                                      @RequestParam String lang,
                                                      @RequestParam(name = "deviceid", required = false) Integer deviceId,
                                                      @RequestParam(name = "accessToken", required = false) String accessToken) {
        if (data == null) {
            return ResponseEntity.badRequest().body("Required data!");
        }

        boolean saveToCache = validateUser(deviceId, accessToken);

        Map<String, Map<String, String>> result = new HashMap<>();
        for (WikiImageInfo wikiImageInfo : data) {
            String rawData = wikiImageInfo.data();
            String title = wikiImageInfo.title();
            Long pageId = wikiImageInfo.pageId();
            if (rawData != null && saveToCache) {
                // save immediately to cache
                wikiService.saveWikiRawDataToCache(title, pageId, rawData);
            } else {
                if (rawData == null) {
                    rawData = wikiService.getWikiRawDataFromCache(title, pageId);
                }
                if (rawData == null) {
                    rawData = wikiService.parseRawImageInfo(title);
                }
                if (rawData != null) {
                    wikiService.saveWikiRawDataToCache(title, pageId, rawData);
                }
            }
            if (rawData != null) {
                Map<String, String> info = null;
                try {
                    info = wikiService.parseImageInfo(rawData, title, lang);
                } catch (SQLException | IOException e) {
                    LOGGER.error("Error parse image info: " + title, e);
                }
                result.put(title, info);
            }
        }
        return ResponseEntity.ok(gson.toJson(result));
    }

    private boolean validateUser(Integer deviceId, String accessToken) {
        if (deviceId != null && accessToken != null) {
            CloudUserDevicesRepository.CloudUserDevice dev = checkToken(deviceId, accessToken);
	        return dev != null;
        }
        return false;
    }
    
    @GetMapping(path = {"/get-poi-by-osmid"}, produces = "application/json")
    @ResponseBody
    public ResponseEntity<String> getPoiByOsmId(@RequestParam double lat,
                                                 @RequestParam double lon,
                                                 @RequestParam long osmid,
                                                @RequestParam String type) throws IOException {
        Feature poi = searchService.searchPoiByOsmId(new LatLon(lat, lon), osmid, type);
        return ResponseEntity.ok(gson.toJson(poi));
    }

    @GetMapping(path = {"/get-poi-by-name"}, produces = "application/json")
    @ResponseBody
    public ResponseEntity<String> getPoiByOsmId(@RequestParam double lat,
                                                @RequestParam double lon,
                                                @RequestParam String enName) throws IOException {
        Feature poi = searchService.searchPoiByEnName(new LatLon(lat, lon), enName);
        return ResponseEntity.ok(gson.toJson(poi));
    }
    
    @GetMapping(path = {"/get-wiki-content"}, produces = "application/json")
    @ResponseBody
    public ResponseEntity<String> getWikipediaContent(@RequestParam String title, @RequestParam String lang) {
        String content = wikiService.getWikipediaContent(title, lang);
        return ResponseEntity.ok(gson.toJson(content));
    }
    
    @GetMapping(path = {"/get-wikivoyage-content"}, produces = "application/json")
    @ResponseBody
    public ResponseEntity<String> getWikivoyageContent(@RequestParam String title, @RequestParam String lang) {
        String content = wikiService.getWikivoyageContent(title, lang);
        return ResponseEntity.ok(gson.toJson(content));
    }

    @GetMapping(path = {"/search-transport-stops"}, produces = "application/json")
    @ResponseBody
    public ResponseEntity<String> searchTransportStops(@RequestParam String northWest,
                                                       @RequestParam String southEast) throws IOException {
        SearchService.TransportStopsSearchResult result = searchService.searchTransportStops(northWest, southEast);
        return ResponseEntity.ok(gson.toJson(result));
    }

    @GetMapping(path = {"/get-transport-route"}, produces = "application/json")
    @ResponseBody
    public ResponseEntity<String> getTransportRoute(@RequestParam double lat,
                                                    @RequestParam double lon,
                                                    @RequestParam long stopId,
                                                    @RequestParam long routeId) throws IOException {
        LatLon transportStopCoords = new LatLon(lat, lon);
        SearchService.TransportRouteFeature result = searchService.getTransportRoute(transportStopCoords, stopId, routeId);
        if (result == null) {
            return ResponseEntity.badRequest().body("Error get transport route!");
        }
        return ResponseEntity.ok(gson.toJson(result));
    }

    @GetMapping(path = {"/get-transport-stop"}, produces = "application/json")
    @ResponseBody
    public ResponseEntity<String> getTransportStop(@RequestParam double lat,
                                                    @RequestParam double lon,
                                                    @RequestParam long stopId) throws IOException {
        LatLon transportStopCoords = new LatLon(lat, lon);
        Feature result = searchService.getTransportStop(transportStopCoords, stopId);
        if (result == null) {
            return ResponseEntity.badRequest().body("Error get transport stop!");
        }
        return ResponseEntity.ok(gson.toJson(result));
    }

    @PostMapping(path = {"/get-poi-photos"}, produces = "application/json")
    @ResponseBody
    public ResponseEntity<String> getPoiPhotos(@RequestBody Map<String, String> tags) {
        WikiHelper.WikiTagData wikiTagData = WikiHelper.INSTANCE.extractWikiTagData(tags);

        String wikidataId = wikiTagData.getWikidataId();
        String wikiCategory = wikiTagData.getWikiCategory();
        String wikiTitle = wikiTagData.getWikiTitle();
        List<WikiImage> wikiImages = wikiTagData.getWikiImages();

        return getPhotos(wikidataId, wikiCategory, wikiTitle, wikiImages);
    }

	private ResponseEntity<String> getPhotos(String wikidataId, String wikiCategory, String wikiTitle,
			List<WikiImage> wikiImages) {
		Set<Map<String, Object>> images = wikiService.processWikiImagesWithDetails(wikidataId, wikiCategory, wikiTitle);

        images.forEach(img -> {
            WikiImage wikiImage = new WikiImage("", (String) img.get("image"), "", "", "");
            WikiMetadata.Metadata metadata = wikiImage.getMetadata();
            metadata.setDate((String) img.get("date"));
            metadata.setAuthor((String) img.get("author"));
            metadata.setLicense((String) img.get("license"));
            metadata.setDescription((String) img.get("description"));
            wikiImage.setMediaId((Long) img.get("mediaId"));

            wikiImages.add(wikiImage);
        });
        FeatureCollection featureCollection = wikiService.convertToFeatureCollection(wikiImages);
        return ResponseEntity.ok(gson.toJson(featureCollection));
	}
    
    @GetMapping(path = {"/get-photos"}, produces = "application/json")
    @ResponseBody
    public ResponseEntity<String> gePhotos(@RequestParam(required = false) String wikidataId, 
    		@RequestParam(required = false) String wikiCategory, @RequestParam(required = false) String wikiTitle) {
        return getPhotos(wikidataId, wikiCategory, wikiTitle, new ArrayList<WikiImage>());
    }

    @GetMapping(path = {"/parse-location"}, produces = "application/json")
    @ResponseBody
    public ResponseEntity<String> parseLocation(@RequestParam String location) {
        LatLon coordinates = searchService.parseLocation(location);
        if (coordinates == null) {
            return ResponseEntity.ok(gson.toJson(null));
        }
        Map<String, Double> result = new HashMap<>();
        result.put("lat", coordinates.getLatitude());
        result.put("lon", coordinates.getLongitude());
        return ResponseEntity.ok(gson.toJson(result));
    }
}
