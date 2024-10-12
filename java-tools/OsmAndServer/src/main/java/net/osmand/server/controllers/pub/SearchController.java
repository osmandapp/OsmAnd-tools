package net.osmand.server.controllers.pub;


import java.io.IOException;
import java.sql.SQLException;
import java.util.*;

import net.osmand.server.api.repo.PremiumUserDevicesRepository;
import net.osmand.server.utils.MultiPlatform;
import net.osmand.util.Algorithms;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.google.gson.Gson;

import net.osmand.data.LatLon;
import net.osmand.server.api.services.OsmAndMapsService;
import net.osmand.server.api.services.SearchService;
import net.osmand.server.api.services.WikiService;
import net.osmand.server.controllers.pub.GeojsonClasses.FeatureCollection;
import org.xmlpull.v1.XmlPullParserException;

import static net.osmand.server.controllers.pub.GeojsonClasses.*;
@Controller
@RequestMapping("/routing/search")
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
    protected PremiumUserDevicesRepository devicesRepository;
    
    private PremiumUserDevicesRepository.PremiumUserDevice checkToken(int deviceId, String accessToken) {
        PremiumUserDevicesRepository.PremiumUserDevice d = devicesRepository.findById(deviceId);
        if (d != null && Algorithms.stringsEqual(d.accesstoken, accessToken)) {
            return d;
        }
        return null;
    }
    
    @RequestMapping(path = "/search", produces = {MediaType.APPLICATION_JSON_VALUE})
    public ResponseEntity<String> search(@RequestParam double lat,
                                         @RequestParam double lon,
                                         @RequestParam String text,
                                         @RequestParam String locale) throws IOException, XmlPullParserException {
        if (!osmAndMapsService.validateAndInitConfig()) {
            return osmAndMapsService.errorConfig();
        }
        List<Feature> features = searchService.search(lat, lon, text, locale);
        return ResponseEntity.ok(gson.toJson(new FeatureCollection(features.toArray(new Feature[0]))));
    }
    
    @RequestMapping(path = {"/search-poi"}, produces = "application/json")
    @ResponseBody
    public ResponseEntity<String> searchPoi(@RequestBody SearchService.PoiSearchData searchData,
                                            @RequestParam String locale,
                                            @RequestParam double lat,
                                            @RequestParam double lon) throws IOException {
        SearchService.PoiSearchResult poiSearchResult = searchService.searchPoi(searchData, locale, new LatLon(lat, lon));
        return ResponseEntity.ok(gson.toJson(poiSearchResult));
    }
    
    @GetMapping(path = {"/get-poi-categories"}, produces = "application/json")
    @ResponseBody
    public ResponseEntity<String> getPoiCategories(@RequestParam String locale) throws XmlPullParserException, IOException {
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
    
    @GetMapping(path = {"/get-wiki-data"}, produces = "application/json")
    @ResponseBody
    public ResponseEntity<String> getWikiData(@RequestParam String northWest,
                                              @RequestParam String southEast,
                                              @RequestParam String lang,
                                              @RequestParam int zoom,
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
                                                      @RequestParam(name = "deviceid", required = false) int deviceId,
                                                      @RequestParam(name = "accessToken", required = false) String accessToken) {
        if (data == null) {
            return ResponseEntity.badRequest().body("Required data!");
        }
        // validate user
        boolean saveToCache = false;
        if (deviceId != 0 && accessToken != null) {
            PremiumUserDevicesRepository.PremiumUserDevice dev = checkToken(deviceId, accessToken);
            if (dev != null) {
                saveToCache = true;
            }
        }
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
    
    @GetMapping(path = {"/get-poi-by-osmid"}, produces = "application/json")
    @ResponseBody
    public ResponseEntity<String> getPoiByOsmId(@RequestParam double lat,
                                                 @RequestParam double lon,
                                                 @RequestParam long osmid,
                                                @RequestParam String type) throws IOException {
        Feature poi = searchService.searchPoiByOsmId(new LatLon(lat, lon), osmid, type);
        return ResponseEntity.ok(gson.toJson(poi));
    }
    
    @GetMapping(path = {"/get-wiki-photos"}, produces = "application/json")
    @ResponseBody
    public ResponseEntity<String> getWikiPhotosById(@RequestParam long id, @RequestParam double lat, @RequestParam double lon) {
        FeatureCollection collection = wikiService.getImagesById(id, lat, lon);
        return ResponseEntity.ok(gson.toJson(collection));
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
    
    @GetMapping(path = {"/get-poi-photos"}, produces = "application/json")
    @ResponseBody
    public ResponseEntity<String> getPoiPhotos(@RequestParam(required = false) String article,
                                               @RequestParam(required = false) String category,
                                               @RequestParam(required = false) String wiki) {
        Set<Map<String, Object>> images = wikiService.processWikiImagesWithDetails(article, category, wiki);
        FeatureCollection featureCollection = wikiService.convertToFeatureCollection(images);
        return ResponseEntity.ok(gson.toJson(featureCollection));
    }
}
