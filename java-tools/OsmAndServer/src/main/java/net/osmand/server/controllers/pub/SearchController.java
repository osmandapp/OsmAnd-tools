package net.osmand.server.controllers.pub;


import java.io.IOException;
import java.util.*;

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

import net.osmand.data.Amenity;
import net.osmand.data.LatLon;
import net.osmand.data.Street;
import net.osmand.search.core.ObjectType;
import net.osmand.search.core.SearchResult;
import net.osmand.server.api.services.OsmAndMapsService;
import net.osmand.server.api.services.SearchService;
import net.osmand.server.api.services.WikiService;
import net.osmand.server.controllers.pub.GeojsonClasses.FeatureCollection;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;
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
}
