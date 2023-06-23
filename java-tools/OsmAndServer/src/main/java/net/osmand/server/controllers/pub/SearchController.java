package net.osmand.server.controllers.pub;

import com.google.gson.Gson;
import net.osmand.data.Amenity;
import net.osmand.data.Street;
import net.osmand.search.core.ObjectType;
import net.osmand.search.core.SearchResult;
import net.osmand.server.api.services.OsmAndMapsService;
import net.osmand.server.api.services.SearchService;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Controller
@RequestMapping("/routing/search")
public class SearchController {
    
    protected static final Log LOGGER = LogFactory.getLog(SearchController.class);
    Gson gson = new Gson();
    
    @Autowired
    OsmAndMapsService osmAndMapsService;
    
    @Autowired
    SearchService searchService;
    
    @RequestMapping(path = "/search", produces = {MediaType.APPLICATION_JSON_VALUE})
    public ResponseEntity<String> search(@RequestParam double lat, @RequestParam double lon, @RequestParam String search) throws IOException {
        if (!osmAndMapsService.validateAndInitConfig()) {
            return osmAndMapsService.errorConfig();
        }
        try {
            List<SearchResult> res = searchService.search(lat, lon, search);
            List<RoutingController.Feature> features = new ArrayList<>();
            int pos = 0;
            int posLoc = 0;
            for (SearchResult sr : res) {
                pos++;
                if (sr.location != null) {
                    posLoc++;
                    double loc = MapUtils.getDistance(sr.location, lat, lon) / 1000.0;
                    String typeString = "";
                    if (!Algorithms.isEmpty(sr.localeRelatedObjectName)) {
                        typeString += " " + sr.localeRelatedObjectName;
                        if (sr.distRelatedObjectName != 0) {
                            typeString += " " + (int) (sr.distRelatedObjectName / 1000.f) + " km";
                        }
                    }
                    if (sr.objectType == ObjectType.HOUSE && (sr.relatedObject instanceof Street)) {
                            typeString += " " + ((Street) sr.relatedObject).getCity().getName();
                        
                    }
                    if (sr.objectType == ObjectType.LOCATION) {
                        typeString += " " + osmAndMapsService.getOsmandRegions().getCountryName(sr.location);
                    }
                    if (sr.object instanceof Amenity) {
                        typeString += " " + ((Amenity) sr.object).getSubType();
                        if (((Amenity) sr.object).isClosed()) {
                            typeString += " (CLOSED)";
                        }
                    }
                    String r = String.format("%d. %s %s [%.2f km, %d, %s, %.2f] ", pos, sr.localeName, typeString, loc,
                            sr.getFoundWordCount(), sr.objectType, sr.getUnknownPhraseMatchWeight());
                    features.add(new RoutingController.Feature(RoutingController.Geometry.point(sr.location)).prop("description", r).
                            prop("index", pos).prop("locindex", posLoc).prop("distance", loc).prop("type", sr.objectType));
                }
            }
            return ResponseEntity.ok(gson.toJson(new RoutingController.FeatureCollection(features.toArray(new RoutingController.Feature[0]))));
        } catch (IOException | RuntimeException e) {
            LOGGER.error(e.getMessage(), e);
            throw e;
        }
    }
    
    @RequestMapping(path = {"/search-poi"}, produces = "application/json")
    @ResponseBody
    public ResponseEntity<String> searchPoi(@RequestBody SearchService.PoiSearchData searchData) throws IOException {
        SearchService.PoiSearchResult poiSearchResult = searchService.searchPoi(searchData);
        if (poiSearchResult != null) {
            return ResponseEntity.ok(gson.toJson(poiSearchResult));
        } else {
            return ResponseEntity.badRequest().body("Error search poi!");
        }
    }
    
    @GetMapping(path = {"/get-poi-categories"}, produces = "application/json")
    @ResponseBody
    public ResponseEntity<String> getPoiCategories() {
        Map<String, List<String>> categoriesNames = searchService.searchPoiCategories();
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
    public ResponseEntity<String> searchPoiCategories(@RequestParam String search) throws IOException {
        Map<String, Map<String, String>> res = searchService.searchPoiCategories(search);
        if (!res.isEmpty()) {
            return ResponseEntity.ok(gson.toJson(res));
        } else {
            return ResponseEntity.badRequest().body("Error get poi categories!");
        }
    }
}
