package net.osmand.server.api.services;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Collectors;

import javax.servlet.AsyncContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import net.osmand.Location;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.geojson.Feature;
import org.geojson.FeatureCollection;
import org.geojson.LngLatAlt;
import org.geojson.Point;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.http.HttpHeaders;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;

@Service
public class PlacesService {

    private static final int TIMEOUT = 20000;
    private static final int THREADS_FOR_PROC = 30;
    
    private static final Log LOGGER = LogFactory.getLog(PlacesService.class);

    private static final int SEARCH_RADIUS = 50;

    private static final String WIKIMEDIA = "wikimedia.org";
    private static final String WIKIPEDIA = "wikipedia.org";
    private static final String WIKI_FILE_PREFIX = "File:";

    @Value("${mapillary.clientid}")
    private String mapillaryClientId;


    private final RestTemplate restTemplate;

	private ThreadPoolTaskExecutor executor;

    @Autowired
    public PlacesService(RestTemplateBuilder builder) {
        this.restTemplate = builder.requestFactory(HttpComponentsClientHttpRequestFactory.class)
        		.setConnectTimeout(TIMEOUT).setReadTimeout(TIMEOUT).build();
        
        this.executor = new ThreadPoolTaskExecutor();
    	executor.setThreadNamePrefix("ImageService");
 		executor.setCorePoolSize(THREADS_FOR_PROC);
 		executor.setKeepAliveSeconds(60);
 		executor.setAllowCoreThreadTimeOut(true);
 		executor.setRejectedExecutionHandler(new ThreadPoolExecutor.AbortPolicy());
 		executor.initialize();
    }
    
	public void processPlacesAround(HttpHeaders headers,
            HttpServletRequest request,
            HttpServletResponse response, ObjectMapper jsonMapper, double lat, double lon) {
		AsyncContext asyncCtx = request.startAsync(request, response);
		long start = System.currentTimeMillis();
		executor.submit(() -> {
			try {
				if(System.currentTimeMillis() - start > TIMEOUT * 3) {
					// waited too long (speedup queue processing)
					response.getWriter().println("{'features':[]}");
					asyncCtx.complete();
					return;
				}
				// request.getParameter("mloc")
				// request.getParameter("app")
				// request.getParameter("lang")
				String osmImage = request.getParameter("osm_image");
				String osmMapillaryKey = request.getParameter("osm_mapillary_key");

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
					response.getWriter().println("{'features':[]}");
					asyncCtx.complete();
					return;
				}
				List<CameraPlace> arr = new ArrayList<>();
				List<CameraPlace> halfvisarr = new ArrayList<>();

				CameraPlace wikimediaPrimaryCameraPlace = processWikimediaData(lat, lon, osmImage);
				CameraPlace mapillaryPrimaryCameraPlace = processMapillaryData(lat, lon, osmMapillaryKey, arr, halfvisarr,
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
				response.getWriter().println(
						String.format("{%s:%s}", jsonMapper.writeValueAsString("features"),
								jsonMapper.writeValueAsString(arr)));
				asyncCtx.complete();
			} catch (Exception e) {
				LOGGER.error("Error processing places: " + e.getMessage());
			}
		});
	}

	private List<CameraPlace> sortByDistance(List<CameraPlace> arr) {
        return arr.stream().sorted(Comparator.comparing(CameraPlace::getDistance)).collect(Collectors.toList());
    }

    private CameraPlace createEmptyCameraPlaceWithTypeOnly(String type) {
        CameraPlace.CameraPlaceBuilder builder = new CameraPlace.CameraPlaceBuilder();
        builder.setType(type);
        return builder.build();
    }
    
    


    private double computeInitialBearing(double cameraLat, double cameraLon, double targetLat, double targetLon) {
        Location cameraLocation = new Location("mapillary");
        cameraLocation.setLatitude(cameraLat);
        cameraLocation.setLongitude(cameraLon);
        Location targetLocation = new Location("target");
        targetLocation.setLatitude(targetLat);
        targetLocation.setLongitude(targetLon);
        return cameraLocation.bearingTo(targetLocation);
    }

    private double computeDistance(double cameraLat, double cameraLon, double targetLat, double targetLon) {
        Location cameraLocation = new Location("mapillary");
        cameraLocation.setLatitude(cameraLat);
        cameraLocation.setLongitude(cameraLon);
        Location targetLocation = new Location("target");
        targetLocation.setLatitude(targetLat);
        targetLocation.setLongitude(targetLon);
        return cameraLocation.distanceTo(targetLocation);
    }

    private double parseCameraAngle(Object cameraAngle) {
        return Double.parseDouble(String.valueOf(cameraAngle));
    }

    private CameraPlace parseFeature(Feature feature, double targetLat, double targetLon, String host, String proto) {
        LngLatAlt coordinates = ((Point) feature.getGeometry()).getCoordinates();
        Map<String, Object> properties = feature.getProperties();
        CameraPlace.CameraPlaceBuilder cameraBuilder = new CameraPlace.CameraPlaceBuilder();
        cameraBuilder.setType("mapillary-photo");
        cameraBuilder.setTimestamp((String) properties.get("captured_at"));
        String key = (String) properties.get("key");
        cameraBuilder.setKey(key);
        cameraBuilder.setCa(parseCameraAngle(properties.get("ca")));
        cameraBuilder.setImageUrl(buildOsmandImageUrl(false, key, host, proto));
        cameraBuilder.setImageHiresUrl(buildOsmandImageUrl(true, key, host, proto));
        cameraBuilder.setUrl(buildOsmandPhotoViewerUrl(key, host, proto));
        cameraBuilder.setExternalLink(false);
        cameraBuilder.setUsername((String) properties.get("username"));
        cameraBuilder.setLat(coordinates.getLatitude());
        cameraBuilder.setLon(coordinates.getLongitude());
        cameraBuilder.setTopIcon("ic_logo_mapillary");
        double bearing = computeInitialBearing(coordinates.getLatitude(), coordinates.getLongitude(), targetLat,
                targetLon);
        cameraBuilder.setBearing(bearing);
        cameraBuilder.setDistance(computeDistance(coordinates.getLatitude(), coordinates.getLongitude(), targetLat,
                targetLon));
        return cameraBuilder.build();
    }

    private String buildUrl(String path, boolean hires, String photoIdKey, String host, String proto) {
        UriComponentsBuilder uriBuilder = UriComponentsBuilder.newInstance();
        uriBuilder.scheme(proto);
        uriBuilder.host(host);
        uriBuilder.path(path);
        if (hires) {
            uriBuilder.queryParam(OsmandPhotoApiConstants.OSMAND_PARAM_HIRES, true);
        }
        uriBuilder.queryParam(OsmandPhotoApiConstants.OSMAND_PARAM_PHOTO_ID, photoIdKey);
        return uriBuilder.build().toString();
    }

    private String buildOsmandImageUrl(boolean hires, String photoIdKey, String host, String proto) {
        return buildUrl(OsmandPhotoApiConstants.OSMAND_GET_IMAGE_ROOT_PATH, hires, photoIdKey, host, proto);
    }

    private String buildOsmandPhotoViewerUrl(String photoIdKey, String host, String proto) {
        return buildUrl(OsmandPhotoApiConstants.OSMAND_PHOTO_VIEWER_ROOT_PATH, false, photoIdKey, host, proto);
    }

    private boolean angleDiff(double angle, double diff) {
        if (angle > 360.0) {
            angle -= 360.0;
        }
        if (angle < -360.0) {
            angle += 360.0;
        }
        return Math.abs(angle) < diff;
    }

    private void splitCameraPlaceByAngel(CameraPlace cp, List<CameraPlace> main, List<CameraPlace> rest) {
        double ca = cp.getCa();
        double bearing = cp.getBearing();
        if (ca > 0d && angleDiff(bearing - ca, 30.0)) {
            main.add(cp);
        } else if (!(ca > 0d && !angleDiff(bearing - ca, 60.0))) {
        	// exclude all with camera angle and angle more than 60 (keep w/o camera and angle < 60)
            rest.add(cp);
        }
    }

    private boolean isPrimaryCameraPlace(CameraPlace cp, String primaryImageKey) {
        return !isEmpty(primaryImageKey) && cp != null && cp.getKey() != null && cp.getKey().equals(primaryImageKey);
    }

    private boolean isEmpty(String str) {
        return str == null || str.isEmpty();
    }

    public CameraPlace processMapillaryData(double lat, double lon, String primaryImageKey,
    		List<CameraPlace> main, List<CameraPlace> rest, String host, String proto) {
		UriComponentsBuilder uriBuilder = UriComponentsBuilder.fromHttpUrl(MapillaryApiConstants.MAPILLARY_API_URL);
		uriBuilder.queryParam(MapillaryApiConstants.MAPILLARY_PARAM_CLOSE_TO, lon, lat);
		uriBuilder.queryParam(MapillaryApiConstants.MAPILLARY_PARAM_RADIUS, SEARCH_RADIUS);
		uriBuilder.queryParam(MapillaryApiConstants.MAPILLARY_PARAM_CLIENT_ID, mapillaryClientId);
		CameraPlace primaryPlace = null;
		try {
			FeatureCollection featureCollection = restTemplate.getForObject(uriBuilder.build().toString(),
					FeatureCollection.class);
			if (featureCollection != null) {
				for (Feature feature : featureCollection.getFeatures()) {
					CameraPlace cp = parseFeature(feature, lat, lon, host, proto);
					if (isPrimaryCameraPlace(cp, primaryImageKey)) {
						primaryPlace = cp;
						continue;
					}
					splitCameraPlaceByAngel(cp, main, rest);
				}
			}
			if (primaryPlace == null && !isEmpty(primaryImageKey)) {

				uriBuilder = UriComponentsBuilder.fromHttpUrl(MapillaryApiConstants.MAPILLARY_API_URL);
				uriBuilder.path(primaryImageKey).queryParam(MapillaryApiConstants.MAPILLARY_PARAM_CLIENT_ID,
						mapillaryClientId);
				Feature f = restTemplate.getForObject(uriBuilder.build().toString(), Feature.class);
				primaryPlace = parseFeature(f, lat, lon, host, proto);
			}
		} catch (RestClientException ex) {
			LOGGER.error("Error Mappillary api " + uriBuilder.build().toString() +": " + ex.getMessage());
		}
		return primaryPlace;
	}


	private String getFilename(String osmImage) {
        if (osmImage.startsWith(WIKI_FILE_PREFIX)) {
            return osmImage;
        }
        return osmImage.substring(osmImage.indexOf(WIKI_FILE_PREFIX));
    }

    private boolean isWikimediaUrl(String osmImage) {
        return osmImage != null && (osmImage.startsWith(WIKI_FILE_PREFIX) ||
                ((osmImage.contains(WIKIMEDIA) || osmImage.contains(WIKIPEDIA))
                && osmImage.contains(WIKI_FILE_PREFIX)));
    }

    private String parseTitle(WikiPage page) {
        String title = page.getTitle();
        if (title.contains(WIKI_FILE_PREFIX)) {
            title = title.substring(title.indexOf(WIKI_FILE_PREFIX) + 5);
        }
        return title;
    }

    private CameraPlace parseWikimediaImage(double targetLat, double targetLon, String filename, 
    		String title, ImageInfo imageInfo ) {
        CameraPlace.CameraPlaceBuilder builder = new CameraPlace.CameraPlaceBuilder();
        builder.setType("wikimedia-photo");
        builder.setTimestamp(imageInfo.timestamp);
        builder.setKey(filename);
        builder.setTitle(title);
        builder.setImageUrl(imageInfo.thumburl);
        builder.setImageHiresUrl(imageInfo.url);
        builder.setUrl(imageInfo.descriptionurl);
        builder.setExternalLink(false);
        builder.setUsername(imageInfo.user);
        builder.setLat(targetLat);
        builder.setLon(targetLon);
        return builder.build();
    }

    public CameraPlace processWikimediaData(double lat, double lon, String osmImage) {
        if (isEmpty(osmImage)) {
            return null;
        }
        CameraPlace primaryImage;
        if (isWikimediaUrl(osmImage)) {
            String filename = getFilename(osmImage);
            if (filename.isEmpty()) {
                return null;
            }
            UriComponentsBuilder uriBuilder = UriComponentsBuilder.fromHttpUrl(WikimediaApiConstants.WIKIMEDIA_API_URL);
            uriBuilder.queryParam(WikimediaApiConstants.WIKIMEDIA_PARAM_TITLES, filename);
            WikiBatch batch = restTemplate.getForObject(uriBuilder.build().toString(), WikiBatch.class);
            if (batch == null || batch.getQuery().getPages().isEmpty()) {
                return null;
            }
            WikiPage page = batch.getQuery().getPages().get(0);
            if (page.getMissing() ) {
                return null;
            }
            if (page.getImageinfo() == null || page.getImageinfo().isEmpty() || 
            		page.getImageinfo().get(0) == null) {
                return null;
            }
            primaryImage = parseWikimediaImage(lat, lon, filename,
            		parseTitle(page), page.getImageinfo().get(0));
        } else {
            CameraPlace.CameraPlaceBuilder builder = new CameraPlace.CameraPlaceBuilder();
            builder.setType("url-photo");
            builder.setImageUrl(osmImage);
            builder.setUrl(osmImage);
            builder.setLat(lat);
            builder.setLon(lon);
            primaryImage = builder.build();
        }
        return primaryImage;
    }

    private static class MapillaryApiConstants {
        static final String MAPILLARY_API_URL = "https://a.mapillary.com/v3/images/";
        static final String MAPILLARY_PARAM_RADIUS = "radius";
        static final String MAPILLARY_PARAM_CLIENT_ID = "client_id";
        static final String MAPILLARY_PARAM_CLOSE_TO = "closeto";
    }

    private static class WikimediaApiConstants {
        static final String WIKIMEDIA_API_URL =
                "https://commons.wikimedia.org/w/api.php?format=json&formatversion=2&action=query&prop=imageinfo" +
                        "&iiprop=timestamp|user|url&iiurlwidth=576";
        static final String WIKIMEDIA_PARAM_TITLES = "titles";
    }

    private static class OsmandPhotoApiConstants {
        static final String OSMAND_GET_IMAGE_ROOT_PATH = "/api/mapillary/get_photo";
        static final String OSMAND_PHOTO_VIEWER_ROOT_PATH = "/api/mapillary/photo-viewer";
        static final String OSMAND_PARAM_PHOTO_ID = "photo_id";
        static final String OSMAND_PARAM_HIRES = "hires";
    }
    
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class ImageInfo {
    	public String timestamp;
        public String user;
        public String thumburl;
        public String url;
        public String descriptionurl;

    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class Query {

        private List<WikiPage> pages;

        public List<WikiPage> getPages() {
            return pages;
        }

        public void setPages(List<WikiPage> pages) {
            this.pages = pages;
        }
    }
    
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class WikiBatch {

        private Boolean batchcomplete;
        private Query query;

        public Boolean getBatchcomplete() {
            return batchcomplete;
        }

        public void setBatchcomplete(Boolean batchcomplete) {
            this.batchcomplete = batchcomplete;
        }

        public Query getQuery() {
            return query;
        }

        public void setQuery(Query query) {
            this.query = query;
        }
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class WikiPage {
        private String title;
        private boolean missing;
        private List<ImageInfo> imageinfo;

        public String getTitle() {
            return title;
        }

        public List<ImageInfo> getImageinfo() {
            return imageinfo;
        }

        public boolean getMissing() {
            return missing;
        }

    }


}
