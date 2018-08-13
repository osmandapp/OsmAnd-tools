package net.osmand.server.mapillary.services;

import net.osmand.Location;
import net.osmand.server.mapillary.CameraPlace;
import net.osmand.server.mapillary.wikidata.ImageInfo;
import net.osmand.server.mapillary.wikidata.Page;
import net.osmand.server.mapillary.wikidata.WikiBatch;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.geojson.Feature;
import org.geojson.FeatureCollection;
import org.geojson.LngLatAlt;
import org.geojson.Point;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import java.util.List;
import java.util.Map;

@Service
public class ImageService {

    private static final Log LOGGER = LogFactory.getLog(ImageService.class);

    private static final int SEARCH_RADIUS = 50;

    private static final String RESULT_MAP_ARR = "arr";
    private static final String RESULT_MAP_HALFVISARR = "halfvisarr";

    private static final String WIKIMEDIA = "wikimedia.org";
    private static final String WIKIPEDIA = "wikipedia.org";
    private static final String WIKI_FILE_PREFIX = "File:";

    @Value("${mapillary.clientid}")
    private String mapillaryClientId;

    private final RestTemplate restTemplate;

    @Autowired
    public ImageService(RestTemplateBuilder builder) {
        this.restTemplate = builder.requestFactory(HttpComponentsClientHttpRequestFactory.class).build();
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

    private CameraPlace parseFeature(Feature feature, double targetLat, double targetLon) {
        LngLatAlt coordinates = ((Point) feature.getGeometry()).getCoordinates();
        Map<String, Object> properties = feature.getProperties();
        CameraPlace.CameraPlaceBuilder cameraBuilder = new CameraPlace.CameraPlaceBuilder();
        cameraBuilder.setType("mapillary-photo");
        cameraBuilder.setTimestamp((String) properties.get("captured_at"));
        String key = (String) properties.get("key");
        cameraBuilder.setKey(key);
        cameraBuilder.setCa(parseCameraAngle(properties.get("ca")));
        cameraBuilder.setImageUrl(buildOsmandImageUrl(false, key));
        cameraBuilder.setImageHiresUrl(buildOsmandImageUrl(true, key));
        cameraBuilder.setUrl(buildOsmandPhotoViewerUrl(key));
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

    private String buildUrl(String rootUrl, boolean hires, String photoIdKey) {
        UriComponentsBuilder uriBuilder =
                UriComponentsBuilder.fromHttpUrl(rootUrl);
        if (hires) {
            uriBuilder.queryParam(OsmandPhotoApiConstants.OSMAND_PARAM_HIRES, true);
        }
        uriBuilder.queryParam(OsmandPhotoApiConstants.OSMAND_PARAM_PHOTO_ID, photoIdKey);
        return uriBuilder.build().toString();
    }

    private String buildOsmandImageUrl(boolean hires, String photoIdKey) {
        return buildUrl(OsmandPhotoApiConstants.OSMAND_GET_IMAGE_ROOT_URL, hires, photoIdKey);
    }

    private String buildOsmandPhotoViewerUrl(String photoIdKey) {
        return buildUrl(OsmandPhotoApiConstants.OSMAND_PHOTO_VIEWER_ROOT_URL, false, photoIdKey);
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

    private void splitCameraPlaceByAngel(CameraPlace cp, Map<String, List<CameraPlace>> resultMap) {
        double ca = cp.getCa();
        double bearing = cp.getBearing();
        if (ca >= 0 && angleDiff(bearing - ca, 30.0)) {
            resultMap.get(RESULT_MAP_ARR).add(cp);
        } else if (ca >= 0 && angleDiff(bearing - ca, 60.0) || ca < 0) {
            resultMap.get(RESULT_MAP_HALFVISARR).add(cp);
        }
    }

    private boolean isPrimaryCameraPlace(CameraPlace cp, String primaryImageKey) {
        return !isEmpty(primaryImageKey) && cp != null && cp.getKey() != null && cp.getKey().equals(primaryImageKey);
    }

    private boolean isEmpty(String str) {
        return str == null || str.isEmpty();
    }

    public CameraPlace processMapillaryData(double lat, double lon, String primaryImageKey,
                                              Map<String, List<CameraPlace>> resultMap) {
        UriComponentsBuilder uriBuilder = UriComponentsBuilder.fromHttpUrl(MapillaryApiConstants.MAPILLARY_API_URL);
        uriBuilder.queryParam(MapillaryApiConstants.MAPILLARY_PARAM_CLOSE_TO, lon, lat);
        uriBuilder.queryParam(MapillaryApiConstants.MAPILLARY_PARAM_RADIUS, SEARCH_RADIUS);
        uriBuilder.queryParam(MapillaryApiConstants.MAPILLARY_PARAM_CLIENT_ID, mapillaryClientId);
        FeatureCollection featureCollection = restTemplate.getForObject(uriBuilder.build().toString(),
                FeatureCollection.class);
        CameraPlace primaryPlace = null;
        if (featureCollection != null) {
            for (Feature feature : featureCollection.getFeatures()) {
                CameraPlace cp = parseFeature(feature, lat, lon);
                if (isPrimaryCameraPlace(cp, primaryImageKey)) {
                    primaryPlace = cp;
                    continue;
                }
                splitCameraPlaceByAngel(cp, resultMap);
            }
        }
        if (primaryPlace == null && !isEmpty(primaryImageKey)) {
            try {
                uriBuilder = UriComponentsBuilder.fromHttpUrl(MapillaryApiConstants.MAPILLARY_API_URL);
                uriBuilder.path(primaryImageKey).queryParam(MapillaryApiConstants.MAPILLARY_PARAM_CLIENT_ID, mapillaryClientId);
                Feature f = restTemplate.getForObject(uriBuilder.build().toString(), Feature.class);
                primaryPlace = parseFeature(f, lat, lon);
            } catch (RestClientException ex) {
                LOGGER.error(ex.getMessage(), ex);
            }
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
        return osmImage.startsWith(WIKI_FILE_PREFIX) || ((osmImage.contains(WIKIMEDIA) || osmImage.contains(WIKIPEDIA))
                && osmImage.contains(WIKI_FILE_PREFIX));
    }

    private boolean isFileMissing(WikiBatch batch) {
        Page page = batch.getQuery().getPages().get(0);
        return page.getMissing() != null && page.getMissing();
    }

    private boolean isImageInfoMissing(WikiBatch batch) {
        return batch.getQuery().getPages().get(0).getImageinfo().isEmpty();
    }

    private ImageInfo getImageInfo(WikiBatch batch) {
        return batch.getQuery().getPages().get(0).getImageinfo().get(0);
    }

    private String parseTitle(WikiBatch batch) {
        String title = batch.getQuery().getPages().get(0).getTitle();
        if (title.contains(WIKI_FILE_PREFIX)) {
            title = title.substring(title.indexOf(WIKI_FILE_PREFIX) + 5);
        }
        return title;
    }

    private CameraPlace parseWikimediaImage(double targetLat, double targetLon, String filename, WikiBatch batch) {
        String title = parseTitle(batch);
        ImageInfo imageInfo = getImageInfo(batch);
        CameraPlace.CameraPlaceBuilder builder = new CameraPlace.CameraPlaceBuilder();
        builder.setType("wikimedia-photo");
        builder.setTimestamp(imageInfo.getTimestamp());
        builder.setKey(filename);
        builder.setTitle(title);
        builder.setImageUrl(imageInfo.getThumburl());
        builder.setImageHiresUrl(imageInfo.getUrl());
        builder.setUrl(imageInfo.getDescriptionurl());
        builder.setExternalLink(false);
        builder.setUsername(imageInfo.getUser());
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
            if (filename == null) {
                return null;
            }
            UriComponentsBuilder uriBuilder = UriComponentsBuilder.fromHttpUrl(WikimediaApiConstants.WIKIMEDIA_API_URL);
            uriBuilder.queryParam(WikimediaApiConstants.WIKIMEDIA_PARAM_TITLES, filename);
            WikiBatch batch = restTemplate.getForObject(uriBuilder.build().toString(), WikiBatch.class);
            if (batch == null) {
                return null;
            }
            if (batch.getQuery().getPages().isEmpty()) {
                return null;
            }
            if (isFileMissing(batch)) {
                return null;
            }
            if (isImageInfoMissing(batch)) {
                return null;
            }
            primaryImage = parseWikimediaImage(lat, lon, filename, batch);
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
        static final String OSMAND_GET_IMAGE_ROOT_URL = "https://osmand.net/api/mapillary/get_photo.php";
        static final String OSMAND_PHOTO_VIEWER_ROOT_URL = "https://osmand.net/api/mapillary/photo-viewer.php";
        static final String OSMAND_PARAM_PHOTO_ID = "photo_id";
        static final String OSMAND_PARAM_HIRES = "hires";
    }
}
