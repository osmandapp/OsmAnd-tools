package net.osmand.server.services.geoip;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

@Service
public class GeoIpService {
    private static final Log LOGGER = LogFactory.getLog(GeoIpService.class);
    private static final String GEO_IP_SERVICE_PATH_TEMPLATE = "http://localhost:8081/json/{addr}";

    private final RestTemplate restTemplate;

    @Autowired
    public GeoIpService(RestTemplateBuilder builder) {
        this.restTemplate = builder.requestFactory(HttpComponentsClientHttpRequestFactory.class).build();
    }

    private void modifyGeoIpDataIfNeeded(GeoIpData geoIpData) {
        if (geoIpData.isLatPresent() && geoIpData.isLonPresent() && !geoIpData.isLatitudePresent()
                && !geoIpData.isLongitudePresent()) {
            geoIpData.setLatitude(geoIpData.getLat());
            geoIpData.setLongitude(geoIpData.getLon());
        }
        if (!geoIpData.isLatPresent() && !geoIpData.isLonPresent() && geoIpData.isLatitudePresent()
                && geoIpData.isLongitudePresent()) {
            geoIpData.setLat(geoIpData.getLatitude());
            geoIpData.setLongitude(geoIpData.getLon());
        }
    }

    public GeoIpData getGeoIpData(String address) {
        UriComponentsBuilder uriBuilder = UriComponentsBuilder.fromHttpUrl(GEO_IP_SERVICE_PATH_TEMPLATE);
        String uri = uriBuilder.buildAndExpand(address).toString();
        GeoIpData geoIpData = null;
        try {
            geoIpData = restTemplate.getForObject(uri, GeoIpData.class);
            if (geoIpData == null) {
                geoIpData = new GeoIpData();
            } else {
                modifyGeoIpDataIfNeeded(geoIpData);
            }
        } catch (RestClientException ex) {
            LOGGER.error(ex.getMessage(), ex);
        }
        return geoIpData;
    }
}
