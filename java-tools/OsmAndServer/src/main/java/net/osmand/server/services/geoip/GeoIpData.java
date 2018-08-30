package net.osmand.server.services.geoip;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class GeoIpData {
    @JsonProperty("ip")
    private String ip;
    @JsonProperty("country_code")
    private String countryCode;
    @JsonProperty("country_name")
    private String countryName;
    @JsonProperty("region_code")
    private String regionCode;
    @JsonProperty("region_name")
    private String regionName;
    @JsonProperty("city")
    private String city;
    @JsonProperty("zip_code")
    private String zipCode;
    @JsonProperty("time_zone")
    private String timeZone;
    @JsonProperty("latitude")
    private Double latitude;
    @JsonProperty("longitude")
    private Double longitude;
    @JsonProperty("metro_code")
    private String metroCode;
    @JsonProperty("lat")
    private Double lat;
    @JsonProperty("lon")
    private Double lon;

    public Double getLatitude() {
        return latitude;
    }

    public void setLatitude(Double latitude) {
        this.latitude = latitude;
    }

    public Double getLongitude() {
        return longitude;
    }

    public void setLongitude(Double longitude) {
        this.longitude = longitude;
    }

    public Double getLat() {
        return lat;
    }

    public void setLat(Double lat) {
        this.lat = lat;
    }

    public Double getLon() {
        return lon;
    }

    public void setLon(Double lon) {
        this.lon = lon;
    }

    public boolean isLatitudePresent() {
        return this.latitude != null;
    }

    public boolean isLongitudePresent() {
        return this.longitude != null;
    }

    public boolean isLatPresent() {
        return this.lat != null;
    }

    public boolean isLonPresent() {
        return this.lon != null;
    }
}
