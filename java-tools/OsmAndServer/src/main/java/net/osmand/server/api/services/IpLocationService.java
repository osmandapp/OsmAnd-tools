package net.osmand.server.api.services;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@Service
public class IpLocationService {

    private static final Log LOGGER = LogFactory.getLog(IpLocationService.class);

    
    @Value("${geoip.url}")
    private String geoipURL;
    
    private ObjectMapper jsonMapper;
    
    public static final String COUNTRY_CODE = "country_code"; // NL
    public static final String COUNTRY_NAME = "country_name"; // Netherlands
    public static final String REGION_NAME = "region_name"; // North Holland
    public static final String REGION_CODE = "region_code"; // NH
    public static final String CITY = "city"; // Amstelveen
    public static final String ZIP_CODE = "zip_code"; // 1157
    public static final String TIME_ZONE = "time_zone"; // Europe/Amsterdam
    public static final String LATITUDE = "latitude"; // 
    public static final String LONGITUDE = "longitude"; // 
    
	
	public String getLocationAsJson(String remoteAddr) throws IOException {
		HashMap<String, Object> value = getRawData(remoteAddr);
		if (value.containsKey("lat") && !value.containsKey("latitude")) {
			value.put("latitude", value.get("lat"));
		} else if (!value.containsKey("lat") && value.containsKey("latitude")) {
			value.put("lat", value.get("latitude"));
		}
		if (value.containsKey("lon") && !value.containsKey("longitude")) {
			value.put("longitude", value.get("lon"));
		} else if (!value.containsKey("lon") && value.containsKey("longitude")) {
			value.put("lon", value.get("longitude"));
		}
		return jsonMapper.writeValueAsString(value);
	}

	private HashMap<String, Object> getRawData(String remoteAddr) throws IOException, MalformedURLException,
			JsonParseException, JsonMappingException {
		URLConnection conn = new URL(geoipURL + remoteAddr).openConnection();
		TypeReference<HashMap<String, Object>> typeRef = new TypeReference<HashMap<String, Object>>() {
		};
		HashMap<String, Object> value = jsonMapper.readValue(conn.getInputStream(), typeRef);
		conn.getInputStream().close();
		return value;
	}

	public String getField(String hostAddress, String field) {
		try {
			HashMap<String, Object> mp = getRawData(hostAddress);
			if(mp.containsKey(field)) {
				return mp.get(field).toString();
			}
		} catch (Exception ex) {
			LOGGER.warn(ex.getMessage(), ex);
		}
		return "";
	}
}
