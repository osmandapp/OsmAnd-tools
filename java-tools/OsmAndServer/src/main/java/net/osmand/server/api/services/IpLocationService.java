package net.osmand.server.api.services;

import net.osmand.util.Algorithms;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.maxmind.geoip.Location;
import com.maxmind.geoip.LookupService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Service;
import org.springframework.beans.factory.annotation.Value;

import java.io.File;
import java.util.LinkedHashMap;
import java.util.Map;


@Service
public class IpLocationService {
	public static final String LAT = "lat"; // used by Web
	public static final String LON = "lon"; // used by Web
	public static final String LATITUDE = "latitude"; // used by Android and iOS
	public static final String LONGITUDE = "longitude"; // used by Android and iOS
	public static final String COUNTRY_CODE = "country_code"; // used by OsmAnd-telegram (NL)
	public static final String COUNTRY_NAME = "country_name"; // used by tools (Netherlands)
	public static final String CITY = "city"; // used by tools (Amstelveen)

	@Value("${geoip.city.dat.v4}")
	private String GeoIPCityV4;

	@Value("${geoip.city.dat.v6}")
	private String GeoIPCityV6;

	private final ObjectMapper jsonMapper = new ObjectMapper();
	private static final Log LOG = LogFactory.getLog(IpLocationService.class);

	public String getLocationAsJson(String ip) throws JsonProcessingException {
		return jsonMapper.writeValueAsString(getAllFields(ip));
	}

	public String getField(String ip, String field) {
		String value = getAllFields(ip).get(field);
		return value == null ? "" : value;
	}

	private Map<String, String> getAllFields(String ip) {
		Map<String, String> fields = new LinkedHashMap<>();
		if (!Algorithms.isEmpty(ip)) {
			boolean ipv6 = ip.contains(":");
			File db = new File(ipv6 ? GeoIPCityV6 : GeoIPCityV4);
			try {
				LookupService lookup = new LookupService(db);
				Location location = ipv6 ? lookup.getLocationV6(ip) : lookup.getLocation(ip);
				if (location == null) {
					LOG.warn(String.format("geoiplookup null location for %s", ip));
					return fields;
				}
				if (!Algorithms.isEmpty(location.countryCode)) {
					fields.put(COUNTRY_CODE, location.countryCode);
				}
				if (!Algorithms.isEmpty(location.countryName)) {
					fields.put(COUNTRY_NAME, location.countryName);
				}
				if (!Algorithms.isEmpty(location.city)) {
					fields.put(CITY, location.city);
				}
				if (location.latitude != 0 || location.longitude != 0) {
					fields.put(LAT, String.valueOf(location.latitude));
					fields.put(LON, String.valueOf(location.longitude));
					fields.put(LATITUDE, String.valueOf(location.latitude));
					fields.put(LONGITUDE, String.valueOf(location.longitude));
				}
			} catch (Exception e) {
				LOG.warn(String.format("geoiplookup failed for %s", ip));
				LOG.info(e);
			}
		}
		return fields;
	}
}
