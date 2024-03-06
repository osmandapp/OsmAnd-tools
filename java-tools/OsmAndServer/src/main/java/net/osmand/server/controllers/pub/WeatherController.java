
package net.osmand.server.controllers.pub;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

import net.osmand.binary.GeocodingUtilities.GeocodingResult;
import net.osmand.server.api.services.OsmAndMapsService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import com.google.gson.Gson;

import net.osmand.obf.preparation.IndexWeatherData;
import net.osmand.obf.preparation.IndexWeatherData.WeatherTiff;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
@RequestMapping("/weather-api")
public class WeatherController {
	
	protected static final Log LOGGER = LogFactory.getLog(WeatherController.class);
	
	// Initial increment value for the weather data time interval
	private static final int INITIAL_INCREMENT = 1;
	
	// Increment value for the ECMWF weather data time interval and for the weekly forecast
	private static final int ECWMF_INITIAL_INCREMENT = 3;
	
	// Increment value for the ECMWF weather data time interval after the first 120 hours
	private static final int ECWMF_INCREMENT = 6;
	
	@Autowired
	OsmAndMapsService osmAndMapsService;
	
	private static final String ECWMF_WEATHER_TYPE = "ecmwf";
	
	Gson gson = new Gson();
	SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd_HH");
	{
		sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
	}

	@Value("${osmand.weather.location}")
	String weatherLocation;
	
	@RequestMapping(path = "/point-info", produces = {MediaType.APPLICATION_JSON_VALUE})
	public ResponseEntity<?> getWeatherForecast(@RequestParam double lat,
	                                            @RequestParam double lon,
	                                            @RequestParam String weatherType,
	                                            @RequestParam(defaultValue = "false") boolean week) {
		File folder = new File(weatherLocation + weatherType + "/tiff/");
		List<Object[]> dt = new ArrayList<>();
		int increment = INITIAL_INCREMENT;
		if (folder.exists()) {
			Calendar c = Calendar.getInstance();
			c.set(Calendar.MINUTE, 0);
			boolean isShifted = false;
			if (week || weatherType.equals(ECWMF_WEATHER_TYPE)) {
				increment = ECWMF_INITIAL_INCREMENT;
				c.setTimeZone(TimeZone.getTimeZone("UTC"));
				int h = c.get(Calendar.HOUR);
				c.set(Calendar.HOUR, h - (h % increment));
			}
			while (true) {
				File fl = new File(folder, sdf.format(c.getTime()) + "00.tiff");
				if (fl.exists()) {
					try {
						Object[] data = new Object[7];
						data[0] = c.getTimeInMillis();
						data[1] = sdf.format(c.getTime()).substring(4).replace('_', ' ') + ":00";
						WeatherTiff wt = new IndexWeatherData.WeatherTiff(fl);
						for (int i = 0; i < wt.getBands() && i < 5; i++) {
							data[2 + i] = wt.getValue(i, lat, lon);
						}
						dt.add(data);
					} catch (IOException e) {
						LOGGER.warn(String.format("Error reading %s: %s", fl.getName(), e.getMessage()), e);
					}
				} else {
					if (weatherType.equals(ECWMF_WEATHER_TYPE) && increment != ECWMF_INCREMENT) {
						increment = ECWMF_INCREMENT;
						isShifted = true;
					} else {
						break;
					}
				}
				if (isShifted) {
					c.add(Calendar.HOUR, ECWMF_INCREMENT - ECWMF_INITIAL_INCREMENT);
					isShifted = false;
				} else {
					c.add(Calendar.HOUR, increment);
				}
			}
		}
		return ResponseEntity.ok(gson.toJson(dt));
	}
	
	static class AddressInfo {
		Map<String, String> cityLocalNames;
		
		AddressInfo(Map<String, String> names) {
			this.cityLocalNames = names;
		}
	}
	
	@GetMapping(path = {"/get-address-by-latlon"})
	@ResponseBody
	public String getAddressByLatlon(@RequestParam("lat") double lat, @RequestParam("lon") double lon) throws IOException, InterruptedException {
		List<GeocodingResult> list = osmAndMapsService.geocoding(lat, lon);
		
		Optional<GeocodingResult> nearestResult = list.stream()
				.min(Comparator.comparingDouble(GeocodingResult::getDistance));
		
		if (nearestResult.isPresent()) {
			GeocodingResult result = nearestResult.get();
			return gson.toJson(new AddressInfo(result.city.getNamesMap(true)));
			
		}
		return gson.toJson(new AddressInfo(Collections.emptyMap()));
	}
	
}
