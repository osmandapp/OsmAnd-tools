
package net.osmand.server.controllers.pub;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

import net.osmand.binary.GeocodingUtilities;
import net.osmand.binary.GeocodingUtilities.GeocodingResult;
import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.data.Amenity;
import net.osmand.data.City;
import net.osmand.data.LatLon;
import net.osmand.data.QuadRect;
import net.osmand.search.SearchUICore;
import net.osmand.search.core.SearchResult;
import net.osmand.server.api.services.OsmAndMapsService;
import net.osmand.server.api.services.SearchService;
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

import static net.osmand.data.City.CityType.valueFromString;

@Controller
@RequestMapping("/weather-api")
public class WeatherController {
	
	protected static final Log LOGGER = LogFactory.getLog(WeatherController.class);
	
	@Autowired
	SearchService searchService;
	
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
						Object[] data = new Object[8];
						data[0] = c.getTimeInMillis();
						data[1] = sdf.format(c.getTime()).substring(4).replace('_', ' ') + ":00";
						WeatherTiff wt = new IndexWeatherData.WeatherTiff(fl);
						for (int i = 0; i < wt.getBands() && i < 5; i++) {
							data[2 + i] = wt.getValue(i, lat, lon, weatherType);
						}
						data[7] = fl.lastModified();
						dt.add(normalizeValues(data, weatherType, increment));
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
	
	private Object[] normalizeValues(Object[] data, String weatherType, int increment) {
		int precipIndex = weatherType.equals(ECWMF_WEATHER_TYPE) ? 4 : 6;
		int pressureIndex = weatherType.equals(ECWMF_WEATHER_TYPE) ? 3 : 4;
		increment = weatherType.equals(ECWMF_WEATHER_TYPE) ? increment : 1;
		data[precipIndex] = ((double) data[precipIndex]) * 3600 / increment;
		data[pressureIndex] = ((double) data[pressureIndex]) * 0.01;
		
		return data;
	}
	
	static class AddressInfo {
		Map<String, String> cityLocalNames;
		LatLon location;
		
		AddressInfo(Map<String, String> names, LatLon location) {
			this.cityLocalNames = names;
			this.location = location;
		}
	}
	
	@GetMapping(path = {"/get-address-by-latlon"})
	@ResponseBody
	public String getAddressByLatlon(@RequestParam double lat, @RequestParam double lon, @RequestParam(required = false) String nw, @RequestParam(required = false) String se) throws IOException, InterruptedException {
		QuadRect searchBbox;
		if (nw == null && se == null) {
			LOGGER.info("Getting city by location");
			return getCityByLocation(lat, lon);
		} else if (nw != null && se != null) {
			LOGGER.info("Getting city by bbox");
			searchBbox = getBbox(lat, lon, nw, se);
		} else {
			return gson.toJson(new AddressInfo(Collections.emptyMap(), new LatLon(lat, lon)));
		}
		return getCityByBbox(lat, lon, searchBbox);
	}
	
	private String getCityByLocation(double lat, double lon) throws IOException, InterruptedException {
		List<GeocodingUtilities.GeocodingResult> list = osmAndMapsService.geocoding(lat, lon);
		
		Optional<GeocodingResult> nearestResult = list.stream()
				.filter(result -> result.city != null)
				.min(Comparator.comparingDouble(GeocodingResult::getDistance));
		
		if (nearestResult.isPresent()) {
			GeocodingResult result = nearestResult.get();
			return gson.toJson(new AddressInfo(result.city.getNamesMap(true), new LatLon(lat, lon)));
			
		}
		return gson.toJson(new AddressInfo(Collections.emptyMap(), new LatLon(lat, lon)));
	}
	
	private String getCityByBbox(double lat, double lon, QuadRect searchBbox) throws IOException {
		Amenity nearestPlace;
		List<BinaryMapIndexReader> usedMapList = new ArrayList<>();
		try {
			List<OsmAndMapsService.BinaryMapIndexReaderReference> mapList = new ArrayList<>();
			mapList.add(osmAndMapsService.getBaseMap());
			usedMapList = osmAndMapsService.getReaders(mapList, null);
			SearchUICore.SearchResultCollection resultCollection = searchService.searchCitiesByBbox(searchBbox, usedMapList);
			nearestPlace = getNearestPlace(resultCollection, lat, lon);
		} finally {
			osmAndMapsService.unlockReaders(usedMapList);
		}
		if (nearestPlace != null) {
			return gson.toJson(new AddressInfo(nearestPlace.getNamesMap(true), nearestPlace.getLocation()));
		} else {
			return gson.toJson(new AddressInfo(Collections.emptyMap(), new LatLon(lat, lon)));
		}
	}
	
	private QuadRect getBbox(double lat, double lon, String nw, String se) {
		List<LatLon> bbox = getBbox(nw, se);
		QuadRect searchBbox = bbox.size() == 2 ? osmAndMapsService.points(null, bbox.get(0), bbox.get(1)) : null;
		if (searchBbox == null) {
			return new QuadRect(lon, lat, lon, lat);
		}
		return searchBbox;
	}
	
	private long getPopulation(Amenity a) {
		String population = a.getAdditionalInfo("population");
		if (population != null) {
			population = population.replaceAll("\\D", "");
			if (population.matches("\\d+")) {
				return Long.parseLong(population);
			}
		}
		City.CityType type = valueFromString(a.getSubType());
		return type.getPopulation();
	}
	
	private List<LatLon> getBbox(String nw, String se) {
		List<LatLon> bbox = new ArrayList<>();
		String[] nwParts = nw.split(",");
		String[] seParts = se.split(",");
		if (nwParts.length == 2 && seParts.length == 2) {
			bbox.add(new LatLon(Double.parseDouble(nwParts[0]), Double.parseDouble(nwParts[1])));
			bbox.add(new LatLon(Double.parseDouble(seParts[0]), Double.parseDouble(seParts[1])));
		}
		return bbox;
	}
	
	private Amenity getNearestPlace(SearchUICore.SearchResultCollection resultCollection, double centerLat, double centerLon) {
		List<SearchResult> foundedPlaces = resultCollection.getCurrentSearchResults();
		List<SearchResult> modifiableFoundedPlaces = new ArrayList<>(foundedPlaces);
		
		modifiableFoundedPlaces.sort((o1, o2) -> {
			if (o1.object instanceof Amenity && o2.object instanceof Amenity) {
				double rating1 = getRating(o1, centerLat, centerLon);
				double rating2 = getRating(o2, centerLat, centerLon);
				return Double.compare(rating2, rating1);
			}
			return 0;
		});
		if (!modifiableFoundedPlaces.isEmpty()) {
			return (Amenity) modifiableFoundedPlaces.get(0).object;
		}
		return null;
	}
	
	private double getRating(SearchResult sr, double centerLat, double centerLon) {
		long population = getPopulation((Amenity) sr.object);
		double lat1 = sr.location.getLatitude();
		double lon1 = sr.location.getLongitude();
		double distance = Math.sqrt(Math.pow(centerLat - lat1, 2) + Math.pow(centerLon - lon1, 2));
		
		return Math.log10(population + 1.0) - distance;
	}
}
