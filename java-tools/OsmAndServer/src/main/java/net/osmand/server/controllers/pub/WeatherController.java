
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
import static net.osmand.obf.preparation.IndexWeatherData.bandIndexByCode;

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
		List<WeatherPoint> dt = new ArrayList<>();
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
						WeatherTiff wt = new IndexWeatherData.WeatherTiff(fl);

						long ts   = c.getTimeInMillis();
						String tm = sdf.format(c.getTime()).substring(4).replace('_', ' ') + ":00";
						long fm   = fl.lastModified();

						WeatherPoint row = buildPoint(wt, lat, lon, weatherType, ts, tm, fm, increment);
						dt.add(row);
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

	static class WeatherPoint {
		long ts;
		String time;
		Double temp;
		Double precip;
		Double wind;
		Double press;
		Double cloud;
		long fileModified;

		WeatherPoint(long ts, String time) {
			this.ts = ts;
			this.time = time;
		}

		private static boolean isValid(Double v) {
			return v != null && v != IndexWeatherData.INEXISTENT_VALUE;
		}
	}

	private static WeatherPoint buildPoint(WeatherTiff wt,
	                                       double lat, double lon,
	                                       String weatherType,
	                                       long ts, String time, long fileModified,
	                                       int increment) {
		Map<Integer, String> codes = wt.getBandData();
		WeatherPoint wp = new WeatherPoint(ts, time);

		Integer iTemp = bandIndexByCode(codes, IndexWeatherData.WeatherParam.TEMP.code);
		Integer iPres = bandIndexByCode(codes, IndexWeatherData.WeatherParam.PRESSURE.code);
		Integer iWind = bandIndexByCode(codes, IndexWeatherData.WeatherParam.WIND.code);
		Integer iPrec = bandIndexByCode(codes, IndexWeatherData.WeatherParam.PRECIP.code);
		Integer iCloud = bandIndexByCode(codes, IndexWeatherData.WeatherParam.CLOUD.code);

		if (iTemp != null) wp.temp = wt.getValue(iTemp, lat, lon, weatherType);
		if (iPres != null) wp.press = wt.getValue(iPres, lat, lon, weatherType);
		if (iWind != null) wp.wind = wt.getValue(iWind, lat, lon, weatherType);
		if (iPrec != null) wp.precip = wt.getValue(iPrec, lat, lon, weatherType);
		if (iCloud != null) wp.cloud = wt.getValue(iCloud, lat, lon, weatherType);

		wp.fileModified = fileModified;
		normalizeValues(wp, weatherType, increment);
		return wp;
	}

	private static void normalizeValues(WeatherPoint p, String weatherType, int increment) {
		p.temp = WeatherPoint.isValid(p.temp) ? Math.round(p.temp * 1000.0) / 1000.0 : null;

		if (WeatherPoint.isValid(p.precip)) {
			final boolean isECWMF = ECWMF_WEATHER_TYPE.equals(weatherType);
			// Divide by inc (for ECMWF the step is 3h or 6h) to normalize it to the average intensity per 1 hour, regardless of the larger forecast step.
			int inc = isECWMF ? Math.max(1, increment) : 1;
			// PRATE in the metadata is the precipitation rate in kg/m²/s (= mm/s). We multiply it by 3600 to convert it to mm/hour.
			p.precip = p.precip * 3600.0 / inc;
			p.precip = Math.round(p.precip * 1000.0) / 1000.0;
		}

		if (WeatherPoint.isValid(p.press)) {
			// Convert PRATE in the metadata from Pascals (Pa) to the standard for weather maps, hectopascals (hPa).
			p.press = p.press * 0.01;
			p.press = Math.round(p.press * 1000.0) / 1000.0;
		}

		p.wind = WeatherPoint.isValid(p.wind) ? Math.round(p.wind * 1000.0) / 1000.0 : null;
		p.cloud = WeatherPoint.isValid(p.cloud) ? Math.round(p.cloud * 1000.0) / 1000.0 : null;
	}
}
