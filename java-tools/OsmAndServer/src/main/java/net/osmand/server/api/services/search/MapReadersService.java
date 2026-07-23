package net.osmand.server.api.services.search;

import static net.osmand.util.LocationParser.parseOpenLocationCode;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Optional;
import java.util.Comparator;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import net.osmand.PlatformUtil;
import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.binary.GeocodingUtilities;
import net.osmand.data.LatLon;
import net.osmand.data.QuadRect;
import net.osmand.map.OsmandRegions;
import net.osmand.osm.MapPoiTypes;
import net.osmand.search.SearchUICore;
import net.osmand.search.core.SearchCoreFactory;
import net.osmand.search.core.SearchResult;
import net.osmand.search.core.SearchSettings;
import net.osmand.server.api.services.OsmAndMapsService;
import net.osmand.util.LocationParser;
import net.osmand.util.TextDirectionUtil;

@Service
public class MapReadersService {

	private static final Log LOGGER = LogFactory.getLog(MapReadersService.class);

	@Autowired
	OsmAndMapsService osmAndMapsService;

	private final OsmandRegions osmandRegions;

	public MapReadersService() {
		try {
			osmandRegions = PlatformUtil.getOsmandRegions();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public OsmandRegions getOsmandRegions() {
		return osmandRegions;
	}

	public List<OsmAndMapsService.BinaryMapIndexReaderReference> getMapRefs(String northWest,
	                                                                        String southEast, double radius,
	                                                                        boolean baseSearch) throws IOException {
		if (!osmAndMapsService.validateAndInitConfig()) {
			return Collections.emptyList();
		}
		if (northWest == null || southEast == null) {
			return Collections.emptyList();
		}
		QuadRect points = getSearchBbox(getBboxCoords(Arrays.asList(northWest, southEast)), radius);
		return getMapsForSearch(points, baseSearch);
	}

	public List<BinaryMapIndexReader> openReaders(
			List<OsmAndMapsService.BinaryMapIndexReaderReference> maps) throws IOException {
		List<BinaryMapIndexReader> readers = new ArrayList<>();
		if (maps == null) {
			return readers;
		}
		try {
			for (OsmAndMapsService.BinaryMapIndexReaderReference ref : maps) {
				if (ref.file.getName().startsWith("World_")) {
					continue;
				}
				BinaryMapIndexReader reader = new BinaryMapIndexReader(new RandomAccessFile(ref.file, "r"), ref.file, true);
				if (reader.containsAddressData() && reader.containsRouteData()) {
					readers.add(reader);
				} else {
					reader.close();
				}
			}
			BinaryMapIndexReader regionsReader = openRegionsReader();
			if (regionsReader != null) {
				readers.add(regionsReader);
			}
		} catch (IOException | RuntimeException e) {
			closeReaders(readers);
			throw e;
		}
		return readers;
	}

	BinaryMapIndexReader openRegionsReader() {
		BinaryMapIndexReader regionsReader = osmandRegions.getFile();
		try {
			if (regionsReader == null || regionsReader.getFile() == null) {
				return null;
			}
			return new BinaryMapIndexReader(new RandomAccessFile(regionsReader.getFile(), "r"), regionsReader);
		} catch (Exception e) {
			LOGGER.warn("Failed to open regions reader for spatial search.", e);
		}
		return null;
	}

	public void closeReaders(List<BinaryMapIndexReader> readers) {
		if (readers == null) {
			return;
		}
		for (BinaryMapIndexReader reader : readers) {
			try {
				reader.close();
			} catch (IOException e) {
				LOGGER.warn("Failed to close spatial test reader.", e);
			}
		}
	}

	public List<OsmAndMapsService.BinaryMapIndexReaderReference> getMapsForSearch(QuadRect searchBbox, boolean baseSearch)
			throws IOException {
		OsmAndMapsService.BinaryMapIndexReaderReference basemap = osmAndMapsService.getBaseMap();
		if (baseSearch) {
			return List.of(basemap);
		} else {
			if (searchBbox != null) {
				List<OsmAndMapsService.BinaryMapIndexReaderReference> list = osmAndMapsService.getObfReaders(searchBbox,
						OsmAndMapsService.ObfReason.SEARCH.value());
				list.add(basemap);
				return list;
			}
		}
		return Collections.emptyList();
	}

	public QuadRect getSearchBbox(List<LatLon> bbox) {
		if (bbox.size() == 2) {
			return osmAndMapsService.points(null, bbox.get(0), bbox.get(1));
		}
		return null;
	}

	public QuadRect getSearchBbox(List<LatLon> bbox, double radius) {
		if (bbox.size() == 2) {
			LatLon northWest = bbox.get(0);
			LatLon southEast = bbox.get(1);
			return osmAndMapsService.points(null,
					new LatLon(northWest.getLatitude() + radius, northWest.getLongitude() - radius),
					new LatLon(southEast.getLatitude() - radius, southEast.getLongitude() + radius));
		}
		return null;
	}

	public List<LatLon> getBboxCoords(List<String> coords) {
		List<LatLon> bbox = new ArrayList<>();
		for (String coord : coords) {
			String[] lanLonArr = coord.split(",");
			bbox.add(new LatLon(Double.parseDouble(lanLonArr[0]), Double.parseDouble(lanLonArr[1])));
		}
		return bbox;
	}

	public String getPoiAddress(LatLon location) throws IOException, InterruptedException {
		if (location != null) {
			List<GeocodingUtilities.GeocodingResult> list = osmAndMapsService.geocoding(location.getLatitude(),
					location.getLongitude());
			Optional<GeocodingUtilities.GeocodingResult> nearestResult = list.stream()
					.min(Comparator.comparingDouble(GeocodingUtilities.GeocodingResult::getDistance));
			if (nearestResult.isPresent()) {
				return nearestResult.get().toString();
			}
		}
		return null;
	}

	public LatLon parseLocation(String locationString, LatLon bboxCentre) throws IOException {
		if (locationString == null || locationString.trim().isEmpty()) {
			return null;
		}
		locationString = TextDirectionUtil.clearDirectionMarks(locationString);
		LocationParser.ParsedOpenLocationCode olcParsed = parseOpenLocationCode(locationString);
		if (olcParsed != null) {
			if (olcParsed.isFull()) {
				return olcParsed.getLatLon();
			}
			LatLon location = searchOlcOnBasemap(locationString, bboxCentre);
			if (location != null) {
				return location;
			}
		}
		return LocationParser.parseLocation(locationString);
	}

	private LatLon searchOlcOnBasemap(String locationString, LatLon bboxCentre) throws IOException {
		if (!osmAndMapsService.validateAndInitConfig()) {
			return null;
		}
		List<BinaryMapIndexReader> usedMapList = new ArrayList<>();

		try {
			QuadRect world = new QuadRect(0, 0, Integer.MAX_VALUE, Integer.MAX_VALUE);
			List<OsmAndMapsService.BinaryMapIndexReaderReference> refs = getMapsForSearch(world, true);
			if (refs.isEmpty()) {
				return null;
			}
			usedMapList = osmAndMapsService.getReaders(refs, null);

			SearchUICore searchUICore = new SearchUICore(MapPoiTypes.getDefault(), PoiTypesService.DEFAULT_SEARCH_LANG, false);
			SearchSettings settings = searchUICore.getSearchSettings();
			settings.setOfflineIndexes(usedMapList);
			settings.setSearchBBox31(world);
			searchUICore.updateSettings(settings);
			SearchCoreFactory.SearchAmenityByNameAPI amenitiesApi = new SearchCoreFactory.SearchAmenityByNameAPI();
			searchUICore.registerAPI(amenitiesApi);
			searchUICore.registerAPI(new SearchCoreFactory.SearchLocationAndUrlAPI(amenitiesApi));

			SearchUICore.SearchResultCollection resultCollection = searchUICore.immediateSearch(locationString,
					bboxCentre);
			if (resultCollection != null && !resultCollection.getCurrentSearchResults().isEmpty()) {
				SearchResult result = resultCollection.getCurrentSearchResults().get(0);
				if (result.object instanceof LatLon location) {
					return location;
				}
			}
		} finally {
			osmAndMapsService.unlockReaders(usedMapList);
		}
		return null;
	}
}
