package net.osmand.server.api.services;

import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.data.*;
import net.osmand.osm.PoiCategory;
import net.osmand.osm.PoiFilter;
import net.osmand.osm.PoiType;
import net.osmand.osm.edit.Node;
import net.osmand.router.TransportStopsRouteReader;
import net.osmand.server.api.services.search.MapReadersService;
import net.osmand.server.controllers.pub.GeojsonClasses;
import net.osmand.util.MapUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;

@Service
public class TransportStopsService {

	@Autowired
	MapReadersService mapReadersService;

	@Autowired
	OsmAndMapsService osmAndMapsService;

	private static final int TOTAL_LIMIT_TRANSPORT_STOPS = 1000;
	private static final int SHOW_NEARBY_ROUTES_RADIUS_METERS = 150;
	private static final int SEARCH_STOP_RADIUS_METERS = 50;
	private static final String KEY_NEARBY_ROUTES = "nearbyRoutes";
	// as in Android TransportStopHelper
	private static final int SHOW_STOPS_RADIUS_METERS = 150;
	private static final int SHOW_SUBWAY_STOPS_FROM_ENTRANCES_RADIUS_METERS = 400;
	private static final int MAX_DISTANCE_BETWEEN_AMENITY_AND_LOCAL_STOPS = 20;
	private static final String TRANSPORTATION_CATEGORY = "transportation";
	private static final String PUBLIC_TRANSPORT_FILTER = "public_transport";
	private static final String PUBLIC_TRANSPORT_STATION_SUBTYPE = "public_transport_station";
	private static final List<String> SUBWAY_ENTRANCE_SUBTYPES = List.of("subway_entrance", PUBLIC_TRANSPORT_STATION_SUBTYPE);

	private static List<LatLon> bboxAroundPoint(double lat, double lon, int radiusMeters) {
		QuadRect rect = MapUtils.calculateLatLonBbox(lat, lon, radiusMeters);
		return Arrays.asList(new LatLon(rect.top, rect.left), new LatLon(rect.bottom, rect.right));
	}

	public TransportStopsSearchResult searchTransportStops(String northWest, String southEast) throws IOException {
		List<LatLon> bbox = mapReadersService.getBboxCoords(Arrays.asList(northWest, southEast));
		if (bbox.size() != 2) {
			return new TransportStopsSearchResult(false, new GeojsonClasses.FeatureCollection());
		}

		TransportStopsReaderResult readerResult = getTransportStopsReader(bbox);
		if (readerResult == null) {
			return new TransportStopsSearchResult(false, new GeojsonClasses.FeatureCollection());
		}

		List<GeojsonClasses.Feature> features = new ArrayList<>();
		boolean useLimit = false;

		try {
			for (TransportStop s : readerResult.transportReaders.readMergedTransportStops(readerResult.request)) {
				if (features.size() >= TOTAL_LIMIT_TRANSPORT_STOPS) {
					useLimit = true;
					break;
				}
				if (!s.isDeleted() && !s.isMissingStop()) {
					GeojsonClasses.Feature feature = convertTransportStopToFeature(s);
					if (feature != null) {
						features.add(feature);
					}
				}
			}
		} finally {
			osmAndMapsService.unlockReaders(readerResult.readers);
		}

		if (features.isEmpty()) {
			return new TransportStopsSearchResult(false, new GeojsonClasses.FeatureCollection());
		}
		return new TransportStopsSearchResult(useLimit, new GeojsonClasses.FeatureCollection(features.toArray(new GeojsonClasses.Feature[0])));
	}

	public TransportRouteFeature getTransportRoute(LatLon transportStopCoords, long stopId, long routeId) throws IOException {
		List<LatLon> bbox = bboxAroundPoint(transportStopCoords.getLatitude(), transportStopCoords.getLongitude(), SEARCH_STOP_RADIUS_METERS);
		TransportStopsReaderResult readerResult = getTransportStopsReader(bbox);
		if (readerResult == null) {
			return null;
		}

		try {
			TransportStop foundStop = null;
			for (TransportStop s : readerResult.transportReaders.readMergedTransportStops(readerResult.request)) {
				if (s.getId() == stopId) {
					foundStop = s;
					break;
				}
			}
			if (foundStop != null) {
				List<TransportRoute> routes = foundStop.getRoutes();
				if (routes == null || routes.isEmpty()) {
					return null;
				}
				for (TransportRoute route : routes) {
					if (route.getId() == routeId) {
						Integer intervalSeconds = route.hasInterval() ? route.calcIntervalInSeconds() : null;
						List<TransportStopWithDetails> stops = route.getForwardStops().stream()
								.map(s -> new TransportStopWithDetails(s.getId(), s.getName(), s.getLocation()))
								.toList();
						List<List<LatLon>> nodes = route.getForwardWays().stream()
								.map(way -> way.getNodes().stream().map(Node::getLatLon).toList())
								.toList();
						return new TransportRouteFeature(route.getId(), intervalSeconds, stops, nodes);
					}
				}
			}
		} finally {
			osmAndMapsService.unlockReaders(readerResult.readers);
		}
		return null;
	}

	public GeojsonClasses.Feature getTransportStop(LatLon transportStopCoords, long stopId) throws IOException {
		List<LatLon> bbox = bboxAroundPoint(transportStopCoords.getLatitude(), transportStopCoords.getLongitude(), SHOW_SUBWAY_STOPS_FROM_ENTRANCES_RADIUS_METERS);
		TransportStopsReaderResult readerResult = getTransportStopsReader(bbox);
		if (readerResult == null) {
			return null;
		}
		try {
			for (TransportStop s : readerResult.transportReaders.readMergedTransportStops(readerResult.request)) {
				if (s.getId() == stopId) {
					return convertTransportStopToFeature(s);
				}
			}
		} finally {
			osmAndMapsService.unlockReaders(readerResult.readers);
		}
		return null;
	}

	public boolean isPublicTransportStop(Amenity amenity) {
		PoiCategory category = amenity.getType();
		if (!TRANSPORTATION_CATEGORY.equals(category.getKeyName())) {
			return false;
		}
		PoiFilter filter = category.getPoiFilterByName(PUBLIC_TRANSPORT_FILTER);
		if (filter == null) {
			return false;
		}
		for (PoiType type : filter.getPoiTypes()) {
			if (type.getKeyName().equals(amenity.getSubType())) {
				return true;
			}
		}
		return false;
	}

	public Long findBestTransportStopId(Amenity amenity) throws IOException {
		LatLon loc = amenity.getLocation();
		boolean isSubwayEntrance = SUBWAY_ENTRANCE_SUBTYPES.contains(amenity.getSubType());
		int radius = isSubwayEntrance ? SHOW_SUBWAY_STOPS_FROM_ENTRANCES_RADIUS_METERS : SHOW_STOPS_RADIUS_METERS;
		TransportStopsReaderResult readerResult = getTransportStopsReader(bboxAroundPoint(loc.getLatitude(), loc.getLongitude(), radius));
		if (readerResult == null) {
			return null;
		}
		try {
			List<TransportStop> stops = new ArrayList<>(readerResult.transportReaders.readMergedTransportStops(readerResult.request));
			stops.sort(Comparator.comparingDouble(s -> MapUtils.getDistance(s.getLocation(), loc)));
			for (TransportStop stop : stops) {
				if (isSubwayEntrance ? isStopOfStation(stop, amenity) : isStopOfAmenity(stop, amenity)) {
					return stop.getId();
				}
			}
			return stops.isEmpty() ? null : stops.get(0).getId();
		} finally {
			osmAndMapsService.unlockReaders(readerResult.readers);
		}
	}

	private static boolean isStopOfAmenity(TransportStop stop, Amenity amenity) {
		LatLon loc = amenity.getLocation();
		String stopName = stop.getName().toLowerCase();
		String amenityName = amenity.getName().toLowerCase();
		boolean sameName = stopName.contains(amenityName) || amenityName.contains(stopName);
		return (sameName && MapUtils.getDistance(stop.getLocation(), loc) < MAX_DISTANCE_BETWEEN_AMENITY_AND_LOCAL_STOPS)
				|| stop.getLocation().equals(loc);
	}

	private static boolean isStopOfStation(TransportStop stop, Amenity amenity) {
		if (PUBLIC_TRANSPORT_STATION_SUBTYPE.equals(amenity.getSubType())
				&& (stop.getName().equals(amenity.getName()) || stop.getEnName(false).equals(amenity.getEnName(false)))) {
			return true;
		}
		for (TransportStopExit exit : stop.getExits()) {
			if (MapUtils.getDistance(exit.getLocation(), amenity.getLocation()) < MapUtils.ROUNDING_ERROR) {
				return true;
			}
		}
		return false;
	}

	public Map<String, Object> getNearbyTransportStops(LatLon stopCoords, long excludeStopId) throws IOException {
		List<LatLon> bbox = bboxAroundPoint(stopCoords.getLatitude(), stopCoords.getLongitude(), SHOW_NEARBY_ROUTES_RADIUS_METERS);
		TransportStopsReaderResult readerResult = getTransportStopsReader(bbox);
		if (readerResult == null) {
			return Map.of(KEY_NEARBY_ROUTES, Collections.<TransportStopRouteFeature>emptyList());
		}
		Set<Long> excludeRouteIds = new HashSet<>();
		List<TransportStop> stops = new ArrayList<>();
		try {
			for (TransportStop s : readerResult.transportReaders.readMergedTransportStops(readerResult.request)) {
				if (s.getId() == excludeStopId) {
					for (TransportStopRouteFeature r : findRoutesByStop(s)) {
						excludeRouteIds.add(r.id());
					}
				} else if (!s.isDeleted() && !s.isMissingStop() && stops.size() < TOTAL_LIMIT_TRANSPORT_STOPS) {
					stops.add(s);
				}
			}
		} finally {
			osmAndMapsService.unlockReaders(readerResult.readers);
		}
		Map<Long, TransportStopRouteFeature> routesById = new LinkedHashMap<>();
		for (TransportStop s : stops) {
			for (TransportStopRouteFeature r : findRoutesByStop(s)) {
				if (!excludeRouteIds.contains(r.id())) {
					routesById.putIfAbsent(r.id(), r);
				}
			}
		}
		return Map.of(KEY_NEARBY_ROUTES, new ArrayList<>(routesById.values()));
	}

	private TransportStopsReaderResult getTransportStopsReader(List<LatLon> bbox) throws IOException {
		if (!osmAndMapsService.validateAndInitConfig()) {
			return null;
		}

		QuadRect searchBbox = mapReadersService.getSearchBbox(bbox);
		if (searchBbox == null) {
			return null;
		}

		int left31 = (int) searchBbox.left;
		int right31 = (int) searchBbox.right;
		int top31 = (int) searchBbox.top;
		int bottom31 = (int) searchBbox.bottom;

		List<OsmAndMapsService.BinaryMapIndexReaderReference> mapList = mapReadersService.getMapsForSearch(searchBbox, false);
		if (mapList.isEmpty()) {
			return null;
		}
		List<BinaryMapIndexReader> readers = osmAndMapsService.getReaders(mapList, null);
		if (readers.isEmpty()) {
			return null;
		}
		TransportStopsRouteReader transportReaders = new TransportStopsRouteReader(readers);
		BinaryMapIndexReader.SearchRequest<TransportStop> request = BinaryMapIndexReader.buildSearchTransportRequest(
				left31, right31, top31, bottom31, -1, new ArrayList<>());

		return new TransportStopsReaderResult(transportReaders, readers, request);
	}

	private GeojsonClasses.Feature convertTransportStopToFeature(TransportStop stop) {
		if (stop == null || stop.getLocation() == null) {
			return null;
		}

		LatLon location = stop.getLocation();
		GeojsonClasses.Feature feature = new GeojsonClasses.Feature(GeojsonClasses.Geometry.point(location));

		feature.prop("id", stop.getId());
		feature.prop("name", stop.getName());

		feature.prop("routes", findRoutesByStop(stop));

		return feature;
	}

	private List<TransportStopRouteFeature> findRoutesByStop(TransportStop stop) {
		if (stop == null) {
			return Collections.emptyList();
		}
		List<TransportRoute> routes = stop.getRoutes();
		if (routes != null && !routes.isEmpty()) {
			TransportStopInfo stopInfo = toStopInfo(stop);
			List<TransportStopRouteFeature> stopRoutes = new ArrayList<>();
			routes.forEach(route -> stopRoutes.add(new TransportStopRouteFeature(route.getId(), route.getName(), route.getType(), route.getRef(), route.getColor(), stopInfo)));
			return stopRoutes;
		}
		return Collections.emptyList();
	}

	private static TransportStopInfo toStopInfo(TransportStop stop) {
		LatLon loc = stop != null ? stop.getLocation() : null;
		if (loc == null) {
			return null;
		}
		double lat = loc.getLatitude();
		double lon = loc.getLongitude();
		return new TransportStopInfo(stop.getId(), lat, lon);
	}

	private record TransportStopsReaderResult(TransportStopsRouteReader transportReaders,
	                                          List<BinaryMapIndexReader> readers,
	                                          BinaryMapIndexReader.SearchRequest<TransportStop> request) {
	}

	public record TransportStopsSearchResult(boolean useLimit, GeojsonClasses.FeatureCollection features) {
	}

	public record TransportStopInfo(long id, double lat, double lon) {
	}

	public record TransportStopRouteFeature(long id, String name, String type, String ref, String color,
	                                        TransportStopInfo stop) {
	}

	public record TransportStopWithDetails(long stopId, String name, LatLon coords) {
	}

	public record TransportRouteFeature(long id, Integer intervalSeconds, List<TransportStopWithDetails> stops,
	                                    List<List<LatLon>> nodes) {
	}
}
