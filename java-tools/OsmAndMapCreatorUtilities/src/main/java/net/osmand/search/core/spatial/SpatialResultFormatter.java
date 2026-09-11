package net.osmand.search.core.spatial;

import net.osmand.data.Amenity;
import net.osmand.data.Building;
import net.osmand.data.City;
import net.osmand.data.LatLon;
import net.osmand.data.MapObject;
import net.osmand.data.Street;
import net.osmand.osm.MapPoiTypes;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

public class SpatialResultFormatter {
	private final SpatialSearchContext searchContext;
	private final LatLon location;
	private final MapPoiTypes poiTypes;

	public SpatialResultFormatter(SpatialSearchContext searchContext, LatLon location, MapPoiTypes poiTypes) {
		this.searchContext = searchContext;
		this.location = location;
		this.poiTypes = poiTypes;
	}

	public String format(SpatialSearchResult result) {
		int tokenCount = result.getParent().getTokenCount();
		double distance = location != null && result.getLatLon() != null
				? MapUtils.getDistance(location, result.getLatLon()) : 0.0;
		StringBuilder name = new StringBuilder();
		SpatialSearchToken.NameIndexAtom atom = result.getFirstRef().getNameIndexAtom();
		Building building = atom.getBuilding();
		boolean poiCategory = atom.isPoiCategory();
		if (building != null && building.isInterpolation() && result.getExtraNameMatch() != null) {
			name.append(result.getExtraNameMatch());
		} else {
			appendName(name, result.getExtraNameMatch(), atom.getBuilding());
		}
		appendName(name, result.getExtraNameMatch(), atom.getObject());
		if (atom.getBuilding() == null && atom.getObject() == null) {
			name.append(atom.getName());
		}

		List<MapObject> objects = result.getObjects();
		Amenity poi = null;
		for (MapObject object : objects) {
			if (object instanceof Amenity amenity && amenity.getType() != null) {
				poi = amenity;
				amenity.setType(poiTypes.getPoiCategoryByName(amenity.getType().getKeyName()));
			}
		}
		List<Street> intersectionStreets = getIntersectionStreets(result);
		String subtype = "";
		String resultType = null;
		for (MapObject object : objects) {
			if (object instanceof Street street) {
				if (intersectionStreets.size() == 2) {
					if (poi == null && building == null) {
						name.setLength(0);
					}
					appendIntersection(name, intersectionStreets);
					if (!poiCategory) {
						resultType = "STREET_INTERSECTION";
					}
				} else {
					appendName(name, result.getExtraNameMatch(), street.getCity());
				}
				break;
			}
			if (object instanceof City city) {
				appendName(name, result.getExtraNameMatch(), city);
				break;
			}
			if (object instanceof Amenity amenity && subtype.isEmpty()) {
				if (poiCategory) {
					appendName(name, amenity.getName(), object);
				} else {
					subtype = " " + amenity.getSubType();
				}
			}
		}
		String sorting = SpatialSearchResult.compareKeyString(result);
		return String.format(Locale.US, "%s [[%d, %s, %s, %.2f km, %s]]", name, tokenCount,
				resultType != null ? resultType : testTypeStr(atom) + subtype, sorting, distance / 1000,
				result.toString(searchContext).replace("\"", "'"));
	}

	private List<Street> getIntersectionStreets(SpatialSearchResult result) {
		SpatialSearchToken.NameIndexAtom first = null;
		SpatialSearchToken.NameIndexAtom second = null;
		SpatialSearchResultsList parent = result.getParent();
		for (int i = 0; i < parent.tCount; i++) {
			SpatialSearchToken.NameIndexAtom atom = parent.linearResults.get(result.parentInd * parent.tCount + i);
			if (atom.getObject() instanceof Street && !atom.isCityStreetName()) {
				if (first == null || Objects.equals(first.getObject().getId(), atom.getObject().getId())) {
					first = atom;
				} else {
					second = atom;
				}
			}
		}
		if (first == null || second == null
				|| !(first.getObject() instanceof Street firstStreet)
				|| !(second.getObject() instanceof Street secondStreet)) {
			return Collections.emptyList();
		}
		List<Street> streets = new ArrayList<>(List.of(firstStreet, secondStreet));
		streets.sort(Comparator.comparing((Street street) -> normalizedName(street.getName()))
				.thenComparing(Street::getId, Comparator.nullsLast(Long::compareTo)));
		return streets;
	}

	private void appendIntersection(StringBuilder result, List<Street> streets) {
		for (Street street : streets) {
			if (!result.isEmpty()) {
				result.append(" - ");
			}
			result.append(street.getName());
		}
	}

	private String normalizedName(String name) {
		return name == null ? "" : name.trim().toLowerCase(Locale.ROOT);
	}

	private void appendName(StringBuilder result, String extraMatch, MapObject object) {
		if (object == null) {
			return;
		}
		String name = object instanceof Building building ? building.getName() : object.getName();
		if ((Algorithms.isEmpty(name) || extraMatch != null) && object instanceof Amenity amenity
				&& amenity.getAdditionalInfo("ref") != null) {
			name = (Algorithms.isEmpty(name) ? "" : name + " ") + amenity.getAdditionalInfo("ref");
		}
		if (Algorithms.isEmpty(name)) {
			return;
		}
		if (!result.isEmpty()) {
			result.append(", ");
		}
		result.append(name);
	}

	private String testTypeStr(SpatialSearchToken.NameIndexAtom atom) {
		if (atom.isBuilding()) {
			return "HOUSE";
		} else if (atom.isPOI()) {
			return "POI";
		} else if (atom.isStreet()) {
			return "STREET";
		} else if (atom.isPoiCategory()) {
			return "POI_TYPE";
		} else if (atom.getObject() instanceof City) {
			return "CITY";
		}
		return atom.typeStr().toUpperCase(Locale.US);
	}
}
