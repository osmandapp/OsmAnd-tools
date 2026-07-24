package net.osmand.server.api.services.search;

import static net.osmand.data.Amenity.OPENING_HOURS;
import static net.osmand.data.MapObject.unzipContent;
import static net.osmand.gpx.GPXUtilities.AMENITY_PREFIX;
import static net.osmand.search.SearchUICore.createAddressString;
import static net.osmand.search.SearchUICore.getDominatedCity;
import static net.osmand.search.SearchUICore.getMainCityName;
import static net.osmand.shared.gpx.GpxUtilities.OSM_PREFIX;
import static net.osmand.util.OpeningHoursParser.parseOpenedHours;

import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.StringJoiner;
import java.util.TimeZone;
import java.util.TreeMap;

import org.jetbrains.annotations.Nullable;
import org.springframework.stereotype.Service;

import net.osmand.binary.ObfConstants;
import net.osmand.data.Amenity;
import net.osmand.data.LatLon;
import net.osmand.data.MapObject;
import net.osmand.osm.AbstractPoiType;
import net.osmand.osm.MapPoiTypes;
import net.osmand.osm.PoiCategory;
import net.osmand.osm.PoiFilter;
import net.osmand.osm.PoiType;
import net.osmand.osm.edit.Entity;
import net.osmand.search.core.ObjectType;
import net.osmand.search.core.SearchCoreFactory;
import net.osmand.search.core.SearchResult;
import net.osmand.search.core.TopIndexFilter;
import net.osmand.server.controllers.pub.GeojsonClasses.Feature;
import net.osmand.server.controllers.pub.GeojsonClasses.Geometry;
import net.osmand.util.Algorithms;
import net.osmand.util.OpeningHoursParser.OpeningHours;

@Service
public class SearchResultConverter {

	public static final String IS_OPENED_PREFIX = "open:";
	public static final String OPENING_HOURS_INFO_SUFFIX = "_info";

	private volatile Map<String, PoiType> poiAdditionalsByKey;

	public enum PoiTypeField {
		NAME("web_name"), EN_NAME("web_en_name"), TYPE("web_type"), ADDRESS_1("web_address1"),
		ADDRESS_2("web_address2"), KEY_NAME("web_keyName"), OSM_TAG("web_typeOsmTag"), OSM_VALUE("web_typeOsmValue"),
		ICON_NAME("web_iconKeyName"), CATEGORY_ICON("web_categoryIcon"), CATEGORY_KEY_NAME("web_categoryKeyName"),
		POI_ADD_CATEGORY_NAME("web_poiAdditionalCategory"), POI_FILTER_NAME("web_poiFilterName"), POI_ID("web_poi_id"),
		POI_NAME("web_poi_name"), POI_COLOR("web_poi_color"), POI_ICON_NAME("web_poi_iconName"),
		POI_TYPE("web_poi_type"), POI_SUBTYPE("web_poi_subType"), POI_OSM_URL("web_poi_osmUrl"), CITY("web_city"),
		// names of all objects matched in a spatial-search result (street, city, ...)
		MATCHED_OBJECTS("web_matched_objects"), VISIBLE_LEVEL("web_visible_level"),
		COMPARE_KEY("web_compare_key"), BBOX_LAT_LON("web_bbox_lat_lon"),
		WIKIDATA_ID("web_wikidata_id"), CITY_TYPE("web_city_type");

		private final String fieldName;

		PoiTypeField(String fieldName) {
			this.fieldName = fieldName;
		}

		public String getFieldName() {
			return fieldName;
		}
	}

	public Feature getFeature(SearchResult result, String timeZone) {
		Feature feature;
		if (result.objectType == ObjectType.POI) {
			feature = getPoiFeature(result, timeZone);
		} else {
			Geometry geometry = Geometry.point(result.location != null ? result.location : new LatLon(0, 0));
			feature = new Feature(geometry).prop(PoiTypeField.TYPE.getFieldName(), result.objectType)
					.prop(PoiTypeField.NAME.getFieldName(), result.localeName);
			if (result.objectType == ObjectType.STREET || result.objectType == ObjectType.HOUSE) {
				if (result.localeRelatedObjectName != null) {
					feature.prop(PoiTypeField.ADDRESS_1.getFieldName(), result.localeRelatedObjectName);
				}
				SearchResult parentResult = result.parentSearchResult;
				if (parentResult != null && parentResult.localeRelatedObjectName != null) {
					feature.prop(PoiTypeField.ADDRESS_2.getFieldName(), parentResult.localeRelatedObjectName);
				}
			} else if (result.objectType == ObjectType.STREET_INTERSECTION) {
				feature.prop(PoiTypeField.NAME.getFieldName(),
						result.localeName + " - " + result.localeRelatedObjectName);
			}
			Map<String, String> tags = getPoiTypeFields(result.object);
			for (Map.Entry<String, String> entry : tags.entrySet()) {
				feature.prop(entry.getKey(), entry.getValue());
			}
		}
		return feature;
	}

	public Feature getPoiFeature(SearchResult result, String timeZone) {
		Amenity amenity = (Amenity) result.object;
		Feature feature = null;

		String poiNameWithAlternateName = result.localeName != null ? result.localeName : amenity.getName();
		if (!Algorithms.isEmpty(result.alternateName)
				&& !Algorithms.objectEquals(result.localeName, result.alternateName)) {
			poiNameWithAlternateName += " (" + result.alternateName + ")";
		}

		feature = new Feature(Geometry.point(amenity.getLocation()))
				.prop(PoiTypeField.TYPE.getFieldName(), result.objectType)
				.prop(PoiTypeField.POI_ID.getFieldName(), amenity.getId())
				.prop(PoiTypeField.POI_NAME.getFieldName(), poiNameWithAlternateName)
				.prop(PoiTypeField.POI_COLOR.getFieldName(), amenity.getColor())

				.prop(PoiTypeField.POI_TYPE.getFieldName(), amenity.getType().getKeyName())
				.prop(PoiTypeField.POI_SUBTYPE.getFieldName(), amenity.getSubType())
				.prop(PoiTypeField.POI_OSM_URL.getFieldName(), getOsmUrl(result));

		Map<String, String> tags = amenity.getAmenityExtensions();
		filterWikiTags(tags);
		for (Map.Entry<String, String> entry : tags.entrySet()) {
			String key = entry.getKey().startsWith(OSM_PREFIX) ? entry.getKey().substring(OSM_PREFIX.length())
					: entry.getKey();
			PoiType additionalType = getPoiAdditionalsByKey().get(key);
			if (additionalType != null && additionalType.isHidden()) {
				continue;
			}
			String value = unzipContent(entry.getValue());
			feature.prop(entry.getKey(), value);
		}
		String openingHoursValue = tags.get(AMENITY_PREFIX + OPENING_HOURS);
		Calendar clientTime = getClientTime(timeZone);
		if (clientTime != null && openingHoursValue != null) {
			String openingHoursInfo = getOpeningHoursInfo(openingHoursValue, clientTime);
			if (openingHoursInfo != null) {
				feature.prop(AMENITY_PREFIX + OPENING_HOURS + OPENING_HOURS_INFO_SUFFIX, openingHoursInfo);
			}
		}
		Map<String, String> names = amenity.getNamesMap(true);
		for (Map.Entry<String, String> entry : names.entrySet()) {
			feature.prop(PoiTypeField.POI_NAME.getFieldName() + ":" + entry.getKey(), entry.getValue());
		}
		feature.prop(PoiTypeField.CITY.getFieldName(), result.addressName);
		String subType = amenity.getSubType();
		if (subType != null && subType.indexOf(';') != -1) {
			subType = subType.substring(0, subType.indexOf(';'));
		}
		PoiType poiType = amenity.getType().getPoiTypeByKeyName(subType);
		if (poiType != null) {
			feature.prop(PoiTypeField.POI_ICON_NAME.getFieldName(), getIconName(poiType));
			Map<String, String> typeTags = getPoiTypeFields(poiType);
			for (Map.Entry<String, String> entry : typeTags.entrySet()) {
				feature.prop(entry.getKey(), entry.getValue());
			}

		}
		return feature;
	}

	public Map<String, String> getPoiTypeFields(Object obj) {
		Map<String, String> tags = new HashMap<>();
		if (obj instanceof PoiType type) {
			if (type.isHidden()) {
				return tags;
			}
			tags.put(PoiTypeField.KEY_NAME.getFieldName(), type.getKeyName());
			tags.put(PoiTypeField.OSM_TAG.getFieldName(), type.getOsmTag());
			tags.put(PoiTypeField.OSM_VALUE.getFieldName(), type.getOsmValue());
			tags.put(PoiTypeField.ICON_NAME.getFieldName(), type.getIconKeyName());
			PoiCategory category = type.getCategory();
			if (category != null) {
				tags.put(PoiTypeField.CATEGORY_ICON.getFieldName(), category.getIconKeyName());
				tags.put(PoiTypeField.CATEGORY_KEY_NAME.getFieldName(), category.getKeyName());
			}
			tags.put(PoiTypeField.POI_ADD_CATEGORY_NAME.getFieldName(), type.getPoiAdditionalCategory());
		} else if (obj instanceof PoiFilter type) {
			tags.put(PoiTypeField.KEY_NAME.getFieldName(), type.getKeyName());
			PoiCategory category = type.getPoiCategory();
			if (category != null) {
				tags.put(PoiTypeField.CATEGORY_ICON.getFieldName(), category.getIconKeyName());
				tags.put(PoiTypeField.CATEGORY_KEY_NAME.getFieldName(), category.getKeyName());
			} else if (obj instanceof PoiCategory cat) {
				tags.put(PoiTypeField.CATEGORY_ICON.getFieldName(), cat.getIconKeyName());
				tags.put(PoiTypeField.CATEGORY_KEY_NAME.getFieldName(), cat.getKeyName());
			}
		} else if (obj instanceof SearchCoreFactory.PoiAdditionalCustomFilter type) {
			tags.put(PoiTypeField.KEY_NAME.getFieldName(), type.getKeyName());
			tags.put(PoiTypeField.ICON_NAME.getFieldName(), type.getIconKeyName());
			type.additionalPoiTypes.stream().findFirst().ifPresent(poiType -> {
				tags.put(PoiTypeField.ICON_NAME.getFieldName(), getIconName(poiType));
				PoiFilter poiFilter = poiType.getFilter();
				if (poiFilter != null) {
					tags.put(PoiTypeField.POI_FILTER_NAME.getFieldName(), poiFilter.getKeyName());
				}
				String additionalCategory = poiType.getPoiAdditionalCategory();
				if (additionalCategory != null) {
					tags.put(PoiTypeField.POI_ADD_CATEGORY_NAME.getFieldName(), additionalCategory);
				}
			});
		} else if (obj instanceof TopIndexFilter type) {
			tags.put(PoiTypeField.CATEGORY_KEY_NAME.getFieldName(), type.getTag());
			tags.put(PoiTypeField.CATEGORY_ICON.getFieldName(), type.getTag());
			tags.put(PoiTypeField.KEY_NAME.getFieldName(), type.getFilterId());
			tags.put(PoiTypeField.NAME.getFieldName(), type.getValue());
		} else if (obj instanceof AbstractPoiType type) {
			tags.put(PoiTypeField.KEY_NAME.getFieldName(), type.getKeyName());
			tags.put(PoiTypeField.ICON_NAME.getFieldName(), type.getIconKeyName());
		} else if (obj instanceof MapObject type) {
			String enName = type.getEnName(false);
			if (Algorithms.isNotEmpty(enName)) {
				tags.put(PoiTypeField.EN_NAME.getFieldName(), enName);
			}
		}
		return tags;
	}

	public SearchResult buildPoiSearchResult(Amenity amenity, String locale, String dominatedCity) {
		String cityName = amenity.getCityFromTagGroups(locale);
		String city = cityName == null ? "" : cityName;
		SearchResult result = new SearchResult();
		result.object = amenity;
		result.objectType = ObjectType.POI;
		result.location = amenity.getLocation();
		result.addressName = calculateAddressString(amenity, city, getMainCityName(city), dominatedCity);
		return result;
	}

	private String calculateAddressString(Amenity amenity, String cityName, String mainCity, String dominatedCity) {
		String streetName = amenity.getStreetName();
		if (Algorithms.isEmpty(streetName)) {
			return cityName.isEmpty() ? null : cityName;
		}
		String houseNumber = amenity.getAdditionalInfo(Amenity.ADDR_HOUSENUMBER);
		String addr = streetName + (Algorithms.isEmpty(houseNumber) ? "" : " " + houseNumber);

		return createAddressString(cityName, mainCity, dominatedCity, addr);
	}

	public void saveSearchResult(List<SearchResult> res, List<Feature> features, String timeZone) {
		for (SearchResult result : res) {
			features.add(getFeature(result, timeZone));
		}
	}

	public void saveAmenityResults(List<Amenity> amenities, Map<Long, Feature> foundFeatures, int remainingLimit,
	                               String locale, String timeZone) {
		String dominatedCity = "";
		Map<String, Integer> cityCounter = new TreeMap<>();
		for (Amenity amenity : amenities) {
			String cityName = amenity.getCityFromTagGroups(locale);
			if (!Algorithms.isEmpty(cityName)) {
				String mainCity = getMainCityName(cityName);
				String domCity = getDominatedCity(cityCounter, mainCity);
				if (domCity != null) {
					dominatedCity = domCity;
					break;
				}
			}
		}
		for (Amenity amenity : amenities) {
			if (remainingLimit <= 0) {
				break;
			}
			long osmId = amenity.getId();
			if (!foundFeatures.containsKey(osmId)) {
				foundFeatures.put(osmId, getPoiFeature(buildPoiSearchResult(amenity, locale, dominatedCity), timeZone));
				remainingLimit--;
			}
		}
	}

	public static Feature mergeFeatures(Feature f1, Feature f2) {
		if (f1 == null)
			return f2;
		if (f2 == null)
			return f1;

		Feature merged = new Feature(f1.geometry != null ? f1.geometry : f2.geometry);
		merged.properties.putAll(f1.properties);
		merged.prop("poiTags", f2.properties);
		return merged;
	}

	private String getOpeningHoursInfo(String openingHoursValue, Calendar calendar) {
		OpeningHours openingHours = parseOpenedHours(openingHoursValue);
		if (openingHours != null) {
			List<OpeningHours.Info> openingHoursInfo = openingHours.getInfo(calendar);
			if (!Algorithms.isEmpty(openingHoursInfo)) {
				StringJoiner openHoursInfos = new StringJoiner(";");
				for (OpeningHours.Info info : openingHoursInfo) {
					String infoString = info.getInfo();
					if (!Algorithms.isEmpty(infoString)) {
						String s = (info.isOpened() ? IS_OPENED_PREFIX : "") + infoString;
						openHoursInfos.add(s);
					}
				}
				return openHoursInfos.toString();
			}
		}
		return null;
	}

	private void filterWikiTags(Map<String, String> tags) {
		tags.entrySet().removeIf(entry -> entry.getKey().startsWith("osm_tag_travel_elo")
				|| entry.getKey().startsWith("osm_tag_travel_topic") || entry.getKey().startsWith("osm_tag_qrank")
				|| entry.getKey().startsWith("osm_tag_wiki_place") || entry.getKey().startsWith("osm_tag_wiki_photo"));
	}

	private String getOsmUrl(SearchResult result) {
		MapObject mapObject = (MapObject) result.object;
		Entity.EntityType type = ObfConstants.getOsmEntityType(mapObject);
		if (type != null) {
			long osmId = ObfConstants.getOsmObjectId(mapObject);
			return "https://www.openstreetmap.org/" + type.name().toLowerCase(Locale.US) + "/" + osmId;
		}
		return null;
	}

	private String getIconName(PoiType poiType) {
		if (poiType != null) {
			if (poiType.getParentType() != null) {
				return poiType.getParentType().getIconKeyName();
			} else if (poiType.getFilter() != null) {
				return poiType.getFilter().getIconKeyName();
			} else if (poiType.getCategory() != null) {
				return poiType.getCategory().getIconKeyName();
			}
		}
		return null;
	}

	private Map<String, PoiType> getPoiAdditionalsByKey() {
		Map<String, PoiType> byKey = poiAdditionalsByKey;
		if (byKey == null) {
			synchronized (this) {
				byKey = poiAdditionalsByKey;
				if (byKey == null) {
					byKey = new HashMap<>();
					for (PoiCategory pc : MapPoiTypes.getDefault().getCategories()) {
						collectPoiAdditionals(pc, byKey);
						for (PoiFilter pf : pc.getPoiFilters()) {
							collectPoiAdditionals(pf, byKey);
						}
						for (PoiType p : pc.getPoiTypes()) {
							collectPoiAdditionals(p, byKey);
						}
					}
					poiAdditionalsByKey = byKey;
				}
			}
		}
		return byKey;
	}

	private void collectPoiAdditionals(AbstractPoiType p, Map<String, PoiType> byKey) {
		List<PoiType> additionals = p.getPoiAdditionals();
		if (additionals != null) {
			for (PoiType pt : additionals) {
				byKey.putIfAbsent(pt.getKeyName(), pt);
			}
		}
	}

	@Nullable
	private static Calendar getClientTime(String timeZone) {
		if (Algorithms.isBlank(timeZone)) {
			return null;
		}
		Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone(timeZone));
		calendar.setTimeInMillis(System.currentTimeMillis());
		return calendar;
	}
}
