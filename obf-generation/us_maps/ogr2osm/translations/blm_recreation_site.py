# -*- coding: utf-8 -*-
import ogr2osm

class USFSRecreationAreaActivitiesTranslation(ogr2osm.TranslationBase):

	def filter_tags(self, attrs):
		if not attrs:
			return

		tags = {}

		FET_TYPE = attrs['FET_TYPE'].strip() # The broad theme of features that may be depicted as points. This particular attribute uses 14 possible categories (codes).
		FET_SUBTYPE = attrs['FET_SUBTYPE'].strip()
		FET_NAME = attrs['FET_NAME'].replace('"','').strip()
		DESCRIPTION = attrs['DESCRIPTION'].strip()
		WEB_LINK = attrs['WEB_LINK'].strip()
		PHOTO_TEXT = attrs['PHOTO_TEXT'].strip()
		WEB_LINK = attrs['WEB_LINK'].strip()
		PHOTO_LINK = attrs['PHOTO_LINK'].strip()
		PHOTO_THUMB = attrs['PHOTO_THUMB'].strip()

		if 'FET_NAME' in attrs and FET_NAME:
			name_normalized = FET_NAME
			if FET_NAME.isupper():
				name_normalized = FET_NAME.lower().title()
			tags['name'] = name_normalized
		if 'DESCRIPTION' in attrs and DESCRIPTION != "<Null>":
			description_normalized = DESCRIPTION
			if DESCRIPTION.isupper():
				description_normalized = DESCRIPTION.lower().title()
			tags['description'] = description_normalized
		if 'WEB_LINK' in attrs and WEB_LINK:
			tags['url'] = WEB_LINK
		if 'PHOTO_TEXT' in attrs and PHOTO_TEXT != WEB_LINK and "http" not in PHOTO_TEXT:
			tags['image_title'] = PHOTO_TEXT
		if 'PHOTO_LINK' in attrs and (PHOTO_LINK.endswith(".jpg") or PHOTO_LINK.endswith(".jpeg") or PHOTO_LINK.endswith(".png")):
			tags['image'] = PHOTO_LINK
		if 'PHOTO_THUMB' in attrs and (PHOTO_THUMB.endswith(".jpg") or PHOTO_THUMB.endswith(".jpeg") or PHOTO_THUMB.endswith(".png")):
			tags['us_maps_photo_thumb'] = PHOTO_THUMB
		if 'FET_SUBTYPE' in attrs and 'FET_TYPE' in attrs:
			if FET_SUBTYPE.lower() == "access point" or FET_SUBTYPE.lower() == "trail head" or FET_TYPE == 7:
				tags.update({'us_maps_recreation_area_activity_trailhead':'yes'})
		if 'FET_SUBTYPE' in attrs:
			if FET_SUBTYPE == "ERMA" or FET_SUBTYPE == "SRMA" or FET_SUBTYPE.lower() == "river mile":
				return
		if 'FET_SUBTYPE' in attrs:
			if FET_SUBTYPE.lower() == "airplane landing strip":
				tags.update({'us_maps_recreation_area_facility_airplane_landing_strip':'yes'})
			if FET_SUBTYPE.lower() == "blm ranger station/field office/contact station":
				tags.update({'us_maps_recreation_area_facility_ranger_station_office':'yes'})
			if FET_SUBTYPE.lower() == "boat launch":
				tags.update({'us_maps_recreation_area_facility_boat_launch':'yes'})
			if FET_SUBTYPE.lower() == "boat ramp":
				tags.update({'us_maps_recreation_area_facility_boat_ramp':'yes'})
			if FET_SUBTYPE.lower() == "boat takeout":
				tags.update({'us_maps_recreation_area_facility_boat_takeout':'yes'})
			if FET_SUBTYPE.lower() == "cabin":
				tags.update({'us_maps_recreation_area_facility_cabin':'yes'})
			if FET_SUBTYPE.lower() == "cabin - reservable - fee":
				tags.update({'us_maps_recreation_area_facility_cabin_rentals':'yes'})
				tags.update({'fee':'yes'})
				tags.update({'reservation':'yes'})
			if FET_SUBTYPE.lower() == "cabin - non reservable - no fee":
				tags.update({'us_maps_recreation_area_facility_cabin':'yes'})
				tags.update({'fee':'no'})
				tags.update({'reservation':'no'})
			if FET_SUBTYPE.lower() == "campground":
				tags.update({'us_maps_recreation_area_activity_campground_camping':'yes'})
			if FET_SUBTYPE.lower().startswith("campsite"):
				if "developed" in FET_SUBTYPE.lower():
					tags.update({'impromptu':'no'})
				if "primitive" in FET_SUBTYPE.lower():
					tags.update({'impromptu':'yes'})
				if "reservable" in FET_SUBTYPE.lower():
					tags.update({'reservation':'yes'})
				if "non reservable" in FET_SUBTYPE.lower():
					tags.update({'reservation':'no'})
				if "fee" in FET_SUBTYPE.lower():
					tags.update({'fee':'yes'})
				if "no fee" in FET_SUBTYPE.lower():
					tags.update({'fee':'no'})
				tags.update({'us_maps_recreation_area_activity_campground_camping':'yes'})
			if FET_SUBTYPE.lower() == "day use site":
				tags.update({'us_maps_recreation_area_activity_day_use_site':'yes'})
			if FET_SUBTYPE.lower() == "fire lookout":
				tags.update({'us_maps_recreation_area_facility_fire_lookout':'yes'})
			if FET_SUBTYPE.lower() == "group shelter":
				tags.update({'us_maps_recreation_area_facility_group_shelter':'yes'})
			if FET_SUBTYPE.lower() == "horse stanchion":
				tags.update({'us_maps_recreation_area_facility_horse_stanchion':'yes'})
			if FET_SUBTYPE.lower().startswith("horse corral"):
				tags.update({'us_maps_recreation_area_facility_horse_corral':'yes'})

			if FET_SUBTYPE.lower() == "interpretive site":
				tags.update({'us_maps_recreation_area_activity_interpretive_areas':'yes'})
			if FET_SUBTYPE.lower() == "kiosk":
				tags.update({'us_maps_recreation_area_facility_kiosk':'yes'})
			if FET_SUBTYPE.lower() == "lighthouse":
				tags.update({'us_maps_recreation_area_facility_lighthouse':'yes'})
			if FET_SUBTYPE.lower() == "natural area/endangered area":
				tags.update({'us_maps_recreation_area_facility_natural_endangered_area':'yes'})
			if FET_SUBTYPE.lower() == "ohv designated area":
				tags.update({'us_maps_recreation_area_activity_ohv_open_area_riding':'yes'})
			if FET_SUBTYPE.lower() == "parking area":
				tags.update({'us_maps_recreation_area_facility_parking':'yes'})
			if FET_SUBTYPE.lower() == "picnic area":
				tags.update({'us_maps_recreation_area_activity_picnicking':'yes'})
			if FET_SUBTYPE.lower() == "point of interest":
				tags.update({'us_maps_recreation_area_activity_poi':'yes'})
			if FET_SUBTYPE.lower() == "potable water":
				tags.update({'drinking_water':'yes'})
			if FET_SUBTYPE.lower() == "rapid":
				tags.update({'us_maps_recreation_area_facility_rapid':'yes'})

			if FET_SUBTYPE.lower() == "rock climbing":
				tags.update({'us_maps_recreation_area_activity_rock_climbing':'yes'})
			if FET_SUBTYPE.lower() == "rock hounding":
				tags.update({'us_maps_recreation_area_activity_rockhounding':'yes'})
			if FET_SUBTYPE.lower() == "rv dump station":
				tags.update({'us_maps_recreation_area_facility_rv_dump_station':'yes'})
			if FET_SUBTYPE.lower() == "scenic overlook":
				tags.update({'us_maps_recreation_area_activity_scenic_overlook':'yes'})

			if FET_SUBTYPE.lower() == "toilet":
				tags.update({'toilets':'yes'})

			if FET_SUBTYPE.lower() == "visitor center":
				tags.update({'us_maps_recreation_area_activity_visitor_centers':'yes'})
			if FET_SUBTYPE.lower() == "warming hut":
				tags.update({'us_maps_recreation_area_facility_warming_hut':'yes'})

		tags.update({'us_maps':'recreation_area'})
		tags.update({'blm':'yes'})
		return tags
