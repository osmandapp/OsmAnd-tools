# -*- coding: utf-8 -*-
import ogr2osm
import re

def sanitize_xml_string(s):
    # Removes invalid ASCII symbols (0-31, except \t, \n, \r)
    return re.sub(r'[\x00-\x08\x0B-\x0C\x0E-\x1F]', '', s)

class USNPSPoisTranslation(ogr2osm.TranslationBase):

	def filter_tags(self, attrs):
		if not attrs:
			return

		tags = {}

		POINAME = attrs['POINAME']
		MAPLABEL = attrs['MAPLABEL']
		POITYPE = attrs['POITYPE']
		POISTATUS = attrs['POISTATUS']
		SEASONAL = attrs['SEASONAL']
		SEASDESC = attrs['SEASDESC']
		OPENTOPUBLIC = attrs['OPENTOPUBLIC']
		NOTES = attrs['NOTES']

		if ('POISTATUS' in attrs and POISTATUS.strip().lower() == "not existing":
			return
		if ('MAPLABEL' in attrs and MAPLABEL) or ('POINAME' in attrs and POINAME):
			if 'MAPLABEL' in attrs and MAPLABEL:
				name = MAPLABEL.strip().strip()
				if name:
					tags['name'] = sanitize_xml_string(name).lower().title().replace("'S ","'s ")
			elif 'POINAME' in attrs and POINAME:
				name = POINAME.strip().strip()
				if name:
					tags['name'] = sanitize_xml_string(name).lower().title().replace("'S ","'s ")
		if ('SEASONAL' in attrs and SEASONAL.lower() == 'yes'):
			tags.update({'seasonal':'yes'})
		if ('SEASONAL' in attrs and SEASONAL.lower() == 'no'):
			tags.update({'seasonal':'no'})
		if 'SEASDESC' in attrs and SEASDESC:
			tags['seasonal_description'] = SEASDESC.strip().strip()
		if ('OPENTOPUBLIC' in attrs and OPENTOPUBLIC.lower() == 'yes'):
			tags.update({'access':'yes'})
		if ('OPENTOPUBLIC' in attrs and OPENTOPUBLIC.lower() == 'no'):
			tags.update({'access':'no'})
		if 'NOTES' in attrs and ("{" not in NOTES or "}" not in NOTES or "<Null>" not in NOTES):
			tags['description'] = sanitize_xml_string(name).strip().lower().title().replace("'S ","'s ")

		if 'POITYPE' in attrs and POITYPE:
			if POITYPE.lower() == "campground":
				tags.update({'us_maps_recreation_area_marker_activity':'campground_camping'})
			if POITYPE.lower() == "restroom":
				tags.update({'toilets':'yes'})
			if POITYPE.lower() == "trailhead":
				tags.update({'us_maps_recreation_area_marker_activity':'trailhead'})
			if POITYPE.lower() == "mile marker":
				tags.update({'us_maps_recreation_area_marker_activity':'mile_marker'}) #
			if POITYPE.lower() == "picnic area":
				tags.update({'us_maps_recreation_area_marker_activity':'picnicking'})
			if POITYPE.lower() == "boat dock":
				tags.update({'us_maps_recreation_area_facility_boat_dock':'boat_dock'}) #
			if POITYPE.lower() == "overlook" or POITYPE.lower() == "viewpoint":
				tags.update({'us_maps_recreation_area_activity_scenic_overlook':'yes'}) #
			if POITYPE.lower() == "interpretive exhibit":
				tags.update({'us_maps_recreation_area_activity_interpretive_areas':'yes'})
			if POITYPE.lower() == "campsite":
				tags.update({'us_maps_recreation_area_marker_activity':'campsite'})
			if POITYPE.lower() == "visitor center":
				tags.update({'us_maps_recreation_area_activity_visitor_centers':'yes'})
			if POITYPE.lower() == "buoy":
				tags.update({'us_maps_recreation_area_facility_buoy':'yes'}) #
			if POITYPE.lower() == "historic building":
				tags.update({'us_maps_recreation_area_facility_historic_building':'yes'}) #
			if POITYPE.lower() == "peak":
				tags.update({'us_maps_recreation_area_activity_peak':'yes'}) #
			if POITYPE.lower() == "bridge":
				tags.update({'us_maps_recreation_area_facility_bridge':'yes'}) #
			if POITYPE.lower() == "boat launch":
				tags.update({'us_maps_recreation_area_facility_boat_launch':'yes'})
			if POITYPE.lower() == "point of interest":
				tags.update({'us_maps_recreation_area_activity_poi':'yes'})
			if POITYPE.lower() == "primitive camping":
				tags.update({'us_maps_recreation_area_activity_campground_camping':'yes'})
				tags.update({'impromptu':'yes'})
			if POITYPE.lower() == "potable water":
				tags.update({'amenity':'drinking_water'})
			if POITYPE.lower() == "food box / food cache":
				tags.update({'us_maps_recreation_area_facility_food_box':'yes'}) #
			if POITYPE.lower() == "bench":
				tags.update({'amenity':'bench'})
			if POITYPE.lower() == "ranger station":
				tags.update({'us_maps_recreation_area_facility_ranger_station_office':'yes'})
			if POITYPE.lower() == "cemetery / graveyard":
				tags.update({'us_maps_recreation_area_facility_cemetery':'yes'}) #
			if POITYPE.lower() == "historic site":
				tags.update({'us_maps_recreation_area_facility_historic_site':'yes'})


		tags.update({'us_maps':'recreation_area'})
		tags.update({'nps':'yes'})
		return tags
