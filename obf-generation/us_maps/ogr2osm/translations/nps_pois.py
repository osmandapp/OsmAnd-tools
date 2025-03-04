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
			if POITYPE.lower() == "restroom" or POITYPE.lower() == "vault toilet" or POITYPE.lower() == "flush toilet" or POITYPE.lower() == "floating restroom":
				tags.update({'amenity':'toilets'})
			if POITYPE.lower() == "trailhead" or POITYPE.lower() == "trail":
				tags.update({'us_maps_recreation_area_marker_activity':'trailhead'})
			if POITYPE.lower() == "all-terrain vehicle trail":
				tags.update({'us_maps_recreation_area_marker_activity':'trailhead'})
				tags.update({'vehicle':'yes'})
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
			if POITYPE.lower() == "point of interest" or POITYPE.lower() == "site of interest":
				tags.update({'us_maps_recreation_area_activity_poi':'yes'})
			if POITYPE.lower() == "primitive camping":
				tags.update({'us_maps_recreation_area_activity_campground_camping':'yes'})
				tags.update({'impromptu':'yes'})
			if POITYPE.lower() == "potable water" or POITYPE.lower() == "water - drinking/potable":
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
				tags.update({'us_maps_recreation_area_facility_historic_site':'yes'}) #
			if POITYPE.lower() == "bus stop / shuttle stop":
				tags.update({'highway':'bus_stop'})
			if POITYPE.lower() == "litter receptacle":
				tags.update({'amenity':'waste_basket'})
			if POITYPE.lower() == "ridge":
				tags.update({'natural':'ridge'})
			if POITYPE.lower() == "waterfall":
				tags.update({'natural':'waterfall'})
			if POITYPE.lower() == "monument":
				tags.update({'historic':'monument'})
			if POITYPE.lower() == "roadside pullout":
				tags.update({'highway':'passing_place'})
			if POITYPE.lower() == "information":
				tags.update({'tourism':'information'})
			if POITYPE.lower() == "trail marker" or POITYPE.lower() == "trail sign":
				tags.update({'tourism':'information'})
				tags.update({'information':'route_marker'})
			if POITYPE.lower() == "canoe / kayak access" or POITYPE.lower() == "canoe/kayak access":
				tags.update({'us_maps_recreation_area_activity_canoe_kayak':'yes'}) #
			if POITYPE.lower() == "picnic table":
				tags.update({'amenity':'picnic_site'})
				tags.update({'picnic_table':'yes'})
			if POITYPE.lower() == "dumpster":
				tags.update({'amenity':'waste_disposal'})
			if POITYPE.lower() == "geyser":
				tags.update({'natural':'geyser'})
			if POITYPE.lower() == "park":
				tags.update({'natural':'park'})
			if POITYPE.lower() == "food service":
				tags.update({'amenity':'fast_food'})
				tags.update({'fast_food':'cafeteria'})
			if POITYPE.lower() == "mountain pass (saddle / gap)":
				tags.update({'mountain_pass':'yes'})
			if POITYPE.lower() == "gateway sign":
				tags.update({'us_maps_recreation_area_facility_gateway_sign':'yes'}) #
			if POITYPE.lower() == "gate":
				tags.update({'barrier':'gate'})
			if POITYPE.lower() == "arch":
				tags.update({'us_maps_recreation_area_facility_arch':'yes'}) #
			if POITYPE.lower() == "shelter":
				tags.update({'amenity':'shelter'})
			if POITYPE.lower() == "entrance station":
				tags.update({'us_maps_recreation_area_facility_entrance_station':'yes'}) #
			if POITYPE.lower() == "locale":
				tags.update({'us_maps_recreation_area_facility_locale':'yes'}) #
			if POITYPE.lower() == "store":
				tags.update({'us_maps_recreation_area_facility_store':'yes'}) #
			if POITYPE.lower() == "regulatory sign":
				tags.update({'us_maps_recreation_area_facility_regulatory_sign':'yes'}) #
			if POITYPE.lower() == "museum":
				tags.update({'tourism':'museum'})
			if POITYPE.lower() == "monument / memorial":
				tags.update({'historic':'memorial'})
			if POITYPE.lower() == "lodging":
				tags.update({'us_maps_recreation_area_facility_cabin_rentals':'yes'})
			if POITYPE.lower() == "amphitheater":
				tags.update({'us_maps_recreation_area_facility_amphitheater':'yes'}) #
			if POITYPE.lower() == "office":
				tags.update({'office':'company'})
			if POITYPE.lower() == "entrance / exit" or POITYPE.lower() == "entrance/exit":
				tags.update({'amenity':'entrance'})
			if POITYPE.lower() == "mooring":
				tags.update({'us_maps_recreation_area_facility_mooring':'yes'})
			if POITYPE.lower() == "historic ruins":
				tags.update({'historic':'ruins'})
			if POITYPE.lower() == "water access":
				tags.update({'whitewater':'put_in'})
			if POITYPE.lower() == "campfire ring":
				tags.update({'leisure':'firepit'})
			if POITYPE.lower() == "lock":
				tags.update({'lock':'yes'})
			if POITYPE.lower() == "wheelchair accessible":
				tags.update({'wheelchair':'yes'})
			if POITYPE.lower() == "beach":
				tags.update({'natural':'beach'})
			if POITYPE.lower() == "cabin" or POITYPE.lower() == "lodge":
				tags.update({'us_maps_recreation_area_facility_cabin':'yes'})
			if POITYPE.lower() == "athletic field":
				tags.update({'leisure':'pitch'})
			if POITYPE.lower() == "lighthouse":
				tags.update({'us_maps_recreation_area_facility_lighthouse':'yes'})
			if POITYPE.lower() == "fishing":
				tags.update({'us_maps_recreation_area_activity_fishing':'yes'})
			if POITYPE.lower() == "headquarters":
				tags.update({'us_maps_recreation_area_facility_headquarters':'yes'}) #
			if POITYPE.lower() == "swimming area":
				tags.update({'us_maps_recreation_area_activity_swimming':'yes'})
			if POITYPE.lower() == "valley":
				tags.update({'natural':'valley'})
			if POITYPE.lower() == "marina":
				tags.update({'leisure':'marina'})
			if POITYPE.lower() == "telephone":
				tags.update({'amenity':'telephone'})
			if POITYPE.lower() == "grill":
				tags.update({'barbecue_grill':'yes'})
			if POITYPE.lower() == "fortification":
				tags.update({'us_maps_recreation_area_facility_fortification':'yes'}) #
			if POITYPE.lower() == "dump station":
				tags.update({'us_maps_recreation_area_facility_rv_dump_station':'yes'})
			if POITYPE.lower() == "populated place":
				tags.update({'us_maps_recreation_area_facility_populated_place':'yes'}) #
			if POITYPE.lower() == "showers":
				tags.update({'amenity':'shower'})
			if POITYPE.lower() == "gas station":
				tags.update({'amenity':'fuel'})
			if POITYPE.lower() == "playground":
				tags.update({'leisure':'playground'})
			if POITYPE.lower() == "lake":
				tags.update({'natural':'water'})
			if POITYPE.lower() == "tree":
				tags.update({'natural':'tree'})
			if POITYPE.lower() == "ferry terminal":
				tags.update({'amenity':'ferry_terminal'})
			if POITYPE.lower() == "information board":
				tags.update({'tourism':'information'})
				tags.update({'information':'board'})
			if POITYPE.lower() == "natural feature":
				tags.update({'us_maps_recreation_area_facility_natural_feature':'yes'}) #
			if POITYPE.lower() == "church":
				tags.update({'amenity':'place_of_worship'})
				tags.update({'building:type':'church'})
			if POITYPE.lower() == "stable":
				tags.update({'us_maps_recreation_area_facility_stable':'yes'}) #
			if POITYPE.lower() == "island":
				tags.update({'place':'island'})
			if POITYPE.lower() == "historic cabin":
				tags.update({'us_maps_recreation_area_facility_historic_cabin':'yes'}) #
			if POITYPE.lower() == "education center":
				tags.update({'us_maps_recreation_area_facility_education_center':'yes'}) #
			if POITYPE.lower() == "administrative office":
				tags.update({'office':'administrative'})
			if POITYPE.lower() == "gift shop":
				tags.update({'shop':'gift'})
			if POITYPE.lower() == "stream":
				tags.update({'waterway':'stream'})
			if POITYPE.lower() == "steps":
				tags.update({'highway':'steps'})
			if POITYPE.lower() == "weather shelter":
				tags.update({'amenity':'shelter'})
				tags.update({'shelter_type':'weather_shelter'})
			if POITYPE.lower() == "university building":
				tags.update({'building':'university'})
			if POITYPE.lower() == "airport":
				tags.update({'aeroway':'aerodrome'})
			if POITYPE.lower() == "recycling":
				tags.update({'amenity':'recycling'})
			if POITYPE.lower() == "garden":
				tags.update({'leisure':'garden'})
			if POITYPE.lower() == "tunnel":
				tags.update({'highway':'tunnel'})
			if POITYPE.lower() == "junction":
				tags.update({'junction':'yes'})
			if POITYPE.lower() == "dam":
				tags.update({'waterway':'dam'})
			if POITYPE.lower() == "cattle guard":
				tags.update({'us_maps_recreation_area_facility_cattle_guard':'yes'}) #
			if POITYPE.lower() == "bike rack":
				tags.update({'amenity':'bicycle_parking'})
				tags.update({'bicycle_parking':'rack'})
			if POITYPE.lower() == "spring":
				tags.update({'natural':'spring'})
			if POITYPE.lower() == "fire hydrant":
				tags.update({'emergency':'fire_hydrant'})
			if POITYPE.lower() == "battery":
				tags.update({'us_maps_recreation_area_facility_battery':'yes'}) #
			if POITYPE.lower() == "tower":
				tags.update({'man_made':'tower'})
			if POITYPE.lower() == "grave":
				tags.update({'cemetery':'grave'})
			if POITYPE.lower() == "bay":
				tags.update({'natural':'bay'})
			if POITYPE.lower() == "basin":
				tags.update({'landuse':'basin'})
			if POITYPE.lower() == "post office":
				tags.update({'amenity':'post_office'})
			if POITYPE.lower() == "barn":
				tags.update({'building':'barn'})
			if POITYPE.lower() == "grove":
				tags.update({'natural':'wood'})
			if POITYPE.lower() == "bicycle - sharing station":
				tags.update({'amenity':'bicycle_rental'})
			if POITYPE.lower() == "memorial":
				tags.update({'historic':'memorial'})
			if POITYPE.lower() == "totem pole":
				tags.update({'tourism':'artwork'})
				tags.update({'artwork_type':'totem_pole'})
			if POITYPE.lower() == "rapids":
				tags.update({'whitewater':'rapid'})
			if POITYPE.lower() == "fee booth":
				tags.update({'barrier':'toll_booth'})
			if POITYPE.lower() == "flag pole":
				tags.update({'man_made':'flagpole'})
			if POITYPE.lower() == "cliff":
				tags.update({'natural':'cliff'})
			if POITYPE.lower() == "canal":
				tags.update({'waterway':'canal'})
			if POITYPE.lower() == "pavilion":
				tags.update({'building':'yes'})
			if POITYPE.lower() == "horseback riding":
				tags.update({'leisure':'horse_riding'})
			if POITYPE.lower() == "historic marker":
				tags.update({'us_maps_recreation_area_facility_historic_marker':'yes'}) #
			if POITYPE.lower() == "first aid station":
				tags.update({'emergency':'ambulance_station'})
			if POITYPE.lower() == "electric vehicle parking":
				tags.update({'amenity':'parking'})
			if POITYPE.lower() == "cave entrance":
				tags.update({'natural':'cave_entrance'})
			if POITYPE.lower() == "airstrip":
				tags.update({'aeroway':'runway'})
			if POITYPE.lower() == "tennis":
				tags.update({'sport':'tennis'})
			if POITYPE.lower() == "ranch":
				tags.update({'us_maps_recreation_area_facility_ranch':'yes'}) #
			if POITYPE.lower() == "patrol cabin":
				tags.update({'us_maps_recreation_area_facility_patrol_cabin':'yes'}) #
			if POITYPE.lower() == "chapel":
				tags.update({'amenity':'place_of_worship'})
				tags.update({'building':'chapel'})
			if POITYPE.lower() == "hut":
				tags.update({'tourism':'alpine_hut'})
			if POITYPE.lower() == "cross-country ski trail":
				tags.update({'us_maps_recreation_area_activity_cross_country_ski_trail':'yes'}) #
			if POITYPE.lower() == "school building":
				tags.update({'building':'school'})
			if POITYPE.lower() == "rv campground":
				tags.update({'us_maps_recreation_area_activity_rv_camping':'yes'})
			if POITYPE.lower() == "golf course":
				tags.update({'leisure':'golf_course'})
			if POITYPE.lower() == "fish cleaning":
				tags.update({'us_maps_recreation_area_facility_fish_cleaning':'yes'}) #
			if POITYPE.lower() == "fence":
				tags.update({'barrier':'gate'})
			if POITYPE.lower() == "":
				tags.update({'us_maps_recreation_area_facility_':'yes'})
			if POITYPE.lower() == "":
				tags.update({'us_maps_recreation_area_facility_':'yes'})
			if POITYPE.lower() == "":
				tags.update({'us_maps_recreation_area_facility_':'yes'})
			if POITYPE.lower() == "":
				tags.update({'us_maps_recreation_area_facility_':'yes'})
			if POITYPE.lower() == "":
				tags.update({'us_maps_recreation_area_facility_':'yes'})


		tags.update({'us_maps':'recreation_area'})
		tags.update({'nps':'yes'})
		return tags
