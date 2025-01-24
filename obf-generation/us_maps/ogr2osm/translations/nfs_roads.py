# -*- coding: utf-8 -*-
import ogr2osm

class USFSRoadsTranslation(ogr2osm.TranslationBase):

	def filter_tags(self, attrs):
		if not attrs:
			return

		tags = {}

		ID = attrs['ID'] # The official identifier of the route.
		NAME = attrs['NAME'] # Common name of the road
		OPER_MAINT_LEVEL = attrs['OPER_MAINT'] # Operational Maintenance Level defines the level to which the road is currently being maintained
		FUNCTIONAL_CLASS = attrs['FUNCTIONAL'] # The grouping of roads by the character of service they provide.
		SURFACE_TYPE = attrs['SURFACE_TY'] # The wearing course; usually designed to resist skidding, traffic abrasion and the disintegrating effects of weather.
		LANES = attrs['LANES'] # The number of lanes the travel way has.
		OPENFORUSETO = attrs['OPENFORUSE'] # OpenForUseTo identifies if a road segment is open to the public for motorized travel.
		SYMBOL_NAME = attrs['SYMBOL_NAM'] # Descriptive name for the category of the road segment, based on the Cartographic Feature File (CFF) Code. This field often is used in map legends.


		if 'ID' in attrs and ID:
			tags['us_maps_road_id'] = ID
		if 'NAME' in attrs and NAME and NAME != 'NO NAME':
			tags['name'] = NAME.lower().title().replace("'S ","'s ")

		if 'OPER_MAINT' in attrs:
			if '0' in OPER_MAINT_LEVEL:
				tags.update({'us_maps_road_oper_maint_level':'0'})
			if '1' in OPER_MAINT_LEVEL:
				tags.update({'us_maps_road_oper_maint_level':'1'})
			if '2' in OPER_MAINT_LEVEL:
				tags.update({'us_maps_road_oper_maint_level':'2'})
			if '3' in OPER_MAINT_LEVEL:
				tags.update({'us_maps_road_oper_maint_level':'3'})
			if '4' in OPER_MAINT_LEVEL:
				tags.update({'us_maps_road_oper_maint_level':'4'})
			if '5' in OPER_MAINT_LEVEL:
				tags.update({'us_maps_road_oper_maint_level':'5'})

		if FUNCTIONAL_CLASS == 'A - ARTERIAL':
			tags.update({'us_maps_road_functional_class':'arterial'})
		if FUNCTIONAL_CLASS == 'C - COLLECTOR':
			tags.update({'us_maps_road_functional_class':'collector'})
		if FUNCTIONAL_CLASS == 'L - LOCAL':
			tags.update({'us_maps_road_functional_class':'local'})
		if FUNCTIONAL_CLASS == 'L - LOCAL IMPORTANT':
			tags.update({'us_maps_road_functional_class':'local_important'})


		if SURFACE_TYPE == 'NATIVE MATERIAL' or SURFACE_TYPE == 'NAT - NATIVE MATERIAL':
			tags.update({'surface':'native_material'})
		if SURFACE_TYPE == 'AC - ASPHALT':
			tags.update({'surface':'asphalt'})
		if SURFACE_TYPE == 'AGG - CRUSHED AGGREGATE OR GRAVEL':
			tags.update({'surface':'crushed_aggregate_or_gravel'})
		if SURFACE_TYPE == 'AGG - LIMESTONE':
			tags.update({'surface':'limestone'})
		if SURFACE_TYPE == 'AGG - SCORIA':
			tags.update({'surface':'scoria'})
		if SURFACE_TYPE == 'BST - BITUMINOUS SURFACE TREATMENT':
			tags.update({'surface':'bituminous_surface_treatment'})
		if SURFACE_TYPE == 'CIN - CINDER SURFACE':
			tags.update({'surface':'cinder'})
		if SURFACE_TYPE == 'CSOIL - COMPACTED SOIL':
			tags.update({'surface':'compacted_soil'})
		if SURFACE_TYPE == 'FSOIL - FROZEN SOIL':
			tags.update({'surface':'frozen_soil'})
		if SURFACE_TYPE == 'GRA - GRASS (NAT)' or SURFACE_TYPE == 'SOD - GRASS':
			tags.update({'surface':'grass'})
		if SURFACE_TYPE == 'IMP - IMPROVED NATIVE MATERIAL':
			tags.update({'surface':'improved_native_material'})
		if SURFACE_TYPE == 'PCC - PORTLAND CEMENT CONCRETE':
			tags.update({'surface':'portland_cement_concrete'})
		if SURFACE_TYPE == 'PIT - PIT RUN SHOT ROCK':
			tags.update({'surface':'pit_run_shot_rock'})
		if SURFACE_TYPE == 'P - PAVED':
			tags.update({'surface':'paved'})
		if SURFACE_TYPE == 'OTHER':
			tags.update({'surface':'other'})

		if 'LANES' in attrs:
			if '1' in LANES or LANES == 'SINGLE':
				tags.update({'lanes':'1'})
			if '2' in LANES:
				tags.update({'lanes':'2'})
			if '3' in LANES:
				tags.update({'lanes':'3'})
			if '4' in LANES:
				tags.update({'lanes':'4'})
			if '5' in LANES:
				tags.update({'lanes':'5'})
			if '6' in LANES:
				tags.update({'lanes':'6'})

		if OPENFORUSETO == 'ADMIN':
			tags.update({'us_maps_road_accessibility_status':'admin'})
		if OPENFORUSETO == 'ALL' or OPENFORUSETO == 'PUBLIC':
			tags.update({'us_maps_road_accessibility_status':'all'})
		if OPENFORUSETO == '':
			tags.update({'us_maps_road_accessibility_status':'no_access'})

		if SYMBOL_NAME == 'Dirt Road, Suitable for Passenger Car':
			tags.update({'us_maps_road_symbol_name':'dirt_road_suitable_for_passenger_car'})
		if SYMBOL_NAME == 'Road, Not Maintained for Passenger Car':
			tags.update({'us_maps_road_symbol_name':'road_not_maintained_for_passenger_car'})
		if SYMBOL_NAME == 'Gravel Road, Suitable for Passenger Car':
			tags.update({'us_maps_road_symbol_name':'gravel_road_suitable_for_passenger_car'})
		if SYMBOL_NAME == 'Paved Road':
			tags.update({'us_maps_road_symbol_name':'paved_road'})

		tags.update({'us_maps':'road'})
		tags.update({'nfs':'yes'})
		return tags
