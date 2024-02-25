# -*- coding: utf-8 -*-
import ogr2osm
import string

class BLMRoadsTrailsTranslation(ogr2osm.TranslationBase):
	def filter_tags(self, attrs):
		if not attrs:
			return

		tags = {}
		road_name = ''
		road_alt_name = ''

		ROUTE_PRMRY_NM = attrs['ROUTE_PRMR'] # The name, including any numeric portion, by which the feature is known according to the person or organization contained in route ownership code attribute.
		ROUTE_SCNDRY_SPCL_DSGNTN_NM = attrs['ROUTE_SCND'] # The name or phrase, including any numeric portion, which identifies the special designation.
		PLAN_ASSET_CLASS = attrs['PLAN_ASSET'] # The basic characteristics of a route including if it is part of the BLM Transportation System as a Road, Primitive Road or Trail.
		PLAN_OHV_ROUTE_DSGNTN = attrs['PLAN_OHV_R'] # OHV designation represents the limitations, which are governed by constraints identified in the Resource Management Plan (RMP) and TMP recommendations that are placed on a feature with regard to use of Off-Highway Vehicles (OHV) only.
#		PLAN_MODE_TRNSPRT = attrs['PLAN_MODE_'] # Mode of transport as identified during the planning process. Indicates the general category of transportation allowed on the route.
#		PLAN_ADD_MODE_TRNSPRT_RSTRT_CD = attrs['PLAN_ADD_M'] # Indicates if there any types of restrictions on mode of transport beyond those associated with the planned mode of transport attribute.
		OBSRVE_MODE_TRNSPRT = attrs['OBSRVE_MOD'] # Indicates the general category of transportation observed on the route.
		OBSRVE_ROUTE_USE_CLASS = attrs['OBSRVE_ROU'] # Describes the observed physical suitability of use of a road in order to aid in safe travel by the public across the BLM road network.
		OBSRVE_SRFCE_TYPE = attrs['OBSRVE_SRF'] # The main surface material of the ground transportation linear feature at the time the observation was made.
		OBSRVE_FUNC_CLASS = attrs['OBSRVE_FUN'] # This attribute groups routes according to the type of service and amount of traffic they have.
		ROUTE_SPCL_DSGNTN_TYPE = attrs['ROUTE_SPCL'] # The special designations applicable to each ground transportation linear feature.
		EXSTNG_AUTH = attrs['EXSTNG_AUT'] # Access ?
		PLAN_ALLOW = attrs['PLAN_ALLOW']
		FILE_NAME = attrs['FILE_NAME']

		if 'ROUTE_PRMR' in attrs and ROUTE_PRMRY_NM and ROUTE_PRMRY_NM.lower() != 'no' and ROUTE_PRMRY_NM.lower() != 'unknown' and ROUTE_PRMRY_NM.lower() != '<null>':
			pre_name = ROUTE_PRMRY_NM.lower().replace("<null","").title().replace("'S ","'s ").replace("Blm ","BLM ")
			road_name = ''.join(filter(lambda x: x in string.printable, pre_name))
			tags['name'] = road_name
		if 'ROUTE_SCND' in attrs and ROUTE_SCNDRY_SPCL_DSGNTN_NM and ROUTE_SCNDRY_SPCL_DSGNTN_NM.lower() != 'no' and ROUTE_SCNDRY_SPCL_DSGNTN_NM.lower() != 'unknown' and ROUTE_SCNDRY_SPCL_DSGNTN_NM.lower() != '<null>':
			pre_alt_name = ROUTE_SCNDRY_SPCL_DSGNTN_NM.lower().replace("<null","").title().replace("'S ","'s ").replace("Blm ","BLM ")
			road_alt_name = ''.join(filter(lambda x: x in string.printable, pre_alt_name))
			if road_alt_name != road_name:
				tags['alt_name'] = road_alt_name
		if PLAN_ASSET_CLASS == 'Transportation System - Primitive Road':
			tags.update({'us_maps':'road'})
			tags.update({'us_maps_road_asset_class':'primitive_road'})
		if PLAN_ASSET_CLASS == 'Transportation System - Road':
			tags.update({'us_maps':'road'})
			tags.update({'us_maps_road_asset_class':'road'})
		if PLAN_ASSET_CLASS == 'Transportation System - Trail':
			tags.update({'us_maps':'trail'})
			tags.update({'us_maps_road_asset_class':'trail'})
		if PLAN_ASSET_CLASS == 'Not Assessed - Trail':
			tags.update({'us_maps':'trail'})
			tags.update({'us_maps_road_asset_class':'not_assessed_trail'})
		if 'PLAN_OHV_R' in attrs and PLAN_OHV_ROUTE_DSGNTN:
			tags['ohv'] = PLAN_OHV_ROUTE_DSGNTN.lower()
		if 'OBSRVE_MOD' in attrs and OBSRVE_MODE_TRNSPRT:
			tags['us_maps_road_transport_category'] = OBSRVE_MODE_TRNSPRT.lower().replace("-","_")
		if OBSRVE_ROUTE_USE_CLASS.lower() == '2wd low':
			tags.update({'us_maps_road_use_class':'2wd_low'})
		if OBSRVE_ROUTE_USE_CLASS.lower().replace(" ","") == '4wdhighclearance/specialized':
			tags.update({'us_maps_road_use_class':'4wd_high_clearance_or_Specialized'})
		if OBSRVE_ROUTE_USE_CLASS.lower() == '4wd low':
			tags.update({'us_maps_road_use_class':'4wd_low'})
		if OBSRVE_ROUTE_USE_CLASS.lower() == 'atv':
			tags.update({'us_maps_road_use_class':'atv'})
		if OBSRVE_ROUTE_USE_CLASS.lower() == 'foot only':
			tags.update({'us_maps_road_use_class':'foot_only'})
		if OBSRVE_ROUTE_USE_CLASS.lower() == 'impassable':
			tags.update({'us_maps_road_use_class':'impassable'})
		if OBSRVE_ROUTE_USE_CLASS.lower() == 'motorized single track':
			tags.update({'us_maps_road_use_class':'motorized_single_track'})
		if OBSRVE_ROUTE_USE_CLASS.lower() == 'non-mechanized':
			tags.update({'us_maps_road_use_class':'non_mechanized'})
		if OBSRVE_ROUTE_USE_CLASS.lower() == 'non-motorized':
			tags.update({'us_maps_road_use_class':'non_motorized'})
		if OBSRVE_ROUTE_USE_CLASS.lower() == 'over snow vehicle':
			tags.update({'us_maps_road_use_class':'over_snow_vehicle'})
		if OBSRVE_ROUTE_USE_CLASS.lower() == 'primitive road - 4wd high clearance':
			tags.update({'us_maps_road_use_class':'primitive_road_4wd_high_clearance'})
		if OBSRVE_ROUTE_USE_CLASS.lower() == 'primitive road - 4wd low clearance':
			tags.update({'us_maps_road_use_class':'primitive_road_4wd_low_clearance'})
		if OBSRVE_ROUTE_USE_CLASS.lower() == 'stock':
			tags.update({'us_maps_road_use_class':'stock'})
		if OBSRVE_ROUTE_USE_CLASS.lower() == 'trail - non motorized':
			tags.update({'us_maps_road_use_class':'trail_non_motorized'})
		if OBSRVE_ROUTE_USE_CLASS.lower() == 'trail-unknown use':
			tags.update({'us_maps_road_use_class':'trail_unknown_use'})
		if OBSRVE_ROUTE_USE_CLASS.lower() == 'trail - utv':
			tags.update({'us_maps_road_use_class':'trail_utv'})
		if OBSRVE_ROUTE_USE_CLASS.lower() == 'Unknown':
			tags.update({'us_maps_road_use_class':'unknown'})
		if OBSRVE_ROUTE_USE_CLASS.lower() == 'utv':
			tags.update({'us_maps_road_use_class':'utv'})

		if OBSRVE_FUNC_CLASS.lower() == 'collector':
			tags.update({'us_maps_road_functional_class':'collector'})
		if OBSRVE_FUNC_CLASS.lower() == 'local':
			tags.update({'us_maps_road_functional_class':'local'})
		if OBSRVE_FUNC_CLASS.lower() == 'resource':
			tags.update({'us_maps_road_functional_class':'resource'})
		if OBSRVE_FUNC_CLASS.lower() == 'unknown':
			tags.update({'us_maps_road_functional_class':'unknown'})

		if OBSRVE_SRFCE_TYPE.lower() == 'natural':
			tags.update({'us_maps_road_surface':'native_material'})
		if OBSRVE_SRFCE_TYPE.lower() == 'natural improved':
			tags.update({'us_maps_road_surface':'improved_native_material'})
		if OBSRVE_SRFCE_TYPE.lower() == 'unknown':
			tags.update({'us_maps_road_surface':'other'})
		if OBSRVE_SRFCE_TYPE.lower() == 'aggregate':
			tags.update({'us_maps_road_surface':'aggregate'})
		if OBSRVE_SRFCE_TYPE.lower() == 'snow':
			tags.update({'us_maps_road_surface':'snow'})
		if OBSRVE_SRFCE_TYPE.lower() == 'solid surface':
			tags.update({'us_maps_road_surface':'solid_surface'})

		if ROUTE_SPCL_DSGNTN_TYPE.lower() == 'blm back country byway':
			tags.update({'us_maps_road_special_designation':'blm_back_country_byway'})
		if ROUTE_SPCL_DSGNTN_TYPE.lower() == 'national historic trail':
			tags.update({'us_maps_road_special_designation':'national_historic_trail'})
		if ROUTE_SPCL_DSGNTN_TYPE.lower() == 'national recreation trail' or ROUTE_SPCL_DSGNTN_TYPE.lower() == 'nrt':
			tags.update({'us_maps_road_special_designation':'national_recreation_trail'})
		if ROUTE_SPCL_DSGNTN_TYPE.lower() == 'national scenic trail' or ROUTE_SPCL_DSGNTN_TYPE.lower() == 'nst':
			tags.update({'us_maps_road_special_designation':'national_scenic_trail'})

		if EXSTNG_AUTH.lower() == 'admin':
			tags.update({'us_maps_existing_auth_status':'admin'})
		if EXSTNG_AUTH.lower() == 'no' or EXSTNG_AUTH.lower() == 'none':
			tags.update({'us_maps_existing_auth_status':'closed'})
		if EXSTNG_AUTH.lower() == 'opened' or EXSTNG_AUTH.lower() == 'yes':
			tags.update({'us_maps_existing_auth_status':'opened'})
		if EXSTNG_AUTH.lower() == 'ohv/atv':
			tags.update({'us_maps_existing_auth_status':'ohv_atv'})

		if 'PLAN_ALLOW' in attrs and PLAN_ALLOW:
			tags['us_maps_road_planned_access'] = PLAN_ALLOW.lower().replace("/","_").replace(" ","_")

		if 'FILE_NAME' in attrs and FILE_NAME:
			tags['us_maps_blm_file_name'] = FILE_NAME.split("_gtlf_")[1]

		tags.update({'blm':'yes'})
		return tags