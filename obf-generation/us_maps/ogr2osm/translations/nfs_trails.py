# -*- coding: utf-8 -*-
import ogr2osm

class USFSTrailsTranslation(ogr2osm.TranslationBase):

	def filter_tags(self, attrs):
		if not attrs:
			return

		tags = {}

		TRAIL_NO = attrs['TRAIL_NO'] # The official numeric or alphanumeric identifier for the trail.
		TRAIL_NAME = attrs['TRAIL_NAME'] # The name that the trail or trail segment is officially or legally known by.
		TRAIL_TYPE = attrs['TRAIL_TYPE'] # A (required) category that reflects the predominant trail surface and general mode of travel accommodated by a trail
		TRAIL_CN = attrs['TRAIL_CN'] # Control number generated in Oracle to uniquely identify each trail across all Forest Service units.
		TRAIL_CLASS = attrs['TRAIL_CLAS'] # The prescribed scale of development for a trail, representing its intended design and management standards (TC 1 – 5)
		ACCESSIBILITY_STATUS = attrs['ACCESSIBIL'] # Accessibility guideline compliance status for trail segments that are designed for hiker/pedestrian use.
		TYPICAL_TREAD_CROSS_SLOPE = attrs['TYPICAL_TR'] # The tread cross slope that the user would generally expect to encounter on the section of trail. Entered in percent.
		TRAIL_SURFACE = attrs['TRAIL_SURF'] # The predominant surface type the user would expect to encounter on the trail or trail segment.
		SURFACE_FIRMNESS = attrs['SURFACE_FI'] # The firmness characteristics of the surface that the user would generally expect to encounter on the trail or trail segment.
		MINIMUM_TRAIL_WIDTH = attrs['MINIMUM_TR'] # The minimum trail width on the trail segment where passage may be physically restricted and no alternative route is readily available.
		NATIONAL_TRAIL_DESIGNATION = attrs['NATIONAL_T'] # The national designation assigned to the trail or trail segment. This includes designations by federal statute for National Historic Trails (NHT), National Scenic Trails (NST), Connecting or Side Trails (C-S), and National Recreation Trails (NRT); and also includes National Millennium Trails (NMT) and Millennium Legacy Trails (MLT).
		ADMIN_ORG = attrs['ADMIN_ORG'] # The administrative unit within the Forest Service where the trail or trail segment physically resides.
		MANAGING_ORG = attrs['MANAGING_O'] # The Forest Service administrative unit that has the long-term responsibility for the management of the trail or trail segment.
		SPECIAL_MGMT_AREA = attrs['SPECIAL_MG'] # Land area, that may be of special management concern or interest, through which the trail or trail segment crosses.
		TERRA_BASE_SYMBOLOGY = attrs['TERRA_BASE'] # This field indicates the Trail Class, or development scale, of the TERRA trail or trail segment
		MVUM_SYMBOL = attrs['MVUM_SYMBO'] # This field indicates the vehicle class or combination of vehicle classes to which the trail is open (Trail open Yearlong or Seasonal: to all vehicles; to vehicles 50” or less in width; to motorcycles; special designation)
		TERRA_MOTORIZED = attrs['TERRA_MOTO'] # This field supports basic display of motorized and non-motorized TERRA trails:
		SNOW_MOTORIZED = attrs['SNOW_MOTOR'] # This field supports basic display of motorized and non-motorized SNOW trails
		WATER_MOTORIZED = attrs['WATER_MOTO'] # This field supports basic display of motorized and non-motorized WATER trails
		ALLOWED_TERRA_USE = attrs['ALLOWED_TE'] # Indicates uses on TERRA Trails that are legally allowed on the trail
		ALLOWED_SNOW_USE = attrs['ALLOWED_SN'] # Indicates uses on SNOW Trails that are legally allowed on the trail
		HIKER_PEDESTRIAN_MANAGED = attrs['HIKER_PEDE'] # Date range for which trail is managed for hiker/pedestrian use.
		HIKER_PEDESTRIAN_ACCPT_DISC = attrs['HIKER_PE_1'] # Date range for which hiker/pedestrian use is accepted or discouraged.
		HIKER_PEDESTRIAN_RESTRICTED = attrs['HIKER_PE_2'] # Date range for which hiker/pedestrian use is restricted.
		PACK_SADDLE_MANAGED = attrs['PACK_SADDL'] # Date range for which pack & saddle use are managed.
		PACK_SADDLE_ACCPT_DISC = attrs['PACK_SAD_1'] # Date range for which pack & saddle use is accepted or discouraged.
		PACK_SADDLE_RESTRICTED = attrs['PACK_SAD_2'] # Date range for which pack & saddle use is restricted.
		BICYCLE_MANAGED = attrs['BICYCLE_MA'] # Date range for which bicycle use is managed
		BICYCLE_ACCPT_DISC = attrs['BICYCLE_AC'] # Date range for which bicycle use is accepted or discouraged.
		BICYCLE_RESTRICTED = attrs['BICYCLE_RE'] # Date range for which bicycle use is restricted.
		MOTORCYCLE_MANAGED = attrs['MOTORCYCLE'] # Date range for which motorcycle use is managed
		MOTORCYCLE_ACCPT_DISC = attrs['MOTORCYC_1'] # Date range for which motorcycle use is accepted or discouraged.
		MOTORCYCLE_RESTRICTED = attrs['MOTORCYC_2'] # Date range for which motorcycle use is restricted
		ATV_MANAGED = attrs['ATV_MANAGE'] # Date range for which ATV use is managed
		ATV_ACCPT_DISC = attrs['ATV_ACCPT_'] # Date range for which ATV use is accepted or discouraged.
		ATV_RESTRICTED = attrs['ATV_RESTRI'] # Date range for which ATV use is restricted
		FOURWD_MANAGED = attrs['FOURWD_MAN'] # Date range for which FOURWD use is managed
		FOURWD_ACCPT_DISC = attrs['FOURWD_ACC'] # Date range for which 4 wheel drive use is accepted or discouraged.
		FOURWD_RESTRICTED = attrs['FOURWD_RES'] # Date range for which 4 wheel drive use is restricted
		SNOWMOBILE_MANAGED = attrs['SNOWMOBILE'] # Date range for which SNOWMOBILE use is managed
		SNOWMOBILE_ACCPT_DISC = attrs['SNOWMOBI_1'] # Date range for which SNOWMOBILE use is accepted or discouraged.
		SNOWMOBILE_RESTRICTED = attrs['SNOWMOBI_2'] # Date range for which SNOWMOBILE use is restricted
		SNOWSHOE_MANAGED = attrs['SNOWSHOE_M'] # Date range for which SNOWSHOE use is managed
		SNOWSHOE_ACCPT_DISC = attrs['SNOWSHOE_A'] # Date range for which SNOWSHOE use is accepted or discouraged.
		SNOWSHOE_RESTRICTED = attrs['SNOWSHOE_R'] # Date range for which SNOWSHOE use is restricted
		XCOUNTRY_SKI_MANAGED = attrs['XCOUNTRY_S'] # Date range for which XCOUNTRY_SKI use is managed
		XCOUNTRY_SKI_ACCPT_DISC = attrs['XCOUNTRY_1'] # Date range for which XCOUNTRY_SKI use is accepted or discouraged.
		XCOUNTRY_SKI_RESTRICTED = attrs['XCOUNTRY_2'] # Date range for which XCOUNTRY_SKI use is restricted
		MOTOR_WATERCRAFT_MANAGED = attrs['MOTOR_WATE'] # Date range for which MOTOR_WATERCRAFT use is managed
		MOTOR_WATERCRAFT_ACCPT_DISC = attrs['MOTOR_WA_1'] # Date range for which MOTOR_WATERCRAFT use is accepted or discouraged.
		MOTOR_WATERCRAFT_RESTRICTED = attrs['MOTOR_WA_2'] # Date range for which MOTOR_WATERCRAFT use is restricted
		NONMOTOR_WATERCRAFT_MANAGED = attrs['NONMOTOR_W'] # Date range for which NONMOTOR_WATERCRAFT use is managed
		NONMOTOR_WATERCRAFT_ACCPT_DISC = attrs['NONMOTOR_1'] # Date range for which NONMOTOR_WATERCRAFT use is accepted or discouraged.
		NONMOTOR_WATERCRAFT_RESTRICTED = attrs['NONMOTOR_2'] # Date range for which NONMOTOR_WATERCRAFT use is restricted
		E_BIKE_CLASS1_MANAGED = attrs['E_BIKE_CLA'] # Date range for which E_BIKE_CLASS1 use is managed
		E_BIKE_CLASS1_ACCPT = attrs['E_BIKE_C_1'] # Date range for which E_BIKE_CLASS1 use is accepted.
		E_BIKE_CLASS1_DISC = attrs['E_BIKE_C_2'] # Date range for which E_BIKE_CLASS1 use is discouraged.
		E_BIKE_CLASS1_RESTRICTED = attrs['E_BIKE_C_3'] # Date range for which E_BIKE_CLASS1 use is restricted
		E_BIKE_CLASS2_MANAGED = attrs['E_BIKE_C_4'] # Date range for which E_BIKE_CLASS2 use is managed
		E_BIKE_CLASS2_ACCPT = attrs['E_BIKE_C_5'] # Date range for which E_BIKE_CLASS2 use is accepted.
		E_BIKE_CLASS2_DISC = attrs['E_BIKE_C_6'] # Date range for which E_BIKE_CLASS2 use is discouraged.
		E_BIKE_CLASS2_RESTRICTED = attrs['E_BIKE_C_7'] # Date range for which E_BIKE_CLASS2 use is restricted
		E_BIKE_CLASS3_MANAGED = attrs['E_BIKE_C_8'] # Date range for which E_BIKE_CLASS3 use is managed
		E_BIKE_CLASS3_ACCPT = attrs['E_BIKE_C_9'] # Date range for which E_BIKE_CLASS3 use is accepted.
		E_BIKE_CLASS3_DISC = attrs['E_BIKE__10'] # Date range for which E_BIKE_CLASS3 use is discouraged.
		E_BIKE_CLASS3_RESTRICTED = attrs['E_BIKE__11'] # Date range for which E_BIKE_CLASS3 use is restricted

		if 'TRAIL_NO' in attrs and TRAIL_NO:
			tags['us_maps_trail_number'] = TRAIL_NO
		if 'TRAIL_NAME' in attrs and TRAIL_NAME and TRAIL_NAME != 'NO NAME':
			tags['name'] = TRAIL_NAME.lower().title().replace("'S ","'s ")
		if 'TRAIL_TYPE' in attrs and TRAIL_TYPE:
			tags['us_maps_trail_type'] = TRAIL_TYPE.lower()
		if 'TRAIL_CN' in attrs and TRAIL_CN:
			tags['us_maps_trail_control_number'] = TRAIL_CN
		if 'TRAIL_CLASS' in attrs and TRAIL_CLASS:
			tags['us_maps_trail_class'] = TRAIL_CLASS
		if ACCESSIBILITY_STATUS == 'NOT ACCESSIBLE':
			tags.update({'us_maps_trail_accessibility_status':'not_accessible'})
		if ACCESSIBILITY_STATUS == 'ACCESSIBLE':
			tags.update({'us_maps_trail_accessibility_status':'accessible'})
		if 'TYPICAL_TR' in attrs and TYPICAL_TREAD_CROSS_SLOPE and TYPICAL_TREAD_CROSS_SLOPE != 'N/A':
			tags['us_maps_typical_tread_cross_slope'] = TYPICAL_TREAD_CROSS_SLOPE.lower().replace("%","")
		if TRAIL_SURFACE == 'AC- ASPHALT' or TRAIL_SURFACE == 'ASPHALT':
			tags.update({'us_maps_trail_surface':'asphalt'})
		if TRAIL_SURFACE == 'AGG - CRUSHED AGGREGATE OR GRAVEL' or TRAIL_SURFACE == 'CRUSHED AGGREGATE OR GRAVEL':
			tags.update({'us_maps_trail_surface':'crushed_aggregate_or_gravel'})
		if TRAIL_SURFACE == 'CHUNK WOOD':
			tags.update({'us_maps_trail_surface':'chunk_wood'})
		if TRAIL_SURFACE == 'CON - CONCRETE' or TRAIL_SURFACE == 'CONCRETE':
			tags.update({'us_maps_trail_surface':'concrete'})
		if TRAIL_SURFACE == 'IMPORTED COMPACTED MATERIAL':
			tags.update({'us_maps_trail_surface':'imported_compacted_material'})
		if TRAIL_SURFACE == 'IMPORTED LOOSE MATERIAL':
			tags.update({'us_maps_trail_surface':'imported_loose_material'})
		if TRAIL_SURFACE == 'NATIVE MATERIAL' or TRAIL_SURFACE == 'NAT - NATIVE MATERIAL':
			tags.update({'us_maps_trail_surface':'native_material'})
		if TRAIL_SURFACE == 'OTHER':
			tags.update({'us_maps_trail_surface':'other'})
		if TRAIL_SURFACE == 'SNOW':
			tags.update({'us_maps_trail_surface':'snow'})
		if TRAIL_SURFACE == 'WATER':
			tags.update({'us_maps_trail_surface':'water'})
		if TRAIL_SURFACE == 'F - FIRM':
			tags.update({'us_maps_trail_surface_firmness':'firm'})
		if TRAIL_SURFACE == 'H - HARD':
			tags.update({'us_maps_trail_surface_firmness':'hard'})
		if TRAIL_SURFACE == 'P - PAVED':
			tags.update({'us_maps_trail_surface_firmness':'paved'})
		if TRAIL_SURFACE == 'S - SOFT':
			tags.update({'us_maps_trail_surface_firmness':'soft'})
		if TRAIL_SURFACE == 'VS - VERY SOFT':
			tags.update({'us_maps_trail_surface_firmness':'very_soft'})
		if 'MINIMUM_TR' in attrs and MINIMUM_TRAIL_WIDTH and MINIMUM_TRAIL_WIDTH != 'N/A':
			tags['us_maps_minimum_trail_width'] = MINIMUM_TRAIL_WIDTH.lower()
		if 'NATIONAL_T' in attrs and NATIONAL_TRAIL_DESIGNATION:
			tags['us_maps_national_trail_designation'] = NATIONAL_TRAIL_DESIGNATION
		if 'ADMIN_ORG' in attrs and ADMIN_ORG:
			tags['us_maps_admin_org'] = ADMIN_ORG
		if 'MANAGING_ORG' in attrs and MANAGING_ORG:
			tags['us_maps_managing_org'] = MANAGING_ORG
		if SPECIAL_MGMT_AREA == 'WSR - WILD':
			tags.update({'us_maps_special_mgmt_area':'wsr_wild'})
		if SPECIAL_MGMT_AREA == 'WSR - SCENIC':
			tags.update({'us_maps_special_mgmt_area':'wsr_scenic'})
		if SPECIAL_MGMT_AREA == 'WSR - RECREATION':
			tags.update({'us_maps_special_mgmt_area':'wsr_recreation'})
		if SPECIAL_MGMT_AREA == 'WSA - WILDERNESS STUDY AREA':
			tags.update({'us_maps_special_mgmt_area':'wsa'})
		if SPECIAL_MGMT_AREA == 'URA - UNROADED AREA':
			tags.update({'us_maps_special_mgmt_area':'ura'})
		if SPECIAL_MGMT_AREA == 'RNA - RESEARCH NATURAL AREA':
			tags.update({'us_maps_special_mgmt_area':'rna'})
		if SPECIAL_MGMT_AREA == 'NRA - NATIONAL RECREATION AREA':
			tags.update({'us_maps_special_mgmt_area':'nra'})
		if SPECIAL_MGMT_AREA == 'NM - NATIONAL MONUMENT':
			tags.update({'us_maps_special_mgmt_area':'nm'})
		if 'TERRA_BASE' in attrs and TERRA_BASE_SYMBOLOGY:
			tags['us_maps_terra_base_symbology'] = TERRA_BASE_SYMBOLOGY.lower()
		if 'MVUM_SYMBO' in attrs and MVUM_SYMBOL:
			tags['us_maps_mvum_symbol'] = MVUM_SYMBOL
		if 'TERRA_MOTO' in attrs and TERRA_MOTORIZED and TERRA_MOTORIZED != 'N/A':
			if TERRA_MOTORIZED == 'N':
				tags.update({'us_maps_terra_motorized':'no'})
			if TERRA_MOTORIZED == 'Y':
				tags.update({'us_maps_terra_motorized':'yes'})
		if 'SNOW_MOTOR' in attrs and SNOW_MOTORIZED and SNOW_MOTORIZED != 'N/A':
			if attrs['SNOW_MOTOR'] == 'N':
				tags.update({'us_maps_snow_motorized':'no'})
			if attrs['SNOW_MOTOR'] == 'Y':
				tags.update({'us_maps_snow_motorized':'yes'})
		if 'WATER_MOTO' in attrs and WATER_MOTORIZED and WATER_MOTORIZED != 'N/A':
			if WATER_MOTORIZED == 'N':
				tags.update({'us_maps_water_motorized':'no'})
			if WATER_MOTORIZED == 'Y':
				tags.update({'us_maps_water_motorized':'yes'})
		if 'ALLOWED_TE' in attrs:
			if '1' in ALLOWED_TERRA_USE:
				tags.update({'foot':'yes'})
			if '2' in ALLOWED_TERRA_USE:
				tags.update({'pack_and_saddle':'yes'})
			if '3' in ALLOWED_TERRA_USE:
				tags.update({'bicycle':'yes'})
			if '4' in ALLOWED_TERRA_USE:
				tags.update({'motorcycle':'yes'})
			if '5' in ALLOWED_TERRA_USE:
				tags.update({'atv':'yes'})
			if '6' in ALLOWED_TERRA_USE:
				tags.update({'4wd':'yes'})
		if 'ALLOWED_SN' in attrs:
			if '1' in ALLOWED_SNOW_USE:
				tags.update({'snowshoe':'yes'})
			if '2' in ALLOWED_SNOW_USE:
				tags.update({'cross_country_ski':'yes'})
			if '3' in ALLOWED_SNOW_USE:
				tags.update({'snowmobile':'yes'})
		if 'HIKER_PEDE' in attrs and HIKER_PEDESTRIAN_MANAGED and HIKER_PEDESTRIAN_MANAGED != 'N/A':
			tags['us_maps_hiker_pedestrian_managed'] = HIKER_PEDESTRIAN_MANAGED
		if 'HIKER_PE_1' in attrs and HIKER_PEDESTRIAN_ACCPT_DISC and HIKER_PEDESTRIAN_ACCPT_DISC != 'N/A':
			tags['us_maps_hiker_pedestrian_accpt_disc'] = HIKER_PEDESTRIAN_ACCPT_DISC
		if 'HIKER_PE_2' in attrs and HIKER_PEDESTRIAN_RESTRICTED and HIKER_PEDESTRIAN_RESTRICTED != 'N/A':
			tags['us_maps_hiker_pedestrian_restricted'] = HIKER_PEDESTRIAN_RESTRICTED
		if 'PACK_SADDL' in attrs and PACK_SADDLE_MANAGED and PACK_SADDLE_MANAGED != 'N/A':
			tags['us_maps_pack_saddle_managed'] = PACK_SADDLE_MANAGED
		if 'PACK_SAD_1' in attrs and PACK_SADDLE_ACCPT_DISC and PACK_SADDLE_ACCPT_DISC != 'N/A':
			tags['us_maps_pack_saddle_accpt_disc'] = PACK_SADDLE_ACCPT_DISC
		if 'PACK_SAD_2' in attrs and PACK_SADDLE_RESTRICTED and PACK_SADDLE_RESTRICTED != 'N/A':
			tags['us_maps_pack_saddle_restricted'] = PACK_SADDLE_RESTRICTED
		if 'BICYCLE_MA' in attrs and BICYCLE_MANAGED and BICYCLE_MANAGED != 'N/A':
			tags['us_maps_bicycle_managed'] = BICYCLE_MANAGED
		if 'BICYCLE_AC' in attrs and BICYCLE_ACCPT_DISC and BICYCLE_ACCPT_DISC != 'N/A':
			tags['us_maps_bicycle_accpt_disc'] = BICYCLE_ACCPT_DISC
		if 'BICYCLE_RE' in attrs and BICYCLE_RESTRICTED and BICYCLE_RESTRICTED != 'N/A':
			tags['us_maps_bicycle_restricted'] = BICYCLE_RESTRICTED
		if 'MOTORCYCLE' in attrs and MOTORCYCLE_MANAGED and MOTORCYCLE_MANAGED != 'N/A':
			tags['us_maps_motorcycle_managed'] = MOTORCYCLE_MANAGED
		if 'MOTORCYC_1' in attrs and MOTORCYCLE_ACCPT_DISC and MOTORCYCLE_ACCPT_DISC != 'N/A':
			tags['us_maps_motorcycle_accpt_disc'] = MOTORCYCLE_ACCPT_DISC
		if 'MOTORCYC_2' in attrs and MOTORCYCLE_RESTRICTED and MOTORCYCLE_RESTRICTED != 'N/A':
			tags['us_maps_motorcycle_restricted'] = MOTORCYCLE_RESTRICTED
		if 'ATV_MANAGE' in attrs and ATV_MANAGED and ATV_MANAGED != 'N/A':
			tags['us_maps_atv_managed'] = ATV_MANAGED
		if 'ATV_ACCPT_' in attrs and ATV_ACCPT_DISC and ATV_ACCPT_DISC != 'N/A':
			tags['us_maps_atv_accpt_disc'] = ATV_ACCPT_DISC
		if 'ATV_RESTRI' in attrs and ATV_RESTRICTED and ATV_RESTRICTED != 'N/A':
			tags['us_maps_atv_restricted'] = ATV_RESTRICTED
		if 'FOURWD_MAN' in attrs and FOURWD_MANAGED and FOURWD_MANAGED != 'N/A':
			tags['us_maps_4wd_managed'] = FOURWD_MANAGED
		if 'FOURWD_ACC' in attrs and FOURWD_ACCPT_DISC and FOURWD_ACCPT_DISC != 'N/A':
			tags['us_maps_4wd_accpt_disc'] = FOURWD_ACCPT_DISC
		if 'FOURWD_RES' in attrs and FOURWD_RESTRICTED and FOURWD_RESTRICTED != 'N/A':
			tags['us_maps_4wd_restricted'] = FOURWD_RESTRICTED
		if 'SNOWMOBILE' in attrs and SNOWMOBILE_MANAGED and SNOWMOBILE_MANAGED != 'N/A':
			tags['us_maps_snowmobile_managed'] = SNOWMOBILE_MANAGED
		if 'SNOWMOBI_1' in attrs and SNOWMOBILE_ACCPT_DISC and SNOWMOBILE_ACCPT_DISC != 'N/A':
			tags['us_maps_snowmobile_accpt_disc'] = SNOWMOBILE_ACCPT_DISC
		if 'SNOWMOBI_2' in attrs and SNOWMOBILE_RESTRICTED and SNOWMOBILE_RESTRICTED != 'N/A':
			tags['us_maps_snowmobile_restricted'] = SNOWMOBILE_RESTRICTED
		if 'SNOWSHOE_M' in attrs and SNOWSHOE_MANAGED and SNOWSHOE_MANAGED != 'N/A':
			tags['us_maps_snowshoe_managed'] = SNOWSHOE_MANAGED
		if 'SNOWSHOE_A' in attrs and SNOWSHOE_ACCPT_DISC and SNOWSHOE_ACCPT_DISC != 'N/A':
			tags['us_maps_snowshoe_accpt_disc'] = SNOWSHOE_ACCPT_DISC
		if 'SNOWSHOE_R' in attrs and SNOWSHOE_RESTRICTED and SNOWSHOE_RESTRICTED != 'N/A':
			tags['us_maps_snowshoe_restricted'] = SNOWSHOE_RESTRICTED
		if 'XCOUNTRY_S' in attrs and XCOUNTRY_SKI_MANAGED and XCOUNTRY_SKI_MANAGED != 'N/A':
			tags['us_maps_xcountry_ski_managed'] = XCOUNTRY_SKI_MANAGED
		if 'XCOUNTRY_1' in attrs and XCOUNTRY_SKI_ACCPT_DISC and XCOUNTRY_SKI_ACCPT_DISC != 'N/A':
			tags['us_maps_xcountry_ski_accpt_disc'] = XCOUNTRY_SKI_ACCPT_DISC
		if 'XCOUNTRY_2' in attrs and XCOUNTRY_SKI_RESTRICTED and XCOUNTRY_SKI_RESTRICTED != 'N/A':
			tags['us_maps_xcountry_ski_restricted'] = XCOUNTRY_SKI_RESTRICTED
		if 'MOTOR_WATE' in attrs and MOTOR_WATERCRAFT_MANAGED and MOTOR_WATERCRAFT_MANAGED != 'N/A':
			tags['us_maps_motor_watercraft_managed'] = MOTOR_WATERCRAFT_MANAGED
		if 'MOTOR_WA_1' in attrs and MOTOR_WATERCRAFT_ACCPT_DISC and MOTOR_WATERCRAFT_ACCPT_DISC != 'N/A':
			tags['us_maps_motor_watercraft_accpt_disc'] = MOTOR_WATERCRAFT_ACCPT_DISC
		if 'MOTOR_WA_2' in attrs and MOTOR_WATERCRAFT_RESTRICTED and MOTOR_WATERCRAFT_RESTRICTED != 'N/A':
			tags['us_maps_motor_watercraft_restricted'] = MOTOR_WATERCRAFT_RESTRICTED
		if 'NONMOTOR_W' in attrs and NONMOTOR_WATERCRAFT_MANAGED and NONMOTOR_WATERCRAFT_MANAGED != 'N/A':
			tags['us_maps_nonmotor_watercraft_managed'] = NONMOTOR_WATERCRAFT_MANAGED
		if 'NONMOTOR_1' in attrs and NONMOTOR_WATERCRAFT_ACCPT_DISC and NONMOTOR_WATERCRAFT_ACCPT_DISC != 'N/A':
			tags['us_maps_nonmotor_watercraft_accpt_disc'] = NONMOTOR_WATERCRAFT_ACCPT_DISC
		if 'NONMOTOR_2' in attrs and NONMOTOR_WATERCRAFT_RESTRICTED and NONMOTOR_WATERCRAFT_RESTRICTED != 'N/A':
			tags['us_maps_nonmotor_watercraft_restricted'] = NONMOTOR_WATERCRAFT_RESTRICTED
		if 'E_BIKE_CLA' in attrs and E_BIKE_CLASS1_MANAGED and E_BIKE_CLASS1_MANAGED != 'N/A':
			tags['us_maps_e_bike_class1_managed'] = E_BIKE_CLASS1_MANAGED
		if 'E_BIKE_C_1' in attrs and E_BIKE_CLASS1_ACCPT and E_BIKE_CLASS1_ACCPT != 'N/A':
			tags['us_maps_e_bike_class1_accpt'] = E_BIKE_CLASS1_ACCPT
		if 'E_BIKE_C_2' in attrs and E_BIKE_CLASS1_DISC and E_BIKE_CLASS1_DISC != 'N/A':
			tags['us_maps_e_bike_class1_disc'] = E_BIKE_CLASS1_DISC
		if 'E_BIKE_C_3' in attrs and E_BIKE_CLASS1_RESTRICTED and E_BIKE_CLASS1_RESTRICTED != 'N/A':
			tags['us_maps_e_bike_class1_restricted'] = E_BIKE_CLASS1_RESTRICTED
		if 'E_BIKE_C_4' in attrs and E_BIKE_CLASS2_MANAGED and E_BIKE_CLASS2_MANAGED != 'N/A':
			tags['us_maps_e_bike_class2_managed'] = E_BIKE_CLASS2_MANAGED
		if 'E_BIKE_C_5' in attrs and E_BIKE_CLASS2_ACCPT and E_BIKE_CLASS2_ACCPT != 'N/A':
			tags['us_maps_e_bike_class2_accpt'] = E_BIKE_CLASS2_ACCPT
		if 'E_BIKE_C_6' in attrs and E_BIKE_CLASS2_DISC and E_BIKE_CLASS2_DISC != 'N/A':
			tags['us_maps_e_bike_class2_disc'] = E_BIKE_CLASS2_DISC
		if 'E_BIKE_C_7' in attrs and E_BIKE_CLASS2_RESTRICTED and E_BIKE_CLASS2_RESTRICTED != 'N/A':
			tags['us_maps_e_bike_class2_restricted'] = E_BIKE_CLASS2_RESTRICTED
		if 'E_BIKE_C_8' in attrs and E_BIKE_CLASS3_MANAGED and E_BIKE_CLASS3_MANAGED != 'N/A':
			tags['us_maps_e_bike_class3_managed'] = E_BIKE_CLASS3_MANAGED
		if 'E_BIKE_C_9' in attrs and E_BIKE_CLASS3_ACCPT and E_BIKE_CLASS3_ACCPT != 'N/A':
			tags['us_maps_e_bike_class3_accpt'] = E_BIKE_CLASS3_ACCPT
		if 'E_BIKE__10' in attrs and E_BIKE_CLASS3_DISC and E_BIKE_CLASS3_DISC != 'N/A':
			tags['us_maps_e_bike_class3_disc'] = E_BIKE_CLASS3_DISC
		if 'E_BIKE__11' in attrs and E_BIKE_CLASS3_RESTRICTED and E_BIKE_CLASS3_RESTRICTED != 'N/A':
			tags['us_maps_e_bike_class3_restricted'] = E_BIKE_CLASS3_RESTRICTED

		tags.update({'us_maps':'trail'})
		tags.update({'nfs':'yes'})
		return tags
