# -*- coding: utf-8 -*-
import ogr2osm
import re

def sanitize_xml_string(s):
    # Removes invalid ASCII symbols (0-31, except \t, \n, \r)
    return re.sub(r'[\x00-\x08\x0B-\x0C\x0E-\x1F]', '', s)

class USNPSTrailsTranslation(ogr2osm.TranslationBase):

	def filter_tags(self, attrs):
		if not attrs:
			return

		tags = {}

		TRLNAME = attrs['TRLNAME']
		MAPLABEL = attrs['MAPLABEL']
		TRLSTATUS = attrs['TRLSTATUS']
		TRLSURFACE = attrs['TRLSURFACE']
		TRLTYPE = attrs['TRLTYPE']
		TRLCLASS = attrs['TRLCLASS']
		TRLUSE = attrs['TRLUSE']
		ACCESSNOTES = attrs['ACCESSNOTES']
		SEASONAL = attrs['SEASONAL']

		if ('MAPLABEL' in attrs and MAPLABEL) or ('TRLNAME' in attrs and TRLNAME):
			if 'MAPLABEL' in attrs and MAPLABEL and MAPLABEL.lower() != 'way':
				name = MAPLABEL.strip().strip()
				if name:
					tags['name'] = sanitize_xml_string(name).lower().title().replace("'S ","'s ")
			elif 'TRLNAME' in attrs and TRLNAME and TRLNAME.lower() != 'trail' and TRLNAME.lower() != 'way':
				name = TRLNAME.strip().strip()
				if name:
					tags['name'] = sanitize_xml_string(name).lower().title().replace("'S ","'s ")
		if ('TRLSTATUS' in attrs and TRLSTATUS):
			match TRLSTATUS:
				case "Decommissioned":
					tags.update({'us_maps_accessibility_status':'decommissioned'})
				case "Exisiting":
					tags.update({'us_maps_accessibility_status':'existing'})
				case "Existing":
					tags.update({'us_maps_accessibility_status':'existing'})
				case "Maintained":
					tags.update({'us_maps_accessibility_status':'maintained'})
				case "Not Applicable":
					tags.update({'us_maps_accessibility_status':'not_applicable'})
				case "Proposed":
					tags.update({'us_maps_accessibility_status':'proposed'})
				case "Temporarily Closed":
					tags.update({'us_maps_accessibility_status':'temporarily_closed'})
				case "Unmaintained":
					tags.update({'us_maps_accessibility_status':'unmaintained'})
		if attrs.get('TRLSURFACE'):
			surface = TRLSURFACE.lower()
			if surface == "aggregate":
				tags.update({'surface': 'aggregate'})
			elif surface == "asphalt":
				tags.update({'surface': 'asphalt'})
			elif surface == "bituminous":
				tags.update({'surface': 'bituminous'})
			elif surface == "brick":
				tags.update({'surface': 'brick'})
			elif surface == "clay":
				tags.update({'surface': 'clay'})
			elif surface == "concrete":
				tags.update({'surface': 'concrete'})
			elif surface == "dirt with gravel":
				tags.update({'surface': 'dirt_with_gravel'})
			elif surface == "earth" or surface == "earth/grass" or surface == "soil":
				tags.update({'surface': 'earth'})
			elif surface == "gravel" or surface == "gravel_road":
				tags.update({'surface': 'gravel'})
			elif surface == "imported compacted material":
				tags.update({'surface': 'imported_compacted_material'})
			elif surface == "imported loose material":
				tags.update({'surface': 'imported_loose_material'})
			elif surface == "lava":
				tags.update({'surface': 'lava'})
			elif surface == "masonry/stone" or surface == "stone":
				tags.update({'surface': 'stone'})
			elif surface == "metal":
				tags.update({'surface': 'metal'})
			elif surface == "native" or surface == "native material":
				tags.update({'surface': 'native_material'})
			elif surface == "other" or surface == "other unpaved":
				tags.update({'surface': 'other'})
			elif surface == "paver":
				tags.update({'surface': 'paver'})
			elif surface == "plastic":
				tags.update({'surface': 'plastic'})
			elif surface == "rubber":
				tags.update({'surface': 'rubber'})
			elif surface == "sand":
				tags.update({'surface': 'sand'})
			elif surface == "snow":
				tags.update({'surface': 'snow'})
			elif surface == "water":
				tags.update({'surface': 'water'})
			elif surface == "wood":
				tags.update({'surface': 'wood'})
			elif surface == "wood chips":
				tags.update({'surface': 'wood_chips'})
		if ('TRLTYPE' in attrs and TRLTYPE):
			match TRLTYPE:
				case "Ferry Route":
					tags.update({'us_maps_trail_type':'ferry_route'})
				case "Park Trail":
					tags.update({'us_maps_trail_type':'park_trail'})
				case "Pedestrian Path":
					tags.update({'us_maps_trail_type':'pedestrian_path'})
				case "Sidewalk":
					tags.update({'us_maps_trail_type':'sidewalk'})
				case "Snow Trail":
					tags.update({'us_maps_trail_type':'snow_trail'})
				case "Standard Terra Trail":
					tags.update({'us_maps_trail_type':'standard_terra_trail'})
				case "Standard/Terra Trail":
					tags.update({'us_maps_trail_type':'standard_terra_trail'})
				case "Steps":
					tags.update({'us_maps_trail_type':'steps'})
				case "Trail":
					tags.update({'us_maps_trail_type':'trail'})
				case "Ferry Route":
					tags.update({'us_maps_trail_type':'ferry_route'})
				case "Water Trail":
					tags.update({'us_maps_trail_type':'water_trail'})

		if attrs.get('TRLCLASS'):
			if TRLCLASS == "Class1" or TRLCLASS == "Class 1: Minimally Developed" or TRLCLASS == "2" or TRLCLASS == "Class2" or TRLCLASS == "Class 2: Minor Development" or TRLCLASS == "Class 2: Moderately Developed":
				tags.update({'us_maps_terra_base_symbology':'tc1-2'})
			if TRLCLASS == "3" or TRLCLASS == "Class3" or TRLCLASS == "Class 3: Developed":
				tags.update({'us_maps_terra_base_symbology':'tc3'})
			if TRLCLASS == "4" or TRLCLASS == "Class4" or TRLCLASS == "Class 4: Highly Developed" or TRLCLASS == "5" or TRLCLASS == "Class5" or TRLCLASS == "Class 5: Fully Developed":
				tags.update({'us_maps_terra_base_symbology':'tc4-5'})
			if TRLCLASS == "6":
				tags.update({'us_maps_terra_base_symbology':'tc6'})

		if 'TRLUSE' in attrs:
			if 'hike' in TRLUSE.lower() or 'hiking' in TRLUSE.lower() or 'walking' in TRLUSE.lower() or 'pedestrian' in TRLUSE.lower() or 'foot' in TRLUSE.lower():
				tags.update({'foot':'yes'})
			if 'pack and saddle' in TRLUSE.lower() or 'packorsaddle' in TRLUSE.lower():
				tags.update({'pack_and_saddle':'yes'})
			if 'horse' in TRLUSE.lower() or 'equestrian' in TRLUSE.lower():
				tags.update({'horse':'yes'})
			if 'bicycle' in TRLUSE.lower() or 'bike' in TRLUSE.lower():
				tags.update({'bicycle':'yes'})
			if 'motorcycle' in TRLUSE.lower():
				tags.update({'motorcycle':'yes'})
			if 'snowshoe' in TRLUSE.lower() :
				tags.update({'snowshoe':'yes'})
			if 'snowmobile' in TRLUSE.lower():
				tags.update({'snowmobile':'yes'})
			if 'sled' in TRLUSE.lower():
				tags.update({'sled':'yes'})
			if 'dog sled' in TRLUSE.lower():
				tags.update({'dog_sled':'yes'})
			if 'atv' in TRLUSE.lower() or 'all-terrain vehicle' in TRLUSE.lower():
				tags.update({'atv':'yes'})
			if 'cross-country ski' in TRLUSE.lower() or 'cross country skiing' in TRLUSE.lower():
				tags.update({'cross_country_ski':'yes'})
			if 'motorized' in TRLUSE.lower():
				tags.update({'us_maps_terra_motorized':'yes'})
			if 'non-motorized' in TRLUSE.lower():
				tags.update({'us_maps_terra_motorized':'no'})
			if 'motorized watercraft' in TRLUSE.lower():
				tags.update({'us_maps_water_motorized':'yes'})
			if 'non-motorized watercraft' in TRLUSE.lower():
				tags.update({'us_maps_water_motorized':'no'})
			if 'wheelchair accessible trail' in TRLUSE.lower():
				tags.update({'wheelchair':'yes'})

		if ('ACCESSNOTES' in attrs and ACCESSNOTES):
			tags['access_notes'] = ACCESSNOTES

		if ('SEASONAL' in attrs and SEASONAL.lower() == 'yes'):
			tags.update({'seasonal':'yes'})

		if ('SEASONAL' in attrs and SEASONAL.lower() == 'no'):
			tags.update({'seasonal':'no'})


		tags.update({'us_maps':'trail'})
		tags.update({'nps':'yes'})
		return tags
