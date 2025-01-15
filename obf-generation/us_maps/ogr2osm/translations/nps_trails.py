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
					tags['name'] = sanitize_xml_string(name)
			elif 'TRLNAME' in attrs and TRLNAME and TRLNAME.lower() != 'trail' and TRLNAME.lower() != 'way':
				name = TRLNAME.strip().strip()
				if name:
					tags['name'] = sanitize_xml_string(name)
		if ('TRLSTATUS' in attrs and TRLSTATUS):
			match TRLSTATUS:
				case "Decommissioned":
					tags.update({'us_maps_trail_accessibility_status':'decommissioned'})
				case "Exisiting" | "Existing":
					tags.update({'us_maps_trail_accessibility_status':'existing'})
				case "Maintained":
					tags.update({'us_maps_trail_accessibility_status':'maintained'})
				case "Not Applicable":
					tags.update({'us_maps_trail_accessibility_status':'not_applicable'})
				case "Proposed":
					tags.update({'us_maps_trail_accessibility_status':'proposed'})
				case "Temporarily Closed":
					tags.update({'us_maps_trail_accessibility_status':'temporarily_closed'})
				case "Unmaintained":
					tags.update({'us_maps_trail_accessibility_status':'unmaintained'})
		if ('TRLSURFACE' in attrs and TRLSURFACE):
			surface = TRLSURFACE.lower()
			match surface:
				case "aggregate":
					tags.update({'surface':'aggregate'})
				case "asphalt":
					tags.update({'surface':'asphalt'})
				case "bituminous":
					tags.update({'surface':'bituminous'})
				case "brick":
					tags.update({'surface':'brick'})
				case "clay":
					tags.update({'surface':'clay'})
				case "concrete":
					tags.update({'surface':'concrete'})
				case "dirt with gravel":
					tags.update({'surface':'dirt_with_gravel'})
				case "earth" | "soil":
					tags.update({'surface':'earth'})
				case "earth/grass":
					tags.update({'surface':'earth_grass'})
				case "gravel" | "gravel road":
					tags.update({'surface':'gravel'})
				case "imported compacted material":
					tags.update({'surface':'imported_compacted_material'})
				case "imported loose material":
					tags.update({'surface':'imported_loose_material'})
				case "lava":
					tags.update({'surface':'lava'})
				case "masonry/stone" | "stone":
					tags.update({'surface':'stone'})
				case "metal":
					tags.update({'surface':'metal'})
				case "native" | "native material":
					tags.update({'surface':'native_material'})
				case "other" | "other unpaved":
					tags.update({'surface':'other'})
				case "paver":
					tags.update({'surface':'paver'})
				case "plastic":
					tags.update({'surface':'plastic'})
				case "rubber":
					tags.update({'surface':'rubber'})
				case "sand":
					tags.update({'surface':'sand'})
				case "snow":
					tags.update({'surface':'snow'})
				case "water":
					tags.update({'surface':'water'})
				case "wood":
					tags.update({'surface':'wood'})
				case "wood chips":
					tags.update({'surface':'wood_chips'})
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
				case "Standard Terra Trail" | "Standard/Terra Trail":
					tags.update({'us_maps_trail_type':'standard_terra_trail'})
				case "Steps":
					tags.update({'us_maps_trail_type':'steps'})
				case "Trail":
					tags.update({'us_maps_trail_type':'trail'})
				case "Ferry Route":
					tags.update({'us_maps_trail_type':'ferry_route'})
				case "Water Trail":
					tags.update({'us_maps_trail_type':'water_trail'})

		if ('TRLCLASS' in attrs and TRLCLASS):
			match TRLCLASS:
				case "Class1" | "Class 1: Minimally Developed" | "2" | "Class2" | "Class 2: Minor Development" | "Class 2: Moderately Developed":
					tags.update({'us_maps_terra_base_symbology':'tc1-2'})
				case "3" | "Class3" | "Class 3: Developed":
					tags.update({'us_maps_terra_base_symbology':'tc3'})
				case "4" | "Class4" | "Class 4: Highly Developed" | "5" | "Class5" | "Class 5: Fully Developed":
					tags.update({'us_maps_terra_base_symbology':'tc4-5'})
				case "6":
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

		if ('SEASONAL' in attrs and SEASONAL.lower() == 'no'):
			tags.update({'seasonal':'no'})


		tags.update({'us_maps':'trail'})
		tags.update({'nps':'yes'})
		return tags
