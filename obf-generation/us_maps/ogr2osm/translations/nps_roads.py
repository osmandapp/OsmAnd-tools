# -*- coding: utf-8 -*-
import ogr2osm
import re

def sanitize_xml_string(s):
    # Removes invalid ASCII symbols (0-31, except \t, \n, \r)
    return re.sub(r'[\x00-\x08\x0B-\x0C\x0E-\x1F]', '', s)

class USNPSRoadsTranslation(ogr2osm.TranslationBase):

	def filter_tags(self, attrs):
		if not attrs:
			return

		tags = {}

		RDNAME = attrs['RDNAME']
		MAPLABEL = attrs['MAPLABEL']
		RDSTATUS = attrs['RDSTATUS']
		RDCLASS = attrs['RDCLASS']
		RDSURFACE = attrs['RDSURFACE']
		RDLANES = attrs['RDLANES']
		SEASONAL = attrs['SEASONAL']
		SEASDESC = attrs['SEASDESC']
		RDHICLEAR = attrs['RDHICLEAR']

		if ('MAPLABEL' in attrs and MAPLABEL) or ('RDNAME' in attrs and RDNAME):
			if 'MAPLABEL' in attrs and MAPLABEL and MAPLABEL.lower() != 'way':
				name = MAPLABEL.strip().strip()
				if name:
					tags['name'] = sanitize_xml_string(name).lower().title().replace("'S ","'s ")
			elif 'RDNAME' in attrs and RDNAME and RDNAME.lower() != 'trail' and RDNAME.lower() != 'way':
				name = RDNAME.strip().strip()
				if name:
					tags['name'] = sanitize_xml_string(name).lower().title().replace("'S ","'s ")

		if ('RDSTATUS' in attrs and RDSTATUS):
			match RDSTATUS:
				case "Decommissioned":
					tags.update({'us_maps_accessibility_status':'decommissioned'})
				case "Exisiting":
					tags.update({'us_maps_accessibility_status':'existing'})
				case "Existing":
					tags.update({'us_maps_accessibility_status':'existing'})
				case "Esisting":
					tags.update({'us_maps_accessibility_status':'existing'})
				case "Planned":
					tags.update({'us_maps_accessibility_status':'planned'})
				case "Temporarily Closed":
					tags.update({'us_maps_accessibility_status':'temporarily_closed'})
		if ('RDCLASS' in attrs and RDCLASS):
			road_class = RDCLASS.lower()
			match road_class:
				case "primary":
					tags.update({'us_maps_road_functional_class':'arterial'})
				case "secondary":
					tags.update({'us_maps_road_functional_class':'collector'})
				case "local":
					tags.update({'us_maps_road_functional_class':'local_important'})
				case "service":
					tags.update({'us_maps_road_functional_class':'local_important'})
				case "service 4x4":
					tags.update({'us_maps_road_functional_class':'local'})
				case "4wd":
					tags.update({'us_maps_road_functional_class':'local'})
				case "parking lot road":
					tags.update({'us_maps_road_functional_class':'local'})

		if ('RDSURFACE' in attrs and RDSURFACE):
			surface = RDSURFACE.lower()
			match surface:
				case "asphalt":
					tags.update({'surface':'asphalt'})
				case "cinders":
					tags.update({'surface':'cinders'})
				case "cobblestone":
					tags.update({'surface':'cobblestone'})
				case "concrete":
					tags.update({'surface':'concrete'})
				case "earth":
					tags.update({'surface':'earth'})
				case "soil":
					tags.update({'surface':'earth'})
				case "native / dirt":
					tags.update({'surface':'earth'})
				case "native or dirt":
					tags.update({'surface':'earth'})
				case "gravel":
					tags.update({'surface':'gravel'})
				case "native":
					tags.update({'surface':'native_material'})
				case "other":
					tags.update({'surface':'other'})
				case "paved":
					tags.update({'surface':'paved'})
				case "other paved":
					tags.update({'surface':'paved'})
				case "unpaved":
					tags.update({'surface':'other_unpaved'})
				case "other unpaved":
					tags.update({'surface':'other_unpaved'})
				case "sand":
					tags.update({'surface':'sand'})
					
		if ('RDLANES' in attrs and RDLANES):
			tags['lanes'] = RDLANES
		if ('SEASONAL' in attrs and SEASONAL.lower() == "yes"):
			tags.update({'seasonal':'yes'})
		if ('SEASONAL' in attrs and SEASONAL.lower() == "no"):
			tags.update({'seasonal':'no'})

		if 'SEASDESC' in attrs and SEASDESC:
			tags['seasonal_description'] = SEASDESC.strip().strip()

		if ('RDHICLEAR' in attrs and RDHICLEAR.lower() == "yes"):
			tags.update({'4wd':'yes'})
		if ('RDHICLEAR' in attrs and RDHICLEAR.lower() == "no"):
			tags.update({'4wd':'no'})

		tags.update({'us_maps':'road'})
		tags.update({'nps':'yes'})
		return tags
