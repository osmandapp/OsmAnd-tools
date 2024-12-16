# -*- coding: utf-8 -*-
import ogr2osm
import string

class OpenAddressesTranslation(ogr2osm.TranslationBase):

	def filter_tags(self, attrs):
		if not attrs:
			return

		tags = {}

		number = attrs['number']
		street = attrs['street']
		unit = attrs['unit']
		city = attrs['city']
		postcode = attrs['postcode']
		if 'number' in attrs and number:
			if number == "0":
				return
			tags['addr:housenumber'] = number
		if 'street' in attrs and street:
			tags['addr:street'] = street.strip().lower().title()
		if 'city' in attrs and city:
			tags['addr:city'] = city.strip().lower().title()
		if 'postcode' in attrs and postcode:
			tags['addr:postcode'] = postcode

		return tags
