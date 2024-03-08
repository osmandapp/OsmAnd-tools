# -*- coding: utf-8 -*-
import ogr2osm

class PublicLandsTranslation(ogr2osm.TranslationBase):

	def filter_tags(self, attrs):
		if not attrs:
			return

		tags = {}
		owner = attrs['owner'].strip().title()

		if 'owner' in attrs and owner:
			tags['name'] = owner

		tags.update({'private_land':'area'})
		return tags
