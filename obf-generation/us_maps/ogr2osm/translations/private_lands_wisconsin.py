# -*- coding: utf-8 -*-
import ogr2osm

class PublicLandsTranslation(ogr2osm.TranslationBase):

	def filter_tags(self, attrs):
		if not attrs:
			return

		tags = {}
		full_mailing_address = attrs['PSTLADRESS'].strip().title()
		primary_owner_name = attrs['OWNERNME1'].strip().title()

		if 'OWNERNME1' in attrs and primary_owner_name:
			tags['name'] = primary_owner_name
		if 'PSTLADRESS' in attrs and full_mailing_address:
			tags['us_private_land_full_mailing_address'] = full_mailing_address

		tags.update({'private_land':'area'})
		return tags
