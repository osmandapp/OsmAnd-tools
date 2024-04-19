# -*- coding: utf-8 -*-
import ogr2osm

class PrivateLandsTranslation(ogr2osm.TranslationBase):

	def filter_tags(self, attrs):
		if not attrs:
			return

		tags = {}

		if 'PARCELID' in attrs and attrs['PARCELID'].startswith('WATER_'): # Wisconsin
			return
		if 'OWN_TYPE' in attrs and (attrs['OWN_TYPE'].lower() == "state" or attrs['OWN_TYPE'].lower() == "federal"): # Utah
			return
		if 'PROPTYPE' in attrs and (attrs['PROPTYPE'].lower() == "row_road" or attrs['PROPTYPE'].lower() == "water"): # Vermont
			return

#		if 'OWNERNME1' in attrs and attrs['OWNERNME1'].startsWith('USA ') or 'U S A' in attrs['OWNERNME1'] or attrs['OWNERNME1'].startswith('US ') or attrs['OWNERNME1'].startswith('U.S.') or ' STATE' in attrs['OWNERNME1'] or attrs['OWNERNME1'].startswith('USD') or attrs['OWNERNME1'] == 'STATE' or attrs['OWNERNME1'].startsWith('STATE ') or 'COUNTY' in attrs['OWNERNME1'] or 'WISCONSIN' in attrs['OWNERNME1'] or 'NATIONAL' in attrs['OWNERNME1'] or 'BRYLE RIVER LLC' in attrs['OWNERNME1'] or 'CONSERVANCY' in attrs['OWNERNME1'] or 'GOODMAN FOREST' in attrs['OWNERNME1'] or 'DEPT OF' in attrs['OWNERNME1'] or 'DNR' in attrs['OWNERNME1'] or 'NAT RESOURCES' in attrs['OWNERNME1'] or attrs['OWNERNME1'].startswith('CITY OF '): #'WIS-MICH POWER' in attrs['OWNERNME1'] or 'BLACK CREEK SOD' in attrs['OWNERNME1'] or attrs['OWNERNME1'].startswith('WOLF RIVER APT') or 'TIMBERLAND' in attrs['OWNERNME1'] or 'SAND VALLEY RESTORATION' in attrs['OWNERNME1'] or 'COOPERATIVE FORESTRY DIV' in attrs['OWNERNME1']  # Wisconsin
#			return

		if 'owner' in attrs and attrs['owner'].lower() != "null":
			tags['name'] = attrs['owner'].strip().title()
		if 'OwnerName' in attrs:
			if attrs['OwnerName'] == "" and 'FILE_NAME' in attrs and attrs['FILE_NAME'] == 'a00000028_fixed': # Montana
				print("null OwnerName")
				tags.update({'private_null':'area'})
				return
			if  attrs['OwnerName'].lower() != "":
				tags['name'] = attrs['OwnerName'].strip().title()
		if 'OWNERNME1' in attrs and attrs['OWNERNME1'].lower() != "null" and 'OWNERSHIP INFORMATION' not in attrs['OWNERNME1']:
			tags['name'] = attrs['OWNERNME1'].strip().title()
		if 'OWNER1' in attrs and attrs['OWNER1'].lower() != "null":
			tags['name'] = attrs['OWNER1'].strip().title()
		if 'PSTLADRESS' in attrs and attrs['PSTLADRESS']:
			tags['us_private_land_full_mailing_address'] = attrs['PSTLADRESS'].strip().title()
		if 'OWN_TYPE' in attrs and attrs['OWN_TYPE']:
			tags['own_type'] = attrs['OWN_TYPE'].strip().lower()

		tags.update({'private_land':'area'})
		return tags
