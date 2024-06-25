# -*- coding: utf-8 -*-
import ogr2osm
import string

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
		if 'POLY_TYPE' in attrs and (attrs['POLY_TYPE'].lower() == "row" or attrs['POLY_TYPE'].lower() == "water" or attrs['POLY_TYPE'].lower() == "PRIV_ROW" or attrs['POLY_TYPE'].lower() == "RAIL_ROW"): # Massachusets
			return
		if 'DESCRIPT' in attrs and (attrs['DESCRIPT'] == "RESIDENTIAL COMMON ELEMENTS, AREAS" or attrs['DESCRIPT'] == "OTHER STATE" or attrs['DESCRIPT'] == "CENTRALLY ASSESSED" or attrs['DESCRIPT'] == "ACREAGE NOT ZONED FOR AGRICULTURAL" or attrs['DESCRIPT'] == "PARCELS WITH NO VALUES (RAIL)" or attrs['DESCRIPT'] == "OTHER COUNTIES" or attrs['DESCRIPT'] == "VACANT COMMERCIAL" or attrs['DESCRIPT'] == "OTHER MUNICIPAL" or attrs['DESCRIPT'] == "RIGHTS-OF-WAY STREETS, ROADS, AND CANALS" or attrs['DESCRIPT'] == "SEWAGE DISPOSAL, BORROW PITS, AND WETLANDS" or attrs['DESCRIPT'] == "RIVERS, LAKES, AND SUBMERGED LANDS" or attrs['DESCRIPT'] == "UTILITIES"): # Florida
			return

#		if 'OWNERNME1' in attrs and attrs['OWNERNME1'].startsWith('USA ') or 'U S A' in attrs['OWNERNME1'] or attrs['OWNERNME1'].startswith('US ') or attrs['OWNERNME1'].startswith('U.S.') or ' STATE' in attrs['OWNERNME1'] or attrs['OWNERNME1'].startswith('USD') or attrs['OWNERNME1'] == 'STATE' or attrs['OWNERNME1'].startsWith('STATE ') or 'COUNTY' in attrs['OWNERNME1'] or 'WISCONSIN' in attrs['OWNERNME1'] or 'NATIONAL' in attrs['OWNERNME1'] or 'BRYLE RIVER LLC' in attrs['OWNERNME1'] or 'CONSERVANCY' in attrs['OWNERNME1'] or 'GOODMAN FOREST' in attrs['OWNERNME1'] or 'DEPT OF' in attrs['OWNERNME1'] or 'DNR' in attrs['OWNERNME1'] or 'NAT RESOURCES' in attrs['OWNERNME1'] or attrs['OWNERNME1'].startswith('CITY OF '): #'WIS-MICH POWER' in attrs['OWNERNME1'] or 'BLACK CREEK SOD' in attrs['OWNERNME1'] or attrs['OWNERNME1'].startswith('WOLF RIVER APT') or 'TIMBERLAND' in attrs['OWNERNME1'] or 'SAND VALLEY RESTORATION' in attrs['OWNERNME1'] or 'COOPERATIVE FORESTRY DIV' in attrs['OWNERNME1']  # Wisconsin
#			return

		if 'owner' in attrs and attrs['owner'].lower() != "null" and attrs['owner']:
			tags['name'] = attrs['owner'].strip().title()
		if 'Owner' in attrs and attrs['Owner'].lower() != "null" and attrs['Owner']:
			tags['name'] = attrs['Owner'].strip().title()
		if 'OWNER' in attrs and attrs['OWNER'].lower() != "null" and attrs['OWNER']:
			tags['name'] = attrs['OWNER'].strip().title()
		if 'ONAME' in attrs and attrs['ONAME'].lower() != "null" and attrs['ONAME']:
			tags['name'] = attrs['ONAME'].strip().title()
		if 'Owner_Name' in attrs and attrs['Owner_Name'].lower() != "null" and attrs['Owner_Name']:
			tags['name'] = attrs['Owner_Name'].strip().title()
		if 'ownname' in attrs and attrs['ownname'].lower() != "null" and attrs['ownname']:
			pre_name = ''.join(filter(lambda x: x in string.printable, attrs['ownname'].strip().title()))
			tags['name'] = pre_name
		if 'FirstOwner' in attrs and attrs['FirstOwner'].lower() != "null" and attrs['FirstOwner']:
			tags['name'] = attrs['FirstOwner'].strip().title()
		if 'OwnerName' in attrs and attrs['OwnerName'].lower() != "null" and attrs['OwnerName']:
			if attrs['OwnerName'] == "" and 'FILE_NAME' in attrs and attrs['FILE_NAME'] == 'a00000028_fixed': # Montana
				print("null OwnerName")
				tags.update({'private_null':'area'})
				return
			if attrs['OwnerName'].lower() != "":
				tags['name'] = attrs['OwnerName'].strip().title()
		if 'OWNERNME1' in attrs and attrs['OWNERNME1'].lower() != "null" and 'OWNERSHIP INFORMATION' not in attrs['OWNERNME1'] and attrs['OWNERNME1']:
			tags['name'] = attrs['OWNERNME1'].strip().title()
		if 'OWNER1' in attrs and attrs['OWNER1'].lower() != "null" and attrs['OWNER1']:
			tags['name'] = attrs['OWNER1'].strip().title()
		if 'NAMEKEY' in attrs and attrs['NAMEKEY'].lower() != "null" and attrs['NAMEKEY']:
			tags['name'] = attrs['NAMEKEY'].strip().title()
		if 'NAME1' in attrs and attrs['NAME1'].lower() != "null" and attrs['NAME1']:
			tags['name'] = attrs['NAME1'].strip().title()
		if 'PSTLADRESS' in attrs and attrs['PSTLADRESS']:
			tags['us_private_land_full_mailing_address'] = attrs['PSTLADRESS'].strip().title()
		if 'OWN_TYPE' in attrs and attrs['OWN_TYPE']:
			tags['own_type'] = attrs['OWN_TYPE'].strip().lower()

		tags.update({'private_land':'area'})
		return tags
