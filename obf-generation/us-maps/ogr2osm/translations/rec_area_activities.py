# -*- coding: utf-8 -*-
import ogr2osm
from io import StringIO
from html.parser import HTMLParser
class MLStripper(HTMLParser):
	def __init__(self):
		super().__init__()
		self.reset()
		self.strict = False
		self.convert_charrefs= True
		self.text = StringIO()
	def handle_data(self, d):
		self.text.write(d)
	def get_data(self):
		return self.text.getvalue()

def strip_tags(text):
	s = MLStripper()
	s.feed(text)
	return s.get_data()

class USFSRecreationAreaActivitiesTranslation(ogr2osm.TranslationBase):

	def filter_tags(self, attrs):
		if not attrs:
			return

		tags = {}

		RECAREANAME = attrs['RECAREANAME'].strip() # Name of the Rec Area.
		RECAREAURL = attrs['RECAREAURL'].strip() # Hyperlink to Forest Service world wide web recreation portal web page.
		FEEDESCRIPTION = attrs['FEEDESCRIPTION'].strip() # Fees charged to the public to access or use this area.
		OPEN_SEASON_START = attrs['OPEN_SEASON_START'].strip() #
		OPEN_SEASON_END = attrs['OPEN_SEASON_END'].strip() #
		OPERATIONAL_HOURS = attrs['OPERATIONAL_HOURS'].strip() #
		FORESTNAME = attrs['FORESTNAME'].strip() #
		RESERVATION_INFO = attrs['RESERVATION_INFO'].strip() #
		MARKERACTIVITY = attrs['MARKERACTIVITY'].lower().strip().replace(" - ","_").replace(" ","_").replace("/","_").replace("-","_").replace("&","and") # The MarkerType chosen to represent an area corresponds to this activity.
		MARKERACTIVITYGROUP = attrs['MARKERACTIVITYGROUP'].strip() # Activities in the Portal are grouped into activity groups. This is the activity group for the activity that corresponds to the chose marker.
		RECAREADESCRIPTION = attrs['RECAREADESCRIPTION'].strip() # Description of the Rec Area.
		RESTRICTIONS = attrs['RESTRICTIONS'].strip() # Limitations and restrictions affecting the public's ability to access and use this area.
		ACTIVITYDESCRIPTION = attrs['ACTIVITYDESCRIPTION'].strip() #
		PARENTACTIVITYNAME = attrs['PARENTACTIVITYNAME'].strip() # The Parent activity name.
		ACTIVITYNAME = attrs['ACTIVITYNAME'] # Name of the activity.
		ACCESSIBILITY = attrs['ACCESSIBILITY'].strip() #
		OPENSTATUS = attrs['OPENSTATUS'].replace("none","").replace("not cleared","").replace("unknown","")

		if 'RECAREANAME' in attrs and RECAREANAME:
			tags['nfs_recreation_area_name'] = RECAREANAME
		if 'MARKERACTIVITY' in attrs and MARKERACTIVITY:
			MARKERACTIVITYARRAY = MARKERACTIVITY.split(';')
			tags['nfs_recreation_area_type'] = MARKERACTIVITYARRAY[0].strip()
# 			if len(MARKERACTIVITYARRAY) == 2:
# 				tags['nfs_recreation_area_type_2'] = MARKERACTIVITYARRAY[1]
# 			if len(MARKERACTIVITYARRAY) > 2:
# 				tags['nfs_recreation_area_type_2'] = MARKERACTIVITYARRAY[1]
# 				tags['nfs_recreation_area_type_3'] = MARKERACTIVITYARRAY[2]
		if 'RECAREAURL' in attrs and RECAREAURL:
			tags['nfs_recreation_area_url'] = RECAREAURL
		if 'FEEDESCRIPTION' in attrs and strip_tags(FEEDESCRIPTION):
			tags['nfs_recreation_area_fee_description'] = strip_tags(FEEDESCRIPTION)
		if 'OPEN_SEASON_START' in attrs and OPEN_SEASON_START:
			tags['nfs_recreation_area_open_season_start'] = OPEN_SEASON_START
		if 'OPEN_SEASON_END' in attrs and OPEN_SEASON_END:
			tags['nfs_recreation_area_open_season_end'] = OPEN_SEASON_END
		if 'OPERATIONAL_HOURS' in attrs and strip_tags(OPERATIONAL_HOURS):
			tags['nfs_recreation_area_operational_hours'] = strip_tags(OPERATIONAL_HOURS)
		if 'FORESTNAME' in attrs and FORESTNAME:
			tags['nfs_recreation_area_forest_name'] = FORESTNAME
		if 'RESERVATION_INFO' in attrs and strip_tags(RESERVATION_INFO):
			tags['nfs_recreation_area_reservation_info'] = strip_tags(RESERVATION_INFO)
		if 'MARKERACTIVITYGROUP' in attrs and MARKERACTIVITYGROUP:
			tags['nfs_recreation_area_marker_activity_group'] = MARKERACTIVITYGROUP.strip().lower().replace(" ;",";").replace(" - ","_").replace(" ","_").replace("/","_").replace("-","_").replace("&","and")
		if 'RECAREADESCRIPTION' in attrs and strip_tags(RECAREADESCRIPTION):
			tags['nfs_recreation_area_description'] = strip_tags(RECAREADESCRIPTION)
		if 'RESTRICTIONS' in attrs and strip_tags(RESTRICTIONS):
			tags['nfs_recreation_area_restrictions'] = strip_tags(RESTRICTIONS)
		if 'ACTIVITYDESCRIPTION' in attrs and ACTIVITYDESCRIPTION:
			tags['nfs_recreation_area_activity_description'] = strip_tags(ACTIVITYDESCRIPTION)
		if 'PARENTACTIVITYNAME' in attrs and PARENTACTIVITYNAME:
			tags['nfs_recreation_area_parent_activity_name'] = PARENTACTIVITYNAME
		if 'ACTIVITYNAME' in attrs and ACTIVITYNAME:
			ACTIVITYNAMEARRAY = ACTIVITYNAME.strip().lower().replace(" ;",";").replace(" - ","_").replace(" ","_").replace("/","_").replace("-","_").replace("&","and").split(';')
			for activity in ACTIVITYNAMEARRAY:
				if activity:
					tags['nfs_recreation_area_activity_'+activity] = 'yes'
		if 'ACCESSIBILITY' in attrs and strip_tags(ACCESSIBILITY):
			tags['nfs_recreation_area_accessibility_info'] = strip_tags(ACCESSIBILITY)
		if 'OPENSTATUS' in attrs and OPENSTATUS:
			OPENSTATUSARRAY = OPENSTATUS.lower().strip().replace(" - ","_").replace(" ","_").replace("/","_").replace("-","_").split(';')
			tags['nfs_recreation_area_open_status'] = OPENSTATUSARRAY[0]
			if len(OPENSTATUSARRAY) > 1:
				tags['nfs_recreation_area_open_status_2'] = OPENSTATUSARRAY[1]

		tags.update({'nfs':'recreation_area'})
		return tags
