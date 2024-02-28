# -*- coding: utf-8 -*-
import ogr2osm

class USFSRecreationSitesTranslation(ogr2osm.TranslationBase):

	def filter_tags(self, attrs):
		if not attrs:
			return

		tags = {}

		MANAGING_ORG = attrs['MANAGING_O']
		SITE_ID = attrs['SITE_ID'] # sub-site not unique ID - local identifier
		SITE_NAME = attrs['SITE_NAME'] # sub-site local identifying unit name
		SITE_SUBTYPE = attrs['SITE_SUBTY'] # sub-site sub type of unit
		DEVELOPMENT_SCALE = attrs['DEVELOPMEN'] # The developed recreation development scale
		DEVELOPMENT_STATUS = attrs['DEVELOPM_1'] # The existence status of the recreation site.
		POSS_ID = attrs['POSS_ID'] # the USFS Point of sale system identifier
		NRRS_ID = attrs['NRRS_ID'] # Recreation.gov ID
		TOTAL_CAPACITY = attrs['TOTAL_CAPA'] # Total persons at one time (PAOT) capacity of recreation site
		FEE_CHARGED = attrs['FEE_CHARGE'] # indicates whether fee is charged
		FEE_TYPE = attrs['FEE_TYPE'] # The type of fee charged at the site
		RECAREA_ENABLE = attrs['RECAREA_EN'] # Yes/No flag that designates whether the area is published on the public website
		RECAREA_DESCRIPTION = attrs['RECAREA_DE'] # Description that appears on the public website
		RECAREA_STATUS = attrs['RECAREA_ST'] # Usability of the site with a list of set conditions
		PARENT_RECAREA = attrs['PARENT_REC'] # the parent recreation area in the hierarchy of the dtabase
		INFORMATION_CONTACT STATION = attrs['INFORMATIO'] # INFORMATION/CONTACT STATION IS A SERVICE GENERALLY PROVIDED AT THIS SITE
		CURRENT_CONDITIONS = attrs['CURRENT_CO'] # Current conditions of the site relating to its usability and accessibility to the public
		OFFICIAL_DESIGNATION = attrs['OFFICIAL_D'] # A set of keywords to ensure that the public can find the site in a web search
		FEE_DESCRIPTION = attrs['FEE_DESCRI'] # Explanation of user fees
		OPERATIONAL_HOURS = attrs['OPERATIONA'] # Time of day when the area is open to the public
		OPEN_SEASON = attrs['OPEN_SEASO'] # Months of the year when the recreation area is open to the public.
		BEST_SEASON = attrs['BEST_SEASO'] # Months most suitable for recreational use.
		BUSIEST_SEASON = attrs['BUSIEST_SE'] # Time of year when the area has the most visitors.
		USAGE_LEVEL = attrs['USAGE_LEVE'] # Rating for the amount of use the area has during the open season.
		PACK_IN_OUT = attrs['PACK_IN_OU'] # Indicates whether visitor must pack out trash (yes = pack out trash)
		PUBLIC_SITE_NAME = attrs['PUBLIC_SIT'] # Name which the site is commonly known by
		ALIAS_NAME = attrs['ALIAS_NAME'] # Another name for the site
		ALTERNATIVE_NAME = attrs['ALTERNATIV'] # Another name for the site
		SITE_ADDRESS = attrs['SITE_ADDRE'] # Address of the sub-site for recreation
		SITE_DIRECTIONS = attrs['SITE_DIREC'] # Information about how to reach the recreation area
		SITE_PHONE_NBR = attrs['SITE_PHONE'] # site's phone number
		SITE_EMAIL_ADDRESS = attrs['SITE_EMAIL'] # site's email address
		REC1STOP_URL = attrs['REC1STOP_U'] # Recreation.gov URL
		USDA_PORTAL_URL = attrs['USDA_PORTA'] # USFS website URL
		IMPORTANT_INFO = attrs['IMPORTANT_'] # Significant information about contacts.
		SITE_CONTACT_NOTES = attrs['SITE_CONTA'] # sub-site employee or general contact information
		RENTALS_AND_GUIDES = attrs['RENTALS_AN'] # List of available rentals (campsites, cabins, rooms, etc) and guides, including outfitters
		PASSES = attrs['PASSES'] # List of required passes that are required to access or use the area
		PERMIT_INFORMATION = attrs['PERMIT_INF'] # List of required permits and the conditions under which the permits are needed
		RESTRICTIONS = attrs['RESTRICTIO'] # Any restrictions for the area, such as no pets
		WATER_AVAILABILITY = attrs['WATER_AVAI'] # Description of public water availability-often the most important information about a site. The information appears in an At-a-Glance table with suggested values of None, Drinking Water, Not for Drinking.
		RESTROOM_AVAILABILITY = attrs['RESTROOM_A'] # Description of available rest rooms, possibly including type, accessibility, and location. The information appears in an At-a-Glance table with suggested values of Pit, vault, or Flush.
		OPERATED_BY = attrs['OPERATED_B'] # Information about whether the site is operated by the Forest Service or a concessionaire, include name and contact information.
		SEASON_DESCRIPTION = attrs['SEASON_DES'] # Description of the open season: weather, general activities, and possible special events
		DIRECTIONS = attrs['DIRECTIONS'] # Information about how to reach the recreation area
		MAXIMUM_ELEVATION = attrs['MAXIMUM_EL'] # Highest elevation in the area
		MINIMUM_ELEVATION = attrs['MINIMUM_EL'] # Lowest elevation in the area
		ACTIVITY_TYPE = attrs['ACTIVITY_T'] # The type of activity available
		ACTIVITY_TYPES = attrs['ACTIVITY_2'] # The total types of activities found at the recreation location
		ALERTS_DESCRIPTION = attrs['ALERTS_DES'] # The description of the alert
		SITE_OPENING_START_DATE = attrs['SITE_OPENI'] # The opening date of the site
		SITE_OPENING_END_DATE = attrs['SITE_OPE_1'] # The closing date of the site
		SITE_SEASON_START_DATE = attrs['SITE_SEASO'] # sub-site season start date
		SITE_SEASON_END_DATE = attrs['SITE_SEA_1'] # sub-site season end date
		ABA_ACCESSIBLE = attrs['ABA_ACCESS'] # Indicates whether the unit meets the ABA Accessible Guidelines
		CAPACITY_SIZE_RATING = attrs['CAPACITY_S'] # Capacity or size rating of the unit
		DRIVEWAY_DOUBLEWIDE = attrs['DRIVEWAY_D'] # Indicates whether the unit has a double-wide driveway
		PICNIC_TABLE = attrs['PICNIC_TAB'] # Indicates whether the unit has a picnic table
		FIRE_PIT = attrs['FIRE_PIT'] # Indicates whether the unit has a fire pit
		TENT_PAD = attrs['TENT_PAD'] # Indicates whether the unit has a tent pad
		WWW_RESERVABLE = attrs['WWW_RESERV'] # Indicates whether the unit can be reserved via the recreation.gov website
		WATERFRONT_UNIT = attrs['WATERFRONT'] # Indicates whether the unit is on the waterfront
		WATER_HOOKUP = attrs['WATER_HOOK'] # Indicates whether the unit has a water hookup
		SEWER_HOOK = attrs['SEWER_HOOK'] # Indicates whether the unit has a sewer hookup
		FOOD_STORAGE_LOCKER = attrs['FOOD_STORA'] # Indicates whether the unit has a food storage locker
		HORSE_HITCHING_POST = attrs['HORSE_HITC'] # Indicates whether the unit has a horse hitching post
		HORSE_STALL_CORRAL = attrs['HORSE_STAL'] # Indicates whether the unit has a horse stall or corral
		LAST_UPDATE = attrs['LAST_UPDAT'] # A date field that shows the last date a record was edited. Can be used to determine if record needs to be updated.


		tags.update({'nfs':'yes'})
		return tags
