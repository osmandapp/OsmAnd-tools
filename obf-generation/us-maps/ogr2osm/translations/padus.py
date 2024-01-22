# -*- coding: utf-8 -*-
import ogr2osm

class PadusTranslation(ogr2osm.TranslationBase):

	def filter_tags(self, attrs):
		if not attrs:
			return

		tags = {}

		Category = attrs['Category']
		Unit_Nm = attrs['Unit_Nm']
		Own_Name = attrs['Own_Name']
		d_Own_Name = attrs['d_Own_Name']
		Mang_Name = attrs['Mang_Name']
		Mang_Type = attrs['Mang_Type']
		d_Mang_Typ = attrs['d_Mang_Typ']
		d_Mang_Nam = attrs['d_Mang_Nam']
		Des_Tp = attrs['Des_Tp']
		d_Des_Tp = attrs['d_Des_Tp']
		Loc_Ds = attrs['Loc_Ds']
		Loc_Mang = attrs['Loc_Mang']
		Pub_Access = attrs['Pub_Access']
		GAP_Sts = attrs['GAP_Sts']
		if 'Category' in attrs and Category:
			tags['padus_category'] = Category.lower()
		if 'Loc_Ds' in attrs and Loc_Ds:
			tags['padus_local_designation'] = Loc_Ds
		if 'Unit_Nm' in attrs and Unit_Nm:
			tags['padus_unit_name'] = Unit_Nm
		if 'Loc_Mang' in attrs and Loc_Mang:
			tags['padus_local_manager'] = Loc_Mang
		if 'Own_Name' in attrs and Own_Name:
			tags['padus_owner_name'] = Own_Name.lower()
		if 'd_Own_Name' in attrs and d_Own_Name:
			tags['padus_owner_fullname'] = d_Own_Name
		if 'Mang_Type' in attrs and Mang_Type:
			tags['padus_manager_type'] = Mang_Type.lower()
		if 'd_Mang_Typ' in attrs and d_Mang_Typ:
			tags['padus_manager_type_fullname'] = d_Mang_Typ
		if 'Mang_Name' in attrs and Mang_Name:
			tags['padus_manager_name'] = Mang_Name.lower()
		if 'd_Mang_Nam' in attrs and d_Mang_Nam:
			tags['padus_manager_name_fullname'] = d_Mang_Nam
		if 'd_Des_Tp' in attrs and d_Des_Tp:
			tags['padus_designation_type_fullname'] = d_Des_Tp
		if 'Des_Tp' in attrs and Des_Tp:
			tags['padus_designation_type'] = Des_Tp.lower()
		if 'Pub_Access' in attrs and Pub_Access:
			tags['padus_public_access'] = Pub_Access.lower()
		if 'GAP_Sts' in attrs and GAP_Sts:
			tags['padus_gap_status_code'] = GAP_Sts

		tags.update({'padus':'area'})
		return tags
