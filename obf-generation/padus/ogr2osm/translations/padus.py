# -*- coding: utf-8 -*-
import ogr2osm

class PadusTranslation(ogr2osm.TranslationBase):

	def filter_tags(self, attrs):
		if not attrs:
			return

		tags = {}

		Category = attrs['Category']
		Unit_Nm = attrs['Unit_Nm']
		Loc_Nm = attrs['Loc_Nm']
		Own_Name = attrs['Own_Name']
		d_Own_Name = attrs['d_Own_Name']
		Mang_Name = attrs['Mang_Name']
		Mang_Type = attrs['Mang_Type']
		d_Mang_Typ = attrs['d_Mang_Typ']
		Des_Tp = attrs['Des_Tp']
		d_Des_Tp = attrs['d_Des_Tp']
		Loc_Ds = attrs['Loc_Ds']
		Loc_Mang = attrs['Loc_Mang']
		if 'Category' in attrs:
			tags['padus_category'] = Category.lower()
		if 'Loc_Nm' in attrs:
			tags['padus_local_name'] = Loc_Nm
		if 'Loc_Ds' in attrs:
			tags['padus_local_designation'] = Loc_Ds
		if 'Unit_Nm' in attrs:
			tags['padus_unit_name'] = Unit_Nm
		if 'Loc_Mang' in attrs:
			tags['padus_local_manager'] = Loc_Mang.lower()
		if 'Own_Name' in attrs:
			tags['padus_owner_name'] = Own_Name.lower()
		if 'd_Own_Name' in attrs:
			tags['padus_owner_fullname'] = d_Own_Name
		if 'Mang_Type' in attrs:
			tags['padus_manager_type'] = Mang_Type.lower()
		if 'd_Mang_Typ' in attrs:
			tags['padus_manager_type_fullname'] = d_Mang_Typ
		if 'Mang_Name' in attrs:
			tags['padus_manager_name'] = Mang_Name.lower()
		if 'd_Des_Tp' in attrs:
			tags['padus_designation_type_fullname'] = d_Des_Tp
		if 'Des_Tp' in attrs:
			tags['padus_designation_type'] = Des_Tp.lower()

		tags.update({'padus':'area'})
		return tags
