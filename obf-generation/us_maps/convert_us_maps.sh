#!/bin/bash
#set -x
# This script it used to prepare some NFS and BLM data for use in OsmAnd
# Data download link: https://data.fs.usda.gov/geodata/edw/datasets.php

dir=$(pwd)
work_dir=/mnt/wd_2tb/us_maps
source_dir_name=source
source_original_dir_name=source_original
tmp_dir=tmp
out_osm_dir=osm
out_obf_dir=obf
ogr2osm_dir=ogr2osm
aggregate_nfs_rec_area_activities_template_name=aggregate_nfs_rec_area_activities_template
mapcreator_dir=/home/xmd5a/utilites/OsmAndMapCreator-main
poi_types_path=/home/xmd5a/git/OsmAnd-resources/poi/poi_types_us-maps.xml
rendering_types_path=/home/xmd5a/git/OsmAnd-resources/obf_creation/rendering_types_us-maps.xml

trails_shp_filename="S_USA.TrailNFS_Publish"
roads_shp_filename="S_USA.RoadCore_FS"
#recreation_sites_shpfilename="S_USA.Rec_RecreationSitesINFRA"
nfs_rec_area_activities_source_filename="S_USA.RECAREAACTIVITIES_V"
blm_roads_trails_name="blm_roads_trails"
blm_recreation_site_source_filename="BLM_Natl_Recreation_Site_Points"

mkdir -p $work_dir/$source_dir_name
mkdir -p $work_dir/$source_original_dir_name
mkdir -p $work_dir/$tmp_dir
mkdir -p $work_dir/$out_osm_dir
mkdir -p $work_dir/$out_obf_dir

cd $work_dir/$tmp_dir
if [[ $? == 0 ]] ; then
	rm -f $work_dir/$tmp_dir/*.*
fi

export source_dir_name
export tmp_dir
export work_dir
export out_osm_dir
export ogr2osm_dir
export dir

function generate_osm_from_shp {
	ogr2ogr $work_dir/$tmp_dir/$1.shp $work_dir/$source_dir_name/$1.shp -explodecollections
	cd $dir/$ogr2osm_dir
	python3 -m ogr2osm --suppress-empty-tags $work_dir/$tmp_dir/$1.shp -o $work_dir/$out_osm_dir/us_$2.osm -t $2.py
}

function download_blm_roads_trails {
	cd $work_dir/$source_original_dir_name/blm
	wget "https://opendata.arcgis.com/api/v3/datasets/f94999eb674d4085be0c86729fe4a151_3/downloads/data?format=shp&spatialRefId=4269&where=1%3D1" -nc -O- | bsdtar -xvf-
	wget "https://opendata.arcgis.com/api/v3/datasets/4a2d788c46804bfbb7288d05184e2936_0/downloads/data?format=shp&spatialRefId=4269&where=1%3D1" -nc -O- | bsdtar -xvf-
	wget "https://opendata.arcgis.com/api/v3/datasets/faaa72b7861842dc9274045b85fe41fd_7/downloads/data?format=shp&spatialRefId=4269&where=1%3D1" -nc -O- | bsdtar -xvf-
	wget "https://opendata.arcgis.com/api/v3/datasets/87c39b7529df474eb7ac0bd06a4ede0c_6/downloads/data?format=shp&spatialRefId=4269&where=1%3D1" -nc -O- | bsdtar -xvf-
	wget "https://opendata.arcgis.com/api/v3/datasets/fe8e9ceb24f34bdb97d5d6c1bb818252_5/downloads/data?format=shp&spatialRefId=4269&where=1%3D1" -nc -O- | bsdtar -xvf-
	wget "https://opendata.arcgis.com/api/v3/datasets/3f759ea461d84fd894cec96e360a121f_4/downloads/data?format=shp&spatialRefId=4269&where=1%3D1" -nc -O- | bsdtar -xvf-
	wget "https://opendata.arcgis.com/api/v3/datasets/2bf84b07ef564902bf9c783826a1f2d5_1/downloads/data?format=shp&spatialRefId=4269&where=1%3D1" -nc -O- | bsdtar -xvf-
	# Add field "FILE_NAME" to shp to allow objects differentiation after merging
	echo "Adding field 'FILE_NAME' to source files"
	python3 $dir/add_file_name_field.py $work_dir/$source_original_dir_name/blm/BLM_Natl_GTLF_Public_Motorized_Roads.shp || if [[ $? == 1 ]]; then exit 1; fi
	python3 $dir/add_file_name_field.py $work_dir/$source_original_dir_name/blm/BLM_Natl_GTLF_Limited_Public_Motorized_Roads.shp || if [[ $? == 1 ]]; then exit 1; fi
	python3 $dir/add_file_name_field.py $work_dir/$source_original_dir_name/blm/BLM_Natl_GTLF_Public_Nonmotorized_Trails.shp || if [[ $? == 1 ]]; then exit 1; fi
	python3 $dir/add_file_name_field.py $work_dir/$source_original_dir_name/blm/BLM_Natl_GTLF_Public_Nonmechanized_Trails.shp || if [[ $? == 1 ]]; then exit 1; fi
	python3 $dir/add_file_name_field.py $work_dir/$source_original_dir_name/blm/BLM_Natl_GTLF_Public_Motorized_Trails.shp || if [[ $? == 1 ]]; then exit 1; fi
	python3 $dir/add_file_name_field.py $work_dir/$source_original_dir_name/blm/BLM_Natl_GTLF_Limited_Public_Motorized_Trails.shp || if [[ $? == 1 ]]; then exit 1; fi
	python3 $dir/add_file_name_field.py $work_dir/$source_original_dir_name/blm/BLM_Natl_GTLF_Public_Not_Assessed_Trails.shp || if [[ $? == 1 ]]; then exit 1; fi
}
# National Forest System Trails
cd $work_dir/$source_dir_name
if [ ! -f $work_dir/$source_dir_name/$trails_shp_filename.shp ]; then
	wget "https://data.fs.usda.gov/geodata/edw/edw_resources/shp/S_USA.TrailNFS_Publish.zip" -nc -O- | bsdtar -xvf-
fi
if [ ! -f "$work_dir/$out_osm_dir/us_nfs_trails.osm" ] ; then
	echo ==============Generating $trails_shp_filename
	generate_osm_from_shp "$trails_shp_filename" "nfs_trails"
fi

# National Forest System Roads
if [ ! -f $work_dir/$source_dir_name/$roads_shp_filename.shp ]; then
	wget "https://data.fs.usda.gov/geodata/edw/edw_resources/shp/S_USA.RoadCore_FS.zip" -nc -O- | bsdtar -xvf-
fi
if [ ! -f "$work_dir/$out_osm_dir/us_nfs_roads.pbf" ] ; then
	echo ==============Generating $roads_shp_filename
	generate_osm_from_shp "$roads_shp_filename" "nfs_roads"
	osmconvert $work_dir/$out_osm_dir/us_nfs_roads.osm --out-pbf > $work_dir/$out_osm_dir/us_nfs_roads.pbf
	rm $work_dir/$out_osm_dir/us_nfs_roads.osm
fi

# NFS Recreation Area Activities
wget "https://data.fs.usda.gov/geodata/edw/edw_resources/fc/S_USA.RECAREAACTIVITIES_V.gdb.zip" -nc -O "$work_dir/$source_dir_name/$nfs_rec_area_activities_source_filename.gdb.zip"
if [ ! -f "$work_dir/$out_osm_dir/us_nfs_rec_area_activities.osm" ] ; then
	echo ==============Generating $nfs_rec_area_activities_source_filename
	if [[ -f $work_dir/$source_dir_name/$nfs_rec_area_activities_source_filename.gpkg ]] ; then
		rm $work_dir/$source_dir_name/$nfs_rec_area_activities_source_filename.gpkg
	fi
	if [[ -f $work_dir/$tmp_dir/${nfs_rec_area_activities_source_filename}_agg.gpkg ]] ; then
		rm $work_dir/$tmp_dir/${nfs_rec_area_activities_source_filename}_agg.gpkg
	fi
	ogr2ogr $work_dir/$tmp_dir/$nfs_rec_area_activities_source_filename.gpkg $work_dir/$source_original_dir_name/$nfs_rec_area_activities_source_filename.gdb.zip -explodecollections
	cp $dir/$aggregate_nfs_rec_area_activities_template_name.py $work_dir/$tmp_dir/
	sed -i "s,INPUT_FILE,$work_dir/$tmp_dir/$nfs_rec_area_activities_source_filename.gpkg,g" $work_dir/$tmp_dir/$aggregate_nfs_rec_area_activities_template_name.py
	sed -i "s,OUTPUT_FILE,$work_dir/$source_dir_name/${nfs_rec_area_activities_source_filename}_agg.gpkg,g" $work_dir/$tmp_dir/$aggregate_nfs_rec_area_activities_template_name.py
	python3 $work_dir/$tmp_dir/$aggregate_nfs_rec_area_activities_template_name.py
	cd $dir/$ogr2osm_dir && python3 -m ogr2osm --suppress-empty-tags $work_dir/$source_dir_name/${nfs_rec_area_activities_source_filename}_agg.gpkg -o $work_dir/$out_osm_dir/us_nfs_rec_area_activities.osm -t nfs_rec_area_activities.py
fi

# BLM Roads and Trails
if [ ! -f "$work_dir/$out_osm_dir/us_blm_roads_trails.osm" ] ; then
	if [ -d $work_dir/$source_original_dir_name/blm ]; then
		rm -f $work_dir/$source_original_dir_name/blm/*.*
	else
		mkdir -p $work_dir/$source_original_dir_name/blm
	fi
	download_blm_roads_trails
	ogrmerge.py -single -overwrite_ds -f "ESRI Shapefile" -o $work_dir/$source_dir_name/$blm_roads_trails_name.shp *.shp
	rm -rf $work_dir/$source_original_dir_name/blm

	echo ==============Generating $blm_roads_trails_name
	generate_osm_from_shp $blm_roads_trails_name $blm_roads_trails_name
fi

# BLM Recreation Site Points
if [ ! -f "$work_dir/$out_osm_dir/us_blm_rec_area_activities.osm" ] ; then
	wget "https://opendata.arcgis.com/api/v3/datasets/7438e9e800914c94bad99f70a4f2092d_1/downloads/data?format=fgdb&spatialRefId=4269&where=1%3D1" -nc -O $work_dir/$source_dir_name/$blm_recreation_site_source_filename.gdb.zip
	echo ==============Generating $blm_recreation_site_source_filename
	cd $dir/$ogr2osm_dir && python3 -m ogr2osm --suppress-empty-tags $work_dir/$source_dir_name/$blm_recreation_site_source_filename.gdb.zip -o $work_dir/$out_osm_dir/us_blm_rec_area_activities.osm -t blm_recreation_site.py
fi



cd $work_dir/$out_obf_dir
bash $mapcreator_dir/utilities.sh generate-obf $work_dir/$out_osm_dir/us_blm_rec_area_activities.osm --poi-types=$poi_types_path --rendering-types=$rendering_types_path
bash $mapcreator_dir/utilities.sh generate-obf $work_dir/$out_osm_dir/us_blm_roads_trails.osm --poi-types=$poi_types_path --rendering-types=$rendering_types_path
bash $mapcreator_dir/utilities.sh generate-obf $work_dir/$out_osm_dir/us_nfs_trails.osm --poi-types=$poi_types_path --rendering-types=$rendering_types_path
bash $mapcreator_dir/utilities.sh generate-obf $work_dir/$out_osm_dir/us_nfs_rec_area_activities.osm --poi-types=$poi_types_path --rendering-types=$rendering_types_path
bash $mapcreator_dir/utilities.sh generate-obf $work_dir/$out_osm_dir/us_nfs_roads.pbf --poi-types=$poi_types_path --rendering-types=$rendering_types_path
