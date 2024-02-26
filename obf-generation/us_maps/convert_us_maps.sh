#!/bin/bash
#set -x
# This script it used to prepare some NFS and BLM data for use in OsmAnd
# Data download link: https://data.fs.usda.gov/geodata/edw/datasets.php

dir=$(pwd)
workdir=/mnt/wd_2tb/us_maps
sourcedirname=source
sourceoriginaldirname=source_original
tmpdirname=tmp
outosmdirname=osm
ogr2osmdirname=ogr2osm
aggregate_rec_area_activities_template_name=aggregate_rec_area_activities_template
mapcreatordir=/home/xmd5a/utilites/OsmAndMapCreator-main/

trailsshpfilename="S_USA.TrailNFS_Publish"
roadsshpfilename="S_USA.RoadCore_FS"
roadsshpfilename="S_USA.RoadCore_FS"
blmroadstrailsname="blm_roads_trails"
#recreation_sites_shpfilename="S_USA.Rec_RecreationSitesINFRA"
rec_area_activities_source_filename="S_USA.RECAREAACTIVITIES_V"

mkdir -p $workdir/$sourcedirname
mkdir -p $workdir/$sourceoriginaldirname
mkdir -p $workdir/$tmpdirname
mkdir -p $workdir/$outosmdirname
mkdir -p $workdir/obf

cd $workdir/$tmpdirname
if [[ $? == 0 ]] ; then
	rm -f $workdir/$tmpdirname/*.*
fi

export sourcedirname
export tmpdirname
export workdir
export outosmdirname
export ogr2osmdirname
export dir

function generate_osm_from_shp {
	ogr2ogr $workdir/$tmpdirname/$1.shp $workdir/$sourcedirname/$1.shp -explodecollections
	cd $dir/$ogr2osmdirname
	python3 -m ogr2osm --suppress-empty-tags $workdir/$tmpdirname/$1.shp -o $workdir/$outosmdirname/us_$2.osm -t $2.py
}

function generate_osm_from_gdb {
	cd $dir/$ogr2osmdirname
	python3 -m ogr2osm --suppress-empty-tags $workdir/$sourcedirname/${rec_area_activities_source_filename}_agg.gpkg -o $workdir/$outosmdirname/us_$2.osm -t $2.py
}

function download_blm {
	cd $workdir/$sourceoriginaldirname/blm
	wget "https://opendata.arcgis.com/api/v3/datasets/f94999eb674d4085be0c86729fe4a151_3/downloads/data?format=shp&spatialRefId=4269&where=1%3D1" -nc -O- | bsdtar -xvf-
	wget "https://opendata.arcgis.com/api/v3/datasets/4a2d788c46804bfbb7288d05184e2936_0/downloads/data?format=shp&spatialRefId=4269&where=1%3D1" -nc -O- | bsdtar -xvf-
# 	wget "https://opendata.arcgis.com/api/v3/datasets/974363b4f22244d49350753f8fb5af32_2/downloads/data?format=shp&spatialRefId=4269&where=1%3D1" -nc -O- | bsdtar -xvf-
	wget "https://opendata.arcgis.com/api/v3/datasets/faaa72b7861842dc9274045b85fe41fd_7/downloads/data?format=shp&spatialRefId=4269&where=1%3D1" -nc -O- | bsdtar -xvf-
	wget "https://opendata.arcgis.com/api/v3/datasets/87c39b7529df474eb7ac0bd06a4ede0c_6/downloads/data?format=shp&spatialRefId=4269&where=1%3D1" -nc -O- | bsdtar -xvf-
	wget "https://opendata.arcgis.com/api/v3/datasets/fe8e9ceb24f34bdb97d5d6c1bb818252_5/downloads/data?format=shp&spatialRefId=4269&where=1%3D1" -nc -O- | bsdtar -xvf-
	wget "https://opendata.arcgis.com/api/v3/datasets/3f759ea461d84fd894cec96e360a121f_4/downloads/data?format=shp&spatialRefId=4269&where=1%3D1" -nc -O- | bsdtar -xvf-
	wget "https://opendata.arcgis.com/api/v3/datasets/2bf84b07ef564902bf9c783826a1f2d5_1/downloads/data?format=shp&spatialRefId=4269&where=1%3D1" -nc -O- | bsdtar -xvf-
	wget "https://opendata.arcgis.com/api/v3/datasets/7438e9e800914c94bad99f70a4f2092d_1/downloads/data?format=fgdb&spatialRefId=4269&where=1%3D1" -nc -O $workdir/$sourcedirname/BLM_Natl_Recreation_Site_Points.gdb.zip
	# Add field "FILE_NAME" to shp to allow objects differentiation after merging
	echo "Adding field 'FILE_NAME' to source files"
	python3 $dir/add_file_name_field.py $workdir/$sourceoriginaldirname/blm/BLM_Natl_GTLF_Public_Motorized_Roads.shp || if [[ $? == 1 ]]; then exit 1; fi
	python3 $dir/add_file_name_field.py $workdir/$sourceoriginaldirname/blm/BLM_Natl_GTLF_Limited_Public_Motorized_Roads.shp || if [[ $? == 1 ]]; then exit 1; fi
# 	python3 $dir/add_file_name_field.py $workdir/$sourceoriginaldirname/blm/BLM_Natl_GTLF_Public_Managed_Trails.shp || if [[ $? == 1 ]]; then exit 1; fi
	python3 $dir/add_file_name_field.py $workdir/$sourceoriginaldirname/blm/BLM_Natl_GTLF_Public_Nonmotorized_Trails.shp || if [[ $? == 1 ]]; then exit 1; fi
	python3 $dir/add_file_name_field.py $workdir/$sourceoriginaldirname/blm/BLM_Natl_GTLF_Public_Nonmechanized_Trails.shp || if [[ $? == 1 ]]; then exit 1; fi
	python3 $dir/add_file_name_field.py $workdir/$sourceoriginaldirname/blm/BLM_Natl_GTLF_Public_Motorized_Trails.shp || if [[ $? == 1 ]]; then exit 1; fi
	python3 $dir/add_file_name_field.py $workdir/$sourceoriginaldirname/blm/BLM_Natl_GTLF_Limited_Public_Motorized_Trails.shp || if [[ $? == 1 ]]; then exit 1; fi
	python3 $dir/add_file_name_field.py $workdir/$sourceoriginaldirname/blm/BLM_Natl_GTLF_Public_Not_Assessed_Trails.shp || if [[ $? == 1 ]]; then exit 1; fi
}
# National Forest System Trails
# cd $workdir/$sourcedirname
# if [ ! -f $workdir/$sourcedirname/$trailsshpfilename.shp ]; then
# 	wget "https://data.fs.usda.gov/geodata/edw/edw_resources/shp/S_USA.TrailNFS_Publish.zip" -nc -O- | bsdtar -xvf-
# fi
# if [ ! -f "$workdir/$outosmdirname/us_nfs_trails.osm" ] ; then
# 	echo ==============Generating $trailsshpfilename
# 	generate_osm_from_shp "$trailsshpfilename" "nfs_trails"
# fi
#
# National Forest System Roads
# if [ ! -f $workdir/$sourcedirname/$roadsshpfilename.shp ]; then
# 	wget "https://data.fs.usda.gov/geodata/edw/edw_resources/shp/S_USA.RoadCore_FS.zip" -nc -O- | bsdtar -xvf-
# fi
# if [ ! -f "$workdir/$outosmdirname/us_nfs_roads.pbf" ] ; then
# 	echo ==============Generating $roadsshpfilename
# 	generate_osm_from_shp "$roadsshpfilename" "nfs_roads"
# 	osmconvert $workdir/$outosmdirname/us_nfs_roads.osm --out-pbf > $workdir/$outosmdirname/us_nfs_roads.pbf
# 	rm $workdir/$outosmdirname/us_nfs_roads.osm
# fi

# NFS Recreation Area Activities
# wget "https://data.fs.usda.gov/geodata/edw/edw_resources/fc/S_USA.RECAREAACTIVITIES_V.gdb.zip" -nc -O "$workdir/$sourcedirname/$rec_area_activities_source_filename.gdb.zip"
# if [ ! -f "$workdir/$outosmdirname/us_rec_area_activities.osm" ] ; then
# 	echo ==============Generating $rec_area_activities_source_filename
# 	if [[ -f $workdir/$sourcedirname/$rec_area_activities_source_filename.gpkg ]] ; then
# 		rm $workdir/$sourcedirname/$rec_area_activities_source_filename.gpkg
# 	fi
# 	if [[ -f $workdir/$tmpdirname/${rec_area_activities_source_filename}_agg.gpkg ]] ; then
# 		rm $workdir/$tmpdirname/${rec_area_activities_source_filename}_agg.gpkg
# 	fi
# 	ogr2ogr $workdir/$tmpdirname/$rec_area_activities_source_filename.gpkg $workdir/$sourceoriginaldirname/$rec_area_activities_source_filename.gdb.zip -explodecollections
# 	cp $dir/$aggregate_rec_area_activities_template_name.py $workdir/$tmpdirname/
# 	sed -i "s,INPUT_FILE,$workdir/$tmpdirname/$rec_area_activities_source_filename.gpkg,g" $workdir/$tmpdirname/$aggregate_rec_area_activities_template_name.py
# 	sed -i "s,OUTPUT_FILE,$workdir/$sourcedirname/${rec_area_activities_source_filename}_agg.gpkg,g" $workdir/$tmpdirname/$aggregate_rec_area_activities_template_name.py
# 	python3 $workdir/$tmpdirname/$aggregate_rec_area_activities_template_name.py
# 	generate_osm_from_gdb "$rec_area_activities_source_filename" "rec_area_activities"
# fi

# BLM Roads and Trails
if [ ! -f "$workdir/$outosmdirname/us_blm_roads_trails.osm" ] ; then
	if [ -d $workdir/$sourceoriginaldirname/blm ]; then
		rm -f $workdir/$sourceoriginaldirname/blm/*.*
	else
		mkdir -p $workdir/$sourceoriginaldirname/blm
	fi
	download_blm
	ogrmerge.py -single -overwrite_ds -f "ESRI Shapefile" -o $workdir/$sourcedirname/$blmroadstrailsname.shp *.shp
	rm -rf $workdir/$sourceoriginaldirname/blm

	echo ==============Generating $blmroadstrailsname
	generate_osm_from_shp $blmroadstrailsname $blmroadstrailsname
fi


# cd $mapcreatordir
# bash utilities.sh generate-obf-files-in-batch $dir/batch-us_maps.xml
