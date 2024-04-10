#!/bin/bash
set -x
# This script it used to prepare some NFS, BLM and parcel data for use in OsmAnd
# Data download link: https://data.fs.usda.gov/geodata/edw/datasets.php
# https://github.com/osmandapp/OsmAnd-Issues/issues/2409

dir=$(pwd)
work_dir=/mnt/wd_2tb/us_maps
source_dir_name=source
source_original_dir_name=source_original
padus_source_dir=/mnt/wd_2tb/padus/shp
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

process_nfs_trails=false
process_nfs_roads=false
process_nfs_rec_area_activities=false
process_nfs_trails=false
process_blm_roads_trails=false
process_blm_recreation_site_points=false

private_lands_test_source_filename="in_marion"
private_lands_wisconsin_source_filename="V9_0_1_Wisconsin_Parcels_2023_10_3_Uncompressed"
private_lands_utah_source_filename="UtahStatewideParcels"
private_lands_maine_source_filename="Maine_parcels_statewide"
private_lands_vermont_source_filename="Vermont_parcels"
private_lands_montana_source_filename="MontanaCadastral_GDB"
private_lands_newjersey_source_filename="New-Jersey_parcelsStatewide"
private_lands_massachusets_source_filename="Massachusets_L3_AGGREGATE"

process_private_lands_test=false
process_private_lands_wisconsin=false
process_private_lands_utah=false
process_private_lands_maine=false
process_private_lands_vermont=false
process_private_lands_montana=false
process_private_lands_newjersey=false
process_private_lands_massachusets=true

url_massachusets="https://s3.us-east-1.amazonaws.com/download.massgis.digital.mass.gov/shapefiles/l3parcels/L3_AGGREGATE_SHP_20240102.zip"

mkdir -p $work_dir/$source_dir_name
mkdir -p $work_dir/$source_original_dir_name
mkdir -p $work_dir/$tmp_dir
mkdir -p $work_dir/$out_osm_dir
mkdir -p $work_dir/$out_obf_dir

cd $work_dir/$tmp_dir
if [[ $? == 0 ]] ; then
	rm -rf $work_dir/$tmp_dir/*.*
fi

export source_dir_name
export tmp_dir
export work_dir
export out_osm_dir
export ogr2osm_dir
export dir
export poi_types_path
export rendering_types_path

function generate_osm_from_shp {
	ogr2ogr $work_dir/$tmp_dir/$1.shp $work_dir/$source_dir_name/$1.shp -explodecollections
	cd $dir/$ogr2osm_dir
	time python3 -m ogr2osm --suppress-empty-tags $work_dir/$tmp_dir/$1.shp -o $work_dir/$out_osm_dir/us_$2.osm -t $2.py
}

function generate_osm_private_lands {
	echo -e "\033[95mGenerating osm from $1\033[0m"
	cd $dir/$ogr2osm_dir && time python3 -m ogr2osm --suppress-empty-tags "$1" -o $work_dir/$out_osm_dir/us_${2}_pl_northamerica.osm -t private_lands.py
	osmconvert $work_dir/$out_osm_dir/us_${2}_pl_northamerica.osm --out-pbf > $work_dir/$out_osm_dir/us_${2}_pl_northamerica.pbf
}

function generate_obf {
	echo -e "\033[95mGenerating obf from $1\033[0m"
	cd $work_dir/$out_obf_dir
	time bash $mapcreator_dir/utilities.sh generate-obf $work_dir/$out_osm_dir/$1 --poi-types=$poi_types_path --rendering-types=$rendering_types_path
}

function run_alg_fixgeometries {
	echo -e "\033[95mrun_alg_fixgeometries $1\033[0m"
	filename=$(basename -- "$1")
	filename="${filename%%.*}"
	qgis_process run native:fixgeometries \
		--distance_units=meters \
		--area_units=m2 \
		--INPUT="$1" \
		--METHOD=0 \
		--OUTPUT="$2/${filename}_fixed.gpkg"
}

function run_alg_dissolve {
	echo -e "\033[95mrun_alg_dissolve $1\033[0m"
	filename=$(basename -- "$1")
	filename="${filename%%.*}"
	qgis_process run native:dissolve \
		--distance_units=meters \
		--area_units=m2 \
		--INPUT="$1" \
		--FIELD="$2" \
		--SEPARATE_DISJOINT=false \
		--OUTPUT="$3/${filename}_dissolved.gpkg"
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
	python3 $dir/add_file_name_field.py $work_dir/$source_original_dir_name/blm/BLM_Natl_GTLF_Public_Motorized_Roads.shp || if [[ $? == 1 ]]; then exit 1; fi
	python3 $dir/add_file_name_field.py $work_dir/$source_original_dir_name/blm/BLM_Natl_GTLF_Limited_Public_Motorized_Roads.shp || if [[ $? == 1 ]]; then exit 1; fi
	python3 $dir/add_file_name_field.py $work_dir/$source_original_dir_name/blm/BLM_Natl_GTLF_Public_Nonmotorized_Trails.shp || if [[ $? == 1 ]]; then exit 1; fi
	python3 $dir/add_file_name_field.py $work_dir/$source_original_dir_name/blm/BLM_Natl_GTLF_Public_Nonmechanized_Trails.shp || if [[ $? == 1 ]]; then exit 1; fi
	python3 $dir/add_file_name_field.py $work_dir/$source_original_dir_name/blm/BLM_Natl_GTLF_Public_Motorized_Trails.shp || if [[ $? == 1 ]]; then exit 1; fi
	python3 $dir/add_file_name_field.py $work_dir/$source_original_dir_name/blm/BLM_Natl_GTLF_Limited_Public_Motorized_Trails.shp || if [[ $? == 1 ]]; then exit 1; fi
	python3 $dir/add_file_name_field.py $work_dir/$source_original_dir_name/blm/BLM_Natl_GTLF_Public_Not_Assessed_Trails.shp || if [[ $? == 1 ]]; then exit 1; fi
}
# National Forest System Trails
if [[ $process_nfs_trails == true ]] ; then
	cd $work_dir/$source_dir_name
	if [ ! -f $work_dir/$source_dir_name/$trails_shp_filename.shp ]; then
		wget "https://data.fs.usda.gov/geodata/edw/edw_resources/shp/S_USA.TrailNFS_Publish.zip" -nc -O- | bsdtar -xvf-
	fi
	if [ ! -f "$work_dir/$out_osm_dir/us_nfs_trails.osm" ] ; then
		echo ==============Generating $trails_shp_filename
		generate_osm_from_shp "$trails_shp_filename" "nfs_trails"
	fi
fi

# National Forest System Roads
if [[ $process_nfs_roads == true ]] ; then
	if [ ! -f $work_dir/$source_dir_name/$roads_shp_filename.shp ]; then
		wget "https://data.fs.usda.gov/geodata/edw/edw_resources/shp/S_USA.RoadCore_FS.zip" -nc -O- | bsdtar -xvf-
	fi
	if [ ! -f "$work_dir/$out_osm_dir/us_nfs_roads.pbf" ] ; then
		echo ==============Generating $roads_shp_filename
		generate_osm_from_shp "$roads_shp_filename" "nfs_roads"
		time osmconvert $work_dir/$out_osm_dir/us_nfs_roads.osm --out-pbf > $work_dir/$out_osm_dir/us_nfs_roads.pbf
		rm $work_dir/$out_osm_dir/us_nfs_roads.osm
	fi
fi

# NFS Recreation Area Activities
if [[ $process_nfs_rec_area_activities == true ]] ; then
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
		time python3 $work_dir/$tmp_dir/$aggregate_nfs_rec_area_activities_template_name.py
		cd $dir/$ogr2osm_dir && python3 -m ogr2osm --suppress-empty-tags $work_dir/$source_dir_name/${nfs_rec_area_activities_source_filename}_agg.gpkg -o $work_dir/$out_osm_dir/us_nfs_rec_area_activities.osm -t nfs_rec_area_activities.py
	fi
fi

# BLM Roads and Trails
if [[ $process_nfs_rec_area_activities == true ]] && [ ! -f "$work_dir/$out_osm_dir/us_blm_roads_trails.osm" ]; then
	if [ -d $work_dir/$source_original_dir_name/blm ]; then
		rm -f $work_dir/$source_original_dir_name/blm/*.*
	else
		mkdir -p $work_dir/$source_original_dir_name/blm
	fi
	download_blm_roads_trails
	time ogrmerge.py -single -overwrite_ds -f "ESRI Shapefile" -o $work_dir/$source_dir_name/$blm_roads_trails_name.shp *.shp
	rm -rf $work_dir/$source_original_dir_name/blm

	echo ==============Generating $blm_roads_trails_name
	generate_osm_from_shp $blm_roads_trails_name $blm_roads_trails_name
fi

# BLM Recreation Site Points
if [[ $process_blm_recreation_site_points == true ]] && [ ! -f "$work_dir/$out_osm_dir/us_blm_rec_area_activities.osm" ]; then
	wget "https://opendata.arcgis.com/api/v3/datasets/7438e9e800914c94bad99f70a4f2092d_1/downloads/data?format=fgdb&spatialRefId=4269&where=1%3D1" -nc -O $work_dir/$source_dir_name/$blm_recreation_site_source_filename.gdb.zip
	echo ==============Generating $blm_recreation_site_source_filename
	cd $dir/$ogr2osm_dir && time python3 -m ogr2osm --suppress-empty-tags $work_dir/$source_dir_name/$blm_recreation_site_source_filename.gdb.zip -o $work_dir/$out_osm_dir/us_blm_rec_area_activities.osm -t blm_recreation_site.py
fi

# Private lands test (Dallas)
if [[ $process_private_lands_test == true ]] && [ ! -f "$work_dir/$out_osm_dir/us_${private_lands_test_source_filename}_pl_northamerica.osm" ]; then
	echo ==============Generating $private_lands_test_source_filename
	cd $dir/$ogr2osm_dir && time python3 -m ogr2osm --suppress-empty-tags $work_dir/$source_dir_name/$private_lands_test_source_filename.gpkg -o $work_dir/$out_osm_dir/us_${private_lands_test_source_filename}_pl_northamerica.osm -t private_lands.py
	osmconvert $work_dir/$out_osm_dir/us_${private_lands_test_source_filename}_pl_northamerica.osm --out-pbf > $work_dir/$out_osm_dir/us_${private_lands_test_source_filename}_pl_northamerica.pbf
# 	rm -f $work_dir/$out_osm_dir/us_${private_lands_test_source_filename}_pl_northamerica.osm
fi

# Private lands Wisconsin
if [[ $process_private_lands_wisconsin == true ]] && [ ! -f "$work_dir/$out_osm_dir/us_wisconsin_pl_northamerica.osm" ]; then
	state_name=wisconsin
	source_file_name=$private_lands_wisconsin_source_filename
	echo ==============Generating $source_file_name

	if [[ ! -f $work_dir/$source_original_dir_name/$source_file_name.gdb.zip ]]; then
		wget "https://uwmadison.box.com/shared/static/j0o2i5fmu5vthmab4mzuanws9lvsmpea.zip" -nc -O "$work_dir/$source_original_dir_name/$source_file_name.zip" # https://www.sco.wisc.edu/parcels/data/
		# Repack gdb because it has wrong directory structure
		unzip "$work_dir/$source_original_dir_name/$source_file_name.zip" -d $work_dir/$tmp_dir/
		if [[ -f $work_dir/$source_dir_name/$source_file_name.gdb.zip ]]; then
			rm -f $work_dir/$source_dir_name/$source_file_name.gdb.zip
		fi
		cd $work_dir/$tmp_dir/$source_file_name && zip -r $work_dir/$source_dir_name/$source_file_name.gdb.zip .
	fi
	qgis_process run native:extractbyexpression --distance_units=meters --area_units=m2 --INPUT="$work_dir/$source_dir_name/$source_file_name.gdb.zip" --EXPRESSION='PARCELID != '\''ROW'\'' AND PARCELID != '\''ROAD'\'' AND PARCELID != '\''WATER'\'' AND strpos(PARCELID,'\''LAKE'\'') < 1 AND strpos(PARCELID,'\''HYDRO'\'') < 1 AND strpos(PARCELID,'\''RIVER'\'') < 1 AND PARCELID != '\''GAP'\'' AND strpos(PARCELID,'\''RAILROAD'\'') < 1' --OUTPUT=$work_dir/$tmp_dir/${source_file_name}_extracted.gpkg
	run_alg_fixgeometries $work_dir/$tmp_dir/${source_file_name}_extracted.gpkg $work_dir/$tmp_dir
	run_alg_dissolve $work_dir/$tmp_dir/${source_file_name}_extracted_fixed.gpkg OWNERNME1 $work_dir/$source_dir_name
	generate_osm_private_lands $work_dir/$source_dir_name/${source_file_name}_extracted_fixed_dissolved.gpkg $state_name
	generate_obf us_${state_name}_pl_northamerica.pbf
fi

# Private lands Utah
if [[ $process_private_lands_utah == true ]] && [ ! -f "$work_dir/$out_osm_dir/us_utah_pl_northamerica.osm" ]; then
	state_name=utah
	source_file_name=$private_lands_utah_source_filename
	echo ==============Generating $source_file_name
	if [[ ! -f $work_dir/$source_original_dir_name/$source_file_name.gdb.zip ]]; then
		wget "https://stg-arcgisazurecdataprod1.az.arcgis.com/exportfiles-5278-21569/UtahStatewideParcels_-1516116324658468324.zip?sv=2018-03-28&sr=b&sig=A1CWCUzGZpz0GohyK51vnx6shIpTtOSxPmljFNDdcxc%3D&se=2024-03-28T07%3A33%3A08Z&sp=r" -O $work_dir/$source_original_dir_name/$source_file_name.gdb.zip
	fi

	run_alg_fixgeometries $work_dir/$source_original_dir_name/$source_file_name.gdb.zip $work_dir/$source_dir_name
	generate_osm_private_lands $work_dir/$source_dir_name/${source_file_name}_fixed.gpkg $state_name
	generate_obf us_${state_name}_pl_northamerica.pbf
fi

# Private lands Maine
if [[ $process_private_lands_maine == true ]] && [ ! -f "$work_dir/$out_osm_dir/us_maine_pl_northamerica.osm" ]; then
	state_name=maine
	source_file_name=$private_lands_maine_source_filename
	echo ==============Generating $source_file_name
# https://maine.hub.arcgis.com/content/9eeed34813b945619bf715c1d5a913d7/about # Maine Parcels Organized Towns Feature
#	https://ago-item-storage.s3.amazonaws.com/9eeed34813b945619bf715c1d5a913d7/Parcels.sd?X-Amz-Security-Token=IQoJb3JpZ2luX2VjENL%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FwEaCXVzLWVhc3QtMSJHMEUCIQDRVcRJD%2BIwXsxb10rTV4ROw1Fzs5MKs%2B4it5UbNF0PGwIgRHMoZe6t3yVMQQFdVep88%2FbmwLCt4s1OJVIibWz98%2FoqvAUI6%2F%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FARAAGgw2MDQ3NTgxMDI2NjUiDIWQBmJhIxV%2BOkNhzyqQBT4H6AaRRrtv3nSPFMeRkgYI6aGL4y8bvBMmx6M%2BhD2qMZE3mBjVIyEJt%2FyLCWzNlV8wN7%2BR99uhT7R%2FVs9S5NMtgHZ3YIniNLQf%2BWdOREYqvkUtY85rtcp8FxqhPjtybpPHDuE0wsRotxZC4sEOBBs9HfS%2F63%2BuOISgkH6eRnjIQlx087EBYKNkOtTKipHJHTWK33gFRskYGl5zmO7%2FBs7AxCR6LSudtoFgeGe%2FpZkzRaWZz%2FLFzS%2BB4ggD7p9cb7pa8ydEzZJjOu3lSJTw8Ig7h2G7ARnLG5R28vVY3npUFKLavlKe0h%2BuqOirIRtcrKUoJk8M45126CxKDtT1bG4%2B3gUvbfYG8pjiWju0fk2It4frknhlautyZRqaHGBJXnGQ3frUr7sCaE426Dcx5lU6wnWSYXcNOFXmDL7f%2BvfMmsw%2Bs%2BLGQic8w7GG0RGr7iCsQ1RnP310AzIzVgn6MDPrxUzEaHha0H%2Bz38%2B%2BCTuuWoHgG%2B1mUtTx7tQuzgRtRBa9wl3N7c0tqPltzi0Pxv7CeUi6mGjv9dLRMMxQWlULedFh5OHGy58oHdAx3hAJR4mRfsYWLnkaXDo%2Bhi43sKgwJlpEoNNYDD1RsUlPcquWopl7WLHoFii0BrrKLxFZMd5NeURCrqkyR0WFY9wnxfo7%2FU3qS5I74WgKwDRam%2Fva5E2tqMNZzy79B8l65KM7wt1MYjQ%2FArAyF1UstzKNZD4BiRgfTXiWYtUtUWa5pUXZAKe2VmhEJxPvJNsxJFSCLXVERlObLS%2Bh1%2F1AI66lAv9TXazfuZTJ4Kd2jhg9Vx7srVxxRBbrxQIUEFtNpwenpi71jCw06WGxNkD1DZ1U4QOz7bI6Or6K3KNKR8MNtDVjMLmjmrAGOrEBQakK7nAKShXWXMS%2FRO%2BAhQmMKBu39dODuahxm4vwPa2%2BMZ7p8kE74Au6yhIJMK7t8T%2BRnFgzck2qm5HEUClugQ3BvCqOe3smCEJihJLjYKuLQbMG9MrB12SxrMkijxN84jmHTLHpHflCVJ%2BsMvUfurx7BcdSygDJ2u6of8KrivpuDRPSvhva%2BkRJhmq%2BL%2FjLQzlLQibthiel%2Fc3jPoT7ZPuceEs0MqU082cEWX%2BnHnHt&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20240329T110313Z&X-Amz-SignedHeaders=host&X-Amz-Expires=300&X-Amz-Credential=ASIAYZTTEKKEZRBT4STA%2F20240329%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Signature=8e32420ab6f2a440197aa57a6a52a7926e3e1cf7587575f3fdf57bb68cc7d2e4

	if [[ ! -f $work_dir/$source_original_dir_name/$source_file_name.gdb.zip ]]; then
		wget "https://ago-item-storage.s3.amazonaws.com/9eeed34813b945619bf715c1d5a913d7/Parcels.sd?X-Amz-Security-Token=IQoJb3JpZ2luX2VjENL%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FwEaCXVzLWVhc3QtMSJHMEUCIQDRVcRJD%2BIwXsxb10rTV4ROw1Fzs5MKs%2B4it5UbNF0PGwIgRHMoZe6t3yVMQQFdVep88%2FbmwLCt4s1OJVIibWz98%2FoqvAUI6%2F%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FARAAGgw2MDQ3NTgxMDI2NjUiDIWQBmJhIxV%2BOkNhzyqQBT4H6AaRRrtv3nSPFMeRkgYI6aGL4y8bvBMmx6M%2BhD2qMZE3mBjVIyEJt%2FyLCWzNlV8wN7%2BR99uhT7R%2FVs9S5NMtgHZ3YIniNLQf%2BWdOREYqvkUtY85rtcp8FxqhPjtybpPHDuE0wsRotxZC4sEOBBs9HfS%2F63%2BuOISgkH6eRnjIQlx087EBYKNkOtTKipHJHTWK33gFRskYGl5zmO7%2FBs7AxCR6LSudtoFgeGe%2FpZkzRaWZz%2FLFzS%2BB4ggD7p9cb7pa8ydEzZJjOu3lSJTw8Ig7h2G7ARnLG5R28vVY3npUFKLavlKe0h%2BuqOirIRtcrKUoJk8M45126CxKDtT1bG4%2B3gUvbfYG8pjiWju0fk2It4frknhlautyZRqaHGBJXnGQ3frUr7sCaE426Dcx5lU6wnWSYXcNOFXmDL7f%2BvfMmsw%2Bs%2BLGQic8w7GG0RGr7iCsQ1RnP310AzIzVgn6MDPrxUzEaHha0H%2Bz38%2B%2BCTuuWoHgG%2B1mUtTx7tQuzgRtRBa9wl3N7c0tqPltzi0Pxv7CeUi6mGjv9dLRMMxQWlULedFh5OHGy58oHdAx3hAJR4mRfsYWLnkaXDo%2Bhi43sKgwJlpEoNNYDD1RsUlPcquWopl7WLHoFii0BrrKLxFZMd5NeURCrqkyR0WFY9wnxfo7%2FU3qS5I74WgKwDRam%2Fva5E2tqMNZzy79B8l65KM7wt1MYjQ%2FArAyF1UstzKNZD4BiRgfTXiWYtUtUWa5pUXZAKe2VmhEJxPvJNsxJFSCLXVERlObLS%2Bh1%2F1AI66lAv9TXazfuZTJ4Kd2jhg9Vx7srVxxRBbrxQIUEFtNpwenpi71jCw06WGxNkD1DZ1U4QOz7bI6Or6K3KNKR8MNtDVjMLmjmrAGOrEBQakK7nAKShXWXMS%2FRO%2BAhQmMKBu39dODuahxm4vwPa2%2BMZ7p8kE74Au6yhIJMK7t8T%2BRnFgzck2qm5HEUClugQ3BvCqOe3smCEJihJLjYKuLQbMG9MrB12SxrMkijxN84jmHTLHpHflCVJ%2BsMvUfurx7BcdSygDJ2u6of8KrivpuDRPSvhva%2BkRJhmq%2BL%2FjLQzlLQibthiel%2Fc3jPoT7ZPuceEs0MqU082cEWX%2BnHnHt&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20240329T110313Z&X-Amz-SignedHeaders=host&X-Amz-Expires=300&X-Amz-Credential=ASIAYZTTEKKEZRBT4STA%2F20240329%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Signature=8e32420ab6f2a440197aa57a6a52a7926e3e1cf7587575f3fdf57bb68cc7d2e4" -O $work_dir/$source_original_dir_name/$source_file_name.gdb.zip
	fi
	run_alg_fixgeometries $work_dir/$source_dir_name/$source_file_name.gdb.zip $work_dir/$source_dir_name
	generate_osm_private_lands $work_dir/$source_dir_name/${source_file_name}_fixed.gpkg $state_name
	generate_obf us_${state_name}_pl_northamerica.pbf
fi

# Private lands Vermont
if [[ $process_private_lands_vermont == true ]]; then
	state_name=vermont
	source_file_name=$private_lands_vermont_source_filename
	if [[ ! -f "$work_dir/$out_osm_dir/us_{$state_name}_pl_northamerica.osm" ]]; then
		echo ==============Generating $source_file_name

		if [[ ! -f $work_dir/$source_original_dir_name/$source_file_name.gdb.zip ]]; then
			wget "https://stg-arcgisazurecdataprod1.az.arcgis.com/exportfiles-16272-4651/FS_VCGI_OPENDATA_Cadastral_VTPARCELS_poly_standardized_parcels_SP_v1_-3156129083514014623.zip?sv=2018-03-28&sr=b&sig=uQpDDu9bLC3Gv31Gi2NwGwgMHLopFXK1fzOkak1rm9A%3D&se=2024-04-01T12%3A48%3A32Z&sp=r" -O $work_dir/$source_original_dir_name/$source_file_name.gdb.zip
		fi
		qgis_process run native:extractbyexpression --distance_units=meters --area_units=m2 --INPUT="$work_dir/$source_dir_name/$source_file_name.gdb.zip" --EXPRESSION='PROPTYPE != '\''ROW_ROAD'\''  AND PROPTYPE != '\''ROW_RAIL'\'' AND PROPTYPE != '\''WATER'\''' --OUTPUT=$work_dir/$tmp_dir/${source_file_name}_extracted.gpkg

		run_alg_fixgeometries $work_dir/$tmp_dir/${source_file_name}_extracted.gpkg $work_dir/$source_dir_name
		generate_osm_private_lands $work_dir/$source_dir_name/${source_file_name}_extracted_fixed.gpkg $state_name
		generate_obf us_${state_name}_pl_northamerica.pbf
	fi
fi

# Private lands Montana
if [[ $process_private_lands_montana == true ]]; then
	state_name=montana
	source_file_name=$private_lands_montana_source_filename
	if [[ ! -f "$work_dir/$out_osm_dir/us_{$state_name}_pl_northamerica.osm" ]]; then
		echo -e "\033[92m\e[44m Processing $source_file_name\e[49m\033[0m"
		if [[ ! -f $work_dir/$source_dir_name/$source_file_name.zip ]]; then
			wget "https://ftpgeoinfo.msl.mt.gov/Data/Spatial/MSDI/Cadastral/MontanaCadastral_GDB.zip" -O $work_dir/$source_original_dir_name/$source_file_name.zip # https://ftpgeoinfo.msl.mt.gov/Data/Spatial/MSDI/Cadastral/
		fi
		run_alg_fixgeometries "/vsizip/$work_dir/$source_original_dir_name/$source_file_name.zip/Montana_Cadastral.gdb/a00000028.gdbtable|layername=OwnerParcel" $work_dir/$tmp_dir
		mv $work_dir/$tmp_dir/a00000028_fixed.gpkg $work_dir/$tmp_dir/${source_file_name}_fixed.gpkg
		time python3 $dir/add_file_name_field.py $work_dir/$tmp_dir/${source_file_name}_fixed.gpkg
		run_alg_dissolve $work_dir/$tmp_dir/${source_file_name}_fixed.gpkg OwnerName $work_dir/$source_dir_name
		generate_osm_private_lands $work_dir/$source_dir_name/${source_file_name}_fixed_dissolved.gpkg $state_name
		generate_obf us_${state_name}_pl_northamerica.pbf
	fi
fi

# Private lands New-Jersey
if [[ $process_private_lands_newjersey == true ]]; then
	state_name=new-jersey
	source_file_name=$private_lands_newjersey_source_filename
	if [[ ! -f "$work_dir/$out_osm_dir/us_{$state_name}_pl_northamerica.osm" ]]; then
		echo -e "\033[92m\e[44m Processing $source_file_name\e[49m\033[0m"
		if [[ ! -f $work_dir/$source_original_dir_name/$source_file_name.gdb.zip ]]; then
			wget "https://geoapps.nj.gov/njgin/parcel/parcelsStatewide.gdb.zip" -O $work_dir/$source_original_dir_name/$source_file_name.gdb.zip # https://njogis-newjersey.opendata.arcgis.com/documents/d543ddcc1e6844319ffa826fee52fccf/about
		fi
# 		run_alg_fixgeometries $work_dir/$source_original_dir_name/$source_file_name.gdb.zip $work_dir/$source_dir_name || exit 1
# 		generate_osm_private_lands $work_dir/$source_dir_name/${source_file_name}_fixed.gpkg $state_name || exit 1
		generate_obf us_${state_name}_pl_northamerica.pbf || exit 1
	fi
fi

# Private lands Massachusets
if [[ $process_private_lands_massachusets == true ]]; then
	state_name=massachusets
	source_file_name=$private_lands_massachusets_source_filename
	if [[ ! -f "$work_dir/$out_osm_dir/us_{$state_name}_pl_northamerica.osm" ]]; then
		echo -e "\033[92m\e[44m Processing $source_file_name\e[49m\033[0m"
		if [[ ! -f $work_dir/$source_original_dir_name/$source_file_name.zip ]]; then
			wget $url_massachusets -O $work_dir/$source_original_dir_name/$source_file_name.zip # https://www.mass.gov/info-details/massgis-data-property-tax-parcels (Download a SHP with all vintages for every municipality)
		fi
# 		mkdir -p $work_dir/$tmp_dir/$state_name
# 		unzip -f "$work_dir/$source_original_dir_name/$source_file_name.zip" -d $work_dir/$tmp_dir/$state_name/ > /dev/null
# 		find $work_dir/$tmp_dir/$state_name -mindepth 2 -type f -exec mv -i '{}' -n $work_dir/$tmp_dir/$state_name ';'
		time ogrmerge.py -single -overwrite_ds -f "GPKG" -o $work_dir/$source_dir_original_name/$source_file_name.gpkg $work_dir/$tmp_dir/$state_name/*.shp || exit 1
# 		run_alg_fixgeometries $work_dir/$source_original_dir_name/$source_file_name.gdb.zip $work_dir/$source_dir_name || exit 1
# 		generate_osm_private_lands $work_dir/$source_dir_name/${source_file_name}_fixed.gpkg $state_name || exit 1
# 		generate_obf us_${state_name}_pl_northamerica.pbf || exit 1
	fi
fi


cd $work_dir/$tmp_dir
if [[ $? == 0 ]] ; then
	rm -rf $work_dir/$tmp_dir/*.*
fi

cd $work_dir/$out_obf_dir
# bash $mapcreator_dir/utilities.sh generate-obf $work_dir/$out_osm_dir/us_blm_rec_area_activities.osm --poi-types=$poi_types_path --rendering-types=$rendering_types_path
# bash $mapcreator_dir/utilities.sh generate-obf $work_dir/$out_osm_dir/us_blm_roads_trails.osm --poi-types=$poi_types_path --rendering-types=$rendering_types_path
# bash $mapcreator_dir/utilities.sh generate-obf $work_dir/$out_osm_dir/us_nfs_trails.osm --poi-types=$poi_types_path --rendering-types=$rendering_types_path
# bash $mapcreator_dir/utilities.sh generate-obf $work_dir/$out_osm_dir/us_nfs_rec_area_activities.osm --poi-types=$poi_types_path --rendering-types=$rendering_types_path
# bash $mapcreator_dir/utilities.sh generate-obf $work_dir/$out_osm_dir/us_nfs_roads.pbf --poi-types=$poi_types_path --rendering-types=$rendering_types_path
# bash $mapcreator_dir/utilities.sh generate-obf $work_dir/$out_osm_dir/us_${private_lands_test_source_filename}_pl_northamerica.pbf --poi-types=$poi_types_path --rendering-types=$rendering_types_path
