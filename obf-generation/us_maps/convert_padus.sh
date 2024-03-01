#!/bin/bash
# This script it intended to prepare PAD-US for use in OsmAnd
# Data download link: https://www.usgs.gov/programs/gap-analysis-project/science/pad-us-data-download
# shp files are required

#set -x
dir=$(pwd)
work_dir=/mnt/wd_2tb/padus
shape_dir=shp
tmp_dir=tmp
out_osm_dir=osm
out_obf_dir=obf
ogr2osm_dir=ogr2osm
mapcreator_dir=/home/xmd5a/utilites/OsmAndMapCreator-main
poi_types_path=/home/xmd5a/git/OsmAnd-resources/poi/poi_types_us-maps.xml
rendering_types_path=/home/xmd5a/git/OsmAnd-resources/obf_creation/rendering_types_us-maps.xml


export shape_dir
export tmp_dir
export work_dir
export out_osm_dir
export ogr2osm_dir
export dir

function rename_padus {
	state_code=${1#*PADUS3_0Combined_State}
	prefix="Us"
	suffix="padus_northamerica"
	case $state_code in
		"AL") padus_name=${prefix}_alabama_${suffix};;
		"AK") padus_name=${prefix}_alaska_${suffix};;
		"AR") padus_name=${prefix}_arkansas_${suffix};;
		"AS") padus_name=${prefix}_american-samoa_${suffix};;
		"AZ") padus_name=${prefix}_arizona_${suffix};;
		"CA") padus_name=${prefix}_california_${suffix};;
		"CO") padus_name=${prefix}_colorado_${suffix};;
		"CT") padus_name=${prefix}_connecticut_${suffix};;
		"DE") padus_name=${prefix}_delaware_${suffix};;
		"DC") padus_name=${prefix}_district-of-columbia_${suffix};;
		"FL") padus_name=${prefix}_florida_${suffix};;
		"GA") padus_name=${prefix}_georgia_${suffix};;
		"GU") padus_name=${prefix}_guam_${suffix};;
		"HI") padus_name=${prefix}_hawaii_${suffix};;
		"ID") padus_name=${prefix}_idaho_${suffix};;
		"IL") padus_name=${prefix}_illinois_${suffix};;
		"IN") padus_name=${prefix}_indiana_${suffix};;
		"IA") padus_name=${prefix}_iowa_${suffix};;
		"KS") padus_name=${prefix}_kansas_${suffix};;
		"KY") padus_name=${prefix}_kentucky_${suffix};;
		"LA") padus_name=${prefix}_louisiana_${suffix};;
		"ME") padus_name=${prefix}_maine_${suffix};;
		"MD") padus_name=${prefix}_maryland_${suffix};;
		"MA") padus_name=${prefix}_massachusetts_${suffix};;
		"MI") padus_name=${prefix}_michigan_${suffix};;
		"MN") padus_name=${prefix}_minnesota_${suffix};;
		"MS") padus_name=${prefix}_mississippi_${suffix};;
		"MO") padus_name=${prefix}_missouri_${suffix};;
		"MP") padus_name=${prefix}_northern_mariana_islands_${suffix};;
		"MT") padus_name=${prefix}_montana_${suffix};;
		"NE") padus_name=${prefix}_nebraska_${suffix};;
		"NV") padus_name=${prefix}_nevada_${suffix};;
		"NH") padus_name=${prefix}_new-hampshire_${suffix};;
		"NJ") padus_name=${prefix}_new-jersey_${suffix};;
		"NM") padus_name=${prefix}_new-mexico_${suffix};;
		"NY") padus_name=${prefix}_new-york_${suffix};;
		"NC") padus_name=${prefix}_north-carolina_${suffix};;
		"ND") padus_name=${prefix}_north-dakota_${suffix};;
		"OH") padus_name=${prefix}_ohio_${suffix};;
		"OK") padus_name=${prefix}_oklahoma_${suffix};;
		"OR") padus_name=${prefix}_oregon_${suffix};;
		"PA") padus_name=${prefix}_pennsylvania_${suffix};;
		"PR") padus_name=${prefix}_puerto-rico_${suffix};;
		"RI") padus_name=${prefix}_rhode-island_${suffix};;
		"SC") padus_name=${prefix}_south-carolina_${suffix};;
		"SD") padus_name=${prefix}_south-dakota_${suffix};;
		"TN") padus_name=${prefix}_tennessee_${suffix};;
		"TX") padus_name=${prefix}_texas_${suffix};;
		"UT") padus_name=${prefix}_utah_${suffix};;
		"VT") padus_name=${prefix}_vermont_${suffix};;
		"VI") padus_name=${prefix}_virgin-islands_${suffix};;
		"VA") padus_name=${prefix}_virginia_${suffix};;
		"WA") padus_name=${prefix}_washington_${suffix};;
		"WV") padus_name=${prefix}_west-virginia_${suffix};;
		"WI") padus_name=${prefix}_wisconsin_${suffix};;
		"WY") padus_name=${prefix}_wyoming_${suffix};;
		* ) echo "Translation for" $state_code "not found"
			padus_name=$1 ;;
	esac
	echo $padus_name
}

function generate_osm {
	padus_name=$(rename_padus $1)
 	echo $padus_name
	ogr2ogr $work_dir/$tmp_dir/$padus_name.shp $work_dir/$shape_dir/$1.shp -explodecollections
	cd $dir/$ogr2osm_dir
	python3 -m ogr2osm $work_dir/$tmp_dir/$padus_name.shp -o $work_dir/$out_osm_dir/$padus_name.osm -t padus.py
}

shopt -s nullglob
for f in $work_dir/$shape_dir/PADUS3_0Combined_State*.shp
do
	base_name=$(basename "${f%.*}")
	padus_name=$(rename_padus $base_name)
	if [ ! -f "$work_dir/$out_osm_dir/$padus_name.osm" ]
	then
		echo ==============Generating $base_name
		generate_osm $base_name
	fi
	if [ ! -f "$work_dir/$out_obf_dir/$padus_name.obf" ]
	then
		cd $work_dir/$out_obf_dir
		bash $mapcreator_dir/utilities.sh generate-obf $work_dir/$out_osm_dir/$padus_name.osm --poi-types=$poi_types_path --rendering-types=$rendering_types_path
	fi
done
