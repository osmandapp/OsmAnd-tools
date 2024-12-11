#!/bin/bash
# This script downloads and converts TIGER data to pbf with splitting by state

source_dir=/mnt/wd_2tb/us_addresses/tiger # Downloaded archive will be extracted there
work_dir=/mnt/wd_2tb/us_addresses/tiger_work # Working directory
output_dir=/mnt/wd_2tb/us_addresses/tiger_output # Output directory
poly_dir=/home/xmd5a/git/OsmAnd-misc/osm-planet/polygons/north-america/us # Polygon directory

tiger_url="https://downloads.opencagedata.com/public/tiger2023-nominatim-preprocessed.csv.tar.gz"
tiger_gz_filename=tiger-nominatim-preprocessed-latest.csv.tar.gz
# output_with_cities=/mnt/wd_2tb/us_addresses/tiger_output_cities
# output_obf_dir=/mnt/wd_2tb/us_addresses/tiger_obf
# map_creator_dir=/home/xmd5a/utilites/OsmAndMapCreator-main/

mkdir -p "$work_dir"
mkdir -p "$source_dir"
mkdir -p "$output_dir"
# mkdir -p "$output_with_cities"
# mkdir -p "$output_obf_dir"

function merge_csv {
	mkdir -p "$1/tmp"
	output_file=$1"/tmp/merged.csv"
	> "$output_file"
	first_file=true
	for file in "$1"/*.csv; do
	  if [ -f "$file" ]; then
	    if [ "$first_file" = true ]; then
	      cat "$file" >> "$output_file"
	      first_file=false
	    else
	      tail -n +2 "$file" >> "$output_file"
	    fi
	  fi
	done
	mv $output_file $2
	rmdir "$1/tmp"
}

find $work_dir/ -maxdepth 1 -type f -name "*.osm" ! -name 'merged*.osm' -delete

wget -nc -O $work_dir/$tiger_gz_filename $tiger_url
if (( $(ls $source_dir -1 | wc -l) < 3000 )); then
	tar -xzf $work_dir/$tiger_gz_filename -C $source_dir/.. || exit 1
fi
if [[ ! -f "$work_dir/merged.csv" ]]; then
	echo Merging CSV...
	merge_csv $source_dir $work_dir || exit 1
fi
if [[ ! -f "$work_dir/merged_replaced.csv" ]]; then
	echo Replacing abbreviations...
	python3 replace_abbreviations.py $work_dir/merged.csv $work_dir/merged_replaced.csv abbreviations.csv || exit 1
fi
if [[ ! -f "$work_dir/merged.osm" ]]; then
	echo Converting CSV to OSM...
	time python3 convert_to_osm.py $work_dir/merged_replaced.csv $work_dir/merged.osm || exit 1
fi
if [[ ! -f "$work_dir/merged_sorted.osm" ]]; then
	echo Sorting and renumbering OSM data...
	time osmium sort --overwrite -o $work_dir/merged_sorted.osm $work_dir/merged.osm || exit 1
fi
if [[ ! -f "$work_dir/merged.pbf" ]]; then
	time osmium cat --overwrite -o $work_dir/merged.pbf $work_dir/merged_sorted.osm || exit 1
fi
if [[ ! -f "$work_dir/merged_renumbered.pbf" ]]; then
	time osmium renumber --overwrite -o $work_dir/merged_renumbered.pbf $work_dir/merged.pbf || exit 1
fi

echo Splitting by polygons...
for poly in $poly_dir/*.poly; do
	file="${poly##*/}"
	file="${file%.*}"
	echo Processing $file ...
	if [[ ! -f $output_dir/$file.pbf ]]; then
		time osmconvert -B=$poly $work_dir/merged_renumbered.pbf --out-pbf > $output_dir/$file.pbf || exit 1
# 		bash get_places_from_overpass.sh $output_dir/$file.pbf "$work_dir/$file.osm"
# 		osmconvert -B=$poly "$work_dir/$file.osm" --out-osm > "$work_dir/${file}_crop.osm"
# 		osmium sort -o "$work_dir/${file}_sorted.osm" "$work_dir/${file}_crop.osm"
# 		osmium cat --overwrite "$work_dir/${file}_sorted.osm" $output_dir/$file.pbf -o $output_with_cities/$file.pbf
# 		cd $output_obf_dir
# 		bash $map_creator_dir/utilities.sh generate-obf $output_with_cities/$file.pbf
# 		bash $map_creator_dir/inspector.sh -c $output_obf_dir/${file}_addr.obf $output_obf_dir/"${file^}".obf +3
# 		cd $work_dir/..
	else
		echo "Already exists. Skipping."
	fi
done
