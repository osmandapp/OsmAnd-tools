#!/bin/bash
set -x
work_dir=$(pwd)
autobahn_de_base_url=https://verkehr.autobahn.de/o/autobahn
autobahn_data_types=("roadworks" "webcam" "warning" "closure")

mkdir -p $work_dir/data/germany

rm -f $work_dir/data/germany/autobahn_list.geojson
wget -nc $autobahn_de_base_url -O $work_dir/data/germany/autobahn_list.geojson || exit 1
readarray -t autobahn < <(jq '.roads[]' $work_dir/data/germany/autobahn_list.geojson | sed 's/"//g' | sed 's/ //g')

for i in "${!autobahn_data_types[@]}"
do
	mkdir -p $work_dir/data/germany/${autobahn_data_types[$i]}
	rm -f $work_dir/data/germany/${autobahn_data_types[$i]}/*.geojson
	for road in "${!autobahn[@]}"
	do
		wget -nc $autobahn_de_base_url/${autobahn[$road]}/services/${autobahn_data_types[$i]} -O $work_dir/data/germany/${autobahn_data_types[$i]}/${autobahn_data_types[$i]}_${autobahn[$road]}.geojson
		sed -i "s/{\"${autobahn_data_types[$i]}\":/{\"type\": \"FeatureCollection\",\"features\":/g" $work_dir/data/germany/${autobahn_data_types[$i]}/${autobahn_data_types[$i]}_${autobahn[$road]}.geojson
		sed -i "s/,\"geometry\":{/,\"type\": \"Feature\",\"geometry\":{/g" $work_dir/data/germany/${autobahn_data_types[$i]}/${autobahn_data_types[$i]}_${autobahn[$road]}.geojson
	done
	rm -f $work_dir/data/germany/${autobahn_data_types[$i]}.geojson
	find . -type f -name "*.geojson" -size -100c -delete
	ogrmerge.py -single -overwrite_ds -f "GeoJSON" -o $work_dir/data/germany/${autobahn_data_types[$i]}.geojson $work_dir/data/germany/${autobahn_data_types[$i]}/*.geojson
done
