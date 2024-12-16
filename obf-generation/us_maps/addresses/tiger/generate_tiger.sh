#!/bin/bash -xe

WORKING_DIR=tiger
OUT_FILE=$WORKING_DIR/tiger.pbf
OUT_FILE_O5M=$WORKING_DIR/tiger.o5m
SCRIPTS=tools/obf-generation/us_maps/addresses/tiger

source_dir=$WORKING_DIR/source # Downloaded archive will be extracted there
work_dir=$WORKING_DIR # Working directory

tiger_url="https://downloads.opencagedata.com/public/tiger2023-nominatim-preprocessed.csv.tar.gz"
tiger_gz_filename=tiger-nominatim-preprocessed-latest.csv.tar.gz

mkdir -p "$work_dir"
mkdir -p "$source_dir"

find $work_dir/ -maxdepth 1 -type f -name "*.osm" ! -name 'merged*.osm' -delete

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

if [[ ! -f "$work_dir/$tiger_gz_filename" ]]; then
	wget -nc -O $work_dir/$tiger_gz_filename $tiger_url
fi    
if (( $(ls $source_dir -1 | wc -l) < 3000 )); then
	tar -xzf $work_dir/$tiger_gz_filename -C $source_dir || exit 1
    mv $source_dir/tiger/*.csv $source_dir/
	rm -rf $source_dir/tiger/
fi
if [[ ! -f "$work_dir/merged.csv" ]]; then
	echo Merging CSV...
	merge_csv $source_dir $work_dir || exit 1
fi
if [[ ! -f "$work_dir/merged_replaced.csv" ]]; then
	echo Replacing abbreviations...
	python3 $SCRIPTS/replace_abbreviations.py $work_dir/merged.csv $work_dir/merged_replaced.csv $SCRIPTS/abbreviations.csv || exit 1
fi
rm $work_dir/merged.csv
rm -rf $source_dir

if [[ ! -f "$work_dir/merged.osm" ]]; then
	echo Converting CSV to OSM...
	time python3 $SCRIPTS/convert_to_osm.py $work_dir/merged_replaced.csv $work_dir/merged.osm || exit 1
fi
rm $work_dir/merged_replaced.csv

if [[ ! -f "$work_dir/merged_sorted.osm" ]]; then
	echo Sorting and renumbering OSM data...
	time osmium sort --overwrite -o $work_dir/merged_sorted.osm $work_dir/merged.osm || exit 1
fi
rm $work_dir/merged.osm

if [[ ! -f "$work_dir/merged.pbf" ]]; then
	time osmium cat --overwrite -o $work_dir/merged.pbf $work_dir/merged_sorted.osm || exit 1
fi
rm $work_dir/merged_sorted.osm

if [[ ! -f "$OUT_FILE" ]]; then
	DUMP=$(lynx -dump -nolist https://osmstats.neis-one.org/?item=elements | grep "Latest Node")
	NODE=$(echo "$DUMP" | grep -oP 'Node: \K[0-9]+')
	WAY=$(echo "$DUMP" | grep -oP 'Way: \K[0-9]+')
	RELATION=$(echo "$DUMP" | grep -oP 'Relation: \K[0-9]+')
	NODE_SHIFTED=$(($NODE<<1))
	WAY_SHIFTED=$(($WAY<<1))
	RELATION_SHIFTED=$(($RELATION<<1))
	time osmium renumber --overwrite --start-id=$NODE_SHIFTED,$WAY_SHIFTED,$RELATION_SHIFTED -o $OUT_FILE $work_dir/merged.pbf || exit 1
fi
rm $work_dir/merged.pbf

if [[ ! -f "$OUT_FILE_O5M" ]]; then
	osmconvert $OUT_FILE --out-o5m -o=$OUT_FILE_O5M
fi

osmium fileinfo --extended $OUT_FILE_O5M