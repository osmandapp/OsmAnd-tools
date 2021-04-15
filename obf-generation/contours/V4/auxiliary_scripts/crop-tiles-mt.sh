#!/bin/bash
# Script cuts tiff tiles by 1° x 1° to rounded borders with optional buffer. 17.99 -> 18, 38.01 -> 38 etc.

echo $1 $2 $3 $4
if [ $# -lt 2 ]; then
  echo "Error: 3 arguments needed"
  echo "Usage: "$(basename $0) "[input-dir] [output-directory] [number-of-threads] [input_extension:default=tif]"
  exit 2
fi
if [ ! -d $1 ]; then
  echo "input dir not found"
  exit 3
fi
if [ ! -d $2 ]; then
  echo "output directory not found"
  exit 3
fi
if [ ! $3 ]; then
  thread_number=2
fi
indir=$1
outdir=$2/
thread_number=$3

buffer=0.000139

if [ ! $4 ]; then
  ext_in="tif"
else ext_in=$4
fi

cd $outdir

export indir
export ext_in
export outdir
export buffer

resize ()
{
	filenamefull=$(basename $1)
	filename=${filenamefull%%.*}
	if [ ! -f $outdir/$filename.$ext_in ]; then
		echo "----------------------------------------------"
		echo "Processing "$1
		echo "----------------------------------------------"
		ul_string=$(gdalinfo $indir/$filename.$ext_in | grep 'Upper Left')
		ul_string=$(echo $ul_string | sed 's/(//g' | sed 's/)//g' | sed 's/,//g')
		lr_string=$(gdalinfo $indir/$filename.$ext_in | grep 'Lower Right')
		lr_string=$(echo $lr_string | sed 's/(//g' | sed 's/)//g' | sed 's/,//g')

		IFS=' ' read -ra array_upper_left <<< $ul_string
		IFS=' ' read -ra array_lower_right <<< $lr_string
		
		lon_min=$(LC_ALL=C /usr/bin/printf "%.*f\n" 0 ${array_upper_left[2]::7})
		lat_max=$(LC_ALL=C /usr/bin/printf "%.*f\n" 0 ${array_upper_left[3]::7})
		lon_max=$(LC_ALL=C /usr/bin/printf "%.*f\n" 0 ${array_lower_right[2]::7})
		lat_min=$(LC_ALL=C /usr/bin/printf "%.*f\n" 0 ${array_lower_right[3]::7})

		lon_min=$(bc <<< $lon_min-$buffer)
		lat_max=$(bc <<< $lat_max+$buffer)
		lon_max=$(bc <<< $lon_max+$buffer)
		lat_min=$(bc <<< $lat_min-$buffer)

		if (( $(echo "$lon_max > 180" |bc -l) )); then
			lon_max=179.999
		fi
		if (( $(echo "$lon_min < -180" |bc -l) )); then
			lon_min=-179.999
		fi
		if (( $(echo "$lat_max > 90" |bc -l) )); then
			lat_max=89.999
		fi
		if (( $(echo "$lat_min < -90" |bc -l) )); then
			lon_min=-89.999
		fi

		echo lon_min=$lon_min lat_max=$lat_max lon_max=$lon_max lat_min=$lat_min

		rm -f $outdir/$filename.geojson
		echo '{ "type": "FeatureCollection","name": "crop","crs": { "type": "name", "properties": { "name": "urn:ogc:def:crs:OGC:1.3:CRS84" } },
		"features": [ { "type": "Feature", "properties": { "properties": null }, "geometry": { "type": "MultiPolygon", "coordinates": [ [ [ [ {lon_min}, {lat_max} ], [ {lon_max}, {lat_max} ], [ {lon_max}, {lat_min} ], [ {lon_min}, {lat_min} ], [ {lon_min}, {lat_max} ] ] ] ] } } ] }' >> $outdir/$filename.geojson

		sed -i s/{lon_min}/$lon_min/g $outdir/$filename.geojson
		sed -i s/{lon_max}/$lon_max/g $outdir/$filename.geojson
		sed -i s/{lat_min}/$lat_min/g $outdir/$filename.geojson
		sed -i s/{lat_max}/$lat_max/g $outdir/$filename.geojson
		gdalwarp -of GTiff -ot Int16 -co "COMPRESS=LZW" -crop_to_cutline -cutline $outdir/$filename.geojson $indir/$filename.$ext_in $outdir/$filename.$ext_in
		rm -f $outdir/$filename.geojson
	else echo "Skipping "$1 "(already processed)"
	fi
}
export -f resize
find "$indir" -maxdepth 1 -type f -name "*.$ext_in" | sort | parallel -P $thread_number --no-notice --bar resize '{}'
