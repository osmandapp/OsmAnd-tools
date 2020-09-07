#!/bin/bash
# Merge tiles in indir1 over tiles in indir2 with parallel

indir1=/mnt/data/ArcticDEM/tiles_resized_2
indir2=/mnt/data/MERITDEM/tiles_resized
outdir=/mnt/intel240/arcticdem_merit_merged2
threads=8

export indir1
export indir2
export outdir

merge()
{
	filename=$(basename $1)
	if [[ ! -f "$outdir/$filename" ]] ; then
		echo $filename
		gdal_merge.py -of GTiff -co COMPRESS=LZW -co PREDICTOR=1 -o "$outdir/$filename" "$indir2/$filename" "$indir1/$filename"
	fi
}
export -f merge
find "$indir1" -maxdepth 1 -type f -name "*.tif" | sort | parallel -P $threads --no-notice --bar merge '{}'
