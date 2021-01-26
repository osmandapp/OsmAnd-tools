#!/bin/bash
# Multithreaded tiff smooth uisng downscale and upscale by GDAL
# requires "parallel", "gdal"
# usage: smooth-gdalwarp-mt.sh input-directory output-directory number-of-threads

if [ $# -lt 2 ]; then
  echo "Error: at least 2 arguments needed"
  echo "Usage: "$(basename $0) "[input-dir] [output-directory] [number-of-threads]"
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
  thread_number=$(( $(nproc) - 2 ))
fi
working_dir=$(pwd)
indir=$1
outdir=$2/
thread_number=$3
cd $outdir

export outdir
export working_dir

process_tiff ()
{
	filenamefull=$(basename $1)
	filename=${filenamefull%%.*}
	if [ ! -f $outdir$filenamefull ]; then
		echo "----------------------------------------------"
		echo "Processing "$1
		echo "----------------------------------------------"
		size_str=$(gdalinfo $1 | grep "Size is" | sed 's/Size is //g')
		width=$(echo $size_str | sed 's/,.*//')
		height=$(echo $size_str | sed 's/.*,//')
		width_mod=$(( $width * 2 ))
		height_mod=$(( $height * 2 ))
		width_mod_2=$(( $width ))
		height_mod_2=$(( $height ))
		smoothed_path=$outdir${filename}_scaled.tif
		gdalwarp -overwrite -ts $width_mod $height_mod -r cubicspline -co "COMPRESS=LZW" -ot Float32 -wo NUM_THREADS=4 -multi $1 $smoothed_path
		gdalwarp -overwrite -ts $width_mod_2 $height_mod_2 -of GTiff -r cubicspline -co "COMPRESS=LZW" -ot Float32 -wo NUM_THREADS=4 -multi $smoothed_path ${smoothed_path}_2
		rm -f $smoothed_path && mv ${smoothed_path}_2 $outdir$filenamefull
	else echo "Skipping "$1 "(already processed)"
	fi
}
export -f process_tiff
find "$indir" -maxdepth 1 -type f -name "*.tif" | sort | parallel -P $3 --no-notice --bar time process_tiff '{}'
