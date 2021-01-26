#!/bin/bash
# Multithreaded tiff tile to contour (osm) converter for OsmAnd
# requires "parallel", "lbzip2" and a lot of RAM
# optional: -p option requires qgis
# usage: make-contour-tile-mt.sh input-directory output-directory smooth(true/false)

# Load balancing depending on tiff size
# 128Gb RAM 32 threads:
threads_number_1=7 # >19M  Max RAM per process: ~30 Gb
threads_number_2=16 # 13M-20M  Max RAM per process: ~10 Gb
threads_number_3=30 # <14M  Max RAM per process: ~5 Gb

TMP_DIR="/var/tmp"

function usage {
        echo "Usage: ./make-contour-tile-mt.sh -i [input-dir] -o [output-directory] { -s [true/false] -p [true/false] }"
	echo "-s: smooth raster before processing. Downscale/upscale is applied for lat>65 tiles."
	echo "-p: split lines by lenth"
}

while getopts ":i:o:s:p" opt; do
  case $opt in
    i) indir="$OPTARG"
    ;;
    o) outdir="$OPTARG"
    ;;
    s) smooth="$OPTARG"
    ;;
    p) split_lines=true
    ;;
    \?) echo -e "\033[91mInvalid option -$OPTARG\033[0m" >&2
	usage
    ;;
  esac
done

if [[ $smooth != "false" ]] || [[ -z $smooth ]] ; then
	smooth=true
fi
if [[ -z $indir ]] ; then
	echo "Input dir not found"
	usage
	exit 1
fi
if [[ -z $outdir ]] ; then
	echo "Output dir not found"
	usage
	exit 1
fi
if [ ! -d $indir ]; then
	echo "input dir not found"
	exit 3
fi
if [ ! -d $outdir ]; then
	echo "output directory not found"
	exit 3
fi

echo "Input dir:" $indir
echo "Output dir:" $outdir
echo "smooth:" $smooth
echo "split_lines:" $split_lines

working_dir=$(pwd)
# thread_number=$3
cd $outdir

export outdir
export TMP_DIR
export working_dir
export smooth
export split_lines

process_tiff ()
{
	filenamefull=$(basename $1)
	filename=${filenamefull%%.*}
	if [ ! -f $outdir/$filename.osm.bz2 ]; then
		echo "----------------------------------------------"
		echo "Processing "$1
		echo "----------------------------------------------"
		src_tiff=$1
		lat=${filename:1:2}
		smoothed_path=${1%/*}/${filename}_smooth.tif
		if [[ $smooth == "true" ]] ; then
			echo "Smoothing raster…"
			if [[ $((10#$lat)) -ge 65 ]] ; then
				size_str=$(gdalinfo $1 | grep "Size is" | sed 's/Size is //g')
				width=$(echo $size_str | sed 's/,.*//')
				height=$(echo $size_str | sed 's/.*,//')
				width_mod=$(( $width / 2))
				height_mod=$(( $height / 2))
				width_mod_2=$(( $width ))
				height_mod_2=$(( $height ))
				gdalwarp -overwrite -ts $width_mod $height_mod -r cubicspline -co "COMPRESS=LZW" -ot Float32 -wo NUM_THREADS=4 -multi $1 $smoothed_path
				gdalwarp -overwrite -ts $width_mod_2 $height_mod_2 -of GTiff -r cubicspline -co "COMPRESS=LZW" -ot Float32 -wo NUM_THREADS=4 -multi $smoothed_path ${smoothed_path}_2
				rm -f $smoothed_path && mv ${smoothed_path}_2 $smoothed_path
			else
				gdalwarp -overwrite -r cubicspline -co "COMPRESS=LZW" -ot Float32 -wo NUM_THREADS=4 -multi $1 $smoothed_path
			fi
			src_tiff=$smoothed_path
		fi
		echo "Extracting shapefile …"
		if [ -f ${TMP_DIR}/$filename.shp ]; then rm ${TMP_DIR}/$filename.shp ${TMP_DIR}/$filename.dbf ${TMP_DIR}/$filename.prj ${TMP_DIR}/$filename.shx; fi
		gdal_contour -i 10 -a height $src_tiff ${TMP_DIR}/$filename.shp
		if [ $? -ne 0 ]; then echo $(date)' Error creating shapefile' & exit 4;fi
		if [[ -f $smoothed_path ]] ; then
			rm -f $smoothed_path
		fi
		if [[ $split_lines == "true" ]] ; then
			echo "Splitting lines by length …"
			time python3 $working_dir/run_alg.py -alg "native:splitlinesbylength" -param1 INPUT -value1 ${TMP_DIR}/$filename.shp -param2 LENGTH -value2 0.05 -param3 OUTPUT -value3 ${TMP_DIR}/${filename}_split.shp
			if [ -f ${TMP_DIR}/$filename.shp ]; then rm ${TMP_DIR}/$filename.shp ${TMP_DIR}/$filename.dbf ${TMP_DIR}/$filename.prj ${TMP_DIR}/$filename.shx; fi
			if [ -f ${TMP_DIR}/${filename}_split.shp ]; then
				mv ${TMP_DIR}/${filename}_split.shp ${TMP_DIR}/$filename.shp
				mv ${TMP_DIR}/${filename}_split.dbf ${TMP_DIR}/$filename.dbf
				mv ${TMP_DIR}/${filename}_split.prj ${TMP_DIR}/$filename.prj
				mv ${TMP_DIR}/${filename}_split.shx ${TMP_DIR}/$filename.shx
			fi
		fi
		echo "Building osm file …"
		if [[ -f $outdir/$filename.osm ]] ; then rm -f $outdir/$filename.osm ; fi
		${working_dir}/ogr2osm.py ${TMP_DIR}/$filename.shp -o $outdir/$filename.osm -e 4326 -t contours.py
		if [ $? -ne 0 ]; then echo $(date)' Error creating OSM file' & exit 5;fi
	
		echo "Compressing to osm.bz2 …"
		lbzip2 -f $outdir/$filename.osm
		if [ $? -ne 0 ]; then echo $(date)' Error compressing OSM file' & exit 6;fi
		if [ -f ${TMP_DIR}/$filename.shp ]; then rm ${TMP_DIR}/$filename.shp ${TMP_DIR}/$filename.dbf ${TMP_DIR}/$filename.prj ${TMP_DIR}/$filename.shx; fi
	else echo "Skipping "$1 "(already processed)"
	fi
}
export -f process_tiff
#find "$indir" -maxdepth 1 -type f -name "*.tif" | sort -R | parallel -P 5 --no-notice --bar time process_tiff '{}'
find "$indir" -maxdepth 1 -type f -name "S*.tif" -size +19M | sort -R | parallel -P $threads_number_1 --no-notice --bar time process_tiff '{}'
find "$indir" -maxdepth 1 -type f -name "S*.tif" -size +13M -size -20M | sort -R | parallel -P $threads_number_2 --no-notice --bar time process_tiff '{}'
find "$indir" -maxdepth 1 -type f -name "S*.tif" -size -14M | sort -R | parallel -P $threads_number_3 --no-notice --bar time process_tiff '{}'
rm -rf $outdir/processing