#!/bin/bash -e
# Multithreaded tiff tile to contour (osm) converter for OsmAnd
# requires "gdal", "parallel", "lbzip2" and a lot of RAM
# optional: -p and -d options requires qgis
# no_smoothing.ini file contains array of tile names to which smoothing will not be applied

# Load balancing depending on tiff size
# For 128Gb RAM and 32 threads machine:
threads_number_0=1 # >100M
threads_number_1=17 # >19M  Max RAM per process without simplifying: ~30 Gb
threads_number_2=30 # 13M-20M  Max RAM per process without simplifying: ~10 Gb
threads_number_3=30 # <14M  Max RAM per process without simplifying: ~5 Gb

export QT_LOGGING_RULES="qt5ct.debug=false"
TMP_DIR="/mnt/wd_2tb/tmp"
isolines_step=10
translation_script=contours.py
function usage {
        echo "Usage: ./make-contour-tile-mt.sh -i [input-dir] -o [output-directory] { -s -p -d -f -t [threads number]}"
	echo "Recommended usage: ./make-contour-tile-mt.sh -i [input-dir] -o [output-directory] -spd -t 1"
	echo "-s: smooth raster before processing. Downscale/upscale is applied for lat>65 tiles."
	echo "-p: split lines by lenth"
	echo "-d: slightly simplify contours with Douglas-Pecker algorithm to reduce file size in half"
	echo "-t: threads number"
	echo "-f: make contours in feet"
	echo "-c: path to cutline in shp format"
}

date
while getopts ":i:o:spdt:fc:" opt; do
  case $opt in
    i) indir="$OPTARG"
    ;;
    o) outdir="$OPTARG"
    ;;
    s) smooth=true
    ;;
    p) split_lines=true
    ;;
    f) make_feet=true
    ;;
    d) simplify=true
    ;;
    t) threads_number_1="$OPTARG"
       threads_number_2="$OPTARG"
       threads_number_3="$OPTARG"
       threads_number_is_set=true
    ;;
    c) path_to_cutline="$OPTARG"
    ;;
    \?) echo -e "\033[91mInvalid option -$OPTARG\033[0m" >&2
	usage
    ;;
  esac
done
if [[ $make_feet == "true" ]] ; then
	isolines_step=40
	translation_script=contours_feet.py
else
	make_feet=false
fi
if [[ $split_lines != "true" ]] ; then
	split_lines=false
fi
if [[ $simplify != "true" ]] ; then
	simplify=false
fi
if [[ $smooth != "true" ]]; then
	smooth=false
fi
if [[ -z $indir ]] ; then
	echo "input dir is not defined"
	usage
	exit 1
fi
if [[ -z $outdir ]] ; then
	echo "output dir is not defined"
	usage
	exit 1
fi
if [ ! -d $indir ]; then
	echo "input dir is not found"
	exit 3
fi
if [ ! -d $outdir ]; then
	echo "output dir is not found"
	exit 3
fi
if [ ! -f $path_to_cutline ]; then
	echo "path to cutline is not found"
	exit 3
fi

echo -e "\e[104minput dir: $indir\e[49m"
echo -e "\e[104moutput dir: $outdir\e[49m"
echo -e "\e[104msmooth: $smooth\e[49m"
echo -e "\e[104msimplify: $simplify\e[49m"
echo -e "\e[104msplit_lines: $split_lines\e[49m"
echo -e "\e[104mmake_feet: $make_feet\e[49m"
echo -e "\e[104misolines_step: $isolines_step\e[49m"
echo -e "\e[104mpath_to_cutline: $path_to_cutline\e[49m"
if [[ $threads_number_is_set ]]; then
	echo -e "\e[104mthreads number: $threads_number_1\e[49m"
fi

working_dir=$(pwd)
cd $outdir

export outdir
export TMP_DIR
export working_dir
export smooth
export simplify
export split_lines
export make_feet
export isolines_step
export translation_script
export path_to_cutline

process_tiff ()
{
	. $working_dir/no_smoothing.ini
	filenamefull=$(basename $1)
	filename=${filenamefull%%.*}
	indir=${1%/*}
	highres_dir=${indir%/*}/$(basename $indir)_highres
	filepath=$1
	if [ -f $highres_dir/$filenamefull ] ; then
		filepath=$highres_dir/$filenamefull
		echo "Highres tile is found"
		echo Using $filepath
	fi
	no_smooth=false
	if [ ! -f $outdir/$filename.osm.bz2 ]; then
		echo "----------------------------------------------"
		echo "Processing "$filename
		echo "----------------------------------------------"
		size_str=$(gdalinfo $filepath | grep "Size is" | sed 's/Size is //g')
		width=$(echo $size_str | sed 's/,.*//')
		height=$(echo $size_str | sed 's/.*,//')
		src_tiff=$filepath
		if [[ $width -ge 30000 ]] ; then
			isolines_step=5
		fi
		if [[ $make_feet == "true" ]] ; then
			if [[ $width -ge 30000 ]] ; then
				isolines_step=20
			fi
			gdal_calc.py -A $filepath --outfile=${TMP_DIR}/$filename.tif --calc="A/0.3048"
			src_tiff=${TMP_DIR}/$filename.tif
		fi
		echo "Using isolines_step="$isolines_step
		lat=${filename:1:2}
		smoothed_path=${TMP_DIR}/${filename}_smooth.tif
		if [[ $smooth == "true" ]] ; then
			for i in ${no_smoothing_array[@]}; do
				if [[ $i == $filename ]] ; then
					echo "No smoothing was applied"
					no_smooth=true
				fi
			done
			if [[ $no_smooth == "false" ]] ; then
				echo "Smoothing raster…"
				if [[ $((10#$lat)) -ge 65 ]] ; then
					width_mod=$(( $width / 2))
					height_mod=$(( $height / 2))
					width_mod_2=$(( $width ))
					height_mod_2=$(( $height ))
					gdalwarp -overwrite -ts $width_mod $height_mod -r cubicspline -co "COMPRESS=LZW" -ot Float32 -wo NUM_THREADS=4 -multi $src_tiff $smoothed_path
					gdalwarp -overwrite -ts $width_mod_2 $height_mod_2 -of GTiff -r cubicspline -co "COMPRESS=LZW" -ot Float32 -wo NUM_THREADS=4 -multi $smoothed_path ${smoothed_path}_2
					rm -f $smoothed_path && mv ${smoothed_path}_2 $smoothed_path
				else
					gdalwarp -overwrite -r cubicspline -co "COMPRESS=LZW" -ot Float32 -wo NUM_THREADS=4 -multi $src_tiff $smoothed_path
				fi
				src_tiff=$smoothed_path
			fi
		fi
		echo "Extracting shapefile …"
		if [ -f ${TMP_DIR}/$filename.shp ]; then rm ${TMP_DIR}/$filename.shp ${TMP_DIR}/$filename.dbf ${TMP_DIR}/$filename.prj ${TMP_DIR}/$filename.shx; fi
		gdal_contour -i $isolines_step -a height $src_tiff ${TMP_DIR}/$filename.shp
		if [ $? -ne 0 ]; then echo $(date)' Error creating shapefile' & exit 4;fi
		if [[ -f $smoothed_path ]] ; then
			rm -f $smoothed_path
		fi
		if [[ $simplify == "true" ]] ; then
			echo "Simplifying lines with Douglas-Pecker algorithm …"
			time python3 $working_dir/run_alg.py -alg "native:simplifygeometries" -param1 INPUT -value1 ${TMP_DIR}/$filename.shp -param2 METHOD -value2 0 -param3 TOLERANCE -value3 1e-05 -param4 OUTPUT -value4 ${TMP_DIR}/${filename}_simplified.shp
			if [ -f ${TMP_DIR}/$filename.shp ]; then rm ${TMP_DIR}/$filename.shp ${TMP_DIR}/$filename.dbf ${TMP_DIR}/$filename.prj ${TMP_DIR}/$filename.shx; fi
			if [ -f ${TMP_DIR}/${filename}_simplified.shp ]; then
				mv ${TMP_DIR}/${filename}_simplified.shp ${TMP_DIR}/$filename.shp
				mv ${TMP_DIR}/${filename}_simplified.dbf ${TMP_DIR}/$filename.dbf
				mv ${TMP_DIR}/${filename}_simplified.prj ${TMP_DIR}/$filename.prj
				mv ${TMP_DIR}/${filename}_simplified.shx ${TMP_DIR}/$filename.shx
			fi
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
		if [[ $path_to_cutline ]] ; then
			echo "Cropping by cutline …"
			time python3 $working_dir/run_alg.py -alg "native:clip" -param1 INPUT -value1 ${TMP_DIR}/$filename.shp -param2 OVERLAY -value2 $path_to_cutline -param3 OUTPUT -value3 ${TMP_DIR}/${filename}_cut.shp
# 			time ogr2ogr ${TMP_DIR}/${filename}_cut.shp ${TMP_DIR}/$filename.shp -clipsrc $path_to_cutline
			if [ -f ${TMP_DIR}/$filename.shp ]; then rm ${TMP_DIR}/$filename.shp ${TMP_DIR}/$filename.dbf ${TMP_DIR}/$filename.prj ${TMP_DIR}/$filename.shx; fi
			if [ -f ${TMP_DIR}/${filename}_cut.shp ]; then
				mv ${TMP_DIR}/${filename}_cut.shp ${TMP_DIR}/$filename.shp
				mv ${TMP_DIR}/${filename}_cut.dbf ${TMP_DIR}/$filename.dbf
				mv ${TMP_DIR}/${filename}_cut.prj ${TMP_DIR}/$filename.prj
				mv ${TMP_DIR}/${filename}_cut.shx ${TMP_DIR}/$filename.shx
			fi

		fi
		echo "Building osm file …"
		if [[ -f $outdir/$filename.osm ]] ; then rm -f $outdir/$filename.osm ; fi
		if [[ -f ${TMP_DIR}/$filename.tif ]] ; then rm -f ${TMP_DIR}/$filename.tif ; fi
		${working_dir}/ogr2osm.py ${TMP_DIR}/$filename.shp -o $outdir/$filename.osm -e 4326 -t $translation_script
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
find "$indir" -maxdepth 1 -type f -name "*.tif" -size +100M | sort -R | parallel -P $threads_number_0 --no-notice --bar time process_tiff '{}'
find "$indir" -maxdepth 1 -type f -name "*.tif" -size +19M | sort -R | parallel -P $threads_number_1 --no-notice --bar time process_tiff '{}'
find "$indir" -maxdepth 1 -type f -name "*.tif" -size +13M -size -20M | sort -R | parallel -P $threads_number_2 --no-notice --bar time process_tiff '{}'
find "$indir" -maxdepth 1 -type f -name "*.tif" -size -14M | sort -R | parallel -P $threads_number_3 --no-notice --bar time process_tiff '{}'
rm -rf $outdir/processing
date