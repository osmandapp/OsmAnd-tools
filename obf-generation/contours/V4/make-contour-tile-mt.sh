#!/bin/bash
# Multithreaded tiff tile to contour (osm) converter for OsmAnd
# requires "parallel"
# usage make-contour-tile-mt.sh input-directory output-directory number-of-threads

# directory for python tools
TOOL_DIR="."
TMP_DIR="/var/tmp/"

if [ $# -lt 2 ]; then
  echo "Error: 3 arguments needed"
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
  thread_number=1
  exit 3
fi
working_dir=$(pwd)
indir=$1
outdir=$2/
thread_number=$3
cd $outdir

export outdir
export TMP_DIR
export working_dir

process_tiff ()
{
	filenamefull=$(basename $1)
	filename=${filenamefull%%.*}
	if [ ! -f $outdir$filename.osm.bz2 ]; then
		echo "----------------------------------------------"
		echo "Processing "$1
		echo "----------------------------------------------"
		echo "Extracting shapefile …"
		if [ -f ${TMP_DIR}$filename.shp ]; then rm ${TMP_DIR}$filename.shp ${TMP_DIR}$filename.dbf ${TMP_DIR}$filename.prj ${TMP_DIR}$filename.shx; fi
		gdal_contour -i 10 -a height $1 ${TMP_DIR}$filename.shp
		if [ $? -ne 0 ]; then echo $(date)' Error creating shapefile' & exit 4;fi
	
		echo "Building osm file …"
		time ${working_dir}/ogr2osm.py ${TMP_DIR}$filename.shp -o $outdir$filename.osm -e 4326 -t contours.py
		if [ $? -ne 0 ]; then echo $(date)' Error creating OSM file' & exit 5;fi
	
		echo "Compressing to osm.bz2 …"
		bzip2 -f $outdir$filename.osm
		if [ $? -ne 0 ]; then echo $(date)' Error compressing OSM file' & exit 6;fi
		if [ -f ${TMP_DIR}$filename.shp ]; then rm ${TMP_DIR}$filename.shp ${TMP_DIR}$filename.dbf ${TMP_DIR}$filename.prj ${TMP_DIR}$filename.shx; fi
	else echo "Skipping "$1 "(already processed)"
	fi
}
export -f process_tiff
find "$indir" -maxdepth 1 -type f -name "*.tif" | sort | parallel -P $3 --no-notice --bar process_tiff '{}'
