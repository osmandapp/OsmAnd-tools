#!/bin/bash
# requires "parallel"
# usage: blur-mt.sh input-directory output-directory number-of-threads

TMP_DIR="/home/xmd5a/tmp/"

echo $1 $2 $3
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
  thread_number=$(( $(nproc) - 2))
fi
working_dir=$(pwd)
indir=$1
outdir=$2/
thread_number=$3
cd $outdir

export indir
export outdir
export TMP_DIR
export working_dir

blur_tiff ()
{
	filenamefull=$(basename $1)
	filename=${filenamefull%%.*}
	if [ ! -f $outdir$filename.tif ]; then
		echo "----------------------------------------------"
		echo "Processing "$1
		echo "----------------------------------------------"

		echo "Creating vrt … " ${TMP_DIR}$filename.vrt $indir/$filename.tif
		gdalbuildvrt -r cubic -srcnodata -9999 ${TMP_DIR}$filename.vrt $indir/$filename.tif
		if [ $? -ne 0 ]; then echo $(date)' Error creating vrt' & exit 5;fi

		echo "Blurring …"
		python3 ${working_dir}/raster_chunk_processing.py -p 1 -m blur_gauss -o 25 -s 1500 --verbose -r 15 -d 2 ${TMP_DIR}$filename.vrt $outdir$filename.tif
		if [ $? -ne 0 ]; then echo $(date)' Error blurring' & exit 6;fi
		rm ${TMP_DIR}$filename.vrt
		gdalwarp -ts 3602 3602 -ot Int16 -of GTiff -co "COMPRESS=LZW" $outdir$filename.tif $outdir$filename.tmp
		rm $outdir$filename.tif
		mv $outdir$filename.tmp $outdir$filename.tif
	else echo "Skipping "$1 "(already processed)"
	fi
}
export -f blur_tiff
find "$indir" -maxdepth 1 -type f -name "*.tif" | sort | parallel -P $thread_number --no-notice --bar time blur_tiff '{}'
