#!/bin/bash
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
  thread_number=2
fi
indir=$1
outdir=$2/
thread_number=$3
cd $outdir

export indir
export outdir

blur_tiff ()
{
	filenamefull=$(basename $1)
	filename=${filenamefull%%.*}
	if [ ! -f $outdir/$filename.tif ]; then
		echo "----------------------------------------------"
		echo "Processing "$1
		echo "----------------------------------------------"
		gdalwarp -ts 3602 3602 -r cubicspline -ot Int16 -co "COMPRESS=LZW" $indir/$filename.tif $outdir/$filename.tmp
		mv $outdir/$filename.tmp $outdir/$filename.tif
	else echo "Skipping "$1 "(already processed)"
	fi
}
export -f blur_tiff
find "$indir" -maxdepth 1 -type f -name "*.tif" | sort | parallel -P $3 --no-notice --bar blur_tiff '{}'
