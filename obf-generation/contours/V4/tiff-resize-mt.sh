#!/bin/bash
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

if [ ! $4 ]; then
  ext_in="tif"
else ext_in=$4
fi

cd $outdir

export indir
export ext_in
export outdir

resize ()
{
	filenamefull=$(basename $1)
	filename=${filenamefull%%.*}
	if [ ! -f $outdir/$filename.$ext_in ]; then
		echo "----------------------------------------------"
		echo "Processing "$1
		echo "----------------------------------------------"
		gdalwarp -ts 3601 3601 -r cubicspline -of GTiff -ot Int16 -co "COMPRESS=LZW" -co "PREDICTOR=2" $indir/$filename.$ext_in $outdir/$filename.tmp
		mv $outdir/$filename.tmp $outdir/$filename.tif
	else echo "Skipping "$1 "(already processed)"
	fi
}
export -f resize
find "$indir" -maxdepth 1 -type f -name "*.$ext_in" | sort | parallel -P $thread_number --no-notice --bar resize '{}'
