#!/bin/bash
echo $1 $2
if [ $# -lt 2 ]; then
  echo "Error: 1 argument needed"
  echo "Usage: "$(basename $0) "[input-dir] [number-of-threads]"
  exit 2
fi
if [ ! -d $1 ]; then
  echo "input dir not found"
  exit 3
fi
if [ ! $2 ]; then
  thread_number=2
fi
indir=$1
thread_number=$2

export indir

resize ()
{
	filenamefull=$(basename $1)
	filename=${filenamefull%%.*}
	if [ ! -f $outdir/$filename.osm.bz2 ]; then
		bzcat $indir/$filename.osm.bz2 | grep /osm > /dev/null
		if [[ $? -eq 1 ]]; then
			echo ERROR! $filename is incomplete. Removing.
			rm -f $indir/$filename.osm.bz2
		fi
#		mv $outdir/$filename.tmp $outdir/$filename.tif
	else echo "Skipping "$1 "(already processed)"
	fi
}
export -f resize
find "$indir" -maxdepth 1 -type f -name "*.osm.bz2" | sort | parallel -P $thread_number --no-notice --bar resize '{}'
