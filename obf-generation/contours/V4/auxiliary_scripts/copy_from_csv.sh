#!/bin/bash
# Auxiliary script for copy_selected_tiles.py
indir=/mnt/data/ArcticDEM/tiles_crop_coastline2
indir=/mnt/data/MERITDEM/tiles_resized
outdir=/mnt/data/tiff_selected12
action="copy"

filename=/home/xmd5a/tmp/1.csv

rm -f ${filename}_tmp
cp $filename ${filename}_tmp
sed -i 's/^.*dummy,//g' ${filename}_tmp
sed -i -e '1d' ${filename}_tmp
sed -i 's/$/.tif/g' ${filename}_tmp

while read line
    do
	echo $line
	echo $indir/$line $outdir/$line
	if [ $action == "move" ] ; then
		mv $indir/$line $outdir/$line
	else
		cp $indir/$line $outdir/$line
	fi
done < ${filename}_tmp
