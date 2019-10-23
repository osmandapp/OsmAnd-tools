#!/bin/bash
for tif in /mnt/data/ALOS/fixed_tiff_arcticdem_2/*.tif; do
echo $tif
gdalwarp -ts 3602 3602 -ot Int16 -co "COMPRESS=LZW" "$tif" "${tif}tmp"
mv "${tif}tmp" "/mnt/data/ALOS/fixed_tiff_arcticdem_2_converted/"$(basename $tif)
done