#!/bin/bash
for gz in data_4/*.tar.gz; do
echo $gz
tar -C /mnt/data/ALOS/tiff_4 -xvzf $gz --wildcards --no-anchored '*AVE_DSM.tif' --strip-components 1
done