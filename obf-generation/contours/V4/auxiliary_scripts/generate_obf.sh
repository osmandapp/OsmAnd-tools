#!/bin/bash
export JAVA_OPTS="-Xmx16096M -Xmn512M"
/home/xmd5a/utilites/OsmAndMapCreator-main/utilities.sh combine-srtm-into-file \
/mnt/data/ALOS/contours-osm-bz2-sakhalin /mnt/data/ALOS/COUNTRY_OBF --filter=sakhalin