#!/bin/bash -xe
# GIT History could be found at https://github.com/osmandapp/OsmAnd-misc/blob/master/osm-live/generate_hourly_osmand_diff.sh

RESULT_DIR="/home/osmlive/_diff"

for DATE_DIR in $(find $RESULT_DIR -maxdepth 1  -type d | sort -r ); do
    COUNT_OBF_FILES=$(find $DATE_DIR -type f -maxdepth 1 -name "*.obf.gz" | wc -l)
    if [ -d $DATE_DIR/src ]; then
        COUNT_OSM_FILES=$(find $DATE_DIR/src -type f  -name "*_diff.osm.gz" | wc -l)
        echo $DATE_DIR $COUNT_OBF_FILES $COUNT_OSM_FILES
    else 
        break;
    fi
done

