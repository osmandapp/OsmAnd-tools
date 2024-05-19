#!/bin/bash
# Usage: generate_obf.sh [input_dir] [output_dir] [filter]
export JAVA_OPTS="-Xmx16096M -Xmn512M"
/home/xmd5a/utilites/OsmAndMapCreator-main/utilities.sh combine-srtm-into-file \
$1 $2 --filter=$3