#!/bin/bash -e
# # 1 #
# This scripts is intended to prepare 1x1 degree tiles for hillshade/slope/tifheightmap for OsmAnd.
# Tiles in tif format are required. Tiles intended to be 1x1 degree in WGS 84, Int16, single channel. Most of tiles used in OsmAnd are 3602x3602 in size but this is not a limit.
# Edit TYPE, TILE_RANGE and paths
# After that run combine_tiles.sh

while getopts ":t:r:" opt; do
  case $opt in
    t) TYPE="$OPTARG" # hillshade/slope/tifheightmap # Type of tiles to generate
    ;;
    r) TILE_RANGE="$OPTARG" # "N25-80W000-030"
    ;;
    \?) echo -e "\033[91mInvalid option -$OPTARG\033[0m" >&2
	echo Usage: ./prepare_tiles.sh -t [hillshade/slope/tifheightmap]
    ;;
  esac
done

echo $TILE_RANGE
## DELETE ALL TILES VRT
rm -f allheighttiles.vrt
rm -f allheighttiles_hillshade.vrt
rm -f allheighttiles_slope.vrt
rm -f allheighttiles_tifheightmap.vrt

TERRAIN=/mnt/wd_2tb/lidar/split # path to folder with terrain tiles
TIFFHEIGHTMAP_OUTPUT=/mnt/wd_2tb/lidar/tiffheightmap
SQLITEDB_OUTPUT=/mnt/wd_2tb/lidar/${TYPE}-tiles
GENERATE_TILE_SCRIPTS_PATH=/home/xmd5a/git/OsmAnd-tools/obf-generation/heightmap # see https://github.com/osmandapp/OsmAnd-tools/tree/master/obf-generation/heightmap

if [[ $TYPE == tifheightmap ]]; then
	ZOOM="14"
else
	ZOOM="14"
fi
VERBOSE="true"


if [[ "$TYPE" == "heightmap" ]] || [[ "$TYPE" == "tifheightmap" ]]; then
  EXTENSION=heightmap.sqlite
  OUTPUT=$TIFFHEIGHTMAP_OUTPUT
else
  EXTENSION=sqlitedb
  OUTPUT=$SQLITEDB_OUTPUT
fi

START_LAT=$((10#${TILE_RANGE:1:2} +0 ))
END_LAT=$((10#${TILE_RANGE:4:2} + 0 ))
START_LON=$((10#${TILE_RANGE:7:3} + 0 ))
END_LON=$((10#${TILE_RANGE:11:3} + 0 ))
LATL=${TILE_RANGE:0:1}
LONL=${TILE_RANGE:6:1}
LAT=$START_LAT

if $VERBOSE; then
  VERB=--verbose
fi

while : ; do
  LON=$START_LON
  while : ; do
      TILE=${LATL}$(printf "%02d" $LAT)${LONL}$(printf "%03d" $LON)
      if [[ -f "$TERRAIN/$TILE.tif" ]]; then
        if $SKIP_EXISTING && [[ -f $OUTPUT/$TILE.$EXTENSION ]]; then
           echo "SKIP EXISTING $TERRAIN/$TILE.$EXTENSION"
        else 
           echo "PROCESS $TERRAIN/$TILE.tif"
           LATP=$LAT; if [ "$LATL" == "S" ]; then LATP=$(( - $LATP)); fi
           LONP=$LON; if [ "$LONL" == "W" ]; then LONP=$(( - $LONP)); fi
           time $GENERATE_TILE_SCRIPTS_PATH/generate_tile.sh $VERB \
        	-lat $LATP -lon $LONP -d $TERRAIN -o $OUTPUT -t ${TYPE} \
        	-z ${ZOOM}
        fi
      else
        echo "SKIP NON EXISTING $TERRAIN/$TILE.tif"
      fi
      if [[ "$LON" == "$END_LON" ]]; then
	break; 
      fi
      LON=$((LON + 1))
  done
  if [[ "$LAT" == "$END_LAT" ]]; then
    break; 
  fi
  LAT=$((LAT + 1))
done