#!/bin/bash -x
# # 2 #
# This script is intended to combine hillshade/slope/tifheightmap tiles into single file for each region for OsmAnd.
# Run prepare_tiles.sh first!
# Regions are stored in OsmAnd-resources/regions.xml (regions.ocbf) in OsmAndMapCreator (always use latest version).
# Edit paths if necessary
# FILTER should be lowercase !!!

while getopts ":t:f:" opt; do
  case $opt in
    t) TYPE="$OPTARG"
    ;;
    f) FILTER=$(tr [A-Z] [a-z] <<< "$OPTARG")
    ;;
    \?) echo -e "\033[91mInvalid option -$OPTARG\033[0m" >&2
	echo Usage: ./combine_tiles.sh -t [hillshade/slope/tifheightmap] -f [filter like 'europe']
    ;;
  esac
done

SLOPE_INPUT=/mnt/wd_2tb/lidar/slope-tiles
SLOPE_OUTPUT=/mnt/wd_2tb/lidar/slope-regions
HILLSHADE_INPUT=/mnt/wd_2tb/lidar/hillshade-tiles
HILLSHADE_OUTPUT=/mnt/wd_2tb/lidar/hillshade-regions
TIFHEIGHTMAP_INPUT=/mnt/wd_2tb/lidar/tiffheightmap
TIFHEIGHTMAP_OUTPUT=/mnt/wd_2tb/lidar/tiffheightmap-regions

OSMANDMAPCREATOR_PATH=/home/xmd5a/utilites/OsmAndMapCreator-main
SKIP_EXISTING=true
export JAVA_OPTS="-Xmx4096M -Xmn512M"

if [[ "$TYPE" == "slope" ]]; then
 MINZOOM=5
 MAXZOOM=11
 PREFIX=Slope_
 REG_ATTR=region_slope
 INPUTFILE=$SLOPE_INPUT
 OUTPUTDIR=$SLOPE_OUTPUT
 MERGE_FORMAT=png
 EXTENSION=.sqlitedb
elif [[ "$TYPE" == "hillshade" ]]; then
 MINZOOM=5
 MAXZOOM=12
 PREFIX=Hillshade_
 REG_ATTR=region_hillshade
 INPUTFILE=$HILLSHADE_INPUT
 OUTPUTDIR=$HILLSHADE_OUTPUT
 MERGE_FORMAT=png
 EXTENSION=.sqlitedb
elif [[ "$TYPE" == "heightmap" ]] || [[ "$TYPE" == "tifheightmap" ]]; then
 MINZOOM=9
 MAXZOOM=15
 PREFIX=Heightmap_
 INPUTFILE=$TIFHEIGHTMAP_INPUT
 OUTPUTDIR=$TIFHEIGHTMAP_OUTPUT
 EXTENSION=.heightmap.sqlite
 REG_ATTR=region_heightmap
 MERGE_FORMAT=tif
fi

if [[ "$TYPE" == "tifheightmap" ]]; then

  LISTDIR="$INPUTFILE/lists"
  rm -rf "$LISTDIR"
  mkdir -p "$LISTDIR"
  mkdir -p "$OUTPUTDIR"
  $OSMANDMAPCREATOR_PATH/utilities.sh list-tiles-for-regions $LISTDIR --prefix=$PREFIX \
        --zoom=$MINZOOM --filter=$FILTER --extension=.tif --region-ocbf-attr=$REG_ATTR \
        --skip-existing=$SKIP_EXISTING

  cd "${INPUTFILE}"
  FILES="$LISTDIR/*.txt"
  for f in $FILES
  do
    if [ -f "$f" ]; then
      FILENAME=$(basename $f .txt)

      echo "Creating VRT file for ${FILENAME} ..."
      gdalbuildvrt -resolution lowest -r cubic -vrtnodata -32768 \
      -input_file_list "$f" "$LISTDIR/${FILENAME}.vrt"

      if $SKIP_EXISTING && [[ -f $OUTPUTDIR/${FILENAME}.tif ]]; then
        echo "SKIP EXISTING $OUTPUTDIR/${FILENAME}.tif"
      else
        if [[ -f $OUTPUTDIR/${FILENAME}.tif ]]; then
          rm "$OUTPUTDIR/${FILENAME}.tif"
        fi
        echo "Generating result file for ${FILENAME} ..."
        gdal_translate -of GTiff -strict -a_nodata -32768.0 \
        -mo "AREA_OR_POINT=POINT" -ot Float32 -co "COMPRESS=LZW" \
        -co "BIGTIFF=YES" -co "SPARSE_OK=TRUE" -co "TILED=YES" \
        -co "BLOCKXSIZE=80" -co "BLOCKYSIZE=80" -co "PREDICTOR=2" \
        "$LISTDIR/${FILENAME}.vrt" "$OUTPUTDIR/${FILENAME}.tif"
      fi

    else
      echo "Warning: Some problem with ${FILENAME}"
    fi
  done
  cd -


else

  $OSMANDMAPCREATOR_PATH/utilities.sh collect-sqlitedb-into-regions $INPUTFILE $OUTPUTDIR \
       --prefix=$PREFIX --minzoom=$MINZOOM --maxzoom=$MAXZOOM \
       --filter=$FILTER --extension=$EXTENSION --region-ocbf-attr=$REG_ATTR \
       --merge-tile-format=$MERGE_FORMAT --skip-existing=$SKIP_EXISTING
fi