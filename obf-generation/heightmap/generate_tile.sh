#!/bin/zsh
# zsh to do simple math
# This script is used to retile DEM files into SQLite databases by country.
# These DBs contain tiles of specified size by zoom levels

# Basic usage is
# ./generate_tile.sh -d /path/to/dem/collection/ -o /path/to/output/dir/ -f N46E008

# Fail on any error
set -e
#set -xe

VERBOSE_PARAM=""
SKIP_EXISTING=""
SRC_PATH=$(dirname "$0")
#SRC_PATH="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
POSITIONAL_ARGS=()
TYPE=heightmap
PROCESSES="${PROCESSES:-2}"
while [[ $# -gt 0 ]]; do
  case $1 in
    -d|--dempath)
      DEMS_PATH="$2"
      shift # past argument
      shift # past value
      ;;
    -c|--color)
      COLOR_SCHEME="$2"
      shift # past argument
      shift # past value
      ;;
    -t|--type)
      TYPE="$2"
      shift # past argument
      shift # past value
      ;;
    -lat)
      LAT="$2"
      shift # past argument
      shift # past value
      ;;
    -lon)
      LON="$2"
      shift # past argument
      shift # past value
      ;;
    -o|--output)
      OUTPUT_PATH="$2"
      shift # past argument
      shift # past value
      ;;
    -p|--processes)
      PROCESSES="$2"
      shift # past argument
      shift # past value
      ;;
    --tilesize)
      TILESIZE="$2"
      shift # past argument
      shift # past value
      ;;
    -z|--zoom)
      ZOOM="$2"
      shift # past argument
      shift # past value
      ;;
    --verbose)
      VERBOSE_PARAM="--verbose"
      shift # past argument with no value
      ;;
    --skip)
      SKIP_EXISTING="--skip"
      shift # past argument with no value
      ;;
    -*|--*)
      echo "Unknown option $1"
      exit 1
      ;;
    *)
      POSITIONAL_ARGS+=("$1") # save positional arg
      shift # past argument
      ;;
  esac
done

if [ -z "$TILE_SIZE" ]; then
  TILE_SIZE=256
  if [[ "$TYPE" == "heightmap" ]] || [[ "$TYPE" == "tifheightmap" ]]; then TILE_SIZE=32; fi
fi


set -- "${POSITIONAL_ARGS[@]}" # restore positional parameters
if [ -z "$LAT" ] || [ -z "$LON" ]; then
   echo "Please specify -lat LAT -lon LON to process the tile"
   exit 1;
fi
LATL='N'; if (( LAT < 0 )); then LATL='S'; fi
LONL='E'; if (( LON < 0 )); then LONL='W'; fi
LATP=$LAT; if (( LAT < 0 )); then LATP=$(( - $LATP)); fi
LONP=$LON; if (( LON < 0 )); then LONP=$(( - $LONP)); fi
TILE=${LATL}$(printf "%02d" $LATP)${LONL}$(printf "%03d" $LONP)

# Step 0. Clean output path and recreate it
WORK_PATH="./.tmp_${TILE}_${TYPE}"
if [ -d "${WORK_PATH}" ]; then
  rm -rf "${WORK_PATH}"
fi
mkdir -p "${WORK_PATH}"

EXTENSION="sqlitedb"; if [[  "$TYPE" == "heightmap" ]]; then EXTENSION="heightmap.sqlite"; fi
OUTPUT_RESULT=${OUTPUT_PATH}/${TILE}.${EXTENSION}
if [ -f "$OUTPUT_RESULT" ]; then
  rm "$OUTPUT_RESULT"
fi

echo "Type file:            $Type"
echo "DEM files path:       $DEMS_PATH"
echo "Output path:          $OUTPUT_PATH"
echo "Work directory:       $WORK_PATH"
echo "Source directory:     $SRC_PATH"

let "TILE_FULL_SIZE = $TILE_SIZE + 1 + 2"
echo "Tile:                 $TILE"
echo "Tile size:            $TILE_SIZE"
echo "Tile size (full):     $TILE_FULL_SIZE"

GDAL2TILES=`which gdal2tiles.py`
GDAL2TILES_PATH=$(dirname "$GDAL2TILES")
if "$SRC_PATH/test_gdal2tiles.py"; then
    echo "gdal2tiles:           $GDAL2TILES (visible to python)"
else
    if [ -z "$PYTHONPATH" ]; then
        export PYTHONPATH="$GDAL2TILES_PATH"
    else
        export PYTHONPATH="$PYTHONPATH:$GDAL2TILES_PATH"
    fi
    "$SRC_PATH/test_gdal2tiles.py" || exit $?
    echo "gdal2tiles:           $GDAL2TILES (added to PYTHONPATH)"
fi


# Step 1. Create GDAL VRT to reference needed DEM files
echo "Creating VRT..."
LATMIN=$(($LAT - 1))
LATMAX=$(($LAT + 1))
LONMIN=$(($LON - 1))
LONMAX=$(($LON + 1))
TLATL='N'; if (( LATMAX < 0 )); then TLATL='S'; fi
LLONL='E'; if (( LONMIN < 0 )); then LLONL='W'; fi
TLATP=$LATMAX; if (( LATMAX < 0 )); then TLATP=$(( - $TLATP)); fi
LLONP=$LONMIN; if (( LONMIN < 0 )); then LLONP=$(( - $LLONP)); fi
TLTILE=${TLATL}$(printf "%02d" $TLATP)${LLONL}$(printf "%03d" $LLONP)
BLATL='N'; if (( LATMIN < 0 )); then BLATL='S'; fi
RLONL='E'; if (( LONMAX < 0 )); then RLONL='W'; fi
BLATP=$LATMIN; if (( LATMIN < 0 )); then BLATP=$(( - $BLATP)); fi
RLONP=$LONMAX; if (( LONMAX < 0 )); then RLONP=$(( - $RLONP)); fi
BRTILE=${BLATL}$(printf "%02d" $BLATP)${RLONL}$(printf "%03d" $RLONP)
TRTILE=${TLATL}$(printf "%02d" $TLATP)${RLONL}$(printf "%03d" $RLONP)
BLTILE=${BLATL}$(printf "%02d" $BLATP)${LLONL}$(printf "%03d" $LLONP)
TCTILE=${TLATL}$(printf "%02d" $TLATP)${LONL}$(printf "%03d" $LONP)
BCTILE=${BLATL}$(printf "%02d" $BLATP)${LONL}$(printf "%03d" $LONP)
CLTILE=${LATL}$(printf "%02d" $LATP)${LLONL}$(printf "%03d" $LLONP)
CRTILE=${LATL}$(printf "%02d" $LATP)${RLONL}$(printf "%03d" $RLONP)
NODATA="0"; if [[  "$TYPE" == "heightmap" ]]; then NODATA="0"; fi
gdalbuildvrt \
    -te $(($LON - 1)) $(($LAT - 1)) $(($LON + 2)) $(($LAT + 2)) \
    -resolution highest \
    -hidenodata \
    -vrtnodata "$NODATA" \
    "$WORK_PATH/heighttiles_$TYPE.vrt" \
    "$DEMS_PATH/$TLTILE.tif" "$DEMS_PATH/$TCTILE.tif" "$DEMS_PATH/$TRTILE.tif" \
    "$DEMS_PATH/$CLTILE.tif" "$DEMS_PATH/$TILE.tif" "$DEMS_PATH/$CRTILE.tif" \
    "$DEMS_PATH/$BLTILE.tif" "$DEMS_PATH/$BCTILE.tif" "$DEMS_PATH/$BRTILE.tif"

# Step 2. Convert VRT to single GeoTIFF file
DELTA=0.4
if [ ! -f "$WORK_PATH/${TYPE}_grid.tif" ]; then
    echo "Baking Tile GeoTIFF..."
    gdal_translate -of GTiff -strict -epo \
        -projwin $(($LON - $DELTA)) $(($LAT + 1 + $DELTA)) $(($LON + 1 + $DELTA)) $(($LAT - $DELTA)) \
        -mo "AREA_OR_POINT=POINT" -ot Int16 -co "COMPRESS=LZW" -co "PREDICTOR=2" -co "BIGTIFF=YES" -co "SPARSE_OK=TRUE" -co "TILED=NO" \
        "$WORK_PATH/heighttiles_$TYPE.vrt" "$WORK_PATH/${TYPE}_grid.tif"
fi


if [[ "$TYPE" == "heightmap" ]] || [[ "$TYPE" == "tifheightmap" ]]; then
# Step 3. Re-project to Mercator
    if [ ! -f "$WORK_PATH/${TYPE}_mercator.tif" ]; then
      echo "Re-projecting..."
      if [ -z "$ZOOM" ]; then
          ZOOM="15"
      else
        if (($ZOOM < 0)); then
          ZOOM="0"
        else
          if (($ZOOM > 31)); then
            ZOOM="31"
          fi
        fi
      fi
      PIXEL_SIZE=$(printf "%.17g" $((40075016.68557848615314309804 / (2 ** $ZOOM * $TILE_SIZE))))
      gdalwarp -of GTiff -co "COMPRESS=LZW" -co "BIGTIFF=YES" -co "PREDICTOR=2" -ot Float32 -co "SPARSE_OK=TRUE" \
        -t_srs "+init=epsg:3857 +over" -r cubic -multi \
        -tr $PIXEL_SIZE $PIXEL_SIZE -tap \
        "$WORK_PATH/${TYPE}_grid.tif" "$WORK_PATH/${TYPE}_ready.tif"
    fi
    #if [ ! -f "$WORK_PATH/${TYPE}_ready.tif" ]; then
    #  echo "Translating..."
    #  gdal_translate -of GTiff -strict \
    #    -mo "AREA_OR_POINT=POINT" -ot Float32 \
    #    -co "COMPRESS=LZW" -co "PREDICTOR=3" -co "BIGTIFF=YES" -co "SPARSE_OK=TRUE" -co "TILED=NO" \
    #    "$WORK_PATH/${TYPE}_mercator.tif" "$WORK_PATH/${TYPE}_ready.tif"
    #fi
    if [[ "$TYPE" == "heightmap" ]]; then
      # Step 4. Slice giant projected GeoTIFF to tiles of specified size and downscale them
      echo "Slicing..."
      mkdir -p "$WORK_PATH/rawtiles"
      "$SRC_PATH/slicer.py" --size=$TILE_SIZE --driver=GTiff --extension=tif $VERBOSE_PARAM \
          "$WORK_PATH/${TYPE}_ready.tif" "$WORK_PATH/rawtiles"

      # Step 5. Generate tiles that overlap each other by 1 heixel
      echo "Overlapping..."
      mkdir -p "$WORK_PATH/tiles"
      "$SRC_PATH/overlap.py" --driver=GTiff --driver-options="COMPRESS=LZW;PREDICTOR=2" --extension=tif $VERBOSE_PARAM \
          "$WORK_PATH/rawtiles" "$WORK_PATH/tiles"
    else
      # Alternative Steps 4-5. Slice projected GeoTIFF to overlapped tiles of specified size and zoom level
      echo "Generating tile GeoTIFFs..."
      "$SRC_PATH/tiler.py" --size=$TILE_FULL_SIZE --overlap=3 --zoom=9 --driver=GTiff \
        --driver-options="COMPRESS=LZW;PREDICTOR=3;SPARSE_OK=TRUE;TILED=YES;BLOCKXSIZE=80;BLOCKYSIZE=80" \
        --extension=tif $VERBOSE_PARAM $SKIP_EXISTING \
        "$WORK_PATH/${TYPE}_ready.tif" "$OUTPUT_PATH"
    fi
else
    echo "Calculating base slope..."
    gdaldem slope -co "COMPRESS=LZW" -co "PREDICTOR=2" -s 111120 -compute_edges "$WORK_PATH/${TYPE}_grid.tif" "$WORK_PATH/base_slope.tif"

    if [ "$TYPE" = "composite" ] || [ "$TYPE" = "hillshade" ]; then
      COLOR_SCHEME="${COLOR_SCHEME:-hillshade_alpha}"
      ZOOM_RANGE=4-12
      TARGET_FILE=hillshade.tif
      echo "Calculating base hillshade..."
      gdaldem hillshade -z 2 -co "COMPRESS=LZW" -co "PREDICTOR=2" -s 111120 -compute_edges "$WORK_PATH/${TYPE}_grid.tif" "$WORK_PATH/base_hillshade.tif"
      gdaldem color-relief -co "COMPRESS=LZW" -co "PREDICTOR=2" "$WORK_PATH/base_slope.tif" "$SRC_PATH/color/color_slope.txt" "$WORK_PATH/base_hillshade_slope.tif"
    
      # merge composite hillshade / slope
      echo "Calculate composite hillshade..."
      composite -quiet -compose Multiply "$WORK_PATH/base_hillshade.tif" "$WORK_PATH/base_hillshade_slope.tif" "$WORK_PATH/pre_composite_hillshade.tif"
      convert -level 28%x70% "$WORK_PATH/pre_composite_hillshade.tif" "$WORK_PATH/pre_composite_convert.tif"
      "$SRC_PATH/gdalcopyproj.py" "$WORK_PATH/base_hillshade.tif" "$WORK_PATH/pre_composite_convert.tif"
      gdalwarp -of GTiff -ot Int16 -co "COMPRESS=LZW" -co "PREDICTOR=2" "$WORK_PATH/pre_composite_convert.tif" "$WORK_PATH/base_composite_hillshade.tif"

      # hillshade
      echo "Calculating hillshade..."
      gdaldem color-relief -alpha "$WORK_PATH/base_composite_hillshade.tif" "$SRC_PATH/color/$COLOR_SCHEME.txt" "$WORK_PATH/hillshade-color.tif" -co "COMPRESS=LZW" -co "PREDICTOR=2"
      gdal_translate -b 1 -b 4 -colorinterp_1 gray "$WORK_PATH/hillshade-color.tif" "$WORK_PATH/hillshade.tif" -co "COMPRESS=LZW" -co "PREDICTOR=2"

    elif [ "$TYPE" = "slope" ]; then
      COLOR_SCHEME="${COLOR_SCHEME:-slopes_main}"
      ZOOM_RANGE=4-11
      TARGET_FILE=slope.tif
      echo "Calculating slope..."
      gdaldem color-relief -alpha "$WORK_PATH/base_slope.tif" "$SRC_PATH/color/$COLOR_SCHEME.txt" "$WORK_PATH/slope.tif" -co "COMPRESS=LZW" -co "PREDICTOR=2"
    fi
    echo "Split into tiles with $PROCESSES procesess "
    gdal2tiles.py --processes $PROCESSES  -z $ZOOM_RANGE "$WORK_PATH/$TARGET_FILE" "$WORK_PATH/tiles/"
fi

if [[ "$TYPE" != "tifheightmap" ]]; then

  # Step 6. Pack overlapped tiles into TileDB
  echo "Packing..."
  mkdir -p "$WORK_PATH/db"
  "$SRC_PATH/packer.py" $VERBOSE_PARAM "$WORK_PATH/tiles" "$WORK_PATH/db"

  # Step 7. Copy output
  echo "Publishing..."
  mv "$WORK_PATH/db/tiles.sqlite" "$OUTPUT_RESULT"

fi

# Step 8. Clean up work
rm -rf "$WORK_PATH"
