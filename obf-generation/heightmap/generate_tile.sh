#!/bin/zsh
# zsh to do simple math
# This script is used to retile DEM files into SQLite databases by country.
# These DBs contain tiles of specified size by zoom levels

# Basic usage is
# ./generate_tile.sh -d /path/to/dem/collection/ -o /path/to/output/dir/ -f N46E008

# Fail on any error
set -e

VERBOSE_PARAM=""
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
    --verbose)
      VERBOSE_PARAM="--verbose"
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
  if [[  "$TYPE" == "heightmap" ]]; then TILE_SIZE=32; fi
fi


set -- "${POSITIONAL_ARGS[@]}" # restore positional parameters
if [ -z "$LAT" ] || [ -z "$LON" ]; then
   echo "Please specify -lat LAT -lon LON to process the tile"
   exit 1;
fi
LATL='N'; if (( LAT < 0 )); then LATL='S'; LAT=$(( - $LAT)); fi
LONL='E'; if (( LON < 0 )); then LONL='W'; LON=$(( - $LON)); fi
TILE=${LATL}$(printf "%02d" $LAT)${LONL}$(printf "%03d" $LON)

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

# let "TILE_FULL_SIZE = $TILE_SIZE + 1 + 2"
echo "Tile:                 $TILE"
echo "Tile size:            $TILE_SIZE"
# echo "Tile size (full):     $TILE_FULL_SIZE"

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


# Step 1. Create GDAL VRT to reference all DEM files
if [ ! -f "allheighttiles_$TYPE.vrt" ]; then
    echo "Creating VRT..."
    NODATA="0"; if [[  "$TYPE" == "heightmap" ]]; then NODATA="0"; fi
    gdalbuildvrt \
        -resolution highest \
        -hidenodata \
        -vrtnodata "$NODATA" \
        "allheighttiles_$TYPE.vrt" "$DEMS_PATH"/*
fi

# Step 2. Convert VRT to single giant GeoTIFF file
DELTA=0.1
if [ ! -f "$WORK_PATH/${TYPE}_grid.tif" ]; then
    echo "Baking Tile GeoTIFF..."
    gdal_translate -of GTiff -strict \
        -projwin $(($LON - $DELTA)) $(($LAT + 1 + $DELTA)) $(($LON + 1 + $DELTA)) $(($LAT - $DELTA)) \
        -mo "AREA_OR_POINT=POINT" -ot Int16 -co "COMPRESS=LZW" -co "BIGTIFF=YES" -co "SPARSE_OK=TRUE" -co "TILED=NO" \
        "allheighttiles_$TYPE.vrt" "$WORK_PATH/${TYPE}_grid.tif"
fi


if [[  "$TYPE" == "heightmap" ]]; then
# Step 3. Re-project to Mercator
    if [ ! -f "$WORK_PATH/${TYPE}_mercator.tif" ]; then
      echo "Re-projecting..."
      gdalwarp -of GTiff -co "COMPRESS=LZW" -co "BIGTIFF=YES" -ot Int16 -co "SPARSE_OK=TRUE" \
        -t_srs "+init=epsg:3857 +over" -r cubic -multi \
        "$WORK_PATH/${TYPE}_grid.tif" "$WORK_PATH/${TYPE}_mercator.tif"
    fi
    # Step 4. Slice giant projected GeoTIFF to tiles of specified size and downscale them
    echo "Slicing..."
    mkdir -p "$WORK_PATH/rawtiles"
    "$SRC_PATH/slicer.py" --size=$TILE_SIZE --driver=GTiff --extension=tif $VERBOSE_PARAM \
        "$WORK_PATH/${TYPE}_mercator.tif" "$WORK_PATH/rawtiles"

    # Step 5. Generate tiles that overlap each other by 1 heixel
    echo "Overlapping..."
    mkdir -p "$WORK_PATH/tiles"
    "$SRC_PATH/overlap.py" --driver=GTiff --driver-options="COMPRESS=LZW" --extension=tif $VERBOSE_PARAM \
        "$WORK_PATH/rawtiles" "$WORK_PATH/tiles"
else
    echo "Calculating base slope..."
    gdaldem slope          -co "COMPRESS=LZW" -s 111120 -compute_edges "$WORK_PATH/${TYPE}_grid.tif" "$WORK_PATH/base_slope.tif"

    if [ "$TYPE" = "composite" ] || [ "$TYPE" = "hillshade" ]; then
      COLOR_SCHEME="${COLOR_SCHEME:-hillshade_alpha}"
      ZOOM_RANGE=4-12
      TARGET_FILE=hillshade.tif
      echo "Calculating base hillshade..."
      gdaldem hillshade -z 2 -co "COMPRESS=LZW" -s 111120 -compute_edges "$WORK_PATH/${TYPE}_grid.tif" "$WORK_PATH/base_hillshade.tif"
	    gdaldem color-relief -co "COMPRESS=LZW" "$WORK_PATH/base_slope.tif" "$SRC_PATH/color/color_slope.txt" "$WORK_PATH/base_hillshade_slope.tif"
    
      # merge composite hillshade / slope
      echo "Calculate composite hillshade..."
      composite -quiet -compose Multiply "$WORK_PATH/base_hillshade.tif" "$WORK_PATH/base_hillshade_slope.tif" "$WORK_PATH/pre_composite_hillshade.tif"
      convert -level 28%x70% "$WORK_PATH/pre_composite_hillshade.tif" "$WORK_PATH/pre_composite_convert.tif"
      "$SRC_PATH/gdalcopyproj.py" "$WORK_PATH/base_hillshade.tif" "$WORK_PATH/pre_composite_convert.tif"
      gdalwarp -of GTiff -ot Int16 -co "COMPRESS=LZW" "$WORK_PATH/pre_composite_convert.tif" "$WORK_PATH/base_composite_hillshade.tif"

      # hillshade
      echo "Calculating hillshade..."
      gdaldem color-relief -alpha "$WORK_PATH/base_composite_hillshade.tif" "$SRC_PATH/color/$COLOR_SCHEME.txt" "$WORK_PATH/hillshade-color.tif" -co "COMPRESS=LZW" 
      gdal_translate -b 1 -b 4 -colorinterp_1 gray "$WORK_PATH/hillshade-color.tif" "$WORK_PATH/hillshade.tif" -co "COMPRESS=LZW"

    elif [ "$TYPE" = "slopes" ]; then
      COLOR_SCHEME="${COLOR_SCHEME:-slopes_main}"
      ZOOM_RANGE=4-11
      TARGET_FILE=slope.tif
      echo "Calculating slope..."
      gdaldem color-relief -alpha "$WORK_PATH/base_slope.tif" "$SRC_PATH/color/$COLOR_SCHEME.txt" "$WORK_PATH/slope.tif" -co "COMPRESS=LZW"  
    fi
    echo "Split into tiles with $PROCESSES procesess "
    gdal2tiles.py --processes $PROCESSES  -z $ZOOM_RANGE "$WORK_PATH/$TARGET_FILE" "$WORK_PATH/tiles/"
fi

# Step 6. Pack overlapped tiles into TileDB
echo "Packing..."
mkdir -p "$WORK_PATH/db"
"$SRC_PATH/packer.py" $VERBOSE_PARAM "$WORK_PATH/tiles" "$WORK_PATH/db"

# Step 7. Copy output
echo "Publishing..."
mv "$WORK_PATH/db/tiles.sqlite" "$OUTPUT_RESULT"

# Step 8. Clean up work
rm -rf "$WORK_PATH"
