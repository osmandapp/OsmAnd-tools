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
TILE_SIZE=32
POSITIONAL_ARGS=()

while [[ $# -gt 0 ]]; do
  case $1 in
    -d|--dempath)
      DEMS_PATH="$2"
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
    -t|--tilesize)
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

set -- "${POSITIONAL_ARGS[@]}" # restore positional parameters
if [ -z "$LAT" ] || [ -z "$LON" ]; then
   echo "Please specify -lat LAT -lon LON to process the tile"
   exit 1;
fi
LATL='N'; if (( LAT < 0 )); then LATL='S'; LAT=$(( - $LAT)); fi
LONL='E'; if (( LON < 0 )); then LONL='W'; LON=$(( - $LON)); fi
TILE=${LATL}$(printf "%02d" $LAT)${LONL}$(printf "%03d" $LON)

# Step 0. Clean output path and recreate it
WORK_PATH="./.tmp_${TILE}"
rm -rf "${WORK_PATH}" || true
mkdir -p "${WORK_PATH}"
OUTPUT_RESULT=${OUTPUT_PATH}/${TILE}.heightmap.sqlite
rm "$OUTPUT_RESULT" || true

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


# Step 1. Create GDAL VRT to reference all DEM files
if [ ! -f "allheighttiles.vrt" ]; then
    echo "Creating VRT..."
    gdalbuildvrt \
        -resolution highest \
        -hidenodata \
        -vrtnodata "0" \
        "allheighttiles.vrt" "$DEMS_PATH"/*
fi

# Step 2. Convert VRT to single giant GeoTIFF file
DELTA=0.1
if [ ! -f "$WORK_PATH/heightdbs.tif" ]; then
    echo "Baking Tile GeoTIFF..."
    gdal_translate -of GTiff \
        -strict \
        -projwin $(($LAT - $DELTA)) $(($LAT + $DELTA)) $(($LON + 1 + $DELTA)) $(($LAT - 1 - $DELTA)) \
        -mo "AREA_OR_POINT=POINT" \
        -ot Int16 \
        -co "COMPRESS=LZW" \
        -co "BIGTIFF=YES" \
        -co "SPARSE_OK=TRUE" \
        -co "TILED=NO" \
        "allheighttiles.vrt" "$WORK_PATH/heightdbs.tif"
fi

# Step 3. Re-project to Mercator
if [ ! -f "$WORK_PATH/heightdbs_mercator.tif" ]; then
    echo "Re-projecting..."
    gdalwarp -of GTiff \
        -co "COMPRESS=LZW" \
        -co "BIGTIFF=YES" \
        -ot Int16 \
        -co "SPARSE_OK=TRUE" \
        -t_srs "+init=epsg:3857 +over" \
        -r cubic \
        -multi \
        "$WORK_PATH/heightdbs.tif" "$WORK_PATH/heightdbs_mercator.tif"
fi

# Step 4. Slice giant projected GeoTIFF to tiles of specified size and downscale them
echo "Slicing..."
mkdir -p "$WORK_PATH/tiles"
"$SRC_PATH/slicer.py" \
    --size=$TILE_SIZE \
    --driver=GTiff \
    --extension=tif \
    $VERBOSE_PARAM \
    "$WORK_PATH/heightdbs_mercator.tif" "$WORK_PATH/tiles"

# Step 5. Generate tiles that overlap each other by 1 heixel
echo "Overlapping..."
mkdir -p "$WORK_PATH/overlapped_tiles"
"$SRC_PATH/overlap.py" \
    --driver=GTiff \
    --driver-options="COMPRESS=LZW" \
    --extension=tif \
    $VERBOSE_PARAM \
    "$WORK_PATH/tiles" "$WORK_PATH/overlapped_tiles"

# Step 6. Pack overlapped tiles into TileDB
echo "Packing..."
mkdir -p "$WORK_PATH/db"
"$SRC_PATH/packer.py" $VERBOSE_PARAM "$WORK_PATH/overlapped_tiles" "$WORK_PATH/db"

# Step 7. Copy output
echo "Publishing..."
mv "$WORK_PATH/db/world.heightmap.sqlite" "$OUTPUT_RESULT"

# Step 8. Clean up work
rm -rf "$WORK_PATH"
