#!/bin/bash

# This script is used to retile DEM files into SQLite databases by country.
# These DBs contain tiles of specified size by zoom levels

# Basic usage is
# ./generate_tile.sh -d /path/to/dem/collection/ -o /path/to/output/dir/ -f N46E008

# Fail on any error
set -e

VERBOSE_PARAM=""
FILE=""
SRC_PATH="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
TILE_SIZE=32
POSITIONAL_ARGS=()

while [[ $# -gt 0 ]]; do
  case $1 in
    -d|--dempath)
      DEMS_PATH="$2"
      shift # past argument
      shift # past value
      ;;
    -f|--file)
      FILE="$2"
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

# Step 0. Clean output path and recreate it
WORK_PATH="./.tmp_${FILE}"
rm -rf "${WORK_PATH}" || true
mkdir -p "${WORK_PATH}"
OUTPUT_RESULT=${OUTPUT_PATH}/${FILE}.heightmap.sqlite
rm "$OUTPUT_RESULT" || true

echo "DEM files path:       $DEMS_PATH"
echo "Output path:          $OUTPUT_PATH"
echo "Work directory:       $WORK_PATH"

let "TILE_FULL_SIZE = $TILE_SIZE + 1 + 2"
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
if [ ! -f "$WORK_PATH/heightdbs.vrt" ]; then
    echo "Creating VRT..."
    gdalbuildvrt \
        -resolution highest \
        -hidenodata \
        -vrtnodata "0" \
        "$WORK_PATH/heightdbs.vrt" "$DEMS_PATH/${FILE}.tif"
fi

# Step 2. Convert VRT to single giant GeoTIFF file
if [ ! -f "$WORK_PATH/heightdbs.tif" ]; then
    echo "Baking giant GeoTIFF..."
    gdal_translate -of GTiff \
        -strict \
        --config GDAL_NUM_THREADS ALL_CPUS \
        -mo "AREA_OR_POINT=POINT" \
        -co "COMPRESS=LZW" \
        -co "BIGTIFF=YES" \
        -co "SPARSE_OK=TRUE" \
        -co "TILED=NO" \
        "$WORK_PATH/heightdbs.vrt" "$WORK_PATH/heightdbs.tif"
fi

# Step 3. Re-project to Mercator
if [ ! -f "$WORK_PATH/heightdbs_mercator.tif" ]; then
    echo "Re-projecting..."
    gdalwarp -of GTiff \
        --config GDAL_NUM_THREADS ALL_CPUS \
        -co "COMPRESS=LZW" \
        -co "BIGTIFF=YES" \
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
