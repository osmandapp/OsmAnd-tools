#!/bin/bash

# This script is used to retile DEM files into SQLite databases by country.
# These DBs contain tiles of specified size by zoom levels

# Basic usage is
# bake_heightmaps.sh /path/to/dem/collection /path/to/work/dir /path/to/output/dir 256 

SRC_PATH="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

DEMS_PATH=$1
shift
echo "DEM files path:       $DEMS_PATH"

WORK_PATH=$1
shift
echo "Work directory:       $WORK_PATH"
mkdir -p "$WORK_PATH"

OUTPUT_PATH=$1
shift
echo "Output path:          $OUTPUT_PATH"
mkdir -p "$OUTPUT_PATH"

TILE_SIZE=$1
let "TILE_INNER_SIZE = $TILE_SIZE - 2"
shift
echo "Tile size:            $TILE_SIZE"
echo "Tile size (inner):    $TILE_INNER_SIZE"

GDAL2TILES=`which gdal2tiles.py`
GDAL2TILES_PATH=$(dirname "$GDAL2TILES")
echo "gdal2tiles:           $GDAL2TILES"

######################################################################
rm -rf "$WORK_PATH"/*
######################################################################

# Step 1. Create GDAL VRT to reference all DEM files
if [ ! -f "$WORK_PATH/heightdbs.vrt" ]; then
	echo "Creating VRT..."
	(cd "$WORK_PATH" && \
	gdalbuildvrt \
		-resolution highest \
		-hidenodata \
		-vrtnodata "-32768" \
		"$WORK_PATH/heightdbs.vrt" "$DEMS_PATH"/*)
fi

# Step 2. Convert VRT to single giant GeoTIFF file
if [ ! -f "$WORK_PATH/heightdbs.tif" ]; then
	echo "Baking giant GeoTIFF..."
	(cd "$WORK_PATH" && \
	gdal_translate -of GTiff \
		-strict \
		--config GDAL_NUM_THREADS ALL_CPUS \
		-mo "AREA_OR_POINT=POINT" \
		-co "COMPRESS=LZW" \
		-co "BIGTIFF=YES" \
		-co "SPARSE_OK=TRUE" \
		-co "TILED=NO" \
		"$WORK_PATH/heightdbs.vrt" "$WORK_PATH/heightdbs.tif")
fi

# Step 3. Re-project to Mercator
if [ ! -f "$WORK_PATH/heightdbs_mercator.tif" ]; then
	echo "Re-projecting..."
	(cd "$WORK_PATH" && \
	gdalwarp -of GTiff \
		--config GDAL_NUM_THREADS ALL_CPUS \
		-co "COMPRESS=LZW" \
		-co "BIGTIFF=YES" \
		-co "SPARSE_OK=TRUE" \
		-co "TILED=YES" \
		-co "BLOCKXSIZE=$TILE_INNER_SIZE" \
		-co "BLOCKYSIZE=$TILE_INNER_SIZE" \
		-t_srs "+init=epsg:3857 +over" \
		-r cubic \
		-multi \
		"$WORK_PATH/heightdbs.tif" "$WORK_PATH/heightdbs_mercator.tif")
fi

# Step 4. Slice giant projected GeoTIFF to tiles of specified size and downscale them
if [ ! -d "$WORK_PATH/tiles" ]; then
	echo "Slicing..."
	mkdir -p "$WORK_PATH/tiles"
	(cd "$WORK_PATH/tiles" && \
	PYTHONPATH="$PYTHONPATH:$GDAL2TILES_PATH" "$SRC_PATH/slicer.py" \
		--size=$TILE_INNER_SIZE \
		--driver=GTiff \
		--extension=tif \
		--verbose \
		"$WORK_PATH/heightdbs_mercator.tif" "$WORK_PATH/tiles")
fi

# Step 5. Generate tiles that overlap each other by 1 heixel
if [ ! -d "$WORK_PATH/overlapped_tiles" ]; then
	echo "Overlapping..."
	mkdir -p "$WORK_PATH/overlapped_tiles"
	(cd "$WORK_PATH/overlapped_tiles" && \
	PYTHONPATH="$PYTHONPATH:$GDAL2TILES_PATH" "$SRC_PATH/overlap.py" \
		--driver=GTiff \
		--extension=tif \
		--verbose \
		"$WORK_PATH/tiles" "$WORK_PATH/overlapped_tiles")
fi

#-co="MTW=ON" \
#		-co="BLOCKXSIZE=$TILE_SIZE" \
#		-co="BLOCKYSIZE=$TILE_SIZE" \
#		-co="TILED=YES" \
#		-co="NBITS=16" \
#		-co="BIGTIFF=NO" \
#		-co="SPARSE_OK=NO" \
#		-co="COMPRESS=NONE" \
	
