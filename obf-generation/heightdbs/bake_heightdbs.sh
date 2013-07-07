#!/bin/bash

# This script is used to retile DEM files into SQLite databases by country.
# These DBs contain tiles of specified size by zoom levels

# Basic usage is
# bake_heightmaps.sh /path/to/dem/collection /path/to/work/dir /path/to/output/dir 256 

SRC_PATH="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

DEMS_PATH=$1
shift
echo "DEM files path: $DEMS_PATH"

WORK_PATH=$1
shift
echo "Work directory: $WORK_PATH"
mkdir -p "$WORK_PATH"

OUTPUT_PATH=$1
shift
echo "Output path:    $OUTPUT_PATH"
mkdir -p "$OUTPUT_PATH"

TILE_SIZE=$1
shift
echo "Tile size:      $TILE_SIZE"

# Step 1. Create GDAL VRT to reference all DEM files
if [ ! -f "$WORK_PATH/heightdbs.vrt" ]; then
	echo "Creating VRT..."
	gdalbuildvrt -resolution highest -hidenodata -vrtnodata "-32768" "$WORK_PATH/heightdbs.vrt" "$DEMS_PATH"/*
fi

# Step 2. Convert VRT to single giant GeoTIFF file
if [ ! -f "$WORK_PATH/heightdbs.tif" ]; then
	echo "Baking giant GeoTIFF..."
	gdal_translate -of GTiff \
		-co "COMPRESS=LZW" \
		-co "BIGTIFF=YES" \
		-co "TILED=YES" \
		-co "SPARSE_OK=TRUE" \
		-co "BLOCKXSIZE=$TILE_SIZE" \
		-co "BLOCKYSIZE=$TILE_SIZE" \
		"$WORK_PATH/heightdbs.vrt" "$WORK_PATH/heightdbs.tif"
fi

# Step 3. Re-project to Mercator
if [ ! -f "$WORK_PATH/heightdbs_mercator.tif" ]; then
	echo "Re-projecting..."
	gdalwarp -of GTiff \
		-co "COMPRESS=LZW" \
		-co "BIGTIFF=YES" \
		-co "TILED=YES" \
		-co "SPARSE_OK=TRUE" \
		-co "BLOCKXSIZE=$TILE_SIZE" \
		-co "BLOCKYSIZE=$TILE_SIZE" \
		-t_srs "+init=epsg:3857 +over" \
		-r cubic \
		-multi \
		"$WORK_PATH/heightdbs.tif" "$WORK_PATH/heightdbs_mercator.tif"
fi

# Step 4. Slice giant projected GeoTIFF to tiles of specified size
#if [ ! -d "$WORK_PATH/tiles" ]; then
	mkdir -p "$WORK_PATH/tiles"
	gdal2tiles=`which gdal2tiles.py`
	(cd "$WORK_PATH/tiles" && \
	PYTHONPATH="$PYTHONPATH:$(dirname $gdal2tiles)" "$SRC_PATH/slicer.py" \
		--size=$TILE_SIZE \
		--driver=GTiff \
		--extension=tif \
		--srcnodata="-32768" \
		--verbose \
		"$WORK_PATH/heightdbs_mercator.tif" "$WORK_PATH/tiles")
	#rm -rf "$WORK_PATH/tiles"
#fi

#-co="MTW=ON" \
#		-co="BLOCKXSIZE=$TILE_SIZE" \
#		-co="BLOCKYSIZE=$TILE_SIZE" \
#		-co="TILED=YES" \
#		-co="NBITS=16" \
#		-co="BIGTIFF=NO" \
#		-co="SPARSE_OK=NO" \
#		-co="COMPRESS=NONE" \
		
# # Built a single virtual file vrt, options ensure to keep ocean white
# gdalbuildvrt -hidenodata -vrtnodata "255" composite.vrt composite/*.tif
# # Merge all tile in a single giant tiff (can last hours or 1-2 days)
# gdal_translate -of GTiff -co "COMPRESS=JPEG" -co "BIGTIFF=YES" -co "TILED=YES" composite.vrt WGS84-all.tif
# # Make a small tiff to check before going further
# gdalwarp -of GTiff -ts 4000 0 composite-all.tif WGS84-all-small.tif
# # Then re-project to Mercator (can last hours or 1-2 days)
# gdalwarp -of GTiff -co "JPEG_QUALITY=90" -co "BIGTIFF=YES" -co "TILED=YES" -co "COMPRESS=JPEG" \
	# -t_srs "+init=epsg:3857 +over" \
	# -r cubic -order 3 -multi WGS84-all.tif all-3857.tif
# # Make a small tiff to check before going further
# gdalwarp -of GTiff -co "COMPRESS=JPEG" -ts 4000 0 all-3857.tif all-small-3857.tif

# # Create a sqlite containing 256x256 png tiles, in TMS numbering scheme (can last for WEEKS)
# ./gdal2tiles_gray2alpha_sqlite.py -z 0-11 all-3857.tif

# # Create country-wide sqlites compatible with Osmand (minutes or hour each, 5-6days complete country list)
# ./extractSqlite.py -i $WORKSPACE/tools/OsmAndMapCreator/src/net/osmand/map/countries.xml -s $JENKINS_HOME/data/all-3857.tif.sqlitedb -o $JENKINS_HOME/hillshade_sqlite/
