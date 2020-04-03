# Script used to create a big sqlite file for hillshading
#   DEM data should be in data/
#   mainly depends on gdal, imagemagick, python-gdal, python-PIL, numpy

# It is strongly advised to split this bash script into smaller ones
# and run them with nohup !
# consider running this world-wide will last a few weeks
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
#  VARIABLES: START_STAGE, END_STAGE, PROCESS, COLOR_SCHEME
# LZW, JPEG
FINAL_COMPRESS="${FINAL_COMPRESS:-LZW}"
INTER_COMPRESS="${INTER_COMPRESS:-LZW}"

if [ -z "$START_STAGE" ]; then
	START_STAGE=1
fi
if [ -z "$END_STAGE" ]; then
	END_STAGE=10
fi
mkdir -p heightdb

if [ "$START_STAGE" -le 1 ] && [ "$END_STAGE" -ge 1 ]; then
	echo "1. Built a single virtual file vrt, options ensure to keep ocean white $(date)"
	gdalbuildvrt -resolution highest -hidenodata -vrtnodata "-32768" virtualtiff.vrt data/*.tif
fi
 
if [ "$START_STAGE" -le 2 ] && [ "$END_STAGE" -ge 2 ]; then
	echo "2. Merge all tile in a single giant tiff (can last hours or 1-2 days) $(date)"
	gdal_translate -of GTiff -strict -co "COMPRESS=$INTER_COMPRESS" -co "BIGTIFF=YES" -co "SPARSE_OK=TRUE" -co "TILED=NO" \
		-mo "AREA_OR_POINT=POINT" virtualtiff.vrt WGS84-all.tif
fi

if [ "$START_STAGE" -le 3 ] && [ "$END_STAGE" -ge 3 ]; then
	echo "3. Make a small tiff to check before going further $(date)"
	rm WGS84-all-small.tif || true
	gdalwarp -of GTiff -ts 4000 0 WGS84-all.tif WGS84-all-small.tif
fi

if [ "$START_STAGE" -le 4 ] && [ "$END_STAGE" -ge 4 ]; then
	echo "4. Split planet to tiles"
	rm -rf tiles/ || true
	# -e option to continue
	gdal2tiles.py --processes 3 -z 4-11 WGS84-all.tif tiles/
	rm WGS84-all.tif || true
fi
