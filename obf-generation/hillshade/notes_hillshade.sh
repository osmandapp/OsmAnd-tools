# Script used to create a big sqlite file for hillshading
#   DEM data should be in data/
#   mainly depends on gdal, imagemagick, python-gdal, python-PIL, numpy

# It is strongly advised to split this bash script into smaller ones
# and run them with nohup !
# consider running this world-wide will last a few weeks
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
mkdir -p hillshade
mkdir -p slopes
mkdir -p composite
# 1. Create hillshade and slope tiles (can last hours or 1-2 days)
for F in data/*.tif
do
	echo $F
	name=$(basename $F)
	gdaldem hillshade -z 2 -s 111120 -compute_edges $F hillshade/hs_$name
	gdaldem slope -compute_edges -s 111120 $F slopes.tif
	gdaldem color-relief slopes.tif $DIR/color_slope.txt slopes/s_$name	
	
done

# 2. Merge hillshade and slopes tiles with imagemagick (can last hours or 1-2 days)
for F in data/*.tif
do
	rm composed.tif
	echo $F
	name=$(basename $F)
	composite -compose Multiply hillshade/hs_$name slopes/s_$name composed.tif
	convert -level 28%x70% composed.tif composite/c_$name
	./gdalcopyproj.py hillshade/hs_$name composite/c_$name
done

# 3. Built a single virtual file vrt, options ensure to keep ocean white
gdalbuildvrt -hidenodata -vrtnodata "255" composite.vrt composite/*.tif
# 4. Merge all tile in a single giant tiff (can last hours or 1-2 days)
gdal_translate -of GTiff -co "COMPRESS=JPEG" -co "BIGTIFF=YES" -co "TILED=YES" composite.vrt WGS84-all.tif
# 5. Make a small tiff to check before going further
gdalwarp -of GTiff -ts 4000 0 composite-all.tif WGS84-all-small.tif
# 6. Then re-project to Mercator (can last hours or 1-2 days)
gdalwarp -of GTiff -co "JPEG_QUALITY=90" -co "BIGTIFF=YES" -co "TILED=YES" -co "COMPRESS=JPEG" \
	-t_srs "+init=epsg:3857 +over" \
	-r cubic -order 3 -multi WGS84-all.tif all-3857.tif
# 7. Make a small tiff to check before going further
gdalwarp -of GTiff -co "COMPRESS=JPEG" -ts 4000 0 all-3857.tif all-small-3857.tif

# 8. Create a sqlite containing 256x256 png tiles, in TMS numbering scheme (can last for WEEKS)
./gdal2tiles_gray2alpha_sqlite.py -z 0-11 all-3857.tif

# Create country-wide sqlites compatible with Osmand (minutes or hour each, 5-6days complete country list)
# ./extractSqlite.py -i $WORKSPACE/tools/OsmAndMapCreator/src/net/osmand/map/countries.xml -s $JENKINS_HOME/data/all-3857.tif.sqlitedb -o $JENKINS_HOME/hillshade_sqlite/
