#!/usr/bin/env bash
# Depth contours of one region from a depth grid, clipped by the OSM land mask, as .osm.gz for OBF generation.
#
#   build_depth_region.sh -n NAME -b "W S E N" -i GRID -m LAND -o OUT_DIR [-l LEVELS] [-r CELL] [-u UPSAMPLE] [-j JOBS]
#
#   -i GRID      raster or VRT with elevation in metres, negative below sea level (GEBCO, EMODnet, CUDEM...);
#                a /vsicurl/ URL works and reads only the region
#   -m LAND      land polygons (OGR source, e.g. land_polygons.gpkg from check_sources.sh)
#   -l LEVELS    depths in metres, default 2,5,10,20,30,50,100,200,500,1000,1500,2000,...,11000
#   -r CELL      output cell in degrees, default the grid's own
#   -u UPSAMPLE  cubic-spline upsampling factor before contouring, smooths the lines of a coarse grid (default 1)
#
# Steps: cut the region (EPSG:4326) -> upsample -> set land to 0 m by the mask -> gdal_contour -> drop short closed
# rings and simplify (depth_contours_filter.py) -> ogr2osm with translations/contours_depth.py -> OUT_DIR/NAME.osm.gz.
# Land is set after smoothing so that it does not pull the sea shallower, and set to 0 m rather than nodata so the
# contours run along the coast instead of stopping short of it.
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
V4=$(cd "$HERE/.." && pwd)
NAME=""; BBOX=""; GRID=""; LAND=""; OUT=""; CELL=""; UPSAMPLE=1; JOBS=4
LEVELS="2,5,10,20,30,50,100,200,500,1000,1500,2000,3000,4000,5000,6000,7000,8000,9000,10000,11000"
while [ $# -gt 0 ]; do
	case "$1" in
		-n) NAME=$2; shift 2 ;;
		-b) BBOX=$2; shift 2 ;;
		-i) GRID=$2; shift 2 ;;
		-m) LAND=$2; shift 2 ;;
		-o) OUT=$2; shift 2 ;;
		-l) LEVELS=$2; shift 2 ;;
		-r) CELL=$2; shift 2 ;;
		-u) UPSAMPLE=$2; shift 2 ;;
		-j) JOBS=$2; shift 2 ;;
		-h|--help) sed -n '2,17p' "$0"; exit 0 ;;
		*) echo "Unknown option $1" >&2; exit 1 ;;
	esac
done
for v in NAME BBOX GRID LAND OUT; do
	[ -n "${!v}" ] || { echo "Missing $v, see --help" >&2; exit 1; }
done
read -r W S E N <<< "$BBOX"
mkdir -p "$OUT"; OUT=$(cd "$OUT" && pwd)
TMP="$OUT/$NAME.tmp"; rm -rf "$TMP"; mkdir -p "$TMP"
export GDAL_NUM_THREADS=$JOBS GDAL_HTTP_MAX_RETRY=5 GDAL_HTTP_RETRY_DELAY=5 GDAL_DISABLE_READDIR_ON_OPEN=EMPTY_DIR
started=$(date +%s)
step() { echo "== $(( $(date +%s) - started ))s $*"; }

if [ -z "$CELL" ]; then
	CELL=$(gdalinfo -json "$GRID" | python3 -c 'import json,sys; print(abs(json.load(sys.stdin)["geoTransform"][1]))')
	# a projected grid (metres) gets the cell of the same size in degrees
	if python3 -c "import sys; sys.exit(0 if float('$CELL') > 1 else 1)"; then
		CELL=$(python3 -c "print(float('$CELL') / 111320)")
	fi
fi
FINE=$(python3 -c "print(float('$CELL') / float('$UPSAMPLE'))")

step "cut $NAME ($W $S $E $N), cell $CELL deg, upsampled x$UPSAMPLE"
gdalwarp -q -overwrite -t_srs EPSG:4326 -te "$W" "$S" "$E" "$N" -tr "$CELL" "$CELL" -r average -ot Float32 \
	-dstnodata nan -multi -wo NUM_THREADS="$JOBS" -co COMPRESS=DEFLATE -co TILED=YES "$GRID" "$TMP/grid.tif"
if [ "$UPSAMPLE" != 1 ]; then
	gdalwarp -q -overwrite -tr "$FINE" "$FINE" -r cubicspline -multi -wo NUM_THREADS="$JOBS" \
		-co COMPRESS=DEFLATE -co TILED=YES "$TMP/grid.tif" "$TMP/fine.tif"
	mv "$TMP/fine.tif" "$TMP/grid.tif"
fi

step "land mask"
ogr2ogr -q -f GPKG -spat "$W" "$S" "$E" "$N" -clipsrc "$W" "$S" "$E" "$N" -nlt MULTIPOLYGON "$TMP/land.gpkg" "$LAND"
gdal_rasterize -q -burn 0 -l "$(ogrinfo -q "$TMP/land.gpkg" | awk -F'[: ]+' 'NR==1{print $2}')" "$TMP/land.gpkg" "$TMP/grid.tif"

step "contours at $LEVELS m"
FL=$(echo "$LEVELS" | tr ',' '\n' | sort -rn | awk '{printf "%s ", -$1}')
# shellcheck disable=SC2086
gdal_contour -q -a elev -fl $FL "$TMP/grid.tif" "$TMP/contours.gpkg"

step "filter and simplify"
python3 "$HERE/depth_contours_filter.py" "$TMP/contours.gpkg" "$TMP/depth.gpkg" --cell "$FINE"

step "osm"
python3 "$V4/ogr2osm.py" -f -t "$V4/translations/contours_depth.py" -o "$TMP/$NAME.osm" "$TMP/depth.gpkg" >/dev/null
gzip -f "$TMP/$NAME.osm"
mv "$TMP/$NAME.osm.gz" "$OUT/$NAME.osm.gz"
mv "$TMP/depth.gpkg" "$OUT/$NAME.gpkg"
rm -rf "$TMP"
step "done: $OUT/$NAME.osm.gz ($(du -h "$OUT/$NAME.osm.gz" | cut -f1)), preview layer $OUT/$NAME.gpkg"
