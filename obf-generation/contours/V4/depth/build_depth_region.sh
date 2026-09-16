#!/usr/bin/env bash
# Depth contours of one region from a depth grid, clipped by the OSM land mask, as .osm.gz for OBF generation.
#
#   build_depth_region.sh -D DATA_DIR -n REGION [-c MAP_CREATOR_DIR] [-k] [-j JOBS]
#   build_depth_region.sh -n NAME -b "W S E N" -i GRID -m LAND -o OUT_DIR [-l LEVELS] [-r CELL] [-u UPSAMPLE]
#                         [-s SMOOTH] [-d SMOOTH_FROM] [-c MAP_CREATOR_DIR] [-j JOBS]
#
#   -D DATA_DIR  folder of download_all.sh: the grid is DATA_DIR/src/..., the land DATA_DIR/mask/land_polygons.gpkg,
#                the output DATA_DIR/build; -n then names a region below, which sets the rest
#   -i GRID      raster or VRT with elevation in metres, negative below sea level (GEBCO, EMODnet, CUDEM...);
#                a /vsicurl/ URL works and reads only the region
#   -m LAND      land polygons (OGR source, e.g. land_polygons.gpkg from check_sources.sh)
#   -l LEVELS    depths in metres, default 2,5,10,20,30,50,100,200,500,1000,1500,2000,...,11000
#   -r CELL      output cell in degrees, default the grid's own
#   -u UPSAMPLE  cubic-spline upsampling factor before contouring, smooths the lines of a coarse grid (default 1)
#   -s SMOOTH    low-pass for the deeper levels: average over SMOOTH x SMOOTH cells, then back (default 4, 1 = off);
#                a flat bottom with sand waves near a level gives hundreds of tiny zigzags without it
#   -d SMOOTH_FROM  levels from this depth down use the smoothed grid, shallower ones the full grid (default 20)
#   -c MAP_CREATOR_DIR  unzipped OsmAndMapCreator: also writes OUT_DIR/NAME.depth.obf
#   -k           keep: do nothing when OUT_DIR/NAME.depth.obf already exists
#   -t TILE      split a region larger than TILE degrees into tiles built in parallel (JOBS at a time); with -c their
#                OBFs are merged into NAME.depth.obf, one map section per tile, without -c the tiles' .osm.gz stay
#                in OUT_DIR/NAME.tiles (default 0, no split)
#
# Regions (-D DATA_DIR -n REGION); options given on the command line win:
#   Netherlands_contours   EMODnet DTM 2024, the bounds of the published Netherlands_contours_2.depth.obf
#   Europe_contours        EMODnet DTM 2024, the bounds of the published Europe_contours_2.depth.obf, 10 degree tiles
#   World_contours         GEBCO_2026 (15"), from 10 m down - 2 and 5 m mean nothing in a 450 m grid, 15 degree tiles
#
# Steps: cut the region (EPSG:4326) -> upsample -> set land to 0 m by the mask -> gdal_contour -> drop short closed
# rings and simplify (depth_contours_filter.py) -> ogr2osm with translations/contours_depth.py -> OUT_DIR/NAME.osm.gz.
# Land is set after smoothing so that it does not pull the sea shallower, and set to 0 m rather than nodata so the
# contours run along the coast instead of stopping short of it.
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
V4=$(cd "$HERE/.." && pwd)
NAME=""; BBOX=""; GRID=""; LAND=""; OUT=""; CELL=""; UPSAMPLE=1; JOBS=4; SMOOTH=4; SMOOTH_FROM=20; MAP_CREATOR=""; DATA=""; TILE=0; KEEP=0
DEEP_LEVELS="1000,1500,2000,3000,4000,5000,6000,7000,8000,9000,10000,11000"
LEVELS=""
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
		-s) SMOOTH=$2; shift 2 ;;
		-d) SMOOTH_FROM=$2; shift 2 ;;
		-c) MAP_CREATOR=$2; shift 2 ;;
		-D) DATA=$2; shift 2 ;;
		-t) TILE=$2; shift 2 ;;
		-k) KEEP=1; shift ;;
		-h|--help) sed -n '2,36p' "$0"; exit 0 ;;
		*) echo "Unknown option $1" >&2; exit 1 ;;
	esac
done
if [ -n "$DATA" ]; then
	case "$NAME" in
		Netherlands_contours)
			: "${BBOX:=1.7 51.1 7.3 55.7}"; : "${GRID:=$DATA/src/emodnet/emodnet_2024.vrt}" ;;
		Europe_contours)
			: "${BBOX:=-31.3 25.4 36.0 71.2}"; : "${GRID:=$DATA/src/emodnet/emodnet_2024.vrt}"
			[ "$TILE" != 0 ] || TILE=10 ;;
		World_contours)
			: "${BBOX:=-180 -79 180 85}"; : "${GRID:=$DATA/src/gebco/gebco_2026.vrt}"
			: "${LEVELS:=10,20,30,50,100,200,500,$DEEP_LEVELS}"
			[ "$TILE" != 0 ] || TILE=15 ;;
		*) echo "Unknown region '$NAME', see --help" >&2; exit 1 ;;
	esac
	: "${LAND:=$DATA/mask/land_polygons.gpkg}"; : "${OUT:=$DATA/build}"
fi
: "${LEVELS:=2,5,10,20,30,50,100,200,500,$DEEP_LEVELS}"
for v in NAME BBOX GRID LAND OUT; do
	[ -n "${!v}" ] || { echo "Missing $v, see --help" >&2; exit 1; }
done
read -r W S E N <<< "$BBOX"
if [ $KEEP -eq 1 ] && [ -f "$OUT/$NAME.depth.obf" ]; then
	echo "== $NAME.depth.obf exists, kept (no -k to rebuild)"; exit 0
fi
mkdir -p "$OUT"; OUT=$(cd "$OUT" && pwd)
if [ -n "$MAP_CREATOR" ]; then MAP_CREATOR=$(cd "$MAP_CREATOR" && pwd); fi
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

TILES=$(python3 -c "
import math
w, s, e, n, t = $W, $S, $E, $N, float('$TILE')
if t <= 0 or (e - w <= t and n - s <= t):
    raise SystemExit
for i in range(math.ceil((e - w) / t)):
    for j in range(math.ceil((n - s) / t)):
        x0, y0 = w + i * t, s + j * t
        print('%s_%02d_%02d %g %g %g %g' % ('$NAME', i, j, x0, y0, min(x0 + t, e), min(y0 + t, n)))
")
if [ -n "$TILES" ]; then
	step "$NAME: $(echo "$TILES" | wc -l | tr -d ' ') tiles of $TILE degrees, $JOBS at a time"
	TILE_OUT="$TMP/tiles"; mkdir -p "$TILE_OUT"
	export SELF="$0" GRID LAND LEVELS CELL UPSAMPLE SMOOTH SMOOTH_FROM MAP_CREATOR TILE_OUT
	echo "$TILES" | xargs -P "$JOBS" -L 1 bash -c '
		name=$1; shift
		args=(-n "$name" -b "$*" -i "$GRID" -m "$LAND" -o "$TILE_OUT" -l "$LEVELS" -r "$CELL" -u "$UPSAMPLE" \
			-s "$SMOOTH" -d "$SMOOTH_FROM" -j 1)
		[ -z "$MAP_CREATOR" ] || args+=(-c "$MAP_CREATOR")
		JAVA_OPTS="${JAVA_OPTS:--Xmx2g}" bash "$SELF" "${args[@]}" > "$TILE_OUT/$name.log" 2>&1 \
			|| { echo "FAILED tile $name:"; tail -20 "$TILE_OUT/$name.log"; exit 255; }
		echo "tile $name: $(tail -1 "$TILE_OUT/$name.log")"' _
	if [ -n "$MAP_CREATOR" ]; then
		step "merge tile OBFs"
		ls "$TILE_OUT"/*.depth.obf > /dev/null 2>&1 || { echo "no tile has contours" >&2; exit 1; }
		rm -f "$OUT/$NAME.depth.obf"
		JAVA_OPTS="${JAVA_OPTS:--Xmx16g}" bash "$MAP_CREATOR/utilities.sh" merge-index "$OUT/$NAME.depth.obf" \
			"$TILE_OUT"/*.depth.obf > "$TMP/merge.log" 2>&1 || { tail -20 "$TMP/merge.log"; exit 1; }
		rm -rf "$TMP"
	else
		rm -rf "$OUT/$NAME.tiles"; mkdir -p "$OUT/$NAME.tiles"
		mv "$TILE_OUT"/*.osm.gz "$OUT/$NAME.tiles/" 2>/dev/null || true
		rm -rf "$TMP"
	fi
	step "done: $(cd "$OUT" && du -sh "$NAME".* | awk '{printf "%s (%s) ", $2, $1}')"
	exit 0
fi

step "cut $NAME ($W $S $E $N), cell $CELL deg, upsampled x$UPSAMPLE"
gdalwarp -q -overwrite -t_srs EPSG:4326 -te "$W" "$S" "$E" "$N" -tr "$CELL" "$CELL" -r average -ot Float32 \
	-dstnodata nan -multi -wo NUM_THREADS="$JOBS" -co COMPRESS=DEFLATE -co TILED=YES "$GRID" "$TMP/grid.tif"
if [ "$UPSAMPLE" != 1 ]; then
	gdalwarp -q -overwrite -tr "$FINE" "$FINE" -r cubicspline -multi -wo NUM_THREADS="$JOBS" \
		-co COMPRESS=DEFLATE -co TILED=YES "$TMP/grid.tif" "$TMP/fine.tif"
	mv "$TMP/fine.tif" "$TMP/grid.tif"
fi

if [ "$SMOOTH" != 1 ]; then
	step "smoothed copy for levels from $SMOOTH_FROM m: average over $SMOOTH cells"
	COARSE=$(python3 -c "print(float('$FINE') * float('$SMOOTH'))")
	gdalwarp -q -overwrite -tr "$COARSE" "$COARSE" -r average -multi -wo NUM_THREADS="$JOBS" "$TMP/grid.tif" "$TMP/coarse.tif"
	gdalwarp -q -overwrite -te "$W" "$S" "$E" "$N" -tr "$FINE" "$FINE" -r cubicspline -multi -wo NUM_THREADS="$JOBS" \
		-co COMPRESS=DEFLATE -co TILED=YES "$TMP/coarse.tif" "$TMP/smooth.tif"
	rm -f "$TMP/coarse.tif"
fi

step "land mask"
ogr2ogr -q -f GPKG -spat "$W" "$S" "$E" "$N" -clipsrc "$W" "$S" "$E" "$N" -nlt MULTIPOLYGON "$TMP/land.gpkg" "$LAND"
LAND_LAYER=$(ogrinfo -q "$TMP/land.gpkg" | awk -F'[: ]+' 'NR==1{print $2}')
for raster in "$TMP/grid.tif" "$TMP/smooth.tif"; do
	if [ -f "$raster" ]; then gdal_rasterize -q -burn 0 -l "$LAND_LAYER" "$TMP/land.gpkg" "$raster"; fi
done

# levels ascending as gdal_contour wants them: -11000 ... -2
levels() { echo "$LEVELS" | tr ',' '\n' | awk -v cmp="$1" -v from="$SMOOTH_FROM" \
	'(cmp=="shallow" && $1<from) || (cmp=="deep" && $1>=from) || cmp=="all" {print $1}' | sort -rn | awk '{printf "%s ", -$1}'; }
step "contours at $LEVELS m"
if [ -f "$TMP/smooth.tif" ]; then
	SHALLOW=$(levels shallow); DEEP=$(levels deep)
	# shellcheck disable=SC2086
	if [ -n "$SHALLOW" ]; then gdal_contour -q -a elev -fl $SHALLOW "$TMP/grid.tif" "$TMP/contours.gpkg"; fi
	# shellcheck disable=SC2086
	if [ -n "$DEEP" ]; then gdal_contour -q -a elev -fl $DEEP "$TMP/smooth.tif" "$TMP/deep.gpkg"; fi
	if [ -f "$TMP/deep.gpkg" ]; then
		if [ -f "$TMP/contours.gpkg" ]; then ogr2ogr -q -append -nln contour "$TMP/contours.gpkg" "$TMP/deep.gpkg"
		else mv "$TMP/deep.gpkg" "$TMP/contours.gpkg"; fi
	fi
else
	# shellcheck disable=SC2086
	gdal_contour -q -a elev -fl $(levels all) "$TMP/grid.tif" "$TMP/contours.gpkg"
fi

step "filter and simplify"
python3 "$HERE/depth_contours_filter.py" "$TMP/contours.gpkg" "$TMP/depth.gpkg" --cell "$FINE"

if [ "$(ogrinfo -q -sql 'SELECT COUNT(*) FROM depth_contours' "$TMP/depth.gpkg" | awk -F'= ' '/COUNT/{print $2}')" = 0 ]; then
	rm -rf "$TMP"; step "no contours in $NAME, nothing written"; exit 0
fi

step "osm"
python3 "$V4/ogr2osm.py" -f -t "$V4/translations/contours_depth.py" -o "$TMP/$NAME.osm" "$TMP/depth.gpkg" >/dev/null
gzip -f "$TMP/$NAME.osm"
mv "$TMP/$NAME.osm.gz" "$OUT/$NAME.osm.gz"
mv "$TMP/depth.gpkg" "$OUT/$NAME.gpkg"
if [ -n "$MAP_CREATOR" ]; then
	step "obf"
	# map section only; the file is written to the working directory, named after the input
	(cd "$TMP" && JAVA_OPTS="${JAVA_OPTS:--Xmx8g}" bash "$MAP_CREATOR/utilities.sh" generate-obf-no-address-no-multipolygon \
		"$OUT/$NAME.osm.gz" > obf.log 2>&1) || { tail -20 "$TMP/obf.log"; exit 1; }
	OBF=$(ls "$TMP"/*.obf 2>/dev/null | head -1)
	[ -n "$OBF" ] || { tail -20 "$TMP/obf.log"; echo "no obf written" >&2; exit 1; }
	mv "$OBF" "$OUT/$NAME.depth.obf"
fi
rm -rf "$TMP"
step "done: $(cd "$OUT" && du -h "$NAME".* | awk '{printf "%s (%s) ", $2, $1}')"
