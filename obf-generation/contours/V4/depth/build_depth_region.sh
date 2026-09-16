#!/usr/bin/env bash
# Depth contours and depth points of one region from a depth grid, clipped by the OSM land mask, as .osm.gz and OBF.
#
#   build_depth_region.sh -D DATA_DIR -n REGION [-c MAP_CREATOR_DIR] [-k] [-j JOBS]
#   build_depth_region.sh -n NAME -b "W S E N" -i GRID -m LAND -o OUT_DIR [-l LEVELS] [-p TIERS] [-r CELL]
#                         [-u UPSAMPLE] [-s SMOOTH] [-d SMOOTH_FROM] [-a RESAMPLING] [-g MIN_RING_CELLS] [-t TILE]
#                         [-c MAP_CREATOR_DIR] [-k] [-j JOBS]
#
#   -D DATA_DIR  folder of download_all.sh: the grid is DATA_DIR/src/..., the land DATA_DIR/mask/land_polygons.gpkg,
#                the output DATA_DIR/build; -n then names a region below, which sets the rest
#   -i GRID      raster or VRT with elevation in metres, negative below sea level (GEBCO, EMODnet, CUDEM...);
#                a /vsicurl/ URL works and reads only the region; several comma-separated grids are laid over each
#                other in that order, a later one wins where it has data
#   -m LAND      land polygons (OGR source, e.g. land_polygons.gpkg from check_sources.sh)
#   -l LEVELS    contour depths in metres, default 2,5,10,20,30,50,100,200,500, then every 200 m to 6000 and every
#                500 m to 11000; "none" for a points-only region
#   -p TIERS     depth points: "SPACING:ZOOMS ...", e.g. "0.3:6-9 0.05:10-12 0.02:13-" - the average depth of every
#                SPACING degree cell, shown from ZOOMS; each tier is its own map section. Default none
#   -r CELL      working cell in degrees, default the grid's own
#   -u UPSAMPLE  cubic-spline upsampling factor before contouring, smooths the lines of a coarse grid (default 1)
#   -s SMOOTH    low-pass for the deeper levels: average over SMOOTH x SMOOTH cells, then back (default 4, 1 = off);
#                a flat bottom with sand waves near a level gives hundreds of tiny zigzags without it
#   -d SMOOTH_FROM  levels from this depth down use the smoothed grid, shallower ones the full grid (default 20)
#   -a RESAMPLING   gdalwarp resampling of the cut (default average); bilinear avoids the steps of a coarse grid
#                   (GEBCO) cut to a much finer cell
#   -g MIN_RING_CELLS  drop closed rings shorter than this many cells (default 8)
#   -t TILE      split a region larger than TILE degrees into tiles built in parallel (JOBS at a time); with -c the
#                tiles' .osm.gz become one map section of contours and one per point tier (generate-single-map) in
#                NAME.depth.obf, without -c they stay in OUT_DIR/NAME.tiles
#   -c MAP_CREATOR_DIR  unzipped OsmAndMapCreator: writes OUT_DIR/NAME.depth.obf (points need its --map-zooms)
#   -k           keep: do nothing when OUT_DIR/NAME.depth.obf already exists
#
# Regions (-D DATA_DIR -n REGION), bounds of the published OBFs; options given on the command line win:
#   Netherlands_contours              Rijkswaterstaat 20 m 2024 over NCP 2019 over EMODnet DTM 2024, contours every 5 m
#                                     to 50 m and points, 0.0002 degree cells, 1 degree tiles
#   Europe_contours                   EMODnet DTM 2024, contours every 5 m to 50 m, 10 m to 200 m, 50 m to 1000 m,
#                                     then as the default, 10 degree tiles
#   Europe_points                     EMODnet DTM 2024, points, 10 degree tiles
#   World_contours                    GEBCO_2026 (15"), contours every 10 m to 300 m, 50 m to 1000 m, then as the
#                                     default - 2 and 5 m mean nothing in a 450 m grid, 15 degree tiles
#   World_Northern_hemisphere_points  GEBCO_2026, points, 15 degree tiles
#   World_Southern_hemisphere_points  GEBCO_2026, points, 15 degree tiles
#   Gulf_of_Mexico_north-west_contours  NOAA CUDEM 1/3" near the coast over GEBCO_2026, contours as Europe and
#                                     points, 50 m cells (GEBCO bilinear, rings under 40 cells dropped), 3 degree tiles
#
# Contours: cut the region (EPSG:4326) -> upsample -> smoothed copy -> set land to 0 m by the mask -> gdal_contour ->
# drop short closed rings and simplify (depth_contours_filter.py) -> ogr2osm with translations/contours_depth.py.
# Land is set after smoothing so that it does not pull the sea shallower, and set to 0 m rather than nodata so the
# contours run along the coast instead of stopping short of it.
# Points: land becomes nodata -> average per tier cell (water only) -> cells centred on land dropped ->
# depth_points_osm.py.
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
V4=$(cd "$HERE/.." && pwd)
NAME=""; BBOX=""; GRID=""; LAND=""; OUT=""; CELL=""; UPSAMPLE=1; JOBS=4; SMOOTH=4; SMOOTH_FROM=""; MAP_CREATOR=""
DATA=""; TILE=0; KEEP=0; LEVELS=""; TIERS=""; RESAMPLING=""; MIN_RING_CELLS=""
# steps FROM:TO:STEP... : comma-separated levels
steps() { local s a b c out=""; for s; do IFS=: read -r a b c <<< "$s"; out+=$(seq "$a" "$c" "$b" | paste -sd, -),; done
	echo "${out%,}"; }
DEEP_LEVELS=$(steps 1000:6000:200 6500:11000:500)
EUROPE_LEVELS="2,$(steps 5:50:5 60:200:10 250:950:50),$DEEP_LEVELS"
LAND_NODATA=-32767
while [ $# -gt 0 ]; do
	case "$1" in
		-n) NAME=$2; shift 2 ;;
		-b) BBOX=$2; shift 2 ;;
		-i) GRID=$2; shift 2 ;;
		-m) LAND=$2; shift 2 ;;
		-o) OUT=$2; shift 2 ;;
		-l) LEVELS=$2; shift 2 ;;
		-p) TIERS=$2; shift 2 ;;
		-r) CELL=$2; shift 2 ;;
		-u) UPSAMPLE=$2; shift 2 ;;
		-j) JOBS=$2; shift 2 ;;
		-s) SMOOTH=$2; shift 2 ;;
		-d) SMOOTH_FROM=$2; shift 2 ;;
		-a) RESAMPLING=$2; shift 2 ;;
		-g) MIN_RING_CELLS=$2; shift 2 ;;
		-c) MAP_CREATOR=$2; shift 2 ;;
		-D) DATA=$2; shift 2 ;;
		-t) TILE=$2; shift 2 ;;
		-k) KEEP=1; shift ;;
		-h|--help) sed -n '2,48p' "$0"; exit 0 ;;
		*) echo "Unknown option $1" >&2; exit 1 ;;
	esac
done
if [ -n "$DATA" ]; then
	EMODNET="$DATA/src/emodnet/emodnet_2024.vrt"; GEBCO="$DATA/src/gebco/gebco_2026.vrt"
	GEBCO_TIERS="0.3:6-9 0.05:10-12 0.02:13-"
	case "$NAME" in
		Netherlands_contours)
			NL="$DATA/src/netherlands"
			: "${BBOX:=1.7 51.1 7.3 55.7}"; : "${GRID:=$EMODNET,$NL/bathymetrie_ncp_juni_2019.tif,$NL/bodemhoogte_20mtr_2024.tif}"
			: "${CELL:=0.0002}"; : "${LEVELS:=2,5,10,15,20,25,30,35,40,45,50,100,200}"
			: "${SMOOTH_FROM:=5}"
			: "${TIERS:=0.01:11-12 0.005:13 0.0025:14-}"; [ "$TILE" != 0 ] || TILE=1 ;;
		Europe_contours)
			: "${BBOX:=-31.3 25.4 36.0 71.2}"; : "${GRID:=$EMODNET}"; : "${LEVELS:=$EUROPE_LEVELS}"
			[ "$TILE" != 0 ] || TILE=10 ;;
		Europe_points)
			: "${BBOX:=-36.0 25.0 41.8 83.1}"; : "${GRID:=$EMODNET}"; : "${LEVELS:=none}"
			: "${TIERS:=0.25:7-8 0.1:9 0.04:10 0.02:11-12 0.01:13-}"; [ "$TILE" != 0 ] || TILE=10 ;;
		World_contours)
			: "${BBOX:=-180 -79 180 85}"; : "${GRID:=$GEBCO}"; : "${LEVELS:=$(steps 10:300:10 350:950:50),$DEEP_LEVELS}"
			[ "$TILE" != 0 ] || TILE=15 ;;
		World_Northern_hemisphere_points)
			: "${BBOX:=-180 0 180 85}"; : "${GRID:=$GEBCO}"; : "${LEVELS:=none}"; : "${TIERS:=$GEBCO_TIERS}"
			[ "$TILE" != 0 ] || TILE=15 ;;
		Gulf_of_Mexico_north-west_contours)
			: "${BBOX:=-96.43 25.77 -84.92 29.40}"; : "${GRID:=$GEBCO,$DATA/src/cudem/cudem.vrt}"; : "${CELL:=0.0005}"
			: "${LEVELS:=$EUROPE_LEVELS}"; : "${SMOOTH_FROM:=5}"; : "${RESAMPLING:=bilinear}"; : "${MIN_RING_CELLS:=40}"
			: "${TIERS:=0.05:9-10 0.02:11-12 0.01:13 0.005:14-}"; [ "$TILE" != 0 ] || TILE=3 ;;
		World_Southern_hemisphere_points)
			: "${BBOX:=-180 -79 180 0}"; : "${GRID:=$GEBCO}"; : "${LEVELS:=none}"; : "${TIERS:=$GEBCO_TIERS}"
			[ "$TILE" != 0 ] || TILE=15 ;;
		*) echo "Unknown region '$NAME', see --help" >&2; exit 1 ;;
	esac
	: "${LAND:=$DATA/mask/land_polygons.gpkg}"; : "${OUT:=$DATA/build}"
fi
: "${LEVELS:=2,5,10,20,30,50,100,200,500,$DEEP_LEVELS}"; : "${SMOOTH_FROM:=20}"; : "${RESAMPLING:=average}"; : "${MIN_RING_CELLS:=8}"
[ "$LEVELS" != none ] || LEVELS=""
for v in NAME BBOX GRID LAND OUT; do
	[ -n "${!v}" ] || { echo "Missing $v, see --help" >&2; exit 1; }
done
[ -n "$LEVELS$TIERS" ] || { echo "Nothing to build: no contour levels and no point tiers" >&2; exit 1; }
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

# obf OSM_GZ [ZOOMS] : OBF with the map section only, printed path; the file is written to the working directory,
# named after the input
obf() {
	local osm=$1 zooms=${2:-} dir
	dir=$(mktemp -d "$TMP/obf.XXXX")
	(cd "$dir" && JAVA_OPTS="${JAVA_OPTS:--Xmx8g}" bash "$MAP_CREATOR/utilities.sh" generate-map "$osm" \
		${zooms:+--map-zooms=$zooms} > obf.log 2>&1) || { tail -20 "$dir/obf.log" >&2; return 1; }
	ls "$dir"/*.obf 2>/dev/null | head -1 | grep . || { tail -20 "$dir/obf.log" >&2; return 1; }
}

# single OUTPUT OSM_GZ... [--map-zooms=ZOOMS] : one map section of many osm files
single() {
	local output=$1; shift
	local dir; dir=$(mktemp -d "$TMP/single.XXXX")
	(cd "$dir" && JAVA_OPTS="${JAVA_OPTS:--Xmx16g}" bash "$MAP_CREATOR/utilities.sh" generate-single-map "$output" \
		--name="$NAME" "$@" > single.log 2>&1) || { tail -20 "$dir/single.log" >&2; return 1; }
	rm -rf "$dir"
}

# merge OUTPUT OBF... : one OBF of all their map sections
merge() {
	local output=$1; shift
	if [ $# -eq 1 ]; then mv "$1" "$output"; return; fi
	rm -f "$output"
	# in a folder of its own: merge-index keeps its POI database in the working folder under a name taken from the
	# output, which parallel tiles of one region share
	local dir; dir=$(mktemp -d "$TMP/merge.XXXX")
	(cd "$dir" && JAVA_OPTS="${JAVA_OPTS:--Xmx16g}" bash "$MAP_CREATOR/utilities.sh" merge-index "$output" "$@" > merge.log 2>&1) \
		|| { tail -20 "$dir/merge.log" >&2; return 1; }
	rm -rf "$dir"
}

if [ -z "$CELL" ]; then
	CELL=$(gdalinfo -json "${GRID%%,*}" | python3 -c 'import json,sys; print(abs(json.load(sys.stdin)["geoTransform"][1]))')
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
	export SELF="$0" GRID LAND LEVELS TIERS CELL UPSAMPLE SMOOTH SMOOTH_FROM RESAMPLING MIN_RING_CELLS MAP_CREATOR TILE_OUT
	echo "$TILES" | xargs -P "$JOBS" -L 1 bash -c '
		name=$1; shift
		args=(-n "$name" -b "$*" -i "$GRID" -m "$LAND" -o "$TILE_OUT" -l "${LEVELS:-none}" -p "$TIERS" -r "$CELL" \
			-u "$UPSAMPLE" -s "$SMOOTH" -d "$SMOOTH_FROM" -a "$RESAMPLING" -g "$MIN_RING_CELLS" -j 1)
		JAVA_OPTS="${JAVA_OPTS:--Xmx2g}" bash "$SELF" "${args[@]}" > "$TILE_OUT/$name.log" 2>&1 \
			|| { echo "FAILED tile $name:"; tail -20 "$TILE_OUT/$name.log"; exit 255; }
		echo "tile $name: $(tail -1 "$TILE_OUT/$name.log")"' _
	if [ -n "$MAP_CREATOR" ]; then
		OBFS=()
		contours=("$TILE_OUT"/*_[0-9][0-9]_[0-9][0-9].osm.gz)
		if [ -f "${contours[0]}" ]; then
			step "contours obf of ${#contours[@]} tiles"
			single "$TMP/contours.obf" "${contours[@]}"; OBFS+=("$TMP/contours.obf")
		fi
		i=0
		for tier in $TIERS; do
			i=$((i + 1)); points=("$TILE_OUT"/*_points$i.osm.gz)
			[ -f "${points[0]}" ] || continue
			step "points $i obf of ${#points[@]} tiles"
			single "$TMP/points$i.obf" --map-zooms="${tier#*:}" "${points[@]}"; OBFS+=("$TMP/points$i.obf")
		done
		[ ${#OBFS[@]} -gt 0 ] || { echo "no tile has depth data" >&2; exit 1; }
		step "merge ${#OBFS[@]} map sections"
		merge "$OUT/$NAME.depth.obf" "${OBFS[@]}"
	else
		rm -rf "$OUT/$NAME.tiles"; mkdir -p "$OUT/$NAME.tiles"
		mv "$TILE_OUT"/*.osm.gz "$OUT/$NAME.tiles/" 2>/dev/null || true
	fi
	rm -rf "$TMP"
	step "done: $(cd "$OUT" && du -sh "$NAME".* | awk '{printf "%s (%s) ", $2, $1}')"
	exit 0
fi

step "cut $NAME ($W $S $E $N), cell $CELL deg, upsampled x$UPSAMPLE"
gdalwarp -q -overwrite -t_srs EPSG:4326 -te "$W" "$S" "$E" "$N" -tr "$CELL" "$CELL" -r "$RESAMPLING" -ot Float32 \
	-dstnodata nan -multi -wo NUM_THREADS="$JOBS" -co COMPRESS=DEFLATE -co TILED=YES ${GRID//,/ } "$TMP/grid.tif"
step "land mask"
ogr2ogr -q -f GPKG -spat "$W" "$S" "$E" "$N" -clipsrc "$W" "$S" "$E" "$N" -nlt MULTIPOLYGON "$TMP/land.gpkg" "$LAND"
LAND_LAYER=$(ogrinfo -q "$TMP/land.gpkg" | awk -F'[: ]+' 'NR==1{print $2}')
OBFS=()

if [ -n "$LEVELS" ]; then
	cp "$TMP/grid.tif" "$TMP/contour_grid.tif"
	if [ "$UPSAMPLE" != 1 ]; then
		gdalwarp -q -overwrite -tr "$FINE" "$FINE" -r cubicspline -multi -wo NUM_THREADS="$JOBS" \
			-co COMPRESS=DEFLATE -co TILED=YES "$TMP/grid.tif" "$TMP/contour_grid.tif"
	fi
	if [ "$SMOOTH" != 1 ]; then
		step "smoothed copy for levels from $SMOOTH_FROM m: average over $SMOOTH cells"
		COARSE=$(python3 -c "print(float('$FINE') * float('$SMOOTH'))")
		gdalwarp -q -overwrite -tr "$COARSE" "$COARSE" -r average -multi -wo NUM_THREADS="$JOBS" \
			"$TMP/contour_grid.tif" "$TMP/coarse.tif"
		gdalwarp -q -overwrite -te "$W" "$S" "$E" "$N" -tr "$FINE" "$FINE" -r cubicspline -multi -wo NUM_THREADS="$JOBS" \
			-co COMPRESS=DEFLATE -co TILED=YES "$TMP/coarse.tif" "$TMP/smooth.tif"
		rm -f "$TMP/coarse.tif"
	fi
	for raster in "$TMP/contour_grid.tif" "$TMP/smooth.tif"; do
		if [ -f "$raster" ]; then gdal_rasterize -q -burn 0 -l "$LAND_LAYER" "$TMP/land.gpkg" "$raster"; fi
	done

	# levels ascending as gdal_contour wants them: -11000 ... -2
	levels() { echo "$LEVELS" | tr ',' '\n' | awk -v cmp="$1" -v from="$SMOOTH_FROM" \
		'(cmp=="shallow" && $1<from) || (cmp=="deep" && $1>=from) || cmp=="all" {print $1}' | sort -rn | awk '{printf "%s ", -$1}'; }
	step "contours at $LEVELS m"
	if [ -f "$TMP/smooth.tif" ]; then
		SHALLOW=$(levels shallow); DEEP=$(levels deep)
		# shellcheck disable=SC2086
		if [ -n "$SHALLOW" ]; then gdal_contour -q -a elev -fl $SHALLOW "$TMP/contour_grid.tif" "$TMP/contours.gpkg"; fi
		# shellcheck disable=SC2086
		if [ -n "$DEEP" ]; then gdal_contour -q -a elev -fl $DEEP "$TMP/smooth.tif" "$TMP/deep.gpkg"; fi
		if [ -f "$TMP/deep.gpkg" ]; then
			if [ -f "$TMP/contours.gpkg" ]; then ogr2ogr -q -append -nln contour "$TMP/contours.gpkg" "$TMP/deep.gpkg"
			else mv "$TMP/deep.gpkg" "$TMP/contours.gpkg"; fi
		fi
	else
		# shellcheck disable=SC2086
		gdal_contour -q -a elev -fl $(levels all) "$TMP/contour_grid.tif" "$TMP/contours.gpkg"
	fi
	rm -f "$TMP/contour_grid.tif" "$TMP/smooth.tif"

	step "filter and simplify"
	python3 "$HERE/depth_contours_filter.py" "$TMP/contours.gpkg" "$TMP/depth.fgb" --cell "$FINE" --min-ring-cells "$MIN_RING_CELLS"
	if [ "$(ogrinfo -so -al "$TMP/depth.fgb" | awk -F': ' '/Feature Count/{print $2}')" = 0 ]; then
		step "no contours in $NAME"
	else
		step "contours osm"
		python3 "$V4/ogr2osm.py" -f -t "$V4/translations/contours_depth.py" -o "$TMP/$NAME.osm" "$TMP/depth.fgb" >/dev/null
		gzip -f "$TMP/$NAME.osm"
		mv "$TMP/$NAME.osm.gz" "$OUT/$NAME.osm.gz"
		mv "$TMP/depth.fgb" "$OUT/$NAME.fgb"
		if [ -n "$MAP_CREATOR" ]; then
			step "contours obf"
			o=$(obf "$OUT/$NAME.osm.gz"); OBFS+=("$o")
		fi
	fi
fi

if [ -n "$TIERS" ]; then
	# land as nodata, so that averages are over water only
	gdal_rasterize -q -burn "$LAND_NODATA" -l "$LAND_LAYER" "$TMP/land.gpkg" "$TMP/grid.tif"
	gdalwarp -q -overwrite -srcnodata "$LAND_NODATA" -dstnodata nan -co COMPRESS=DEFLATE -co TILED=YES \
		"$TMP/grid.tif" "$TMP/water.tif"
	i=0
	for tier in $TIERS; do
		i=$((i + 1)); spacing=${tier%%:*}; zooms=${tier#*:}
		step "points every $spacing deg from zoom $zooms"
		# cells aligned to the spacing everywhere (-tap), so neighbouring tiles share one grid
		gdalwarp -q -overwrite -tap -te "$W" "$S" "$E" "$N" -tr "$spacing" "$spacing" -r average -ot Float32 \
			-srcnodata nan -dstnodata nan "$TMP/water.tif" "$TMP/tier.tif"
		# a cell centred on land is dropped even if some water around it was averaged
		gdal_rasterize -q -burn nan -l "$LAND_LAYER" "$TMP/land.gpkg" "$TMP/tier.tif"
		osm="$OUT/${NAME}_points$i.osm.gz"
		python3 "$HERE/depth_points_osm.py" "$TMP/tier.tif" "$osm" --bbox "$W" "$S" "$E" "$N" --first-id $((i * 100000000))
		if [ "$(zcat < "$osm" | grep -c -m1 '<node')" = 0 ]; then
			rm -f "$osm"; continue
		fi
		if [ -n "$MAP_CREATOR" ]; then
			o=$(obf "$osm" "$zooms"); OBFS+=("$o")
		fi
	done
fi

if [ -n "$MAP_CREATOR" ]; then
	if [ ${#OBFS[@]} -eq 0 ]; then
		rm -rf "$TMP"; step "no depth data in $NAME, nothing written"; exit 0
	fi
	step "merge ${#OBFS[@]} map sections"
	merge "$OUT/$NAME.depth.obf" "${OBFS[@]}"
fi
rm -rf "$TMP"
step "done: $(cd "$OUT" && du -h "$NAME".* "$NAME"_points* 2>/dev/null | awk '{printf "%s (%s) ", $2, $1}')"
