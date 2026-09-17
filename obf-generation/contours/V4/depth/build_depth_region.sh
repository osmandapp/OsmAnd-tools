#!/usr/bin/env bash
# Depth contours and depth points of one region from a depth grid, clipped by the OSM land mask, as .osm.gz and OBF.
#
#   build_depth_region.sh -D DATA_DIR -n REGION [-c MAP_CREATOR_DIR] [-k] [-j JOBS]
#   build_depth_region.sh -n NAME -b "W S E N" -i GRID -m LAND -o OUT_DIR [-l LEVELS] [-p TIERS] [-r CELL]
#                         [-u UPSAMPLE] [-s SMOOTH] [-d SMOOTH_FROM] [-a RESAMPLING] [-g MIN_RING_CELLS] [-t TILE]
#                         [-w OVERVIEW] [-x EXCLUDE [-X LAYER]] [-e ENC_DIR] [-F FILL] [-N NO_DETAILED]
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
#   -a RESAMPLING   gdalwarp resampling of the cut (default average); bilinear avoids the steps of a coarse grid;
#                   auto: every grid on its own, average when its cells are about the cell or finer, bilinear when coarser
#                   (EMODnet under a 20 m survey), then laid over each other
#                   (GEBCO) cut to a much finer cell
#   -g MIN_RING_CELLS  drop closed rings shorter than this many cells (default 8)
#   -w OVERVIEW  "CELL:ZOOMS", e.g. "0.02:5-8": the levels of 200 m and deeper again from the grid averaged to CELL
#                degrees, as their own map section shown at ZOOMS - the main contours start at zoom 9. Default none
#   -x EXCLUDE   polygons (OGR source, layer -X or the first) where another region has better data: no contours and
#                no points there, the overview stays
#   -e ENC_DIR   S-57 ENC cells (NOAA ENC_ROOT): inside the approach and harbour cells (bands 4-6) the charted contours
#                and soundings (depth_enc_osm.py) replace the grid; soundings thinned as ENC_TIERS, the charted levels
#                with no contourtype (0.9, 3.6 m...) only from zoom 15, depth areas as the fill (depth_areas_osm.py)
#   -F FILL      depth areas (fill), for grids in chart datum: "GRID[,PLUS,MINUS]", elevation = GRID + PLUS - MINUS
#                brings a grid in a land datum to chart datum (NAP + NLGEO2018 - NLLAT2018 = LAT); the shifted grid
#                replaces GRID in -i, so the contours are in chart datum too. Only where GRID has data (no EMODnet
#                fill), the bands dries, 0-2, 2-5,
#                5-10 m come from the same rasters as the contours of those levels (smoothed from SMOOTH_FROM), so
#                their edges lie on the contours; depth_areas_osm.py --nested
#   -t TILE      split a region larger than TILE degrees into tiles built in parallel (JOBS at a time); with -c the
#                tiles' .osm.gz become one map section of contours, one of the overview and one per point tier
#                (generate-single-map) in
#                NAME.depth.obf, without -c they stay in OUT_DIR/NAME.tiles; env TILE_WITH=RASTER keeps only the tiles
#                touching one of that VRT's files
#   -N NO_DETAILED  "OUTPUT_NAME:COVERAGE[,COVERAGE...][:OVERVIEW_COVERAGE,...]", tiles with -c only: a second map
#                OUT_DIR/OUTPUT_NAME.depth.obf of the same tiles with the coverages of detailed maps cut out
#                (depth_osm_exclude.py: soundings inside dropped, contours cut at the edge); the overview section is
#                cut by OVERVIEW_COVERAGEs only, kept whole without them. Europe_contours makes Europe_no_detailed,
#                World_contours World_no_eu_detailed, with coverages cached in DATA_DIR/src/coverage (depth_coverage.py)
#   -c MAP_CREATOR_DIR  unzipped OsmAndMapCreator: writes OUT_DIR/NAME.depth.obf (points need its --map-zooms)
#   -k           keep: do nothing when OUT_DIR/NAME.depth.obf already exists
#   env RENDERING_TYPES  rendering_types.xml for OsmAndMapCreator instead of its own (new tags before a nightly)
#
# Regions (-D DATA_DIR -n REGION), bounds of the published OBFs; options given on the command line win:
#   Netherlands_contours              Rijkswaterstaat 20 m 2024 over NCP 2019 over EMODnet DTM 2024, contours every 5 m
#                                     to 50 m and points, 0.0002 degree cells, 1 degree tiles; fill from the 20 m grid
#                                     in LAT (NLLAT2018)
#   Ireland_contours                  INFOMAR 10 m inshore over 25 m over EMODnet DTM 2024, LAT, as Netherlands_contours;
#                                     fill from the INFOMAR grids
#   France_contours                   SHOM coastal DTMs 5-20 m (11 zones) over EMODnet DTM 2024, chart datum, as
#                                     Netherlands_contours, only the 1 degree tiles touching a zone; fill from SHOM
#   Great_Britain_contours            UKHO ADMIRALTY surveys (BAG, averaged to 0.0002 degrees) over EMODnet DTM 2024,
#                                     LAT, as France_contours: only the 1 degree tiles touching a survey
#   Denmark_contours                  (not published, small gain) Danmarks Dybdemodel 50 m 2024 over EMODnet DTM 2024,
#                                     mean sea level, contours as
#                                     Netherlands_contours, 0.0005 degree cells (bilinear), 1 degree tiles, no fill
#   Europe_contours                   EMODnet DTM 2024, contours every 5 m to 50 m, 10 m to 200 m, 50 m to 1000 m,
#                                     then as the default, overview 0.02 degrees for zooms 5-8, 10 degree tiles;
#                                     Europe_* leave out the Kartverket coverage (Norway_contours) when it is downloaded;
#                                     also Europe_no_detailed.depth.obf: without Ireland, France, Great Britain (surveys),
#                                     Netherlands and Norway coverage
#   Europe_points                     EMODnet DTM 2024, points, 10 degree tiles
#   Norway_contours                   Kartverket Sjøkart - Dybdedata (vector), see build_depth_kartverket.sh
#   New-zealand_contours              LINZ chart vector data, 5 scale bands merged by depth_bands_merge.py (the largest
#                                     scale wins), then as Norway_contours; main islands only (165..180 E)
#   World_contours                    GEBCO_2026 (15"), contours every 10 m to 300 m, 50 m to 1000 m, then as the
#                                     default - 2 and 5 m mean nothing in a 450 m grid, overview 0.02 degrees for
#                                     zooms 5-8, 15 degree tiles; also World_no_eu_detailed.depth.obf: without EMODnet
#                                     (Europe_*, overview too), the Europe_no_detailed regions, New Zealand and the
#                                     CUDEM of Gulf_of_Mexico_north-west
#   World_Northern_hemisphere_points  GEBCO_2026, points, 15 degree tiles
#   World_Southern_hemisphere_points  GEBCO_2026, points, 15 degree tiles
#   Gulf_of_Mexico_north-west_contours  NOAA CUDEM 1/3" near the coast over GEBCO_2026, contours as Europe and
#                                     points, 50 m cells (GEBCO bilinear, rings under 40 cells dropped), overview as
#                                     Europe, 3 degree tiles; NOAA ENC inside its approach and harbour charts
#
# Contours: cut the region (EPSG:4326) -> upsample -> smoothed copy -> set land to 0 m by the mask -> gdal_contour ->
# drop short closed rings and simplify (depth_contours_filter.py) -> ogr2osm with translations/contours_depth.py.
# Land is set after smoothing so that it does not pull the sea shallower, and set to 0 m rather than nodata so the
# contours run along the coast instead of stopping short of it.
# Points: land becomes nodata -> depth_points_osm.py --shoalest: per tier cell the shoalest water cell, placed where
# it is.
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
V4=$(cd "$HERE/.." && pwd)
NAME=""; BBOX=""; GRID=""; LAND=""; OUT=""; CELL=""; UPSAMPLE=1; JOBS=4; SMOOTH=4; SMOOTH_FROM=""; MAP_CREATOR=""
DATA=""; TILE=0; TILE_WITH="${TILE_WITH:-}"; KEEP=0; LEVELS=""; TIERS=""; RESAMPLING=""; MIN_RING_CELLS=""; OVERVIEW=""
NO_DETAILED=""; EXCLUDE=""; EXCLUDE_LAYER=""; ENC=""; ENC_AREAS=""; FILL=""; ENC_TIERS="0.02:10-11 0.008:12 0.003:13-14 0.001:15-"
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
		-w) OVERVIEW=$2; shift 2 ;;
		-x) EXCLUDE=$2; shift 2 ;;
		-X) EXCLUDE_LAYER=$2; shift 2 ;;
		-e) ENC=$2; shift 2 ;;
		-F) FILL=$2; shift 2 ;;
		-N) NO_DETAILED=$2; shift 2 ;;
		-h|--help) sed -n '2,95p' "$0"; exit 0 ;;
		*) echo "Unknown option $1" >&2; exit 1 ;;
	esac
done
if [ -n "$DATA" ]; then
	EMODNET="$DATA/src/emodnet/emodnet_2024.vrt"; GEBCO="$DATA/src/gebco/gebco_2026.vrt"
	GEBCO_TIERS="0.3:6-9 0.05:10-12 0.02:13-"; OVERVIEW_Z5_8="0.02:5-8"
	KARTVERKET=$(ls -d "$DATA"/src/norway/*.gdb 2>/dev/null | head -1 || true)
	NL="$DATA/src/netherlands"
	# coverage NAME "OPTIONS" SOURCE... : DATA/src/coverage/NAME.gpkg of depth_coverage.py, rebuilt when the sources
	# change (content of a small file such as a VRT, size and time of a large one); COVERAGE is set to it. A missing
	# source fails the build
	coverage() {
		local name=$1 opts=$2 src sig; shift 2
		COVERAGE="$DATA/src/coverage/$name.gpkg"
		for src in "$@"; do
			[ -e "$src" ] || { echo "$NAME: no $src for the $name coverage" >&2; exit 1; }
		done
		sig=$(python3 - "$opts" "$@" <<-'PY'
		import hashlib, os, sys
		h = hashlib.md5(sys.argv[1].encode())
		for p in sys.argv[2:]:
		    st = os.stat(p)
		    h.update(open(p, 'rb').read() if os.path.isfile(p) and st.st_size < 1 << 20 else b'%d %d' % (st.st_size, st.st_mtime))
		print(h.hexdigest())
		PY
		)
		if [ "$(cat "$COVERAGE.sig" 2>/dev/null)" != "$sig" ]; then rm -f "$COVERAGE"; fi
		if [ ! -f "$COVERAGE" ]; then
			echo "== coverage $name"
			mkdir -p "$DATA/src/coverage"
			# shellcheck disable=SC2086
			python3 "$HERE/depth_coverage.py" $opts "$@" "$COVERAGE.tmp.gpkg" 2>&1 | grep -v numpy
			mv "$COVERAGE.tmp.gpkg" "$COVERAGE"; echo "$sig" > "$COVERAGE.sig"
		fi
	}
	detailed_europe() {
		local c=""
		coverage ireland "" "$DATA/src/ireland/ireland.vrt"; c+=",$COVERAGE"
		coverage france "" "$DATA/src/france/france.vrt"; c+=",$COVERAGE"
		coverage uk "" "$DATA/src/uk/uk.vrt"; c+=",$COVERAGE"
		coverage netherlands "" "$NL/bathymetrie_ncp_juni_2019.tif" "$NL/bodemhoogte_20mtr_2024.tif"; c+=",$COVERAGE"
		coverage norway "--layer datakvalitet" "${KARTVERKET:-$DATA/src/norway/Dybdedata.gdb}"; c+=",$COVERAGE"
		DETAILED_EUROPE=${c#,}
	}
	case "$NAME" in
		Norway_contours)
			exec "$HERE/build_depth_kartverket.sh" -D "$DATA" -n "$NAME" -j "$JOBS" ${MAP_CREATOR:+-c "$MAP_CREATOR"} \
				$([ $KEEP -eq 1 ] && echo -k) ;;
		New-zealand_contours)
			# LINZ chart vector data: the scale bands merged (the largest scale wins), then built as Norway
			: "${OUT:=$DATA/build}"
			if [ ! -f "$DATA/src/nz/linz_hydro.gpkg" ]; then
				echo "$NAME: no src/nz/linz_hydro.gpkg (download_all.sh --only nz, needs LINZ_API_KEY)" >&2; exit 1
			fi
			if [ $KEEP -eq 1 ] && [ -f "$OUT/$NAME.depth.obf" ]; then echo "== $NAME.depth.obf exists, kept"; exit 0; fi
			mkdir -p "$OUT"
			python3 "$HERE/depth_bands_merge.py" "$DATA/src/nz/linz_hydro.gpkg" "$OUT/$NAME.bands.gpkg" \
				--bbox ${BBOX:-165 -48 180 -33.5} 2>&1 | grep -v numpy
			"$HERE/build_depth_kartverket.sh" -n "$NAME" -i "$OUT/$NAME.bands.gpkg" -m "$DATA/mask/land_polygons.gpkg" \
				-o "$OUT" -j "$JOBS" ${MAP_CREATOR:+-c "$MAP_CREATOR"}
			# the merged bands (about 0.3 GB) are only an intermediate
			rm -f "$OUT/$NAME.bands.gpkg"; exit 0 ;;
		Netherlands_contours)
			: "${BBOX:=1.7 51.1 7.3 55.7}"; : "${GRID:=$EMODNET,$NL/bathymetrie_ncp_juni_2019.tif,$NL/bodemhoogte_20mtr_2024.tif}"
			: "${RESAMPLING:=auto}"; : "${CELL:=0.0002}"; : "${LEVELS:=2,5,10,15,20,25,30,35,40,45,50,100,200}"
			: "${SMOOTH_FROM:=5}"
			: "${TIERS:=0.01:11-12 0.005:13 0.0025:14-}"; [ "$TILE" != 0 ] || TILE=1
			if [ -f "$NL/nl_nsgi_nllat2018.tif" ]; then
				: "${FILL:=$NL/bodemhoogte_20mtr_2024.tif,$NL/nl_nsgi_nlgeo2018.tif,$NL/nl_nsgi_nllat2018.tif}"
			fi ;;
		Ireland_contours)
			# INFOMAR grids are LAT already: the fill needs no datum shift
			IE="$DATA/src/ireland/ireland.vrt"
			: "${BBOX:=-11.8 51.2 -5.3 55.6}"; : "${GRID:=$EMODNET,$IE}"; : "${FILL:=$IE}"
			: "${RESAMPLING:=auto}"; : "${CELL:=0.0002}"; : "${LEVELS:=2,5,10,15,20,25,30,35,40,45,50,100,200}"; : "${SMOOTH_FROM:=5}"
			: "${TIERS:=0.01:11-12 0.005:13 0.0025:14-}"; [ "$TILE" != 0 ] || TILE=1 ;;
		France_contours)
			# SHOM coastal DTMs in chart datum (PBMA), scattered along the Channel and Atlantic coast: only the 1 degree
			# tiles touching one of them are built
			FR="$DATA/src/france/france.vrt"
			: "${BBOX:=-5.5 43.3 2.6 51.2}"; : "${GRID:=$EMODNET,$FR}"; : "${FILL:=$FR}"; : "${TILE_WITH:=$FR}"
			: "${RESAMPLING:=auto}"; : "${CELL:=0.0002}"; : "${LEVELS:=2,5,10,15,20,25,30,35,40,45,50,100,200}"; : "${SMOOTH_FROM:=5}"
			: "${TIERS:=0.01:11-12 0.005:13 0.0025:14-}"; [ "$TILE" != 0 ] || TILE=1 ;;
		Great_Britain_contours)
			# UKHO surveys downloaded by hand (download_all.sh uk): only the 1 degree tiles touching one of them
			UK="$DATA/src/uk/uk.vrt"
			if [ ! -f "$UK" ]; then
				echo "$NAME: no src/uk/uk.vrt (surveys into src/uk/incoming, download_all.sh --only uk, check_sources.sh)" >&2
				exit 1
			fi
			: "${BBOX:=-8.7 49.8 2.0 60.9}"; : "${GRID:=$EMODNET,$UK}"; : "${FILL:=$UK}"; : "${TILE_WITH:=$UK}"
			: "${RESAMPLING:=auto}"; : "${CELL:=0.0002}"; : "${LEVELS:=2,5,10,15,20,25,30,35,40,45,50,100,200}"; : "${SMOOTH_FROM:=5}"
			: "${TIERS:=0.01:11-12 0.005:13 0.0025:14-}"; [ "$TILE" != 0 ] || TILE=1 ;;
		Denmark_contours)
			# not published: 50 m against EMODnet's 115 m is a small gain, and a separate region would overlap
			# Europe_contours. Mean sea level, not chart datum: no fill, a drying band would be wrong on the tidal west
			# coast
			DK="$DATA/src/denmark/denmark.vrt"
			: "${BBOX:=7.5 54.4 15.6 57.9}"; : "${GRID:=$EMODNET,$DK}"; : "${CELL:=0.0005}"; : "${RESAMPLING:=bilinear}"
			: "${LEVELS:=2,5,10,15,20,25,30,35,40,45,50,100,200}"; : "${SMOOTH_FROM:=5}"
			: "${TIERS:=0.02:10-11 0.01:12 0.005:13-}"; [ "$TILE" != 0 ] || TILE=1 ;;
		Europe_contours)
			: "${BBOX:=-31.3 25.4 36.0 71.2}"; : "${GRID:=$EMODNET}"; : "${LEVELS:=$EUROPE_LEVELS}"
			: "${OVERVIEW:=$OVERVIEW_Z5_8}"
			if [ -n "$KARTVERKET" ]; then : "${EXCLUDE:=$KARTVERKET}"; : "${EXCLUDE_LAYER:=datakvalitet}"; fi
			if [ -z "$NO_DETAILED" ]; then detailed_europe; NO_DETAILED="Europe_no_detailed:$DETAILED_EUROPE"; fi
			[ "$TILE" != 0 ] || TILE=10 ;;
		Europe_points)
			: "${BBOX:=-36.0 25.0 41.8 83.1}"; : "${GRID:=$EMODNET}"; : "${LEVELS:=none}"
			: "${TIERS:=0.25:7-8 0.1:9 0.04:10 0.02:11-12 0.01:13-}"; [ "$TILE" != 0 ] || TILE=10
			if [ -n "$KARTVERKET" ]; then : "${EXCLUDE:=$KARTVERKET}"; : "${EXCLUDE_LAYER:=datakvalitet}"; fi ;;
		World_contours)
			: "${BBOX:=-180 -79 180 85}"; : "${GRID:=$GEBCO}"; : "${LEVELS:=$(steps 10:300:10 350:950:50),$DEEP_LEVELS}"
			: "${OVERVIEW:=$OVERVIEW_Z5_8}"
			if [ -z "$NO_DETAILED" ]; then
				detailed_europe
				coverage emodnet "--cell 0.02" "$EMODNET"; emodnet=$COVERAGE
				# the box of New-zealand_contours: LINZ also has the Pacific islands and the Ross Sea
				coverage new-zealand "--bbox 165 -48 180 -33.5 --layer area_1 --layer area_2 --layer area_3 --layer area_4 --layer area_5" \
					"$DATA/src/nz/linz_hydro.gpkg"; nz=$COVERAGE
				coverage gulf-cudem "--bbox -96.43 25.77 -84.92 29.40" "$DATA/src/cudem/cudem.vrt"; gulf=$COVERAGE
				NO_DETAILED="World_no_eu_detailed:$emodnet,$DETAILED_EUROPE,$nz,$gulf:$emodnet"
			fi
			[ "$TILE" != 0 ] || TILE=15 ;;
		World_Northern_hemisphere_points)
			: "${BBOX:=-180 0 180 85}"; : "${GRID:=$GEBCO}"; : "${LEVELS:=none}"; : "${TIERS:=$GEBCO_TIERS}"
			[ "$TILE" != 0 ] || TILE=15 ;;
		Gulf_of_Mexico_north-west_contours)
			: "${BBOX:=-96.43 25.77 -84.92 29.40}"; : "${GRID:=$GEBCO,$DATA/src/cudem/cudem.vrt}"; : "${CELL:=0.0005}"
			: "${LEVELS:=$EUROPE_LEVELS}"; : "${SMOOTH_FROM:=5}"; : "${RESAMPLING:=bilinear}"; : "${MIN_RING_CELLS:=40}"
			: "${TIERS:=0.05:9-10 0.02:11-12 0.01:13 0.005:14-}"; : "${OVERVIEW:=$OVERVIEW_Z5_8}"
			if [ -d "$DATA/src/noaa_enc/ENC_ROOT" ]; then : "${ENC:=$DATA/src/noaa_enc/ENC_ROOT}"; fi
			[ "$TILE" != 0 ] || TILE=3 ;;
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
# a Python segfault inside GDAL prints the Python stack
export PYTHONFAULTHANDLER=1
export GDAL_NUM_THREADS=$JOBS GDAL_HTTP_MAX_RETRY=5 GDAL_HTTP_RETRY_DELAY=5 GDAL_DISABLE_READDIR_ON_OPEN=EMPTY_DIR
started=$(date +%s)
step() { echo "== $(( $(date +%s) - started ))s $*"; }

# obf OSM_GZ [ZOOMS] : OBF with the map section only, printed path; the file is written to the working directory,
# named after the input
obf() {
	local osm=$1 zooms=${2:-} dir
	dir=$(mktemp -d "$TMP/obf.XXXX")
	(cd "$dir" && JAVA_OPTS="${JAVA_OPTS:--Xmx8g}" bash "$MAP_CREATOR/utilities.sh" generate-map "$osm" \
		${zooms:+--map-zooms=$zooms} ${RENDERING_TYPES:+--rendering-types=$RENDERING_TYPES} > obf.log 2>&1) || { tail -20 "$dir/obf.log" >&2; return 1; }
	ls "$dir"/*.obf 2>/dev/null | head -1 | grep . || { tail -20 "$dir/obf.log" >&2; return 1; }
}

# single OUTPUT OSM_GZ... [--map-zooms=ZOOMS] : one map section of many osm files
single() {
	local output=$1; shift
	local dir; dir=$(mktemp -d "$TMP/single.XXXX")
	(cd "$dir" && JAVA_OPTS="${JAVA_OPTS:--Xmx16g}" bash "$MAP_CREATOR/utilities.sh" generate-single-map "$output" \
		--name="$NAME" "$@" ${RENDERING_TYPES:+--rendering-types=$RENDERING_TYPES} > single.log 2>&1) || { tail -20 "$dir/single.log" >&2; return 1; }
	# an old OsmAndMapCreator prints its usage for an unknown command and exits with 0
	[ -f "$output" ] || { tail -20 "$dir/single.log" >&2; return 1; }
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

# ENC: charted contours and soundings where approach and harbour cells exist; the grid is left out there (-x)
ENC_OSM=()
if [ -n "$ENC" ]; then
	step "ENC cells of $ENC"
	python3 "$HERE/depth_enc_osm.py" "$ENC" --bbox "$W" "$S" "$E" "$N" --contours "$OUT/${NAME}_enc.osm.gz" \
		--minor "$OUT/${NAME}_enc_minor.osm.gz" --areas "$TMP/enc_areas.gpkg" --soundings "$TMP/enc_soundings.gpkg" --coverage "$TMP/enc_coverage.gpkg" 2>&1 | grep -v numpy
	EXCLUDE="$TMP/enc_coverage.gpkg"; EXCLUDE_LAYER=coverage
	ENC_OSM+=("$OUT/${NAME}_enc.osm.gz:" "$OUT/${NAME}_enc_minor.osm.gz:15-")
	python3 "$HERE/depth_areas_osm.py" "$TMP/enc_areas.gpkg" "$OUT/${NAME}_enc_areas.osm.gz" --layer areas \
		--field mindepth --land "$LAND" --bbox "$W" "$S" "$E" "$N" 2>&1 | grep -v numpy
	# multipolygons: generate-map, not generate-single-map
	ENC_AREAS="$OUT/${NAME}_enc_areas.osm.gz"
	spacings=""; for tier in $ENC_TIERS; do spacings+="${tier%%:*} "; done
	python3 "$HERE/depth_soundings_osm.py" "$TMP/enc_soundings.gpkg" "$OUT/${NAME}_enc_points" --layer soundings \
		--field depth --tiers "$spacings" --land "$LAND" --land-cell 0.0001 --bbox "$W" "$S" "$E" "$N" --first-id 900000000 2>&1 | grep -v numpy
	i=0
	for tier in $ENC_TIERS; do i=$((i + 1)); ENC_OSM+=("$OUT/${NAME}_enc_points$i.osm.gz:${tier#*:}"); done
fi

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
# TILE_WITH: only the tiles touching one of the files of this raster (a VRT of scattered zones)
zones = None
if '$TILE_WITH':
    from osgeo import gdal
    gdal.UseExceptions()
    zones = []
    for f in gdal.Open('$TILE_WITH').GetFileList()[1:]:
        ds = gdal.Open(f); g = ds.GetGeoTransform()
        zones.append((g[0], g[3] + g[5] * ds.RasterYSize, g[0] + g[1] * ds.RasterXSize, g[3]))
for i in range(math.ceil((e - w) / t)):
    for j in range(math.ceil((n - s) / t)):
        x0, y0 = w + i * t, s + j * t
        x1, y1 = min(x0 + t, e), min(y0 + t, n)
        if zones is not None and not any(a < x1 and c > x0 and b < y1 and d > y0 for a, b, c, d in zones):
            continue
        print('%s_%02d_%02d %g %g %g %g' % ('$NAME', i, j, x0, y0, x1, y1))
")
if [ -n "$TILES" ]; then
	step "$NAME: $(echo "$TILES" | wc -l | tr -d ' ') tiles of $TILE degrees, $JOBS at a time"
	TILE_OUT="$TMP/tiles"; mkdir -p "$TILE_OUT"
	export SELF="$0" GRID LAND LEVELS TIERS CELL UPSAMPLE SMOOTH SMOOTH_FROM RESAMPLING MIN_RING_CELLS OVERVIEW EXCLUDE \
		EXCLUDE_LAYER FILL MAP_CREATOR TILE_OUT
	echo "$TILES" | xargs -P "$JOBS" -L 1 bash -c '
		name=$1; shift
		args=(-n "$name" -b "$*" -i "$GRID" -m "$LAND" -o "$TILE_OUT" -l "${LEVELS:-none}" -p "$TIERS" -r "$CELL" \
			-u "$UPSAMPLE" -s "$SMOOTH" -d "$SMOOTH_FROM" -a "$RESAMPLING" -g "$MIN_RING_CELLS" -w "$OVERVIEW" \
			-x "$EXCLUDE" -X "$EXCLUDE_LAYER" -F "$FILL" -j 1)
		JAVA_OPTS="${JAVA_OPTS:--Xmx2g}" bash "$SELF" "${args[@]}" > "$TILE_OUT/$name.log" 2>&1 \
			|| { echo "FAILED tile $name:"; tail -20 "$TILE_OUT/$name.log"; exit 255; }
		echo "tile $name: $(tail -1 "$TILE_OUT/$name.log")"' _
	if [ -n "$MAP_CREATOR" ]; then
		# sections DIR OUTPUT : the map sections of the tiles in DIR, built in parallel (each in its own JVM), merged
		sections() {
			local dir=$1 output=$2 pre; pre="$TMP/$(basename "$1")_"
			OBFS=(); PIDS=()
			contours=("$dir"/*_[0-9][0-9]_[0-9][0-9].osm.gz)
			if [ -f "${contours[0]}" ]; then
				step "contours obf of ${#contours[@]} tiles"
				single "${pre}contours.obf" "${contours[@]}" & PIDS+=($!); OBFS+=("${pre}contours.obf")
			fi
			overview=("$dir"/*_overview.osm.gz)
			if [ -n "$OVERVIEW" ] && [ -f "${overview[0]}" ]; then
				step "overview obf of ${#overview[@]} tiles"
				single "${pre}overview.obf" --map-zooms="${OVERVIEW#*:}" "${overview[@]}" & PIDS+=($!); OBFS+=("${pre}overview.obf")
			fi
			i=0
			for tier in $TIERS; do
				i=$((i + 1)); points=("$dir"/*_points$i.osm.gz)
				[ -f "${points[0]}" ] || continue
				step "points $i obf of ${#points[@]} tiles"
				single "${pre}points$i.obf" --map-zooms="${tier#*:}" "${points[@]}" & PIDS+=($!); OBFS+=("${pre}points$i.obf")
			done
			if [ -n "$ENC_AREAS" ]; then
				obf "$ENC_AREAS" > "${pre}enc_areas.section" & PIDS+=($!)
			fi
			areas=("$dir"/*_areas.osm.gz)
			if [ -f "${areas[0]}" ]; then
				step "depth areas obf of ${#areas[@]} tiles"
				single "${pre}areas.obf" "${areas[@]}" & PIDS+=($!); OBFS+=("${pre}areas.obf")
			fi
			for e in ${ENC_OSM[@]+"${ENC_OSM[@]}"}; do
				o="${pre}$(basename "${e%%:*}" .osm.gz).obf"; z=${e#*:}
				single "$o" ${z:+--map-zooms="$z"} "${e%%:*}" & PIDS+=($!); OBFS+=("$o")
			done
			[ ${#OBFS[@]} -gt 0 ] || { echo "no tile has depth data" >&2; exit 1; }
			for pid in "${PIDS[@]}"; do wait "$pid" || exit 1; done
			if [ -n "$ENC_AREAS" ]; then OBFS+=("$(cat "${pre}enc_areas.section")"); fi
			step "merge ${#OBFS[@]} map sections into $(basename "$output")"
			merge "$output" "${OBFS[@]}"
		}
		sections "$TILE_OUT" "$OUT/$NAME.depth.obf"
		if [ -n "$NO_DETAILED" ]; then
			IFS=: read -r nd_name nd_cover nd_overview <<< "$NO_DETAILED"
			step "$nd_name: the tiles without the detailed coverage"
			nd_dir="$TMP/no_detailed"; mkdir -p "$nd_dir"
			cut_tile() {
				local tile=$1 cover=$nd_cover args=() c
				case $tile in *_overview.osm.gz) cover=$nd_overview ;; esac
				if [ -z "$cover" ]; then cp "$tile" "$nd_dir/"; return; fi
				IFS=, read -ra covers <<< "$cover"
				for c in "${covers[@]}"; do args+=(--exclude "$c"); done
				python3 "$HERE/depth_osm_exclude.py" "$tile" "$nd_dir/$(basename "$tile")" "${args[@]}" 2>&1 | grep -v numpy
			}
			export -f cut_tile; export HERE nd_dir nd_cover nd_overview
			ls "$TILE_OUT"/*.osm.gz | xargs -P "$JOBS" -n 1 bash -c 'cut_tile "$0" || exit 255' \
				|| { echo "FAILED cutting the detailed coverage" >&2; exit 1; }
			sections "$nd_dir" "$OUT/$nd_name.depth.obf"
		fi
	else
		rm -rf "$OUT/$NAME.tiles"; mkdir -p "$OUT/$NAME.tiles"
		mv "$TILE_OUT"/*.osm.gz "$OUT/$NAME.tiles/" 2>/dev/null || true
	fi
	rm -rf "$TMP"
	step "done: $(cd "$OUT" && du -sh "$NAME".* | awk '{printf "%s (%s) ", $2, $1}')"
	exit 0
fi

# fill: its grid brought to chart datum; that grid replaces the original in GRID, so contours and fill share a datum
if [ -n "$FILL" ]; then
	IFS=, read -r fill_grid fill_plus fill_minus <<< "$FILL"
	warp() { gdalwarp -q -overwrite -t_srs EPSG:4326 -te "$W" "$S" "$E" "$N" -tr "$CELL" "$CELL" -r "$2" -ot Float32 \
		-dstnodata nan "$1" "$3"; }
	# a grid in chart datum is used as it is
	if [ -n "$fill_plus" ]; then
		step "fill grid to chart datum"
		warp "$fill_grid" average "$TMP/fill.tif"
		warp "$fill_plus" bilinear "$TMP/fill_plus.tif"; warp "$fill_minus" bilinear "$TMP/fill_minus.tif"
		# sum of the bands with NaN where any is missing: a VRT pixel function, no numpy needed
		srcs="<SimpleSource><SourceFilename relativeToVRT=\"1\">fill.tif</SourceFilename><SourceBand>1</SourceBand></SimpleSource>"
		srcs+="<SimpleSource><SourceFilename relativeToVRT=\"1\">fill_plus.tif</SourceFilename><SourceBand>1</SourceBand></SimpleSource>"
		srcs+="<ComplexSource><SourceFilename relativeToVRT=\"1\">fill_minus.tif</SourceFilename><SourceBand>1</SourceBand><ScaleRatio>-1</ScaleRatio></ComplexSource>"
		size=$(gdalinfo -json "$TMP/fill.tif" | python3 -c 'import json,sys; print(*json.load(sys.stdin)["size"])')
		gt=$(gdalinfo -json "$TMP/fill.tif" | python3 -c 'import json,sys; print(",".join(map(repr, json.load(sys.stdin)["geoTransform"])))')
		cat > "$TMP/fill_sum.vrt" <<-VRT
		<VRTDataset rasterXSize="${size% *}" rasterYSize="${size#* }"><SRS>EPSG:4326</SRS><GeoTransform>$gt</GeoTransform>
		<VRTRasterBand dataType="Float32" band="1" subClass="VRTDerivedRasterBand"><NoDataValue>nan</NoDataValue>
		<PixelFunctionType>sum</PixelFunctionType><PixelFunctionArguments propagateNoData="true"/>$srcs
		</VRTRasterBand></VRTDataset>
		VRT
		gdal_translate -q -co COMPRESS=DEFLATE -co TILED=YES "$TMP/fill_sum.vrt" "$TMP/fill_datum.tif"
		rm -f "$TMP/fill.tif" "$TMP/fill_plus.tif" "$TMP/fill_minus.tif" "$TMP/fill_sum.vrt"
		grids=""; IFS=, read -ra parts <<< "$GRID"
		for g in "${parts[@]}"; do [ "$g" = "$fill_grid" ] && g="$TMP/fill_datum.tif"; grids+="${grids:+,}$g"; done
		GRID=$grids
	fi
fi

step "cut $NAME ($W $S $E $N), cell $CELL deg, upsampled x$UPSAMPLE"
if [ "$RESAMPLING" = auto ]; then
	cuts=(); IFS=, read -ra grids <<< "$GRID"
	for g in "${grids[@]}"; do
		cell=$(gdalinfo -json "$g" | python3 -c 'import json,sys; c = abs(json.load(sys.stdin)["geoTransform"][1]); print(c / 111320 if c > 1 else c)')
		r=$(python3 -c "print('bilinear' if float('$cell') > 1.5 * float('$CELL') else 'average')")
		gdalwarp -q -overwrite -t_srs EPSG:4326 -te "$W" "$S" "$E" "$N" -tr "$CELL" "$CELL" -r "$r" -ot Float32 \
			-dstnodata nan -multi -wo NUM_THREADS="$JOBS" "$g" "$TMP/cut${#cuts[@]}.tif"
		cuts+=("$TMP/cut${#cuts[@]}.tif")
	done
	gdalwarp -q -overwrite -srcnodata nan -dstnodata nan -co COMPRESS=DEFLATE -co TILED=YES "${cuts[@]}" "$TMP/grid.tif"
	rm -f "${cuts[@]}"
else
	gdalwarp -q -overwrite -t_srs EPSG:4326 -te "$W" "$S" "$E" "$N" -tr "$CELL" "$CELL" -r "$RESAMPLING" -ot Float32 \
		-dstnodata nan -multi -wo NUM_THREADS="$JOBS" -co COMPRESS=DEFLATE -co TILED=YES ${GRID//,/ } "$TMP/grid.tif"
fi
step "land mask"
ogr2ogr -q -f GPKG -spat "$W" "$S" "$E" "$N" -clipsrc "$W" "$S" "$E" "$N" -nlt MULTIPOLYGON "$TMP/land.gpkg" "$LAND"
LAND_LAYER=$(ogrinfo -q "$TMP/land.gpkg" | awk -F'[: ]+' 'NR==1{print $2}')
# exclude RASTER : nodata where the EXCLUDE polygons are
exclude() {
	[ -n "$EXCLUDE" ] || return 0
	[ -n "$EXCLUDE_LAYER" ] || EXCLUDE_LAYER=$(ogrinfo -ro -q "$EXCLUDE" | awk -F'[: ]+' 'NR==1{print $2}')
	gdal_rasterize -q -burn nan -l "$EXCLUDE_LAYER" "$EXCLUDE" "$1"
}
OBFS=()

if [ -n "$LEVELS" ]; then
	cp "$TMP/grid.tif" "$TMP/contour_grid.tif"
	exclude "$TMP/contour_grid.tif"
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

# overview: the levels of 200 m and deeper from the grid averaged to a coarse cell, for the zooms below the contours
OVERVIEW_LEVELS=$(echo "$LEVELS" | tr ',' '\n' | awk '$1 >= 200 && $1 % 200 == 0' | sort -rn | awk '{printf "%s ", -$1}')
if [ -n "$OVERVIEW" ] && [ -n "$OVERVIEW_LEVELS" ]; then
	ov_cell=${OVERVIEW%%:*}
	step "overview contours every 200 m, $ov_cell deg cells, zooms ${OVERVIEW#*:}"
	gdalwarp -q -overwrite -te "$W" "$S" "$E" "$N" -tr "$ov_cell" "$ov_cell" -r average -ot Float32 -srcnodata nan \
		-dstnodata nan "$TMP/grid.tif" "$TMP/overview.tif"
	gdal_rasterize -q -burn 0 -l "$LAND_LAYER" "$TMP/land.gpkg" "$TMP/overview.tif"
	# shellcheck disable=SC2086
	gdal_contour -q -a elev -fl $OVERVIEW_LEVELS "$TMP/overview.tif" "$TMP/overview.gpkg"
	python3 "$HERE/depth_contours_filter.py" "$TMP/overview.gpkg" "$TMP/overview.fgb" --cell "$ov_cell" \
		--min-ring-cells "$MIN_RING_CELLS"
	if [ "$(ogrinfo -so -al "$TMP/overview.fgb" | awk -F': ' '/Feature Count/{print $2}')" != 0 ]; then
		python3 "$V4/ogr2osm.py" -f -t "$V4/translations/contours_depth.py" -o "$TMP/${NAME}_overview.osm" \
			"$TMP/overview.fgb" >/dev/null
		gzip -f "$TMP/${NAME}_overview.osm"
		mv "$TMP/${NAME}_overview.osm.gz" "$OUT/${NAME}_overview.osm.gz"
		if [ -n "$MAP_CREATOR" ]; then
			o=$(obf "$OUT/${NAME}_overview.osm.gz" "${OVERVIEW#*:}"); OBFS+=("$o")
		fi
	fi
	rm -f "$TMP/overview.tif" "$TMP/overview.gpkg" "$TMP/overview.fgb"
fi

# fill: "shallower than L" for L = 0, 2, 5, 10 m, each from the raster its contour came from (the smoothed one from
# SMOOTH_FROM), only where the fill grid has data (not from EMODnet); depth_areas_osm.py --nested makes bands of them
if [ -n "$FILL" ]; then
	step "depth areas where $(basename "$fill_grid") has data"
	fill_src=$fill_grid; [ -f "$TMP/fill_datum.tif" ] && fill_src="$TMP/fill_datum.tif"
	# shallower than 0 m dries (class -1), than 2 m is 0-2 (0), than 5 m is 2-5 (2), than 10 m is 5-10 (5)
	for level_class in 0:-1 2:0 5:2 10:5; do
		level=${level_class%%:*}; class=${level_class#*:}
		raster="$TMP/contour_grid.tif"
		if [ -f "$TMP/smooth.tif" ] && [ "$level" -ge "$SMOOTH_FROM" ]; then raster="$TMP/smooth.tif"; fi
		if [ ! -f "$raster" ]; then
			[ -f "$TMP/fill_cut.tif" ] || warp "$fill_src" average "$TMP/fill_cut.tif"
			raster="$TMP/fill_cut.tif"
		fi
		# the fill grid on the cells of the raster, added x 0: nan outside the fill grid, the raster's value inside
		size=$(gdalinfo -json "$raster" | python3 -c 'import json,sys; print(*json.load(sys.stdin)["size"])')
		gt=$(gdalinfo -json "$raster" | python3 -c 'import json,sys; print(",".join(map(repr, json.load(sys.stdin)["geoTransform"])))')
		if [ ! -f "$TMP/coverage_${size// /_}.tif" ]; then
			# shellcheck disable=SC2086
			gdalwarp -q -overwrite -t_srs EPSG:4326 -te "$W" "$S" "$E" "$N" -ts $size -r near -ot Float32 -dstnodata nan \
				"$fill_src" "$TMP/coverage_${size// /_}.tif"
		fi
		cat > "$TMP/fill_level.vrt" <<-VRT
		<VRTDataset rasterXSize="${size% *}" rasterYSize="${size#* }"><SRS>EPSG:4326</SRS><GeoTransform>$gt</GeoTransform>
		<VRTRasterBand dataType="Float32" band="1" subClass="VRTDerivedRasterBand"><NoDataValue>nan</NoDataValue>
		<PixelFunctionType>sum</PixelFunctionType><PixelFunctionArguments propagateNoData="true"/>
		<SimpleSource><SourceFilename relativeToVRT="0">$raster</SourceFilename><SourceBand>1</SourceBand></SimpleSource>
		<ComplexSource><SourceFilename relativeToVRT="0">$TMP/coverage_${size// /_}.tif</SourceFilename><SourceBand>1</SourceBand><ScaleRatio>0</ScaleRatio></ComplexSource>
		</VRTRasterBand></VRTDataset>
		VRT
		# the band above the level (the upper level is out of reach); an older GDAL also writes the band below it
		rm -f "$TMP/band.gpkg"
		gdal_contour -q -p -amin emin -amax emax -fl "-$level" 100000 "$TMP/fill_level.vrt" "$TMP/band.gpkg"
		ogr2ogr -q -f GPKG -append -nln areas "$TMP/areas.gpkg" "$TMP/band.gpkg" \
			-sql "SELECT geom, CAST($class AS REAL) AS mindepth FROM contour WHERE emin > -$level - 0.001 AND emax > -$level + 0.001"
	done
	rm -f "$TMP"/coverage_*.tif "$TMP/fill_level.vrt" "$TMP/fill_cut.tif" "$TMP/band.gpkg"
	if [ -f "$TMP/areas.gpkg" ]; then
		python3 "$HERE/depth_areas_osm.py" "$TMP/areas.gpkg" "$OUT/${NAME}_areas.osm.gz" --layer areas --field mindepth \
			--nested --land "$TMP/land.gpkg" --bbox "$W" "$S" "$E" "$N" \
			--simplify "$(python3 -c "print(float('$CELL') / 2)")" 2>&1 | grep -v numpy
		if [ -n "$MAP_CREATOR" ] && [ "$(zcat < "$OUT/${NAME}_areas.osm.gz" | grep -c -m1 '<way')" != 0 ]; then
			o=$(obf "$OUT/${NAME}_areas.osm.gz"); OBFS+=("$o")
		fi
	fi
	rm -f "$TMP/fill_datum.tif" "$TMP/areas.gpkg"
fi
rm -f "$TMP/contour_grid.tif" "$TMP/smooth.tif"

if [ -n "$TIERS" ]; then
	# land as nodata, so that averages are over water only
	gdal_rasterize -q -burn "$LAND_NODATA" -l "$LAND_LAYER" "$TMP/land.gpkg" "$TMP/grid.tif"
	gdalwarp -q -overwrite -srcnodata "$LAND_NODATA" -dstnodata nan -co COMPRESS=DEFLATE -co TILED=YES \
		"$TMP/grid.tif" "$TMP/water.tif"
	exclude "$TMP/water.tif"
	i=0
	for tier in $TIERS; do
		i=$((i + 1)); spacing=${tier%%:*}; zooms=${tier#*:}
		step "points every $spacing deg from zoom $zooms"
		# per spacing cell the shoalest water cell at its own position, as on charts (not a regular grid)
		osm="$OUT/${NAME}_points$i.osm.gz"
		python3 "$HERE/depth_points_osm.py" "$TMP/water.tif" "$osm" --bbox "$W" "$S" "$E" "$N" --shoalest "$spacing" \
			--first-id $((i * 100000000))
		if [ "$(zcat < "$osm" | grep -c -m1 '<node')" = 0 ]; then
			rm -f "$osm"; continue
		fi
		if [ -n "$MAP_CREATOR" ]; then
			o=$(obf "$osm" "$zooms"); OBFS+=("$o")
		fi
	done
fi

if [ -n "$MAP_CREATOR" ]; then
	for e in ${ENC_OSM[@]+"${ENC_OSM[@]}"}; do
		z=${e#*:}; o=$(obf "${e%%:*}" "$z"); OBFS+=("$o")
	done
	if [ -n "$ENC_AREAS" ]; then o=$(obf "$ENC_AREAS"); OBFS+=("$o"); fi
	if [ ${#OBFS[@]} -eq 0 ]; then
		rm -rf "$TMP"; step "no depth data in $NAME, nothing written"; exit 0
	fi
	step "merge ${#OBFS[@]} map sections"
	merge "$OUT/$NAME.depth.obf" "${OBFS[@]}"
fi
rm -rf "$TMP"
step "done: $(cd "$OUT" && du -h "$NAME".* "$NAME"_points* "$NAME"_overview* "$NAME"_enc* 2>/dev/null | awk '{printf "%s (%s) ", $2, $1}')"
