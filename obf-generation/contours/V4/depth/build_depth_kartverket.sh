#!/usr/bin/env bash
# Depth contours and soundings of Norway from Kartverket "Sjøkart - Dybdedata" (FGDB, vector) as .osm.gz and OBF.
#
#   build_depth_kartverket.sh -D DATA_DIR [-n NAME] [-b "W S E N"] [-t TILE] [-j JOBS] [-c MAP_CREATOR_DIR] [-k]
#   build_depth_kartverket.sh -i FGDB -m LAND -o OUT_DIR [-n NAME] [-b "W S E N"] [-p TIERS] [-t TILE] [-j JOBS]
#                             [-c MAP_CREATOR_DIR] [-k]
#
#   -D DATA_DIR  folder of download_all.sh: the FGDB is DATA_DIR/src/norway/*.gdb, the land
#                DATA_DIR/mask/land_polygons.gpkg, the output DATA_DIR/build
#   -n NAME      default Norway_contours
#   -b BBOX      only this box (lon/lat), default all of the data
#   -i FGDB      the Dybdedata file geodatabase (or any OGR source with its dybdekurve and dybdepunkt layers). The
#                map keeps 2 m and every 5 m as its contours and leaves the 1 m steps of a harbour survey out;
#                env MINOR_ZOOMS="15-" keeps the metre levels down to 10 m as a map section of those zooms
#   -m LAND      land polygons; soundings on land are dropped, contours are kept as they are
#   -p TIERS     soundings "SPACING:ZOOMS ...", the shallowest sounding of every SPACING degree cell shown from ZOOMS,
#                default "0.02:10-11 0.008:12 0.0025:13-14 0.001:15-"
#   -t TILE      without -b: tiles of TILE degrees built JOBS at a time, default 1; 0 = one piece. Every spacing
#                should divide TILE, or a cell across a tile edge gives a sounding on both sides
#   -j JOBS      default 4
#   -c MAP_CREATOR_DIR  unzipped OsmAndMapCreator: writes OUT_DIR/NAME.depth.obf
#   -k           keep: do nothing when OUT_DIR/NAME.depth.obf already exists
#   env RENDERING_TYPES  rendering_types.xml for OsmAndMapCreator instead of its own (new tags before a nightly)
#
# The contours are the charted ones (dybdekurve, 0 m = chart datum): only the SRS is dropped so that ogr2osm does not
# swap lat/lon (a line clipped at a tile edge is a multiline, ogr2osm writes its parts as ways), then
# translations/contours_depth.py as for the gridded regions. The soundings (dybdepunkt) are thinned
# per tier by depth_soundings_osm.py, the depth areas (dybdeareal) become the fill by depth_areas_osm.py.
# Tiles are needed for the land: a complete land polygon is a continent, a point test against it takes 0.16 s and a
# union per fill cell 15 s. The scripts clip and rasterize it themselves now; tiles keep ogr2osm and OsmAndMapCreator
# from taking all of Norway (8 GB of OSM) in one piece.
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
V4=$(cd "$HERE/.." && pwd)
NAME=Norway_contours; BBOX=""; FGDB=""; LAND=""; OUT=""; DATA=""; MAP_CREATOR=""; KEEP=0; TILE=1; JOBS=4
TIERS="0.02:10-11 0.008:12 0.0025:13-14 0.001:15-"
# soundings are checked against the land rasterized at this cell, about 10 m
LAND_CELL=0.0001
while [ $# -gt 0 ]; do
	case "$1" in
		-n) NAME=$2; shift 2 ;;
		-b) BBOX=$2; shift 2 ;;
		-i) FGDB=$2; shift 2 ;;
		-m) LAND=$2; shift 2 ;;
		-o) OUT=$2; shift 2 ;;
		-p) TIERS=$2; shift 2 ;;
		-t) TILE=$2; shift 2 ;;
		-j) JOBS=$2; shift 2 ;;
		-c) MAP_CREATOR=$2; shift 2 ;;
		-D) DATA=$2; shift 2 ;;
		-k) KEEP=1; shift ;;
		-h|--help) sed -n '2,28p' "$0"; exit 0 ;;
		*) echo "Unknown option $1" >&2; exit 1 ;;
	esac
done
if [ -n "$DATA" ]; then
	: "${FGDB:=$(ls -d "$DATA"/src/norway/*.gdb | head -1)}"; : "${LAND:=$DATA/mask/land_polygons.gpkg}"; : "${OUT:=$DATA/build}"
fi
for v in FGDB LAND OUT; do
	[ -n "${!v}" ] || { echo "Missing $v, see --help" >&2; exit 1; }
done
if [ $KEEP -eq 1 ] && [ -f "$OUT/$NAME.depth.obf" ]; then
	echo "== $NAME.depth.obf exists, kept (no -k to rebuild)"; exit 0
fi
mkdir -p "$OUT"; OUT=$(cd "$OUT" && pwd)
FGDB=$(cd "$(dirname "$FGDB")" && pwd)/$(basename "$FGDB"); LAND=$(cd "$(dirname "$LAND")" && pwd)/$(basename "$LAND")
if [ -n "$MAP_CREATOR" ]; then MAP_CREATOR=$(cd "$MAP_CREATOR" && pwd); fi
TMP="$OUT/$NAME.tmp"; rm -rf "$TMP"; mkdir -p "$TMP"
export PYTHONFAULTHANDLER=1
started=$(date +%s)
step() { echo "== $(( $(date +%s) - started ))s $*"; }

# obf OSM_GZ [ZOOMS] : OBF with the map section only, printed path
obf() {
	local osm=$1 zooms=${2:-} dir
	dir=$(mktemp -d "$TMP/obf.XXXX")
	(cd "$dir" && JAVA_OPTS="${JAVA_OPTS:--Xmx16g}" bash "$MAP_CREATOR/utilities.sh" generate-map "$osm" \
		${zooms:+--map-zooms=$zooms} ${RENDERING_TYPES:+--rendering-types=$RENDERING_TYPES} > obf.log 2>&1) || { tail -20 "$dir/obf.log" >&2; return 1; }
	ls "$dir"/*.obf 2>/dev/null | head -1 | grep . || { tail -20 "$dir/obf.log" >&2; return 1; }
}

# single OUTPUT OSM_GZ... [--map-zooms=ZOOMS] : one map section of many osm files
single() {
	local output=$1; shift
	local dir; dir=$(mktemp -d "$TMP/single.XXXX")
	(cd "$dir" && JAVA_OPTS="${JAVA_OPTS:--Xmx16g}" bash "$MAP_CREATOR/utilities.sh" generate-single-map "$output" \
		--name="$NAME" "$@" ${RENDERING_TYPES:+--rendering-types=$RENDERING_TYPES} > single.log 2>&1) || { tail -20 "$dir/single.log" >&2; return 1; }
	[ -f "$output" ] || { tail -20 "$dir/single.log" >&2; return 1; }
	rm -rf "$dir"
}

# merge OSM section files into OUT/NAME.depth.obf: SECTION_OBF...
merge() {
	rm -f "$OUT/$NAME.depth.obf"
	if [ $# -eq 1 ]; then mv "$1" "$OUT/$NAME.depth.obf"; return; fi
	(cd "$TMP" && JAVA_OPTS="${JAVA_OPTS:--Xmx16g}" bash "$MAP_CREATOR/utilities.sh" merge-index "$OUT/$NAME.depth.obf" "$@" \
		> merge.log 2>&1) || { tail -20 "$TMP/merge.log" >&2; exit 1; }
	[ -f "$OUT/$NAME.depth.obf" ] || { tail -20 "$TMP/merge.log" >&2; exit 1; }
}

if [ -z "$BBOX" ] && [ "$TILE" != 0 ]; then
	# tiles of whole TILE degrees over the data, only those with any contour, sounding or area
	TILES=$(python3 - "$FGDB" "$TILE" "$NAME" <<-'PY' 2>&1 | grep -v numpy
	import math, sys
	from osgeo import ogr
	ogr.UseExceptions()
	src, t, name = ogr.Open(sys.argv[1]), float(sys.argv[2]), sys.argv[3]
	layers = [src.GetLayer(n) for n in ('dybdekurve', 'dybdepunkt', 'dybdeareal')]
	ext = [l.GetExtent() for l in layers]
	w, e = min(x[0] for x in ext), max(x[1] for x in ext)
	s, n = min(x[2] for x in ext), max(x[3] for x in ext)
	i0, j0 = math.floor(w / t), math.floor(s / t)
	for i in range(i0, math.ceil(e / t)):
	    for j in range(j0, math.ceil(n / t)):
	        box = (i * t, j * t, (i + 1) * t, (j + 1) * t)
	        for l in layers:
	            l.SetSpatialFilterRect(*box)
	        if any(l.GetFeatureCount() for l in layers):
	            print('%s_%02d_%02d %g %g %g %g' % (name, i - i0, j - j0, *box))
	PY
	)
	step "$NAME: $(echo "$TILES" | wc -l | tr -d ' ') tiles of $TILE degrees with data, $JOBS at a time"
	TILE_OUT="$TMP/tiles"; mkdir -p "$TILE_OUT"
	export SELF="$0" FGDB LAND TIERS TILE_OUT MINOR_ZOOMS MIN_RING_CELLS MIN_LINE_M
	echo "$TILES" | xargs -P "$JOBS" -L 1 bash -c '
		name=$1; shift
		bash "$SELF" -n "$name" -b "$*" -i "$FGDB" -m "$LAND" -o "$TILE_OUT" -p "$TIERS" -t 0 > "$TILE_OUT/$name.log" 2>&1 \
			|| { echo "FAILED tile $name:"; tail -20 "$TILE_OUT/$name.log"; exit 255; }
		echo "tile $name: $(tail -1 "$TILE_OUT/$name.log")"' _
	if [ -n "$MAP_CREATOR" ]; then
		OBFS=(); PIDS=()
		contours=("$TILE_OUT"/*_[0-9][0-9]_[0-9][0-9].osm.gz)  # *_minor.osm.gz does not match
		if [ -f "${contours[0]}" ]; then
			step "contours obf of ${#contours[@]} tiles"
			single "$TMP/contours.obf" "${contours[@]}" & PIDS+=($!); OBFS+=("$TMP/contours.obf")
		fi
		minor=("$TILE_OUT"/*_minor.osm.gz)
		if [ -f "${minor[0]}" ]; then
			step "minor contours obf of ${#minor[@]} tiles"
			single "$TMP/minor.obf" --map-zooms="$MINOR_ZOOMS" "${minor[@]}" & PIDS+=($!); OBFS+=("$TMP/minor.obf")
		fi
		areas=("$TILE_OUT"/*_areas.osm.gz)
		if [ -f "${areas[0]}" ]; then
			step "depth areas obf of ${#areas[@]} tiles"
			single "$TMP/areas.obf" "${areas[@]}" & PIDS+=($!); OBFS+=("$TMP/areas.obf")
		fi
		i=0
		for tier in $TIERS; do
			i=$((i + 1)); points=("$TILE_OUT"/*_points$i.osm.gz)
			[ -f "${points[0]}" ] || continue
			step "points $i obf of ${#points[@]} tiles"
			single "$TMP/points$i.obf" --map-zooms="${tier#*:}" "${points[@]}" & PIDS+=($!); OBFS+=("$TMP/points$i.obf")
		done
		[ ${#OBFS[@]} -gt 0 ] || { echo "no tile has depth data" >&2; exit 1; }
		for pid in "${PIDS[@]}"; do wait "$pid" || exit 1; done
		step "merge ${#OBFS[@]} map sections"
		merge "${OBFS[@]}"
	else
		rm -rf "$OUT/$NAME.tiles"; mkdir -p "$OUT/$NAME.tiles"
		mv "$TILE_OUT"/*.osm.gz "$OUT/$NAME.tiles/" 2>/dev/null || true
	fi
	rm -rf "$TMP"
	step "done: $(cd "$OUT" && du -sh "$NAME".* | awk '{printf "%s (%s) ", $2, $1}')"
	exit 0
fi

SPAT=(); CLIP=()
if [ -n "$BBOX" ]; then read -r W S E N <<< "$BBOX"; SPAT=(-spat "$W" "$S" "$E" "$N"); CLIP=(-clipsrc "$W" "$S" "$E" "$N"); fi

# The map's contours are the standard levels: 2 m and every 5 m. A detailed survey is charted in 1 m steps (Kartverket
# in harbours); those levels are a mess at every zoom, so they are left out. env MINOR_ZOOMS="15-" keeps the ones down
# to 10 m (1, 3, 4, 6, 7, 8, 9) in NAME_minor.osm.gz instead, as a map section of those zooms.
step "$NAME contours from $FGDB"
rm -f "$OUT/$NAME.osm.gz" "$OUT/${NAME}_minor.osm.gz"
MINOR_ZOOMS="${MINOR_ZOOMS-}"  # env MINOR_ZOOMS="15-" to keep the metre levels as a section of those zooms
# a ring of 16 cells of 0.00005 degrees is about 80 m, a chart's smallest closed contour
MIN_RING_CELLS="${MIN_RING_CELLS:-16}"; MIN_LINE_M="${MIN_LINE_M:-30}"
for part in main ${MINOR_ZOOMS:+minor}; do
	if [ "$part" = main ]; then where="dybde = 2 OR dybde % 5 = 0"; out="$OUT/$NAME.osm.gz"
	else where="dybde <= 10 AND dybde <> 2 AND dybde % 5 <> 0"; out="$OUT/${NAME}_minor.osm.gz"; fi
	ogr2ogr -f FlatGeobuf "$TMP/depth.fgb" "$FGDB" ${SPAT[@]+"${SPAT[@]}"} ${CLIP[@]+"${CLIP[@]}"} \
		-nlt MULTILINESTRING -a_srs None -lco SPATIAL_INDEX=NO \
		-sql "SELECT SHAPE, dybde AS depth FROM dybdekurve WHERE dybde > 0 AND ($where)"
	# a tile without contours leaves no readable FlatGeobuf (header only or no file)
	lines=$(ogrinfo -so -al "$TMP/depth.fgb" 2>/dev/null | awk -F': ' '/Feature Count/{print $2}') || lines=0
	lines=${lines:-0}
	step "$part contours osm: $lines lines"
	if [ "$lines" != 0 ]; then
		# a multibeam survey leaves small rings and scraps of line on the chart; on the map they read as broken lines
		python3 "$HERE/depth_contours_filter.py" "$TMP/depth.fgb" "$TMP/clean.fgb" --cell 0.00005 \
			--min-ring-cells "$MIN_RING_CELLS" --min-line-m "$MIN_LINE_M" 2>&1 | grep -v numpy
		mv "$TMP/clean.fgb" "$TMP/depth.fgb"
		python3 "$V4/ogr2osm.py" -f -t "$V4/translations/contours_depth.py" -o "$TMP/$NAME.osm" "$TMP/depth.fgb" >/dev/null
		gzip -f "$TMP/$NAME.osm"; mv "$TMP/$NAME.osm.gz" "$out"
	fi
	rm -f "$TMP/depth.fgb"
done

step "soundings"
LAND_CUT="$LAND"
if [ -n "$BBOX" ]; then
	ogr2ogr -q -f GPKG ${SPAT[@]+"${SPAT[@]}"} ${CLIP[@]+"${CLIP[@]}"} -nlt MULTIPOLYGON "$TMP/land.gpkg" "$LAND"; LAND_CUT="$TMP/land.gpkg"
fi
spacings=""; for tier in $TIERS; do spacings+="${tier%%:*} "; done
python3 "$HERE/depth_soundings_osm.py" "$FGDB" "$OUT/${NAME}_points" --tiers "$spacings" --land "$LAND_CUT" \
	--land-cell "$LAND_CELL" ${BBOX:+--bbox $BBOX} --first-id 100000000 2>&1 | grep -v numpy
for f in "$OUT/${NAME}"_points*.osm.gz; do
	[ "$(zcat < "$f" | grep -c -m1 '<node')" != 0 ] || rm -f "$f"
done

step "depth areas"
python3 "$HERE/depth_areas_osm.py" "$FGDB" "$OUT/${NAME}_areas.osm.gz" --layer dybdeareal --field minimumsdybde \
	--land "$LAND_CUT" ${BBOX:+--bbox $BBOX} 2>&1 | grep -v numpy
[ "$(zcat < "$OUT/${NAME}_areas.osm.gz" | grep -c -m1 '<way')" != 0 ] || rm -f "$OUT/${NAME}_areas.osm.gz"

if [ -n "$MAP_CREATOR" ]; then
	OBFS=(); PIDS=(); i=0
	step "map sections"
	if [ -f "$OUT/$NAME.osm.gz" ]; then obf "$OUT/$NAME.osm.gz" > "$TMP/section0" & PIDS+=($!); fi
	if [ -f "$OUT/${NAME}_minor.osm.gz" ]; then obf "$OUT/${NAME}_minor.osm.gz" "$MINOR_ZOOMS" > "$TMP/section_minor" & PIDS+=($!); fi
	if [ -f "$OUT/${NAME}_areas.osm.gz" ]; then obf "$OUT/${NAME}_areas.osm.gz" > "$TMP/section_areas" & PIDS+=($!); fi
	for tier in $TIERS; do
		i=$((i + 1))
		[ -f "$OUT/${NAME}_points$i.osm.gz" ] || continue
		obf "$OUT/${NAME}_points$i.osm.gz" "${tier#*:}" > "$TMP/section$i" & PIDS+=($!)
	done
	[ ${#PIDS[@]} -gt 0 ] || { echo "no depth data in $NAME" >&2; exit 1; }
	for pid in "${PIDS[@]}"; do wait "$pid" || exit 1; done
	for f in "$TMP"/section*; do OBFS+=("$(cat "$f")"); done
	step "merge ${#OBFS[@]} map sections"
	merge "${OBFS[@]}"
fi
rm -rf "$TMP"
step "done: $(cd "$OUT" && du -h "$NAME".* "$NAME"_* 2>/dev/null | awk '{printf "%s (%s) ", $2, $1}')"
