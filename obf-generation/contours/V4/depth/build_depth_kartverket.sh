#!/usr/bin/env bash
# Depth contours and soundings of Norway from Kartverket "Sjøkart - Dybdedata" (FGDB, vector) as .osm.gz and OBF.
#
#   build_depth_kartverket.sh -D DATA_DIR [-n NAME] [-b "W S E N"] [-c MAP_CREATOR_DIR] [-k]
#   build_depth_kartverket.sh -i FGDB -m LAND -o OUT_DIR [-n NAME] [-b "W S E N"] [-p TIERS] [-c MAP_CREATOR_DIR] [-k]
#
#   -D DATA_DIR  folder of download_all.sh: the FGDB is DATA_DIR/src/norway/*.gdb, the land
#                DATA_DIR/mask/land_polygons.gpkg, the output DATA_DIR/build
#   -n NAME      default Norway_contours
#   -b BBOX      only this box (lon/lat), default all of the data
#   -i FGDB      the Dybdedata file geodatabase (or any OGR source with its dybdekurve and dybdepunkt layers)
#   -m LAND      land polygons; soundings on land are dropped, contours are kept as they are
#   -p TIERS     soundings "SPACING:ZOOMS ...", the shallowest sounding of every SPACING degree cell shown from ZOOMS,
#                default "0.02:10-11 0.008:12 0.003:13-14 0.001:15-"
#   -c MAP_CREATOR_DIR  unzipped OsmAndMapCreator: writes OUT_DIR/NAME.depth.obf
#   -k           keep: do nothing when OUT_DIR/NAME.depth.obf already exists
#   env RENDERING_TYPES  rendering_types.xml for OsmAndMapCreator instead of its own (new tags before a nightly)
#
# The contours are the charted ones (dybdekurve, 0 m = chart datum): only the SRS is dropped so that ogr2osm does not
# swap lat/lon, then translations/contours_depth.py as for the gridded regions. The soundings (dybdepunkt) are thinned
# per tier by depth_soundings_osm.py, the depth areas (dybdeareal) become the fill by depth_areas_osm.py.
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
V4=$(cd "$HERE/.." && pwd)
NAME=Norway_contours; BBOX=""; FGDB=""; LAND=""; OUT=""; DATA=""; MAP_CREATOR=""; KEEP=0
TIERS="0.02:10-11 0.008:12 0.003:13-14 0.001:15-"
while [ $# -gt 0 ]; do
	case "$1" in
		-n) NAME=$2; shift 2 ;;
		-b) BBOX=$2; shift 2 ;;
		-i) FGDB=$2; shift 2 ;;
		-m) LAND=$2; shift 2 ;;
		-o) OUT=$2; shift 2 ;;
		-p) TIERS=$2; shift 2 ;;
		-c) MAP_CREATOR=$2; shift 2 ;;
		-D) DATA=$2; shift 2 ;;
		-k) KEEP=1; shift ;;
		-h|--help) sed -n '2,21p' "$0"; exit 0 ;;
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
if [ -n "$MAP_CREATOR" ]; then MAP_CREATOR=$(cd "$MAP_CREATOR" && pwd); fi
TMP="$OUT/$NAME.tmp"; rm -rf "$TMP"; mkdir -p "$TMP"
started=$(date +%s)
step() { echo "== $(( $(date +%s) - started ))s $*"; }
SPAT=(); CLIP=()
if [ -n "$BBOX" ]; then read -r W S E N <<< "$BBOX"; SPAT=(-spat "$W" "$S" "$E" "$N"); CLIP=(-clipsrc "$W" "$S" "$E" "$N"); fi

# obf OSM_GZ [ZOOMS] : OBF with the map section only, printed path
obf() {
	local osm=$1 zooms=${2:-} dir
	dir=$(mktemp -d "$TMP/obf.XXXX")
	(cd "$dir" && JAVA_OPTS="${JAVA_OPTS:--Xmx16g}" bash "$MAP_CREATOR/utilities.sh" generate-map "$osm" \
		${zooms:+--map-zooms=$zooms} ${RENDERING_TYPES:+--rendering-types=$RENDERING_TYPES} > obf.log 2>&1) || { tail -20 "$dir/obf.log" >&2; return 1; }
	ls "$dir"/*.obf 2>/dev/null | head -1 | grep . || { tail -20 "$dir/obf.log" >&2; return 1; }
}

step "$NAME contours from $FGDB"
ogr2ogr -f FlatGeobuf "$TMP/depth.fgb" "$FGDB" ${SPAT[@]+"${SPAT[@]}"} ${CLIP[@]+"${CLIP[@]}"} \
	-explodecollections -nlt LINESTRING -a_srs None \
	-sql "SELECT SHAPE, dybde AS depth FROM dybdekurve WHERE dybde > 0"
step "contours osm: $(ogrinfo -so -al "$TMP/depth.fgb" | awk -F': ' '/Feature Count/{print $2}') lines"
python3 "$V4/ogr2osm.py" -f -t "$V4/translations/contours_depth.py" -o "$TMP/$NAME.osm" "$TMP/depth.fgb" >/dev/null
gzip -f "$TMP/$NAME.osm"; mv "$TMP/$NAME.osm.gz" "$OUT/$NAME.osm.gz"

step "soundings"
LAND_CUT="$LAND"
if [ -n "$BBOX" ]; then
	ogr2ogr -q -f GPKG ${SPAT[@]+"${SPAT[@]}"} ${CLIP[@]+"${CLIP[@]}"} -nlt MULTIPOLYGON "$TMP/land.gpkg" "$LAND"; LAND_CUT="$TMP/land.gpkg"
fi
spacings=""; for tier in $TIERS; do spacings+="${tier%%:*} "; done
python3 "$HERE/depth_soundings_osm.py" "$FGDB" "$OUT/${NAME}_points" --tiers "$spacings" --land "$LAND_CUT" \
	${BBOX:+--bbox $BBOX} --first-id 100000000 2>&1 | grep -v numpy

step "depth areas"
python3 "$HERE/depth_areas_osm.py" "$FGDB" "$OUT/${NAME}_areas.osm.gz" --layer dybdeareal --field minimumsdybde \
	--land "$LAND_CUT" ${BBOX:+--bbox $BBOX} 2>&1 | grep -v numpy

if [ -n "$MAP_CREATOR" ]; then
	OBFS=(); PIDS=(); i=0
	step "map sections"
	obf "$OUT/$NAME.osm.gz" > "$TMP/section0" & PIDS+=($!)
	obf "$OUT/${NAME}_areas.osm.gz" > "$TMP/section_areas" & PIDS+=($!)
	for tier in $TIERS; do
		i=$((i + 1))
		obf "$OUT/${NAME}_points$i.osm.gz" "${tier#*:}" > "$TMP/section$i" & PIDS+=($!)
	done
	for pid in "${PIDS[@]}"; do wait "$pid" || exit 1; done
	for f in "$TMP"/section*; do OBFS+=("$(cat "$f")"); done
	step "merge ${#OBFS[@]} map sections"
	rm -f "$OUT/$NAME.depth.obf"
	(cd "$TMP" && JAVA_OPTS="${JAVA_OPTS:--Xmx16g}" bash "$MAP_CREATOR/utilities.sh" merge-index "$OUT/$NAME.depth.obf" "${OBFS[@]}" \
		> merge.log 2>&1) || { tail -20 "$TMP/merge.log" >&2; exit 1; }
	[ -f "$OUT/$NAME.depth.obf" ] || { tail -20 "$TMP/merge.log" >&2; exit 1; }
fi
rm -rf "$TMP"
step "done: $(cd "$OUT" && du -h "$NAME".* "$NAME"_points* 2>/dev/null | awk '{printf "%s (%s) ", $2, $1}')"
