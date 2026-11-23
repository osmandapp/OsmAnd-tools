#!/usr/bin/env bash
# Depth contours, soundings and depth areas of the Swiss lakes from swisstopo swissBATHY3D.
#
#   build_depth_swiss_lakes.sh -D DATA_DIR [-n NAME] [-b "W S E N"] [-c MAP_CREATOR_DIR] [-j JOBS] [-k]
#
#   -D DATA_DIR  the source tiles DATA_DIR/src/swiss_lakes/asc/<lake>/*.asc (unzipped swissBATHY3D grids), the
#                output DATA_DIR/build
#   -n NAME      default Switzerland_lakes_contours
#   -b BBOX      only this box (lon/lat), default all of the lakes
#   -c MAP_CREATOR_DIR  unzipped OsmAndMapCreator: writes OUT_DIR/NAME.depth.obf
#   -j JOBS      default 4
#   -k           keep: do nothing when OUT_DIR/NAME.depth.obf already exists
#   env TIERS    soundings "SPACING:ZOOMS ...", default "0.05:9-10 0.02:11-12 0.01:13 0.005:14 0.0025:15 0.001:16-22"
#   env LEVELS   the contour depths in metres, default "2 5 10 20 50 100 200"
#   env CELL     the working grid in degrees, default 0.00002 (about 2 m)
#   env MAX_SEGMENT  the longest contour segment in kilometres, default 50 (see ogr2osm.py --max-segment)
#   env AREAS        1 (default) writes the depth areas of the grids, 0 leaves them out
#   env CONTOUR_SIMPLIFY_M  simplify the contours by this many metres before the areas are built, default 2.
#                MapCreator drops points of its own when it writes the map (skipSomeNodes), and on a curved isobath
#                those long chords cut the corner by tens of metres: the area edge and the line no longer coincide.
#                Simplifying first, with a tolerance above MapCreator's 0.58 m threshold, keeps every vertex of both
#                the areas and the lines, so the band edge stays on its isobath.
#   env AREA_SIMPLIFY  simplification of the areas in degrees, default 0 (keeps the vertices of the contours)
#   env LAKE_LEVELS  "lake=level ..." overrides the water level table below (metres above sea level, LN02)
#   env SHORE_FILL   1 (default) interpolates the gap between the survey data and the shore; 0 keeps the gaps
#   env SHORE_FILL_LIMIT  fill only where distance_to_land + distance_to_data <= this, metres, default 500
#   env SHORE_FILL_DATA   fill no farther than this from real data, metres, default 300
#   env SHORE_FILL_OPEN_WIDTH  water at least this far from land is open water, metres, default 50
#   env SHORE_FILL_OPEN_DISTANCE  fill only within this distance of open water (at least OPEN_WIDTH from land),
#                metres, default 100; keeps the fill out of narrow river beds and deltas
#   env SHORE_FILL_EDGE  fill within this distance of land even where the open-water guard fails, metres,
#                default 50 (the shallow bands reach the shore and the islands)
#   env SHORE_FILL_TILE / SHORE_FILL_MARGIN  fill tile and overlap, cells, defaults 1024 and 256
#   env CONTOUR_SNAP  extend an open contour ending this many metres from the water outline to it, default 100
#   env BAND_SPLIT  1 (default) splits a band face that spans several classes along the contour lines
#                inside it; 0 keeps every face whole
#   env BAND_CUMULATIVE  0 (default) writes the bands as disjoint faces bounded by their isobaths;
#                1 grows every class over the shallower ones (the old overlapping fill)
#   env BAND_CHECK  1 (default) checks every band polygon at many points of its area against the unfilled
#                depth raster, then samples the raster water for points no band covers; 0 skips the check
#   env BAND_CHECK_STRICT  0 (default) only reports a significant defect, 1 fails the build
#   env BAND_CHECK_MARGIN / BAND_CHECK_AREA  boundary tolerance, metres (default 1.5), and the smallest bad
#                area reported, km2 (default 0.01)
#   env BAND_COVERAGE  number of raster water points sampled for uncovered fill (default 200, 0 off)
#   env BAND_REQUIRE  control points "LON LAT CLASS ..." every point must be covered by the band CLASS
#                of its depth, e.g. BAND_REQUIRE="9.615401 47.506687 2" pins the Bodensee regression point
#   env SECTION_ZOOMS  "9-10;11-12" limits the zoom ranges of the contours and areas sections (test builds only);
#                the soundings keep the zooms of their TIERS
#   env KEEP_TMP  1 keeps the working directory instead of removing it; the path is printed at the start
#
# swissBATHY3D is the height of the lake bed above sea level (LN02) at 1-3 m, so a lake becomes depth only after
# its water level is subtracted. The table holds the long-term mean levels of the lakes; the seasonal range (a few
# decimetres, up to a metre) shifts the shallow bands, deeper contours stay where they are.
#
# The contours keep the standard levels (2, 5, 10, 20, 50, 100, 200 m): the metre steps of the source have no
# contourtype of their own and would be drawn as an untyped line from zoom 12.
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
V4=$(cd "$HERE/.." && pwd)
NAME=Switzerland_lakes_contours; BBOX="${BBOX:-}"; DATA=""; OUT="${OUT:-}"; MAP_CREATOR=""; KEEP=0; JOBS=4
TIERS="${TIERS:-0.05:9-10 0.02:11-12 0.01:13 0.005:14 0.0025:15 0.001:16-22}"
SECTION_ZOOMS="${SECTION_ZOOMS:-}"
LEVELS="${LEVELS:-2 5 10 20 50 100 200}"
CELL="${CELL:-0.00002}"
CONTOUR_SIMPLIFY_M="${CONTOUR_SIMPLIFY_M:-2}"
while [ $# -gt 0 ]; do
	case "$1" in
		-n) NAME=$2; shift 2 ;;
		-b) BBOX=$2; shift 2 ;;
		-c) MAP_CREATOR=$2; shift 2 ;;
		-D) DATA=$2; shift 2 ;;
		-j) JOBS=$2; shift 2 ;;
		-k) KEEP=1; shift ;;
		-h|--help) sed -n '2,44p' "$0"; exit 0 ;;
		*) echo "Unknown option $1" >&2; exit 1 ;;
	esac
done
[ -n "$DATA" ] || { echo "No -D DATA_DIR, see --help" >&2; exit 1; }
SRC="$DATA/src/swiss_lakes/asc"
OUT="${OUT:-$DATA/build}"
[ -f "$OUT/$NAME.depth.obf" ] && [ $KEEP -eq 1 ] && { echo "$OUT/$NAME.depth.obf exists, keep"; exit 0; }
[ -d "$SRC" ] || { echo "No tiles in $SRC, unzip the swissBATHY3D archives there" >&2; exit 1; }
mkdir -p "$OUT"
TMP=$(mktemp -d)
if [ "${KEEP_TMP:-0}" != 0 ]; then
	echo "temporary files: $TMP"
	trap - EXIT
else
	trap 'rm -rf "$TMP"' EXIT
fi
export PYTHONFAULTHANDLER=1
export GDAL_NUM_THREADS=$JOBS
step() { echo "== $(date +%H:%M:%S) $*"; }

# the water level of a lake: the long-term mean level above sea level (LN02), metres
lake_level() {
	case "$1" in
		aegerisee) echo 724.0 ;;
		baldeggersee) echo 463.0 ;;
		bielersee) echo 429.0 ;;
		bodensee) echo 395.3 ;;
		brienzersee) echo 564.0 ;;
		hallwilersee) echo 449.0 ;;
		lacdejoux) echo 1004.0 ;;
		lacleman) echo 372.0 ;;
		lacneuchatel) echo 429.4 ;;
		lagomaggiore) echo 193.0 ;;
		lungernsee) echo 689.0 ;;
		murtensee) echo 429.0 ;;
		rotsee) echo 419.0 ;;
		sarnersee) echo 469.0 ;;
		sempachersee) echo 504.0 ;;
		silsersee) echo 1797.0 ;;
		silvaplanersee) echo 1791.0 ;;
		thunersee) echo 558.0 ;;
		vierwaldstaettersee) echo 434.0 ;;
		walensee) echo 419.0 ;;
		zuerichsee) echo 406.0 ;;
		zugersee) echo 414.0 ;;
		*) echo "" ;;
	esac
}
# the name of the lake in OSM: the water outline is looked up by it (see osm_lake_outline.py)
lake_name() {
	case "$1" in
		aegerisee) echo "Ägerisee" ;;
		baldeggersee) echo "Baldeggersee" ;;
		bielersee) echo "Bielersee" ;;
		bodensee) echo "Bodensee" ;;
		brienzersee) echo "Brienzersee" ;;
		hallwilersee) echo "Hallwilersee" ;;
		lacdejoux) echo "Lac de Joux" ;;
		lacleman) echo "Genfersee" ;;
		lacneuchatel) echo "Neuenburgersee" ;;
		lagomaggiore) echo "Lago Maggiore" ;;
		lungernsee) echo "Lungernsee" ;;
		murtensee) echo "Murtensee" ;;
		rotsee) echo "Rotsee" ;;
		sarnersee) echo "Sarnersee" ;;
		sempachersee) echo "Sempachersee" ;;
		silsersee) echo "Silsersee" ;;
		silvaplanersee) echo "Silvaplanersee" ;;
		thunersee) echo "Thunersee" ;;
		vierwaldstaettersee) echo "Vierwaldstättersee" ;;
		walensee) echo "Walensee" ;;
		zuerichsee) echo "Zürichsee" ;;
		zugersee) echo "Zugersee" ;;
		*) echo "" ;;
	esac
}
# "lake=level" words on the command line win over the table
level_of() {
	local kv
	for kv in ${LAKE_LEVELS:-}; do
		[ "${kv%%=*}" = "$1" ] && { echo "${kv#*=}"; return; }
	done
	lake_level "$1"
}
# the extent of a raster as "W S E N"
grid_extent() {
	gdalinfo -json "$1" | python3 -c '
import json, sys
c = json.load(sys.stdin)["cornerCoordinates"]
print(c["upperLeft"][0], c["lowerRight"][1], c["upperRight"][0], c["upperLeft"][1])'
}

# the lakes from the largest to the smallest: the map sections follow the same order
ALL_LAKES="bodensee lacleman lacneuchatel vierwaldstaettersee thunersee brienzersee zuerichsee bielersee"
ALL_LAKES="$ALL_LAKES zugersee walensee murtensee sempachersee sarnersee hallwilersee aegerisee lacdejoux"
ALL_LAKES="$ALL_LAKES lagomaggiore lungernsee baldeggersee silsersee silvaplanersee rotsee"
# env LAKES narrows the build to a few lakes, e.g. LAKES="zugersee rotsee"
LAKES="${LAKES:-$ALL_LAKES}"
# keep only the lakes with tiles on disk
present=""
for lake in $LAKES; do
	ls "$SRC/$lake"/*.asc >/dev/null 2>&1 && present="$present $lake" || echo "no tiles of $lake, skipped"
done
LAKES=${present# }
[ -n "$LAKES" ] || { echo "No lake tiles in $SRC" >&2; exit 1; }

# ---------------------------------------------------------------- the depth rasters
step "the depth rasters of $(( $(echo $LAKES | wc -w) )) lakes at $CELL deg"
rm -rf "$TMP/depth"; mkdir -p "$TMP/depth"
for lake in $LAKES; do
	level=$(level_of "$lake")
	[ -n "$level" ] || { echo "no water level of $lake, set LAKE_LEVELS=$lake=LEVEL" >&2; exit 1; }
	vrt="$TMP/$lake.vrt"
	gdalbuildvrt -q "$vrt" "$SRC/$lake"/*.asc
	# the height of the bed as depth below the water level: the water is negative, the land is NaN
	# the .asc tiles carry no .prj, the product is always CHLV95/LN02 (EPSG:2056); the nodata value is in the
	# header and differs between the lakes (-9999 and -3.4e38), so GDAL takes it from there
	gdalwarp -q -overwrite -s_srs EPSG:2056 -t_srs EPSG:4326 -tr "$CELL" "$CELL" -r average \
		-dstnodata nan "$vrt" "$TMP/${lake}_h.tif"
	gdal_calc.py -A="$TMP/${lake}_h.tif" --calc="numpy.where(A < $level, A - $level, numpy.nan)" \
		--outfile="$TMP/depth/$lake.tif" --type=Float32 >/dev/null 2>&1 \
		|| { echo "gdal_calc failed for $lake" >&2; exit 1; }
	rm -f "$TMP/${lake}_h.tif" "$vrt"
	stats=$(gdalinfo -json -stats "$TMP/depth/$lake.tif" 2>/dev/null | python3 -c 'import json, sys; b = json.load(sys.stdin)["bands"][0]; print("%.1f..%.1f" % (b["minimum"], b["maximum"]))')
	step "$lake: level $level m, depth $stats m"
done

# the water outline the map draws: the fill, the bands, the contours and the soundings must end on it,
# not on the edge of the raster cell that holds the shore
WATER="$DATA/src/swiss_lakes/water.gpkg"
wargs=()
for lake in $LAKES; do
	name=$(lake_name "$lake")
	[ -n "$name" ] && wargs+=(--lake "$lake=$name")
done
if [ ${#wargs[@]} -gt 0 ]; then
	step "the water outlines"
	timeout 900 python3 "$HERE/osm_lake_outline.py" "$WATER" "${wargs[@]}" 2>&1 | tail -3 || true
fi

# ---------------------------------------------------------------- the gap between the data and the shore
# The survey tiles stop at the shore cell and the map's shore is the OSM outline: the strip between them
# has no depth, and the shallow bands stop short of the land. The fill solves laplace(h)=0 there with the
# land at 0 m and the survey depths as boundaries; the islands are holes of the water polygon, so they are
# land as well. Narrow channels (river beds, deltas) are left alone: the fill must stay near open water.
FILL_DIR="$TMP/fill"; mkdir -p "$FILL_DIR"
lake_in_water() {
	[ -f "$WATER" ] || return 1
	python3 - "$WATER" "$1" <<-'PY'
	import sys
	from osgeo import ogr
	src = ogr.Open(sys.argv[1])
	lyr = src.GetLayer(0) if src is not None else None
	if lyr is None:
	    sys.exit(1)
	lyr.SetAttributeFilter("lake = '%s'" % sys.argv[2].replace("'", "''"))
	sys.exit(0 if lyr.GetFeatureCount() else 1)
	PY
}
if [ "${SHORE_FILL:-1}" != 0 ] && [ -f "$WATER" ]; then
	step "the gap between the data and the shore"
	for lake in $LAKES; do
		tif="$TMP/depth/$lake.tif"
		[ -f "$tif" ] || continue
		if ! lake_in_water "$lake"; then
			echo "$lake: no water outline, the shore gap is left as it is"
			continue
		fi
		out="$FILL_DIR/$lake"; mkdir -p "$out"
		python3 "$HERE/shore_fill.py" "$tif" "$WATER" "$out" --lake "$lake" \
			--name "${lake}_filled" --limit "${SHORE_FILL_LIMIT:-500}" \
			--data-limit "${SHORE_FILL_DATA:-300}" \
			--open-width "${SHORE_FILL_OPEN_WIDTH:-50}" \
			--open-distance "${SHORE_FILL_OPEN_DISTANCE:-100}" \
			--edge "${SHORE_FILL_EDGE:-50}" \
			--tile "${SHORE_FILL_TILE:-1024}" --margin "${SHORE_FILL_MARGIN:-256}" -j "$JOBS" \
			2>&1 | tail -2
	done
else
	step "the shore fill is off"
fi
# the raster for the contours, the areas and the soundings: the filled one when it exists
depth_of() {
	local filled="$FILL_DIR/$1/${1}_filled.vrt"
	if [ -f "$filled" ]; then echo "$filled"; else echo "$TMP/depth/$1.tif"; fi
}

# ---------------------------------------------------------------- the contours
step "the contours"
rm -f "$TMP/contours.gpkg" "$TMP/depth.fgb"
# gdal_contour stops at the first level that is not deeper than the previous one, so the levels go deepest first
fl=$(echo "$LEVELS" | tr ' ' '\n' | sort -rn | awk '{printf "-%s ", $1}')
first=1
for lake in $LAKES; do
	rm -f "$TMP/${lake}_c.gpkg" "$TMP/${lake}.fgb"
	# shellcheck disable=SC2086
	gdal_contour -q -a elev -fl $fl "$(depth_of "$lake")" "$TMP/${lake}_c.gpkg"
	# one grid cell is a few metres: single-cell rings and short pieces are noise of the survey, not a contour
	# the survey stops short of the OSM shore: an open line ending within CONTOUR_SNAP of the water outline is
	# extended to it, otherwise the band face wraps around the dangling end and takes the wrong class
	water_opt=()
	[ -f "$WATER" ] && water_opt=(--water "$WATER" --lake "$lake" --snap "${CONTOUR_SNAP:-100}")
	python3 "$HERE/depth_contours_filter.py" "$TMP/${lake}_c.gpkg" "$TMP/${lake}.fgb" --cell "$CELL" \
		--min-ring-cells "${MIN_RING_CELLS:-4}" --min-line-m "${MIN_LINE_M:-30}" \
		--simplify "${CONTOUR_SIMPLIFY:-0}" --simplify-m "${CONTOUR_SIMPLIFY_M:-0}" \
		"${water_opt[@]}"
	if [ "$first" = 1 ]; then upd=""; else upd="-update -append"; fi
	ogr2ogr -q -f GPKG $upd -nln contours "$TMP/contours.gpkg" "$TMP/${lake}.fgb"
	first=0
	rm -f "$TMP/${lake}_c.gpkg" "$TMP/${lake}.fgb"
done
step "contours: $(ogrinfo -so -al "$TMP/contours.gpkg" contours 2>/dev/null | grep 'Feature Count')"

if [ -f "$WATER" ]; then
	step "the contours cut by the water outline"
	ogr2ogr -q -f GPKG "$TMP/contours_wet.gpkg" "$TMP/contours.gpkg" -clipsrc "$WATER" \
		|| { echo "cannot cut the contours by $WATER" >&2; exit 1; }
	mv "$TMP/contours_wet.gpkg" "$TMP/contours.gpkg"
fi

step "the contours as osm"
python3 "$V4/ogr2osm.py" -f --max-segment="${MAX_SEGMENT:-50}" -t "$V4/translations/contours_depth.py" \
	-o "$TMP/$NAME.osm" "$TMP/contours.gpkg" >/dev/null
gzip -f "$TMP/$NAME.osm"

# ---------------------------------------------------------------- the depth areas
# The bands shallower than 2, 5 and 10 m, as the other depth maps fill them. The depth raster keeps the land as a
# positive height, so a band traced from it would cover the shore: the land is set to a sentinel below every level,
# and only the water cells take part. The OSM land polygons cover the lakes themselves, so there is nothing to cut
# the bands out of: the areas are cut by an empty layer.
AREA_STEP=""
if [ "${AREAS:-1}" != 0 ]; then
	step "the depth areas"
	python3 - "$TMP/no_land.gpkg" <<-'PY'
	import sys
	from osgeo import ogr, osr
	drv = ogr.GetDriverByName('GPKG')
	ds = drv.CreateDataSource(sys.argv[1])
	srs = osr.SpatialReference(); srs.ImportFromEPSG(4326)
	srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
	ds.CreateLayer('land', srs, ogr.wkbPolygon)
	ds = None
	PY
	rm -f "$TMP/areas.gpkg"
	for lake in $LAKES; do
		[ -f "$TMP/contours.gpkg" ] || continue
		[ -f "$TMP/areas.gpkg" ] && append=--append || append=""
		# The band boundaries are the contour lines themselves: a band traced from the raster cells is a
		# staircase, and at a high zoom the isobath visibly leaves it. The faces of the water outline and the
		# 2/5/10 m lines are classified by the same filled raster the lines came from.
		split_opt=()
		[ "${BAND_SPLIT:-1}" != 0 ] && split_opt=(--split)
		python3 "$HERE/depth_bands_from_contours.py" "$TMP/contours.gpkg" "$WATER" "$TMP/areas.gpkg" \
			--lake "$lake" --raster "$(depth_of "$lake")" --cell "${BAND_CELL:-0.05}" --snap "${BAND_SNAP:-10}" \
			"${split_opt[@]}" $append \
			|| { echo "the bands of $lake failed" >&2; exit 1; }
	done
	if [ "${BAND_CHECK:-1}" != 0 ] && [ -f "$TMP/areas.gpkg" ]; then
		step "the band fill check"
		bad=0
		for lake in $LAKES; do
			tif="$(depth_of "$lake")"
			[ -f "$tif" ] || continue
			cov_opt=()
			[ "${BAND_COVERAGE:-200}" != 0 ] && cov_opt=(--coverage "${BAND_COVERAGE:-200}")
			req_opt=()
			if [ -n "${BAND_REQUIRE:-}" ]; then
				set -- $BAND_REQUIRE
				while [ "$#" -ge 3 ]; do
					req_opt+=(--require "$1" "$2" "$3")
					shift 3
				done
			fi
			python3 "$HERE/depth_fill_check.py" "$TMP/areas.gpkg" "$tif" --lake "$lake" \
				--margin "${BAND_CHECK_MARGIN:-1.5}" --min-area-km2 "${BAND_CHECK_AREA:-0.01}" \
				--max-report "${BAND_CHECK_REPORT:-10}" "${cov_opt[@]}" "${req_opt[@]}" || bad=1
		done
		if [ "$bad" != 0 ] && [ "${BAND_CHECK_STRICT:-0}" != 0 ]; then
			echo "the band fill check failed" >&2
			exit 1
		fi
	fi
	if [ -f "$TMP/areas.gpkg" ]; then
		step "depth areas: $(ogrinfo -so -al "$TMP/areas.gpkg" areas 2>/dev/null | grep 'Feature Count')"
		[ -f "$WATER" ] && wopt="--water $WATER" || wopt=""
		# The faces of areas.gpkg are already disjoint and bounded by the isobaths (0-2, 2-5, 5-10 m).
		# --cumulative grows every class over the shallower ones, so the polygons lie under each other;
		# without it every band follows exactly the two isobaths that bound it.
		cum_opt=()
		[ "${BAND_CUMULATIVE:-0}" != 0 ] && cum_opt=(--cumulative)
		python3 "$HERE/depth_areas_osm.py" "$TMP/areas.gpkg" "$OUT/${NAME}_areas.osm.gz" --layer areas --field mindepth \
			"${cum_opt[@]}" --land "$TMP/no_land.gpkg" $wopt --simplify "${AREA_SIMPLIFY:-0}" --max-segment "${MAX_SEGMENT:-50}" 2>&1 | grep -v numpy
		AREA_STEP="$OUT/${NAME}_areas.osm.gz"
	fi
fi

# ---------------------------------------------------------------- the map sections
OBFS=()
if [ -n "$MAP_CREATOR" ]; then
	step "the contours section"
	dir="$TMP/section_contours"; mkdir -p "$dir"
	(cd "$dir" && JAVA_OPTS="${JAVA_OPTS:--Xmx8g}" bash "$MAP_CREATOR/utilities.sh" generate-single-map \
		"$dir/contours.obf" --name="$NAME" ${SECTION_ZOOMS:+--map-zooms="$SECTION_ZOOMS"} "$TMP/$NAME.osm.gz" \
		${RENDERING_TYPES:+--rendering-types=$RENDERING_TYPES} > obf.log 2>&1) || { tail -20 "$dir/obf.log" >&2; exit 1; }
	OBFS+=("$dir/contours.obf")
	if [ -n "$AREA_STEP" ]; then
		step "the areas section"
		dir="$TMP/section_areas"; mkdir -p "$dir"
		(cd "$dir" && JAVA_OPTS="${JAVA_OPTS:--Xmx8g}" bash "$MAP_CREATOR/utilities.sh" generate-single-map \
			"$dir/areas.obf" --name="$NAME" ${SECTION_ZOOMS:+--map-zooms="$SECTION_ZOOMS"} "$AREA_STEP" \
			${RENDERING_TYPES:+--rendering-types=$RENDERING_TYPES} > obf.log 2>&1) || { tail -20 "$dir/obf.log" >&2; exit 1; }
		OBFS+=("$dir/areas.obf")
	fi
fi

i=0
for tier in $TIERS; do
	i=$((i + 1)); spacing=${tier%%:*}; zooms=${tier#*:}
	step "soundings every $spacing deg from zoom $zooms"
	parts=(); ln=0
	for lake in $LAKES; do
		tif=$(depth_of "$lake")
		read -r W S E N <<< "$(grid_extent "$tif")"
		[ -z "$BBOX" ] || read -r W S E N <<< "$BBOX"
		wet=()
		[ -f "$WATER" ] && wet=(--water "$WATER")
		interp=()
		[ -f "$FILL_DIR/$lake/${lake}_filled_interp.vrt" ] && interp=(--interp "$FILL_DIR/$lake/${lake}_filled_interp.vrt")
		python3 "$HERE/depth_points_osm.py" "$tif" "$TMP/points_${lake}.osm.gz" --bbox "$W" "$S" "$E" "$N" \
			--shoalest "$spacing" --thin "${wet[@]}" "${interp[@]}" --first-id $((i * 1000000000 + ln * 100000000))
		parts+=("$TMP/points_${lake}.osm.gz")
		ln=$((ln + 1))
	done
	[ ${#parts[@]} -gt 0 ] || continue
	zcat "${parts[@]}" | grep '^<node' > "$TMP/nodes.txt" || true
	{
		printf "<?xml version='1.0' encoding='UTF-8'?>\n<osm version='0.6' upload='false' generator='depth_points_osm'>\n"
		cat "$TMP/nodes.txt"
		printf '</osm>\n'
	} | gzip > "$OUT/${NAME}_points$i.osm.gz"
	rm -f "$TMP/nodes.txt"
	step "soundings: $(zcat "$OUT/${NAME}_points$i.osm.gz" | grep -c '^<node')"
	if [ -n "$MAP_CREATOR" ]; then
		dir="$TMP/section_points$i"; mkdir -p "$dir"
		(cd "$dir" && JAVA_OPTS="${JAVA_OPTS:--Xmx8g}" bash "$MAP_CREATOR/utilities.sh" generate-single-map \
			"$dir/points$i.obf" --name="$NAME" --map-zooms="$zooms" "$OUT/${NAME}_points$i.osm.gz" \
			${RENDERING_TYPES:+--rendering-types=$RENDERING_TYPES} > points.log 2>&1) || { tail -20 "$dir/points.log" >&2; exit 1; }
		OBFS+=("$dir/points$i.obf")
	fi
done

if [ -n "$MAP_CREATOR" ]; then
	step "merge ${#OBFS[@]} map sections into $NAME.depth.obf"
	dir=$(mktemp -d "$TMP/merge.XXXX")
	(cd "$dir" && JAVA_OPTS="${JAVA_OPTS:--Xmx16g}" bash "$MAP_CREATOR/utilities.sh" merge-index "$OUT/$NAME.depth.obf" \
		"${OBFS[@]}" > merge.log 2>&1) || { tail -20 "$dir/merge.log" >&2; exit 1; }
	rm -rf "$dir"
	step "done: $OUT/$NAME.depth.obf ($(du -h "$OUT/$NAME.depth.obf" | cut -f1))"
fi
