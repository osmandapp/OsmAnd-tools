#!/usr/bin/env bash
# Depth contours and soundings of the Great Lakes as .osm.gz and OBF.
#
# The source is the NOAA NCEI "Great Lakes Bathymetry" collection: the charted contours of Erie, Michigan, Huron
# and Ontario (ESRI shapefiles, WGS84, the depth in metres in the DEPTH field), the 3 arc-second grids of all five
# lakes and the soundings from those grids: the negative values are the depths below the low water datum of the
# lake, the positive ones are the land above it. The contours and the fill areas are traced from the same grid by
# default: the charted contours are a different, more detailed model, and a fill cut from the grid would not lie on
# them (Erie, Michigan, Huron and Ontario). CONTOURS=vector keeps the charted lines for comparison.
#
#   build_depth_great_lakes.sh -D DATA_DIR [-n NAME] [-b "W S E N"] [-c MAP_CREATOR_DIR] [-j JOBS] [-k]
#
#   -D DATA_DIR  folder of download_all.sh: the contours DATA_DIR/src/great_lakes/contours/<lake>/*.shp, the grids
#                DATA_DIR/src/great_lakes/grids/<lake>_lld/<lake>_lld.tif, the output DATA_DIR/build
#   -n NAME      default Great_Lakes_contours
#   -b BBOX      only this box (lon/lat), default all of the lakes
#   -c MAP_CREATOR_DIR  unzipped OsmAndMapCreator: writes OUT_DIR/NAME.depth.obf
#   -j JOBS      default 4
#   -k           keep: do nothing when OUT_DIR/NAME.depth.obf already exists
#   env TIERS    soundings "SPACING:ZOOMS ...", the shallowest sounding of every SPACING degree cell shown from ZOOMS,
#                default "0.05:9-10 0.02:11-12 0.01:13 0.005:14 0.0025:15 0.001:16-22"
#   env LEVELS   the contour depths in metres, default "2 5 10 20 50 100 200"
#   env CONTOURS  grid (default) traces every lake from its 3 arc-second grid, the same grid as the depth areas and
#                the soundings, so the fill boundaries lie on the isobaths; vector keeps the charted NOAA contours
#                of Erie, Michigan, Huron and Ontario, which are a more detailed but different model
#   env SHORE_FILL  1 (default) fills the gap between the negative grid values and the OSM shore when raster data
#                is used (CONTOURS=grid, and Superior in either mode); 0 keeps the gaps. With CONTOURS=vector the
#                charted lakes are not filled: their lines are another model and the fill would not lie on them.
#   env SHORE_FILL_LIMIT  fill only where distance_to_land + distance_to_data <= this, metres, default 3000
#   env SHORE_FILL_DATA   fill no farther than this from real data, metres, default 2000
#   env SHORE_FILL_OPEN_WIDTH  water at least this far from land is open water, metres, default 100
#   env SHORE_FILL_OPEN_DISTANCE  fill only within this distance of open water (at least OPEN_WIDTH from land),
#                metres, default 150; keeps the fill out of narrow river beds and deltas
#   env SHORE_FILL_EDGE  fill within this distance of land even where the open-water guard fails, metres,
#                default 200; without it a narrow channel keeps a cell-wide gap at the banks, and the bands
#                stop short of an island
#   env SHORE_FILL_TILE / SHORE_FILL_MARGIN  fill tile and overlap, cells, defaults 2048 and 256
#   env RENDERING_TYPES  rendering_types.xml for OsmAndMapCreator instead of its own (new tags before a nightly)
#   env MAX_SEGMENT  the longest contour segment in kilometres, default 50 (see ogr2osm.py --max-segment)
#   env AREAS        1 (default) writes the depth areas of the grids, 0 leaves them out
#   env AREA_SIMPLIFY  simplification of the areas in degrees, default 0 (keeps the vertices of the contours)
#   env CONTOUR_SIMPLIFY_M  simplify the grid contours by this many metres before the bands are built, default 2.
#                MapCreator drops points of its own when it writes the map (skipSomeNodes), and on a curved isobath
#                the chords cut the corner, so the band edge leaves the line. Simplifying first keeps the vertices
#                of both; 0 disables it. Not used with CONTOURS=vector, whose bands come from the grid, not the lines.
#   env BAND_CHECK  1 (default) checks every grid band polygon at many points of its area against the source grid;
#                0 skips the check (CONTOURS=vector has no disjoint band faces and is not checked)
#   env BAND_CHECK_STRICT  0 (default) only reports a significant defect, 1 fails the build
#   env BAND_CHECK_MARGIN / BAND_CHECK_AREA  boundary tolerance, metres (default 3), and the smallest bad area
#                reported, km2 (default 0.05)
#
# The contours keep the standard levels (2, 5, 10, 20, 50, 100, 200 m): the metre steps of the source have no
# contourtype of their own, so they would be drawn as an untyped line from zoom 12 and would clutter the chart.
# The vertical datum of the source is the low water datum of every lake, so the zero line is the shore.
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
V4=$(cd "$HERE/.." && pwd)
NAME=Great_Lakes_contours; BBOX="${BBOX:-}"; DATA=""; OUT="${OUT:-}"; MAP_CREATOR=""; KEEP=0; JOBS=4
TIERS="${TIERS:-0.05:9-10 0.02:11-12 0.01:13 0.005:14 0.0025:15 0.001:16-22}"
LEVELS="${LEVELS:-2 5 10 20 50 100 200}"
CONTOURS="${CONTOURS:-grid}"
while [ $# -gt 0 ]; do
	case "$1" in
		-n) NAME=$2; shift 2 ;;
		-b) BBOX=$2; shift 2 ;;
		-c) MAP_CREATOR=$2; shift 2 ;;
		-D) DATA=$2; shift 2 ;;
		-j) JOBS=$2; shift 2 ;;
		-k) KEEP=1; shift ;;
		-h|--help) sed -n '2,50p' "$0"; exit 0 ;;
		*) echo "Unknown option $1" >&2; exit 1 ;;
	esac
done
[ -n "$DATA" ] || { echo "No -D DATA_DIR, see --help" >&2; exit 1; }
GL="$DATA/src/great_lakes"
OUT="${OUT:-$DATA/build}"
[ -f "$OUT/$NAME.depth.obf" ] && [ $KEEP -eq 1 ] && { echo "$OUT/$NAME.depth.obf exists, keep"; exit 0; }
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
# the lakes in the order of their place in the map
LAKES="${LAKES:-superior michigan huron erie ontario}"
# the name of the lake in OSM: the water outline is looked up by it (see osm_lake_outline.py)
lake_name() {
	case "$1" in
		superior) echo "Lake Superior" ;;
		michigan) echo "Lake Michigan" ;;
		huron) echo "Lake Huron" ;;
		erie) echo "Lake Erie" ;;
		ontario) echo "Lake Ontario" ;;
		*) echo "" ;;
	esac
}
# the grid of a lake: the soundings and the contours of Superior come from it
grid_of() { echo "$GL/grids/$1_lld/$1_lld.tif"; }
# the extent of a raster as "W S E N"
grid_extent() {
	gdalinfo -json "$1" | python3 -c '
import json, sys
c = json.load(sys.stdin)["cornerCoordinates"]
print(c["upperLeft"][0], c["lowerRight"][1], c["upperRight"][0], c["upperLeft"][1])'
}

# the water outline the map draws: the fill, the bands, the contours and the soundings must end on it, not on the
# edge of the grid cell that holds the shore
WATER="$DATA/src/great_lakes/water.gpkg"
wargs=()
for lake in $LAKES; do
	name=$(lake_name "$lake")
	[ -n "$name" ] && wargs+=(--lake "$lake=$name")
done
if [ ${#wargs[@]} -gt 0 ]; then
	step "the water outlines"
	mkdir -p "$(dirname "$WATER")"
	timeout 900 python3 "$HERE/osm_lake_outline.py" "$WATER" "${wargs[@]}" 2>&1 | tail -3 || true
fi

# ---------------------------------------------------------------- the gap between the grid and the shore
# The grid stores the land as a positive height, so its zero line is a cell edge, not the OSM shore the map draws:
# the strip between the last negative cell and the shore has no depth, and the shallow bands stop short of the
# land. Only a raster model gets this fill: the charted lines of the vector mode are another model, and a grid fill
# would not lie on them. Superior has no charted lines and is filled in either mode.
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
	step "the gap between the grid data and the shore"
	for lake in $LAKES; do
		if [ "$CONTOURS" != grid ] && [ "$lake" != superior ]; then
			echo "$lake: the charted contours are not the grid model, no shore fill"
			continue
		fi
		tif=$(grid_of "$lake")
		[ -f "$tif" ] || { echo "no grid of $lake"; continue; }
		if ! lake_in_water "$lake"; then
			echo "$lake: no water outline, the shore gap is left as it is"
			continue
		fi
		out="$FILL_DIR/$lake"; mkdir -p "$out"
		python3 "$HERE/shore_fill.py" "$tif" "$WATER" "$out" --lake "$lake" \
			--name "${lake}_filled" --limit "${SHORE_FILL_LIMIT:-3000}" \
			--data-limit "${SHORE_FILL_DATA:-2000}" \
			--open-width "${SHORE_FILL_OPEN_WIDTH:-100}" \
			--open-distance "${SHORE_FILL_OPEN_DISTANCE:-150}" \
			--edge "${SHORE_FILL_EDGE:-200}" \
			--tile "${SHORE_FILL_TILE:-2048}" --margin "${SHORE_FILL_MARGIN:-256}" -j "$JOBS" \
			2>&1 | tail -2
	done
else
	step "the shore fill is off"
fi
# the raster for the contours, the areas and the soundings: the filled one when it exists
depth_of() {
	local filled="$FILL_DIR/$1/${1}_filled.vrt"
	if [ -f "$filled" ]; then echo "$filled"; else echo "$(grid_of "$1")"; fi
}

# ---------------------------------------------------------------- the contours
step "the contours"
rm -f "$TMP/contours.gpkg"
levels=$(echo "$LEVELS" | tr ' ' ',')
# gdal_contour stops at the first level that is not deeper than the previous one, so the levels go deepest first
fl=$(echo "$LEVELS" | tr ' ' '\n' | sort -rn | awk '{printf "-%s ", $1}')
first=1
for lake in michigan huron erie ontario; do
	if [ "$CONTOURS" = grid ]; then
		tif=$(depth_of "$lake")
		[ -f "$tif" ] || { echo "no grid of $lake"; continue; }
		step "$lake: the contours of its grid"
		rm -f "$TMP/${lake}_grid.fgb"
		# shellcheck disable=SC2086
		gdal_contour -q -a elev -fl $fl "$tif" "$TMP/${lake}_grid.fgb"
		if [ "$first" = 1 ]; then upd=""; else upd="-update -append"; fi
		ogr2ogr -q -f GPKG $upd -nln contours "$TMP/contours.gpkg" "$TMP/${lake}_grid.fgb" -nlt MULTILINESTRING \
			-dialect SQLite -sql "SELECT CAST(ABS(elev) AS INTEGER) AS depth, geometry FROM contour" -a_srs EPSG:4326
		rm -f "$TMP/${lake}_grid.fgb"
		first=0
		continue
	fi
	shp=$(ls "$GL/contours/$lake"/*.shp 2>/dev/null | head -1)
	[ -n "$shp" ] || { echo "no contours of $lake"; continue; }
	layer=$(basename "$shp" .shp)
	# the first lake writes the file, the others add their features to its layer
	if [ "$first" = 1 ]; then upd=""; else upd="-update -append"; fi
	ogr2ogr -q -f GPKG $upd -nln contours "$TMP/contours.gpkg" "$shp" -nlt MULTILINESTRING \
		-dialect SQLite -sql "SELECT CAST(DEPTH AS INTEGER) AS depth, geometry FROM $layer WHERE DEPTH IN ($levels)" \
		-a_srs EPSG:4326
	first=0
	step "$lake: $shp"
done

# Superior has no contours of its own: they come from its grid
if [ -f "$(grid_of superior)" ]; then
	step "superior: the contours of its grid"
	# shellcheck disable=SC2086
	gdal_contour -q -a elev -fl $fl "$(depth_of superior)" "$TMP/superior.fgb"
	ogr2ogr -q -f GPKG -update -append -nln contours "$TMP/contours.gpkg" "$TMP/superior.fgb" -nlt MULTILINESTRING \
		-dialect SQLite -sql "SELECT CAST(ABS(elev) AS INTEGER) AS depth, geometry FROM contour" -a_srs EPSG:4326
	rm -f "$TMP/superior.fgb"
fi
step "contours: $(ogrinfo -so -al "$TMP/contours.gpkg" contours 2>/dev/null | grep 'Feature Count')"

if [ -f "$WATER" ]; then
	step "the contours cut by the water outline"
	ogr2ogr -q -f GPKG "$TMP/contours_wet.gpkg" "$TMP/contours.gpkg" -clipsrc "$WATER" \
		|| { echo "cannot cut the contours by $WATER" >&2; exit 1; }
	mv "$TMP/contours_wet.gpkg" "$TMP/contours.gpkg"
fi

# The bands are built from these lines, so both must keep every vertex through the map writer: MapCreator
# simplifies a high-zoom section on its own (skipSomeNodes, ~0.58 m) and on a curved isobath its chords cut
# the corner. Simplify first, with a larger tolerance, and the writer has nothing left to drop.
simplify_m="${CONTOUR_SIMPLIFY_M:-2}"
if [ "$CONTOURS" = grid ] && [ "$simplify_m" != 0 ]; then
	step "the contours simplified by ${simplify_m} m before the bands"
	rm -f "$TMP/contours_simple.fgb" "$TMP/contours_simple.gpkg"
	python3 "$HERE/depth_contours_filter.py" "$TMP/contours.gpkg" "$TMP/contours_simple.fgb" \
		--cell 0.0008333333 --min-ring-cells 0 --min-line-m 0 --simplify-m "$simplify_m" \
		|| { echo "cannot simplify the contours" >&2; exit 1; }
	ogr2ogr -q -f GPKG -nln contours "$TMP/contours_simple.gpkg" "$TMP/contours_simple.fgb" \
		|| { echo "cannot write the simplified contours" >&2; exit 1; }
	mv "$TMP/contours_simple.gpkg" "$TMP/contours.gpkg"
	rm -f "$TMP/contours_simple.fgb"
fi

step "the contours as osm"
python3 "$V4/ogr2osm.py" -f --max-segment="${MAX_SEGMENT:-50}" -t "$V4/translations/contours_depth.py" -o "$TMP/$NAME.osm" "$TMP/contours.gpkg" >/dev/null
gzip -f "$TMP/$NAME.osm"

# ---------------------------------------------------------------- the depth areas
# The bands shallower than 2, 5 and 10 m, as the other depth maps fill them. The source grid keeps the land as a
# positive height, so a band traced from it would cover the shore: the land is set to a sentinel below every level,
# and only the water cells of the grid take part. The OSM land polygons cover the lakes themselves, so there is
# nothing to cut the bands out of: the areas are cut by an empty layer.
AREA_STEP=""
if [ "${AREAS:-1}" != 0 ]; then
	step "the depth areas of the grids"
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
	first=1
	for lake in $LAKES; do
		tif=$(depth_of "$lake")
		[ -f "$tif" ] || { echo "no grid of $lake"; continue; }
		step "$lake: depth areas"
		if [ "$CONTOURS" = grid ]; then
			# The band boundaries are the contour lines themselves: a band traced from the raster cells is a
			# staircase, and at a high zoom the isobath visibly leaves it. The faces of the water outline and
			# the 2/5/10 m lines are classified by the same filled raster the lines came from.
			[ -f "$TMP/areas.gpkg" ] && append=--append || append=""
			[ "${BAND_SPLIT:-1}" != 0 ] && split_opt=(--split)
			python3 "$HERE/depth_bands_from_contours.py" "$TMP/contours.gpkg" "$WATER" "$TMP/areas.gpkg" \
				--lake "$lake" --raster "$tif" --cell "${BAND_CELL:-0.25}" --snap "${BAND_SNAP:-100}" --join "${BAND_JOIN:-0}" \
				"${split_opt[@]}" $append \
				|| { echo "the bands of $lake failed" >&2; exit 1; }
		else
			# the charted vector lines are another model; the areas still come from the grid
			# the water as depth (negative), the land just below 0: a shallow sentinel keeps the shore in the 0-2
			# band, which the water outline then cuts back to the land; a deep sentinel (-1000) falls into the 10+
			# class, which is not written, and leaves a cell-wide hole in the 5-10 band at every island
			rm -f "$TMP/depth_areas.tif"
			gdal_calc.py -A="$tif" --calc="numpy.where(A < 0, A, -0.01)" --outfile="$TMP/depth_areas.tif" --type=Float32 >/dev/null 2>&1 \
				|| { echo "gdal_calc failed for $lake" >&2; exit 1; }
			for level_class in 2:0 5:2 10:5; do
				level=${level_class%%:*}; class=${level_class#*:}
				rm -f "$TMP/band.gpkg"
				gdal_contour -p -amin emin -amax emax -fl "-$level" "$TMP/depth_areas.tif" "$TMP/band.gpkg" >/dev/null
				# the polygons above the level (the upper level is out of reach); an older GDAL also writes the one below
				if [ "$first" = 1 ]; then upd=""; else upd="-update -append"; fi
				ogr2ogr -q -f GPKG $upd -nln areas "$TMP/areas.gpkg" "$TMP/band.gpkg" \
					-sql "SELECT geom, CAST($class AS REAL) AS mindepth FROM contour WHERE emin > -$level - 0.001 AND emax > -$level + 0.001"
				first=0
			done
		fi
	done
	rm -f "$TMP/depth_areas.tif" "$TMP/band.gpkg"
	if [ "${BAND_CHECK:-1}" != 0 ] && [ "$CONTOURS" = grid ] && [ -f "$TMP/areas.gpkg" ]; then
		step "the band fill check"
		bad=0
		for lake in $LAKES; do
			tif=$(depth_of "$lake")
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
				--margin "${BAND_CHECK_MARGIN:-3}" --min-area-km2 "${BAND_CHECK_AREA:-0.05}" \
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
		# the grid bands are disjoint, so each class takes the shallower ones (no inner rings); the charted -p
		# bands are already "shallower than" polygons and are cut apart
		if [ "$CONTOURS" = grid ]; then nesting=--cumulative; else nesting=--nested; fi
		python3 "$HERE/depth_areas_osm.py" "$TMP/areas.gpkg" "$OUT/${NAME}_areas.osm.gz" --layer areas --field mindepth \
			$nesting --land "$TMP/no_land.gpkg" $wopt --simplify "${AREA_SIMPLIFY:-0}" --max-segment "${MAX_SEGMENT:-50}" 2>&1 | grep -v numpy
		AREA_STEP="$OUT/${NAME}_areas.osm.gz"
	fi
fi

# ---------------------------------------------------------------- the soundings
OBFS=()
if [ -n "$MAP_CREATOR" ]; then
	# the map section of the contours: the name of the section is the name of the map
	dir="$TMP/section_contours"; mkdir -p "$dir"
	(cd "$dir" && JAVA_OPTS="${JAVA_OPTS:--Xmx8g}" bash "$MAP_CREATOR/utilities.sh" generate-single-map \
		"$dir/contours.obf" --name="$NAME" "$TMP/$NAME.osm.gz" \
		${RENDERING_TYPES:+--rendering-types=$RENDERING_TYPES} > obf.log 2>&1) || { tail -20 "$dir/obf.log" >&2; exit 1; }
	OBFS+=("$dir/contours.obf")
	# the areas keep the name of the map too: all the sections of one map share it
	if [ -n "$AREA_STEP" ]; then
		dir="$TMP/section_areas"; mkdir -p "$dir"
		(cd "$dir" && JAVA_OPTS="${JAVA_OPTS:--Xmx8g}" bash "$MAP_CREATOR/utilities.sh" generate-single-map \
			"$dir/areas.obf" --name="$NAME" "$AREA_STEP" \
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
		[ -f "$tif" ] || { echo "no grid of $lake"; continue; }
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
	# the soundings of all the lakes in one file: the nodes of all of them, with the head of the first
	zcat "${parts[@]}" | grep '^<node' > "$TMP/nodes.txt"
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
