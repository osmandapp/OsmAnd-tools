#!/usr/bin/env bash
# Check what download_all.sh left in DIR before building depth OBFs, and build a VRT per gridded source.
#
#   check_sources.sh DIR [-j JOBS]
#
# For every source: file count against the expected one, every raster opens (gdalinfo), CRS is EPSG:4326 where
# expected, a known sea point is below 0 m (depths are negative), and leftover archives or partial files.
# Writes DIR/mask/land_polygons.gpkg (indexed copy of the land polygons) and DIR/src/gebco/gebco_2026.vrt, DIR/src/gebco_tid/gebco_2026_tid.vrt, DIR/src/emodnet/emodnet_2024.vrt,
# DIR/src/cudem/cudem.vrt. Prints OK / WARN / FAIL lines and exits with 1 when anything failed.
set -uo pipefail

OUT=""; JOBS=4
while [ $# -gt 0 ]; do
	case "$1" in
		-j) JOBS=$2; shift 2 ;;
		-h|--help) sed -n '2,9p' "$0"; exit 0 ;;
		*) OUT=$1; shift ;;
	esac
done
[ -n "$OUT" ] && [ -d "$OUT" ] || { echo "Give the download folder: check_sources.sh DIR" >&2; exit 1; }
for tool in gdalinfo gdallocationinfo gdalbuildvrt ogrinfo; do
	command -v $tool >/dev/null || { echo "FAIL $tool not found (GDAL)" >&2; exit 1; }
done
SRC="$OUT/src"; FAILED=0

ok()   { echo "OK   $*"; }
warn() { echo "WARN $*"; }
fail() { echo "FAIL $*"; FAILED=1; }

# rasters_open NAME FILE... : every raster opens; prints the broken ones
rasters_open() {
	local name=$1; shift
	local broken
	broken=$(printf "%s\0" "$@" | xargs -0 -P "$JOBS" -I{} sh -c 'gdalinfo "$1" >/dev/null 2>&1 || echo "$1"' _ {})
	if [ -n "$broken" ]; then fail "$name: $(echo "$broken" | wc -l | tr -d ' ') rasters do not open:"; echo "$broken" | sed 's/^/       /'
	else ok "$name: all $# rasters open"; fi
}

# count NAME ACTUAL EXPECTED
count() {
	if [ "$2" -eq "$3" ]; then ok "$1: $2 files"
	elif [ "$2" -eq 0 ]; then fail "$1: no files"
	else fail "$1: $2 files, expected $3"; fi
}

# leftovers NAME DIR : archives that were not unzipped and partial downloads
leftovers() {
	local n
	n=$(find "$2" -maxdepth 2 \( -name '*.zip' -o -name '*.part*' -o -name '*.tmp' \) 2>/dev/null | wc -l | tr -d ' ')
	[ "$n" -eq 0 ] || warn "$1: $n archives or partial files left (download or unzip did not finish)"
}

# crs FILE : EPSG code of the raster's coordinate system - the last ID of its WKT, the first ones are its datum,
# ellipsoid and units
crs() {
	gdalinfo -json "$1" 2>/dev/null | python3 -c '
import json, re, sys
wkt = json.load(sys.stdin).get("coordinateSystem", {}).get("wkt", "")
ids = re.findall(r"ID\[\"EPSG\",(\d+)\]", wkt)
print("EPSG:" + ids[-1] if ids else "unknown")' 2>/dev/null || echo unknown
}

# epsg4326 NAME FILE
epsg4326() {
	local c; c=$(crs "$2")
	if [ "$c" = "EPSG:4326" ]; then ok "$1: EPSG:4326"; else warn "$1: $c, not EPSG:4326"; fi
}

# sea NAME RASTER LON LAT PLACE : the value at a sea point is negative
sea() {
	local v
	v=$(gdallocationinfo -valonly -wgs84 "$2" "$3" "$4" 2>/dev/null | head -1)
	# -9999 and below are nodata markers of CUDEM and others, not depths
	if [ -z "$v" ] || [ "$v" = "nan" ] || awk -v v="$v" 'BEGIN{exit !(v <= -9999)}'; then warn "$1: no value at $5 ($3 $4)"
	elif awk -v v="$v" 'BEGIN{exit !(v < 0)}'; then ok "$1: $5 is $v m"
	else fail "$1: $5 is $v m, a depth should be negative"; fi
}

# vrt NAME OUTPUT [-allow_projection_difference] FILE... : every file must end up in the VRT - gdalbuildvrt skips a
# file whose coordinate system differs from the first one's with only a warning
vrt() {
	local name=$1 output=$2 opts=(); shift 2
	if [ "${1:-}" = -allow_projection_difference ]; then opts=("$1"); shift; fi
	if ! gdalbuildvrt -q "${opts[@]}" "$output" "$@" 2>/dev/null; then fail "$name: gdalbuildvrt failed"; return; fi
	local n; n=$(grep -c '<SourceFilename' "$output")
	if [ "$n" -eq $# ]; then ok "$name: $(basename "$output") of $n files"
	else fail "$name: $(basename "$output") has $n of $# files (different coordinate systems?)"; fi
}

echo "== mask"
SHP=$(find "$OUT/mask" -name 'land_polygons.shp' 2>/dev/null | head -1)
if [ -z "$SHP" ]; then fail "mask: land_polygons.shp not found"
else
	n=$(ogrinfo -so -al "$SHP" 2>/dev/null | awk -F': ' '/Feature Count/{print $2}')
	if [ "${n:-0}" -gt 500000 ]; then ok "mask: $n land polygons"; else fail "mask: ${n:-0} land polygons, expected over 500000"; fi
	leftovers mask "$OUT/mask"
	# a shapefile has no spatial index, so cutting a region out of it reads the whole file; a GeoPackage has one
	GPKG="$OUT/mask/land_polygons.gpkg"
	if [ ! -f "$GPKG" ] || [ "$SHP" -nt "$GPKG" ]; then
		rm -f "$GPKG"
		if ogr2ogr -f GPKG -nln land_polygons -nlt MULTIPOLYGON -gt 65536 "$GPKG" "$SHP" 2>/dev/null; then ok "mask: land_polygons.gpkg written"
		else fail "mask: land_polygons.gpkg could not be written"; rm -f "$GPKG"; fi
	else ok "mask: land_polygons.gpkg is up to date"; fi
fi

echo "== gebco"
FILES=("$SRC"/gebco/gebco_2026_n*_geotiff.tif)
[ -e "${FILES[0]}" ] || FILES=()
count gebco ${#FILES[@]} 8
if [ ${#FILES[@]} -gt 0 ]; then
	rasters_open gebco "${FILES[@]}"
	epsg4326 gebco "${FILES[0]}"
	vrt gebco "$SRC/gebco/gebco_2026.vrt" "${FILES[@]}"
	sea gebco "$SRC/gebco/gebco_2026.vrt" -30 0 "mid-Atlantic"
	sea gebco "$SRC/gebco/gebco_2026.vrt" 150 -40 "Tasman Sea"
fi
leftovers gebco "$SRC/gebco"

echo "== gebco_tid"
FILES=("$SRC"/gebco_tid/gebco_2026_tid_*_geotiff.tif)
[ -e "${FILES[0]}" ] || FILES=()
count gebco_tid ${#FILES[@]} 8
if [ ${#FILES[@]} -gt 0 ]; then
	rasters_open gebco_tid "${FILES[@]}"
	vrt gebco_tid "$SRC/gebco_tid/gebco_2026_tid.vrt" "${FILES[@]}"
fi
leftovers gebco_tid "$SRC/gebco_tid"

echo "== emodnet"
FILES=(); while IFS= read -r f; do FILES+=("$f"); done < <(find -L "$SRC/emodnet" -mindepth 2 -name '*_2024.tif' 2>/dev/null | sort)
count emodnet ${#FILES[@]} 59
if [ ${#FILES[@]} -gt 0 ]; then
	rasters_open emodnet "${FILES[@]}"
	epsg4326 emodnet "${FILES[0]}"
	vrt emodnet "$SRC/emodnet/emodnet_2024.vrt" "${FILES[@]}"
	sea emodnet "$SRC/emodnet/emodnet_2024.vrt" 4 55 "North Sea"
	sea emodnet "$SRC/emodnet/emodnet_2024.vrt" 18 35 "Ionian Sea"
fi
leftovers emodnet "$SRC/emodnet"

echo "== noaa_enc"
n=$(find "$SRC/noaa_enc" -name '*.000' 2>/dev/null | wc -l | tr -d ' ')
if [ "$n" -gt 1000 ]; then ok "noaa_enc: $n S-57 cells"; else fail "noaa_enc: $n S-57 cells, expected over 1000"; fi
CELL=$(find "$SRC/noaa_enc" -name '*.000' 2>/dev/null | head -1)
if [ -n "$CELL" ]; then
	if ogrinfo -so "$CELL" 2>/dev/null | grep -q .; then ok "noaa_enc: $(basename "$CELL") opens"; else fail "noaa_enc: $(basename "$CELL") does not open"; fi
fi
leftovers noaa_enc "$SRC/noaa_enc"

echo "== cudem"
LIST=$(ls "$SRC"/cudem/urllist*.txt 2>/dev/null | head -1)
FILES=(); while IFS= read -r f; do FILES+=("$f"); done < <(find -L "$SRC/cudem/tiles" -name '*.tif' 2>/dev/null | sort)
count cudem ${#FILES[@]} "$( [ -n "$LIST" ] && grep -cE '\.tif$' "$LIST" || echo 373)"
if [ ${#FILES[@]} -gt 0 ]; then
	rasters_open cudem "${FILES[@]}"
	epsg4326 cudem "${FILES[0]}"
	# the tiles are NAD83, some with NAVD88 heights declared and some without: the same horizontal system
	vrt cudem "$SRC/cudem/cudem.vrt" -allow_projection_difference "${FILES[@]}"
	sea cudem "$SRC/cudem/cudem.vrt" -94.8 29.2 "Galveston Bay entrance"
fi

echo "== norway"
GDB=$(find "$SRC/norway" -maxdepth 3 -name '*.gdb' -type d 2>/dev/null | head -1)
if [ -z "$GDB" ]; then fail "norway: no .gdb folder"
else
	# layers are listed as "1: name" by older GDAL and "Layer: name" by GDAL 3.x
	n=$(ogrinfo -so "$GDB" 2>/dev/null | grep -cE '^([0-9]+|Layer): ')
	if [ "$n" -gt 0 ]; then ok "norway: $(basename "$GDB") with $n layers"; else fail "norway: $(basename "$GDB") does not open"; fi
fi
leftovers norway "$SRC/norway"

echo "== netherlands"
FILES=("$SRC"/netherlands/*.tif)
[ -e "${FILES[0]}" ] || FILES=()
count netherlands ${#FILES[@]} 3
if [ ${#FILES[@]} -gt 0 ]; then
	rasters_open netherlands "${FILES[@]}"
	for f in "${FILES[@]}"; do
		echo "     $(basename "$f"): $(crs "$f")"
	done
fi

echo
if [ $FAILED -eq 0 ]; then echo "All checks passed"; else echo "Some checks FAILED"; fi
exit $FAILED
