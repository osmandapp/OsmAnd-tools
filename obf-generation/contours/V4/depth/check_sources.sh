#!/usr/bin/env bash
# Check what download_all.sh left in DIR before building depth OBFs, and build a VRT per gridded source.
#
#   check_sources.sh DIR [-j JOBS]
#
# For every source: file count against the expected one, every raster opens (gdalinfo), CRS is EPSG:4326 where
# expected, a known sea point is below 0 m (depths are negative), and leftover archives or partial files.
# Writes DIR/mask/land_polygons.gpkg (indexed copy of the land polygons) and DIR/src/gebco/gebco_2026.vrt, DIR/src/gebco_tid/gebco_2026_tid.vrt, DIR/src/emodnet/emodnet_2024.vrt,
# DIR/src/cudem/cudem.vrt, DIR/src/ireland/ireland.vrt, DIR/src/denmark/denmark.vrt, DIR/src/france/france.vrt. Checks
# DIR/src/uk/uk.vrt (the blended mosaic of download_all.sh) and its coastal fill uk_filled.vrt, and the masked EMODnet
# mosaic and coastal fill in DIR/src/france/fill, without rebuilding them. Prints OK / WARN / FAIL lines and exits with 1
# when anything failed.
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
count netherlands ${#FILES[@]} 5
if [ ${#FILES[@]} -gt 0 ]; then
	rasters_open netherlands "${FILES[@]}"
	for f in "${FILES[@]}"; do
		echo "     $(basename "$f"): $(crs "$f")"
	done
fi

echo "== ireland"
# INFOMAR tiles without data were left as .empty; the 10 m inshore grid goes on top of the 25 m one
# 25 m first: in a VRT the later files lie on top, so the 10 m grid wins where both have data
FILES=(); while IFS= read -r f; do FILES+=("$f"); done < <(find "$SRC/ireland/25m" -name '*.tif' 2>/dev/null | sort; find "$SRC/ireland/10m" -name '*.tif' 2>/dev/null | sort)
if [ ${#FILES[@]} -eq 0 ]; then fail "ireland: no files"
else
	ok "ireland: $(find "$SRC/ireland/25m" -name '*.tif' | wc -l | tr -d ' ') tiles of 25 m, $(find "$SRC/ireland/10m" -name '*.tif' | wc -l | tr -d ' ') of 10 m"
	rasters_open ireland "${FILES[@]}"
	epsg4326 ireland "${FILES[0]}"
	if gdalbuildvrt -q -resolution highest "$SRC/ireland/ireland.vrt" "${FILES[@]}" 2>/dev/null; then
		ok "ireland: ireland.vrt of ${#FILES[@]} files"
		sea ireland "$SRC/ireland/ireland.vrt" -6.05 53.33 "Dublin Bay"
	else fail "ireland: gdalbuildvrt failed"; fi
fi
leftovers ireland "$SRC/ireland"

echo "== france"
FILES=("$SRC"/france/*.tif)
[ -e "${FILES[0]}" ] || FILES=()
count france ${#FILES[@]} 11
if [ ${#FILES[@]} -gt 0 ]; then
	rasters_open france "${FILES[@]}"
	# zones of 5, 10 and 20 m: the VRT keeps the finest cell
	if gdalbuildvrt -q -resolution highest "$SRC/france/france.vrt" "${FILES[@]}" 2>/dev/null; then
		ok "france: france.vrt of ${#FILES[@]} files"
		sea france "$SRC/france/france.vrt" -1.67 43.40 "Saint-Jean-de-Luz bay"
	else fail "france: gdalbuildvrt failed"; fi
fi
leftovers france "$SRC/france"
FRFILL="$SRC/france/fill"
for v in france_base.tif france_with_emodnet.vrt france_filled.vrt france_filled_interp.vrt; do
	if [ ! -f "$FRFILL/$v" ]; then fail "france: fill/$v not found (run download_all.sh --only france)"
	elif gdalinfo "$FRFILL/$v" >/dev/null 2>&1; then ok "france: fill/$v opens"
	else fail "france: fill/$v does not open"; fi
done
# This file exists only when the SHOM grids really have an enclosed hole to fill with EMODnet.
if [ -f "$FRFILL/france_emodnet_fill.tif" ]; then
	if gdalinfo "$FRFILL/france_emodnet_fill.tif" >/dev/null 2>&1; then ok "france: fill/france_emodnet_fill.tif opens"
	else fail "france: fill/france_emodnet_fill.tif does not open"; fi
fi
if [ -f "$FRFILL/france_filled.vrt" ]; then
	# There is no SHOM zone at Pleherel: the separate EMODnet map covers it, the French map must not.
	v=$(python3 - "$SRC/emodnet/emodnet_2024.vrt" "$FRFILL/france_with_emodnet.vrt" "$FRFILL/france_filled.vrt" <<'EOF' 2>/dev/null
import math, sys
import rasterio
pts = [(-2.40, 48.66), (-2.3513, 48.6617)]
with rasterio.open(sys.argv[1]) as emodnet, rasterio.open(sys.argv[2]) as base, rasterio.open(sys.argv[3]) as fill:
    source = float(next(emodnet.sample([pts[0]]))[0])
    values = [(float(next(base.sample([pt]))[0]), float(next(fill.sample([pt]))[0])) for pt in pts]
print(f"EMODnet={source:g}; " + "; ".join(f"combined={a:g}, filled={b:g}" for a, b in values))
sys.exit(0 if math.isfinite(source) and source < 0
         and all(math.isnan(a) and math.isnan(b) for a, b in values) else 1)
EOF
)
	if [ $? -eq 0 ]; then ok "france: no EMODnet coastal band at Pleherel-Plage ($v)"
	else fail "france: EMODnet coastal band is present at Pleherel-Plage: $v"; fi
fi
if [ -f "$FRFILL/france_filled_interp.vrt" ]; then
	v=$(python3 - "$FRFILL/france_with_emodnet.vrt" "$FRFILL/france_filled.vrt" "$FRFILL/france_filled_interp.vrt" <<'EOF' 2>/dev/null
import math, sys
import rasterio
pt = (-2.0251, 48.6159)
with rasterio.open(sys.argv[1]) as base, rasterio.open(sys.argv[2]) as fill, rasterio.open(sys.argv[3]) as interp:
    before = float(next(base.sample([pt]))[0])
    depth = float(next(fill.sample([pt]))[0])
    marker = float(next(interp.sample([pt]))[0])
print(f"before={before:g} depth={depth:g} interp={marker:g}")
sys.exit(0 if math.isnan(before) and math.isfinite(depth) and depth < 0 and marker > 0 else 1)
EOF
)
	if [ $? -eq 0 ]; then ok "france: SHOM coastal gap at Saint-Malo is interpolated ($v)"
	else fail "france: SHOM coastal gap at Saint-Malo is not interpolated: $v"; fi
fi
if [ -f "$FRFILL/france_with_emodnet.vrt" ]; then
	v=$(python3 - "$SRC/emodnet/emodnet_2024.vrt" "$FRFILL/france_base.tif" "$FRFILL/france_with_emodnet.vrt" <<'EOF' 2>/dev/null
import math, sys
import rasterio
open_sea = (-2.45, 48.75)
shom = (-1.67, 43.40)
with rasterio.open(sys.argv[1]) as emodnet, rasterio.open(sys.argv[2]) as base, rasterio.open(sys.argv[3]) as combined:
    sea = float(next(emodnet.sample([open_sea]))[0])
    cut = float(next(combined.sample([open_sea]))[0])
    a = float(next(base.sample([shom]))[0])
    b = float(next(combined.sample([shom]))[0])
print(f"EMODnet open sea={sea:g}, selected={cut:g}; SHOM={a:g}, combined={b:g}")
sys.exit(0 if math.isfinite(sea) and sea < 0 and math.isnan(cut)
         and math.isfinite(a) and a == b else 1)
EOF
)
	if [ $? -eq 0 ]; then ok "france: EMODnet stays out of open sea and does not replace SHOM ($v)"
	else fail "france: invalid EMODnet extent or SHOM priority: $v"; fi
fi
if [ -f "$FRFILL/france_filled.vrt" ] && [ -f "$SRC/uk/uk_filled.vrt" ]; then
	v=$(python3 - "$SRC/uk/uk_filled.vrt" "$FRFILL/france_filled.vrt" <<'EOF' 2>/dev/null
import math, sys
import rasterio
pts = [(1.32, 51.13), (1.32, 51.10)]
with rasterio.open(sys.argv[1]) as uk, rasterio.open(sys.argv[2]) as france:
    values = [(float(next(uk.sample([pt]))[0]), float(next(france.sample([pt]))[0])) for pt in pts]
print("; ".join(f"UK={a:g}, France={b:g}" for a, b in values))
sys.exit(0 if all(math.isfinite(a) and math.isnan(b) for a, b in values) else 1)
EOF
)
	if [ $? -eq 0 ]; then ok "france: UK land and water at Dover are masked out ($v)"
	else fail "france: UK coverage is duplicated at Dover: $v"; fi
fi

echo "== uk"
FILES=(); while IFS= read -r f; do FILES+=("$f"); done < <(find "$SRC/uk/grid" -name '*.tif' 2>/dev/null | sort)
n=$(find "$SRC/uk/incoming" -mindepth 1 -maxdepth 1 \( -name '*.zip' -o -name '*.zip.part' \) 2>/dev/null | wc -l | tr -d ' ')
[ "$n" -eq 0 ] || warn "uk: $n zips left in incoming (conversion did not finish; rerun download_all.sh --only uk)"
if [ ${#FILES[@]} -eq 0 ]; then fail "uk: no surveys in src/uk/grid (downloaded by hand from seabed.admiralty.co.uk)"
else
	rasters_open uk "${FILES[@]}"
	# uk.vrt is the blended mosaic written by uk_build_vrt.py, not a VRT of all the tifs: never rebuild it here
	for v in "$SRC/uk/emodnet_fill.vrt" "$SRC/uk/emodnet_home.vrt" "$SRC/uk/uk.vrt" "$SRC/uk/uk_interp.vrt" "$SRC/uk/uk_filled.vrt" "$SRC/uk/uk_filled_interp.vrt"; do
		if [ ! -f "$v" ]; then fail "uk: $(basename "$v") not found (run download_all.sh --only uk)"
		elif gdalinfo "$v" >/dev/null 2>&1; then ok "uk: $(basename "$v") opens"
		else fail "uk: $(basename "$v") does not open"; fi
	done
	# the unblended list is what uk_build_vrt.py read; the blended VRT itself only needs the strips
	missing=$(while IFS= read -r f; do [ -f "$f" ] || echo "$f"; done < "$SRC/uk/uk_vrt_sources.txt")
	if [ -z "$missing" ]; then ok "uk: $(grep -c . "$SRC/uk/uk_vrt_sources.txt") sources of uk_vrt_sources.txt exist"
	else fail "uk: $(echo "$missing" | wc -l | tr -d ' ') sources of uk_vrt_sources.txt are missing"
		echo "$missing" | head -5 | sed 's/^/       /'; fi
	sea uk "$SRC/uk/uk.vrt" 0.3703 52.988 "North Sea"
	# the Dee gap was empty in uk.vrt; the coastal fill must have a shallow value there
	v=$(python3 - "$SRC/uk/uk_filled.vrt" <<'EOF' 2>/dev/null
import math, sys
import rasterio
with rasterio.open(sys.argv[1]) as ds:
    v = float(next(ds.sample([(-3.20, 53.38)]))[0])
print(f"{v:g}")
sys.exit(0 if math.isfinite(v) and v <= 0 else 1)
EOF
)
	if [ $? -eq 0 ]; then ok "uk: coastal fill at Dee gap (-3.20 53.38) is $v m"
	else fail "uk: coastal fill at Dee gap (-3.20 53.38) is '$v', expected a depth <= 0"; fi
	# the drying bank at the mouth of the Tay keeps its positive heights; they were clamped to 0 before
	v=$(python3 - "$SRC/uk/uk.vrt" "$SRC/uk/uk_filled.vrt" <<'EOF' 2>/dev/null
import sys
import numpy as np
import rasterio
from rasterio.windows import Window
lon, lat = -3.112839, 56.401750
with rasterio.open(sys.argv[1]) as base, rasterio.open(sys.argv[2]) as fill:
    x = int((lon - base.bounds.left) / base.res[0])
    y = int((base.bounds.top - lat) / base.res[1])
    w = Window(x - 4, y - 4, 9, 9)
    a = base.read(1, window=w)
    b = fill.read(1, window=w)
    both = np.isfinite(a) & np.isfinite(b)
    same = bool(np.all(a[both] == b[both]))
    bank = bool(np.isfinite(a).any() and np.nanmax(a) > -1.0)
print(f"bank={bank} same={same}")
sys.exit(0 if bank and same else 1)
EOF
)
	if [ $? -eq 0 ]; then ok "uk: drying bank at Tay (-3.112839 56.401750) stays a shallow bank"
	else fail "uk: drying bank at Tay (-3.112839 56.401750) is too deep or changed"; fi
	# the land of the neighbouring countries is not this map's coast: it stays empty, and the
	# coastal fill does not reach into their waters
	v=$(python3 - "$SRC/uk/uk_filled.vrt" <<'EOF' 2>/dev/null
import sys
import rasterio
pts = [(1.85, 50.95, "Calais"), (2.37, 51.03, "Dunkirk"), (1.32, 51.13, "Dover")]
with rasterio.open(sys.argv[1]) as ds:
    for lon, lat, name in pts:
        v = float(next(ds.sample([(lon, lat)]))[0])
        if name == "Dover":
            if v != 0:
                print(f"{name} {v}")
                sys.exit(1)
        elif v == v:  # not NaN
            print(f"{name} {v}")
            sys.exit(1)
EOF
)
	if [ $? -eq 0 ]; then ok "uk: the coast of France and Belgium stays out of the map"
	else fail "uk: foreign land or water in the map: $v"; fi
	# The EMODnet base of the UK map ends at the midpoint to the neighbouring land: the French side of
	# the Channel stays empty, the British side keeps its values.
	v=$(python3 - "$SRC/uk/emodnet_home.vrt" "$SRC/emodnet/emodnet_2024.vrt" "$SRC/uk/uk_filled.vrt" <<'EOF' 2>/dev/null
import math, sys
import rasterio
# French side of the Channel and of Normandy, British side of the Channel, the open sea and the North Sea.
pts = [(1.80, 50.98, "Calais NW", False), (1.60, 50.91, "Cap Gris-Nez N", False),
       (2.18, 51.08, "Dunkirk N", False), (-1.62, 49.75, "Cherbourg N", False),
       (0.30, 50.20, "Channel off Normandy", False),
       (1.32, 51.10, "Dover water", True), (-1.55, 50.60, "Lyme Bay", True),
       (0.37, 52.99, "North Sea", True), (-3.20, 50.20, "Start Point", True)]
with rasterio.open(sys.argv[1]) as home, rasterio.open(sys.argv[2]) as full, \
     rasterio.open(sys.argv[3]) as filled:
    for lon, lat, name, keep in pts:
        h = float(next(home.sample([(lon, lat)]))[0])
        f = float(next(full.sample([(lon, lat)]))[0])
        u = float(next(filled.sample([(lon, lat)]))[0])
        if keep and not (math.isfinite(h) and math.isfinite(f) and math.isfinite(u)):
            print(f"{name}: home={h:g} full={f:g} filled={u:g}")
            sys.exit(1)
        if not keep and (math.isfinite(h) or math.isfinite(u)):
            print(f"{name}: home={h:g} full={f:g} filled={u:g}")
            sys.exit(1)
print("ok")
EOF
)
	if [ $? -eq 0 ]; then ok "uk: the EMODnet base stops at the midpoint to the French land"
	else fail "uk: the EMODnet base crosses the midpoint to the French land: $v"; fi
fi

echo "== nz"
NZ="$SRC/nz/linz_hydro.gpkg"
if [ ! -f "$NZ" ]; then fail "nz: linz_hydro.gpkg not found (download_all.sh --only nz, needs LINZ_API_KEY)"
else
	n=$(ogrinfo -ro -q "$NZ" 2>/dev/null | grep -cE '^[0-9]+: (contour|sounding|area)_[1-5]')
	if [ "$n" -eq 15 ]; then ok "nz: linz_hydro.gpkg with 15 layers"; else fail "nz: linz_hydro.gpkg has $n of 15 layers"; fi
fi

echo "== denmark"
DK="$SRC/denmark/ddm_50m.dybde.tiff"
if [ ! -f "$DK" ]; then fail "denmark: ddm_50m.dybde.tiff not found"
else
	rasters_open denmark "$DK"
	c=$(crs "$DK"); if [ "$c" = "EPSG:3034" ]; then ok "denmark: $c"; else warn "denmark: $c, expected EPSG:3034"; fi
	# the model gives depths as positive numbers: the VRT turns them negative like every other grid
	if gdal_translate -q -of VRT -scale 0 1 0 -1 "$DK" "$SRC/denmark/denmark.vrt" 2>/dev/null; then
		ok "denmark: denmark.vrt with depths negative"
		sea denmark "$SRC/denmark/denmark.vrt" 11.0 56.3 "Kattegat"
	else fail "denmark: denmark.vrt could not be written"; fi
fi

echo
if [ $FAILED -eq 0 ]; then echo "All checks passed"; else echo "Some checks FAILED"; fi
exit $FAILED
