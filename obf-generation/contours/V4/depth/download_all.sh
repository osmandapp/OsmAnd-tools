#!/usr/bin/env bash
# Download the free depth sources and the land mask into one folder (resumable, size-checked).
#
#   download_all.sh -o DIR [--data] [--mask] [--only src1,src2] [-j JOBS] [--dry-run]
#
# --data  depth sources into DIR/src/<name>/:
#   gebco        GEBCO_2026 elevation, 8 GeoTIFF tiles, 15"                 ~4.2 GB zip
#   gebco_tid    GEBCO_2026 type identifier grid (measured vs interpolated) ~0.1 GB zip
#   emodnet      EMODnet DTM 2024, all 58 tiles, 1/16'                      ~11.4 GB zip
#   noaa_enc     NOAA ENC, all US charts, S-57                               ~0.8 GB zip
#   cudem        NOAA CUDEM 1/3" topobathy, US coast, 373 tiles              ~8.3 GB
#   (cudem_ninth NOAA CUDEM 1/9" topobathy, 930 tiles, ~187 GB: disabled until there is disk space)
#   norway       Kartverket "Sjøkart - Dybdedata", whole country, FGDB       ~2.3 GB zip
#   netherlands  Rijkswaterstaat bottom grids 20 m 2024, Zeeland, NCP 2019   ~0.3 GB
#   ireland      INFOMAR bathymetry 25 m (Irish waters) and 10 m (inshore), LAT, from the GSI ImageServer
# --mask  OSM land polygons (osmdata.openstreetmap.de, coastline only) into DIR/mask/  ~0.9 GB zip
# Without --data and --mask both are downloaded.
#
# Archives are unzipped as soon as they are complete and then deleted; an empty <archive>.done marker keeps
# a rerun from downloading them again. Every step can be rerun: finished files are skipped, broken ones resumed.
# Germany (BSH NAUTHIS) is not included: its WFS download service is disabled (checked 2026-09-16).
# Finland (Traficom depth WFS) is not included: its licence allows non-commercial, non-navigational use only.
# Denmark (Danmarks Dybdemodel 50 m) is not included: Dataforsyningen downloads need a login.
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
OUT=""; ONLY=""; JOBS=4; DRY=0; DATA=0; MASK=0
while [ $# -gt 0 ]; do
	case "$1" in
		-o) OUT=$2; shift 2 ;;
		--data) DATA=1; shift ;;
		--mask) MASK=1; shift ;;
		--only) ONLY=$2; DATA=1; shift 2 ;;
		-j) JOBS=$2; shift 2 ;;
		--dry-run) DRY=1; shift ;;
		-h|--help) sed -n '2,25p' "$0"; exit 0 ;;
		*) echo "Unknown option $1" >&2; exit 1 ;;
	esac
done
[ -n "$OUT" ] || { echo "Give the target folder: -o DIR" >&2; exit 1; }
if [ $DATA -eq 0 ] && [ $MASK -eq 0 ]; then DATA=1; MASK=1; fi
mkdir -p "$OUT"; OUT=$(cd "$OUT" && pwd)
SRC="$OUT/src"

want() { # want NAME -> true if this source is selected
	[ $DATA -eq 1 ] || return 1
	if [ -n "$ONLY" ]; then case ",$ONLY," in *",$1,"*) return 0 ;; *) return 1 ;; esac; fi
	[ "$1" != cudem_ninth ]
}
remote_size() { curl -sIL "$1" | awk 'tolower($1)=="content-length:"{v=$2} /^HTTP/{c=$2} END{gsub("\r","",v); print (c==200 ? v+0 : 0)}'; }
local_size() { if [ -f "$1" ]; then wc -c < "$1" | tr -d ' '; else echo 0; fi; }

# fetch URL FILE : resumable single-file download with size check
fetch() {
	local url=$1 file=$2 total have
	if [ -f "$file.done" ]; then echo "ok $(basename "$file") (unzipped earlier)"; return 0; fi
	total=$(remote_size "$url")
	if [ "$total" -le 0 ]; then echo "FAILED (no size) $url" >&2; return 1; fi
	for attempt in $(seq 1 50); do
		have=$(local_size "$file")
		if [ "$have" -eq "$total" ]; then echo "ok $(basename "$file") ($total bytes)"; return 0; fi
		if [ "$have" -gt "$total" ]; then rm -f "$file"; fi
		mkdir -p "$(dirname "$file")"
		curl -sL --retry 5 --retry-delay 5 -C - -o "$file" "$url" || sleep 5
	done
	echo "FAILED after retries $url" >&2; return 1
}

# unzip_rm ZIP DIR : unzip a complete archive, delete it and leave ZIP.done
unzip_rm() {
	local zip=$1 dir=$2
	if [ -f "$zip.done" ]; then return 0; fi
	mkdir -p "$dir"
	unzip -oq "$zip" -d "$dir"
	rm -f "$zip"; : > "$zip.done"
	echo "unzipped and removed $(basename "$zip")"
}
# arcgis_tile URL DIR NAME W S E N COLS ROWS : one tile of an ArcGIS ImageServer grid as a compressed GeoTIFF with
# nodata 0 (INFOMAR writes no data as 0). A preview at a quarter of the cells comes first: most tiles of the extent
# are open ocean or land, those are left as NAME.empty
arcgis_tile() {
	local url=$1 dir=$2 name=$3 w=$4 s=$5 e=$6 n=$7 cols=$8 rows=$9 get
	if [ -f "$dir/$name.tif" ] || [ -f "$dir/$name.empty" ]; then return 0; fi
	get="$url/exportImage?bbox=$w,$s,$e,$n&bboxSR=4326&imageSR=4326&format=tiff&pixelType=F32"
	get+="&interpolation=RSP_NearestNeighbor&f=image"
	curl -sfL --retry 5 --retry-delay 5 -o "$dir/$name.preview" "$get&size=$((cols / 4)),$((rows / 4))" \
		|| { echo "FAILED preview $name" >&2; return 1; }
	gdal_translate -q -a_nodata 0 "$dir/$name.preview" "$dir/$name.preview.tif" || { echo "FAILED preview $name" >&2; return 1; }
	# no statistics = no cell with data
	if ! gdalinfo -stats "$dir/$name.preview.tif" 2>/dev/null | grep -q STATISTICS_MAXIMUM; then
		rm -f "$dir/$name".preview*; : > "$dir/$name.empty"; return 0
	fi
	rm -f "$dir/$name".preview*
	curl -sfL --retry 5 --retry-delay 5 -o "$dir/$name.part" "$get&size=$cols,$rows" || { echo "FAILED $name" >&2; return 1; }
	gdal_translate -q -a_nodata 0 -co COMPRESS=DEFLATE -co PREDICTOR=3 -co TILED=YES "$dir/$name.part" "$dir/$name.tmp.tif" \
		&& mv "$dir/$name.tmp.tif" "$dir/$name.tif" && rm -f "$dir/$name.part" && echo "ok $name"
}

# arcgis_image URL DIR : the whole grid of an ArcGIS ImageServer at its own cell size, 4000 cells a tile
arcgis_image() {
	local url=$1 dir=$2
	mkdir -p "$dir"
	curl -sfL --retry 5 "$url?f=json" | python3 -c '
import json, math, sys
d = json.load(sys.stdin); e, px, n = d["extent"], d["pixelSizeX"], 4000
for i in range(math.ceil((e["xmax"] - e["xmin"]) / px / n)):
    for j in range(math.ceil((e["ymax"] - e["ymin"]) / px / n)):
        x0, y1 = e["xmin"] + i * n * px, e["ymax"] - j * n * px
        print("%03d_%03d %.12f %.12f %.12f %.12f %d %d" % (i, j, x0, y1 - n * px, x0 + n * px, y1, n, n))' \
	| xargs -P "$JOBS" -L 1 bash -c 'arcgis_tile "$0" "$@"' "$url" "$dir"
}
export -f fetch remote_size local_size unzip_rm arcgis_tile

report() { # dry run: print total size of URL list on stdin
	xargs -P 8 -I{} bash -c 'remote_size "$1"' _ {} | awk -v n="$1" '{s+=$1;c++} END{printf "%-12s %5d files %9.2f GB\n", n, c, s/1e9}'
}

if [ $MASK -eq 1 ]; then
	echo "== mask"
	U=https://osmdata.openstreetmap.de/download/land-polygons-complete-4326.zip
	if [ $DRY -eq 1 ]; then echo $U | report mask
	else
		# a fresh copy every run: the land polygons are rebuilt daily from the OSM coastline, so a partial file
		# from an earlier run may belong to another version
		rm -f "$OUT/mask/$(basename $U).done" "$OUT/mask/$(basename $U).tmp"
		fetch $U "$OUT/mask/$(basename $U).tmp"
		mv "$OUT/mask/$(basename $U).tmp" "$OUT/mask/$(basename $U)"
		rm -rf "$OUT/mask/land-polygons-complete-4326"
		unzip_rm "$OUT/mask/$(basename $U)" "$OUT/mask"
	fi
fi
if want gebco; then
	echo "== gebco"
	if [ $DRY -eq 1 ]; then echo "https://dap.ceda.ac.uk/bodc/gebco/global/gebco_2026/ice_surface_elevation/geotiff/gebco_2026_geotiff.zip?download=1" | report gebco
	else "$HERE/download_gebco.sh" -y 2026 -g elevation -f geotiff -o "$SRC/gebco" --unzip; fi
fi
if want gebco_tid; then
	echo "== gebco_tid"
	if [ $DRY -eq 1 ]; then echo "https://dap.ceda.ac.uk/bodc/gebco/global/gebco_2026/type_identifier_grid/geotiff/gebco_2026_tid_geotiff.zip?download=1" | report gebco_tid
	else "$HERE/download_gebco.sh" -y 2026 -g tid -f geotiff -o "$SRC/gebco_tid" --unzip; fi
fi
if want emodnet; then
	echo "== emodnet"
	if [ $DRY -eq 1 ]; then "$HERE/download_emodnet.sh" --list | awk 'NR>1{s+=$6;c++} END{printf "%-12s %5d files %9.2f GB\n","emodnet",c,s/1000}'
	else "$HERE/download_emodnet.sh" --all -j "$JOBS" -o "$SRC/emodnet" --unzip; fi
fi
if want noaa_enc; then
	echo "== noaa_enc"
	U=https://charts.noaa.gov/ENCs/All_ENCs.zip
	if [ $DRY -eq 1 ]; then echo $U | report noaa_enc
	else fetch $U "$SRC/noaa_enc/All_ENCs.zip"; unzip_rm "$SRC/noaa_enc/All_ENCs.zip" "$SRC/noaa_enc"; fi
fi
cudem() { # cudem NAME DATASET_DIR LIST_ID
	local name=$1 dir=$2 id=$3 list
	echo "== $name"
	mkdir -p "$SRC/$name"; list="$SRC/$name/urllist$id.txt"
	curl -sL --retry 5 -o "$list" "https://coast.noaa.gov/htdata/raster2/elevation/$dir/urllist$id.txt"
	if [ $DRY -eq 1 ]; then grep -E '\.tif$' "$list" | report "$name"; return; fi
	grep -E '\.tif$' "$list" | xargs -P "$JOBS" -I{} bash -c 'fetch "$1" "$2/$(basename "$1")"' _ {} "$SRC/$name/tiles"
}
if want cudem; then cudem cudem NCEI_third_Topobathy_2014_8580 8580; fi
# disabled until there is disk space for ~187 GB
# if want cudem_ninth; then cudem cudem_ninth NCEI_ninth_Topobathy_2014_8483 8483; fi
if want norway; then
	echo "== norway"
	U=https://nedlasting.geonorge.no/geonorge/Basisdata/Dybdedata/FGDB/Basisdata_0000_Norge_4258_Dybdedata_FGDB.zip
	if [ $DRY -eq 1 ]; then echo $U | report norway
	else fetch $U "$SRC/norway/$(basename $U)"; unzip_rm "$SRC/norway/$(basename $U)" "$SRC/norway"; fi
fi
if want netherlands; then
	echo "== netherlands"
	R=https://downloads.rijkswaterstaatdata.nl
	NL="$R/bodemhoogte_20mtr/bodemhoogte_20mtr_2024.tif $R/bodemhoogte_zeeland/bodemhoogte_zeeland.tif $R/bathymetrie_ncp/bathymetrie_ncp_juni_2019.tif"
	# NSGI hydroid (LAT) and NAP geoid of PROJ: their difference is how far chart datum lies below NAP
	NL="$NL https://cdn.proj.org/nl_nsgi_nllat2018.tif https://cdn.proj.org/nl_nsgi_nlgeo2018.tif"
	if [ $DRY -eq 1 ]; then printf "%s\n" $NL | report netherlands
	else for u in $NL; do fetch "$u" "$SRC/netherlands/$(basename "$u")"; done; fi
fi
if want ireland; then
	echo "== ireland"
	B=https://gsi.geodata.gov.ie/imagehost/rest/services/Marine
	if [ $DRY -eq 1 ]; then echo "ireland      ArcGIS ImageServer tiles, size known after the download"
	else
		arcgis_image "$B/IE_GSI_MI_Bathymetry_25m_IE_Waters_WGS84_LAT_GRID/ImageServer" "$SRC/ireland/25m"
		arcgis_image "$B/IE_GSI_MI_Bathymetry_10m_Inshore_IE_WGS84_LAT_GRID/ImageServer" "$SRC/ireland/10m"
	fi
fi
echo "Done: $OUT"
