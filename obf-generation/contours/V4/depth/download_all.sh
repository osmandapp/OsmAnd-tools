#!/usr/bin/env bash
# Download every free depth source used for depth contours/points into one folder (resumable, size-checked).
#
#   download_all.sh -o DIR [--only src1,src2] [--unzip] [--cudem-ninth] [-j JOBS] [--dry-run]
#
# Sources (DIR/<name>/):
#   gebco        GEBCO_2026 elevation, 8 GeoTIFF tiles, 15"                 ~4.2 GB zip
#   gebco_tid    GEBCO_2026 type identifier grid (measured vs interpolated) ~0.1 GB zip
#   emodnet      EMODnet DTM 2024, all 58 tiles, 1/16'                      ~11.4 GB zip
#   noaa_enc     NOAA ENC, all US charts, S-57                               ~0.8 GB zip
#   cudem        NOAA CUDEM 1/3" topobathy, US coast, 373 tiles              ~8.3 GB
#   cudem_ninth  NOAA CUDEM 1/9" topobathy, 930 tiles (only with --cudem-ninth)  ~187 GB
#   norway       Kartverket "Sjøkart - Dybdedata", whole country, FGDB       ~2.3 GB zip
#   netherlands  Rijkswaterstaat bottom grids 20 m 2024, Zeeland, NCP 2019   ~0.3 GB
# Germany (BSH NAUTHIS) is not included: its WFS download service is disabled (checked 2026-09-16).
# Every step can be rerun; finished files are skipped, broken ones resumed.
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
OUT=""; ONLY=""; UNZIP_OPT=""; NINTH=0; JOBS=4; DRY=0
while [ $# -gt 0 ]; do
	case "$1" in
		-o) OUT=$2; shift 2 ;;
		--only) ONLY=$2; shift 2 ;;
		--unzip) UNZIP_OPT="--unzip"; shift ;;
		--cudem-ninth) NINTH=1; shift ;;
		-j) JOBS=$2; shift 2 ;;
		--dry-run) DRY=1; shift ;;
		-h|--help) sed -n '2,20p' "$0"; exit 0 ;;
		*) echo "Unknown option $1" >&2; exit 1 ;;
	esac
done
[ -n "$OUT" ] || { echo "Give the target folder: -o DIR" >&2; exit 1; }
mkdir -p "$OUT"; OUT=$(cd "$OUT" && pwd)

want() { # want NAME -> true if this source is selected
	if [ -n "$ONLY" ]; then case ",$ONLY," in *",$1,"*) return 0 ;; *) return 1 ;; esac; fi
	[ "$1" != cudem_ninth ] || [ $NINTH -eq 1 ]
}
remote_size() { curl -sIL "$1" | awk 'tolower($1)=="content-length:"{v=$2} /^HTTP/{c=$2} END{gsub("\r","",v); print (c==200 ? v+0 : 0)}'; }
local_size() { if [ -f "$1" ]; then wc -c < "$1" | tr -d ' '; else echo 0; fi; }

# fetch URL FILE : resumable single-file download with size check
fetch() {
	local url=$1 file=$2 total have
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
export -f fetch remote_size local_size

unzip_into() { [ -n "$UNZIP_OPT" ] && unzip -oq "$1" -d "$2" && echo "unzipped $(basename "$1")" || true; }

report() { # dry run: print total size of URL list on stdin
	xargs -P 8 -I{} bash -c 'remote_size "$1"' _ {} | awk -v n="$1" '{s+=$1;c++} END{printf "%-12s %5d files %9.2f GB\n", n, c, s/1e9}'
}

if want gebco; then
	echo "== gebco"
	if [ $DRY -eq 1 ]; then echo "https://dap.ceda.ac.uk/bodc/gebco/global/gebco_2026/ice_surface_elevation/geotiff/gebco_2026_geotiff.zip?download=1" | report gebco
	else "$HERE/download_gebco.sh" -y 2026 -g elevation -f geotiff -o "$OUT/gebco" $UNZIP_OPT; fi
fi
if want gebco_tid; then
	echo "== gebco_tid"
	if [ $DRY -eq 1 ]; then echo "https://dap.ceda.ac.uk/bodc/gebco/global/gebco_2026/type_identifier_grid/geotiff/gebco_2026_tid_geotiff.zip?download=1" | report gebco_tid
	else "$HERE/download_gebco.sh" -y 2026 -g tid -f geotiff -o "$OUT/gebco_tid" $UNZIP_OPT; fi
fi
if want emodnet; then
	echo "== emodnet"
	if [ $DRY -eq 1 ]; then "$HERE/download_emodnet.sh" --list | awk 'NR>1{s+=$6;c++} END{printf "%-12s %5d files %9.2f GB\n","emodnet",c,s/1000}'
	else "$HERE/download_emodnet.sh" --all -j "$JOBS" -o "$OUT/emodnet" $UNZIP_OPT; fi
fi
if want noaa_enc; then
	echo "== noaa_enc"
	U=https://charts.noaa.gov/ENCs/All_ENCs.zip
	if [ $DRY -eq 1 ]; then echo $U | report noaa_enc
	else fetch $U "$OUT/noaa_enc/All_ENCs.zip" && unzip_into "$OUT/noaa_enc/All_ENCs.zip" "$OUT/noaa_enc"; fi
fi
cudem() { # cudem NAME DATASET_DIR LIST_ID
	local name=$1 dir=$2 id=$3 list
	echo "== $name"
	mkdir -p "$OUT/$name"; list="$OUT/$name/urllist$id.txt"
	curl -sL --retry 5 -o "$list" "https://coast.noaa.gov/htdata/raster2/elevation/$dir/urllist$id.txt"
	if [ $DRY -eq 1 ]; then grep -E '\.tif$' "$list" | report "$name"; return; fi
	grep -E '\.tif$' "$list" | xargs -P "$JOBS" -I{} bash -c 'fetch "$1" "$2/$(basename "$1")"' _ {} "$OUT/$name/tiles"
}
if want cudem; then cudem cudem NCEI_third_Topobathy_2014_8580 8580; fi
if want cudem_ninth; then cudem cudem_ninth NCEI_ninth_Topobathy_2014_8483 8483; fi
if want norway; then
	echo "== norway"
	U=https://nedlasting.geonorge.no/geonorge/Basisdata/Dybdedata/FGDB/Basisdata_0000_Norge_4258_Dybdedata_FGDB.zip
	if [ $DRY -eq 1 ]; then echo $U | report norway
	else fetch $U "$OUT/norway/$(basename $U)" && unzip_into "$OUT/norway/$(basename $U)" "$OUT/norway"; fi
fi
if want netherlands; then
	echo "== netherlands"
	R=https://downloads.rijkswaterstaatdata.nl
	NL="$R/bodemhoogte_20mtr/bodemhoogte_20mtr_2024.tif $R/bodemhoogte_zeeland/bodemhoogte_zeeland.tif $R/bathymetrie_ncp/bathymetrie_ncp_juni_2019.tif"
	if [ $DRY -eq 1 ]; then printf "%s\n" $NL | report netherlands
	else for u in $NL; do fetch "$u" "$OUT/netherlands/$(basename "$u")"; done; fi
fi
echo "Done: $OUT"
