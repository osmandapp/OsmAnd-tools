#!/usr/bin/env bash
# Download a global GEBCO grid (resumable, parallel byte ranges, size-checked).
#
#   download_gebco.sh [-y YEAR] [-g elevation|tid|sub_ice] [-f geotiff|netcdf] [-j PARTS] [-o DIR] [--unzip]
#
# --unzip unzips the complete archive, deletes it and leaves an empty <archive>.done so a rerun skips it.
#
# Defaults: -y 2026 -g elevation -f geotiff -j 8 -o .
# The CEDA server drops long connections, so the file is fetched in PARTS ranges that are resumed until
# each one has its exact length; rerunning the script continues where it stopped.
set -euo pipefail

YEAR=2026; GRID=elevation; FMT=geotiff; PARTS=8; OUT=.; DO_UNZIP=0
while [ $# -gt 0 ]; do
	case "$1" in
		-y) YEAR=$2; shift 2 ;;
		-g) GRID=$2; shift 2 ;;
		-f) FMT=$2; shift 2 ;;
		-j) PARTS=$2; shift 2 ;;
		-o) OUT=$2; shift 2 ;;
		--unzip) DO_UNZIP=1; shift ;;
		-h|--help) sed -n '2,11p' "$0"; exit 0 ;;
		*) echo "Unknown option $1" >&2; exit 1 ;;
	esac
done

BASE="https://dap.ceda.ac.uk/bodc/gebco/global/gebco_${YEAR}"
case "$GRID/$FMT" in
	elevation/geotiff) PATH_="ice_surface_elevation/geotiff/gebco_${YEAR}_geotiff.zip" ;;
	elevation/netcdf)  PATH_="ice_surface_elevation/netcdf/GEBCO_${YEAR}.zip" ;;
	tid/geotiff)       PATH_="type_identifier_grid/geotiff/gebco_${YEAR}_tid_geotiff.zip" ;;
	tid/netcdf)        PATH_="type_identifier_grid/netcdf/gebco_${YEAR}_tid.zip" ;;
	sub_ice/geotiff)   PATH_="sub_ice_topography_bathymetry/geotiff/gebco_${YEAR}_sub_ice_topo_geotiff.zip" ;;
	sub_ice/netcdf)    PATH_="sub_ice_topography_bathymetry/netcdf/GEBCO_${YEAR}_sub_ice.zip" ;;
	*) echo "Unsupported grid/format: $GRID/$FMT" >&2; exit 1 ;;
esac
URL="$BASE/$PATH_?download=1"
NAME=$(basename "$PATH_")
mkdir -p "$OUT"; cd "$OUT"

size_of() { if [ -f "$1" ]; then wc -c < "$1" | tr -d ' '; else echo 0; fi; }

TOTAL=$(curl -sIL "$URL" | awk 'tolower($1)=="content-length:"{v=$2} END{gsub("\r","",v); print v+0}')
if [ "$TOTAL" -le 0 ]; then
	echo "Cannot get size of $URL (file layout of GEBCO_$YEAR may differ, check https://www.gebco.net)" >&2; exit 1
fi
if [ -f "$NAME.done" ]; then
	echo "$NAME already downloaded and unzipped"; exit 0
elif [ "$(size_of "$NAME")" = "$TOTAL" ]; then
	echo "$NAME already complete ($TOTAL bytes)"
else
	echo "Downloading $NAME: $TOTAL bytes in $PARTS parts"
	CHUNK=$(( TOTAL / PARTS + 1 ))
	fetch_part() {
		local i=$1 start=$(( $1 * CHUNK )) end=$(( ($1 + 1) * CHUNK - 1 ))
		[ $end -ge $TOTAL ] && end=$(( TOTAL - 1 ))
		local want=$(( end - start + 1 )) part="$NAME.part$i" have
		for attempt in $(seq 1 50); do
			have=$(size_of "$part")
			[ "$have" -ge "$want" ] && return 0
			curl -sL --retry 5 --retry-delay 5 -r $(( start + have ))-$end "$URL" >> "$part" || sleep 5
		done
		echo "Part $i incomplete after retries" >&2; return 1
	}
	pids=()
	for i in $(seq 0 $(( PARTS - 1 ))); do fetch_part "$i" & pids+=($!); done
	for p in "${pids[@]}"; do wait "$p"; done
	: > "$NAME.tmp"
	for i in $(seq 0 $(( PARTS - 1 ))); do cat "$NAME.part$i" >> "$NAME.tmp"; done
	if [ "$(size_of "$NAME.tmp")" != "$TOTAL" ]; then
		echo "Assembled size $(size_of "$NAME.tmp") != $TOTAL, rerun to resume" >&2; rm -f "$NAME.tmp"; exit 1
	fi
	mv "$NAME.tmp" "$NAME"; rm -f "$NAME".part*
	echo "Done: $NAME"
fi

if [ $DO_UNZIP -eq 1 ]; then
	unzip -oq "$NAME"
	rm -f "$NAME"; : > "$NAME.done"
	echo "Unzipped and removed $NAME"
fi
