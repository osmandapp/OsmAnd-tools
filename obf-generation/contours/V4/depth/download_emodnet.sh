#!/usr/bin/env bash
# Download EMODnet Digital Bathymetry (DTM) tiles (resumable, size-checked).
#
#   download_emodnet.sh [-v 2024|2022] [-f tif|nc|asc] [-j JOBS] [-o DIR] [--unzip] [--list] TILE... | --bbox W S E N | --all
#
# Examples:
#   download_emodnet.sh --list                          # tile names, bounds and zip sizes
#   download_emodnet.sh D5 D6 E6                        # North Sea, Baltic, Adriatic pilot tiles
#   download_emodnet.sh --bbox 3 51 9 56                # every tile touching the box
#   download_emodnet.sh --all -j 4                      # whole Europe (~11.4 GB zipped GeoTIFF for 2024)
# Tile bounds come from the EMODnet metadata catalogue (sextant.ifremer.fr), sizes from the download server.
set -euo pipefail

VER=2024; FMT=tif; JOBS=2; OUT=.; DO_UNZIP=0; MODE=tiles; LIST=0; BBOX=(); TILES=()
while [ $# -gt 0 ]; do
	case "$1" in
		-v) VER=$2; shift 2 ;;
		-f) FMT=$2; shift 2 ;;
		-j) JOBS=$2; shift 2 ;;
		-o) OUT=$2; shift 2 ;;
		--unzip) DO_UNZIP=1; shift ;;
		--list) LIST=1; shift ;;
		--all) MODE=all; shift ;;
		--bbox) MODE=bbox; BBOX=("$2" "$3" "$4" "$5"); shift 5 ;;
		-h|--help) sed -n '2,12p' "$0"; exit 0 ;;
		-*) echo "Unknown option $1" >&2; exit 1 ;;
		*) TILES+=("$1"); shift ;;
	esac
done

case "$VER" in
	2024) DIR=v12 ;;
	2022) DIR=v11 ;;
	*) echo "Unsupported version $VER (known: 2024, 2022)" >&2; exit 1 ;;
esac
SERVER="https://downloads.emodnet-bathymetry.eu/$DIR"

# name minLon maxLon minLat maxLat for every tile of this DTM release
tile_bounds() {
	curl -s -X POST "https://sextant.ifremer.fr/geonetwork/srv/api/search/records/_search" \
		-H "Content-Type: application/json" -H "Accept: application/json" \
		-d "{\"size\":200,\"_source\":[\"resourceTitleObject\",\"geom\"],\"query\":{\"query_string\":{\"query\":\"resourceTitleObject.default:\\\"DTM $VER\\\" AND resourceTitleObject.default:Tile\"}}}" |
	python3 -c '
import json, sys
for h in json.load(sys.stdin)["hits"]["hits"]:
    s = h["_source"]; title = s["resourceTitleObject"]["default"]
    if "Tile " not in title or not s.get("geom"): continue
    ring = s["geom"][0]["coordinates"][0]; xs = [p[0] for p in ring]; ys = [p[1] for p in ring]
    print(title.split("Tile ")[-1].strip(), min(xs), max(xs), min(ys), max(ys))
' | sort
}

remote_size() {
	curl -sIL "$SERVER/$1" | awk 'tolower($1)=="content-length:"{v=$2} /^HTTP/{code=$2} END{gsub("\r","",v); print (code==200 ? v+0 : 0)}'
}

if [ $LIST -eq 1 ]; then
	printf "%-4s %9s %9s %8s %8s %10s\n" tile minLon maxLon minLat maxLat MB
	tile_bounds | while read -r t x0 x1 y0 y1; do
		s=$(remote_size "${t}_${VER}.${FMT}.zip")
		printf "%-4s %9.3f %9.3f %8.3f %8.3f %10.1f\n" "$t" "$x0" "$x1" "$y0" "$y1" "$(echo "$s / 1000000" | bc -l)"
	done
	exit 0
fi

case "$MODE" in
	all)  while read -r t; do TILES+=("$t"); done < <(tile_bounds | awk '{print $1}') ;;
	bbox) while read -r t; do TILES+=("$t"); done < <(tile_bounds | awk -v W="${BBOX[0]}" -v S="${BBOX[1]}" -v E="${BBOX[2]}" -v N="${BBOX[3]}" \
			'$2<E && $3>W && $4<N && $5>S {print $1}') ;;
esac
if [ ${#TILES[@]} -eq 0 ]; then echo "No tiles selected (give TILE names, --bbox or --all; see --list)" >&2; exit 1; fi

mkdir -p "$OUT"; cd "$OUT"
echo "Tiles (${#TILES[@]}): ${TILES[*]}"

fetch() {
	local tile=$1 name="${1}_${VER}.${FMT}.zip" want have
	want=$(remote_size "$name")
	if [ "$want" -le 0 ]; then echo "MISSING $name on server" >&2; return 1; fi
	for attempt in $(seq 1 50); do
		have=$( [ -f "$name" ] && wc -c < "$name" | tr -d ' ' || echo 0 )
		if [ "$have" -ge "$want" ]; then
			echo "ok $name ($want bytes)"
			[ "$DO_UNZIP" -eq 1 ] && unzip -oq "$name" -d "${tile}_${VER}"
			return 0
		fi
		curl -sL --retry 5 --retry-delay 5 -C - -o "$name" "$SERVER/$name" || sleep 5
	done
	echo "FAILED $name after retries" >&2; return 1
}
export -f fetch remote_size
export SERVER VER FMT DO_UNZIP
printf "%s\n" "${TILES[@]}" | xargs -P "$JOBS" -I{} bash -c 'fetch "$1"' _ {}
