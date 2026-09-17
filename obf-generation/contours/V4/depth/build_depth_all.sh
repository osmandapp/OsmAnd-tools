#!/bin/bash
# Build several depth maps at the same time, each by build_depth_region.sh -D, with a log of its own.
#
#   build_depth_all.sh -D DATA_DIR -c MAP_CREATOR_DIR [-j JOBS] [-P PARALLEL] [-k] REGION...
#
#   -D DATA_DIR  folder of download_all.sh; logs go to DATA_DIR/build/logs/REGION.log
#   -c MAP_CREATOR_DIR  unzipped OsmAndMapCreator
#   -j JOBS      tiles built at a time inside every region (build_depth_region.sh -j), default 4
#   -P PARALLEL  regions built at a time, default all of them
#   -k           keep: a region whose OBF exists is skipped
#   REGION       names known to build_depth_region.sh, e.g. Netherlands_contours Europe_contours World_contours
#                World_Northern_hemisphere_points
#
# Regions do not depend on each other: Europe_no_detailed and World_no_eu_detailed are cut by coverages made from the
# sources (src/coverage, built once under a lock), not from the regional OBFs. Prints one line when a region starts
# and one when it ends; a failed region prints the end of its log. Exits with 1 when any region failed.
# Every region runs JOBS tiles and its map sections (one Java each, env JAVA_OPTS, -Xmx16g by default) at a time, so
# memory and cores grow with PARALLEL.
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
DATA=""; MAP_CREATOR=""; JOBS=4; PARALLEL=0; KEEP=""
while [ $# -gt 0 ]; do
	case "$1" in
		-D) DATA=$2; shift 2 ;;
		-c) MAP_CREATOR=$2; shift 2 ;;
		-j) JOBS=$2; shift 2 ;;
		-P) PARALLEL=$2; shift 2 ;;
		-k) KEEP=-k; shift ;;
		-h|--help) sed -n '2,19p' "$0"; exit 0 ;;
		-*) echo "Unknown option $1" >&2; exit 1 ;;
		*) break ;;
	esac
done
[ -n "$DATA" ] && [ -n "$MAP_CREATOR" ] && [ $# -gt 0 ] || { echo "Missing -D, -c or regions, see --help" >&2; exit 1; }
LOGS="$DATA/build/logs"; mkdir -p "$LOGS"
[ "$PARALLEL" -gt 0 ] || PARALLEL=$#

START=$(date +%s)
build() {
	local region=$1 start log=$LOGS/$1.log
	start=$(date +%s)
	echo "$(date +%H:%M) start $region"
	# shellcheck disable=SC2086
	if bash "$HERE/build_depth_region.sh" -D "$DATA" -n "$region" -c "$MAP_CREATOR" -j "$JOBS" $KEEP > "$log" 2>&1; then
		echo "$(date +%H:%M) done $region in $(( ($(date +%s) - start) / 60 )) min: $(grep '^== .*done' "$log" | tail -1 | sed 's/^== [0-9]*s //')"
	else
		echo "$(date +%H:%M) FAILED $region after $(( ($(date +%s) - start) / 60 )) min, $log:"
		tail -30 "$log" | sed 's/^/    /'
		return 1
	fi
}
export -f build; export HERE DATA MAP_CREATOR JOBS KEEP LOGS
status=0
printf '%s\n' "$@" | xargs -P "$PARALLEL" -n 1 bash -c 'build "$0"' || status=1
echo "all regions in $(( ($(date +%s) - START) / 60 )) min$([ $status -eq 0 ] || echo ', some FAILED')"
exit $status
