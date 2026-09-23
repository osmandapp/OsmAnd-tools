#!/bin/bash
# Turn lanes of OsmAnd against Valhalla on one region, end to end:
#   ./auto_test.sh --pbf /path/to/region.osm.pbf [--obf /path/to/region.obf] --name test_name
#                  [--junctions=1000] [--none-lanes] [--clear] [--port 8002]
#
#  1. OsmAndMapCreator/ beside tools/ (downloaded from the night build when missing)
#  2. the obf: --obf, or generated from the pbf into tmp/<name>.obf (reused next time)
#  3. tmp/<name>.json by generate-turn-lanes-test
#  4. Valhalla installed into valhalla/ beside tools/, tiles built from the pbf, served on --port
#  5. compare.sh -> tmp/<name>.csv
# MAPCREATOR_DIR=/path overrides the OsmAndMapCreator dir (e.g. a local build of a branch).
# --clear rebuilds the Valhalla tiles.
set -e

DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
TOOLS=$( cd "$DIR/../.." && pwd )
ROOT=$( dirname "$TOOLS" )
MC=${MAPCREATOR_DIR:-$ROOT/OsmAndMapCreator}
TMP="$ROOT/tmp"
VALHALLA_PY="$TOOLS/python/valhalla"
VALHALLA_DIR="$ROOT/valhalla"
MC_URL=http://download.osmand.net/latest-night-build/OsmAndMapCreator-main.zip

PBF= OBF= NAME= JUNCTIONS=1000 NONE_LANES= CLEAR= PORT=8002
while [ $# -gt 0 ]; do
	case "$1" in
		--pbf) PBF=$2; shift ;;
		--pbf=*) PBF=${1#*=} ;;
		--obf) OBF=$2; shift ;;
		--obf=*) OBF=${1#*=} ;;
		--name) NAME=$2; shift ;;
		--name=*) NAME=${1#*=} ;;
		--junctions) JUNCTIONS=$2; shift ;;
		--junctions=*) JUNCTIONS=${1#*=} ;;
		--port) PORT=$2; shift ;;
		--port=*) PORT=${1#*=} ;;
		--none-lanes) NONE_LANES=--none-lanes ;;
		--clear) CLEAR=1 ;;
		*) echo "Unknown argument $1"; exit 1 ;;
	esac
	shift
done
[ -f "$PBF" ] || { echo "--pbf /path/to/region.osm.pbf is required (Valhalla routes on it)"; exit 1; }
PBF=$( cd "$(dirname "$PBF")" && pwd )/$(basename "$PBF")
[ -n "$NAME" ] || NAME=$(basename "$PBF" | sed -e 's/\.osm\.pbf$//' -e 's/\.pbf$//')
mkdir -p "$TMP" "$MC"
MC=$( cd "$MC" && pwd )

# 1. map creator
if [ ! -f "$MC/utilities.sh" ]; then
	echo "Downloading $MC_URL"
	mkdir -p "$MC"
	if command -v wget >/dev/null; then
		wget -q -O "$MC/OsmAndMapCreator.zip" "$MC_URL"
	else
		curl -sfL -o "$MC/OsmAndMapCreator.zip" "$MC_URL"
	fi
	unzip -q -o "$MC/OsmAndMapCreator.zip" -d "$MC"
	rm "$MC/OsmAndMapCreator.zip"
fi
# the nightly build is made from master, which may not have the utility yet
if ! for j in "$MC"/*.jar "$MC"/lib/*.jar; do unzip -p "$j" net/osmand/MainUtilities.class 2>/dev/null; done \
		| LC_ALL=C grep -q generate-turn-lanes-test; then
	echo "$MC has no generate-turn-lanes-test, put a build with it there or point MAPCREATOR_DIR to one"
	exit 1
fi

# 2. obf
if [ -z "$OBF" ]; then
	OBF="$TMP/$NAME.obf"
	if [ ! -f "$OBF" ]; then
		echo "Generating $OBF from $(basename "$PBF")"
		WORK="$TMP/${NAME}_obf"
		rm -rf "$WORK" && mkdir -p "$WORK"
		(cd "$WORK" && bash "$MC/utilities.sh" generate-obf "$PBF")
		GENERATED=$(ls "$WORK"/*.obf 2>/dev/null | head -1)
		[ -n "$GENERATED" ] || { echo "generate-obf wrote no obf"; exit 1; }
		mv "$GENERATED" "$OBF"
		rm -rf "$WORK"
	fi
fi
[ -f "$OBF" ] || { echo "No such obf: $OBF"; exit 1; }
OBF=$( cd "$(dirname "$OBF")" && pwd )/$(basename "$OBF")

# 3. test json; the driving side is read from regions.ocbf beside the obf, or from one we know of
JSON="$TMP/$NAME.json"
REGIONS=
if [ ! -f "$(dirname "$OBF")/regions.ocbf" ]; then
	for r in "$TMP/regions.ocbf" "$ROOT/regions.ocbf" "$MC/regions.ocbf"; do
		[ -f "$r" ] && { REGIONS="--regions=$r"; break; }
	done
fi
echo "Generating $JSON"
(cd "$TMP" && bash "$MC/utilities.sh" generate-turn-lanes-test "$OBF" "$JSON" --junctions=$JUNCTIONS $NONE_LANES $REGIONS)

# 4. valhalla
if [ ! -x "$VALHALLA_DIR/venv/bin/python" ]; then
	python3 "$VALHALLA_PY/build_valhalla.py" --dir "$VALHALLA_DIR"
fi
LOG="$TMP/${NAME}_valhalla.log"
if curl -s -o /dev/null "http://localhost:$PORT/status"; then
	echo "Port $PORT is busy (another Valhalla?), stop it or pass --port"
	exit 1
fi
# own process group, so that valhalla_service, which start_server.py runs as its child, is stopped with it
set -m
python3 "$VALHALLA_PY/start_server.py" "$PBF" --dir "$VALHALLA_DIR" --port $PORT ${CLEAR:+--rebuild} > "$LOG" 2>&1 &
SERVER=$!
disown $SERVER
set +m
stop_server() {
	kill -- -$SERVER 2>/dev/null || return 0
	for i in 1 2 3 4 5 6 7 8 9 10; do
		kill -0 -- -$SERVER 2>/dev/null || return 0
		sleep 1
	done
	kill -9 -- -$SERVER 2>/dev/null || true
}
trap stop_server EXIT
echo "Starting Valhalla on :$PORT (log $LOG)"
until curl -s -o /dev/null "http://localhost:$PORT/status"; do
	kill -0 $SERVER 2>/dev/null || { cat "$LOG"; echo "Valhalla did not start"; exit 1; }
	sleep 2
done

# 5. compare
"$DIR/compare.sh" --json "$JSON" --valhalla "http://localhost:$PORT" --out "$TMP/$NAME.csv"
