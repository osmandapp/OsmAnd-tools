#!/bin/bash
# Compare a generate-turn-lanes-test json with Valhalla, writes <json name>.csv beside the json.
#   ./compare.sh --json /path/to/test.json --valhalla http://localhost:8002 [--out test.csv] [--all]
# See compare.py for the columns and statuses.
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
exec python3 "$DIR/compare.py" "$@"
