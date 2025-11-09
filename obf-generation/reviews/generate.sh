#!/bin/bash
set -eu -o pipefail

utils_dir=OsmAndMapCreator
work_dir=$(pwd)
mangrove_file="${work_dir}/mangrove-$(date -u --iso-8601 date).json"

./download_mangrove.py "${mangrove_file}"

${utils_dir}/utilities.sh generate-reviews-obf --input=${mangrove_file} --dir=${work_dir}
