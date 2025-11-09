#!/bin/bash
set -eu -o pipefail

script_dir=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
java_tools_dir="${script_dir}/../../java-tools"
utils_dir="${script_dir}/OsmAndMapCreator"

rm -rf "${utils_dir}"
pushd "${java_tools_dir}"
./gradlew buildDistribution
popd
mkdir "${utils_dir}"
pushd "${utils_dir}"
unzip "${java_tools_dir}/OsmAndMapCreator/build/distributions/OsmAndMapCreator.zip"
popd
