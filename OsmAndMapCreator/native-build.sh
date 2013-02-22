#!/bin/bash
SCRIPT_LOC="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
LIB_LOC="$SCRIPT_LOC"/src/

cd "$SCRIPT_LOC"/../../

core/externals/configure.sh
core/qtbase-desktop/build.sh # neeeded?

if [ ! -d baked/amd64-linux-gcc-amd64-linux-gcc ]; then 
build/amd64-linux-gcc.sh
fi
(cd baked/amd64-linux-gcc-amd64-linux-gcc && make)
cp binaries/linux/amd64/libOsmAndCore.so "$LIB_LOC"/OsmAndCore-linux-amd64.lib

if [ ! -d baked/i686-linux-gcc-i686-linux-gcc ]; then 
build/i686-linux-gcc.sh 
fi
(cd baked/i686-linux-gcc-i686-linux-gcc && make)
cp binaries/linux/i686/libOsmAndCore.so "$LIB_LOC"/OsmAndCore-linux-x86.lib
