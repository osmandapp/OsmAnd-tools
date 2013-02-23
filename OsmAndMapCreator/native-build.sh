#!/bin/bash
SCRIPT_LOC="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
LIB_LOC="$SCRIPT_LOC/src"
ROOT_LOC="$SCRIPT_LOC/../.."

"$ROOT_LOC/core/externals/configure.sh"
"$ROOT_LOC/core/externals/qtbase-desktop/build.sh"

if [ ! -d "$ROOT_LOC/baked/amd64-linux-gcc-amd64-linux-gcc" ]; then 
	"$ROOT_LOC/build/amd64-linux-gcc.sh"
fi
(cd "$ROOT_LOC/baked/amd64-linux-gcc-amd64-linux-gcc" && make -j`nproc` OsmAndCore)
cp "$ROOT_LOC/binaries/linux/amd64/libOsmAndCore.so" "$LIB_LOC/OsmAndCore-linux-amd64.lib"

if [ ! -d "$ROOT_LOC/baked/i686-linux-gcc-i686-linux-gcc" ]; then 
	"$ROOT_LOC/build/i686-linux-gcc.sh"
fi
(cd "$ROOT_LOC/baked/i686-linux-gcc-i686-linux-gcc" && make -j`nproc` OsmAndCore)
cp "$ROOT_LOC/binaries/linux/i686/libOsmAndCore.so" "$LIB_LOC/OsmAndCore-linux-x86.lib"
