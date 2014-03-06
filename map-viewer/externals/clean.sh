#!/bin/bash

if [ -z "$BASH_VERSION" ]; then
	exec bash "$0" "$@"
	exit $?
fi

SRCLOC="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

for external in $SRCLOC/* ; do
	if ls -1 $external/upstream.* >/dev/null 2>&1
	then
		echo "Cleaning '"$(basename "$external")"'..."
		rm -rf $external/upstream.*
	fi
done