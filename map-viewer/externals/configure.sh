#!/bin/bash

if [ -z "$BASH_VERSION" ]; then
	exec bash "$0" "$@"
	exit $?
fi

SRCLOC="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Fail on any error
set -e

for external in $SRCLOC/* ; do
	if [ -d "$external" ]; then
		if [ -e "$external/configure.sh" ]; then
			$external/configure.sh
		fi
	fi
done