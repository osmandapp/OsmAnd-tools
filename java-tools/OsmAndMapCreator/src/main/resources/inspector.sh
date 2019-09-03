#!/bin/bash
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
if [ -z "$JAVA_OPTS" ]; then 
	JAVA_OPTS="-Xms64M -Xmx512M"
fi
java -Djava.util.logging.config.file="$DIR/logging.properties" $JAVA_OPTS -cp "$DIR/OsmAndMapCreator.jar:$DIR/lib/*.jar" net.osmand.obf.BinaryInspector $@
