#!/bin/bash

#This file generates OBF files from Overpass osm file.
#Usage: Path_to_overpass Working_dir Path_to_Regions.ocbf

if [ -z "$JAVA_OPTS" ]; then
        JAVA_OPTS="-Xms64M -Xmx512M"
fi
java -Djava.util.logging.config.file=logging.properties $JAVA_OPTS -cp "./OSMLiveObfCreator.jar:./lib/OsmAnd-core.jar:./lib/*.jar:./lib-gl/*.jar" net.osmand.data.diff.OSMLiveObfCreator $@
