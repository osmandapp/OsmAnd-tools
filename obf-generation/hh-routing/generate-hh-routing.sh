#!/bin/bash -e

# Since 2024, OsmAnd Team has deployed the Highway-Hierarchy routing algorithm.
# HH-routing is designed to calculate continent-scale routes incredibly quickly.
# HH-algorithm uses pre-generated shortcuts to reduce the roads graph drastically.

# Standard OsmAnd OBF maps includes HH-data for the following profiles: car, bicycle.
# Note: pre-generated HH-data might be incompatible with non-default routing parameters.

# This script may be used to enrich OBF-files with additional HH-data, e.g.:
# - to support more profiles (pedestrian)
# - to support more parameters (short_way, avoid_, allow_, prefer_, etc)
# - to add HH-data to custom maps (JOSM osm/pbf processed with OsmAndMapCreator)

# Step-by-step instructions:
# 1. Download and install OsmAndMapCreator, specify OSMAND_MAP_CREATOR_PATH
# 2. Specify OBF_DIRECTORY (it must start with lowercase [a-z] without spaces)
# 3. OBF file(s) names must match the mask: [A-Z]*_*_2.obf or [A-Z]*_*.road.obf
# 4. Tune JAVA_OPTS and THREADS to suit your machine
# 5. Place OBF file(s) into OBF_DIRECTORY 
# 6. Run script and pray for success :-)
# 7. Upload OBF(s) to OsmAnd and enjoy!

# ALL_PROFILES: comma-separated list of profiles to generate

# ALL_PARAMS: route parameters for each profile listed in ALL_PROFILES
# Use "@" to split profiles and "---" to split parameters of the profile.
# Parameters for profile might be empty or combined with empty.

# Default profiles/params in details:
# ALL_PROFILES="car,bicycle,pedestrian"
# ALL_PARAMS="@---height_obstacles@---height_obstacles"
# Three profiles will be generated:
# 1) car with empty parameters (default)
# 2) bicycle with empty AND "height_obstacles" parameters
# 3) pedestrian with empty AND "height_obstacles" parameters
# "height_obstacles" is "use elevation data" (OsmAnd default for bicycle/pedestrian)

### Please install OsmAndMapCreator ###
OSMAND_MAP_CREATOR_PATH=/tmp/OsmAndMapCreator
export JAVA_OPTS="-Xmx32g"
THREADS=16

### OBF-file(s) must match [A-Z]*_*_2.obf or [A-Z]*_*.road.obf ###
OBF_DIRECTORY=/tmp/obf-files # no spaces, starts lowercase
PREFIX=$(basename $OBF_DIRECTORY)
MATCH_1="[A-Z]*_*road.obf"
MATCH_2="[A-Z]*_*_2.obf"

# Steps #
CLEAN=true
BUILD_NETWORK_POINTS=true
BUILD_NETWORK_SHORTCUTS=true
AUGMENT_OBF_WITH_HH_DATA=true
VERIFY=true

### HH profiles ###
ALL_PROFILES="car,bicycle,pedestrian" # 1:car 2:bicycle 3:pedestrian
ALL_PARAMS="@---height_obstacles@---height_obstacles" # 1:car(empty) @ 2:bicycle(empty,height_obstacles) @ 3:pedestrian(empty,height_obstacles)

### Validate OBF(s) ###
cd $OBF_DIRECTORY || exit
ls -1 $OBF_DIRECTORY/$MATCH_1 $OBF_DIRECTORY/$MATCH_2 2>/dev/null | grep obf || exit

### Run CLEAN ###
if [ "$CLEAN" = "true" ]; then
  rm -vf $OBF_DIRECTORY/${PREFIX}_*.{obf,osm,hhdb,chdb}
  ARGS="$ARGS --clean"
fi

### Run BUILD_NETWORK_POINTS ###
if [ "$BUILD_NETWORK_POINTS" = "true" ]; then
  IFS=","
  for PROFILE in $ALL_PROFILES; do
    echo BUILD_NETWORK_POINTS $PROFILE
    $OSMAND_MAP_CREATOR_PATH/utilities.sh hh-routing-prepare "$OBF_DIRECTORY" --routing_profile="$PROFILE" --threads=$THREADS $ARGS &
  done
  wait # until all BUILD_NETWORK_POINTS finished
  IFS=" "
fi

### Run BUILD_NETWORK_SHORTCUTS ###
if [ "$BUILD_NETWORK_SHORTCUTS" = "true" ]; then
  echo BUILD_NETWORK_SHORTCUTS $ALL_PROFILES $ALL_PARAMS
  $OSMAND_MAP_CREATOR_PATH/utilities.sh hh-routing-shortcuts "$OBF_DIRECTORY" --routing_profile="$ALL_PROFILES" --routing_params="$ALL_PARAMS" --threads=$THREADS $ARGS
fi

### Run AUGMENT_OBF_WITH_HH_DATA ###
if [ "$AUGMENT_OBF_WITH_HH_DATA" = "true" ]; then
  IFS=","
  for PROFILE in $ALL_PROFILES; do
    DB="${PREFIX}_${PROFILE}.chdb"
    $OSMAND_MAP_CREATOR_PATH/utilities.sh hh-routing-obf-write --db="$DB" --obf="$OBF_DIRECTORY" --update-existing-files --threads=$THREADS
  done
  IFS=" "
fi

### Run VERIFY ###
if [ "$VERIFY" = "true" ]; then
  for OBF in $OBF_DIRECTORY/$MATCH_1 $OBF_DIRECTORY/$MATCH_2; do
    test -f "$OBF" || continue
    $OSMAND_MAP_CREATOR_PATH/inspector.sh -vhhrouting $OBF | grep -A1 "Highway routing" && continue
    echo
    echo "$OBF generation failed (has no HH section)"
    exit 1
  done
fi

echo
echo success
echo

