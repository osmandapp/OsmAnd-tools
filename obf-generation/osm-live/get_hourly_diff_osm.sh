#!/bin/bash -xe
# GIT History could be found at https://github.com/osmandapp/OsmAnd-misc/blob/master/osm-live/generate_hourly_osmand_diff.sh

# For local test
# How to generate files locally:
# 1. Create .proc_timestamp in RESULT_DIR with date/time: "2020-12-30 10:10"
# 2. Execute this script
# 3. Stop when osm is done
#REMOTE_SSH_STRING="ssh jenkins@builder.osmand.net"

RESULT_DIR="/home/osmlive/"
# CURRENT_SEC=$(date -u "+%s")
START="$(cat $RESULT_DIR/.proc_timestamp)"
echo "Begin with timestamp: $START"
START_ARRAY=($START)
START_DAY=${START_ARRAY[0]}
START_TIME=${START_ARRAY[1]}
DATE_LOG_FILE=date.log
DB_DELAY=15

PERIOD_1_SEC=600; #300 disable 5 min 
PERIOD_2_SEC=600;
PERIOD_3_SEC=1200;
PERIOD_4_SEC=1800;
PERIOD_5_SEC=2400;

if [ -f "$DATE_LOG_FILE" ]; then 
  LOG_SIZE=$(wc -l <"$DATE_LOG_FILE") 
  if [ $LOG_SIZE -ge 1000  ]; then
    echo "START" > $DATE_LOG_FILE
  fi
fi

while true; do
  START_DATE="${START_DAY}T${START_TIME}:00Z"
  START_SEC=$(date -u --date="$START_DATE" "+%s")
  # database timestamp
  DB_SEC=$(date -u --date="$($REMOTE_SSH_STRING /home/overpass/osm3s/cgi-bin/timestamp | tail -1)" "+%s")

  if [ -z "$PERIOD" ]; then
    PERIOD_SEC=$PERIOD_1_SEC;
    if (( $DB_SEC > $START_SEC + $PERIOD_5_SEC + $DB_DELAY )); then
      PERIOD_SEC=$PERIOD_5_SEC;
    elif (( $DB_SEC > $START_SEC + $PERIOD_4_SEC + $DB_DELAY )); then
      PERIOD_SEC=$PERIOD_4_SEC;
    elif (( $DB_SEC > $START_SEC + $PERIOD_3_SEC + $DB_DELAY )); then
      PERIOD_SEC=$PERIOD_3_SEC;
    elif (( $DB_SEC > $START_SEC + $PERIOD_2_SEC + $DB_DELAY )); then
      PERIOD_SEC=$PERIOD_2_SEC;
    fi
  else 
    PERIOD_SEC=$PERIOD
  fi

  NEXT="$START_DAY $START_TIME $PERIOD_SEC seconds"

  NSTART_TIME=$(date +'%H' -d "$NEXT"):$(date +'%M' -d "$NEXT")
  NSTART_DAY=$(date +'%Y' -d "$NEXT")-$(date +'%m' -d "$NEXT")-$(date +'%d' -d "$NEXT")
  if [ ! "$NSTART_DAY" = "$START_DAY" ]; then
    NSTART_TIME="00:00"
  fi
  END_DATE="${NSTART_DAY}T${NSTART_TIME}:00Z"
  END_SEC=$(date -u --date="$END_DATE" "+%s")
  # give 60 seconds delay to wait for overpass to finish internal ops
  if (( $END_SEC > $DB_SEC - $DB_DELAY )); then
    echo "END date $END_DATE is in the future of database!!!"
    exit 0;
  fi;

  DATE_NAME="$(echo ${NSTART_DAY:2} | tr '-' _ | tr ':' _ )"
  TIME_NAME="$(echo ${NSTART_TIME} | tr '-' _ | tr ':' _ )"
  if [ "$TIME_NAME" = "00_00" ]; then
    DATE_NAME="$(echo ${START_DAY:2} | tr '-' _ | tr ':' _ )"
    TIME_NAME="24_00"
  fi
  FILENAME_START="${DATE_NAME}__${TIME_NAME}-$PERIOD_SEC-before"
  FILENAME_END="${DATE_NAME}__${TIME_NAME}-$PERIOD_SEC-after"
  FILENAME_CHANGE="${DATE_NAME}__${TIME_NAME}-$PERIOD_SEC-diff"
  FILENAME_START_REL="${DATE_NAME}__${TIME_NAME}-$PERIOD_SEC-before-rel"
  FILENAME_END_REL="${DATE_NAME}__${TIME_NAME}-$PERIOD_SEC-after-rel"
  FILENAME_DIFF="${DATE_NAME}_${TIME_NAME}"
  FINAL_FOLDER=$RESULT_DIR/_diff/$DATE_NAME/
  FINAL_FILE=$FINAL_FOLDER/$FILENAME_DIFF.obf.gz
  mkdir -p $FINAL_FOLDER/src/

  FULL_QUERY="
    // 1. get all nodes, ways, relation changed between START - END
    (
      node(changed:\"$START_DATE\",\"$END_DATE\");
      way(changed:\"$START_DATE\",\"$END_DATE\");
      relation(changed:\"$START_DATE\",\"$END_DATE\");
    )->.a;
    // 2.1 retrieve all nodes/ways for changed relation, 
    // so later we could retrieve all relations for Ways (not only changed)  and make ways complete
    // (way(r.a);.a;) ->.a; 
    // (node(r.a);.a;) ->.a; 
    // 2.2 retrieve all ways for changed nodes to change ways geometry
    (way(bn.a);.a;) ->.a; // get all ways by nodes
    // 2.3 retrieve all relations for changed nodes / ways, so we can propagate right tags to them
    (relation(bn.a);.a;) ->.a;
    (relation(bw.a);.a;) ->.a;
    // 3. final step make all relations / way / node complete
    (way(r.a);.a;) ->.a; 
    (node(r.a);.a;) ->.a;
    (node(w.a);.a;) ->.a;
    .a out meta;
  "

  FULL_QUERY_REL="
    // get all relation changed between START - END
    (
      relation(changed:\"$START_DATE\",\"$END_DATE\");
    )->.a;
    // 1. retrieve all members of changed relation (nodes/ways) to set .b    
    (way(r.a);) ->.b; 
    (node(r.a);.b;) ->.b;
    // 2. complete ways
    (node(w.b);.b;) ->.b;
    // 3. find incomplete relations for all members to set .c
    (relation(bw.b);) ->.c;
    (relation(bn.b);.c;) ->.c;

    // 3. print .b and .c, relations from set .a already in .c set
    .b out meta;
    .c out meta;
  "

  QUERY_START="[timeout:3600][maxsize:2000000000][date:\"$START_DATE\"]; $FULL_QUERY";
  QUERY_END="[timeout:3600][maxsize:2000000000][date:\"$END_DATE\"]; $FULL_QUERY";
  QUERY_DIFF="[timeout:3600][maxsize:2000000000][diff:\"$START_DATE\",\"$END_DATE\"];
    (
      node(changed:\"$START_DATE\",\"$END_DATE\");
      way(changed:\"$START_DATE\",\"$END_DATE\");
      relation(changed:\"$START_DATE\",\"$END_DATE\");
    )->.a;
    .a out geom meta;
  "
  QUERY_START_REL="[timeout:3600][maxsize:2000000000][date:\"$START_DATE\"]; $FULL_QUERY_REL";
  QUERY_END_REL="[timeout:3600][maxsize:2000000000][date:\"$END_DATE\"]; $FULL_QUERY_REL";

  #######################
  echo # 1. Query rich diffs - 2 in parallel
  START_T=$(date -u "+%H:%M")

  OVERPASS_STDERR=stderr.$$.tmp # stderr must be analyzed because osm3s_query might not drop error code
  ERROR_IN_BACKGROUND=failed.$$.tmp # "bash -e" does not drop error on "false & false & false & wait; echo oops"

  rm -f $ERROR_IN_BACKGROUND $OVERPASS_STDERR

  echo -e "$QUERY_START" | $REMOTE_SSH_STRING /home/overpass/osm3s/bin/osm3s_query >$FILENAME_START.osm 2>>$OVERPASS_STDERR || touch $ERROR_IN_BACKGROUND &
  echo -e "$QUERY_END" | $REMOTE_SSH_STRING /home/overpass/osm3s/bin/osm3s_query >$FILENAME_END.osm 2>>$OVERPASS_STDERR || touch $ERROR_IN_BACKGROUND &
  echo "$QUERY_DIFF" | $REMOTE_SSH_STRING /home/overpass/osm3s/bin/osm3s_query >$FILENAME_CHANGE.osm 2>>$OVERPASS_STDERR || touch $ERROR_IN_BACKGROUND &
  wait
  echo -e "$QUERY_START_REL" | $REMOTE_SSH_STRING /home/overpass/osm3s/bin/osm3s_query >$FILENAME_START_REL.osm 2>>$OVERPASS_STDERR || touch $ERROR_IN_BACKGROUND &
  echo -e "$QUERY_END_REL" | $REMOTE_SSH_STRING /home/overpass/osm3s/bin/osm3s_query >$FILENAME_END_REL.osm 2>>$OVERPASS_STDERR || touch $ERROR_IN_BACKGROUND &
  wait

  cat $OVERPASS_STDERR # verbose

  END_T=$(date -u "+%H:%M")
  echo "$START_DATE -> $END_DATE - $END_T ($START_T)" >> $DATE_LOG_FILE

  if grep "error:" $OVERPASS_STDERR || test -f $ERROR_IN_BACKGROUND; then
    echo "Fatal: osm3s_query failed in background job(s)"
    rm -f $ERROR_IN_BACKGROUND $OVERPASS_STDERR
    ls -la *.osm && rm -vf *.osm
    exit 1
  fi

  rm -f $OVERPASS_STDERR

  #######################

  # 1.1 checks
  if ! grep -q "<\/osm>"  $FILENAME_START.osm; then
    rm $FILENAME_START.osm;
    exit 1;
  fi
  if ! grep -q "<\/osm>"  $FILENAME_END.osm; then
    rm $FILENAME_END.osm;
    exit 1;
  fi
  if ! grep -q "<\/osm>"  $FILENAME_CHANGE.osm; then
    rm $FILENAME_CHANGE.osm;
    exit 1;
  fi
  if ! grep -q "<\/osm>"  $FILENAME_START_REL.osm; then
    rm $FILENAME_START_REL.osm;
    exit 1;
  fi
  if ! grep -q "<\/osm>"  $FILENAME_END_REL.osm; then
    rm $FILENAME_END_REL.osm;
    exit 1;
  fi

  rm -f $ERROR_IN_BACKGROUND

  gzip -c $FILENAME_START.osm  > $FINAL_FOLDER/src/${FILENAME_DIFF}_before.osm.gz || touch $ERROR_IN_BACKGROUND &
  gzip -c $FILENAME_END.osm    > $FINAL_FOLDER/src/${FILENAME_DIFF}_after.osm.gz || touch $ERROR_IN_BACKGROUND &
  gzip -c $FILENAME_CHANGE.osm > $FINAL_FOLDER/src/${FILENAME_DIFF}_diff.osm.gz || touch $ERROR_IN_BACKGROUND &
  gzip -c $FILENAME_START_REL.osm  > $FINAL_FOLDER/src/${FILENAME_DIFF}_before_rel.osm.gz || touch $ERROR_IN_BACKGROUND &
  gzip -c $FILENAME_END_REL.osm    > $FINAL_FOLDER/src/${FILENAME_DIFF}_after_rel.osm.gz || touch $ERROR_IN_BACKGROUND &
  wait;

  if [ -f $ERROR_IN_BACKGROUND ]; then
    echo "Fatal: gzip failed in background job(s)"
    ls -la $FINAL_FOLDER/src/${FILENAME_DIFF}/*.osm.gz && rm -vf $FINAL_FOLDER/src/${FILENAME_DIFF}/*.osm.gz
    ls -la *.osm && rm -vf *.osm
    rm -f $ERROR_IN_BACKGROUND
    exit 1
  fi

  TZ=UTC touch -c -d "$END_DATE" $FINAL_FOLDER/src/${FILENAME_DIFF}_before.osm.gz
  TZ=UTC touch -c -d "$END_DATE" $FINAL_FOLDER/src/${FILENAME_DIFF}_after.osm.gz
  TZ=UTC touch -c -d "$END_DATE" $FINAL_FOLDER/src/${FILENAME_DIFF}_diff.osm.gz
  TZ=UTC touch -c -d "$END_DATE" $FINAL_FOLDER/src/${FILENAME_DIFF}_before_rel.osm.gz
  TZ=UTC touch -c -d "$END_DATE" $FINAL_FOLDER/src/${FILENAME_DIFF}_after_rel.osm.gz

  rm *.osm
  # NEXT ITERATION
  START_DAY=$NSTART_DAY
  START_TIME=$NSTART_TIME

  echo "$NSTART_DAY $NSTART_TIME" > "${RESULT_DIR}.proc_timestamp"
done
