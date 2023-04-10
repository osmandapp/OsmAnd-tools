#!/bin/bash
# GIT History could be found at https://github.com/osmandapp/OsmAnd-misc/blob/master/osm-live/generate_hourly_osmand_diff.sh

RESULT_DIR="/home/osmlive"
OSMAND_MAP_CREATOR_PATH=OsmAndMapCreator
export JAVA_OPTS="-Xms512M -Xmx24014M"
chmod +x $OSMAND_MAP_CREATOR_PATH/utilities.sh
SRTM_DIR="/home/relief-data/srtm/"
# File to store processed timestamp as oneline: 2022-10-12 18:00
TIMESTAMP_FILE=/home/osmlive/.proc_diff_timestamp
# TIMESTAMP_FILE=.timestamp

QUERY_LOW_EMMISIONS_ZONE="[timeout:3600][maxsize:160000000];
    relation[\"boundary\"=\"low_emission_zone\"];
    (._;>;);
    out meta;"
LOW_EMMISION_ZONE_FILE=low_emission_zone.osm.gz
## UPDATE LOW_EMMISION_ZONE_FILE once per day 
if ! test "`find $LOW_EMMISION_ZONE_FILE -mmin -1440`"; then 
    echo "$QUERY_LOW_EMMISIONS_ZONE" | $REMOTE_SSH_STRING /home/overpass/osm3s/bin/osm3s_query  | gzip > $LOW_EMMISION_ZONE_FILE
fi

for DATE_DIR in $(find $RESULT_DIR/_diff -maxdepth 1  -type d | sort ); do
    COUNT_OBF_FILES=$(find $DATE_DIR -maxdepth 1 -type f  -name "*.obf.gz" | wc -l)
    if [ ! -d $DATE_DIR/src ]; then
        continue;
    fi
    COUNT_OSM_FILES=$(find $DATE_DIR/src -type f  -name "*_diff.osm.gz" | wc -l)
    if [ $COUNT_OSM_FILES -le $COUNT_OBF_FILES ]; then
        continue;
    fi

    echo "###  Process " $DATE_DIR $COUNT_OBF_FILES $COUNT_OSM_FILES
    for DIFF_FILE in $(ls $DATE_DIR/src/*_diff.osm.gz); do
        # cut _diff.osm.gz
        BASENAME=$(basename $DIFF_FILE);
        BASENAME=${BASENAME%_diff.osm.gz}
        OBF_FILE=$DATE_DIR/${BASENAME}.obf.gz
        if [ ! -f $OBF_FILE ]; then
            echo "Process missing file $OBF_FILE"
            if [ ! -f $DATE_DIR/src/${BASENAME}_after.osm.gz ]; then
                echo "Missing file $DATE_DIR/src/${BASENAME}_after.osm.gz"
                exit 1;
            fi
            if [ ! -f $DATE_DIR/src/${BASENAME}_before.osm.gz ]; then
                echo "Missing file $DATE_DIR/src/${BASENAME}_before.osm.gz"
                exit 1;
            fi

            echo "### 1. Generate obf files : $(date -u)"
            # SRTM takes too much time and memory at this step (probably it could be used at the change step)
            $OSMAND_MAP_CREATOR_PATH/utilities.sh generate-obf-no-address $DATE_DIR/src/${BASENAME}_after.osm.gz  \
                --ram-process --add-region-tags --extra-relations="$LOW_EMMISION_ZONE_FILE" & # --srtm="$SRTM_DIR" &
            $OSMAND_MAP_CREATOR_PATH/utilities.sh generate-obf-no-address $DATE_DIR/src/${BASENAME}_before.osm.gz  \
                --ram-process --add-region-tags --extra-relations="$LOW_EMMISION_ZONE_FILE" & # --srtm="$SRTM_DIR" &
            wait

            echo "### 2. Generate diff files : $(date -u)"
            $OSMAND_MAP_CREATOR_PATH/utilities.sh generate-obf-diff \
                ${BASENAME}_before.obf ${BASENAME}_after.obf ${BASENAME}_diff.obf $DIFF_FILE &
            wait

            echo "### 3. Split files : $(date -u)"
            DATE_NAME=${BASENAME:0:8} #22_10_11
            TIME_NAME=${BASENAME:9:12} #20_30
            $OSMAND_MAP_CREATOR_PATH/utilities.sh split-obf ${BASENAME}_diff.obf $RESULT_DIR  "$DATE_NAME" "_$TIME_NAME" --srtm="$SRTM_DIR"
            
            gzip -c ${BASENAME}_diff.obf > $DATE_DIR/${BASENAME}.obf.gz
            touch -r $DIFF_FILE $DATE_DIR/${BASENAME}.obf.gz

            rm -r *.osm || true
            rm -r *.rtree* || true
            rm -r *.obf || true

            # 2022-10-12 18:00
            echo "20${DATE_NAME//_/-} ${TIME_NAME//_/:}" > $TIMESTAMP_FILE
        fi 
    done
done

