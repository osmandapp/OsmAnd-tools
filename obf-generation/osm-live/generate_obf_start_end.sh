#!/bin/bash -xe

RESULT_DIR="/home/osmlive"
OSMAND_MAP_CREATOR_PATH=OsmAndMapCreator
export JAVA_OPTS="-Xms512M -Xmx24014M"
chmod +x $OSMAND_MAP_CREATOR_PATH/utilities.sh
SRTM_DIR="/home/relief-data/srtm/"
# File to store processed timestamp as oneline: 2022-10-12 18:00
TIMESTAMP_FILE=/home/osmlive/.proc_start_end_obf_timestamp
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

    # folder for store _after.obf _before.obf _before_rel.obf _after_rel_m.obf
    mkdir -p $DATE_DIR/inter/

    COUNT_INTER_FILES=$(find $DATE_DIR/inter -type f -name "*_before_after_done.log" | wc -l)
    if [ ! -d $DATE_DIR/src ]; then
        continue;
    fi
    COUNT_OSM_FILES=$(find $DATE_DIR/src -type f  -name "*_diff.osm.gz" | wc -l)
    if [ $COUNT_OSM_FILES -le $COUNT_INTER_FILES ]; then
        continue;
    fi
    echo "###  Process " $DATE_DIR $COUNT_INTER_FILES $COUNT_OSM_FILES
    for DIFF_FILE in $(ls $DATE_DIR/src/*_diff.osm.gz); do
        # cut _diff.osm.gz
        BASENAME=$(basename $DIFF_FILE);
        BASENAME=${BASENAME%_diff.osm.gz}
        BEFORE_OBF_FILE=$DATE_DIR/inter/${BASENAME}_before.obf
        AFTER_OBF_FILE=$DATE_DIR/inter/${BASENAME}_after.obf
        BEFORE_REL_OBF_FILE=$DATE_DIR/inter/${BASENAME}_before_rel.obf
        AFTER_REL_M_OBF_FILE=$DATE_DIR/inter/${BASENAME}_after_rel_m.obf
        if [ ! -f $BEFORE_OBF_FILE ]; then
            echo "Process missing file $BEFORE_OBF_FILE $AFTER_OBF_FILE $BEFORE_REL_OBF_FILE $AFTER_REL_M_OBF_FILE"
            if [ ! -f $DATE_DIR/src/${BASENAME}_after.osm.gz ]; then
                echo "Missing file $DATE_DIR/src/${BASENAME}_after.osm.gz"
                exit 1;
            fi
            if [ ! -f $DATE_DIR/src/${BASENAME}_before.osm.gz ]; then
                echo "Missing file $DATE_DIR/src/${BASENAME}_before.osm.gz"
                exit 1;
            fi
            if [ ! -f $DATE_DIR/src/${BASENAME}_after_rel.osm.gz ]; then
                echo "Missing file $DATE_DIR/src/${BASENAME}_after_rel.osm.gz"
                exit 1;
            fi
            if [ ! -f $DATE_DIR/src/${BASENAME}_before_rel.osm.gz ]; then
                echo "Missing file $DATE_DIR/src/${BASENAME}_before_rel.osm.gz"
                exit 1;
            fi

            echo "### 1. Generate relation osm : $(date -u) . All nodes and ways copy from before_rel to after_rel " &
            $OSMAND_MAP_CREATOR_PATH/utilities.sh generate-relation-osm \
                $DATE_DIR/src/${BASENAME}_before_rel.osm.gz $DATE_DIR/src/${BASENAME}_after_rel.osm.gz $DATE_DIR/src/${BASENAME}_after_rel_m.osm.gz
            

            echo "### 2. Generate obf files : $(date -u) . Will store into $DATE_DIR/inter/"
            $OSMAND_MAP_CREATOR_PATH/utilities.sh generate-obf-no-address $DATE_DIR/src/${BASENAME}_after.osm.gz  \
                --ram-process --add-region-tags --extra-relations="$LOW_EMMISION_ZONE_FILE" --upload $DATE_DIR/inter/ &
            $OSMAND_MAP_CREATOR_PATH/utilities.sh generate-obf-no-address $DATE_DIR/src/${BASENAME}_before.osm.gz  \
                --ram-process --add-region-tags --extra-relations="$LOW_EMMISION_ZONE_FILE" --upload $DATE_DIR/inter/ &
            $OSMAND_MAP_CREATOR_PATH/utilities.sh generate-obf-no-address $DATE_DIR/src/${BASENAME}_before_rel.osm.gz \
                --ram-process --add-region-tags --upload $DATE_DIR/inter/ &
            $OSMAND_MAP_CREATOR_PATH/utilities.sh generate-obf-no-address $DATE_DIR/src/${BASENAME}_after_rel_m.osm.gz \
                --ram-process --add-region-tags --upload $DATE_DIR/inter/ &
            wait

            # marked intermediate step was processed for counting
            touch $DATE_DIR/inter/${BASENAME}_before_after_done.log

            rm $DATE_DIR/src/${BASENAME}_after_rel_m.osm.gz
            rm -r *.osm || true
            rm -r *.rtree* || true
            rm -r *.obf || true

            echo "20${DATE_NAME//_/-} ${TIME_NAME//_/:}" > $TIMESTAMP_FILE
        fi 
    done
done

