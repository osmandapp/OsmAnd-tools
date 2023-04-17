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
    if [ "$DATE_DIR" = "$RESULT_DIR/_diff" ]; then
        continue
    fi
    if [ ! -d $DATE_DIR/src ]; then
        continue;
    fi
    # folder for store _after.obf _before.obf _before_rel.obf _after_rel_m.obf
    mkdir -p $DATE_DIR/obf/
    COUNT_OBF_FILES=$(find $DATE_DIR/obf -type f -name "*.done" | wc -l)
    COUNT_OSM_FILES=$(find $DATE_DIR/src -type f -name "*_diff.osm.gz" | wc -l)
    if [ $COUNT_OSM_FILES -le $COUNT_OBF_FILES ]; then
        continue;
    fi
    echo "###  Process " $DATE_DIR $COUNT_OBF_FILES $COUNT_OSM_FILES
    for DIFF_FILE in $(ls $DATE_DIR/src/*_diff.osm.gz); do
        # cut _diff.osm.gz
        BASENAME=$(basename $DIFF_FILE);
        BASENAME=${BASENAME%_diff.osm.gz}
        PROC_FILE=$DATE_DIR/obf/${BASENAME}.done
        if [ ! -f $PROC_FILE ]; then
            echo "Process missing file ${PROC_FILE} $(date -u)"
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
                $DATE_DIR/src/${BASENAME}_before_rel.osm.gz $DATE_DIR/src/${BASENAME}_after_rel.osm.gz ${BASENAME}_after_rel_m.osm.gz
            

            echo "### 2. Generate obf files : $(date -u) . Will store into $DATE_DIR/obf/"
            $OSMAND_MAP_CREATOR_PATH/utilities.sh generate-obf-no-address $DATE_DIR/src/${BASENAME}_after.osm.gz  \
                --ram-process --add-region-tags --extra-relations="$LOW_EMMISION_ZONE_FILE" --upload $DATE_DIR/obf/ &
            $OSMAND_MAP_CREATOR_PATH/utilities.sh generate-obf-no-address $DATE_DIR/src/${BASENAME}_before.osm.gz  \
                --ram-process --add-region-tags --extra-relations="$LOW_EMMISION_ZONE_FILE" --upload $DATE_DIR/obf/ &
            $OSMAND_MAP_CREATOR_PATH/utilities.sh generate-obf-no-address $DATE_DIR/src/${BASENAME}_before_rel.osm.gz \
                --ram-process --add-region-tags --upload $DATE_DIR/obf/ &
            $OSMAND_MAP_CREATOR_PATH/utilities.sh generate-obf-no-address ${BASENAME}_after_rel_m.osm.gz \
                --ram-process --add-region-tags --upload $DATE_DIR/obf/ &
            wait
            
            echo "Complete file ${PROC_FILE} $(date -u)"
            # marked intermediate step was processed for counting
            touch ${PROC_FILE}

            rm -r *.osm.gz || true
            rm -r *.osm || true
            rm -r *.rtree* || true
            rm -r *.obf || true

            DATE_NAME=${BASENAME:0:8} #22_10_11
            TIME_NAME=${BASENAME:9:12} #20_30

            echo "20${DATE_NAME//_/-} ${TIME_NAME//_/:}" > $TIMESTAMP_FILE
        fi 
    done
done

