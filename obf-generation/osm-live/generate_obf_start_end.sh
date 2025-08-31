#!/bin/bash -e

RESULT_DIR="/home/osmlive"
OSMAND_MAP_CREATOR_PATH=OsmAndMapCreator
export JAVA_OPTS="-Xms512M -Xmx24014M"
chmod +x $OSMAND_MAP_CREATOR_PATH/utilities.sh
SRTM_DIR="/home/relief-data/srtm/"
QUERY_LOW_EMMISIONS_ZONE="[timeout:3600][maxsize:160000000];
    relation[\"boundary\"=\"low_emission_zone\"];
    (._;>;);
    out meta;"
LOW_EMMISION_ZONE_FILE=lez/low_emission_zone.osm.gz
mkdir -p lez
## UPDATE LOW_EMMISION_ZONE_FILE once per day 
if ! test "`find $LOW_EMMISION_ZONE_FILE -mmin -1440`"; then 
    echo "$QUERY_LOW_EMMISIONS_ZONE" | $REMOTE_SSH_STRING /home/overpass/osm3s/bin/osm3s_query  | gzip > $LOW_EMMISION_ZONE_FILE
fi
for DATE_DIR in $(find $RESULT_DIR/_diff -maxdepth 1  -type d | sort ); do
    if [ ! -d $DATE_DIR/src ]; then
        continue;
    fi
    if [ "$DATE_DIR" = "$RESULT_DIR/_diff" ]; then
        continue
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
        if [ -f $PROC_FILE ] || [ -f $DATE_DIR/${BASENAME}.obf.gz ]; then
            continue;
        fi

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
        if [ ! -f $DATE_DIR/src/${BASENAME}_diff.osm.gz ]; then
            echo "Missing file $DATE_DIR/src/${BASENAME}_diff.osm.gz"
            exit 1;
        fi

        echo "### 1. Generate relation osm : $(date -u) . All nodes and ways copy from before_rel to after_rel " &
        $OSMAND_MAP_CREATOR_PATH/utilities.sh generate-relation-osm \
            $DATE_DIR/src/${BASENAME}_before_rel.osm.gz $DATE_DIR/src/${BASENAME}_after_rel.osm.gz \
            $DATE_DIR/src/${BASENAME}_diff.osm.gz $DATE_DIR/src/${BASENAME}_after.osm.gz ${BASENAME}_after_rel_m.osm.gz

        echo "### 2. Generate obf files : $(date -u) . Will store into $DATE_DIR/obf/"
        $OSMAND_MAP_CREATOR_PATH/utilities.sh generate-obf-no-address $DATE_DIR/src/${BASENAME}_after.osm.gz  \
            --ram-process --add-region-tags --extra-relations="$LOW_EMMISION_ZONE_FILE" --upload $DATE_DIR/obf/ &
        $OSMAND_MAP_CREATOR_PATH/utilities.sh generate-obf-no-address $DATE_DIR/src/${BASENAME}_before.osm.gz  \
            --ram-process --add-region-tags --extra-relations="$LOW_EMMISION_ZONE_FILE" --upload $DATE_DIR/obf/ &
        $OSMAND_MAP_CREATOR_PATH/utilities.sh generate-obf-no-address-no-multipolygon $DATE_DIR/src/${BASENAME}_before_rel.osm.gz \
            --ram-process --add-region-tags --upload $DATE_DIR/obf/ &
        $OSMAND_MAP_CREATOR_PATH/utilities.sh generate-obf-no-address-no-multipolygon ${BASENAME}_after_rel_m.osm.gz \
            --ram-process --add-region-tags --upload $DATE_DIR/obf/ &
        wait            

        
        AFTER_OBF=$DATE_DIR/obf/${BASENAME}_after.obf
        BEFORE_OBF=$DATE_DIR/obf/${BASENAME}_before.obf
        AFTER_REL_M_OBF=$DATE_DIR/obf/${BASENAME}_after_rel_m.obf
        BEFORE_REL_OBF=$DATE_DIR/obf/${BASENAME}_before_rel.obf
        if [ -f $AFTER_OBF ] && [ -f $BEFORE_OBF ] && [ -f $AFTER_REL_M_OBF ] && [ -f $BEFORE_REL_OBF ]; then
            # marked intermediate step was processed for counting
            touch ${PROC_FILE}
            echo "Complete file ${PROC_FILE} $(date -u)"
        else
            echo "ERROR. One of file:${AFTER_OBF} ${BEFORE_OBF} ${AFTER_REL_M_OBF} ${BEFORE_REL_OBF} did not generated!"
            exit 1;
        fi

        if [ "$1" != "--force" ]; then
            echo "Check sizes of OBFs"
            AFTER_OBF_SIZE=$(ls -l $AFTER_OBF | awk '{print $5}')
            BEFORE_OBF_SIZE=$(ls -l $BEFORE_OBF | awk '{print $5}')
            AFTER_REL_M_OBF_SIZE=$(ls -l $AFTER_REL_M_OBF | awk '{print $5}')
            BEFORE_REL_OBF_SIZE=$(ls -l $BEFORE_REL_OBF | awk '{print $5}')
            K1=1
            if [ AFTER_OBF_SIZE > BEFORE_OBF_SIZE ]; then
                K1=$(($AFTER_OBF_SIZE/$BEFORE_OBF_SIZE));
            else
                K1=$(($BEFORE_OBF_SIZE/$AFTER_OBF_SIZE));
            fi
            if [ "$K1" -ge 2 ]; then
                echo "ERROR. Size is too different ${AFTER_OBF} !≈ ${BEFORE_OBF} [ ${AFTER_OBF_SIZE} !≈ ${BEFORE_OBF_SIZE} ] bytes !"
                exit 1;
            fi
            K2=1
            if [ AFTER_REL_M_OBF_SIZE > BEFORE_REL_OBF_SIZE ]; then
                K2=$(($AFTER_REL_M_OBF_SIZE/$BEFORE_REL_OBF_SIZE));
            else
                K2=$(($BEFORE_REL_OBF_SIZE/$AFTER_REL_M_OBF_SIZE));
            fi
            if [ "$K2" -ge 2 ]; then
                echo "ERROR. Size is too different ${AFTER_REL_M_OBF} !≈ ${BEFORE_REL_OBF} [ ${AFTER_REL_M_OBF_SIZE} !≈ ${BEFORE_REL_OBF_SIZE} ] bytes !"
                exit 1;
            fi
        fi
        rm *.osm.gz || true
        rm *.osm || true
        rm *.rtree* || true
        rm *.obf || true
    done
done
