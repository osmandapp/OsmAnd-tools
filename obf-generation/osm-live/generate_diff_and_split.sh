#!/bin/bash -e

RESULT_DIR="/home/osmlive"
OSMAND_MAP_CREATOR_PATH=OsmAndMapCreator
export JAVA_OPTS="-Xms512M -Xmx24014M"
chmod +x $OSMAND_MAP_CREATOR_PATH/utilities.sh
SRTM_DIR="/home/relief-data/srtm/"
for DATE_DIR in $(find $RESULT_DIR/_diff -maxdepth 1  -type d | sort ); do
    if [ "$DATE_DIR" = "$RESULT_DIR/_diff" ]; then
        continue
    fi
    COUNT_OBF_FILES=$(find $DATE_DIR -maxdepth 1 -type f  -name "*.obf.gz" | wc -l)
    if [ ! -d $DATE_DIR/obf ]; then
        continue;
    fi
    COUNT_IOBF_FILES=$(find $DATE_DIR/obf -type f -name "*.done" | wc -l)
    if [ $COUNT_IOBF_FILES -le $COUNT_OBF_FILES ]; then
        continue;
    fi
    echo "###  Process $DATE_DIR $COUNT_OBF_FILES $COUNT_IOBF_FILES "
    for PROC_FILE in $(ls $DATE_DIR/obf/*.done); do
        # cut _diff.osm.gz
        BASENAME=$(basename $PROC_FILE);
        BASENAME=${BASENAME%*.done}
        OBF_FILE=$DATE_DIR/${BASENAME}.obf.gz
        DIFF_FILE=$DATE_DIR/src/${BASENAME}_diff.osm.gz
        BEFORE_OBF_FILE=$DATE_DIR/obf/${BASENAME}_before.obf
        AFTER_OBF_FILE=$DATE_DIR/obf/${BASENAME}_after.obf
        BEFORE_REL_OBF_FILE=$DATE_DIR/obf/${BASENAME}_before_rel.obf
        AFTER_REL_M_OBF_FILE=$DATE_DIR/obf/${BASENAME}_after_rel_m.obf
        if [ -f $OBF_FILE ]; then
            continue;
        fi
        
        echo "Process missing file $OBF_FILE"
        if [ ! -f $BEFORE_OBF_FILE ]; then
            echo "Missing file $BEFORE_OBF_FILE"
            exit 1;
        fi
        if [ ! -f $AFTER_OBF_FILE ]; then
            echo "Missing file $AFTER_OBF_FILE"
            exit 1;
        fi
        if [ ! -f $BEFORE_REL_OBF_FILE ]; then
            echo "Missing file $BEFORE_REL_OBF_FILE"
            exit 1;
        fi
        if [ ! -f $AFTER_REL_M_OBF_FILE ]; then
            echo "Missing file $AFTER_REL_M_OBF_FILE"
            exit 1;
        fi

        echo "### 1. Generate diff files : $(date -u)"
        $OSMAND_MAP_CREATOR_PATH/utilities.sh generate-obf-diff \
            ${BEFORE_OBF_FILE} ${AFTER_OBF_FILE} ${BASENAME}_diff.obf $DIFF_FILE &
        $OSMAND_MAP_CREATOR_PATH/utilities.sh generate-obf-diff-no-transport \
            ${BEFORE_REL_OBF_FILE} ${AFTER_REL_M_OBF_FILE} ${BASENAME}_diff_rel.obf &
        wait

        echo "#### 2. Merge ${BASENAME}_diff_rel.obf into ${BASENAME}_diff.obf . Avoid osmand_change=delete"
        # TESTONLY: comment after test
        $OSMAND_MAP_CREATOR_PATH/utilities.sh merge-obf-diff ${BASENAME}_diff_rel.obf ${BASENAME}_diff.obf ${BASENAME}_diff_test.obf
        # TESTONLY: uncomment after test
        # $OSMAND_MAP_CREATOR_PATH/utilities.sh merge-obf-diff ${BASENAME}_diff_rel.obf ${BASENAME}_diff.obf

        echo "### 3. Split files : $(date -u)"
        DATE_NAME=${BASENAME:0:8} #22_10_11
        TIME_NAME=${BASENAME:9:12} #20_30
        $OSMAND_MAP_CREATOR_PATH/utilities.sh split-obf ${BASENAME}_diff.obf $RESULT_DIR  "$DATE_NAME" "_$TIME_NAME" --srtm="$SRTM_DIR"

        gzip -c ${BASENAME}_diff.obf > $DATE_DIR/${BASENAME}.obf.gz
        # TESTONLY: comment after test
        mkdir -p $DATE_DIR/test
        gzip -c ${BASENAME}_diff_test.obf > $DATE_DIR/test/${BASENAME}_test.obf.gz
        touch -r $DIFF_FILE $DATE_DIR/${BASENAME}.obf.gz

        # Remove intermediate obf files
        # rm ${BEFORE_OBF_FILE}
        # rm ${AFTER_OBF_FILE}
        # rm ${BEFORE_REL_OBF_FILE}
        # rm ${AFTER_REL_M_OBF_FILE}
        rm -r *.osm || true
        rm -r *.rtree* || true
        rm -r *.obf || true
    done
done
