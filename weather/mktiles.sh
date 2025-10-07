#!/bin/bash -xe
SCRIPT_PROVIDER_MODE=$1
THIS_LOCATION="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

ROOT_FOLDER=$(pwd)
GFS="gfs"
ECMWF="ecmwf"
TIFF_FOLDER="tiff"
# bands should correspond to download_weather.sh
BANDS=("TCDC:entire atmosphere" "TMP:2 m above ground" "PRMSL:mean sea level" "GUST:surface" "PRATE:surface" "UGRD:planetary boundary" "VGRD:planetary boundary")
BANDS_NAMES=("cloud" "temperature" "pressure" "wind" "precip" "windspeed_u" "windspeed_v")

TILES_FOLDER=tiles

TILES_ZOOM_GEN="${TILES_ZOOM_GEN:-3}"
TILES_ZOOM_RES="${TILES_ZOOM_RES:-5}"
PARALLEL_TO_TILES="${PARALLEL_TO_TILES:-2}"
OS=$(uname -a)


generate_tiles() {
    MODE=$1
    local BANDS_NAMES_LOCAL=("${BANDS_NAMES[@]}")
    local BANDS_DESCRIPTIONS_LOCAL=("${BANDS[@]}")

    rm *.O.tiff || true
    for WFILE in ${TIFF_FOLDER}/*.tiff
    do
        local FILE_NAME=$WFILE
        local TIMESTAMP_NOW=0
        local TIMESTAMP_FILE_FORECAST_DATE=0
	gdalinfo $WFILE | grep STATISTICS_MAXIMUM
        if [[ $OS =~ "Darwin" ]]; then
            FILE_NAME="${FILE_NAME//".tiff"}"
            FILE_NAME="${FILE_NAME//"tiff"}"
            FILE_NAME="${FILE_NAME:1}"

            TIMESTAMP_NOW=$(TZ=GMT date "+%s")
            TIMESTAMP_FILE_FORECAST_DATE=$(TZ=GMT date -jf "%Y%m%d_%H00" "${FILE_NAME}" "+%s")

        else
            FILE_NAME="${FILE_NAME//"tiff/"}"
            FILE_NAME="${FILE_NAME//".tiff"}"

            local DATE_PART=${FILE_NAME:0:8}
            local HOURS_PART=${FILE_NAME:9:2}
            TIMESTAMP_NOW=$(TZ=GMT date +%s)
            TIMESTAMP_FILE_FORECAST_DATE=$(TZ=GMT date -d "${DATE_PART} ${HOURS_PART}00" '+%s')
        fi

        # Don't run script for outdated yesterday's files
        local DAYS_DIFFERECE=$(( ($TIMESTAMP_NOW - $TIMESTAMP_FILE_FORECAST_DATE) / (24 * 3600) ))
        if [[ $DAYS_DIFFERECE -ge 1 ]]; then
            echo "Skip"
            echo "Skip: file is outdated  $WFILE"
            continue
        fi

        BS=$(basename $WFILE)
        ## generate gdal2tiles fo a given band with given rasterization
        local FILE_NAME="${BS%%.*}"
        local IMG_SIZE=$(( 2 ** TILES_ZOOM_RES * 256)) # generate (2^Z) 256 px
        gzip -cd $TIFF_FOLDER/${FILE_NAME}.tiff.gz  > ${FILE_NAME}_orig.O.tiff
        gdal_translate -projwin -180 84 180 -84 -of GTiff \
            ${FILE_NAME}_orig.O.tiff ${FILE_NAME}_cut.O.tiff
        gdalwarp -of GTiff -t_srs epsg:3857 -r cubic -multi \
            ${FILE_NAME}_cut.O.tiff ${FILE_NAME}_webmerc.O.tiff
        for TILES_BAND in ${!BANDS_NAMES_LOCAL[@]}; do
            local TILES_BAND_NAME=${BANDS_NAMES_LOCAL[$TILES_BAND]}
            local BAND_IND=$(( $TILES_BAND + 1 ))
            local FILE_BAND_NAME=${FILE_NAME}_${TILES_BAND_NAME}
            gdal_translate -b ${BAND_IND} -outsize $IMG_SIZE $IMG_SIZE -r lanczos \
                     ${FILE_NAME}_webmerc.O.tiff ${FILE_BAND_NAME}_img.M.tiff
            gdaldem color-relief -alpha ${FILE_BAND_NAME}_img.M.tiff "${THIS_LOCATION}/${TILES_BAND_NAME}_color.txt" \
                     ${FILE_BAND_NAME}_clr.M.tiff
            mkdir -p $TILES_FOLDER/$TILES_BAND_NAME/$FILE_NAME
            gdal2tiles.py -w none --tilesize=512 --processes=${PARALLEL_TO_TILES} -z 1-${TILES_ZOOM_GEN} \
                    ${FILE_BAND_NAME}_clr.M.tiff $TILES_FOLDER/$TILES_BAND_NAME/$FILE_NAME
            # rm $TILES_FOLDER/$TILES_BAND_NAME/$FILE_NAME/*.html || true
        done
        rm *.M.tiff || true
        rm *.O.tiff || true
    done
}


if [[ $SCRIPT_PROVIDER_MODE =~ $GFS ]]; then
    echo "============================ GFS Provider tile making ======================================="
    cd $GFS
    # html to test data
    cp "${THIS_LOCATION}/browser.html" .
    cp -r "${THIS_LOCATION}/script" .
    cp -r "${THIS_LOCATION}/css" .
    # generating tiles
    generate_tiles $GFS
elif [[ $SCRIPT_PROVIDER_MODE =~ $ECMWF ]]; then
    echo "============================ ECMWF Provider tile making ======================================="
    cd $ECMWF
    cp "${THIS_LOCATION}/browser.html" .
    cp -r "${THIS_LOCATION}/script" .
    cp -r "${THIS_LOCATION}/css" .
    generate_tiles $ECMWF
fi
