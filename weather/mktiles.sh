#!/bin/bash -xe
THIS_LOCATION="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

TIFF_FOLDER=tiff
DW_FOLDER=raw

BANDS=("TCDC:entire atmosphere" "TMP:2 m above ground" "PRMSL:mean sea level" "GUST:surface" "PRATE:surface" "UGRD:planetary boundary" "VGRD:planetary boundary")
BANDS_NAMES=("cloud" "temperature" "pressure" "wind" "precip" "windspeed_u" "windspeed_v")

TILES_FOLDER=tiles
TILES_ZOOM_GEN=3
TILES_ZOOM_RES=5
PARALLEL_TO_TILES=2


generate_tiles() {
    rm *.O.tiff || true
    for WFILE in ${TIFF_FOLDER}/*.tiff
    do
        BS=$(basename $WFILE)
        ## generate gdal2tiles fo a given band with given rasterization
        local FILE_NAME="${BS%%.*}"
        local IMG_SIZE=$(( 2 ** TILES_ZOOM_RES * 256)) # generate (2^Z) 256 px
        gzip -cd $TIFF_FOLDER/${FILE_NAME}.tiff.gz  > ${FILE_NAME}_orig.O.tiff
        gdal_translate -projwin -180 84 180 -84 -of GTiff \
            ${FILE_NAME}_orig.O.tiff ${FILE_NAME}_cut.O.tiff
        gdalwarp -of GTiff -t_srs epsg:3857 -r cubic -multi \
            ${FILE_NAME}_cut.O.tiff ${FILE_NAME}_webmerc.O.tiff
        for TILES_BAND in ${!BANDS_NAMES[@]}; do
            local TILES_BAND_NAME=${BANDS_NAMES[$TILES_BAND]}
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


# 0. html to test data
cp "${THIS_LOCATION}/browser.html" .
cp -r "${THIS_LOCATION}/script" .
cp -r "${THIS_LOCATION}/css" .

# 1. generate tiles
generate_tiles
