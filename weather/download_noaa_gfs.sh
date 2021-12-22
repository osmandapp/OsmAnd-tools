#!/bin/bash -xe
THIS_LOCATION="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
BASE_URL=${BASE_URL:-"https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod/"}
PROVIDER=${PROVIDER:-"gfs"}
LAYER=${LAYER:-"atmos"}
# usually 4 hours is enough to get freshest files 
DELAY_HOURS=${DELAY_HOURS:-4}
BANDS=("TCDC:entire atmosphere" "TMP:2 m above ground" "PRMSL:mean sea level" "GUST:surface" "PRATE:surface")
BANDS_NAMES="cloud temperature pressure wind precip"
FILE_PREFIX=${FILE_PREFIX:-"gfs.t"}
FILE_NAME=${FILE_NAME:-"z.pgrb2.0p25.f"}
MINUTES_TO_KEEP=${MINUTES_TO_KEEP:-1800} # 30 hours
HOURS_TO_DOWNLOAD=${HOURS_TO_DOWNLOAD:-36}

DW_FOLDER=raw
TIFF_FOLDER=tiff

TILES_FOLDER=tiles
TILES_ZOOM_GEN=6
TILES_ZOOM_RES=6
TILES_BAND=5
TILES_BAND_NAME=cloud

OS=$(uname -a)
TIME_ZONE="GMT"
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

#https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod/gfs.20211207/00/atmos/gfs.t00z.pgrb2.0p25.f000
get_raw_files() {
    rm $DW_FOLDER/*.gt || true
    rm $DW_FOLDER/*.gt.idx || true
    if [[ $OS =~ "Darwin" ]]; then
        HOURS=$(date -u -v-${DELAY_HOURS}H '+%-H')]
        DATE=$(date -u -v-${DELAY_HOURS}H '+%Y%m%d')
    else
        HOURS=$(date -u '+%-H' -d "-${DELAY_HOURS} hours")
        DATE=$(date -u '+%Y%m%d' -d "-${DELAY_HOURS} hours")
    fi
    # Round down HOURS to 0/6/12/18
    RNDHOURS=$(printf "%02d" $(( $HOURS / 6 * 6 )))
    DOWNLOAD_URL="${BASE_URL}${PROVIDER}.${DATE}"
    local url="$DOWNLOAD_URL/${RNDHOURS}/$LAYER/"
    for (( c=0; c<=${HOURS_TO_DOWNLOAD}; c++ ))
    do
        local h=$c
        if [ $c -lt 10 ]; then
            h="00$h"
        elif [ $c -lt 100 ]; then
            h="0$h"
        fi
        local filename="${FILE_PREFIX}${RNDHOURS}${FILE_NAME}${h}"
        local file_link="${url}${filename}"
        local file_link_indx="${url}${filename}.idx"
        local filetime=""
        if [[ $OS =~ "Darwin" ]]; then
            filetime=$(date -ju -v+${c}H -f '%Y%m%d %H%M' '+%Y%m%d_%H%M' "${DATE} ${RNDHOURS}00")
        else
            filetime=$(date -d "${DATE} ${RNDHOURS}00 +${c} hours" '+%Y%m%d_%H%M')
        fi
        mkdir -p "$DW_FOLDER/$DATE"
        cd $DW_FOLDER; 
        ( cd $DATE; wget -nv -N --no-if-modified-since $file_link_indx --timeout=900 )
        rm $filetime.gt.idx || true
        ln -s $DATE/${filename}.idx $filetime.gt.idx
        ( cd $DATE; wget -nv -N --no-if-modified-since $file_link --timeout=900 )
        rm $filetime.gt || true
        ln -s $DATE/${filename} $filetime.gt
        cd ..;
    done
}
         
get_bands_tiff() {
    for WFILE in ${DW_FOLDER}/*.gt
    do
        band_numbers=""
        for i in ${!BANDS[@]}; do
            local b_num=$(cat $WFILE.idx | grep "${BANDS[$i]}" | awk 'NR==1{print $1}' | awk -F ":" '{print $1}')
            band_numbers="$band_numbers -b $b_num"
        done
        mkdir -p $TIFF_FOLDER/
        BS=$(basename $WFILE)
        gdal_translate $band_numbers -mask "none" $WFILE $TIFF_FOLDER/${BS}.tiff

        ## generate gdal2tiles fo a given band with given rasterization
        local FILE_NAME="${BS%%.*}"
        local IMG_SIZE=$(( 2 ** TILES_ZOOM_RES * 256)) # generate (2^Z) 256 px
        gdalwarp -of GTiff --config 4 4 \
            -co "SPARSE_OK=TRUE" -t_srs "+init=epsg:3857 +over" \
            -r cubic -multi \
            $TIFF_FOLDER/${BS}.tiff ${FILE_NAME}.M.tiff
        gdal_translate -b ${TILES_BAND} ${FILE_NAME}.M.tiff ${FILE_NAME}.PM.tiff  -outsize $IMG_SIZE $IMG_SIZE -r lanczos
        gdaldem color-relief -alpha ${FILE_NAME}.PM.tiff "${THIS_LOCATION}/${TILES_BAND_NAME}_color.txt" ${FILE_NAME}.APM.tiff
        gdal_translate -of VRT -ot Byte -scale ${FILE_NAME}.APM.tiff ${FILE_NAME}.APM.vrt
        mkdir -p $TILES_FOLDER/$TILES_BAND_NAME/$FILE_NAME
        gdal2tiles.py -z $ZOOM ${FILE_NAME}.APM.vrt  $TILES_FOLDER/$TILES_BAND_NAME/$FILE_NAME
        #rm *M.tiff || true
        #rm *.vrt || true

    done
}
# cleanup 
find $DW_FOLDER/ -type f -mmin +${MINUTES_TO_KEEP} -delete
find $DW_FOLDER/ -type d -empty -delete
get_raw_files
get_bands_tiff

find $TIFF_FOLDER/ -type f -mmin +${MINUTES_TO_KEEP} -delete
#rm -rf $DW_FOLDER/
