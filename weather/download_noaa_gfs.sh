#!/bin/bash -xe
BASE_URL=${BASE_URL:-"https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod/"}
PROVIDER=${PROVIDER:-"gfs"}
LAYER=${LAYER:-"atmos"}
# usually 4 hours is enough to get freshest files 
DELAY_HOURS=${DELAY_HOURS:-4}
BANDS=("TCDC:entire atmosphere" "TMP:2 m above ground" "PRMSL:mean sea level" "GUST:surface" "PRATE:surface")
BANDS_DIR="cloud temperature pressure wind precip"
FILE_PREFIX=${FILE_PREFIX:-"gfs.t"}
FILE_NAME=${FILE_NAME:-"z.pgrb2.0p25.f"}
OS=$(uname -a)
TIME_ZONE="GMT"
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# folder structure
# band/YYYYmmdd/hh.tiff
# GUST:surface/20211206/15.tiff

# band/YYYYmmdd/hh/{z}/{x}/{y}.png
# GUST:surface/20211206/15/{z}/{x}/{y}.png

#https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod/gfs.20211207/00/atmos/gfs.t00z.pgrb2.0p25.f000
get_0_24() {
    if [[ $OS =~ "Darwin" ]]; then
        HOURS=$(date -u -v-${DELAY_HOURS}H '+%H')]
        DATE=$(date -u -v-${DELAY_HOURS}H '+%Y%m%d')
    else
        HOURS=$(date -u '+%H' -d "-${DELAY_HOURS} hours")
        DATE=$(date -u '+%Y%m%d' -d "-${DELAY_HOURS} hours")
    fi
    # Round down HOURS to 0/6/12/18
    RNDHOURS=$(( $HOURS / 6 * 6 ))
    DOWNLOAD_URL="${BASE_URL}${PROVIDER}.${DATE}"
    local url="$DOWNLOAD_URL/${RNDHOURS}/$LAYER/${FILE_PREFIX}${RNDHOURS}${FILE_NAME}"
    for (( c=0; c<=2; c++ ))
    do
        local h=$c
        if [ $c -lt 10 ]; then
            h="00$h"
        elif [ $c -lt 100 ]; then
            h="0$h"
        fi
        local file_link="${url}${h}"
        local file_link_indx="${url}${h}.idx"
        local filetime=""
        if [[ $OS =~ "Darwin" ]]; then
            filetime=$(date -ju -v+${c}H -f '%Y%m%d %H%M' '+%Y%m%d_%H%M' "${DATE} ${RNDHOURS}00")
        else
            filetime=$(date -d "${DATE} ${RNDHOURS}00 +${c} hours" '+%Y%m%d_%H%M')
        fi
        mkdir -p "tmp/"
        wget $file_link_indx --timeout=900 -O tmp/${LAYER}-${filetime}.idx
        if [[ $? -ne 0 ]]; then
            echo -en "${RED} $file_link_indx not downloaded${NC}"
            exit 1;
        else
            echo -en "${GREEN} $file_link_indx downloaded${NC}"
        fi
        wget $file_link --timeout=900 -O tmp/${LAYER}-${filetime}
        if [[ $? -ne 0 ]]; then
            echo -en "${RED} $file_link not downloaded${NC}"
            exit 1;
        else
            echo -en "${GREEN} $file_link downloaded${NC}"
        fi
    done
}
         
get_bands_tiff() {
    local file_list=$(find tmp)
    for WFILE in $file_list
    do
        if [[ $WFILE =~ "*.idx" ]]; then
            continue
        fi
        band_numbers=""
        for i in ${!BANDS[@]}; do
            local b_num=$(cat $WFILE.idx | grep "${BANDS[$i]}" | awk 'NR==1{print $1}' | awk -F ":" '{print $1}')
            band_numbers="$band_numbers -b $b_num"
        done
        local b_dir=$(echo $WFILE | awk -F / '{print "/"$2"/"$3"/"}')
        b_dir="tiff/$b_dir"
        mkdir -p $b_dir
        gdal_translate $band_numbers -mask "none" $WFILE ${b_dir}world.tiff
    done
}

get_0_24
get_bands_tiff
rm -rf tmp/
