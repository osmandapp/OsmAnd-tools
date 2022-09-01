#!/bin/bash -xe
THIS_LOCATION="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
BASE_URL=${BASE_URL:-"https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod/"}
PROVIDER=${PROVIDER:-"gfs"}
LAYER=${LAYER:-"atmos"}
# usually 4 hours is enough to get freshest files 
DELAY_HOURS=${DELAY_HOURS:-4}
BANDS=("TCDC:entire atmosphere" "TMP:2 m above ground" "PRMSL:mean sea level" "GUST:surface" "PRATE:surface")
BANDS_NAMES=("cloud" "temperature" "pressure" "wind" "precip")
FILE_PREFIX=${FILE_PREFIX:-"gfs.t"}
FILE_NAME=${FILE_NAME:-"z.pgrb2.0p25.f"}
MINUTES_TO_KEEP=${MINUTES_TO_KEEP:-1800} # 30 hours
HOURS_1H_TO_DOWNLOAD=${HOURS_1H_TO_DOWNLOAD:-36}
HOURS_3H_TO_DOWNLOAD=${HOURS_3H_TO_DOWNLOAD:-180}

DW_FOLDER=raw
TIFF_FOLDER=tiff
SPLIT_ZOOM_TIFF=4

OS=$(uname -a)
TIME_ZONE="GMT"
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

if [[ $OS =~ "Darwin" ]]; then
    HOURS=$(date -u -v-${DELAY_HOURS}H '+%-H')
    DATE=$(date -u -v-${DELAY_HOURS}H '+%Y%m%d')
else
    HOURS=$(date -u '+%-H' -d "-${DELAY_HOURS} hours")
    DATE=$(date -u '+%Y%m%d' -d "-${DELAY_HOURS} hours")
fi
# Round down HOURS to 0/6/12/18
RNDHOURS=$(printf "%02d" $(( $HOURS / 6 * 6 )))

should_download_file() {
    local filename=$1
    local url=$2

    if [[ -f $filename ]]; then
        # File is already dowlnloaded
        disk_file_modified_time="$(TZ=UMT0 date -r ${filename} +'%a, %d %b %Y %H:%M:%S GMT')"
        local server_response=$(curl -s -I --header "If-Modified-Since: $disk_file_modified_time" $file_link_indx | head -1)
    
        if [[ $server_file_modified_time_response =~ "HTTP/2 304" ]]; then
            # Server don't have update for this file. Don't need to download it.
            echo 0
            return
        elif [[ $server_file_modified_time_response =~ "HTTP/2 403" ]]; then
            # Server is blocking. Let's wait a bit
            sleep 60
        fi  
    fi
    echo 1
}

#https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod/gfs.20211207/00/atmos/gfs.t00z.pgrb2.0p25.f000
get_raw_files() {
    HOURS_START=$1
    HOURS_ALL=$2
    HOURS_INC=$3
    DOWNLOAD_URL="${BASE_URL}${PROVIDER}.${DATE}"
    local url="$DOWNLOAD_URL/${RNDHOURS}/$LAYER/"
    for (( c=${HOURS_START}; c<=${HOURS_ALL}; c+=${HOURS_INC} ))
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

        if [[ $( should_download_file "$DATE/$filename.idx" "$file_link_indx" ) -eq 1 ]]; then
            echo "Downloading index: ${filename}.idx"
            ( cd $DATE; curl -s --retry 3 --connect-timeout 60 --retry-max-time 300 $file_link_indx --output ${filename}.idx )
            ln -s $DATE/${filename}.idx $filetime.gt.idx
        else 
            echo "Skipping index: ${filename}.idx"   
        fi
        sleep 5

        for i in ${!BANDS[@]}; do
            if [[ $( should_download_file "$DATE/${BANDS_NAMES[$i]}_$filetime" "$file_link" ) -eq 1 ]]; then
                echo "Downloading file: ${BANDS_NAMES[$i]}_${filetime}"
                cd $DATE
                local indexes=$( cat ${filename}.idx | grep -A 1 "${BANDS[$i]}" | awk -F ":" '{print $2}' )
                local start_index=$( echo $indexes | awk -F " " '{print $1}' )
                local end_index=$( echo $indexes | awk -F " " '{print $2}' )
                curl -s --retry 3 --connect-timeout 60 --retry-max-time 300 --range $start_index-$end_index $file_link --output ${BANDS_NAMES[$i]}_${filetime}
                cd ..
                ln -s $DATE/${BANDS_NAMES[$i]}_${filetime} ${BANDS_NAMES[$i]}_${filetime}.gt 
            else   
                echo "Skipping file: ${BANDS_NAMES[$i]}_${filetime}"
            fi
            sleep 2
        done
        cd ..;
    done
}
         
generate_bands_tiff() {
    for WFILE in ${DW_FOLDER}/*.gt
    do
        local FILE_NAME="${WFILE//"raw/"}"
        FILE_NAME="${FILE_NAME//".gt"}"
        mkdir -p $TIFF_FOLDER/

        gdal_translate $WFILE $TIFF_FOLDER/${FILE_NAME}.tiff
        MAXVALUE=$((1<<${SPLIT_ZOOM_TIFF}))

        mkdir -p $TIFF_FOLDER/${FILE_NAME}/
        "$THIS_LOCATION"/slicer.py --zoom ${SPLIT_ZOOM_TIFF} --extraPoints 2 $TIFF_FOLDER/${FILE_NAME}.tiff $TIFF_FOLDER/${FILE_NAME}/
        # generate subgeotiffs into folder
        # 1440*720 / (48*48) = 450
        rm $TIFF_FOLDER/${FILE_NAME}/*.gz || true
        find $TIFF_FOLDER/${FILE_NAME}/ -maxdepth 1 -type f ! -name '*.gz' -exec gzip "{}" \;
        # for (( x=0; x< $MAXVALUE; x++ )); do
            # for (( y=0; y< $MAXVALUE; y++ )); do
                #local filename=${SPLIT_ZOOM_TIFF}_${x}_${y}.tiff
                # gdal_translate  -srcwin TODO $TIFF_FOLDER/${BS}.tiff $TIFF_FOLDER/${BS}/$filename
            # done
        # done
        rm $TIFF_FOLDER/${FILE_NAME}.tiff.gz || true
        gzip --keep $TIFF_FOLDER/${FILE_NAME}.tiff
    done
}

# 1. cleanup old files to not process them
rm $DW_FOLDER/*.gt || true
rm $DW_FOLDER/*.gt.idx || true

# # 2. download raw files and generate tiffs
get_raw_files 0 $HOURS_1H_TO_DOWNLOAD 1 & 
get_raw_files $HOURS_1H_TO_DOWNLOAD $HOURS_3H_TO_DOWNLOAD 3 &
wait
# generate_bands_tiff

# # 3. redownload what's missing again (double check)
get_raw_files 0 $HOURS_1H_TO_DOWNLOAD 1 & 
get_raw_files $HOURS_1H_TO_DOWNLOAD $HOURS_3H_TO_DOWNLOAD 3 &
wait
generate_bands_tiff

find . -type f -mmin +${MINUTES_TO_KEEP} -delete
find . -type d -empty -delete
# # #rm -rf $DW_FOLDER/
