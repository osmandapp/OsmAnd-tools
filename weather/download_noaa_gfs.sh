#!/bin/bash -xe
THIS_LOCATION="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
BASE_URL=${BASE_URL:-"https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod/"}
PROVIDER=${PROVIDER:-"gfs"}
LAYER=${LAYER:-"atmos"}
# usually 4 hours is enough to get freshest files 
DELAY_HOURS=${DELAY_HOURS:-4}
BANDS=("TCDC:entire atmosphere" "TMP:2 m above ground" "PRMSL:mean sea level" "GUST:surface" "PRATE:surface" "UGRD:planetary boundary" "VGRD:planetary boundary")
BANDS_NAMES=("cloud" "temperature" "pressure" "wind" "precip" "windspeed_u" "windspeed_v")
FILE_PREFIX=${FILE_PREFIX:-"gfs.t"}
FILE_NAME=${FILE_NAME:-"z.pgrb2.0p25.f"}
MINUTES_TO_KEEP=${MINUTES_TO_KEEP:-1800} # 30 hours
HOURS_1H_TO_DOWNLOAD=${HOURS_1H_TO_DOWNLOAD:-36}
HOURS_3H_TO_DOWNLOAD=${HOURS_3H_TO_DOWNLOAD:-180}

DW_FOLDER=raw
TIFF_FOLDER=tiff
TIFF_TEMP_FOLDER=tiff_temp
SPLIT_ZOOM_TIFF=4
DEBUG_M0DE=0

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

        if [[ $server_response =~ "HTTP/2 304" ]]; then
            # Server don't have update for this file. Don't need to download it.
            echo 0
            return
        elif [[ $server_response =~ "HTTP/2 403" ]]; then   
            # We're blocked by server. Wait a bit and continue download
            sleep 60
        fi  
    fi
    echo 1
}


download() {
    local filename=$1
    local url=$2
    local start_byte_offset=$3
    local end_byte_offset=$4
    if [ -z "$start_byte_offset" ] && [ -z "$end_byte_offset" ]; then
        curl -s $url --output ${filename}
    else
        curl -s --range $start_byte_offset-$end_byte_offset $url --output ${filename}
    fi
}


# custom download with retry and result checking
download_with_retry() {
    local filename=$1
    local url=$2
    local start_byte_offset=$3
    local end_byte_offset=$4

    if [[ $( should_download_file "$filename" "$url" ) -eq 1 ]]; then
        echo "Download try 1: ${filename}"
        download $filename $url $start_byte_offset $end_byte_offset
    else 
        echo "Skip downloading: ${filename}"   
        return
    fi

    if [[ $( should_download_file "$filename" "$url" ) -eq 1 ]]; then
        echo "Download Error: ${filename} not downloaded! Wait 10 sec and retry."
        sleep 10
        echo "Download try 2: ${filename}"
        download $filename $url $start_byte_offset $end_byte_offset
    else 
        echo "Downloading success: ${filename}"   
        return    
    fi

    if [[ $( should_download_file "$filename" "$url" ) -eq 1 ]]; then
        echo "Download Error: ${filename} not downloaded! Wait 1 min and retry."
        sleep 60
        echo "Download try 3: ${filename}"
        download $filename $url $start_byte_offset $end_byte_offset
    else 
        return    
    fi

    if [[ $( should_download_file "$filename" "$url" ) -eq 1 ]]; then
        echo "Download Error: ${filename} not downloaded! Wait 5 min and retry."
        sleep 300
        echo "Download try 4: ${filename}"
        download $filename $url $start_byte_offset $end_byte_offset
    else 
        return    
    fi

    if [[ $( should_download_file "$filename" "$url" ) -eq 1 ]]; then
        echo "Download Error: ${filename} not downloaded! Wait 10 min and retry."
        sleep 600
        echo "Download try 5: ${filename}"
        download $filename $url $start_byte_offset $end_byte_offset
    else 
        return    
    fi

    if [[ $( should_download_file "$filename" "$url" ) -eq 1 ]]; then
        echo "Download Error: ${filename} not downloaded! Wait 1 hour and retry."
        sleep 600
        echo "Download try 6: ${filename}"
        download $filename $url $start_byte_offset $end_byte_offset
    else 
        return    
    fi
    
    if [[ $( should_download_file "$filename" "$url" ) -eq 1 ]]; then
        echo "Fatal Download Error: ${filename} still not downloaded!"
    fi

    return
}

create_link_if_needed() {
    local file_path=$1
    local link_path=$2
    if [ ! -f "$link_path" ]; then
        ln -s $file_path $link_path 
    fi
}


#https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod/gfs.20211207/00/atmos/gfs.t00z.pgrb2.0p25.f000
get_raw_files() {
    echo "============================ get_raw_files() ======================================="
    mkdir -p $DW_FOLDER/
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

        # Download index file
        download_with_retry "$DATE/$filename.idx" "$file_link_indx"
        create_link_if_needed $DATE/${filename}.idx $filetime.gt.idx

        # Download channels data by 
        for i in ${!BANDS[@]}; do
            cd $DATE
            local channel_index_lines=$( cat ${filename}.idx | grep -A 1 "${BANDS[$i]}" | awk -F ":" '{print $2}' )
            local start_byte_offset=$( echo $channel_index_lines | awk -F " " '{print $1}' )
            local end_byte_offset=$( echo $channel_index_lines | awk -F " " '{print $2}' )     
            download_with_retry "${BANDS_NAMES[$i]}_$filetime" "$file_link" $start_byte_offset $end_byte_offset
            cd ..
            create_link_if_needed $DATE/${BANDS_NAMES[$i]}_${filetime} ${BANDS_NAMES[$i]}_${filetime}.gt 
            # ln -s $DATE/${BANDS_NAMES[$i]}_${filetime} ${BANDS_NAMES[$i]}_${filetime}.gt 
        done
        cd ..;

        if [[ $DEBUG_M0DE == 1 ]]; then
            return
        fi    
    done
}


generate_bands_tiff() {
    echo "============================= generate_bands_tiff() ==================================="
    mkdir -p $TIFF_FOLDER/
    mkdir -p $TIFF_TEMP_FOLDER/
    for WFILE in ${DW_FOLDER}/*.gt
    do
        local FILE_NAME=$WFILE
        if [[ $OS =~ "Darwin" ]]; then
            FILE_NAME="${FILE_NAME//"raw"}"
            FILE_NAME="${FILE_NAME//".gt"}"
            FILE_NAME="${FILE_NAME:1}"
        else
            FILE_NAME="${FILE_NAME//"raw/"}"
            FILE_NAME="${FILE_NAME//".gt"}"
        fi

        local FOLDER_NAME=$FILE_NAME
        for i in ${!BANDS_NAMES[@]}; do
            FOLDER_NAME="${FOLDER_NAME//"${BANDS_NAMES[$i]}_"}"
        done

        mkdir -p $TIFF_TEMP_FOLDER/$FOLDER_NAME
        gdal_translate $WFILE $TIFF_TEMP_FOLDER/$FOLDER_NAME/${FILE_NAME}.tiff -ot Float32
    done
}


join_tiff_files() {
    echo "============================ join_tiff_files() ===================================="
    cd $TIFF_TEMP_FOLDER
    for CHANNELS_FOLDER in *
    do
        local ALL_CHANNEL_FILES_EXISTS=1
        cd $CHANNELS_FOLDER

        # Create channels list in correct order
        touch settings.txt
        for i in ${!BANDS_NAMES[@]}; do
            if [ ! -f "${BANDS_NAMES[$i]}_$CHANNELS_FOLDER.tiff" ]; then
                ALL_CHANNEL_FILES_EXISTS=0
                break
            fi
            echo "${BANDS_NAMES[$i]}_$CHANNELS_FOLDER.tiff" >> settings.txt
        done

        if [ $ALL_CHANNEL_FILES_EXISTS == 0 ]; then
            echo "Joining Error:  ${BANDS_NAMES[$i]}_$CHANNELS_FOLDER.tiff  not exists. Skip joining."
            cd ..
            continue
        fi

        # Create "Virtual Tiff" with layers order from settings.txt
        gdalbuildvrt bigtiff.vrt -separate -input_file_list settings.txt
        # Create joined tiff from "Virtual Tiff"
        gdal_translate bigtiff.vrt ../../$TIFF_FOLDER/$CHANNELS_FOLDER.tiff -ot Float32
        # Write tiff layers names
        python "$THIS_LOCATION"/set_band_desc.py ../../$TIFF_FOLDER/$CHANNELS_FOLDER.tiff 1 "TCDC:entire atmosphere"  2 "TMP:2 m above ground"  3 "PRMSL:mean sea level" 4 "GUST:surface"  5 "PRATE:surface" 6 "UGRD:planetary boundary" 7 "VGRD:planetary boundary"
        rm settings.txt
        cd ..
    done
    cd ..
}


split_tiles() {
    echo "=============================== split_tiles() ======================================="
    cd $TIFF_FOLDER
    for JOINED_TIFF_NAME in *.tiff
    do
        JOINED_TIFF_NAME="${JOINED_TIFF_NAME//".tiff"}"
        echo "JOINED_TIFF_NAME: $JOINED_TIFF_NAME"

        mkdir -p ${JOINED_TIFF_NAME}
        MAXVALUE=$((1<<${SPLIT_ZOOM_TIFF}))
 
        "$THIS_LOCATION"/slicer.py --zoom ${SPLIT_ZOOM_TIFF} --extraPoints 2 ${JOINED_TIFF_NAME}.tiff ${JOINED_TIFF_NAME}/  
        # generate subgeotiffs into folder
        # 1440*720 / (48*48) = 450
        find ${JOINED_TIFF_NAME}/ -name "*.gz" -delete
        find ${JOINED_TIFF_NAME}/ -maxdepth 1 -type f ! -name '*.gz' -exec gzip "{}" \;    
        # # for (( x=0; x< $MAXVALUE; x++ )); do
        #     # for (( y=0; y< $MAXVALUE; y++ )); do
        #         #local filename=${SPLIT_ZOOM_TIFF}_${x}_${y}.tiff
        #         # gdal_translate  -srcwin TODO $TIFF_FOLDER/${BS}.tiff $TIFF_FOLDER/${BS}/$filename
        #     # done
        # # done

        rm ${JOINED_TIFF_NAME}.tiff.gz || true
        gzip --keep ${JOINED_TIFF_NAME}.tiff
    done
    cd ..
}


# # Debug short case:
# DEBUG_M0DE=1
# rm -rf $DW_FOLDER/
# rm -rf $TIFF_FOLDER/
# rm -rf $TIFF_TEMP_FOLDER/
# get_raw_files 0 $HOURS_1H_TO_DOWNLOAD 1
# generate_bands_tiff
# join_tiff_files
# split_tiles



# 1. cleanup old files to not process them
rm -rf $DW_FOLDER/* || true
rm -rf $TIFF_TEMP_FOLDER/* || true

# 2. download raw files and generate tiffs
get_raw_files 0 $HOURS_1H_TO_DOWNLOAD 1 & 
get_raw_files $HOURS_1H_TO_DOWNLOAD $HOURS_3H_TO_DOWNLOAD 3 &
wait

# 3. generate tiff tiles
generate_bands_tiff
join_tiff_files
split_tiles

# 4. cleanup new temp files
find . -type f -mmin +${MINUTES_TO_KEEP} -delete
find . -type d -empty -delete
rm -rf $TIFF_TEMP_FOLDER/* || true
rm -rf $DW_FOLDER/* || true

echo "DONE!"
