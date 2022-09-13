#!/bin/bash -xe
SCRIPT_PROVIDER_MODE=$1
THIS_LOCATION="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
ROOT_FOLDER=$(pwd)
GFS="gfs"
ECMWF="ecmwf"
DOWNLOAD_FOLDER="raw"
TIFF_FOLDER="tiff"
TIFF_TEMP_FOLDER="tiff_temp"
FULL_MODE='full_mode'
LATEST_MODE='lstest_mode'

GFS_BANDS=("TCDC:entire atmosphere" "TMP:2 m above ground" "PRMSL:mean sea level" "GUST:surface" "PRATE:surface" "UGRD:planetary boundary" "VGRD:planetary boundary")
GFS_BANDS_NAMES=("cloud" "temperature" "pressure" "wind" "precip" "windspeed_u" "windspeed_v")
ECMWF_BANDS=("Temperature" "Pressure" "10 metre u-velocity" "10 metre v-velocity" "Total precipitation")
ECMWF_BANDS_NAMES=("2t" "msl" "10u" "10v" "tp")

MINUTES_TO_KEEP_TIFF_FILES=${MINUTES_TO_KEEP_TIFF_FILES:-1800} # 30 hours
HOURS_1H_TO_DOWNLOAD=${HOURS_1H_TO_DOWNLOAD:-36}
HOURS_3H_TO_DOWNLOAD=${HOURS_3H_TO_DOWNLOAD:-180}

OS=$(uname -a)
TIME_ZONE="GMT"
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

DEBUG_M0DE=0


setup_folders_on_start() {
    mkdir -p "$ROOT_FOLDER/$GFS"
    mkdir -p "$ROOT_FOLDER/$ECMWF"
    mkdir -p "$DOWNLOAD_FOLDER/"
    mkdir -p "$TIFF_FOLDER/"
    mkdir -p "$TIFF_TEMP_FOLDER/"

    rm -rf $DOWNLOAD_FOLDER/* || true
    rm -rf $TIFF_TEMP_FOLDER/* || true
    if [[ $DEBUG_M0DE == 1 ]]; then
        rm -rf $TIFF_FOLDER/* || true 
    fi  
}


clean_temp_files_on_finish() {
    if [[ $DEBUG_M0DE != 1 ]]; then
        rm -rf $TIFF_TEMP_FOLDER/* || true
        rm -rf $DOWNLOAD_FOLDER/* || true
    fi
    # Delete outdated tiff files if needed
    find . -type f -mmin +${MINUTES_TO_KEEP_TIFF_FILES} -delete
    find . -type d -empty -delete 
}


should_download_file() {
    local FILENAME=$1
    local URL=$2

    if [[ -f $FILENAME ]]; then
        # File is already dowlnloaded
        local DISK_FILE_MODIFIED_TIME="$(TZ=UMT0 date -r ${FILENAME} +'%a, %d %b %Y %H:%M:%S GMT')"
        local SERVER_RESPONSE=$(curl -s -I --header "If-Modified-Since: $DISK_FILE_MODIFIED_TIME" $URL | head -1)

        if [[ $SERVER_RESPONSE =~ "304" ]]; then
            # Server don't have update for this file. Don't need to download it.
            echo 0
            return
        elif [[ $SERVER_RESPONSE =~ "403" ]]; then   
            # We're blocked by server. Wait a bit and continue download
            sleep 60
        elif [[ $SERVER_RESPONSE =~ "404" ]]; then   
            echo 0
            return
        fi  
    elif [[ $(curl -s -I $URL | head -1) =~ "404"  ]]; then   
        # File not found. Skip 
        echo 0
        return
    fi
    echo 1
}


download() {
    local FILENAME=$1
    local URL=$2
    local START_BYTE_OFFSET=$3
    local END_BYTE_OFFSET=$4
    if [ -z "$START_BYTE_OFFSET" ] && [ -z "$END_BYTE_OFFSET" ]; then
        # download whole file
        curl -k $URL --output ${FILENAME} --http1.1  || sleep 1
    else
        # download part file by byte offset
        curl -k --range $START_BYTE_OFFSET-$END_BYTE_OFFSET $URL --output ${FILENAME} --http1.1 || sleep 1
    fi
}


# Custom download with retry and result checking.
# It's need because of too regular interupts or blockings from weather server.
download_with_retry() {
    local FILENAME=$1
    local URL=$2
    local START_BYTE_OFFSET=$3
    local END_BYTE_OFFSET=$4

    if [[ $( should_download_file "$FILENAME" "$URL" ) -eq 1 ]]; then
        echo "Download try 1: ${FILENAME}"
        download $FILENAME $URL $START_BYTE_OFFSET $END_BYTE_OFFSET
    else 
        echo "Skip downloading: ${FILENAME}"   
        return
    fi

    if [[ $( should_download_file "$FILENAME" "$URL" ) -eq 1 ]]; then
        echo "Download Error: ${FILENAME} not downloaded! Wait 10 sec and retry."
        sleep 10
        echo "Download try 2: ${FILENAME}"
        download $FILENAME $URL $START_BYTE_OFFSET $END_BYTE_OFFSET
    else 
        echo "Downloading success with try 1: ${FILENAME}"   
        return    
    fi

    if [[ $( should_download_file "$FILENAME" "$URL" ) -eq 1 ]]; then
        echo "Download Error: ${FILENAME} not downloaded! Wait 1 min and retry."
        sleep 60
        echo "Download try 3: ${FILENAME}"
        download $FILENAME $URL $START_BYTE_OFFSET $END_BYTE_OFFSET
    else 
        echo "Downloading success with try 2: ${FILENAME}"   
        return    
    fi

    if [[ $( should_download_file "$FILENAME" "$URL" ) -eq 1 ]]; then
        echo "Download Error: ${FILENAME} not downloaded! Wait 5 min and retry."
        sleep 300
        echo "Download try 4: ${FILENAME}"
        download $FILENAME $URL $START_BYTE_OFFSET $END_BYTE_OFFSET
    else 
        echo "Downloading success with try 3: ${FILENAME}"   
        return    
    fi

    if [[ $( should_download_file "$FILENAME" "$URL" ) -eq 1 ]]; then
        echo "Download Error: ${FILENAME} not downloaded! Wait 10 min and retry."
        sleep 600
        echo "Download try 5: ${FILENAME}"
        download $FILENAME $URL $START_BYTE_OFFSET $END_BYTE_OFFSET
    else 
        echo "Downloading success with try 4: ${FILENAME}"   
        return    
    fi

    if [[ $( should_download_file "$FILENAME" "$URL" ) -eq 1 ]]; then
        echo "Download Error: ${FILENAME} not downloaded! Wait 1 hour and retry."
        sleep 600
        echo "Download try 6: ${FILENAME}"
        download $FILENAME $URL $START_BYTE_OFFSET $END_BYTE_OFFSET
    else 
        echo "Downloading success with try 5: ${FILENAME}"   
        return    
    fi
    
    if [[ $( should_download_file "$FILENAME" "$URL" ) -eq 1 ]]; then
        echo "Fatal Download Error: ${FILENAME} still not downloaded!"
    fi

    return
}


get_raw_gfs_files() {
    echo "============================ get_raw_gfs_files() ======================================="
    local HOURS_START=$1
    local HOURS_ALL=$2
    local HOURS_INC=$3
    local LAYER=${LAYER:-"atmos"}

    # Prepare all time parameters for url:
    # (usually 4 hours offset is enough to get freshest files)
    local DELAY_HOURS=${DELAY_HOURS:-4}
    if [[ $OS =~ "Darwin" ]]; then
        HOURS=$(date -u -v-${DELAY_HOURS}H '+%-H')
        DATE=$(date -u -v-${DELAY_HOURS}H '+%Y%m%d')
    else
        HOURS=$(date -u '+%-H' -d "-${DELAY_HOURS} hours")
        DATE=$(date -u '+%Y%m%d' -d "-${DELAY_HOURS} hours")
    fi
    # Round down HOURS to 0/6/12/18
    local RNDHOURS=$(printf "%02d" $(( $HOURS / 6 * 6 )))
    local URL_BASE="https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod/gfs.${DATE}/${RNDHOURS}/$LAYER/"


    for (( c=${HOURS_START}; c<=${HOURS_ALL}; c+=${HOURS_INC} ))
    do
        local FORECAST_HOURS_OFFSET=$c
        if [ $c -lt 10 ]; then
            FORECAST_HOURS_OFFSET="00$FORECAST_HOURS_OFFSET"
        elif [ $c -lt 100 ]; then
            FORECAST_HOURS_OFFSET="0$FORECAST_HOURS_OFFSET"
        fi
        local FILENAME="gfs.t${RNDHOURS}z.pgrb2.0p25.f${FORECAST_HOURS_OFFSET}"
        local FILE_INDEX_URL="${URL_BASE}${FILENAME}.idx"
        local FILE_DATA_URL="${URL_BASE}${FILENAME}"
        local FILETIME=""
        if [[ $OS =~ "Darwin" ]]; then
            FILETIME=$(date -ju -v+${c}H -f '%Y%m%d %H%M' '+%Y%m%d_%H%M' "${DATE} ${RNDHOURS}00")
        else
            FILETIME=$(date -d "${DATE} ${RNDHOURS}00 +${c} hours" '+%Y%m%d_%H%M')
        fi


        # Download index file
        cd $DOWNLOAD_FOLDER; 
        download_with_retry "$FILETIME.index" "$FILE_INDEX_URL"

        # Download needed bands forecast data
        if [[ -f "$FILETIME.index" ]]; then   
            for i in ${!GFS_BANDS[@]}; do
                # Parse from index file start and end byte offset for needed band
                local CHANNEL_INDEX_LINES=$( cat ${FILETIME}.index | grep -A 1 "${GFS_BANDS[$i]}" | awk -F ":" '{print $2}' )
                local START_BYTE_OFFSET=$( echo $CHANNEL_INDEX_LINES | awk -F " " '{print $1}' )
                local END_BYTE_OFFSET=$( echo $CHANNEL_INDEX_LINES | awk -F " " '{print $2}' ) 

                # Make partial download for needed band data only  
                # https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod/gfs.20211207/00/atmos/gfs.t00z.pgrb2.0p25.f000
                download_with_retry "${GFS_BANDS_NAMES[$i]}_$FILETIME.gt" "$FILE_DATA_URL" $START_BYTE_OFFSET $END_BYTE_OFFSET

                # Generate tiff for downloaded band
                mkdir -p "../$TIFF_TEMP_FOLDER/$FILETIME"
                gdal_translate "${GFS_BANDS_NAMES[$i]}_$FILETIME.gt" "../$TIFF_TEMP_FOLDER/$FILETIME/${GFS_BANDS_NAMES[$i]}_$FILETIME.tiff" -ot Float32 -stats | sleep 1
            done
        else
            echo "Error: Index file not downloaded. Skip downloading weather data."
        fi    
        cd ..;

        if [[ $DEBUG_M0DE == 1 ]]; then
            return
        fi    
    done
}


generate_bands_tiff() {
    echo "============================= generate_bands_tiff() ==================================="
    for WFILE in ${DOWNLOAD_FOLDER}/*.gt
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
        for i in ${!GFS_BANDS_NAMES[@]}; do
            FOLDER_NAME="${FOLDER_NAME//"${GFS_BANDS_NAMES[$i]}_"}"
        done

        mkdir -p $TIFF_TEMP_FOLDER/$FOLDER_NAME
        gdal_translate $WFILE $TIFF_TEMP_FOLDER/$FOLDER_NAME/${FILE_NAME}.tiff -ot Float32 -stats | sleep 1
    done
}


join_tiff_files() {
    echo "============================ join_tiff_files() ===================================="
    MODE=$1
    local BANDS_NAMES=()
    local BANDS_DESCRIPTIONS=()
    if [[ $MODE == "$GFS" ]]; then
        BANDS_NAMES=("${GFS_BANDS_NAMES[@]}")  
        BANDS_DESCRIPTIONS=("${GFS_BANDS[@]}")  
    elif [[ $MODE == "$ECMWF" ]]; then
        BANDS_NAMES=("${ECMWF_BANDS_NAMES[@]}")  
        BANDS_DESCRIPTIONS=("${ECMWF_BANDS[@]}")  
    fi

    mkdir -p $TIFF_FOLDER/
    cd $TIFF_TEMP_FOLDER
    for CHANNELS_FOLDER in *
    do
        if [ ! -d "$CHANNELS_FOLDER" ]; then
            echo "Directory $CHANNELS_FOLDER not exist. Skip"
            continue
        fi
        cd $CHANNELS_FOLDER

        # Create channels list in correct order
        touch settings.txt
        local ALL_CHANNEL_FILES_EXISTS=1
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
        gdal_translate bigtiff.vrt ../../$TIFF_FOLDER/$CHANNELS_FOLDER.tiff -ot Float32 -stats | sleep 1
        
        # # Write tiff layers names
        local BANDS_RENAMING_COMMAND='python "$THIS_LOCATION"/set_band_desc.py ../../$TIFF_FOLDER/$CHANNELS_FOLDER.tiff '
        for i in ${!BANDS_DESCRIPTIONS[@]}; do
            NUMBER=$(( $i + 1))
            DESCRIPTION="${BANDS_DESCRIPTIONS[$i]}"
            BANDS_RENAMING_COMMAND+=$NUMBER
            BANDS_RENAMING_COMMAND+=' "'
            BANDS_RENAMING_COMMAND+=$DESCRIPTION
            BANDS_RENAMING_COMMAND+='" '
        done
        eval $BANDS_RENAMING_COMMAND

        rm settings.txt
        rm bigtiff.vrt
        cd ..
    done
    cd ..
}


split_tiles() {
    echo "=============================== split_tiles() ======================================="
    cd $TIFF_FOLDER
    local SPLIT_ZOOM_TIFF=4
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

        rm ${JOINED_TIFF_NAME}.tiff.gz || true
        gzip --keep ${JOINED_TIFF_NAME}.tiff
    done
    cd ..
}


find_latest_ecmwf_forecat_date() {
    local SEARCH_MODE=$1
    local FORECAST_DATE=""
    local FORECAST_RND_TIME=""
    local HOURS_INCREMENT=12
    local MAX_HOURS_SEARCHING=5*24

    for (( HOURS_OFFSET=0; HOURS_OFFSET<=${MAX_HOURS_SEARCHING}; HOURS_OFFSET+=${HOURS_INCREMENT} ))
    do
        local SEARCHING_DATE=""
        local SEARCHING_HOURS=""
        if [[ $OS =~ "Darwin" ]]; then
            SEARCHING_DATE=$(date -u -v-$(($HOURS_OFFSET))H '+%Y%m%d')
            SEARCHING_HOURS=$(date -u -v-$(($HOURS_OFFSET))H '+%H')
        else
            SEARCHING_DATE=$(date -u '+%Y%m%d' -d "-${HOURS_OFFSET} hours")
            SEARCHING_HOURS=$(date -u '+%-H' -d "-${HOURS_OFFSET} hours")
        fi

        local SEARCHING_RND_HOURS="00"
        if [[ $SEARCHING_HOURS > 11 ]]; then
            SEARCHING_RND_HOURS="12"
        fi 

        local CHECKING_FORECAST_TIME="0"
        if [[ $SEARCH_MODE =~ $FULL_MODE ]]; then
            CHECKING_FORECAST_TIME="240"
        fi

        # https://data.ecmwf.int/forecasts/20220909/00z/0p4-beta/oper/20220909000000-0h-oper-fc.index
        # https://data.ecmwf.int/forecasts/20220909/12z/0p4-beta/oper/20220909000000-0h-oper-fc.index
        local CHECKING_FILE_URL="https://data.ecmwf.int/forecasts/"$SEARCHING_DATE"/"$SEARCHING_RND_HOURS"z/0p4-beta/oper/"$SEARCHING_DATE"000000-"$CHECKING_FORECAST_TIME"h-oper-fc.index"
        local HEAD_RESPONSE=$(curl -s -I $CHECKING_FILE_URL | head -1)

        if [[ $HEAD_RESPONSE =~ "200" ]]; then
            FORECAST_DATE=$SEARCHING_DATE
            FORECAST_RND_TIME=$SEARCHING_RND_HOURS
            break
        fi
    done

    if [[ $FORECAST_DATE == "" ]]; then
        echo "Error"
        return
    fi

    echo "$FORECAST_DATE $FORECAST_RND_TIME"
    return 
}


get_raw_ecmwf_files() {
    echo "============================ get_raw_ecmwf_files() ======================================="

    if [[ $1 == "Error" ]]; then
        return
    fi
    local FORECAST_DATE=$1
    local FORECAST_RND_TIME=$2

    # Download forecast files
    local MAX_FORECAST_HOURS=240
    local FORECAST_INCREMENT_HOURS=3
    for (( FORECAST_HOUR=0; FORECAST_HOUR<=${MAX_FORECAST_HOURS}; FORECAST_HOUR+=${FORECAST_INCREMENT_HOURS} ))
    do
        local FILETIME=""
        if [[ $OS =~ "Darwin" ]]; then
            FILETIME=$(date -ju -v+${FORECAST_HOUR}H -f '%Y%m%d %H%M' '+%Y%m%d_%H%M' "${FORECAST_DATE} ${FORECAST_RND_TIME}00")
        else
            FILETIME=$(date -d "${FORECAST_DATE} ${FORECAST_RND_TIME}00 +${FORECAST_HOUR} hours" '+%Y%m%d_%H%M')
        fi
        local FORECAST_URL_BASE="https://data.ecmwf.int/forecasts/"$FORECAST_DATE"/"$FORECAST_RND_TIME"z/0p4-beta/oper/"$FORECAST_DATE"000000-"$FORECAST_HOUR"h-oper-fc"

        # Download index file
        local INDEX_FILE_URL="$FORECAST_URL_BASE.index"
        download_with_retry "$DOWNLOAD_FOLDER/$FILETIME.index" "$INDEX_FILE_URL"

        # Download needed bands forecast data
        if [[ -f "$DOWNLOAD_FOLDER/$FILETIME.index" ]]; then   
            for i in ${!ECMWF_BANDS_NAMES[@]}; do
                # Parse from index file start and end byte offset for needed band
                local CHANNEL_LINE=$( cat $DOWNLOAD_FOLDER/$FILETIME.index | grep -A 0 "${ECMWF_BANDS_NAMES[$i]}" )
                local BYTE_START=$( echo $CHANNEL_LINE | awk -F "offset" '{print $2}' | awk '{print $2}' | awk -F "," '{print $1}' | awk -F "}" '{print $1}' ) 
                local BYTE_LENGTH=$( echo $CHANNEL_LINE | awk -F "length" '{print $2}' | awk '{print $2}' | awk -F "," '{print $1}' | awk -F "}" '{print $1}' ) 
                local BYTE_END=$(($BYTE_START + $BYTE_LENGTH)) 

                # Make partial download for needed band data only
                # https://data.ecmwf.int/forecasts/20220909/00z/0p4-beta/oper/20220909000000-0h-oper-fc.grib2
                local SAVING_FILENAME="${ECMWF_BANDS_NAMES[$i]}_$FILETIME"
                download_with_retry "$DOWNLOAD_FOLDER/$SAVING_FILENAME.grib2" "$FORECAST_URL_BASE.grib2" $BYTE_START $BYTE_END

                # Generate tiff for downloaded band
                mkdir -p "$TIFF_TEMP_FOLDER/$FILETIME"
                gdal_translate "$DOWNLOAD_FOLDER/$SAVING_FILENAME.grib2" "$TIFF_TEMP_FOLDER/$FILETIME/$SAVING_FILENAME.tiff" -ot Float32 -stats | sleep 1
            done
        else
            echo "Error: Index file not downloaded. Skip downloading weather data."
        fi

        if [[ $DEBUG_M0DE == 1 ]]; then
            return
        fi    
    done
}



# # Uncomment for fast debug mode
# DEBUG_M0DE=1


if [[ $SCRIPT_PROVIDER_MODE == $GFS ]]; then
    cd "$ROOT_FOLDER/$GFS"
    setup_folders_on_start
    get_raw_gfs_files 0 $HOURS_1H_TO_DOWNLOAD 1
    get_raw_gfs_files $HOURS_1H_TO_DOWNLOAD $HOURS_3H_TO_DOWNLOAD 3
    join_tiff_files $GFS
    split_tiles
    clean_temp_files_on_finish
elif [[ $SCRIPT_PROVIDER_MODE == $ECMWF ]]; then
    cd "$ROOT_FOLDER/$ECMWF"
    setup_folders_on_start

    # Find and download latest full forecast (from 0h to 240h)
    FULL_FORECAST_SEARCH_RESULT=$(find_latest_ecmwf_forecat_date $FULL_MODE)
    get_raw_ecmwf_files $FULL_FORECAST_SEARCH_RESULT

    # Find the most latest forecast folder. (But it can be not full yet. From 0h to 9h, by example). 
    # Overrite "yesterday's" full forecatst with all existing "today's" files. If it needed.
    LATEST_FORECAST_SEARCH_RESULT=$(find_latest_ecmwf_forecat_date $LATEST_MODE)
    if [[ $LATEST_FORECAST_SEARCH_RESULT != $FULL_FORECAST_SEARCH_RESULT ]]; then
        get_raw_ecmwf_files $LATEST_FORECAST_SEARCH_RESULT
    fi

    join_tiff_files $ECMWF
    split_tiles
    clean_temp_files_on_finish
fi
