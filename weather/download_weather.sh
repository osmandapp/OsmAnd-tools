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
LATEST_MODE='latest_mode'

GFS_BANDS_FULL_NAMES=("TCDC:entire atmosphere" "TMP:2 m above ground" "PRMSL:mean sea level" "GUST:surface" "PRATE:surface" "UGRD:planetary boundary" "VGRD:planetary boundary")
GFS_BANDS_SHORT_NAMES=("cloud" "temperature" "pressure" "wind" "precip" "windspeed_u" "windspeed_v")

ECMWF_BANDS_FULL_NAMES=("TMP:2 m above ground" "PRMSL:mean sea level" "PRATE:surface" "UGRD:planetary boundary" "VGRD:planetary boundary")
ECMWF_BANDS_SHORT_NAMES_ORIG=("2t" "msl" "tp" "10u" "10v")
ECMWF_BANDS_SHORT_NAMES_SAVING=("temperature" "pressure" "precip" "windspeed_u" "windspeed_v")

MINUTES_TO_KEEP_TIFF_FILES=${MINUTES_TO_KEEP_TIFF_FILES:-3600} # 60 hours (temporary due ECMWF old data available, default 1800 - 30 hours)
HOURS_1H_TO_DOWNLOAD=${HOURS_1H_TO_DOWNLOAD:-36}
HOURS_3H_TO_DOWNLOAD=${HOURS_3H_TO_DOWNLOAD:-192}

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
    sleep 5
    find . -type f -mmin +${MINUTES_TO_KEEP_TIFF_FILES} -delete  || echo "Error: Temp file is already deleted"
    find . -type d -empty -delete  || echo "Error: Temp file is already deleted"
}


should_download_file() {
    local FILENAME=$1
    local URL=$2

    if [[ -f $FILENAME ]]; then
        # File is already dowlnloaded
        local DISK_FILE_MODIFIED_TIME="$(TZ=GMT date -r ${FILENAME} +'%a, %d %b %Y %H:%M:%S GMT')"
        local SERVER_RESPONSE=$(curl -s -I -L --header "If-Modified-Since: $DISK_FILE_MODIFIED_TIME" $URL | head -1)

        if [[ $SERVER_RESPONSE =~ "302" ]]; then
            # Maybe server is blocking us and redirectding to Error Html page. Like this:
            # https://www.weather.gov/abusive-user-block
            sleep 300
            echo 0
            return
        elif [[ $SERVER_RESPONSE =~ "304" ]]; then
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
    elif [[ $(curl -s -I -L $URL | head -1) =~ "404"  ]]; then   
        # File not found. Skip 
        echo 0
        return    
    elif [[ $(curl -s -I -L $URL | head -1) =~ "302"  ]]; then   
        # Maybe server is blocking us and redirectding to Error Html page. Like this:
        # https://www.weather.gov/abusive-user-block 
        # Try to wait a bit and download again.
        sleep 300
        echo 1
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
        curl -k -L $URL --output ${FILENAME} --http1.1   || echo "Error of CURL with downloading $URl"
    else
        # download part file by byte offset
        curl -k -L --range $START_BYTE_OFFSET-$END_BYTE_OFFSET $URL --output ${FILENAME} --http1.1  || echo "Error of CURL with partial downloading $URl"
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
        echo "Download Error: ${FILENAME} not downloaded! Wait 1 min and retry."
        sleep 60
        echo "Download try 2: ${FILENAME}"
        download $FILENAME $URL $START_BYTE_OFFSET $END_BYTE_OFFSET
    else 
        echo "Downloading success with try 1: ${FILENAME}"   
        return    
    fi

    # if [[ $( should_download_file "$FILENAME" "$URL" ) -eq 1 ]]; then
    #     echo "Download Error: ${FILENAME} not downloaded! Wait 5 min and retry."
    #     sleep 300
    #     echo "Download try 3: ${FILENAME}"
    #     download $FILENAME $URL $START_BYTE_OFFSET $END_BYTE_OFFSET
    # else 
    #     echo "Downloading success with try 2: ${FILENAME}"   
    #     return    
    # fi

    # if [[ $( should_download_file "$FILENAME" "$URL" ) -eq 1 ]]; then
    #     echo "Download Error: ${FILENAME} not downloaded! Wait 10 min and retry."
    #     sleep 600
    #     echo "Download try 4: ${FILENAME}"
    #     download $FILENAME $URL $START_BYTE_OFFSET $END_BYTE_OFFSET
    # else 
    #     echo "Downloading success with try 3: ${FILENAME}"   
    #     return    
    # fi

    # if [[ $( should_download_file "$FILENAME" "$URL" ) -eq 1 ]]; then
    #     echo "Download Error: ${FILENAME} not downloaded! Wait 1 hour and retry."
    #     sleep 600
    #     echo "Download try 5: ${FILENAME}"
    #     download $FILENAME $URL $START_BYTE_OFFSET $END_BYTE_OFFSET
    # else 
    #     echo "Downloading success with try 4: ${FILENAME}"   
    #     return    
    # fi
    
    if [[ $( should_download_file "$FILENAME" "$URL" ) -eq 1 ]]; then
        echo "Fatal Download Error: ${FILENAME} still not downloaded!"
    fi

    return
}

is_file_content_with_html() {
    local FILENAME=$1
    if grep -q "<!doctype html>" "$FILENAME"; then
        echo 0
    else
        echo 1    
    fi
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
        HOURS=$(TZ=GMT date -u -v-${DELAY_HOURS}H '+%-H')
        DATE=$(TZ=GMT date -u -v-${DELAY_HOURS}H '+%Y%m%d')
    else
        HOURS=$(TZ=GMT date -u '+%-H' -d "-${DELAY_HOURS} hours")
        DATE=$(TZ=GMT date -u '+%Y%m%d' -d "-${DELAY_HOURS} hours")
    fi
    # Round down HOURS to 0/6/12/18
    local RNDHOURS=$(printf "%02d" $(( $HOURS / 6 * 6 )))
    # local URL_BASE="https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod/gfs.${DATE}/${RNDHOURS}/$LAYER/"
    local URL_BASE="https://s3.amazonaws.com/noaa-gfs-bdp-pds/gfs.${DATE}/${RNDHOURS}/$LAYER/"

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
            FILETIME=$(TZ=GMT date -ju -v+${c}H -f '%Y%m%d %H%M' '+%Y%m%d_%H%M' "${DATE} ${RNDHOURS}00")
        else
            FILETIME=$(TZ=GMT date -d "${DATE} ${RNDHOURS}00 +${c} hours" '+%Y%m%d_%H%M')
        fi


        # Download index file
        cd $DOWNLOAD_FOLDER; 
        sleep 1
        download_with_retry "$FILETIME.index" "$FILE_INDEX_URL"

        # Download needed bands forecast data
        if [[ -f "$FILETIME.index" ]]; then   
            for i in ${!GFS_BANDS_FULL_NAMES[@]}; do
                # Parse from index file start and end byte offset for needed band
                local CHANNEL_INDEX_LINES=$( cat ${FILETIME}.index | grep -A 1 "${GFS_BANDS_FULL_NAMES[$i]}" | awk -F ":" '{print $2}' )
                local START_BYTE_OFFSET=$( echo $CHANNEL_INDEX_LINES | awk -F " " '{print $1}' )
                local END_BYTE_OFFSET=$( echo $CHANNEL_INDEX_LINES | awk -F " " '{print $2}' ) 

                # Make partial download for needed band data only  
                # https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod/gfs.20211207/00/atmos/gfs.t00z.pgrb2.0p25.f000
                sleep 1
                download_with_retry "${GFS_BANDS_SHORT_NAMES[$i]}_$FILETIME.gt" "$FILE_DATA_URL" $START_BYTE_OFFSET $END_BYTE_OFFSET

                if [[ -f "${GFS_BANDS_SHORT_NAMES[$i]}_$FILETIME.gt" ]]; then  
                    if [[ $( is_file_content_with_html "${GFS_BANDS_SHORT_NAMES[$i]}_$FILETIME.gt" ) -eq 1 ]]; then
                        # File is downloaded and is of correct type.
                        # Generate tiff for downloaded band
                        echo "Partial downloading success. Start gdal_translate"
                        mkdir -p "../$TIFF_TEMP_FOLDER/$FILETIME"
                        gdal_translate "${GFS_BANDS_SHORT_NAMES[$i]}_$FILETIME.gt" "../$TIFF_TEMP_FOLDER/$FILETIME/${GFS_BANDS_SHORT_NAMES[$i]}_$FILETIME.tiff" -ot Float32 -stats  || echo "Error of gdal_translate"
                        TZ=UTC touch -t "${DATE}${RNDHOURS}00" "../$TIFF_TEMP_FOLDER/$FILETIME/${GFS_BANDS_SHORT_NAMES[$i]}_$FILETIME.tiff"
                    else
                        echo "Fatal Error: Partial downloaded data contains HTML content. May be we are blocked."
                        cat "${GFS_BANDS_SHORT_NAMES[$i]}_$FILETIME.gt"
                        rm "${GFS_BANDS_SHORT_NAMES[$i]}_$FILETIME.gt"
                        # cd ..;
                        return
                    fi
                else
                    echo "Error: Index file not downloaded. Skip downloading weather data."
                fi
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


join_tiff_files() {
    echo "============================ join_tiff_files() ===================================="
    MODE=$1
    local BANDS_SHORT_NAMES=()
    local BANDS_DESCRIPTIONS=()
    if [[ $MODE == "$GFS" ]]; then
        BANDS_SHORT_NAMES=("${GFS_BANDS_SHORT_NAMES[@]}")  
        BANDS_DESCRIPTIONS=("${GFS_BANDS_FULL_NAMES[@]}")  
    elif [[ $MODE == "$ECMWF" ]]; then
        BANDS_SHORT_NAMES=("${ECMWF_BANDS_SHORT_NAMES_SAVING[@]}")  
        BANDS_DESCRIPTIONS=("${ECMWF_BANDS_FULL_NAMES[@]}")  
    fi

    mkdir -p $TIFF_FOLDER/
    cd $TIFF_TEMP_FOLDER
    for DATE_FOLDER in *
    do
        if [ ! -d "${DATE_FOLDER}" ]; then
            echo "Error: Directory ${DATE_FOLDER} not exist. Skip"
            continue
        fi
        cd $DATE_FOLDER

        # Create channels list in correct order
        touch settings.txt
        local ALL_CHANNEL_FILES_EXISTS=1
        local FILE_DATE_CP
        for i in ${!BANDS_SHORT_NAMES[@]}; do
            if [ ! -f "${BANDS_SHORT_NAMES[$i]}_${DATE_FOLDER}.tiff" ]; then
                ALL_CHANNEL_FILES_EXISTS=0
                break
            fi
            FILE_DATE_CP=${BANDS_SHORT_NAMES[$i]}_${DATE_FOLDER}.tiff
            echo "${BANDS_SHORT_NAMES[$i]}_${DATE_FOLDER}.tiff" >> settings.txt
        done

        if [ $ALL_CHANNEL_FILES_EXISTS == 0 ]; then
            echo "Joining Error:  ${BANDS_SHORT_NAMES[$i]}_${DATE_FOLDER}.tiff  not exists. Skip joining."
            cd ..
            continue
        fi

        # Create "Virtual Tiff" with layers order from settings.txt
        gdalbuildvrt bigtiff.vrt -separate -input_file_list settings.txt
        # Create joined tiff from "Virtual Tiff"
        local TARGET_FILE=../../${TIFF_FOLDER}/${DATE_FOLDER}.tiff
        gdal_translate bigtiff.vrt $TARGET_FILE -ot Float32 -stats  || echo "Error of gdal_translate"
        
        # # Write tiff layers names
        local BANDS_RENAMING_COMMAND='python "$THIS_LOCATION"/set_band_desc.py $TARGET_FILE '
        for i in ${!BANDS_DESCRIPTIONS[@]}; do
            NUMBER=$(( $i + 1))
            DESCRIPTION="${BANDS_DESCRIPTIONS[$i]}"
            BANDS_RENAMING_COMMAND+=$NUMBER
            BANDS_RENAMING_COMMAND+=' "'
            BANDS_RENAMING_COMMAND+=$DESCRIPTION
            BANDS_RENAMING_COMMAND+='" '
        done
        eval $BANDS_RENAMING_COMMAND
        touch -r $FILE_DATE_CP $TARGET_FILE

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
        if [ ! -f "$JOINED_TIFF_NAME" ]; then
            echo "Error: File $JOINED_TIFF_NAME not exist. Skip"
            continue
        fi

        JOINED_TIFF_NAME="${JOINED_TIFF_NAME//".tiff"}"
        echo "JOINED_TIFF_NAME: $JOINED_TIFF_NAME"

        mkdir -p ${JOINED_TIFF_NAME}
        MAXVALUE=$((1<<${SPLIT_ZOOM_TIFF}))
 
        "$THIS_LOCATION"/slicer.py --zoom ${SPLIT_ZOOM_TIFF} --extraPoints 2 ${JOINED_TIFF_NAME}.tiff ${JOINED_TIFF_NAME}/  
        # generate subgeotiffs into folder
        # 1440*720 / (48*48) = 450
        find ${JOINED_TIFF_NAME}/ -name "*.gz" -delete
        find ${JOINED_TIFF_NAME}/ -maxdepth 1 -type f ! -name '*.gz' -exec touch -r ${JOINED_TIFF_NAME}.tiff "{}" \;    
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
            SEARCHING_DATE=$(TZ=GMT date -u -v-$(($HOURS_OFFSET))H '+%Y%m%d')
            SEARCHING_HOURS=$(TZ=GMT date -u -v-$(($HOURS_OFFSET))H '+%-H')
        else
            SEARCHING_DATE=$(TZ=GMT date -u '+%Y%m%d' -d "-${HOURS_OFFSET} hours")
            SEARCHING_HOURS=$(TZ=GMT date -u '+%-H' -d "-${HOURS_OFFSET} hours")
        fi

        local SEARCHING_RND_HOURS="00"
        if [[ $SEARCHING_HOURS -gt 11 ]]; then
            SEARCHING_RND_HOURS="12"
        fi 

        local CHECKING_FORECAST_TIME="0"
        if [[ $SEARCH_MODE =~ $FULL_MODE ]]; then
            CHECKING_FORECAST_TIME="240"
        fi

        # https://data.ecmwf.int/forecasts/20220909/00z/ifs/0p4-beta/oper/20220909000000-0h-oper-fc.index
        # https://data.ecmwf.int/forecasts/20220909/12z/ifs/0p4-beta/oper/20220909000000-0h-oper-fc.index
        local CHECKING_FILE_URL="https://data.ecmwf.int/forecasts/"$SEARCHING_DATE"/"$SEARCHING_RND_HOURS"z/ifs/0p4-beta/oper/"$SEARCHING_DATE$SEARCHING_RND_HOURS"0000-"$CHECKING_FORECAST_TIME"h-oper-fc.index"
        local HEAD_RESPONSE=$(curl -s -I -L $CHECKING_FILE_URL | head -1)

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
    local PREV_FILETIME=""
    for (( FORECAST_HOUR=0; FORECAST_HOUR<=${MAX_FORECAST_HOURS}; FORECAST_HOUR+=${FORECAST_INCREMENT_HOURS} ))
    do
        local FILETIME=""
        if [[ $OS =~ "Darwin" ]]; then
            FILETIME=$(TZ=GMT date -ju -v+${FORECAST_HOUR}H -f '%Y%m%d %H%M' '+%Y%m%d_%H%M' "${FORECAST_DATE} ${FORECAST_RND_TIME}00")
        else
            FILETIME=$(TZ=GMT date -d "${FORECAST_DATE} ${FORECAST_RND_TIME}00 +${FORECAST_HOUR} hours" '+%Y%m%d_%H%M')
        fi
        local FORECAST_URL_BASE="https://data.ecmwf.int/forecasts/"$FORECAST_DATE"/"$FORECAST_RND_TIME"z/ifs/0p4-beta/oper/"$FORECAST_DATE"000000-"$FORECAST_HOUR"h-oper-fc"

        # Download index file
        local INDEX_FILE_URL="$FORECAST_URL_BASE.index"
        download_with_retry "$DOWNLOAD_FOLDER/$FILETIME.index" "$INDEX_FILE_URL"

        # Download needed bands forecast data
        if [[ -f "$DOWNLOAD_FOLDER/$FILETIME.index" ]]; then   
            for i in ${!ECMWF_BANDS_SHORT_NAMES_ORIG[@]}; do
                # Parse from index file start and end byte offset for needed band
                local CHANNEL_LINE=$( cat $DOWNLOAD_FOLDER/$FILETIME.index | grep -A 0 "${ECMWF_BANDS_SHORT_NAMES_ORIG[$i]}" )
                local BYTE_START=$( echo $CHANNEL_LINE | awk -F "offset" '{print $2}' | awk '{print $2}' | awk -F "," '{print $1}' | awk -F "}" '{print $1}' ) 
                local BYTE_LENGTH=$( echo $CHANNEL_LINE | awk -F "length" '{print $2}' | awk '{print $2}' | awk -F "," '{print $1}' | awk -F "}" '{print $1}' ) 
                local BYTE_END=$(($BYTE_START + $BYTE_LENGTH)) 

                # Make partial download for needed band data only
                # https://data.ecmwf.int/forecasts/20220909/00z/ifs/0p4-beta/oper/20220909000000-0h-oper-fc.grib2
                local SAVING_FILENAME="${ECMWF_BANDS_SHORT_NAMES_SAVING[$i]}_$FILETIME"
                download_with_retry "$DOWNLOAD_FOLDER/$SAVING_FILENAME.grib2" "$FORECAST_URL_BASE.grib2" $BYTE_START $BYTE_END
                GRIB_SIZE=$(wc -c "$DOWNLOAD_FOLDER/$SAVING_FILENAME.grib2" | awk '{print $1}')
                if (( $GRIB_SIZE < 5000 )); then
                    echo "Warning! Looks like $SAVING_FILENAME.grib2 is empty or contains invalid data"
                    continue
                fi
                cnvgrib2to1 "$DOWNLOAD_FOLDER/$SAVING_FILENAME.grib2" "$DOWNLOAD_FOLDER/$SAVING_FILENAME.grib1" || echo "cnvgrib2to1 error"
                rm "$DOWNLOAD_FOLDER/$SAVING_FILENAME.grib2"
                # Generate tiff for downloaded band
                mkdir -p "$TIFF_TEMP_FOLDER/$FILETIME"
                local PREV_FILENAME="${ECMWF_BANDS_SHORT_NAMES_SAVING[$i]}_$PREV_FILETIME"
                if [ ${ECMWF_BANDS_SHORT_NAMES_SAVING[$i]} == "precip" ] && [ -n "$PREV_FILETIME" ] && [ -f "$DOWNLOAD_FOLDER/$PREV_FILENAME.grib1" ]; then
                    echo "Calculate precipitation"
                    gdal_calc.py -A "$DOWNLOAD_FOLDER/$SAVING_FILENAME.grib1" -B "$DOWNLOAD_FOLDER/$PREV_FILENAME.grib1" --co COMPRESS=NONE --type=Float32 --outfile="$TIFF_TEMP_FOLDER/$FILETIME/$SAVING_FILENAME.tiff" --calc="(A-B)" --overwrite
                else
                    gdal_translate -outsize 1440 721 -r cubic "$DOWNLOAD_FOLDER/$SAVING_FILENAME.grib1" "$TIFF_TEMP_FOLDER/$FILETIME/$SAVING_FILENAME.tiff" -ot Float32 -stats -colorinterp_1 undefined || echo "gdal_translate error"
                fi
                if [[ ${ECMWF_BANDS_SHORT_NAMES_SAVING[$i]} == "temperature" ]] ; then
                    echo "Converting tmp from K to C"
                    gdal_calc.py -A "$TIFF_TEMP_FOLDER/$FILETIME/$SAVING_FILENAME.tiff" --co COMPRESS=NONE --type=Float32 --outfile="$TIFF_TEMP_FOLDER/$FILETIME/$SAVING_FILENAME.tiff" --calc="A-273" --overwrite
                fi
                TZ=UTC touch -t "${FORECAST_DATE}${FORECAST_RND_TIME}00" "$TIFF_TEMP_FOLDER/$FILETIME/$SAVING_FILENAME.tiff"
            done
            PREV_FILETIME=$FILETIME 
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
    # Overrite yesterday's full forecasts with all existing today's files. If it needed.
    LATEST_FORECAST_SEARCH_RESULT=$(find_latest_ecmwf_forecat_date $LATEST_MODE)
    if [[ $LATEST_FORECAST_SEARCH_RESULT != $FULL_FORECAST_SEARCH_RESULT ]]; then
        get_raw_ecmwf_files $LATEST_FORECAST_SEARCH_RESULT
    fi

    join_tiff_files $ECMWF
    split_tiles
    clean_temp_files_on_finish
fi
