BASE_URL="https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod/"
PROVIDER="gfs"
LAYER="atmos"
DATE=$(date +"%Y%m%d")
DOWNLOAD_URL="$BASE_URL$PROVIDER.$DATE"
FILE_FORECAST_PATTERN="gfs\.t..z\.pgrb2\.0p25\.f..."
FILE_FORECAST_PATTERN_IDX="gfs\.t..z\.pgrb2\.0p25\.f...\.idx"
#need forecast for minimum 24+6=030 hours
FILE_FORECAST_PATTERN_24="gfs\.t..z\.pgrb2\.0p25\.f03*"
FILE_PART1="gfs.t"
FILE_PART2="z.pgrb2.0p25.f"
OS=$(uname -a)
TIME_ZONE="GMT"
BANDS=("TCDC:entire atmosphere" "TMP:2 m above ground" "PRMSL:mean sea level" "GUST:surface" "PRATE:surface")
BANDS_DIR="cloud temperature pressure wind precip"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# folder structure
# band/YYYYmmdd/hh.tiff
# GUST:surface/20211206/15.tiff

# band/YYYYmmdd/hh/{z}/{x}/{y}.png
# GUST:surface/20211206/15/{z}/{x}/{y}.png

LAST_HOURLY_LINK=""
LAST_FORECAST=""

#https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod/gfs.20211207/00/atmos/gfs.t00z.pgrb2.0p25.f000
get_0_24() {
    local current_timestamp=$(TZ=GMT date +"%s")
    local last_hour_string=$(echo $LAST_HOURLY_LINK | awk -F / '{print $10"."$11}' | awk -F '.' '{print $2" "$3}')
    local last_hour=$(echo $LAST_HOURLY_LINK | awk -F / '{print $10"."$11}' | awk -F '.' '{print $3}')
    last_hour_timestamp=0


    if [[ $OS =~ "Darwin" ]]; then
        last_hour_timestamp=$(TZ=$TIME_ZONE date -j -f "%Y%m%d %H" "$last_hour_string" "+%s")
    fi
    if [[ $OS =~ "Linux" ]]; then
        last_hour_timestamp=$(TZ=$TIME_ZONE date -d "$last_hour_string" "+%s")
    fi

    if [ $last_hour_timestamp -eq 0 ]; then
        echo -en "${RED}FAIL. Unpossible to determine time for download${NC}"
        exit
    fi

    local diff_time=$(($current_timestamp - $last_hour_timestamp))
    diff_time=$(($diff_time/3600))
    to_24=$(($diff_time + 24))
    for (( c=$diff_time; c<=$to_24; c++ ))
    do
        local h=$c
        if [ $c -lt 10 ]; then
            h="00$h"
        elif [ $c -lt 100 ]; then
            h="0$h"
        fi
        local file_link="$LAST_HOURLY_LINK$LAYER/$FILE_PART1$last_hour$FILE_PART2$h"
        local file_link_id="$LAST_HOURLY_LINK$LAYER/$FILE_PART1$last_hour$FILE_PART2$h.idx"
        local file_timestamp=$(($last_hour_timestamp + c * 3600))
        local file_folder=""
        if [[ $OS =~ "Darwin" ]]; then
            file_folder=$(TZ=$TIME_ZONE date -r $file_timestamp "+%Y%m%d/%H")
        fi
        if [[ $OS =~ "Linux" ]]; then
            file_folder=$(TZ=GMT date -d @$file_timestamp "+%Y%m%d/%H")
        fi
        if [ -z "$file_folder" ]; then
            echo -en "${RED}FAIL. Unpossible to determine folder for timestamp $file_timestamp${NC}"
            exit
        fi
        mkdir -p "tmp/$file_folder"
        if [[ $LAST_FORECAST =~ "$file_link" && $LAST_FORECAST =~ "$file_link_id" ]]; then
            wget $file_link_id --timeout=900 -P tmp/$file_folder
            if [[ $? -ne 0 ]]; then
                echo -en "${RED}$file_link_id not downloaded${NC}"
            else
                echo -en "${GREEN}$file_link_id downloaded${NC}"
            fi

            wget $file_link --timeout=900 -P tmp/$file_folder
            if [[ $? -ne 0 ]]; then
                echo -en "${RED}$file_link not downloaded${NC}"
            else
                echo -en "${GREEN}$file_link downloaded${NC}"
            fi
        else
            echo -en "${RED}FAIL. Forecast data is not full $LAST_HOURLY_LINK${NC}"
            exit
        fi
    done
}
         
get_bands_tiff() {
    local file_list=$(find tmp)
    for WFILE in $file_list
    do
        if [[ $WFILE =~ $FILE_FORECAST_PATTERN_IDX ]]; then
            continue
        fi
        if [[ $WFILE =~ $FILE_FORECAST_PATTERN ]]; then
            band_numbers=""
            for i in ${!BANDS[@]}; do
                local b_num=$(cat $WFILE.idx | grep "${BANDS[$i]}" | awk 'NR==1{print $1}' | awk -F ":" '{print $1}')
                band_numbers="$band_numbers -b $b_num"
            done
            local b_dir=$(echo $WFILE | awk -F / '{print "/"$2"/"$3"/"}')
            b_dir="tiff/$b_dir"
            mkdir -p $b_dir
            gdal_translate $band_numbers -mask "none" $WFILE ${b_dir}world.tiff
        fi
    done
}


#get_bands_tiff
#exit

INDEX_PAGE=$(lynx -dump -listonly $DOWNLOAD_URL | sort -r)
for HOURLY_LINK in $INDEX_PAGE
do
    if [[ $HOURLY_LINK =~ "https://"  && "$HOURLY_LINK" != "$BASE_URL" ]]; then
        HOURLY_LINK_PAGE=$(lynx -dump -listonly $HOURLY_LINK$LAYER)
        FORECAST_PRESENT=0
        for WFILE in $HOURLY_LINK_PAGE
        do
            if [[ $WFILE =~ $FILE_FORECAST_PATTERN_24 ]]; then
                FORECAST_PRESENT=1
                break
            fi
        done
        if [ $FORECAST_PRESENT -eq 1 ]; then
            echo -en "${GREEN}Found forecast in $HOURLY_LINK${NC}"
            LAST_FORECAST="${HOURLY_LINK_PAGE}"
            LAST_HOURLY_LINK=$HOURLY_LINK
            break
        else
            echo -en "${YELLOW}No forecast present in $HOURLY_LINK${NC}"
        fi
    fi
done

#echo "get_0_24"
get_0_24
get_bands_tiff
#rm -rf tmp/

#echo "LAST_HOURLY_LINK " $LAST_HOURLY_LINK
#echo "LAST_HOUR " $LAST_HOUR
