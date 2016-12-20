#!/bin/bash
# Renames GeoTIFF from "out_-5--200.tif" (http://www.opensnowmap.org/download/dem_tar/dem_tar.tar) to "N43S213.tif"
function parse_coords {
	XM=1;
	STR=$1;
 	STR=${STR#out_}
	if [ "-" == ${STR:0:1} ]; then
		XM=-1;
		STR=${STR:1}
		RES="S"
	else
		RES="N"
	fi
	X=${STR%%-*}
	if [[ $XM == "-1" ]]; then
		X=$(( $X + 1 ))
	fi
	if [[ $XM == "1" ]] && [[ $X -gt "0" ]]; then
		X=$(( ${X} - 1 ));
		FLAG=1;
	fi
	if [[ $XM == "1" ]] && [[ $X == "0" ]] && [[ $FLAG -ne "1" ]]; then
		RES="S";
		X=$(( ${X} + 1 ));
	fi

	if (( $X < 10)); then
		X=0$X
	fi
	RES="${RES}$X"
	STR=${STR#*-}
	YM=1
	if [ "-" == ${STR:0:1} ]; then
		YM=-1;
		STR=${STR:1}
		RES="${RES}W"
	else
		RES="${RES}E"
	fi
	Y=$STR
	if (( $Y < 10)); then
		RES="${RES}0"
	fi
	if (( $Y < 100)); then
		RES="${RES}0"
	fi
	RES="${RES}$Y"
	echo "$RES.tif"
}

for tif in *.tif; do
	outfile=${tif#*_}
	outfile=${outfile%.*}
	outfile=$(parse_coords $outfile)
	echo "$tif" "$outfile"
	mv "$(pwd)/$tif" "$outfile"
	
done