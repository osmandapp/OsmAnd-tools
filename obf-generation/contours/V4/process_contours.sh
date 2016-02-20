#!/bin/bash
# Converts *.shp.gz in parent dir to osm.bz2
#for x in *.shp.gz; do
#	gunzip $x
#done
function parse_coords {
	XM=1;
	STR=$1;
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
	echo "$RES"
}

shp_count=$(ls -1 ../*.shp 2>&1 | wc -l)
for shp in ../*.shp; do
	outfile=${shp#*_}
	outfile=${outfile%.*}
	outfile=$(parse_coords $outfile)
	echo
	echo "Processing" $shp "to" $outfile
 	if [ ! -f "../$outfile.osm.bz2" ] ; then
 		bz2_count=$(ls -1 ../*.bz2 2>&1 | wc -l)
 		time ./ogr2osm.py $shp -o ../$outfile.osm -t contours.py && bzip2 ../$outfile.osm
 		echo $(basename $shp) "  " $(echo $shp_count $bz2_count | awk '{ print ($2/$1)*100 }') % \| $bz2_count "from" $shp_count
 	fi
	
done
