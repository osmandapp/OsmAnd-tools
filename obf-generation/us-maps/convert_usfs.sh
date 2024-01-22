#!/bin/bash
#set -x
dir=$(pwd)
workdir=/mnt/wd_2tb/USFS
shapedirname=shp
tmpdirname=tmp
outosmdirname=input
ogr2osmdirname=ogr2osm
trailsshpfilename="S_USA.TrailNFS_Publish"
roadsshpfilename="S_USA.RoadCore_FS"

mapcreatordir=/home/xmd5a/utilites/OsmAndMapCreator-main/

export shapedirname
export tmpdirname
export workdir
export outosmdirname
export ogr2osmdirname
export dir

function generate_osm {
	ogr2ogr $workdir/$tmpdirname/$1.shp $workdir/$shapedirname/$1.shp -explodecollections
	cd $dir/$ogr2osmdirname
	python3 -m ogr2osm $workdir/$tmpdirname/$1.shp -o $workdir/$outosmdirname/us.$2.osm -t $2.py
}

if [ ! -f "$workdir/$outosmdirname/us.nfs_trails.osm" ] ; then
	echo ==============Generating $trailsshpfilename
	generate_osm "$trailsshpfilename" "nfs_trails"
fi
if [ ! -f "$workdir/$outosmdirname/us.nfs_roads.pbf" ] ; then
	echo ==============Generating $roadsshpfilename
	generate_osm "$roadsshpfilename" "nfs_roads"
	osmconvert $workdir/$outosmdirname/us.nfs_roads.osm --out-pbf > $workdir/$outosmdirname/us.nfs_roads.pbf
	rm $workdir/$outosmdirname/us.nfs_roads.osm
fi

cd $mapcreatordir

bash utilities.sh generate-obf-files-in-batch $dir/batch-usfs.xml
