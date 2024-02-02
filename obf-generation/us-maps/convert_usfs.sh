#!/bin/bash
#set -x
dir=$(pwd)
workdir=/mnt/wd_2tb/USFS
sourcedirname=source
sourceoriginaldirname=source_original
tmpdirname=tmp
outosmdirname=osm
ogr2osmdirname=ogr2osm
aggregate_rec_area_activities_template_name=aggregate_rec_area_activities_template
mapcreatordir=/home/xmd5a/utilites/OsmAndMapCreator-main/

trailsshpfilename="S_USA.TrailNFS_Publish"
roadsshpfilename="S_USA.RoadCore_FS"
#recreation_sites_shpfilename="S_USA.Rec_RecreationSitesINFRA"
rec_area_activities_source_filename="S_USA.RECAREAACTIVITIES_V"

cd $workdir/$tmpdirname
if [[ $? == 0 ]] ; then
	rm -f $workdir/$tmpdirname/*.*
fi

export sourcedirname
export tmpdirname
export workdir
export outosmdirname
export ogr2osmdirname
export dir

function generate_osm_from_shp {
	ogr2ogr $workdir/$tmpdirname/$1.shp $workdir/$sourcedirname/$1.shp -explodecollections
	cd $dir/$ogr2osmdirname
	python3 -m ogr2osm --suppress-empty-tags $workdir/$tmpdirname/$1.shp -o $workdir/$outosmdirname/us_$2.osm -t $2.py
}

function generate_osm_from_gdb {
	cd $dir/$ogr2osmdirname
	python3 -m ogr2osm --suppress-empty-tags $workdir/$sourcedirname/${rec_area_activities_source_filename}_agg.gpkg -o $workdir/$outosmdirname/us_$2.osm -t $2.py
}

# if [ ! -f "$workdir/$outosmdirname/us_nfs_trails.osm" ] ; then
# 	echo ==============Generating $trailsshpfilename
# 	generate_osm_from_shp "$trailsshpfilename" "nfs_trails"
# fi
# if [ ! -f "$workdir/$outosmdirname/us_nfs_roads.pbf" ] ; then
# 	echo ==============Generating $roadsshpfilename
# 	generate_osm_from_shp "$roadsshpfilename" "nfs_roads"
# 	osmconvert $workdir/$outosmdirname/us_nfs_roads.osm --out-pbf > $workdir/$outosmdirname/us_nfs_roads.pbf
# 	rm $workdir/$outosmdirname/us_nfs_roads.osm
# fi

if [ ! -f "$workdir/$outosmdirname/us_rec_area_activities.osm" ] ; then
	echo ==============Generating $rec_area_activities_source_filename
	if [[ -f $workdir/$sourcedirname/$rec_area_activities_source_filename.gpkg ]] ; then
		rm $workdir/$sourcedirname/$rec_area_activities_source_filename.gpkg
	fi
	if [[ -f $workdir/$tmpdirname/${rec_area_activities_source_filename}_agg.gpkg ]] ; then
		rm $workdir/$tmpdirname/${rec_area_activities_source_filename}_agg.gpkg
	fi
	ogr2ogr $workdir/$tmpdirname/$rec_area_activities_source_filename.gpkg $workdir/$sourceoriginaldirname/$rec_area_activities_source_filename.gdb.zip -explodecollections
	cp $dir/$aggregate_rec_area_activities_template_name.py $workdir/$tmpdirname/
	sed -i "s,INPUT_FILE,$workdir/$tmpdirname/$rec_area_activities_source_filename.gpkg,g" $workdir/$tmpdirname/$aggregate_rec_area_activities_template_name.py
	sed -i "s,OUTPUT_FILE,$workdir/$sourcedirname/${rec_area_activities_source_filename}_agg.gpkg,g" $workdir/$tmpdirname/$aggregate_rec_area_activities_template_name.py
	python3 $workdir/$tmpdirname/$aggregate_rec_area_activities_template_name.py
	generate_osm_from_gdb "$rec_area_activities_source_filename" "rec_area_activities"
fi

cd $mapcreatordir

bash utilities.sh generate-obf-files-in-batch $dir/batch-usfs.xml
