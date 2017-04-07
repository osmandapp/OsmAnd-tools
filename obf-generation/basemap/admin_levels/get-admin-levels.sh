#!/bin/bash
work_dir=$(echo $(pwd))
query_overpass () {
	echo Downloading admin_level_$1
	curl 'http://builder.osmand.net:8081/api/interpreter' -H 'Accept: */*' -H 'Content-Type: application/x-www-form-urlencoded; charset=UTF-8' --data "data=%5Bout%3Axml%5D%5Btimeout%3A925%5D%3B%0A(%0A++way%5B%22admin_level%22%3D%22$1%22%5D%5B%22type%22%3D%22boundary%22%5D%5B%22boundary%22!%3D%22aboriginal_lands%22%5D%5B%22boundary%22!%3D%22disputed%22%5D%5B%22dispute%22!%3D%22yes%22%5D%5B%22disputed%22!%3D%22yes%22%5D%5B%22disputed_by%22!~%22.*%22%5D%5B%22boundary%22!%3D%22maritime%22%5D%3B%0A++relation%5B%22admin_level%22%3D%22$1%22%5D%5B%22type%22%3D%22boundary%22%5D%5B%22boundary%22!%3D%22aboriginal_lands%22%5D%5B%22boundary%22!%3D%22disputed%22%5D%5B%22dispute%22!%3D%22yes%22%5D%5B%22disputed%22!%3D%22yes%22%5D%5B%22disputed_by%22!~%22.*%22%5D%5B%22boundary%22!%3D%22maritime%22%5D%3B%0A)%3B%0Aout+body%3B%0A%3E%3B%0Aout+skel+qt%3B" --compressed > admin_level_$1.osm
}


# [out:xml][timeout:925];
# (
#   way["admin_level"="2"]["type"="boundary"]["boundary"!="aboriginal_lands"]["boundary"!="disputed"]["dispute"!="yes"]["disputed"!="yes"]["disputed_by"!~".*"]["boundary"!="maritime"];
#   relation["admin_level"="2"]["type"="boundary"]["boundary"!="aboriginal_lands"]["boundary"!="disputed"]["dispute"!="yes"]["disputed"!="yes"]["disputed_by"!~".*"]["boundary"!="maritime"];
# );
# out body;
# >;
# out skel qt;

clean () {
	osmfilter admin_level_$1.osm --keep-tags="all no" --drop-relations --fake-author --out-osm > admin_level_$1_flt.osm
	osmosis --read-xml file="admin_level_${1}_flt.osm" --sort type="TypeThenId" --write-xml file="admin_level_${1}_flt_sorted.osm" && mv -f admin_level_${1}_flt_sorted.osm admin_level_${1}_flt.osm

}
add_admin_level_tag () {
	sed "s/<\/way>/\t<tag k=\"admin_level\" v=\"$1\"\/>\n\t<\/way>/g" admin_level_$1_flt.osm > admin_level_$1_flt.osm_new && mv -f admin_level_$1_flt.osm_new admin_level_$1_flt.osm
}
fix_basemap_roads () {
	cd /home/xmd5a/utilites/OsmAndMapCreator-main
	java -XX:+UseParallelGC -Xmx18096M -Xmn512M -Djava.util.logging.config.file=/home/xmd5a/git/OsmAnd-tools/obf-generation/batch-logging.properties -cp "./OsmAndMapCreator.jar:./lib/OsmAnd-core.jar:./lib/*.jar:./lib-gl/*.jar" net.osmand.osm.util.FixBasemapRoads $work_dir/admin_level_$1_flt.osm $work_dir/proc_line_admin_level_$1_out.osm
	cd $work_dir

}

#query_overpass 2
#query_overpass 4
#clean 2
#clean 4
#add_admin_level_tag 2
#add_admin_level_tag 4
#fix_basemap_roads 2
fix_basemap_roads 4

#osmconvert proc_*.osm > proc_line_admin_level_out.osm
#bzip2 proc_line_admin_level_out.osm
