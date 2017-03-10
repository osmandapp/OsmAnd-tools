#!/bin/bash
query_overpass () {
	echo Downloading admin_level_$1
	curl 'http://builder.osmand.net:8081/api/interpreter' -H 'Accept: */*' -H 'Content-Type: application/x-www-form-urlencoded; charset=UTF-8' --data "data=%5Bout%3Axml%5D%5Btimeout%3A925%5D%3B%0A(%0A++relation%5B%22admin_level%22%3D%22$1%22%5D%5B%22type%22%3D%22boundary%22%5D%3B%0A++way%5B%22admin_level%22%3D%22$1%22%5D%5B%22type%22%3D%22boundary%22%5D%3B%0A)%3B%0Aout+body%3B%0A%3E%3B%0Aout+skel+qt%3B" --compressed > admin_level_$1.osm
}

query_overpass 2
#query_overpass 3
query_overpass 4

remove_tags () {
	osmfilter admin_level_$1.osm --keep-tags="all admin_level=*" --out-osm > admin_level_$1_flt.osm
}
remove_tags 2
#remove_tags 3
remove_tags 4