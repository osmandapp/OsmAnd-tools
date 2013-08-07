# This is a copy of the Jenkins GenerateAddressIndexes job

WORKDIR=".work"
rm -rf ${WORKDIR} 
mkdir -p ${WORKDIR} ${WORKDIR}/osm ${WORKDIR}/gen ${WORKDIR}/index

cd ${WORKDIR}

countries="europe/british-isles europe/france europe/germany europe/italy north-america/canada"
# test with 2 small countries
#countries="europe/azores europe/albania"

for country in ${countries}; do
    basecountry=$(basename ${country})
    echo "${country} and the basename ${basecountry}"
    # Download countries
    wget -O ${basecountry}.osm.pbf -nv "http://download.geofabrik.de/${country}-latest.osm.pbf"
    # convert to fastest intermediate format
    osmconvert --drop-author ${basecountry}.osm.pbf --out-o5m -o=${basecountry}.o5m
    # filter only the necessary stuff out of the entire osm file
    osmfilter ${basecountry}.o5m --keep="boundary=administrative addr:* place=* is_in=* highway=residential =unclassified =pedestrian =living_street =service =road =unclassified =tertiary" --keep-ways-relations="boundary=administrative" --keep-ways= --keep-nodes= --keep-relations= --out-o5m > ${basecountry}_address.o5m
    # convert back to format suitable for OsmAndMapCreator
    osmconvert ${basecountry}_address.o5m --out-pbf -o=osm/${basecountry}_address.osm.pbf
    # delete original .osm.pbf and intermediate .o5m files
    rm -f ${basecountry}.osm.pbf ${basecountry}.o5m ${basecountry}_address.o5m
done

# Do a separate set of actions for the Russia map downloaded from github
 wget -nv http://gis-lab.info/projects/osm_dump/dump/latest/RU.osm.pbf
 osmconvert --drop-author RU.osm.pbf --out-o5m -o=RU.o5m
 osmfilter RU.o5m --keep="boundary=administrative addr:* place=* is_in=* highway=residential =unclassified =pedestrian =living_street =service =road =unclassified =tertiary" --keep-ways-relations="boundary=administrative" --keep-ways= --keep-nodes= --keep-relations= --out-o5m > russia_address.o5m
 osmconvert russia_address.o5m --out-pbf -o=osm/russia_address.osm.pbf
 rm -rf RU.osm.pbf RU.o5m russia_address.o5m


cd ..

echo "now starting OsmAndMapCreator to create the address maps" 
java -XX:+UseParallelGC -Xmx8096M -Xmn512M -Djava.util.logging.config.file=tools/obf-generation/batch-logging.properties -cp "tools/OsmAndMapCreator/OsmAndMapCreator.jar:tools/OsmAndMapCreator/lib/*.jar" net.osmand.data.index.IndexBatchCreator tools/obf-generation/indexes-address-batch-generate-inmem.xml

