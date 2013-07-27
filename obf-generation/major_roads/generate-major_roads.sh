# This script will create "major roads" maps for some really big countries like Germany, france, Canada, etc.
# It uses osmfilter and osmconvert to prepare the geofabrik .osm.pbf files before they are "fed" to OsmAndMapCreator


#where does this script start from
WORKDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd ${WORKDIR}

#WORKDIR=`pwd`
echo ${WORKDIR}

OSMDIR=${WORKDIR}/osm
GENDIR=${WORKDIR}/gen
INDEXDIR=${WORKDIR}/index/

# small countries to test with
#countries="europe/albania europe/andorra europe/azores"
# real countries
countries="europe/british-isles europe/france europe/germany europe/italy north-america/canada"

# just to be sure: clean up before start
rm -rf ${OSMDIR} ${GENDIR} ${INDEXDIR}
mkdir -p ${OSMDIR} ${GENDIR} ${INDEXDIR}

for country in ${countries}; do
    basecountry=$(basename ${country})
    echo "${country} and the basename ${basecountry}"
    # Download countries
    #wget -O $OSMDIR/${country%latest.osm.pbf}major_roads.osm.pbf "http://download.geofabrik.de/$country.osm.pbf"
    wget -O ${OSMDIR}/${basecountry}.osm.pbf "http://download.geofabrik.de/${country}-latest.osm.pbf"
    # convert to fastest intermediate format
    ~/osmplanet/osmconvert32 --drop-author --drop-brokenrefs ${OSMDIR}/${basecountry}.osm.pbf -o=${OSMDIR}/${basecountry}.o5m
    # filter only the necessary highways out of the entire osm file
    ~/osmtools/osmfilter ${OSMDIR}/${basecountry}.o5m  --keep="highway=motorway =motorway_link =trunk =trunk_link =primary =primary_link =secondary =secondary_link place=city =town =village" --keep-relations= --drop-tags=    > ${OSMDIR}/${basecountry}-major_roads.o5m
    # convert back to  format suitable for OsmAndMapCreator
    ~/osmplanet/osmconvert32 ${OSMDIR}/${basecountry}-major_roads.o5m -o=${OSMDIR}/${basecountry}-major_roads.osm.pbf
    # delete original .osm.pbf (also to prevent OsmAndMapCreator from picking it up) and intermediate .o5m files
    # sleep 10 seconds in case of write-behind caching of previous process
    sleep 10
    rm -f ${OSMDIR}/${basecountry}.osm.pbf ${OSMDIR}/${basecountry}.o5m ${OSMDIR}/${basecountry}-major_roads.o5m
done


# Now start the OsmAndMapCreator process
# This can easily be done "in memory"
echo 'Running java net.osmand.data.index.IndexBatchCreator'
java -XX:+UseParallelGC -Xmx4096M -Xmn512M -Djava.util.logging.config.file=build-scripts/batch-logging.properties -cp "DataExtractionOSM/OsmAndMapCreator.jar:DataExtractionOSM/lib/*.jar" net.osmand.data.index.IndexBatchCreator build-scripts/major_roads/major_roads-batch-generate.xml


# clean up after our job
rm -rf ${OSMDIR} ${GENDIR} ${INDEXDIR}

