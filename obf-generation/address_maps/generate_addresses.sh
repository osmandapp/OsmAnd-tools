# This script creates the address maps directly from the complete country .osm.pbf

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
    #wget -O $OSMDIR/${basecountry%-latest.osm.pbf}-major_roads.osm.pbf "http://download.geofabrik.de/${country}-latest.osm.pbf"
    # not every shell interpreter understands above % replacing. Just do it in two steps
    wget "http://download.geofabrik.de/${country}-latest.osm.pbf"
    mv "${basecountry}-latest.osm.pbf" "$OSMDIR/${basecountry}-address.osm.pbf"
done    


# Now start the OsmAndMapCreator process
echo 'Running java net.osmand.data.index.IndexBatchCreator'
java -XX:+UseParallelGC -Xmx8192M -Xmn512M -Djava.util.logging.config.file=build-scripts/batch-logging.properties -cp "DataExtractionOSM/OsmAndMapCreator.jar:DataExtractionOSM/lib/*.jar" net.osmand.data.index.IndexBatchCreator build-scripts/address_maps/address-batch-generate.xml


# clean up after our job
rm -rf ${OSMDIR} ${GENDIR} ${INDEXDIR}

