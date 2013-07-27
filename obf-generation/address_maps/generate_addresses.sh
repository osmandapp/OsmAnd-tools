#where does this script start from
WORKDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd ${WORKDIR}

#WORKDIR=`pwd`
echo ${WORKDIR}

TMPDIR=${WORKDIR}/tmp/
OSMDIR=${WORKDIR}/osm
GENDIR=${WORKDIR}/gen
INDEXDIR=${WORKDIR}/index/
OUTDIR=${WORKDIR}/output/

# small countries to test with
#countries="europe/albania europe/andorra europe/azores"
# real countries
countries="europe/british-isles europe/france europe/germany europe/italy north-america/canada"

# clean up before start
rm -rf $OSMDIR
mkdir -p $OSMDIR

for country in ${countries}; do
    basecountry=$(basename ${country})
    echo "${country} and the basename ${basecountry}"
    # Download countries
    #wget -O $OSMDIR/${country%latest.osm.pbf}major_roads.osm.pbf "http://download.geofabrik.de/$country.osm.pbf"
    wget -O ${OSMDIR}/${basecountry}.osm.pbf "http://download.geofabrik.de/${country}-latest.osm.pbf"
    # convert to fastest intermediate format
    osmconvert --drop-author --drop-brokenrefs ${OSMDIR}/${basecountry}.osm.pbf -o=${OSMDIR}/${basecountry}.o5m
    # filter only the necessary stuff out of the entire osm file
    osmfilter ${OSMDIR}/${basecountry}.o5m  --keep="boundary=administrative addr:* place=* is_in=* highway=residential =unclassified =pedestrian" --keep-ways-relations="boundary=administrative" --keep-ways= --keep-nodes= --keep-relations=   > ${OSMDIR}/${basecountry}_address.o5m
    # convert back to  format suitable for OsmAndMapCreator
    osmconvert ${OSMDIR}/${basecountry}_address.o5m -o=${OSMDIR}/${basecountry}_address.osm.pbf
    # delete original .osm.pbf (also to prevent OsmAndMapCreator from picking it up) and intermediate .o5m files
    # sleep 10 seconds in case of write-behind caching of previous process
    sleep 10
    rm -f ${OSMDIR}/${basecountry}.osm.pbf ${OSMDIR}/${basecountry}.o5m ${OSMDIR}/${basecountry}_address.o5m
done
