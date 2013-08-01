# remove backup and create new backup
# we should not rm, just do incremental updates for now! rm -rf backup 

# remove all previous files
mkdir -p ~/indexes/uploaded

rm -rf .work
mkdir -p .work/osm
if [ -z $INDEXES_FILE ]; then INDEXES_FILE="build-scripts/regions/indexes.xml"; echo "$INDEXES_FILE"; fi

echo 'Running java net.osmand.data.index.IndexBatchCreator with $INDEXES_FILE'
java -XX:+UseParallelGC -Xmx4096M -Xmn512M -Djava.util.logging.config.file=build-scripts/batch-logging.properties -cp "DataExtractionOSM/OsmAndMapCreator.jar:DataExtractionOSM/lib/*.jar" net.osmand.data.index.IndexBatchCreator build-scripts/indexes-batch-generate.xml "$INDEXES_FILE"
