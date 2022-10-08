wget https://download.osmand.net/latest-night-build/OsmAndMapCreator-main.zip
unzip OsmAndMapCreator-main.zip -d OsmAndMapCreator
docker build -t osmandapp/osmand-mapcreator:latest .

# docker login -u osmandapp
# docker push osmandapp/osmand-mapcreator:latest

# Example to run (result in current folder)
docker run --mount type=bind,source="$(pwd)",target=/home/work osmandapp/osmand-mapcreator generate-obf ./andorra_europe.pbf
docker run --mount type=bind,source="$(pwd)",target=/home/work osmandapp/osmand-mapcreator generate-obf https://builder.osmand.net/osm-extract/andorra_europe/andorra_europe.pbf

