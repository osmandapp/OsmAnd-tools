FROM ubuntu:22.04

RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y \
        libxml2-utils python3-pip libprotobuf-dev protobuf-compiler \
        gdal-bin libgdal-dev python3-gdal osmctools && \
    rm -rf /var/lib/apt/lists/* && \
    python3 -m pip install -U pip setuptools wheel && \
    pip install --upgrade protobuf ogr2osm

ENTRYPOINT ["ogr2osm"]