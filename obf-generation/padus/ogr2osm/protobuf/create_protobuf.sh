#!/bin/bash

rm fileformat.proto ../ogr2osm/fileformat_pb2.py
curl -L https://raw.githubusercontent.com/openstreetmap/OSM-binary/master/osmpbf/fileformat.proto >> fileformat.proto
protoc --python_out=../ogr2osm ./fileformat.proto

rm osmformat.proto ../ogr2osm/osmformat_pb2.py
curl -L https://raw.githubusercontent.com/openstreetmap/OSM-binary/master/osmpbf/osmformat.proto >> osmformat.proto
protoc --python_out=../ogr2osm ./osmformat.proto

