#!/usr/bin/python3

# python3 -m dump_esri 'esri_endpoint' geojson_name
import json
from esridump.dumper import EsriDumper
import os
import sys

endpoint = sys.argv[1]
output = sys.argv[2]

d = EsriDumper(endpoint) #'https://wfs.schneidercorp.com/arcgis/rest/services/WellsCountyIN_WFS/MapServer/5'

all_features = list(d)
f = open(output, "a")
geojson_output=json.dumps(all_features, indent=2)

f.write("{\"type\": \"FeatureCollection\",\"features\":"+geojson_output+'}')
f.close()
