#/usr/bin/python3
import json
from esridump.dumper import EsriDumper
import geojson_validator

d = EsriDumper('https://wfs.schneidercorp.com/arcgis/rest/services/WellsCountyIN_WFS/MapServer/5')

all_features = list(d)
# json_object = json.loads(all_features)
f = open("demofile3.geojson", "a")
geojson_output=json.dumps(all_features, indent=2)
geojson_validator.validate_structure(geojson_output, check_crs=False)

f.write(geojson_validator.validate_structure(geojson_output))
f.close()

