import os
import sys
from PyQt5.QtCore import QVariant
from qgis.core import *

# Step 1 : Adding field
full_file_name = sys.argv[1]
if os.path.isfile(full_file_name) == False:
	print("Error: file "+full_file_name+" is not found")
	exit(1)
head, tail = os.path.split(full_file_name)
file_name = tail.split(".")[0]
layer = QgsVectorLayer(full_file_name, "layer", "ogr")
provider = layer.dataProvider()
file_name_field = QgsField("FILE_NAME", QVariant.String)
provider.addAttributes([file_name_field])
layer.updateFields()

# Step 2 : Updating field for each feature
idx = provider.fieldNameIndex('FILE_NAME')
for feature in layer.getFeatures():
    attrs = {idx : file_name.lower()}
    layer.dataProvider().changeAttributeValues({feature.id() : attrs})