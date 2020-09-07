#!/usr/bin/python3
# Script for adding selected tiles as new layers from 2 dirs in QGIS. Use in python console inside QGIS.
# How to use:
# 1.Add grid_polygon_buffered.geojson layer
# 2.Select tile in this layer
# 3.Run this script

import sys

layer = qgis.utils.iface.activeLayer()
selected_features = layer.selectedFeatures()
dir1 = "/media/xmd5a/BACKUP/terrain/N/"
dir2 = "/mnt/data/MERITDEM/tiles_resized/"
suffix1 = "o"
suffix2 = "n"
for i in selected_features:
    attrs = i.__geo_interface__
    tile_id = attrs.get('properties').get('tile_id')
    filename1 = dir1 + tile_id + ".tif"
    filename2 = dir2 + tile_id + ".tif"
    print(filename1,filename2)
    iface.addRasterLayer(filename1,tile_id + " " + suffix1)
    iface.addRasterLayer(filename2,tile_id + " " + suffix2)