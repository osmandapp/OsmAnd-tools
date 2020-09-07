#!/usr/bin/python3
# Script for copying selected tiles to dir in QGIS. Use in python console inside QGIS.
# How to use:
# 1.Setup paths in copy_from_csv.sh
# 2.Add grid_polygon_buffered.geojson layer
# 3.Select needed tiles in grid_polygon_buffered layer.
# 4.Run this script

from qgis import processing
import os

processing.run("native:saveselectedfeatures", {'INPUT':'/home/xmd5a/scenaries/qgis/grid_polygon_buffered.geojson','OUTPUT':'/home/xmd5a/tmp/1.csv'})
os.system("/home/xmd5a/scenaries/qgis/copy_from_csv.sh")