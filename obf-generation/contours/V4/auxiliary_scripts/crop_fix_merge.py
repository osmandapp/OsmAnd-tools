#!/usr/bin/python3
# Script for semi-automatic processing of GeoTIFF tiles in QGIS. Use in python console inside QGIS.
# How to use:
# 1.Open tiffs (vrt) and grid_polygon_buffered.geojson
# 2.Select needed tiles in grid_polygon_buffered layer.
# 3.Add and edit (if needed) cutline (vector polygon layer).
# 4.Run this script. Tiles in dir2 will be on top of dir1.

import sys

layer = qgis.utils.iface.activeLayer()
selected_features = layer.selectedFeatures()
cutline = "/home/xmd5a/1.shp"

#dir1 = "/mnt/data/ArcticDEM/tiles_crop_coastline2/"
dir2 = "/mnt/data/MERITDEM/tiles_resized/"
dir1 = "/media/xmd5a/BACKUP/terrain/N/"

outputDir = "/mnt/data/tiff_selected12/"

for i in selected_features:
    attrs = i.__geo_interface__
    tile_id = attrs.get('properties').get('tile_id')
    print(tile_id)
    filename1 = dir1 + tile_id + ".tif"
    filename2 = dir2 + tile_id + ".tif"
    filenameCrop = outputDir + tile_id + "_crop.tif"
    filenameOut = outputDir + tile_id + ".tif"

    warpArgs = "-ts 3602 3602 -ot Int16 -srcnodata -9999 -q -multi -co COMPRESS=LZW -co PREDICTOR=2 -co ZLEVEL=6 -co NUM_THREADS=7 -t_srs 'EPSG:4326' -cutline " + cutline + " " + filename2 + " " + filenameCrop
#    iface.addRasterLayer(filename1,tile_id + " " + suffix1)
#    iface.addRasterLayer(filename2,tile_id + " " + suffix2)
    os.system("gdalwarp" + " " + warpArgs)
    print(warpArgs)
    mergeArgs = " -o " + filenameOut + " " + filename1 + " " + filenameCrop
    print(mergeArgs)
    os.system("rm" + " " + filenameOut)
    os.system("gdal_merge.py" + " " + mergeArgs)
    os.system("rm" + " " + filenameCrop)
