#!/usr/bin/python3
# Script splits geotiff by 1° x 1° tiles (WGS 84) with overlap (bufferX, bufferY)

import sys
import os
import time
import os.path
import subprocess
from qgis.core import (
	QgsVectorLayer,
	QgsFeature,
	QgsGeometry,
	QgsVectorFileWriter,
	QgsProject
)

def main():
	tileStartX = -180 # lon min +1 (W,E)   left
	tileEndX = 180 # lon max +1 (W,E)     right
	tileStartY = 84 # lat max +1 (N,S) top
	tileEndY = 56 # lat min +1 (N,S)   bottom
	targetBaseFolder = "/mnt/data/ArcticDEM/tiles_resized_2"
	rasterLayer = "/mnt/data/ArcticDEM/tiles_resized/arcticdem.vrt"

	stepX = 1
	stepY = 1

	width =  360
	height = 180

	iterationsX = int(width / stepX)
	iterationsY = int(height / stepY)

	print ("iterationsY " + str(iterationsY))
	print ("iterationsX " + str(iterationsX))

	bufferX = 0.000416
	bufferY = 0.000416

	j = 0
	i = 0
	lat_str = "N"
	lon_str = "E"
	lat = 0
	lon = 0


	#######  MAIN   #######

	for j in range(0,iterationsY):
		for i in range(0,iterationsX):
			y = str(-(j - tileStartY))
			x = str(i + tileStartX)
			if int(x) > tileEndX:
				break
			if int(y) < tileEndY:
				break

			if int(y) > 0:
				lat_str = "N"
				lat = str(int(y) - 1)
			elif int(y) <= 0:
				lat_str = "S"
				lat = str(abs(int(y) - 1))
			if int(x) >= 0:
				lon_str = "E"
				lon = x
			elif int(x) < 0:
				lon_str = "W"
				lon = str(abs(int(x)))
			if int(lat) < 10:
				lat = "0" + lat
			if int(lon) < 100 and int(lon) >= 10:
				lon = "0" + lon
			elif int(lon) < 10:
				lon = "00" + lon
			tileId = lat_str + lat + lon_str + lon

			minX = (tileStartX + i * stepX) - bufferX
			maxY = (tileStartY - j * stepY) + bufferY
			maxX = (minX + stepX) + 2 * bufferX
			minY = (maxY - stepY) -  2 * bufferY
			print ("Processing tile " + tileId)
#			print(str(x) + " " + str(tileEndX))
#			print(str(y) + " " + str(tileEndY))
			time.sleep(0.1)
			wkt = "POLYGON ((" + str(minX) + " " + str(maxY)+ ", " + str(maxX) + " " + str(maxY) + ", " + str(maxX) + " " + str(minY) + ", " + str(minX) + " " + str(minY) + ", " + str(minX) + " " + str(maxY) + "))"
			tileLayer = QgsVectorLayer("Polygon?crs=epsg:4326", "tile", "memory")
			provider = tileLayer.dataProvider()
			tileFeature = QgsFeature()
			tileFeature.setGeometry(QgsGeometry.fromWkt(wkt))
			provider.addFeatures( [ tileFeature ] )
			QgsVectorFileWriter.writeAsVectorFormat(tileLayer, targetBaseFolder + "/tile.geojson", "UTF-8", tileLayer.crs(), driverName="GeoJSON")
	#		crs = rasterLayer.crs()
	#		print (crs.toProj4())
	#
			warpArgs = "-ts 3602 3602 -ot Int16 -q -multi -co COMPRESS=LZW -co PREDICTOR=2 -co ZLEVEL=6 -co NUM_THREADS=7 -t_srs 'EPSG:4326' -r cubicspline -crop_to_cutline -cutline " + targetBaseFolder + "/tile.geojson " + rasterLayer + " " + targetBaseFolder + "/" + tileId + ".tif"
			if (not os.path.exists(targetBaseFolder + "/" + tileId + ".tif")):
				os.environ["CPL_DEBUG"] = "OFF"
				os.system("gdalwarp" + " " + warpArgs)
			#remove the temporary tile
			QgsProject.instance().removeMapLayers( [tileLayer.id()] )
try:
	main()
except KeyboardInterrupt:
	sys.exit()