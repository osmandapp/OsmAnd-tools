#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import math
import sys
import os
from optparse import OptionParser
from osgeo import gdal, gdalconst

# =============================================================================
# =============================================================================
# =============================================================================

class OsmAndHeightMapSlicer(object):
    # -------------------------------------------------------------------------
    def __init__(self, arguments):

        ### Analyze arguments

        self.declareOptions()
        self.options, self.args = self.parser.parse_args(args=arguments)

        # Check we have both input and output
        if not self.args or len(self.args) != 2:
            self.error("Input file or output path not specified")

        # Check output path
        self.outputDir = self.args[-1]
        self.args = self.args[:-1]

        # Check input file
        self.inputFile = self.args[-1]
        self.args = self.args[:-1]

        # Check input file exists
        if not os.path.exists(self.inputFile) or not os.path.isfile(self.inputFile):
            self.error("Input file does not exist")

        # Check output path exists
        if not os.path.exists(self.outputDir):
            os.makedirs(self.outputDir)

        # Parse options
        if self.options.driverOptions != None:
            self.options.driverOptions = self.options.driverOptions.split(';')
        else:
            self.options.driverOptions = []

    # -------------------------------------------------------------------------
    def declareOptions(self):

        usage = "Usage: %prog [options] input_file output_path"
        p = OptionParser(usage)
        p.add_option("--verbose", action="store_true", dest="verbose",
            help="Print status messages to stdout")
        p.add_option("--size", dest="size", type="int",
            help="Size of tile.")
        p.add_option("--overlap", dest="overlap", type="int",
            help="Overlapped part of tile size.")
        p.add_option("--zoom", dest="zoom", type="int",
            help="Zoom of tiles.")
        p.add_option("--driver", dest="driver", type="string",
            help="Tile output driver.")
        p.add_option("--driver-options", dest="driverOptions", type="string",
            help="Tile output driver options.")
        p.add_option("--extension", dest="extension", type="string",
            help="Tile file extension.")

        p.set_defaults(verbose=False, driverOptions=None)

        self.parser = p

    # -------------------------------------------------------------------------
    def prepareInput(self):
        gdal.AllRegister()

        self.outDriver = gdal.GetDriverByName(self.options.driver)
        if not self.outDriver:
            raise Exception("The '%s' driver was not found", self.options.driver)
        self.memDriver = gdal.GetDriverByName('MEM')
        if not self.memDriver:
            raise Exception("The 'MEM' driver was not found")

        self.inputDataset = gdal.Open(self.inputFile, gdal.GA_ReadOnly)
        if not self.inputDataset:
            self.error("Failed to open input file using GDAL")

        if self.inputDataset.RasterCount != 1:
            self.error("Input file must have 1 raster band, instead it has %s band(s)" % self.inputDataset.RasterCount)

        self.inputBand = self.inputDataset.GetRasterBand(1)
        if self.inputBand.GetRasterColorTable():
            self.error("Input file must not have color table")

        if self.inputBand.DataType != gdalconst.GDT_Float32:
            self.error("Input file must have single raster band with Float32 type")

        self.inputGeoTransform = self.inputDataset.GetGeoTransform()
        if (self.inputGeoTransform[2], self.inputGeoTransform[4]) != (0,0):
            self.error("Georeference of the raster must not contain rotation or skew")
        self.wkt = self.inputDataset.GetProjection()
        self.gcps = self.inputDataset.GetGCPs()

        # Constants
        self.earthInMeters = 40075016.68557848615314309804
        self.earthIn31 = 2 ** 31
        self.tileSizeIn31 = 2 ** (31 - self.options.zoom)
        self.maxTileIndex = 2 ** self.options.zoom - 1
        self.tileSizeInMeters = self.tileSizeIn31 * self.earthInMeters / self.earthIn31

        if self.options.verbose:
            print("Input raster w=%s, h=%s, base-zoom=%s" % (self.inputDataset.RasterXSize,
            self.inputDataset.RasterYSize, self.options.zoom))

    # -------------------------------------------------------------------------
    def bakeTiles(self):      

        # Get indices of edge tiles
        overlap = self.tileSizeInMeters / (self.options.size - self.options.overlap) * self.options.overlap / 2
        gapX, gapY = overlap + self.inputGeoTransform[1], overlap + self.inputGeoTransform[5]
        inputRight = self.inputGeoTransform[0] + self.inputDataset.RasterXSize * self.inputGeoTransform[1]
        inputBottom = self.inputGeoTransform[3] + self.inputDataset.RasterYSize * self.inputGeoTransform[5]
        tileMinX, tileMinY = self.metersToTile(self.inputGeoTransform[0] + gapX, self.inputGeoTransform[3] - gapY)
        tileMaxX, tileMaxY = self.metersToTile(inputRight - gapX, inputBottom + gapY)

        # Crop tile indices
        tileMinX, tileMinY = self.cropTileIndex(tileMinX + 1), self.cropTileIndex(tileMinY + 1)
        tileMaxX, tileMaxY = self.cropTileIndex(tileMaxX - 1), self.cropTileIndex(tileMaxY - 1)

        totalTiles = (tileMaxX - tileMinX + 1) * (tileMaxY - tileMinY + 1)
        if totalTiles < 0:
            totalTiles = 0

        if self.options.verbose:
            print("Tiles to process: %s" % (totalTiles))

        processedTiles = 0;

        for tileY in range(tileMinY, tileMaxY + 1):
            for tileX in range(tileMinX, tileMaxX + 1):
                outputTileFile = os.path.join(self.outputDir, str(self.options.zoom), str(tileX), "%s.%s" % (tileY, self.options.extension))

                # Skip if this tile already exists
                #if os.path.exists(outputTileFile):
                #    if self.options.verbose:
                #        print("Skipping tile TMS Y%sX%s" % (tileY, tileX))
                #    continue

                if self.options.verbose:
                    print("Baking tile TMS Y%sX%s" % (tileY, tileX))

                # Create directories for the tile
                if not os.path.exists(os.path.dirname(outputTileFile)):
                    os.makedirs(os.path.dirname(outputTileFile))

                # Get bounds of this tile with overlap (in meters)
                tileLeft, tileTop = self.metersFrom31(tileX * self.tileSizeIn31, tileY * self.tileSizeIn31)
                tileRight = tileLeft + self.tileSizeInMeters + overlap
                tileBottom = tileTop - self.tileSizeInMeters - overlap
                tileLeft, tileTop = tileLeft - overlap, tileTop + overlap

                if self.options.verbose:
                    print("\t Bounds (meters): l=%s, t=%s, r=%s, b=%s (w=%s, h=%s)" % (
                        tileLeft, tileTop, tileRight, tileBottom,
                        tileRight - tileLeft, tileTop - tileBottom))

                # Calculate pixel coordinates and raster size of the tile
                dataSizeX = math.floor((tileRight - tileLeft) / self.inputGeoTransform[1] + 2)
                dataSizeY = math.floor((tileBottom - tileTop) / self.inputGeoTransform[5] + 2)
                pixelLeft = (tileLeft - self.inputGeoTransform[0]) / self.inputGeoTransform[1]
                pixelTop = (tileTop - self.inputGeoTransform[3]) / self.inputGeoTransform[5]
                dataLeft, dataTop = math.floor(pixelLeft), math.floor(pixelTop)
                if dataLeft == pixelLeft:
                    dataLeft -=1
                if dataTop == pixelTop:
                    dataTop -=1
                tileLeft = dataLeft * self.inputGeoTransform[1] + self.inputGeoTransform[0]
                tileTop = dataTop * self.inputGeoTransform[5] + self.inputGeoTransform[3]

                # Create target dataset
                targetDataset = self.outDriver.Create(outputTileFile, dataSizeX, dataSizeY,
                    1, self.inputBand.DataType, options = self.options.driverOptions)
                targetDataset.SetGeoTransform( (tileLeft, self.inputGeoTransform[1], 0.0,
                    tileTop, 0.0, self.inputGeoTransform[5]) )
                targetDataset.SetGCPs(self.gcps, self.wkt)

                # Store tile in target dataset
                targetDataset.WriteRaster(0, 0, dataSizeX, dataSizeY,
                    self.inputDataset.ReadRaster(dataLeft, dataTop, dataSizeX, dataSizeY, dataSizeX, dataSizeY))

                if not self.options.verbose:
                    processedTiles += 1
                    self.progress(processedTiles / float(totalTiles))

    # -------------------------------------------------------------------------
    def error(self, msg, details = "" ):
        if details:
            self.parser.error(msg + "\n\n" + details)
        else:
            self.parser.error(msg)

    # -------------------------------------------------------------------------
    def progress(self, complete = 0.0):
        gdal.TermProgress_nocb(complete)

    # -------------------------------------------------------------------------
    def metersIn31(self, m):
        return math.floor((m / self.earthInMeters + 0.5) * self.earthIn31)

    # -------------------------------------------------------------------------
    def metersTo31(self, x, y):
        return (self.metersIn31(x), self.earthIn31 - 1 - self.metersIn31(y))

    # -------------------------------------------------------------------------
    def metersToTile(self, x, y):
        tx = math.floor(self.metersIn31(x) / self.tileSizeIn31)
        ty = math.floor((self.earthIn31 - 1 - self.metersIn31(y)) / self.tileSizeIn31)
        return (tx, ty)

    # -------------------------------------------------------------------------
    def metersOutof31(self, с):
        return (с / self.earthIn31 - 0.5) * self.earthInMeters;

    # -------------------------------------------------------------------------
    def metersFrom31(self, x, y):
        return (self.metersOutof31(x), self.metersOutof31(self.earthIn31 - y))

    # -------------------------------------------------------------------------
    def cropTileIndex(self, t):
        result = t
        if t > self.maxTileIndex:
            result = t - self.maxTileIndex - 1
        if t < 0:
            result = t + self.maxTileIndex + 1
        return result

    # -------------------------------------------------------------------------
    def process(self):

        self.prepareInput()
        self.bakeTiles()

# =============================================================================
# =============================================================================
# =============================================================================

if __name__=='__main__':
    argv = gdal.GeneralCmdLineProcessor( sys.argv )
    if argv:
        slicer = OsmAndHeightMapSlicer( argv[1:] )
        slicer.process()
