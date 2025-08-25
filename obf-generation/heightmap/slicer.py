#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import os
from optparse import OptionParser
from osgeo import gdal, gdalconst
#from gdal2tiles import GlobalMercator
from osgeo_utils.gdal2tiles import GlobalMercator

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

        if self.inputBand.DataType != gdalconst.GDT_Int16:
            self.error("Input file must have single raster band with Int16 type")

        self.inputGeoTransform = self.inputDataset.GetGeoTransform()
        if (self.inputGeoTransform[2], self.inputGeoTransform[4]) != (0,0):
            self.error("Georeference of the raster must not contain rotation or skew")

        self.inputBoundsMinX = self.inputGeoTransform[0]
        self.inputBoundsMaxX = self.inputGeoTransform[0] + self.inputDataset.RasterXSize*self.inputGeoTransform[1]
        self.inputBoundsMaxY = self.inputGeoTransform[3]
        self.inputBoundsMinY = self.inputGeoTransform[3] - self.inputDataset.RasterYSize*self.inputGeoTransform[1]

        self.mercator = GlobalMercator(self.options.size)
        self.minZoom = 0
        self.baseZoom = self.mercator.ZoomForPixelSize(self.inputGeoTransform[1])
        self.zoomTileBounds = list(range(0, self.baseZoom + 1))

        if self.options.verbose:
            print("Input raster w=%s, h=%s, min-zoom=%s, base-zoom=%s" %(self.inputDataset.RasterXSize, self.inputDataset.RasterYSize, self.minZoom, self.baseZoom))

    # -------------------------------------------------------------------------
    def bakeBaseTiles(self):

        # Max tile index
        maxTileIndex = 2**self.baseZoom-1

        # Get bounds in tiles for base zoom
        tileMinX, tileMinY = self.mercator.MetersToTile(self.inputBoundsMinX, self.inputBoundsMinY, self.baseZoom)
        tileMaxX, tileMaxY = self.mercator.MetersToTile(self.inputBoundsMaxX, self.inputBoundsMaxY, self.baseZoom)

        # Crop tile indices
        tileMinX, tileMinY = max(0, tileMinX), max(0, tileMinY)
        tileMaxX, tileMaxY = min(maxTileIndex, tileMaxX), min(maxTileIndex, tileMaxY)

        self.zoomTileBounds[self.baseZoom] = (tileMinX, tileMinY, tileMaxX, tileMaxY)

        totalTiles = (abs(tileMaxX - tileMinX) + 1) * (abs(tileMaxY - tileMinY) + 1)
        processedTiles = 0;

        for tileY in range(tileMinY, tileMaxY+1):
            for tileX in range(tileMinX, tileMaxX+1):
                outputTileFile = os.path.join(self.outputDir, str(self.baseZoom), str(tileX), "%s.%s" % (tileY, self.options.extension))

                # Skip if this tile already exists
                if os.path.exists(outputTileFile):
                    if self.options.verbose:
                        print("Skipping base tile TMS %sx%s@%s" % (tileX, tileY, self.baseZoom))
                    continue

                if self.options.verbose:
                    print("Baking base tile TMS %sx%s@%s" % (tileX, tileY, self.baseZoom))

                # Create directories for the tile
                if not os.path.exists(os.path.dirname(outputTileFile)):
                    os.makedirs(os.path.dirname(outputTileFile))

                # Get bounds of this tile (in meters)
                tileBoundsInMeters = self.mercator.TileBounds(tileX, tileY, self.baseZoom)
                if self.options.verbose:
                    print("\t Bounds (meters): l=%s, t=%s, r=%s, b=%s (w=%s, h=%s)" % (
                        tileBoundsInMeters[0], tileBoundsInMeters[1], tileBoundsInMeters[2], tileBoundsInMeters[3],
                        tileBoundsInMeters[3] - tileBoundsInMeters[1], tileBoundsInMeters[2] - tileBoundsInMeters[0]))

                # Convert tile bounds to source raster coordinates (swap top and bottom)
                tileBoundsInInput = self.boundsInMetersToRaster(self.inputDataset, tileBoundsInMeters[0], tileBoundsInMeters[3], tileBoundsInMeters[2], tileBoundsInMeters[1])
                tileSourceSize = (tileBoundsInInput[2], tileBoundsInInput[3])
                if self.options.verbose:
                    print("\t Bounds ( input): l=%s, t=%s, w=%s, h=%s" % (tileBoundsInInput[0], tileBoundsInInput[1], tileBoundsInInput[2], tileBoundsInInput[3]))

                # Get tile bounds in output
                tileBoundsInOutput = (0, 0, tileSourceSize[0], tileSourceSize[1])
                if self.options.verbose:
                    print("\t Bounds (output): l=%s, t=%s, w=%s, h=%s" % (tileBoundsInOutput[0], tileBoundsInOutput[1], tileBoundsInOutput[2], tileBoundsInOutput[3]))

                # Crop tile bounds in source not to exceed input bounds (and change source bounds accordingly)
                tileBoundsInInput, tileBoundsInOutput = self.cropRastersBounds(self.inputDataset.RasterXSize, self.inputDataset.RasterYSize, *tileBoundsInInput + tileBoundsInOutput)
                if self.options.verbose:
                    print("\t BoundsC( input): l=%s, t=%s, w=%s, h=%s" % (tileBoundsInInput[0], tileBoundsInInput[1], tileBoundsInInput[2], tileBoundsInInput[3]))
                    print("\t BoundsC(output): l=%s, t=%s, w=%s, h=%s" % (tileBoundsInOutput[0], tileBoundsInOutput[1], tileBoundsInOutput[2], tileBoundsInOutput[3]))

                if tileBoundsInInput[2] == 0 or tileBoundsInInput[3] == 0:
                    if self.options.verbose:
                        print("Skipping base tile TMS %sx%s@%s cause it has 0 dimensions in source" % (tileX, tileY, self.baseZoom))
                    continue
                if tileBoundsInOutput[2] == 0 or tileBoundsInOutput[3] == 0:
                    if self.options.verbose:
                        print("Skipping base tile TMS %sx%s@%s cause it has 0 dimensions in target" % (tileX, tileY, self.baseZoom))
                    continue

                # Create target dataset
                sourceDataset = self.memDriver.Create('', tileSourceSize[0], tileSourceSize[1], 1, self.inputBand.DataType)

                # Read data from raster
                sourceDataset.WriteRaster(tileBoundsInOutput[0], tileBoundsInOutput[1], tileBoundsInOutput[2], tileBoundsInOutput[3],
                    self.inputDataset.ReadRaster(tileBoundsInInput[0], tileBoundsInInput[1], tileBoundsInInput[2], tileBoundsInInput[3], tileBoundsInOutput[2], tileBoundsInOutput[3]))

                if tileSourceSize[0] != self.options.size or tileSourceSize[1] != self.options.size:
                    if self.options.verbose:
                        print("\t Size   (scaled): w=%s, h=%s" % (self.options.size, self.options.size))

                    # Create target dataset
                    targetDataset = self.memDriver.Create('', self.options.size, self.options.size, 1, self.inputBand.DataType)

                    # Scale dataset
                    self.scaleDataset(sourceDataset, targetDataset)

                    # Save target dataset to a tile
                    self.outDriver.CreateCopy(outputTileFile, targetDataset, strict=1, options = self.options.driverOptions)

                    del targetDataset
                else:
                    # Save source dataset to a tile
                    self.outDriver.CreateCopy(outputTileFile, sourceDataset, strict=1, options = self.options.driverOptions)

                # Remove source dataset
                del sourceDataset

                if not self.options.verbose:
                    processedTiles += 1
                    self.progress(processedTiles / float(totalTiles))

    # -------------------------------------------------------------------------
    def bakeDownscaledTiles(self):

        for zoom in range(self.baseZoom-1, self.minZoom-1, -1):
            # Max tile index
            maxTileIndex = 2**zoom-1

            # Get bounds in tiles for base zoom
            tileMinX, tileMinY = self.mercator.MetersToTile(self.inputBoundsMinX, self.inputBoundsMinY, zoom)
            tileMaxX, tileMaxY = self.mercator.MetersToTile(self.inputBoundsMaxX, self.inputBoundsMaxY, zoom)

            # Crop tile indices
            tileMinX, tileMinY = max(0, tileMinX), max(0, tileMinY)
            tileMaxX, tileMaxY = min(maxTileIndex, tileMaxX), min(maxTileIndex, tileMaxY)

            self.zoomTileBounds[zoom] = (tileMinX, tileMinY, tileMaxX, tileMaxY)

            totalTiles = (abs(tileMaxX - tileMinX) + 1) * (abs(tileMaxY - tileMinY) + 1)
            processedTiles = 0;

            for tileY in range(tileMinY, tileMaxY + 1):
                for tileX in range(tileMinX, tileMaxX + 1):
                    outputTileFile = os.path.join(self.outputDir, str(zoom), str(tileX), "%s.%s" % (tileY, self.options.extension))

                    # Skip if this tile already exists
                    if os.path.exists(outputTileFile):
                        if self.options.verbose:
                            print("Skipping overview tile TMS %sx%s@%s" % (tileX, tileY, self.baseZoom))
                        continue

                    if self.options.verbose:
                        print("Baking overview tile TMS %sx%s@%s" % (tileX, tileY, zoom))

                    # Create directories for the tile
                    if not os.path.exists(os.path.dirname(outputTileFile)):
                        os.makedirs(os.path.dirname(outputTileFile))

                    # Create source dataset
                    sourceDataset = self.memDriver.Create('', 2*self.options.size, 2*self.options.size, 1, self.inputBand.DataType)

                    # Read 4 upper-scale tiles
                    for yzTileY in range(2*tileY,2*tileY+2):
                        for yzTileX in range(2*tileX,2*tileX+2):
                            uzTileMinX, uzTileMinY, uzTileMaxX, uzTileMaxY = self.zoomTileBounds[zoom+1]

                            if yzTileX < uzTileMinX or yzTileX > uzTileMaxX or yzTileY < uzTileMinY or yzTileY > uzTileMaxY:
                                continue

                            upperTileFile = os.path.join(self.outputDir, str(zoom+1), str(yzTileX), "%s.%s" % (yzTileY, self.options.extension))
                            # tile was 0 width probably
                            if not os.path.exists(upperTileFile):
                                continue
                            upperTileDataset = gdal.Open(upperTileFile, gdal.GA_ReadOnly)

                            col = yzTileX
                            if tileX != 0:
                                col %= (2*tileX)
                            row = yzTileY
                            if tileY != 0:
                                row %= (2*tileY)
                            row = 1 - row

                            sourceDataset.WriteRaster(col * self.options.size, row * self.options.size, self.options.size, self.options.size,
                                upperTileDataset.ReadRaster(0, 0, self.options.size, self.options.size))

                            del upperTileDataset

                    # Create target dataset
                    targetDataset = self.memDriver.Create('', self.options.size, self.options.size, 1, self.inputBand.DataType)

                    # Scale dataset
                    self.scaleDataset(sourceDataset, targetDataset)

                    # Save target dataset to a tile
                    self.outDriver.CreateCopy(outputTileFile, targetDataset, strict=1, options = self.options.driverOptions)

                    # Remove target dataset
                    del targetDataset

                    # Remove source dataset
                    del sourceDataset

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
    def boundsInMetersToRaster(self, dataset, xMin, yMin, xMax, yMax):

        geoTransform = dataset.GetGeoTransform()

        l = int((xMin - geoTransform[0]) / geoTransform[1] + 0.001)
        t = int((yMin - geoTransform[3]) / geoTransform[5] + 0.001)
        w = int((xMax - xMin) / geoTransform[1] + 0.5)
        h = int((yMax - yMin) / geoTransform[5] + 0.5)

        return (l, t, w, h)

    # -------------------------------------------------------------------------
    def cropRasterBounds(self, rMaxSize, rOffset, rSize, wOffset, wSize):
        if rOffset < 0:
            wOffset = int(wSize * (float(abs(rOffset)) / rSize))
            wSize -= wOffset
            rSize -= int(rSize * (float(abs(rOffset)) / rSize))
            rOffset = 0
        if rOffset + rSize > rMaxSize:
            wSize = int(wSize * (float(rMaxSize - rOffset) / rSize))
            rSize = rMaxSize - rOffset

        return (rOffset, rSize, wOffset, wSize)

    # -------------------------------------------------------------------------
    def cropRastersBounds(self, rMaxSizeX, rMaxSizeY, rOffsetX, rOffsetY, rSizeX, rSizeY, wOffsetX, wOffsetY, wSizeX, wSizeY):
        xCropped = self.cropRasterBounds(rMaxSizeX, rOffsetX, rSizeX, wOffsetX, wSizeX)
        yCropped = self.cropRasterBounds(rMaxSizeY, rOffsetY, rSizeY, wOffsetY, wSizeY)

        return (xCropped[0], yCropped[0], xCropped[1], yCropped[1]), (xCropped[2], yCropped[2], xCropped[3], yCropped[3])

    # -------------------------------------------------------------------------
    def scaleDataset(self, source, target):
        source.SetGeoTransform( (0.0, target.RasterXSize / float(source.RasterXSize), 0.0, 0.0, 0.0, target.RasterYSize / float(source.RasterYSize)) )
        target.SetGeoTransform( (0.0, 1.0, 0.0, 0.0, 0.0, 1.0) )

        res = gdal.ReprojectImage(source, target, None, None, gdal.GRA_Cubic)
        if res != 0:
            self.error("ReprojectImage() failed error %d" % (res))

    # -------------------------------------------------------------------------
    def process(self):

        self.prepareInput()
        self.bakeBaseTiles()
        self.bakeDownscaledTiles()

# =============================================================================
# =============================================================================
# =============================================================================

if __name__=='__main__':
    argv = gdal.GeneralCmdLineProcessor( sys.argv )
    if argv:
        slicer = OsmAndHeightMapSlicer( argv[1:] )
        slicer.process()
