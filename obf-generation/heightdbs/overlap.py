#!/usr/bin/python3

import sys
import os
from optparse import OptionParser, OptionGroup
from osgeo import gdal, gdalconst
from gdal2tiles import GlobalMercator

# =============================================================================
# =============================================================================
# =============================================================================

class OsmAndHeightMapOverlap(object):
    # -------------------------------------------------------------------------
    def __init__(self, arguments):

        ### Analyze arguments

        self.declareOptions()
        self.options, self.args = self.parser.parse_args(args=arguments)
        
        # Check we have both input and output
        if not self.args or len(self.args) != 2:
            self.error("Input path or output path not specified")

        # Check output path
        self.outputDir = self.args[-1]
        self.args = self.args[:-1]

        # Check input path
        self.inputPath = self.args[-1]
        self.args = self.args[:-1]

        # Check input path exists
        if not os.path.exists(self.inputPath):
            self.error("Input path does not exist")

        # Check output path exists
        if not os.path.exists(self.outputDir):
            os.makedirs(self.outputDir)

    # -------------------------------------------------------------------------
    def declareOptions(self):

        usage = "Usage: %prog [options] input_file output_path"
        p = OptionParser(usage)
        p.add_option("--verbose", action="store_true", dest="verbose",
            help="Print status messages to stdout")
        p.add_option("--driver", dest="driver", type="string",
            help="Size of tile.")
        p.add_option("--extension", dest="extension", type="string",
            help="Size of tile.")

        p.set_defaults(verbose=False)

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

        self.minZoom = 31
        self.maxZoom = 0
        self.zoomTileBounds = list(range(0, 32))
        self.inputTiles = list()
        self.inputFiles = dict()

        # Collect all input files
        for zoomDir in os.listdir(self.inputPath):
            zoomPath = os.path.join(self.inputPath, zoomDir)

            zoom = int(zoomDir)
            if zoom < self.minZoom:
                self.minZoom = zoom
            if zoom > self.maxZoom:
                self.maxZoom = zoom

            tmsTileMinX = 2**zoom - 1
            tmsTileMaxX = 0
            tmsTileMinY = 2**zoom - 1
            tmsTileMaxY = 0

            for xTilesDir in os.listdir(zoomPath):
                xTilesPath = os.path.join(zoomPath, xTilesDir)

                tmsTileX = int(xTilesDir)
                if tmsTileX < tmsTileMinX:
                    tmsTileMinX = tmsTileX
                if tmsTileX > tmsTileMaxX:
                    tmsTileMaxX = tmsTileX

                for yTileFile in os.listdir(xTilesPath):
                    tilePath = os.path.join(xTilesPath, yTileFile)

                    tmsTileY = int(os.path.splitext(yTileFile)[0])
                    if tmsTileY < tmsTileMinY:
                        tmsTileMinY = tmsTileY
                    if tmsTileY > tmsTileMaxY:
                        tmsTileMaxY = tmsTileY

                    self.inputTiles.append( (tmsTileX, tmsTileY, zoom, tilePath) )
                    if zoom not in self.inputFiles:
                        self.inputFiles[zoom] = dict()
                    if tmsTileX not in self.inputFiles[zoom]:
                        self.inputFiles[zoom][tmsTileX] = dict()
                    self.inputFiles[zoom][tmsTileX][tmsTileY] = tilePath

            self.zoomTileBounds[zoom] = (tmsTileMinX, tmsTileMinY, tmsTileMaxX, tmsTileMaxY)

    # -------------------------------------------------------------------------
    def overlapTiles(self):

        processedTiles = 0
        for inputTile in self.inputTiles:

            tmsTileX = inputTile[0]
            tmsTileY = inputTile[1]
            tileZoom = inputTile[2]

            if self.options.verbose:
                print(tmsTileX,"x",tmsTileY,"@",tileZoom,"...")

            outputTileFile = os.path.join(self.outputDir, str(tileZoom), str(tmsTileX), "%s.%s" % (tmsTileY, self.options.extension))
            if not os.path.exists(os.path.dirname(outputTileFile)):
                os.makedirs(os.path.dirname(outputTileFile))

            # Open base tile
            baseDataset = gdal.Open(inputTile[3], gdal.GA_ReadOnly)
            if self.options.verbose:
                print("\t",baseDataset.RasterXSize,"x",baseDataset.RasterYSize," -> ",baseDataset.RasterXSize+2,"x",baseDataset.RasterYSize+2)

            # Create target dataset
            targetDataset = self.memDriver.Create('', baseDataset.RasterXSize+2, baseDataset.RasterYSize+2, 1, baseDataset.GetRasterBand(1).DataType)

            # Copy base to target
            if self.options.verbose:
                print("\t =0  ",tmsTileX,"x",tmsTileY,"@",tileZoom)
            targetDataset.WriteRaster(1, 1, baseDataset.RasterXSize, baseDataset.RasterYSize,
                baseDataset.ReadRaster(0, 0, baseDataset.RasterXSize, baseDataset.RasterYSize))
            del baseDataset

            # Left tile
            if tmsTileX > self.zoomTileBounds[tileZoom][0]:
                filename = self.inputFiles[tileZoom][tmsTileX - 1][tmsTileY]
                dataset = gdal.Open(filename, gdal.GA_ReadOnly)

                targetDataset.WriteRaster(0, 1, 1, dataset.RasterYSize,
                    dataset.ReadRaster(dataset.RasterXSize - 1, 0, 1, dataset.RasterYSize))
                
                del dataset

                if self.options.verbose:
                    print("\t +L  ",tmsTileX-1,"x",tmsTileY,"@",tileZoom)
            
            # Right tile
            if tmsTileX < self.zoomTileBounds[tileZoom][2]:
                filename = self.inputFiles[tileZoom][tmsTileX + 1][tmsTileY]
                dataset = gdal.Open(filename, gdal.GA_ReadOnly)

                targetDataset.WriteRaster(targetDataset.RasterXSize - 1, 1, 1, dataset.RasterYSize,
                    dataset.ReadRaster(0, 0, 1, dataset.RasterYSize))
                
                del dataset

                if self.options.verbose:
                    print("\t +R  ",tmsTileX+1,"x",tmsTileY,"@",tileZoom)

            # Top tile
            if tmsTileY < self.zoomTileBounds[tileZoom][3]:
                filename = self.inputFiles[tileZoom][tmsTileX][tmsTileY + 1]
                dataset = gdal.Open(filename, gdal.GA_ReadOnly)

                targetDataset.WriteRaster(1, 0, dataset.RasterXSize, 1,
                    dataset.ReadRaster(0, dataset.RasterYSize - 1, dataset.RasterXSize, 1))
                
                del dataset

                if self.options.verbose:
                    print("\t +T  ",tmsTileX,"x",tmsTileY+1,"@",tileZoom)

            # Bottom tile
            if tmsTileY > self.zoomTileBounds[tileZoom][1]:
                filename = self.inputFiles[tileZoom][tmsTileX][tmsTileY - 1]
                dataset = gdal.Open(filename, gdal.GA_ReadOnly)

                targetDataset.WriteRaster(1, targetDataset.RasterYSize - 1, dataset.RasterXSize, 1,
                    dataset.ReadRaster(0, 0, dataset.RasterXSize, 1))
                
                del dataset

                if self.options.verbose:
                    print("\t +B  ",tmsTileX,"x",tmsTileY-1,"@",tileZoom)

            # Top-left corner
            if tmsTileX > self.zoomTileBounds[tileZoom][0] and tmsTileY < self.zoomTileBounds[tileZoom][3]:
                filename = self.inputFiles[tileZoom][tmsTileX - 1][tmsTileY + 1]
                dataset = gdal.Open(filename, gdal.GA_ReadOnly)

                targetDataset.WriteRaster(0, 0, 1, 1,
                    dataset.ReadRaster(dataset.RasterXSize - 1, dataset.RasterYSize - 1, 1, 1))
                
                del dataset

                if self.options.verbose:
                    print("\t +TL ",tmsTileX-1,"x",tmsTileY+1,"@",tileZoom)

            # Top-right corner
            if tmsTileX < self.zoomTileBounds[tileZoom][2] and tmsTileY < self.zoomTileBounds[tileZoom][3]:
                filename = self.inputFiles[tileZoom][tmsTileX + 1][tmsTileY + 1]
                dataset = gdal.Open(filename, gdal.GA_ReadOnly)

                targetDataset.WriteRaster(targetDataset.RasterXSize - 1, 0, 1, 1,
                    dataset.ReadRaster(0, dataset.RasterYSize - 1, 1, 1))
                
                del dataset

                if self.options.verbose:
                    print("\t +TR ",tmsTileX+1,"x",tmsTileY+1,"@",tileZoom)

            # Bottom-left corner
            if tmsTileX > self.zoomTileBounds[tileZoom][0] and tmsTileY > self.zoomTileBounds[tileZoom][1]:
                filename = self.inputFiles[tileZoom][tmsTileX - 1][tmsTileY - 1]
                dataset = gdal.Open(filename, gdal.GA_ReadOnly)

                targetDataset.WriteRaster(0, targetDataset.RasterYSize - 1, 1, 1,
                    dataset.ReadRaster(dataset.RasterXSize - 1, 0, 1, 1))
                
                del dataset

                if self.options.verbose:
                    print("\t +BL ",tmsTileX-1,"x",tmsTileY-1,"@",tileZoom)

            # Bottom-right corner
            if tmsTileX < self.zoomTileBounds[tileZoom][2] and tmsTileY > self.zoomTileBounds[tileZoom][1]:
                filename = self.inputFiles[tileZoom][tmsTileX + 1][tmsTileY - 1]
                dataset = gdal.Open(filename, gdal.GA_ReadOnly)

                targetDataset.WriteRaster(targetDataset.RasterXSize - 1, targetDataset.RasterYSize - 1, 1, 1,
                    dataset.ReadRaster(0, 0, 1, 1))
                
                del dataset

                if self.options.verbose:
                    print("\t +BR ",tmsTileX+1,"x",tmsTileY-1,"@",tileZoom)

            # Write target to file
            self.outDriver.CreateCopy(outputTileFile, targetDataset, strict=1)
            del targetDataset

            if not self.options.verbose:
                processedTiles += 1
                self.progress(processedTiles / float(len(self.inputTiles)))

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
    def process(self):

        self.prepareInput()
        self.overlapTiles()

# =============================================================================
# =============================================================================
# =============================================================================

if __name__=='__main__':
    argv = gdal.GeneralCmdLineProcessor( sys.argv )
    if argv:
        overlap = OsmAndHeightMapOverlap( argv[1:] )
        overlap.process()
