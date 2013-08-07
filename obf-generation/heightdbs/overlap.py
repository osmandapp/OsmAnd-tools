#!/usr/bin/python3

import sys
import os
from optparse import OptionParser, OptionGroup
from osgeo import gdal

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

        self.minZoom = 31
        self.maxZoom = 0
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

            for xTilesDir in os.listdir(zoomPath):
                xTilesPath = os.path.join(zoomPath, xTilesDir)

                tmsTileX = int(xTilesDir)

                for yTileFile in os.listdir(xTilesPath):
                    tilePath = os.path.join(xTilesPath, yTileFile)

                    tmsTileY = int(os.path.splitext(yTileFile)[0])

                    self.inputTiles.append( (tmsTileX, tmsTileY, zoom, tilePath) )
                    if zoom not in self.inputFiles:
                        self.inputFiles[zoom] = dict()
                    if tmsTileX not in self.inputFiles[zoom]:
                        self.inputFiles[zoom][tmsTileX] = dict()
                    self.inputFiles[zoom][tmsTileX][tmsTileY] = tilePath

    # -------------------------------------------------------------------------
    def overlapTiles(self):

        processedTiles = 0
        for inputTile in self.inputTiles:

            tmsTileX = inputTile[0]
            tmsTileY = inputTile[1]
            tileZoom = inputTile[2]
            tmsTilesCount = 2**tileZoom
            tmsTileX_plus1 = (tmsTileX + 1) % tmsTilesCount
            tmsTileY_minus1 = (tmsTileY - 1) if (tmsTileY > 0) else (tmsTilesCount - 1)

            outputTileFile = os.path.join(self.outputDir, str(tileZoom), str(tmsTileX), "%s.%s" % (tmsTileY, self.options.extension))

            # Skip if already exists
            if os.path.exists(outputTileFile):
                print("Skipping ",tmsTileX,"x",tmsTileY,"@",tileZoom,"...")
                continue
            
            # Create directories for the tile
            if self.options.verbose:
                print("Baking ",tmsTileX,"x",tmsTileY,"@",tileZoom,"...")

            if not os.path.exists(os.path.dirname(outputTileFile)):
                os.makedirs(os.path.dirname(outputTileFile))

            # Open base tile
            baseDataset = gdal.Open(inputTile[3], gdal.GA_ReadOnly)
            if self.options.verbose:
                print("\t",baseDataset.RasterXSize,"x",baseDataset.RasterYSize," -> ",baseDataset.RasterXSize+1,"x",baseDataset.RasterYSize+1)

            # Create target dataset
            targetDataset = self.memDriver.Create('', baseDataset.RasterXSize+1, baseDataset.RasterYSize+1, 1, baseDataset.GetRasterBand(1).DataType)

            # Copy base to target
            if self.options.verbose:
                print("\t =0  ",tmsTileX,"x",tmsTileY,"@",tileZoom,"=",inputTile[3])
            targetDataset.WriteRaster(0, 0, baseDataset.RasterXSize, baseDataset.RasterYSize,
                baseDataset.ReadRaster(0, 0, baseDataset.RasterXSize, baseDataset.RasterYSize))
            del baseDataset

            # Right tile
            if tmsTileX_plus1 in self.inputFiles[tileZoom]:
                filename = self.inputFiles[tileZoom][tmsTileX_plus1][tmsTileY]
                dataset = gdal.Open(filename, gdal.GA_ReadOnly)

                if self.options.verbose:
                    print("\t +R  ",tmsTileX_plus1,"x",tmsTileY,"@",tileZoom,"=",filename)

                targetDataset.WriteRaster(targetDataset.RasterXSize - 1, 0, 1, dataset.RasterYSize,
                    dataset.ReadRaster(0, 0, 1, dataset.RasterYSize))
                
                del dataset

            # Bottom tile
            if tmsTileY_minus1 in self.inputFiles[tileZoom][tmsTileX]:
                filename = self.inputFiles[tileZoom][tmsTileX][tmsTileY_minus1]
                dataset = gdal.Open(filename, gdal.GA_ReadOnly)

                if self.options.verbose:
                    print("\t +B  ",tmsTileX,"x",tmsTileY_minus1,"@",tileZoom,"=",filename)

                targetDataset.WriteRaster(0, targetDataset.RasterYSize - 1, dataset.RasterXSize, 1,
                    dataset.ReadRaster(0, 0, dataset.RasterXSize, 1))
                
                del dataset

            # Bottom-right corner
            if tmsTileX_plus1 in self.inputFiles[tileZoom] and tmsTileY_minus1 in self.inputFiles[tileZoom][tmsTileX_plus1]:
                filename = self.inputFiles[tileZoom][tmsTileX_plus1][tmsTileY_minus1]
                dataset = gdal.Open(filename, gdal.GA_ReadOnly)

                if self.options.verbose:
                    print("\t +BR ",tmsTileX_plus1,"x",tmsTileY_minus1,"@",tileZoom,"=",filename)

                targetDataset.WriteRaster(targetDataset.RasterXSize - 1, targetDataset.RasterYSize - 1, 1, 1,
                    dataset.ReadRaster(0, 0, 1, 1))
                
                del dataset

            # Write target to file
            self.outDriver.CreateCopy(outputTileFile, targetDataset, strict=1, options = self.options.driverOptions)
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
