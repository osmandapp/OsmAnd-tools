#!/usr/bin/python3

import sys
import os
from optparse import OptionParser, OptionGroup
import sqlite3
import etree from lxml

# =============================================================================
# =============================================================================
# =============================================================================

class OsmAndHeightMapPacker(object):
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
        p.add_option("--countries", dest="countries", type="string",
            help="Countries definition file.")

        p.set_defaults(verbose=False)

        self.parser = p

    # -------------------------------------------------------------------------
    def prepareInput(self):

        self.minZoom = 31
        self.maxZoom = 0
        self.zoomTileBounds = list(range(0, 32))
        self.inputTiles = list()
        self.inputFiles = dict()

        # Open XML
        self.contriesDom = etree.parse(self.options.countries)
        if self.contriesDom == None:
            self.error("Failed to open countries XML file")

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
    def packTilesByCountries(self):

    
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
        self.packTilesByCountries()

# =============================================================================
# =============================================================================
# =============================================================================

if __name__=='__main__':
    packer = OsmAndHeightMapPacker( argv[1:] )
    packer.process()
