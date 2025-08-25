#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import os
from optparse import OptionParser
import sqlite3
#from gdal2tiles import GlobalMercator
from osgeo_utils.gdal2tiles import GlobalMercator

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

        p.set_defaults(verbose=False)

        self.parser = p

    # -------------------------------------------------------------------------
    def prepareInput(self):

        self.minZoom = 31
        self.maxZoom = 0
        self.inputTiles = list()
        self.mercator = GlobalMercator()

        # Collect all input files
        for zoomDir in os.listdir(self.inputPath):
            zoomPath = os.path.join(self.inputPath, zoomDir)
            if not os.path.isdir(zoomPath):
                continue
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

                    self.inputTiles.append((tmsTileX, tmsTileY, zoom, tilePath))

    # -------------------------------------------------------------------------
    def packTilesInDBs(self):
        dbFileName = os.path.join(self.outputDir, "tiles.sqlite")
        dbinsert = 0
        # Create database
        if os.path.exists(dbFileName):
            os.unlink(dbFileName)
        db = sqlite3.connect(dbFileName)

        # Create and fill info table
        c = db.cursor()
        c.execute(
            """
            CREATE TABLE info (
                desc TEXT,
                tilenumbering TEXT,
                minzoom INTEGER,
                maxzoom INTEGER,
                ellipsoid,
                inverted_y,
                timeSupported,
                tilesize
            )
            """)
        c.execute(
            """
            INSERT INTO info (
                desc,
                tilenumbering,
                minzoom,
                maxzoom,
                ellipsoid,
                inverted_y,
                timeSupported,
                tilesize
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                "World HeightMap",
                "OSM",
                self.minZoom,
                self.maxZoom,
                0,
                0,
                False,
                32,
            ))

        # Create tiles table and index
        c.execute(
            """
            CREATE TABLE tiles (
                x INTEGER,
                y INTEGER,
                z INTEGER,
                image BLOB,
                PRIMARY KEY(x, y, z)
            )
            """)
        c.execute(
            """
            CREATE INDEX _tiles_index
                ON tiles(x, y, z)
            """)

        # Free up current cursor
        c = None
        db.commit()

        # Insert files as blobs in database
        c = db.cursor()
        for tmsTileX, tmsTileY, zoom, tileFilename in self.inputTiles:
            maxTileIndex = 2**zoom - 1
            tileX = tmsTileX
            tileY = maxTileIndex - tmsTileY

            if self.options.verbose:
                print("... %dx%d@%d: %s" % (
                    tileX,
                    tileY,
                    zoom,
                    tileFilename,
                ))

            
            with open(tileFilename, "rb") as tileFile:
                c.execute(
                    """
                    INSERT INTO tiles (
                        x,
                        y,
                        z,
                        image
                    ) VALUES (?, ?, ?, ?)
                    """, (tileX, tileY, zoom, sqlite3.Binary(tileFile.read())))
                dbinsert = dbinsert + 1
            if dbinsert > 100:
                dbinsert = 0
                db.commit()
        # Release database
        db.commit()
        db.close()
        del db

    # -------------------------------------------------------------------------
    def error(self, msg, details = ""):
        if details:
            self.parser.error(msg + "\n\n" + details)
        else:
            self.parser.error(msg)

    # -------------------------------------------------------------------------
    def process(self):

        self.prepareInput()
        self.packTilesInDBs()

# =============================================================================
# =============================================================================
# =============================================================================

if __name__=='__main__':
    packer = OsmAndHeightMapPacker( sys.argv[1:] )
    packer.process()
