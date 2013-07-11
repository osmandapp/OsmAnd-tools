#!/usr/bin/python3

import sys
import os
from optparse import OptionParser, OptionGroup
import sqlite3
from lxml import etree
from gdal2tiles import GlobalMercator
import math

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
        self.mercator = GlobalMercator()

        # Open XML
        self.countriesDom = etree.parse(self.options.countries)
        if self.countriesDom == None:
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
    def packTilesInDBs(self):
        areas = self.countriesDom.xpath("//*[@heightmapArea='yes']")

        areasProcessed = 0
        for area in areas:
            areaType = area.tag

            if areaType == "region":
                regionName = area.get("name")
                countryName = area.getparent().get("name")
                continentName = area.getparent().getparent().get("name")
            elif areaType == "country":
                regionName = None
                countryName = area.get("name")
                continentName = area.getparent().get("name")
            elif areaType == "continent":
                regionName = None
                countryName = None
                continentName = area.get("name")

            areaName = "-".join([ item.capitalize() for item in filter(None, [ continentName, countryName, regionName ]) ])

            if self.options.verbose:
                print("Processing ",areaName)

            areaDbFileName = os.path.join(self.outputDir, areaName + ".heights.tiledb")
            
            areaBBox = [ int(item) for item in area.xpath("@bbox")[0].split(" ") ]
            
            # Decompress 1-degree tileset
            areaCompressed1DegreeTileSet = area.xpath("./tiles/text()")[0].split(";")
            area1DegreeTileSet = set()
            for compressedTile in areaCompressed1DegreeTileSet:
                if "x" not in compressedTile:
                    tileLonLat = [ int(item) for item in compressedTile.split(" ") ]
                    area1DegreeTileSet.add((tileLonLat[0], tileLonLat[1]))
                    continue

                borderTiles = compressedTile.split("x")
                borderTile0 = [ int(item) for item in filter(None, borderTiles[0].split(" ")) ]
                borderTile1 = [ int(item) for item in filter(None, borderTiles[1].split(" ")) ]
                
                lonMin = min(borderTile0[0], borderTile1[0])
                lonMax = max(borderTile0[0], borderTile1[0])
                latMin = min(borderTile0[1], borderTile1[1])
                latMax = max(borderTile0[1], borderTile1[1])

                for lon in range(lonMin, lonMax + 1):
                    for lat in range(latMin, latMax + 1):
                        area1DegreeTileSet.add((lon, lat))

            # Collect tiles for packing
            tilesForPacking = set()
            for zoom in range(self.minZoom, self.maxZoom + 1):

                maxTileIndex = 2**zoom - 1
                zoomBounds = self.zoomTileBounds[zoom]

                for tileLonLat in area1DegreeTileSet:
                    tiles = self.getTilesIn1Degree(tileLonLat[0], tileLonLat[1], zoom)
                    for tileId in tiles:

                        # Skip this tile if it is outside of available data
                        # Remember, tileId is in OSM, and data borders are in TMS
                        tmsX = tileId[0]
                        tmsY = maxTileIndex - tileId[1]
                        if tmsX < zoomBounds[0] or tmsX > zoomBounds[2] or tmsY < zoomBounds[1] or tmsY > zoomBounds[3]:
                            continue

                        tilesForPacking.add((zoom, tileId[0], tileId[1]))

            if self.options.verbose:
                print("\tTile files to pack:", len(tilesForPacking))

            # Create database
            if os.path.exists(areaDbFileName):
                os.unlink(areaDbFileName)
            db = sqlite3.connect(areaDbFileName)
            
            # Create and fill info table
            c = db.cursor()
            c.execute(
                """
                CREATE TABLE info (
                    desc TEXT,
                    tilenumbering TEXT,
                    minzoom INTEGER,
                    maxzoom INTEGER,
                    minlon INTEGER,
                    maxlon INTEGER,
                    minlat INTEGER,
                    maxlat INTEGER)
                """)
            c.execute(
                """
                INSERT INTO info (
                    desc,
                    tilenumbering,
                    minzoom,
                    maxzoom,
                    minlon,
                    maxlon,
                    minlat,
                    maxlat) VALUES ( ?, ?, ?, ?, ?, ?, ?, ? )
                """, (areaName + " HeightMap", "OSM", self.minZoom, self.maxZoom, areaBBox[0], areaBBox[2], areaBBox[1], areaBBox[3]))

            # Create tiles table and index
            c.execute(
                """
                CREATE TABLE tiles (
                    x INTEGER,
                    y INTEGER,
                    zoom INTEGER,
                    data BLOB,
                    PRIMARY KEY(x, y, zoom))
                """)
            c.execute(
                """
                CREATE INDEX idx
                    ON tiles(x, y, zoom)
                """)

            # Free up current cursor
            c = None
            db.commit()

            # Insert files as blobs in database
            tilesPacked = 0
            for tileId in tilesForPacking:
                zoom = tileId[0]
                tileX = tileId[1]
                tileY = tileId[2]

                maxTileIndex = 2**zoom - 1
                tmsX = tileX
                tmsY = maxTileIndex - tileY

                # Check file exists for this TMS tile
                if zoom not in self.inputFiles:
                    continue
                if tmsX not in self.inputFiles[zoom]:
                    continue
                if tmsY not in self.inputFiles[zoom][tmsX]:
                    continue

                tileFilename = self.inputFiles[zoom][tmsX][tmsY]            

                if self.options.verbose:
                    print("\t+", tileX, "x", tileY, "@", zoom, "from", tileFilename)
                
                c = db.cursor()
                with open(tileFilename, "rb") as tileFile:
                    c.execute(
                        """
                        INSERT INTO tiles (
                            x,
                            y,
                            zoom,
                            data) VALUES ( ?, ?, ?, ?, ?, ? )
                        """, (tileX, tileY, zoom, sqlite3.Binary(tileFile.read())))
                    db.commit()

                if not self.options.verbose:
                    tilesPacked += 1
                    self.progress(tilesPacked / float(len(tilesForPacking)))

            # Release database
            db.close()
            del db

            if not self.options.verbose:
                areasProcessed += 1
                self.progress(areasProcessed / float(len(areas)))

    # -------------------------------------------------------------------------
    def getTilesIn1Degree(self, lon, lat, zoom):
        tlTile = self.getTileId(lon, lat, zoom)
        brTile = self.getTileId(lon+1, lat-1, zoom)

        xMin = min(tlTile[0], brTile[0])
        xMax = max(tlTile[0], brTile[0])
        yMin = min(tlTile[1], brTile[1])
        yMax = max(tlTile[1], brTile[1])

        tiles = set()
        for x in range(xMin, xMax+1):
            for y in range(yMin, yMax+1):
                tiles.add((x, y))

        return tiles

    # -------------------------------------------------------------------------
    def normalizeLongitude(self, lon):
        while lon < -180.0 or lon > 180.0:
            if lon < 0.0:
                lon += 360.0
            else:
                lon -= 360.0
        return lon

    # -------------------------------------------------------------------------
    def normalizeLatitude(self, lat):
        while lat < -90.0 or lat > 90.0:
            if lat < 0.0:
                lat += 180.0
            else:
                lat -= 180.0

        if lat < -85.0511:
            return -85.0511
        elif lat > 85.0511:
            return 85.0511
    
        return lat

    # -------------------------------------------------------------------------
    def getTileId(self, lon, lat, zoom):
        lon = self.normalizeLongitude(lon)
        lat = self.normalizeLongitude(lat)
        zoomPow = 1 << zoom

        x = math.floor((lon + 180.0)/360.0 * zoomPow)

        y = math.log( math.tan(math.radians(lat)) + 1.0/math.cos(math.radians(lat)) )
        if math.isinf(y) or math.isnan(y):
            lat = -89.9 if lat < 0 else 89.9
            y = math.log( math.tan(math.radians(lat)) + 1.0/math.cos(math.radians(lat)) )
        y = math.floor((1.0 - y / math.pi) / 2.0 * zoomPow)

        return (x, y)
    
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
        self.packTilesInDBs()

# =============================================================================
# =============================================================================
# =============================================================================

if __name__=='__main__':
    packer = OsmAndHeightMapPacker( sys.argv[1:] )
    packer.process()
