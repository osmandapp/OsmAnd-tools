#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import os
from optparse import OptionParser
from osgeo import gdal

import math

class WeatherGeoTiffSlicer(object):
    # -------------------------------------------------------------------------
    def __init__(self, arguments):

        # Analyze arguments
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

        usage = "Usage: %prog [options] input_weather_geotiff_file output_path"
        p = OptionParser(usage)
        p.add_option("--verbose", action="store_true", dest="verbose",
            help="Print status messages to stdout")
        p.add_option("--zoom", dest="zoom", type="int",
            help="Base zoom.")
        p.add_option("--extraPoints", dest="extraPoints", type="int",
            help="Extra points.")
        p.add_option("--disableStatWarning", action="store_true", dest="disableStatWarning",
            help="Disable statistics warnings for bands with no valid pixels.")

        p.set_defaults(verbose=False, zoom=4, tileSize=256, extraPoints=2, disableStatWarning=False)

        self.parser = p

    # -------------------------------------------------------------------------
    def process(self):

        baseZoom = self.options.zoom
        geoTilesCount = 1 << baseZoom
        geoTileExtraPoints = self.options.extraPoints

        tileSize = 512

        self.geoDS = gdal.Open(self.inputPath, gdal.GA_ReadOnly)
        if not self.geoDS:
            self.error("Failed to open input file using GDAL")

        self.inputGeoTransform = self.geoDS.GetGeoTransform()
        if (self.inputGeoTransform[2], self.inputGeoTransform[4]) != (0, 0):
            self.error("Georeference of the raster must not contain rotation or skew")

        self.pixelWidth = self.inputGeoTransform[1]
        self.pixelWidth2 = self.pixelWidth / 2.0

        self.inputBoundsMinX = self.inputGeoTransform[0]
        self.inputBoundsMaxX = self.inputGeoTransform[0] + self.geoDS.RasterXSize * self.pixelWidth
        self.inputBoundsMaxY = self.inputGeoTransform[3]
        self.inputBoundsMinY = self.inputGeoTransform[3] - self.geoDS.RasterYSize * self.pixelWidth

        if self.inputBoundsMinX > -5 and self.inputBoundsMaxX > 185:
            warpOptions = gdal.WarpOptions(
                format="GTiff",
                outputBounds=[-180.125, -90.125, 180.125, 90.125],
                warpOptions=["SOURCE_EXTRA=1000"],
            )
            inputDataset = gdal.Warp(
                "/vsimem/input_dataset.tif", self.geoDS, options=warpOptions
            )
        else:
            inputDataset = self.geoDS

        self.mercator = GlobalMercator(tileSize)
        geoTileExt = geoTileExtraPoints * self.pixelWidth

        for geoTileY in range(geoTilesCount):
            for geoTileX in range(geoTilesCount):

                inputBoundsMinX, inputBoundsMinY = self.mercator.PixelsToMeters(
                    geoTileX * tileSize,
                    geoTileY * tileSize,
                    baseZoom,
                )
                inputBoundsMaxX, inputBoundsMaxY = self.mercator.PixelsToMeters(
                    (geoTileX + 1) * tileSize,
                    (geoTileY + 1) * tileSize,
                    baseZoom,
                )

                minLat, minLon = self.mercator.MetersToLatLon(inputBoundsMinX, inputBoundsMaxY)
                maxLat, maxLon = self.mercator.MetersToLatLon(inputBoundsMaxX, inputBoundsMinY)

                minLat = minLat + geoTileExt
                maxLat = maxLat - geoTileExt
                minLon = minLon - geoTileExt
                maxLon = maxLon + geoTileExt

                minLat = minLat - minLat % self.pixelWidth + self.pixelWidth2
                maxLat = maxLat - maxLat % self.pixelWidth + self.pixelWidth2
                minLon = minLon - minLon % self.pixelWidth - self.pixelWidth2
                maxLon = maxLon - maxLon % self.pixelWidth + self.pixelWidth2

                #normalize using source gtiff bounds

                """
                if minLon < mercatorMinLon:
                    minLon = mercatorMinLon
                if maxLon > mercatorMaxLon:
                    maxLon = mercatorMaxLon
                if maxLat < mercatorMinLat:
                    maxLat = mercatorMinLat
                if minLat > mercatorMaxLat:
                    minLat = mercatorMaxLat
                """

                geoTileOutputFile = os.path.join(
                    self.outputDir,
                    "%d_%d_%d.tiff" % (baseZoom, geoTileX, geoTileY),
                )

                # Create directories for the tile
                if not os.path.exists(os.path.dirname(geoTileOutputFile)):
                    os.makedirs(os.path.dirname(geoTileOutputFile))

                translateOptions = gdal.TranslateOptions(
                    format="GTiff",
                    projWin=[minLon, minLat, maxLon, maxLat],
                    stats=True,
                )
                
                if self.options.disableStatWarning:
                    def suppress_stats_warning_handler(err_class, err_num, err_msg):
                        if "Failed to compute statistics, no valid pixels found in sampling" not in err_msg:
                            print(f"GDAL Error: {err_msg}")
                    
                    gdal.PushErrorHandler(suppress_stats_warning_handler)
                
                geoTileDS = gdal.Translate(geoTileOutputFile, inputDataset, options=translateOptions)
                
                if self.options.disableStatWarning:
                    gdal.PopErrorHandler()
                geoTileDS = None

        inputDataset = None
        self.geoDS = None
        

    # -------------------------------------------------------------------------
    def error(self, msg, details=""):
        if details:
            self.parser.error(msg + "\n\n" + details)
        else:
            self.parser.error(msg)


# =============================================================================
# =============================================================================
# =============================================================================

MAXZOOMLEVEL = 32

class GlobalMercator(object):
    r"""
    TMS Global Mercator Profile
    ---------------------------

    Functions necessary for generation of tiles in Spherical Mercator projection,
    EPSG:3857.

    Such tiles are compatible with Google Maps, Bing Maps, Yahoo Maps,
    UK Ordnance Survey OpenSpace API, ...
    and you can overlay them on top of base maps of those web mapping applications.

    Pixel and tile coordinates are in TMS notation (origin [0,0] in bottom-left).

    What coordinate conversions do we need for TMS Global Mercator tiles::

         LatLon      <->       Meters      <->     Pixels    <->       Tile

     WGS84 coordinates   Spherical Mercator  Pixels in pyramid  Tiles in pyramid
         lat/lon            XY in meters     XY pixels Z zoom      XYZ from TMS
        EPSG:4326           EPSG:387
         .----.              ---------               --                TMS
        /      \     <->     |       |     <->     /----/    <->      Google
        \      /             |       |           /--------/          QuadTree
         -----               ---------         /------------/
       KML, public         WebMapService         Web Clients      TileMapService

    What is the coordinate extent of Earth in EPSG:3857?

      [-20037508.342789244, -20037508.342789244, 20037508.342789244, 20037508.342789244]
      Constant 20037508.342789244 comes from the circumference of the Earth in meters,
      which is 40 thousand kilometers, the coordinate origin is in the middle of extent.
      In fact you can calculate the constant as: 2 * math.pi * 6378137 / 2.0
      $ echo 180 85 | gdaltransform -s_srs EPSG:4326 -t_srs EPSG:3857
      Polar areas with abs(latitude) bigger then 85.05112878 are clipped off.

    What are zoom level constants (pixels/meter) for pyramid with EPSG:3857?

      whole region is on top of pyramid (zoom=0) covered by 256x256 pixels tile,
      every lower zoom level resolution is always divided by two
      initialResolution = 20037508.342789244 * 2 / 256 = 156543.03392804062

    What is the difference between TMS and Google Maps/QuadTree tile name convention?

      The tile raster itself is the same (equal extent, projection, pixel size),
      there is just different identification of the same raster tile.
      Tiles in TMS are counted from [0,0] in the bottom-left corner, id is XYZ.
      Google placed the origin [0,0] to the top-left corner, reference is XYZ.
      Microsoft is referencing tiles by a QuadTree name, defined on the website:
      http://msdn2.microsoft.com/en-us/library/bb259689.aspx

    The lat/lon coordinates are using WGS84 datum, yes?

      Yes, all lat/lon we are mentioning should use WGS84 Geodetic Datum.
      Well, the web clients like Google Maps are projecting those coordinates by
      Spherical Mercator, so in fact lat/lon coordinates on sphere are treated as if
      the were on the WGS84 ellipsoid.

      From MSDN documentation:
      To simplify the calculations, we use the spherical form of projection, not
      the ellipsoidal form. Since the projection is used only for map display,
      and not for displaying numeric coordinates, we don't need the extra precision
      of an ellipsoidal projection. The spherical projection causes approximately
      0.33 percent scale distortion in the Y direction, which is not visually
      noticeable.

    How do I create a raster in EPSG:3857 and convert coordinates with PROJ.4?

      You can use standard GIS tools like gdalwarp, cs2cs or gdaltransform.
      All of the tools supports -t_srs 'epsg:3857'.

      For other GIS programs check the exact definition of the projection:
      More info at http://spatialreference.org/ref/user/google-projection/
      The same projection is designated as EPSG:3857. WKT definition is in the
      official EPSG database.

      Proj4 Text:
        +proj=merc +a=6378137 +b=6378137 +lat_ts=0.0 +lon_0=0.0 +x_0=0.0 +y_0=0
        +k=1.0 +units=m +nadgrids=@null +no_defs

      Human readable WKT format of EPSG:3857:
         PROJCS["Google Maps Global Mercator",
             GEOGCS["WGS 84",
                 DATUM["WGS_1984",
                     SPHEROID["WGS 84",6378137,298.257223563,
                         AUTHORITY["EPSG","7030"]],
                     AUTHORITY["EPSG","6326"]],
                 PRIMEM["Greenwich",0],
                 UNIT["degree",0.0174532925199433],
                 AUTHORITY["EPSG","4326"]],
             PROJECTION["Mercator_1SP"],
             PARAMETER["central_meridian",0],
             PARAMETER["scale_factor",1],
             PARAMETER["false_easting",0],
             PARAMETER["false_northing",0],
             UNIT["metre",1,
                 AUTHORITY["EPSG","9001"]]]
    """

    def __init__(self, tile_size: int = 256) -> None:
        "Initialize the TMS Global Mercator pyramid"
        self.tile_size = tile_size
        self.initialResolution = 2 * math.pi * 6378137 / self.tile_size
        # 156543.03392804062 for tile_size 256 pixels
        self.originShift = 2 * math.pi * 6378137 / 2.0
        # 20037508.342789244

    def LatLonToMeters(self, lat, lon):
        "Converts given lat/lon in WGS84 Datum to XY in Spherical Mercator EPSG:3857"

        mx = lon * self.originShift / 180.0
        my = math.log(math.tan((90 + lat) * math.pi / 360.0)) / (math.pi / 180.0)

        my = my * self.originShift / 180.0
        return mx, my

    def MetersToLatLon(self, mx, my):
        "Converts XY point from Spherical Mercator EPSG:3857 to lat/lon in WGS84 Datum"

        lon = (mx / self.originShift) * 180.0
        lat = (my / self.originShift) * 180.0

        lat = 180 / math.pi * (2 * math.atan(math.exp(lat * math.pi / 180.0)) - math.pi / 2.0)
        return lat, lon

    def PixelsToMeters(self, px, py, zoom):
        "Converts pixel coordinates in given zoom level of pyramid to EPSG:3857"

        res = self.Resolution(zoom)
        mx = px * res - self.originShift
        my = py * res - self.originShift
        return mx, my

    def MetersToPixels(self, mx, my, zoom):
        "Converts EPSG:3857 to pyramid pixel coordinates in given zoom level"

        res = self.Resolution(zoom)
        px = (mx + self.originShift) / res
        py = (my + self.originShift) / res
        return px, py

    def PixelsToTile(self, px, py):
        "Returns a tile covering region in given pixel coordinates"

        tx = int(math.ceil(px / float(self.tile_size)) - 1)
        ty = int(math.ceil(py / float(self.tile_size)) - 1)
        return tx, ty

    def PixelsToRaster(self, px, py, zoom):
        "Move the origin of pixel coordinates to top-left corner"

        mapSize = self.tile_size << zoom
        return px, mapSize - py

    def MetersToTile(self, mx, my, zoom):
        "Returns tile for given mercator coordinates"

        px, py = self.MetersToPixels(mx, my, zoom)
        return self.PixelsToTile(px, py)

    def TileBounds(self, tx, ty, zoom):
        "Returns bounds of the given tile in EPSG:3857 coordinates"

        minx, miny = self.PixelsToMeters(tx * self.tile_size, ty * self.tile_size, zoom)
        maxx, maxy = self.PixelsToMeters((tx + 1) * self.tile_size, (ty + 1) * self.tile_size, zoom)
        return (minx, miny, maxx, maxy)

    def TileLatLonBounds(self, tx, ty, zoom):
        "Returns bounds of the given tile in latitude/longitude using WGS84 datum"

        bounds = self.TileBounds(tx, ty, zoom)
        minLat, minLon = self.MetersToLatLon(bounds[0], bounds[1])
        maxLat, maxLon = self.MetersToLatLon(bounds[2], bounds[3])

        return (minLat, minLon, maxLat, maxLon)

    def Resolution(self, zoom):
        "Resolution (meters/pixel) for given zoom level (measured at Equator)"

        # return (2 * math.pi * 6378137) / (self.tile_size * 2**zoom)
        return self.initialResolution / (2 ** zoom)

    def ZoomForPixelSize(self, pixelSize):
        "Maximal scaledown zoom of the pyramid closest to the pixelSize."

        for i in range(MAXZOOMLEVEL):
            if pixelSize > self.Resolution(i):
                return max(0, i - 1)  # We don't want to scale up
        return MAXZOOMLEVEL - 1

    def GoogleTile(self, tx, ty, zoom):
        "Converts TMS tile coordinates to Google Tile coordinates"

        # coordinate origin is moved from bottom-left to top-left corner of the extent
        return tx, (2 ** zoom - 1) - ty

    def QuadTree(self, tx, ty, zoom):
        "Converts TMS tile coordinates to Microsoft QuadTree"

        quadKey = ""
        ty = (2 ** zoom - 1) - ty
        for i in range(zoom, 0, -1):
            digit = 0
            mask = 1 << (i - 1)
            if (tx & mask) != 0:
                digit += 1
            if (ty & mask) != 0:
                digit += 2
            quadKey += str(digit)

        return quadKey

# =============================================================================
# =============================================================================
# =============================================================================

if __name__ == "__main__":
    slicer = WeatherGeoTiffSlicer(sys.argv[1:])
    slicer.process()
