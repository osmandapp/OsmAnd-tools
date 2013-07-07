#! /usr/bin/python

import sys

try:
    from osgeo import gdal
    from osgeo import osr
except:
    import gdal
    print 'You are using "old gen" bindings. OsmAnd HeightMap Slicer needs "new gen" bindings.'
    sys.exit(1)

import os
import math
from gdal2tiles import GlobalMercator

MAXZOOMLEVEL = 32

# =============================================================================
# =============================================================================
# =============================================================================

class OsmAndHeightMapSlicer(object):

    # -------------------------------------------------------------------------
    def process(self):
        """The main processing function, runs all the main steps of processing"""

        # Opening and preprocessing of the input file
        self.open_input()

        # Generation of the lowest tiles
        self.generate_base_tiles()

        # Generation of the overview tiles (higher in the pyramid)
        self.generate_overview_tiles()

    # -------------------------------------------------------------------------
    def error(self, msg, details = "" ):
        """Print an error message and stop the processing"""

        if details:
            self.parser.error(msg + "\n\n" + details)
        else:
            self.parser.error(msg)

    # -------------------------------------------------------------------------
    def progressbar(self, complete = 0.0):
        """Print progressbar for float value 0..1"""

        gdal.TermProgress_nocb(complete)

    # -------------------------------------------------------------------------
    def stop(self):
        """Stop the rendering immediately"""
        self.stopped = True

    # -------------------------------------------------------------------------
    def __init__(self, arguments ):
        """Constructor function - initialization"""

        # Check version of GDAL
        gdalVersion = gdal.VersionInfo()
        if gdalVersion < 1900:
            self.error("GDAL version 1.9+ required")
        
        self.stopped = False
        self.input = None
        self.output = None

        # RUN THE ARGUMENT PARSER:

        self.optparse_init()
        self.options, self.args = self.parser.parse_args(args=arguments)
        if not self.args:
            self.error("No input file specified")

        # POSTPROCESSING OF PARSED ARGUMENTS:

        # Tile format
        self.tilesize = self.options.size
        self.tiledriver = self.options.driver
        self.tileext = self.options.extension

        # Should we read bigger window of the input raster and scale it down?
        # Note: Modified leter by open_input()
        # Not for 'near' resampling
        # Not for Wavelet based drivers (JPEG2000, ECW, MrSID)
        # Not for 'raster' profile
        self.scaledquery = True
        # How big should be query window be for scaling down
        # Later on reset according the chosen resampling algorightm
        self.querysize = 4 * self.tilesize

        # Should we use Read on the input file for generating overview tiles?
        # Note: Modified later by open_input()
        # Otherwise the overview tiles are generated from existing underlying tiles
        self.overviewquery = False

        # Is output directory the last argument?

        # Test output directory, if it doesn't exist
        if os.path.isdir(self.args[-1]) or ( len(self.args) > 1 and not os.path.exists(self.args[-1])):
            self.output = self.args[-1]
            self.args = self.args[:-1]

        # More files on the input not directly supported yet

        if (len(self.args) > 1):
            self.error("Processing of several input files is not supported.",
            """Please first use a tool like gdal_vrtmerge.py or gdal_merge.py on the files:
gdal_vrtmerge.py -o merged.vrt %s""" % " ".join(self.args))

        self.input = self.args[0]

        # Default values for not given options

        if not self.output:
            # Directory with input filename without extension in actual directory
            self.output = os.path.splitext(os.path.basename( self.input ))[0]

        # Supported options

        self.resampling = None

        # if self.options.resampling == 'average':
        #     try:
        #         if gdal.RegenerateOverview:
        #             pass
        #     except:
        #         self.error("'average' resampling algorithm is not available.", "Please use -r 'near' argument or upgrade to newer version of GDAL.")

        # elif self.options.resampling == 'antialias':
        #     try:
        #         if numpy:
        #             pass
        #     except:
        #         self.error("'antialias' resampling algorithm is not available.", "Install PIL (Python Imaging Library) and numpy.")

        # elif self.options.resampling == 'near':
        #     self.resampling = gdal.GRA_NearestNeighbour
        #     self.querysize = self.tilesize

        # elif self.options.resampling == 'bilinear':
        #     self.resampling = gdal.GRA_Bilinear
        #     self.querysize = self.tilesize * 2

        # elif self.options.resampling == 'cubic':
        #     self.resampling = gdal.GRA_Cubic

        # elif self.options.resampling == 'cubicspline':
        #     self.resampling = gdal.GRA_CubicSpline

        # elif self.options.resampling == 'lanczos':
        #     self.resampling = gdal.GRA_Lanczos

        # User specified zoom levels
        self.tminz = None
        self.tmaxz = None
        if self.options.zoom:
            minmax = self.options.zoom.split('-',1)
            minmax.extend([''])
            min, max = minmax[:2]
            self.tminz = int(min)
            if max:
                self.tmaxz = int(max)
            else:
                self.tmaxz = int(min) 

        # Output the results

        if self.options.verbose:
            print "Options: %s" % self.options
            print "Input: %s" % self.input
            print "Output: %s" % self.output
            print "Cache: %s MB" % (gdal.GetCacheMax() / 1024 / 1024)
            print ''

    # -------------------------------------------------------------------------
    def optparse_init(self):
        """Prepare the option parser for input (argv)"""

        from optparse import OptionParser, OptionGroup
        usage = "Usage: %prog [options] input_file(s) [output]"
        p = OptionParser(usage)
        p.add_option("-v", "--verbose", action="store_true", dest="verbose",
            help="Print status messages to stdout")
        p.add_option('--zoom', dest="zoom",
            help="Zoom levels to render (format:'2-5' or '10').")
        p.add_option('--resume', dest="resume", action="store_true",
            help="Resume mode. Generate only missing files.")
        p.add_option('--srcnodata', dest="srcnodata", metavar="NODATA",
            help="NODATA transparency value to assign to the input data")
        p.add_option("--size", dest="size", type="int",
            help="Size of tile.")
        p.add_option("--driver", dest="driver", type="string",
            help="Size of tile.")
        p.add_option("--extension", dest="extension", type="string",
            help="Size of tile.")

        p.set_defaults(verbose=False, resume=False)

        self.parser = p

    # -------------------------------------------------------------------------
    def open_input(self):
        """Initialization of the input raster, reprojection if necessary"""

        gdal.AllRegister()

        # Initialize necessary GDAL drivers

        self.out_drv = gdal.GetDriverByName( self.tiledriver )
        self.mem_drv = gdal.GetDriverByName( 'MEM' )

        if not self.out_drv:
            raise Exception("The '%s' driver was not found, is it available in this GDAL build?", self.tiledriver)
        if not self.mem_drv:
            raise Exception("The 'MEM' driver was not found, is it available in this GDAL build?")

        # Open the input file

        if self.input:
            self.in_ds = gdal.Open(self.input, gdal.GA_ReadOnly)
        else:
            raise Exception("No input file was specified")

        if not self.in_ds:
            # Note: GDAL prints the ERROR message too
            self.error("It is not possible to open the input file '%s'." % self.input )

        # Read metadata from the input file
        if self.in_ds.RasterCount != 1:
            self.error( "Input file '%s' must have 1 raster band" % self.input )

        if self.in_ds.GetRasterBand(1).GetRasterColorTable():
            self.error( "Input file '%s' must not have color table" % self.input )

        # Get NODATA value
        self.in_nodata = []
        for i in range(1, self.in_ds.RasterCount+1):
            if self.in_ds.GetRasterBand(i).GetNoDataValue() != None:
                self.in_nodata.append( self.in_ds.GetRasterBand(i).GetNoDataValue() )
        if self.options.srcnodata:
            nds = list(map( float, self.options.srcnodata.split(',')))
            if len(nds) < self.in_ds.RasterCount:
                self.in_nodata = (nds * self.in_ds.RasterCount)[:self.in_ds.RasterCount]
            else:
                self.in_nodata = nds

        if self.options.verbose:
            print "Input file: ( %sP x %sL - %s bands, nodata = %s)" % (self.in_ds.RasterXSize, self.in_ds.RasterYSize, self.in_ds.RasterCount, self.in_nodata)

        #
        # Here we should have RGBA input dataset opened in self.in_ds
        #

        # Read the georeference 

        self.out_gt = self.in_ds.GetGeoTransform()

        # Report error in case rotation/skew is in geotransform (possible only in 'raster' profile)
        if (self.out_gt[2], self.out_gt[4]) != (0,0):
            self.error("Georeference of the raster contains rotation or skew. Such raster is not supported. Please use gdalwarp first.")

        #
        # Here we expect: pixel is square, no rotation on the raster
        #

        # Output Bounds - coordinates in the output SRS
        self.ominx = self.out_gt[0]
        self.omaxx = self.out_gt[0]+self.in_ds.RasterXSize*self.out_gt[1]
        self.omaxy = self.out_gt[3]
        self.ominy = self.out_gt[3]-self.in_ds.RasterYSize*self.out_gt[1]

        if self.options.verbose:
            print "Bounds: %s %s %s %s" % (round(self.ominx, 13), self.ominy, self.omaxx, self.omaxy)

        #
        # Calculating ranges for tiles in different zoom levels
        #

        self.mercator = GlobalMercator(self.tilesize) # from globalmaptiles.py

        # Function which generates SWNE in LatLong for given tile
        self.tileswne = self.mercator.TileLatLonBounds

        # Generate table with min max tile coordinates for all zoomlevels
        self.tminmax = list(range(0,32))
        for tz in range(0, 32):
            tminx, tminy = self.mercator.MetersToTile( self.ominx, self.ominy, tz )
            tmaxx, tmaxy = self.mercator.MetersToTile( self.omaxx, self.omaxy, tz )
            # crop tiles extending world limits (+-180,+-90)
            tminx, tminy = max(0, tminx), max(0, tminy)
            tmaxx, tmaxy = min(2**tz-1, tmaxx), min(2**tz-1, tmaxy)
            self.tminmax[tz] = (tminx, tminy, tmaxx, tmaxy)

        # TODO: Maps crossing 180E (Alaska?)

        # Get the minimal zoom level (map covers area equivalent to one tile) 
        if self.tminz == None:
            self.tminz = self.mercator.ZoomForPixelSize( self.out_gt[1] * max( self.in_ds.RasterXSize, self.in_ds.RasterYSize) / float(self.tilesize) )

        # Get the maximal zoom level (closest possible zoom level up on the resolution of raster)
        if self.tmaxz == None:
            self.tmaxz = self.mercator.ZoomForPixelSize( self.out_gt[1] )

        if self.options.verbose:
            print "Bounds (latlong): %s %s" % (self.mercator.MetersToLatLon( self.ominx, self.ominy), self.mercator.MetersToLatLon( self.omaxx, self.omaxy))
            print "MinZoomLevel: %s" % self.tminz
            print "MaxZoomLevel: %s (%s)" % (self.tmaxz, self.mercator.Resolution( self.tmaxz ))

    # -------------------------------------------------------------------------
    def generate_base_tiles(self):
        """Generation of the base tiles (the lowest in the pyramid) directly from the input raster"""

        print "Generating Base Tiles:"

        if self.options.verbose:
            print ''
            print "Tiles generated from the max zoom level:"
            print "----------------------------------------"
            print ''

        # Set the bounds
        tminx, tminy, tmaxx, tmaxy = self.tminmax[self.tmaxz]

        ds = self.in_ds
        querysize = self.querysize

        #print tminx, tminy, tmaxx, tmaxy
        tcount = (1+abs(tmaxx-tminx)) * (1+abs(tmaxy-tminy))
        #print tcount
        ti = 0

        tz = self.tmaxz
        for ty in range(tmaxy, tminy-1, -1): #range(tminy, tmaxy+1):
            for tx in range(tminx, tmaxx+1):

                if self.stopped:
                    break
                ti += 1
                tilefilename = os.path.join(self.output, str(tz), str(tx), "%s.%s" % (ty, self.tileext))
                if self.options.verbose:
                    print "(%s / %s, %s)" % (ti, tcount, tilefilename)

                if self.options.resume and os.path.exists(tilefilename):
                    if self.options.verbose:
                        print "Tile generation skiped because of --resume"
                    else:
                        self.progressbar( ti / float(tcount) )
                    continue

                # Create directories for the tile
                if not os.path.exists(os.path.dirname(tilefilename)):
                    os.makedirs(os.path.dirname(tilefilename))

                # Get bounds of this tile
                bounds = self.mercator.TileBounds(tx, ty, tz)
                if self.options.verbose:
                    print "\tTile bounds %s %s %s %s" % (bounds[0], bounds[1], bounds[2], bounds[3])

                # Don't scale up by nearest neighbour, better change the querysize
                # to the native resolution (and return smaller query tile) for scaling

                rb, wb = self.geo_query(ds, bounds[0], bounds[1], bounds[2], bounds[3])
                nativesize = wb[0]+wb[2] # Pixel size in the raster covering query geo extent
                if self.options.verbose:
                    print "\tNative Extent (querysize %s): %s %s " % (nativesize, rb, wb)

                # Tile bounds in raster coordinates for ReadRaster query
                rb, wb = self.geo_query(ds, bounds[0], bounds[1], bounds[2], bounds[3], querysize=querysize)

                rx, ry, rxsize, rysize = rb
                wx, wy, wxsize, wysize = wb

                if self.options.verbose:
                    print "\tReadRaster Extent: (%s, %s, %s, %s) (%s, %s, %s, %s)" % (rx, ry, rxsize, rysize, wx, wy, wxsize, wysize)

                # Query is in 'nearest neighbour' but can be bigger in then the tilesize
                # We scale down the query to the tilesize by supplied algorithm.

                # Tile dataset in memory
                dstile = self.mem_drv.Create('', self.tilesize, self.tilesize, 1)
                data = ds.ReadRaster(rx, ry, rxsize, rysize, wxsize, wysize)
                
                if self.tilesize == querysize:
                    # Use the ReadRaster result directly in tiles ('nearest neighbour' query)
                    dstile.WriteRaster(wx, wy, wxsize, wysize, data)
                    
                    # Note: For source drivers based on WaveLet compression (JPEG2000, ECW, MrSID)
                    # the ReadRaster function returns high-quality raster (not ugly nearest neighbour)
                    # TODO: Use directly 'near' for WaveLet files
                else:
                    # Big ReadRaster query in memory scaled to the tilesize - all but 'near' algo
                    dsquery = self.mem_drv.Create('', querysize, querysize, 1)
                    # TODO: fill the null value in case a tile without alpha is produced (now only png tiles are supported)
                    #for i in range(1, tilebands+1):
                    #   dsquery.GetRasterBand(1).Fill(tilenodata)
                    dsquery.WriteRaster(wx, wy, wxsize, wysize, data)
                    
                    self.scale_query_to_tile(dsquery, dstile, tilefilename)
                    del dsquery

                del data

                # Write a copy of tile to png/jpg
                self.out_drv.CreateCopy(tilefilename, dstile, strict=0)

                del dstile

                if not self.options.verbose:
                    self.progressbar( ti / float(tcount) )

    # -------------------------------------------------------------------------
    def generate_overview_tiles(self):
        """Generation of the overview tiles (higher in the pyramid) based on existing tiles"""

        print "Generating Overview Tiles:"

        # Usage of existing tiles: from 4 underlying tiles generate one as overview.

        tcount = 0
        for tz in range(self.tmaxz-1, self.tminz-1, -1):
            tminx, tminy, tmaxx, tmaxy = self.tminmax[tz]
            tcount += (1+abs(tmaxx-tminx)) * (1+abs(tmaxy-tminy))

        ti = 0

        # querysize = tilesize * 2

        for tz in range(self.tmaxz-1, self.tminz-1, -1):
            tminx, tminy, tmaxx, tmaxy = self.tminmax[tz]
            for ty in range(tmaxy, tminy-1, -1): #range(tminy, tmaxy+1):
                for tx in range(tminx, tmaxx+1):

                    if self.stopped:
                        break

                    ti += 1
                    tilefilename = os.path.join( self.output, str(tz), str(tx), "%s.%s" % (ty, self.tileext) )

                    if self.options.verbose:
                        print "(%s / %s, %s)" % (ti, tcount, tilefilename)

                    if self.options.resume and os.path.exists(tilefilename):
                        if self.options.verbose:
                            print "Tile generation skiped because of --resume"
                        else:
                            self.progressbar( ti / float(tcount) )
                        continue

                    # Create directories for the tile
                    if not os.path.exists(os.path.dirname(tilefilename)):
                        os.makedirs(os.path.dirname(tilefilename))

                    dsquery = self.mem_drv.Create('', 2*self.tilesize, 2*self.tilesize, 1)
                    # TODO: fill the null value
                    #for i in range(1, tilebands+1):
                    #   dsquery.GetRasterBand(1).Fill(tilenodata)
                    dstile = self.mem_drv.Create('', self.tilesize, self.tilesize, 1)

                    # TODO: Implement more clever walking on the tiles with cache functionality
                    # probably walk should start with reading of four tiles from top left corner
                    # Hilbert curve

                    children = []
                    # Read the tiles and write them to query window
                    for y in range(2*ty,2*ty+2):
                        for x in range(2*tx,2*tx+2):
                            minx, miny, maxx, maxy = self.tminmax[tz+1]
                            if x >= minx and x <= maxx and y >= miny and y <= maxy:
                                dsquerytile = gdal.Open( os.path.join( self.output, str(tz+1), str(x), "%s.%s" % (y, self.tileext)), gdal.GA_ReadOnly)
                                if (ty==0 and y==1) or (ty!=0 and (y % (2*ty)) != 0):
                                    tileposy = 0
                                else:
                                    tileposy = self.tilesize
                                if tx:
                                    tileposx = x % (2*tx) * self.tilesize
                                elif tx==0 and x==1:
                                    tileposx = self.tilesize
                                else:
                                    tileposx = 0
                                dsquery.WriteRaster( tileposx, tileposy, self.tilesize, self.tilesize,
                                    dsquerytile.ReadRaster(0,0,self.tilesize,self.tilesize))
                                children.append( [x, y, tz+1] )

                    self.scale_query_to_tile(dsquery, dstile, tilefilename)
                    
                    # Write a copy of tile
                    self.out_drv.CreateCopy(tilefilename, dstile, strict=0)

                    if self.options.verbose:
                        print "\tbuild from zoom %s tiles: (%s, %s) (%s, %s) (%s, %s) (%s, %s)" % (tz+1, 2*tx, 2*ty, 2*tx+1, 2*ty, 2*tx, 2*ty+1, 2*tx+1, 2*ty+1)

                    if not self.options.verbose:
                        self.progressbar( ti / float(tcount) )


    # -------------------------------------------------------------------------
    def geo_query(self, ds, ulx, uly, lrx, lry, querysize = 0):
        """For given dataset and query in cartographic coordinates
        returns parameters for ReadRaster() in raster coordinates and
        x/y shifts (for border tiles). If the querysize is not given, the
        extent is returned in the native resolution of dataset ds."""

        geotran = ds.GetGeoTransform()
        rx= int((ulx - geotran[0]) / geotran[1] + 0.001)
        ry= int((uly - geotran[3]) / geotran[5] + 0.001)
        rxsize= int((lrx - ulx) / geotran[1] + 0.5)
        rysize= int((lry - uly) / geotran[5] + 0.5)

        if not querysize:
            wxsize, wysize = rxsize, rysize
        else:
            wxsize, wysize = querysize, querysize

        # Coordinates should not go out of the bounds of the raster
        wx = 0
        if rx < 0:
            rxshift = abs(rx)
            wx = int( wxsize * (float(rxshift) / rxsize) )
            wxsize = wxsize - wx
            rxsize = rxsize - int( rxsize * (float(rxshift) / rxsize) )
            rx = 0
        if rx+rxsize > ds.RasterXSize:
            wxsize = int( wxsize * (float(ds.RasterXSize - rx) / rxsize) )
            rxsize = ds.RasterXSize - rx

        wy = 0
        if ry < 0:
            ryshift = abs(ry)
            wy = int( wysize * (float(ryshift) / rysize) )
            wysize = wysize - wy
            rysize = rysize - int( rysize * (float(ryshift) / rysize) )
            ry = 0
        if ry+rysize > ds.RasterYSize:
            wysize = int( wysize * (float(ds.RasterYSize - ry) / rysize) )
            rysize = ds.RasterYSize - ry

        return (rx, ry, rxsize, rysize), (wx, wy, wxsize, wysize)

    # -------------------------------------------------------------------------
    def scale_query_to_tile(self, dsquery, dstile, tilefilename=''):
        """Scales down query dataset to the tile dataset"""

        querysize = dsquery.RasterXSize
        tilesize = dstile.RasterXSize
        tilebands = dstile.RasterCount

        if self.resampling == None:

            # Function: gdal.RegenerateOverview()
            res = gdal.RegenerateOverview( dsquery.GetRasterBand(1), dstile.GetRasterBand(1), 'average' )
            if res != 0:
                self.error("RegenerateOverview() failed on %s, error %d" % (tilefilename, res))

        else:

            # Other algorithms are implemented by gdal.ReprojectImage().
            dsquery.SetGeoTransform( (0.0, tilesize / float(querysize), 0.0, 0.0, 0.0, tilesize / float(querysize)) )
            dstile.SetGeoTransform( (0.0, 1.0, 0.0, 0.0, 0.0, 1.0) )

            res = gdal.ReprojectImage(dsquery, dstile, None, None, self.resampling)
            if res != 0:
                self.error("ReprojectImage() failed on %s, error %d" % (tilefilename, res))

# =============================================================================
# =============================================================================
# =============================================================================

if __name__=='__main__':
    argv = gdal.GeneralCmdLineProcessor( sys.argv )
    if argv:
        slicer = OsmAndHeightMapSlicer( argv[1:] )
        slicer.process()
