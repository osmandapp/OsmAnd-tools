#!/usr/bin/env python
#******************************************************************************
# From $Id: gdal2tiles.py 19288 2010-04-02 18:36:17Z rouault $
# VERSION MODIFIED FROM ORIGINAL, come with no warranty
# Yves Cainaud
# input: vrt file (-addalpha) in 3857 projection (projection is forced due
# to weird effect in AutoCreateWarpedVRT)
# 2 bands: 1 grayscale, one alpha mask

import sys

import sqlite3
import StringIO

try: sys.path.insert(0,'/Library/Frameworks/GDAL.framework/Versions/1.9/Python/2.6/site-packages')
except : pass
try:
	from osgeo import gdal
	from osgeo import osr
except:
	import gdal
	print('You are using "old gen" bindings. gdal2tiles needs "new gen" bindings.')
	sys.exit(1)

import os
import math

try:
	from PIL import Image, ImageFilter
	import numpy
	import osgeo.gdal_array as gdalarray
except:
	# 'antialias' resampling is not available
	pass

__version__ = "$Id: gdal2tiles.py 19288 2010-04-02 18:36:17Z rouault $"

resampling_list = ('antialias')
profile_list = ('mercator') 

class SqliteTileStorage():
	""" Sqlite files methods for simple tile storage"""

	def __init__(self, type):
		self.type=type
	
	def create(self, filename, overwrite=False):
		""" Create a new storage file, overwrite or not if already exists"""
		self.filename=filename
		CREATEINDEX=True
		
		if overwrite:
			if os.path.isfile(self.filename):
				os.unlink(self.filename)
		else:
			if os.path.isfile(self.filename):
				CREATEINDEX=False
				
		self.db = sqlite3.connect(self.filename)
		
		cur = self.db.cursor()
		cur.execute(
			"""
			CREATE TABLE IF NOT EXISTS tiles (
				x int,
				y int,
				z int, 
				s int,
				image blob,
				PRIMARY KEY(x,y,z,s))
			""")
		cur.execute(
			"""
			CREATE TABLE IF NOT EXISTS info (
				desc TEXT,
				tilenumbering TEXT,
				minzoom int,
				maxzoom int)
			""")
		
		if CREATEINDEX:
			cur.execute(
				"""
				CREATE INDEX IND
				ON tiles(x,y,z,s)
				""")
				
		cur.execute("insert into info(desc) values('Simple sqlite tile storage..')")
		
		cur.execute("insert into info(tilenumbering) values(?)",(self.type,))
		
		self.db.commit()
		
	def open(self, filename) :
		""" Open an existing file"""
		self.filename=filename
		if os.path.isfile(self.filename):
			self.db = sqlite3.connect(self.filename)
			return True
		else:
			return False
			
	def writeImageFile(self, x, y, z, f) :
		""" write a single tile from a file """
		cur = self.db.cursor()
		cur.execute('insert into tiles (z, x, y,s,image) \
						values (?,?,?,?,?)',
						(z, x, y, 0, sqlite3.Binary(f.read())))
		self.db.commit()
		
	def writeImage(self, x, y, z, image) :
		""" write a single tile from string """
		cur = self.db.cursor()
		cur.execute('insert into tiles (z, x, y,s,image) \
						values (?,?,?,?,?)',
						(z, x, y, 0, sqlite3.Binary(image)))
		self.db.commit()
		
	def readImage(self, x, y, z) :
		""" read a single tile as string """
		
		cur = self.db.cursor()
		cur.execute("select image from tiles where x=? and y=? and z=?", (x, y, z))
		res = cur.fetchone()
		if res:
			image = str(res[0])
			return image
		else :
			print "None found"
			return None
		
	def createFromDirectory(self, filename, basedir, overwrite=False) :
		""" Create a new sqlite file from a z/y/x.ext directory structure"""
		
		ls=os.listdir(basedir)
		
		self.create(filename, overwrite)
		cur = self.db.cursor()
		
		for zs in os.listdir(basedir):
			zz=int(zs)
			for xs in os.listdir(basedir+'/'+zs+'/'):
				xx=int(xs)
				for ys in os.listdir(basedir+'/'+zs+'/'+'/'+xs+'/'):
					yy=int(ys.split('.')[0])
					print zz, yy, xx
					z=zz
					x=xx
					y=yy
					print basedir+'/'+zs+'/'+'/'+xs+'/'+ys
					f=open(basedir+'/'+zs+'/'+'/'+xs+'/'+ys)
					cur.execute('insert into tiles (z, x, y,image) \
								values (?,?,?,?)',
								(z, x, y,  sqlite3.Binary(f.read())))
								
	def createBigPlanetFromTMS(self, targetname, overwrite=False):
		""" Create a new sqlite with BigPlanet numbering scheme from a TMS one"""
		target=SqliteTileStorage('BigPlanet')
		target.create(targetname, overwrite)
		cur = self.db.cursor()
		cur.execute("select x, y, z from tiles")
		res = cur.fetchall()
		for (x, y, z) in res:
			xx= x
			zz= 17 - z
			yy= 2**zz - y -1
			im=self.readImage(x,y,z)
			target.writeImage(xx,yy,zz,im)
		
	def createTMSFromBigPlanet(self, targetname, overwrite=False):
		""" Create a new sqlite with TMS numbering scheme from a BigPlanet one"""
		target=SqliteTileStorage('TMS')
		target.create(targetname, overwrite)
		cur = self.db.cursor()
		cur.execute("select x, y, z from tiles")
		res = cur.fetchall()
		for (x, y, z) in res:
			xx= x
			zz= 17 - z
			yy= 2**zz - y -1
			im=self.readImage(x,y,z)
			target.writeImage(xx,yy,zz,im)
	
	def createTMSFromOSM(self, targetname, overwrite=False):
		""" Create a new sqlite with TMS numbering scheme from a OSM/Bing/Googlemaps one"""
		target=SqliteTileStorage('TMS')
		target.create(targetname, overwrite)
		cur = self.db.cursor()
		cur.execute("select x, y, z from tiles")
		res = cur.fetchall()
		for (x, y, z) in res:
			xx= x
			zz= z
			yy= 2**zz - y
			im=self.readImage(x,y,z)
			target.writeImage(xx,yy,zz,im)
	
	def createOSMFromTMS(self, targetname, overwrite=False):
		""" Create a new sqlite with OSM/Bing/Googlemaps numbering scheme from a TMS one"""
		target=SqliteTileStorage('OSM')
		target.create(targetname, overwrite)
		cur = self.db.cursor()
		cur.execute("select x, y, z from tiles")
		res = cur.fetchall()
		for (x, y, z) in res:
			xx= x
			zz= z
			yy= 2**zz - y
			im=self.readImage(x,y,z)
			target.writeImage(xx,yy,zz,im)
		

# =============================================================================
# =============================================================================
# =============================================================================

__doc__globalmaptiles = """
globalmaptiles.py

Global Map Tiles as defined in Tile Map Service (TMS) Profiles
==============================================================

Functions necessary for generation of global tiles used on the web.
It contains classes implementing coordinate conversions for:

  - GlobalMercator (based on EPSG:900913 = EPSG:3785)
	   for Google Maps, Yahoo Maps, Microsoft Maps compatible tiles
  - GlobalGeodetic (based on EPSG:4326)
	   for OpenLayers Base Map and Google Earth compatible tiles

More info at:

http://wiki.osgeo.org/wiki/Tile_Map_Service_Specification
http://wiki.osgeo.org/wiki/WMS_Tiling_Client_Recommendation
http://msdn.microsoft.com/en-us/library/bb259689.aspx
http://code.google.com/apis/maps/documentation/overlays.html#Google_Maps_Coordinates

Created by Klokan Petr Pridal on 2008-07-03.
Google Summer of Code 2008, project GDAL2Tiles for OSGEO.

In case you use this class in your product, translate it to another language
or find it usefull for your project please let me know.
My email: klokan at klokan dot cz.
I would like to know where it was used.

Class is available under the open-source GDAL license (www.gdal.org).
"""

import math

MAXZOOMLEVEL = 32

class GlobalMercator(object):
	"""
	TMS Global Mercator Profile
	---------------------------

	Functions necessary for generation of tiles in Spherical Mercator projection,
	EPSG:900913 (EPSG:gOOglE, Google Maps Global Mercator), EPSG:3785, OSGEO:41001.

	Such tiles are compatible with Google Maps, Microsoft Virtual Earth, Yahoo Maps,
	UK Ordnance Survey OpenSpace API, ...
	and you can overlay them on top of base maps of those web mapping applications.
	
	Pixel and tile coordinates are in TMS notation (origin [0,0] in bottom-left).

	What coordinate conversions do we need for TMS Global Mercator tiles::

		 LatLon	  <->	   Meters	  <->	 Pixels	<->	   Tile	 

	 WGS84 coordinates   Spherical Mercator  Pixels in pyramid  Tiles in pyramid
		 lat/lon			XY in metres	 XY pixels Z zoom	  XYZ from TMS 
		EPSG:4326		   EPSG:900913										 
		 .----.			  ---------			   --				TMS	  
		/	  \	 <->	 |	   |	 <->	 /----/	<->	  Google	
		\	  /			 |	   |		   /--------/		  QuadTree   
		 -----			   ---------		 /------------/				   
	   KML, public		 WebMapService		 Web Clients	  TileMapService

	What is the coordinate extent of Earth in EPSG:900913?

	  [-20037508.342789244, -20037508.342789244, 20037508.342789244, 20037508.342789244]
	  Constant 20037508.342789244 comes from the circumference of the Earth in meters,
	  which is 40 thousand kilometers, the coordinate origin is in the middle of extent.
	  In fact you can calculate the constant as: 2 * math.pi * 6378137 / 2.0
	  $ echo 180 85 | gdaltransform -s_srs EPSG:4326 -t_srs EPSG:900913
	  Polar areas with abs(latitude) bigger then 85.05112878 are clipped off.

	What are zoom level constants (pixels/meter) for pyramid with EPSG:900913?

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

	The lat/lon coordinates are using WGS84 datum, yeh?

	  Yes, all lat/lon we are mentioning should use WGS84 Geodetic Datum.
	  Well, the web clients like Google Maps are projecting those coordinates by
	  Spherical Mercator, so in fact lat/lon coordinates on sphere are treated as if
	  the were on the WGS84 ellipsoid.
	 
	  From MSDN documentation:
	  To simplify the calculations, we use the spherical form of projection, not
	  the ellipsoidal form. Since the projection is used only for map display,
	  and not for displaying numeric coordinates, we don't need the extra precision
	  of an ellipsoidal projection. The spherical projection causes approximately
	  0.33 percent scale distortion in the Y direction, which is not visually noticable.

	How do I create a raster in EPSG:900913 and convert coordinates with PROJ.4?

	  You can use standard GIS tools like gdalwarp, cs2cs or gdaltransform.
	  All of the tools supports -t_srs 'epsg:900913'.

	  For other GIS programs check the exact definition of the projection:
	  More info at http://spatialreference.org/ref/user/google-projection/
	  The same projection is degined as EPSG:3785. WKT definition is in the official
	  EPSG database.

	  Proj4 Text:
		+proj=merc +a=6378137 +b=6378137 +lat_ts=0.0 +lon_0=0.0 +x_0=0.0 +y_0=0
		+k=1.0 +units=m +nadgrids=@null +no_defs

	  Human readable WKT format of EPGS:900913:
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

	def __init__(self, tileSize=256):
		"Initialize the TMS Global Mercator pyramid"
		self.tileSize = tileSize
		self.initialResolution = 2 * math.pi * 6378137 / self.tileSize
		# 156543.03392804062 for tileSize 256 pixels
		self.originShift = 2 * math.pi * 6378137 / 2.0
		# 20037508.342789244

	def LatLonToMeters(self, lat, lon ):
		"Converts given lat/lon in WGS84 Datum to XY in Spherical Mercator EPSG:900913"

		mx = lon * self.originShift / 180.0
		my = math.log( math.tan((90 + lat) * math.pi / 360.0 )) / (math.pi / 180.0)

		my = my * self.originShift / 180.0
		return mx, my

	def MetersToLatLon(self, mx, my ):
		"Converts XY point from Spherical Mercator EPSG:900913 to lat/lon in WGS84 Datum"

		lon = (mx / self.originShift) * 180.0
		lat = (my / self.originShift) * 180.0

		lat = 180 / math.pi * (2 * math.atan( math.exp( lat * math.pi / 180.0)) - math.pi / 2.0)
		return lat, lon

	def PixelsToMeters(self, px, py, zoom):
		"Converts pixel coordinates in given zoom level of pyramid to EPSG:900913"

		res = self.Resolution( zoom )
		mx = px * res - self.originShift
		my = py * res - self.originShift
		return mx, my
		
	def MetersToPixels(self, mx, my, zoom):
		"Converts EPSG:900913 to pyramid pixel coordinates in given zoom level"
				
		res = self.Resolution( zoom )
		px = (mx + self.originShift) / res
		py = (my + self.originShift) / res
		return px, py
	
	def PixelsToTile(self, px, py):
		"Returns a tile covering region in given pixel coordinates"

		tx = int( math.ceil( px / float(self.tileSize) ) - 1 )
		ty = int( math.ceil( py / float(self.tileSize) ) - 1 )
		return tx, ty

	def PixelsToRaster(self, px, py, zoom):
		"Move the origin of pixel coordinates to top-left corner"
		
		mapSize = self.tileSize << zoom
		return px, mapSize - py
		
	def MetersToTile(self, mx, my, zoom):
		"Returns tile for given mercator coordinates"
		
		px, py = self.MetersToPixels( mx, my, zoom)
		return self.PixelsToTile( px, py)

	def TileBounds(self, tx, ty, zoom):
		"Returns bounds of the given tile in EPSG:900913 coordinates"
		
		minx, miny = self.PixelsToMeters( tx*self.tileSize, ty*self.tileSize, zoom )
		maxx, maxy = self.PixelsToMeters( (tx+1)*self.tileSize, (ty+1)*self.tileSize, zoom )
		return ( minx, miny, maxx, maxy )

	def TileLatLonBounds(self, tx, ty, zoom ):
		"Returns bounds of the given tile in latutude/longitude using WGS84 datum"

		bounds = self.TileBounds( tx, ty, zoom)
		minLat, minLon = self.MetersToLatLon(bounds[0], bounds[1])
		maxLat, maxLon = self.MetersToLatLon(bounds[2], bounds[3])
		 
		return ( minLat, minLon, maxLat, maxLon )
		
	def Resolution(self, zoom ):
		"Resolution (meters/pixel) for given zoom level (measured at Equator)"
		
		# return (2 * math.pi * 6378137) / (self.tileSize * 2**zoom)
		return self.initialResolution / (2**zoom)
		
	def ZoomForPixelSize(self, pixelSize ):
		"Maximal scaledown zoom of the pyramid closest to the pixelSize."
		
		for i in range(MAXZOOMLEVEL):
			if pixelSize > self.Resolution(i):
				if i!=0:
					return i-1
				else:
					return 0 # We don't want to scale up
		
	def GoogleTile(self, tx, ty, zoom):
		"Converts TMS tile coordinates to Google Tile coordinates"
		
		# coordinate origin is moved from bottom-left to top-left corner of the extent
		return tx, (2**zoom - 1) - ty

	def QuadTree(self, tx, ty, zoom ):
		"Converts TMS tile coordinates to Microsoft QuadTree"
		
		quadKey = ""
		ty = (2**zoom - 1) - ty
		for i in range(zoom, 0, -1):
			digit = 0
			mask = 1 << (i-1)
			if (tx & mask) != 0:
				digit += 1
			if (ty & mask) != 0:
				digit += 2
			quadKey += str(digit)
			
		return quadKey


# =============================================================================
# =============================================================================
# =============================================================================

class GDAL2Tiles(object):

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
		
		self.stopped = False
		self.input = None
		self.output = None

		# Tile format
		self.tilesize = 256
		self.tiledriver = 'PNG'
		self.tileext = 'png'
		
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
		
		# RUN THE ARGUMENT PARSER:
		
		self.optparse_init()
		self.options, self.args = self.parser.parse_args(args=arguments)
		if not self.args:
			self.error("No input file specified")

		# POSTPROCESSING OF PARSED ARGUMENTS:

		# Workaround for old versions of GDAL
		try:
			if (self.options.verbose and self.options.resampling == 'near') or gdal.TermProgress_nocb:
				pass
		except:
			self.error("This version of GDAL is not supported. Please upgrade to 1.6+.")
			#,"You can try run crippled version of gdal2tiles with parameters: -v -r 'near'")
		
		# Is output directory the last argument?

		# Overwrite output, default to 'input.sqlitedb'
		self.output=self.args[-1]+'.sqlitedb'
		self.store = SqliteTileStorage('TMS')
		self.store.create(self.output,True) 

		# More files on the input not directly supported yet
		
		if (len(self.args) > 1):
			self.error("Processing of several input files is not supported.",
			"""Please first use a tool like gdal_vrtmerge.py or gdal_merge.py on the files:
gdal_vrtmerge.py -o merged.vrt %s""" % " ".join(self.args))
			# TODO: Call functions from gdal_vrtmerge.py directly
			
		self.input = self.args[0]
		
		# Supported options

		self.resampling = 'antialias'
		
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
			print("Options:", self.options)
			print("Input:", self.input)
			print("Output:", self.output)
			print("Cache: %s MB" % (gdal.GetCacheMax() / 1024 / 1024))
			print('')

	# -------------------------------------------------------------------------
	def optparse_init(self):
		"""Prepare the option parser for input (argv)"""
		
		from optparse import OptionParser, OptionGroup
		usage = "Usage: %prog [options] input_file(s) [output]"
		p = OptionParser(usage, version="%prog "+ __version__)
		p.add_option('-s', '--s_srs', dest="s_srs", metavar="SRS",
						  help="The spatial reference system used for the source input data")
		p.add_option('-z', '--zoom', dest="zoom",
						  help="Zoom levels to render (format:'2-5' or '10').")
		p.add_option('-e', '--resume', dest="resume", action="store_true",
						  help="Resume mode. Generate only missing files.")
		p.add_option('-a', '--srcnodata', dest="srcnodata", metavar="NODATA",
						  help="NODATA transparency value to assign to the input data")
		p.add_option("-v", "--verbose",
						  action="store_true", dest="verbose",
						  help="Print status messages to stdout")
		
		# TODO: MapFile + TileIndexes per zoom level for efficient MapServer WMS
		#g = OptionGroup(p, "WMS MapServer metadata", "Options for generated mapfile and tileindexes for MapServer")
		#g.add_option("-i", "--tileindex", dest='wms', action="store_true"
		#				 help="Generate tileindex and mapfile for MapServer (WMS)")
		# p.add_option_group(g)

		p.set_defaults(verbose=False, profile="mercator", resampling='antialias', resume=False)

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

		if self.options.verbose:
			print("Input file:", "( %sP x %sL - %s bands)" % (self.in_ds.RasterXSize, self.in_ds.RasterYSize, self.in_ds.RasterCount))

		if not self.in_ds:
			# Note: GDAL prints the ERROR message too
			self.error("It is not possible to open the input file '%s'." % self.input )
			
		# Read metadata from the input file
		if self.in_ds.RasterCount == 0:
			self.error( "Input file '%s' has no raster band" % self.input )
			

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
			print("NODATA: %s" % self.in_nodata)

		#
		# Here we should have RGBA input dataset opened in self.in_ds
		#

		if self.options.verbose:
			print("Preprocessed file:", "( %sP x %sL - %s bands)" % (self.in_ds.RasterXSize, self.in_ds.RasterYSize, self.in_ds.RasterCount))

		# Spatial Reference System of the input raster

		self.in_srs = osr.SpatialReference()
		self.in_srs.ImportFromEPSG(3857)
		self.in_srs_wkt = self.in_srs.ExportToWkt()

		# Spatial Reference System of tiles
		
		self.out_srs = osr.SpatialReference()
		self.out_srs.ImportFromEPSG(3857)
		
		# Are the reference systems the same? Reproject if necessary.

		self.out_ds = None
		
					
		if (self.in_ds.GetGeoTransform() == (0.0, 1.0, 0.0, 0.0, 0.0, 1.0)) and (self.in_ds.GetGCPCount() == 0):
			self.error("There is no georeference - neither affine transformation (worldfile) nor GCPs. You can generate only 'raster' profile tiles.",
			"Either gdal2tiles with parameter -p 'raster' or use another GIS software for georeference e.g. gdal_transform -gcp / -a_ullr / -a_srs")
		
		if self.out_ds and self.options.verbose:
			print("Projected file:", "tiles.vrt", "( %sP x %sL - %s bands)" % (self.out_ds.RasterXSize, self.out_ds.RasterYSize, self.out_ds.RasterCount))
	
		if not self.out_ds:
			self.out_ds = self.in_ds

		#
		# Here we should have a raster (out_ds) in the correct Spatial Reference system
		#

		# Get alpha band (either directly or from NODATA value)
		
		# MOD XX
		
		# rem if no alpha band self.alphaband = self.out_ds.GetRasterBand(1)
		self.grayband = self.out_ds.GetRasterBand(1)

		# Read the georeference 

		self.out_gt = self.out_ds.GetGeoTransform()
			
			
		# Report error in case rotation/skew is in geotransform (possible only in 'raster' profile)
		if (self.out_gt[2], self.out_gt[4]) != (0,0):
			self.error("Georeference of the raster contains rotation or skew. Such raster is not supported. Please use gdalwarp first.")
			# TODO: Do the warping in this case automaticaly

		#
		# Here we expect: pixel is square, no rotation on the raster
		#

		# Output Bounds - coordinates in the output SRS
		self.ominx = self.out_gt[0]
		self.omaxx = self.out_gt[0]+self.out_ds.RasterXSize*self.out_gt[1]
		self.omaxy = self.out_gt[3]
		self.ominy = self.out_gt[3]-self.out_ds.RasterYSize*self.out_gt[1]
		# Note: maybe round(x, 14) to avoid the gdal_translate behaviour, when 0 becomes -1e-15

		if self.options.verbose:
			print("Bounds (output srs):", round(self.ominx, 13), self.ominy, self.omaxx, self.omaxy)

		#
		# Calculating ranges for tiles in different zoom levels
		#

		self.mercator = GlobalMercator() # from globalmaptiles.py
		
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
			self.tminz = self.mercator.ZoomForPixelSize( self.out_gt[1] * max( self.out_ds.RasterXSize, self.out_ds.RasterYSize) / float(self.tilesize) )

		# Get the maximal zoom level (closest possible zoom level up on the resolution of raster)
		
		if self.tmaxz == None:
			self.tmaxz = self.mercator.ZoomForPixelSize( self.out_gt[1] )
		
		if self.options.verbose:
			print("Bounds (latlong):", self.mercator.MetersToLatLon( self.ominx, self.ominy), self.mercator.MetersToLatLon( self.omaxx, self.omaxy))
			print('MinZoomLevel:', self.tminz)
			print("MaxZoomLevel:", self.tmaxz, "(", self.mercator.Resolution( self.tmaxz ),")")

	# -------------------------------------------------------------------------
	def generate_base_tiles(self):
		"""Generation of the base tiles (the lowest in the pyramid) directly from the input raster"""
		
		print("Generating Base Tiles:")
		
		if self.options.verbose:
			#mx, my = self.out_gt[0], self.out_gt[3] # OriginX, OriginY
			#px, py = self.mercator.MetersToPixels( mx, my, self.tmaxz)
			#print "Pixel coordinates:", px, py, (mx, my)
			print('')
			print("Tiles generated from the max zoom level:")
			print("----------------------------------------")
			print('')


		# Set the bounds
		tminx, tminy, tmaxx, tmaxy = self.tminmax[self.tmaxz]

		# Just the center tile
		#tminx = tminx+ (tmaxx - tminx)/2
		#tminy = tminy+ (tmaxy - tminy)/2
		#tmaxx = tminx
		#tmaxy = tminy
		
		ds = self.out_ds
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


				# Tile bounds in EPSG:900913
				b = self.mercator.TileBounds(tx, ty, tz)
				#print "\tgdalwarp -ts 256 256 -te %s %s %s %s %s %s_%s_%s.tif" % ( b[0], b[1], b[2], b[3], "tiles.vrt", tz, tx, ty)

				# Don't scale up by nearest neighbour, better change the querysize
				# to the native resolution (and return smaller query tile) for scaling
				rb, wb = self.geo_query( ds, b[0], b[3], b[2], b[1])
				nativesize = wb[0]+wb[2] # Pixel size in the raster covering query geo extent
				#if self.options.verbose:
				#	print("\tNative Extent (querysize",nativesize,"): ", rb, wb)
				# Tile bounds in raster coordinates for ReadRaster query
				rb, wb = self.geo_query( ds, b[0], b[3], b[2], b[1], querysize=querysize)
				rx, ry, rxsize, rysize = rb
				wx, wy, wxsize, wysize = wb
				

				# Tile dataset in memory
				dstile = self.mem_drv.Create('', self.tilesize, self.tilesize, 1)
				## data is replaced by black data later
				data = self.grayband.ReadRaster(rx, ry, rxsize, rysize, wxsize, wysize)
				# rem if no alpha band alpha = self.alphaband.ReadRaster(rx, ry, rxsize, rysize, wxsize, wysize)

				# Big ReadRaster query in memory scaled to the tilesize - all but 'near' algo
				# rem if no alpha band dsquery = self.mem_drv.Create('', querysize, querysize, 2)
				dsquery = self.mem_drv.Create('', querysize, querysize, 1)
				dsquery.WriteRaster(wx, wy, wxsize, wysize, data, band_list=[1])
				# rem if no alpha band dsquery.WriteRaster(wx, wy, wxsize, wysize, alpha, band_list=[2])
				self.scale_query_to_tile(dsquery, dstile, tx, ty, tz)
				del dsquery
				del data
				# rem if no alpha band del alpha
				del dstile
					
				if not self.options.verbose:
					self.progressbar( ti / float(tcount) )
		
	# -------------------------------------------------------------------------
	def generate_overview_tiles(self):
		"""Generation of the overview tiles (higher in the pyramid) based on existing tiles"""
		
		print("Generating Overview Tiles:")
		
		
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

					dsquery = Image.new('RGBA',(2*self.tilesize, 2*self.tilesize))
					dstile = Image.new('RGBA',(self.tilesize, self.tilesize))

					children = []
					# Read the tiles and write them to query window
					for y in range(2*ty,2*ty+2):
						for x in range(2*tx,2*tx+2):
							minx, miny, maxx, maxy = self.tminmax[tz+1]
							if x >= minx and x <= maxx and y >= miny and y <= maxy:
								#dsquerytile = gdal.Open( os.path.join( self.output, str(tz+1), str(x), "%s.%s" % (y, self.tileext)), gdal.GA_ReadOnly)
								#dsquerytile = Image.open(os.path.join(self.output, str(tz+1), str(x), "%s.%s" % (y, self.tileext)))
								buf= StringIO.StringIO()
								buf.write(self.store.readImage(x,y,tz+1))
								buf.seek(0)
								dsquerytile = Image.open(buf)
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

								box= (tileposx, tileposy)
								dsquery.paste(dsquerytile,box)
								children.append( [x, y, tz+1] )

					#self.scale_query_to_tile(dsquery, dstile, tilefilename)
					
					## Write a copy of tile to png/jpg
					#if self.options.resampling != 'antialias':
						## Write a copy of tile to png/jpg
						#self.out_drv.CreateCopy(tilefilename, dstile, strict=0)
					
					dstile = dsquery.resize((self.tilesize, self.tilesize),Image.BILINEAR)
					#dstile.save(tilefilename)
					buf= StringIO.StringIO()
					dstile.save(buf,self.tiledriver)
					self.store.writeImage(tx, ty, tz, buf.getvalue())

					if self.options.verbose:
						print("\tbuild from zoom", tz+1," tiles:", (2*tx, 2*ty), (2*tx+1, 2*ty),(2*tx, 2*ty+1), (2*tx+1, 2*ty+1))

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
	def scale_query_to_tile(self, dsquery, dstile, tx, ty, tz):
		"""Scales down query dataset to the tile dataset"""

		querysize = dsquery.RasterXSize
		tilesize = dstile.RasterXSize
		# Scaling by PIL (Python Imaging Library) - improved Lanczos
		
		# mod XX: to accomodate with 2 band hilshades
		array = numpy.zeros((querysize, querysize, 4), numpy.uint8)
		white=numpy.zeros((querysize, querysize), numpy.uint8)
		white.fill(255)
		#for i in [0,1,2]:
		#	array[:,:,i] = gdalarray.BandReadAsArray(dsquery.GetRasterBand(1), 0, 0, querysize, querysize)
		
		hillshade = 255 - gdalarray.BandReadAsArray(dsquery.GetRasterBand(1), 0, 0, querysize, querysize) 
		#mask = gdalarray.BandReadAsArray(dsquery.GetRasterBand(2), 0, 0, querysize, querysize) / 255
		array[:,:,3] = hillshade #*mask

		im = Image.fromarray(array, 'RGBA') # Always four bands
		im1 = im.filter(ImageFilter.BLUR).resize((tilesize,tilesize), Image.ANTIALIAS)
		#im1.save(tilefilename,self.tiledriver)
		buf= StringIO.StringIO()
		im1.save(buf,self.tiledriver)
		self.store.writeImage(tx, ty, tz, buf.getvalue())
			

	# -------------------------------------------------------------------------

# =============================================================================
# =============================================================================
# =============================================================================

if __name__=='__main__':
	argv = gdal.GeneralCmdLineProcessor( sys.argv )
	if argv:
		gdal2tiles = GDAL2Tiles( argv[1:] )
		gdal2tiles.process()
