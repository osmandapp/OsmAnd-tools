#!/usr/bin/python
# create Osmand-compatible sqlite from a big sqlite, only for areas with
# hillshade_sqlite='yes' in tile list xml file.

import os, sys, pdb
import sqlite3
import math
from lxml import etree
from optparse import OptionParser

class TileNames (object):
    """
    >>> l = Layer("name", maxresolution=0.019914, size="256,256")
    >>> t = Tile(l, 18, 20, 0)
    """
    __slots__ = ( "x", "y", "z" )
    def __init__ (self, x=0, y=0, z=0):
        """
        >>> l = Layer("name", maxresolution=0.019914, size="256,256")
        >>> t = Tile(l, 18, 20, 0)
        >>> t.x 
        18
        >>> t.y
        20
        >>> t.z
        0
        >>> print t.data
        None
        """
        self.x = x
        self.y = y
        self.z = z

    def size (self):
        return 256
        
    def params (self):
        return(self.x, self.y,self.z)
        
    def bounds (self):
        # return bounds of a tile
        n = 2.0 ** self.z
        minlon_deg = (self.x) / n * 360.0 - 180.0
        ytile = (n-1) -(self.y) ##beware, TMS spec !
        lat_rad = math.atan(math.sinh(math.pi * (1 - 2 * ytile / n)))
        minlat_deg = math.degrees(lat_rad)
        
        maxlon_deg = (self.x+1) / n * 360.0 - 180.0
        ytile = (n-1) -(self.y+1) ##beware, TMS spec !
        lat_rad = math.atan(math.sinh(math.pi * (1 - 2 * ytile / n)))
        maxlat_deg = math.degrees(lat_rad)
        return (minlon_deg, minlat_deg, maxlon_deg, maxlat_deg)

    def center(self):
        # return tile center coordinate
        n = 2.0 ** self.z
        lon_deg = (self.x+0.5) / n * 360.0 - 180.0
        ytile = (n-1) -(self.y+0.5) ##beware, TMS spec !
        lat_rad = math.atan(math.sinh(math.pi * (1 - 2 * ytile / n)))
        lat_deg = math.degrees(lat_rad)
        return(lon_deg, lat_deg)
        
    def toGoogle(self):
        # reverse y
        return(self.x, int(2**self.z - self.y),self.z)
        
    def toOsmand(self):
        # reverse y
        return(self.x, int(2**self.z - self.y -1),int(17-self.z))
        
    def fromLL(self, lat_deg, lon_deg, zoom):
        lat_rad = math.radians(lat_deg)
        n = 2.0 ** zoom
        x_tile = (lon_deg + 180.0) / 360.0 * n
        y_tile = (1.0 - math.log(math.tan(lat_rad) + (1 / math.cos(lat_rad))) / math.pi) / 2.0 * n
        #url = 'http://c.tile.openstreetmap.org/'+str(zoom)+'/'+str(xtile)+'/'+str(ytile)+'.png'
        y_tile = (2**zoom-1) - int(y_tile) ##beware, TMS spec !
        self.x=int(x_tile)
        self.y=int(y_tile)
        self.z=int(zoom)
        
        return self
 
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
                desc,
                tilenumbering,
                minzoom,
                maxzoom)
            """)
        
        if CREATEINDEX:
            cur.execute(
                """
                CREATE INDEX IND
                ON tiles(x,y,z,s)
                """)
                
        cur.execute("insert into info (desc) values ('Simple sqlite tile storage..')")
        self.db.commit()
        cur.execute("update info set tilenumbering=?",(self.type,))
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
        try:
            cur.execute('insert into tiles (z, x, y,s,image) \
                            values (?,?,?,?,?)',
                            (z, x, y, 0, sqlite3.Binary(f.read())))
            self.db.commit()
        except:
            print "failed to write", x, y, z
            sys.stdout.flush()
            cur.execute('insert into tiles (z, x, y,s,image) \
                            values (?,?,?,?,?)',
                            (z, x, y, 0, sqlite3.Binary(f.read())))
            self.db.commit()
            print "second attempt ok"
        
    def writeImage(self, x, y, z, image) :
        """ write a single tile from string """
        cur = self.db.cursor()
        try:
	        cur.execute('insert or replace into tiles (z, x, y,s,image) \
	                        values (?,?,?,?,?)',
	                        (z, x, y, 0, sqlite3.Binary(image)))
	        self.db.commit()
        except:
            print "failed to write", x, y, z
            sys.stdout.flush()
            cur.execute('insert or replace into tiles (z, x, y,s,image) \
                            values (?,?,?,?,?)',
                            (z, x, y, 0, sqlite3.Binary(image)))
            self.db.commit()
            print "second attempt ok"
        
    def readImage(self, x, y, z) :
        """ read a single tile as string """
        
        cur = self.db.cursor()
        cur.execute("select image from tiles where x=? and y=? and z=?", (x, y, z))
        res = cur.fetchone()
        if res:
            image = str(res[0])
            return image
        else :
            return None
        
    def updateMinMaxZoom(self):
        cur = self.db.cursor()
        cur.execute("select z from tiles")
        zoom = cur.fetchall()
        zoom.sort()
        minZoom= str(zoom[0][0])
        maxZoom= str(zoom[-1][0])
        cur.execute("update info set minzoom=?",(minZoom,))
        self.db.commit()
        cur.execute("update info set maxzoom=?",(maxZoom,))
        self.db.commit()
        
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
            yy= 2**z - y -1
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

def listTiles(tree):
    tilelists=tree.findall('.//tiles')
    tilelist=[]
    for l in tilelists:
        try: tilelist.extend(l.text.split(';'))
        except: continue
    return list(set(tilelist)) 

parser = OptionParser()
parser.add_option("-i", "--input", dest="inputXMLfilename",
                  help="XML file with file to generate and tilelists (countries.xml)", metavar="FILE")
parser.add_option("-s", "--sqlite", dest="inputSqlite",
                  help="Original Sqlite file to read", metavar="FILE")
parser.add_option("-o", "--output", dest="outputDirectory",
                  help="Output directory", metavar="FILE")


(options, args) = parser.parse_args()
if not options.inputXMLfilename:
    print "You must provide an input filename, -h or --help for help"
    exit(1)
if not options.outputDirectory:
    print "You must provide an output directory, -h or --help for help"
    exit(1)
if not options.inputSqlite:
    print "You must provide an input sqlite, -h or --help for help"
    exit(1)
 
inputXMLfilename=options.inputXMLfilename
outputDirectory=options.outputDirectory
inputSqlite=options.inputSqlite

inTree=etree.parse(inputXMLfilename)

Aois=inTree.xpath("//*[@hillshade_sqlite='yes']")
for area in Aois:
    # Find a name for the area of interest
    areaType=area.tag
    if areaType=='region':
        region=area.get('name')
        country=area.getparent().get('name')
        continent=area.getparent().getparent().get('name')
        name=country+'_'+region+'_'+continent
    if areaType=='country':
        region=''
        country=area.get('name')
        continent=area.getparent().get('name')
        name=country+'_'+continent
    if areaType=='continent':
        region=''
        country=''
        continent=area.get('name')
        name=continent
    print "Processing ", name
    outName='Hillshade_'+name.capitalize()+'.sqlitedb'
    finalName='Hillshade_'+name.capitalize()+'.sqlitedb.work'
    
    # Get tile list from this record and children
    tile1degList=listTiles(area)
    
    
    outputFilename=os.path.join(outputDirectory,outName)
    finalOutputFilename=os.path.join(outputDirectory,finalName)
    if os.path.exists(outputFilename):
        print "file exists, skip", outputFilename
        continue
    outputUploadedFilename=os.path.join(outputDirectory,"uploaded/"+outName)
    if os.path.exists(outputUploadedFilename):
        print "file exists, skip", outputUploadedFilename
        continue
    sys.stdout.flush()
    
    store=SqliteTileStorage('TMS')
    store.open(inputSqlite)
    out_storeTMS=SqliteTileStorage('BigPlanet')
    out_storeTMS.create(outputFilename,True)
    
    for tile1deg in tile1degList:
        lon=float(tile1deg.split(' ')[0])
        lat=float(tile1deg.split(' ')[1])
        print lon,lat
        sys.stdout.flush()
        for z in range(4,12):
            tile=TileNames()
            tile.fromLL(lat,lon,float(z))
            xmin=tile.x
            ymin=tile.y
            tile.fromLL(lat+1,lon+1,z)
            xmax=tile.x
            ymax=tile.y
            for xx in range(xmin-1, xmax+1):
                for yy in range(ymin-1, ymax+1):
                    tile=store.readImage(xx,yy,z)
                    xxx= xx
                    zzz= 17 - z
                    yyy= 2**z - yy -1
                    exists=out_storeTMS.readImage(xxx,yyy,zzz)
                    if tile and not exists : out_storeTMS.writeImage(xxx,yyy,zzz,tile)
    
    out_storeTMS.updateMinMaxZoom()
    out_storeTMS.db.close()
    os.rename(outputFilename, finalOutputFilename)
    sys.stdout.flush()
    



