#!/usr/bin/python
# create Osmand-compatible sqlite
# from a TMS sqlite

import math
import os, sys, pdb
import re
import sqlite3
import math

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
        
    def fromLL (self, lat_deg, lon_deg, zoom):
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

if len(sys.argv) <=2:
    print 'usage: ./tiles2osmandsqlite.py inputfile outputfile'
    exit()
infile=sys.argv[1]
outfile=sys.argv[2]

store=SqliteTileStorage('TMS')
store.open(infile)
store.createBigPlanetFromTMS(outfile, True)

