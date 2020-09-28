#!/usr/bin/env python
# -*- coding: utf-8 -*-

''' ogr2osm beta

This program takes any vector data understadable by OGR and outputs an OSM file
with that data.

By default tags will be naively copied from the input data. Hooks are provided
so that, with a little python programming, you can translate the tags however
you like. More hooks are provided so you can filter or even modify the features
themselves.

To use the hooks, create a file in the translations/ directory called myfile.py
and run ogr2osm.py -t myfile. This file should define a function with the name
of each hook you want to use. For an example, see the uvmtrans.py file.

The program will use projection metadata from the source, if it has any. If
there is no projection information, or if you want to override it, you can use
-e or -p to specify an EPSG code or Proj.4 string, respectively. If there is no
projection metadata and you do not specify one, EPSG:4326 will be used (WGS84
latitude-longitude)

For additional usage information, run ogr2osm.py --help

Copyright (c) 2012-2013 Paul Norman <penorman@mac.com>, Sebastiaan Couwenberg 
<sebastic@xs4all.nl>, The University of Vermont <andrew.guertin@uvm.edu>

Released under the MIT license: http://opensource.org/licenses/mit-license.php

Based very heavily on code released under the following terms:

(c) Iván Sánchez Ortega, 2009
<ivan@sanchezortega.es>
###############################################################################
#  "THE BEER-WARE LICENSE":                                                   #
#  <ivan@sanchezortega.es> wrote this file. As long as you retain this notice #
#  you can do whatever you want with this stuff. If we meet some day, and you #
#  think this stuff is worth it, you can buy me a beer in return.             #
###############################################################################

'''


import sys
import os
import optparse
import logging as l
import re
l.basicConfig(level=l.DEBUG, format="%(message)s")

from osgeo import ogr
from osgeo import osr
from geom import *


'''

See http://lxml.de/tutorial.html for the source of the includes

lxml should be the fastest method

'''

try:
    from lxml import etree
    l.debug("running with lxml.etree")
except ImportError:
    try:
        # Python 2.5
        import xml.etree.ElementTree as etree
        l.debug("running with ElementTree on Python 2.5+")
    except ImportError:
        try:
            # normal cElementTree install
            import cElementTree as etree
            l.debug("running with cElementTree")
        except ImportError:
            try:
                # normal ElementTree install
                import elementtree.ElementTree as etree
                l.debug("running with ElementTree")
            except ImportError:
                l.error("Failed to import ElementTree from any known place")
                raise


# Setup program usage
usage = """%prog SRCFILE

SRCFILE can be a file path or a org PostgreSQL connection string such as:
"PG:dbname=pdx_bldgs user=emma host=localhost" (including the quotes)"""
parser = optparse.OptionParser(usage=usage)
parser.add_option("-t", "--translation", dest="translationMethod",
                  metavar="TRANSLATION",
                  help="Select the attribute-tags translation method. See " +
                  "the translations/ directory for valid values.")
parser.add_option("-o", "--output", dest="outputFile", metavar="OUTPUT",
                  help="Set destination .osm file name and location.")
parser.add_option("-e", "--epsg", dest="sourceEPSG", metavar="EPSG_CODE",
                  help="EPSG code of source file. Do not include the " +
                       "'EPSG:' prefix. If specified, overrides projection " +
                       "from source metadata if it exists.")
parser.add_option("-p", "--proj4", dest="sourcePROJ4", metavar="PROJ4_STRING",
                  help="PROJ.4 string. If specified, overrides projection " +
                       "from source metadata if it exists.")
parser.add_option("-v", "--verbose", dest="verbose", action="store_true")
parser.add_option("-d", "--debug-tags", dest="debugTags", action="store_true",
                  help="Output the tags for every feature parsed.")
parser.add_option("-f", "--force", dest="forceOverwrite", action="store_true",
                  help="Force overwrite of output file.")

parser.add_option("--encoding", dest="encoding",
                  help="Encoding of the source file. If specified, overrides " +
                  "the default of utf-8", default="utf-8")

parser.add_option("--significant-digits",  dest="significantDigits", type=int,
                  help="Number of decimal places for coordinates", default=9)
                  
parser.add_option("--rounding-digits",  dest="roundingDigits", type=int,
                  help="Number of decimal places for rounding", default=7)

parser.add_option("--no-memory-copy", dest="noMemoryCopy", action="store_true",
                    help="Do not make an in-memory working copy")
                    
parser.add_option("--no-upload-false", dest="noUploadFalse", action="store_true",
                    help="Omit upload=false from the completed file to surpress JOSM warnings when uploading.")

parser.add_option("--id", dest="id", type=int, default=0,
                    help="ID to start counting from for the output file. Defaults to 0.")

parser.add_option("--idfile", dest="idfile", type=str, default=None,
                    help="Read ID to start counting from from a file.")

parser.add_option("--saveid", dest="saveid", type=str, default=None,
                    help="Save last ID after execution to a file.")

# Positive IDs can cause big problems if used inappropriately so hide the help for this
parser.add_option("--positive-id", dest="positiveID", action="store_true",
                    help=optparse.SUPPRESS_HELP)

# Add version attributes. Again, this can cause big problems so surpress the help
parser.add_option("--add-version", dest="addVersion", action="store_true",
                    help=optparse.SUPPRESS_HELP)

# Add timestamp attributes. Again, this can cause big problems so surpress the help
parser.add_option("--add-timestamp", dest="addTimestamp", action="store_true",
                    help=optparse.SUPPRESS_HELP)

parser.add_option("--sql", dest="sqlQuery", type=str, default=None,
                     help="SQL query to execute on a PostgreSQL source")

parser.set_defaults(sourceEPSG=None, sourcePROJ4=None, verbose=False,
                    debugTags=False,
                    translationMethod=None, outputFile=None,
                    forceOverwrite=False, noUploadFalse=False)

# Parse and process arguments
(options, args) = parser.parse_args()

try:
    if options.sourceEPSG:
        options.sourceEPSG = int(options.sourceEPSG)
except:
    parser.error("EPSG code must be numeric (e.g. '4326', not 'epsg:4326')")

if len(args) < 1:
    parser.print_help()
    parser.error("you must specify a source filename")
elif len(args) > 1:
    parser.error("you have specified too many arguments, " +
                 "only supply the source filename")
                 
if options.addTimestamp:
    from datetime import datetime
    
# Input and output file
# if no output file given, use the basename of the source but with .osm
source = args[0]
sourceIsDatabase = bool(re.match('^PG:', source))

if options.outputFile is not None:
    options.outputFile = os.path.realpath(options.outputFile)
elif sourceIsDatabase:
    parser.error("ERROR: An output file must be explicitly specified when using a database source")
else:
    (base, ext) = os.path.splitext(os.path.basename(source))
    options.outputFile = os.path.join(os.getcwd(), base + ".osm")

if options.sqlQuery and not sourceIsDatabase:
    parser.error("ERROR: You must use a database source when specifying a query with --sql")

if not options.forceOverwrite and os.path.exists(options.outputFile):
    parser.error("ERROR: output file '%s' exists" % (options.outputFile))
l.info("Preparing to convert '%s' to '%s'." % (source, options.outputFile))

# Projection
if not options.sourcePROJ4 and not options.sourceEPSG:
    l.info("Will try to detect projection from source metadata, or fall back to EPSG:4326")
elif options.sourcePROJ4:
    l.info("Will use the PROJ.4 string: " + options.sourcePROJ4)
elif options.sourceEPSG:
    l.info("Will use EPSG:" + str(options.sourceEPSG))

# Stuff needed for locating translation methods
if options.translationMethod:
    # add dirs to path if necessary
    (root, ext) = os.path.splitext(options.translationMethod)
    if os.path.exists(options.translationMethod) and ext == '.py':
        # user supplied translation file directly
        sys.path.insert(0, os.path.dirname(root))
    else:
        # first check translations in the subdir translations of cwd
        sys.path.insert(0, os.path.join(os.getcwd(), "translations"))
        # then check subdir of script dir
        sys.path.insert(1, os.path.join(os.path.dirname(__file__), "translations"))
        # (the cwd will also be checked implicityly)

    # strip .py if present, as import wants just the module name
    if ext == '.py':
        options.translationMethod = os.path.basename(root)

    try:
        translations = __import__(options.translationMethod, fromlist = [''])
    except ImportError as e:
        parser.error("Could not load translation method '%s'. Translation "
               "script must be in your current directory, or in the "
               "translations/ subdirectory of your current or ogr2osm.py "
               "directory. The following directories have been considered: %s"
               % (options.translationMethod, str(sys.path)))
    except SyntaxError as e:
        parser.error("Syntax error in '%s'. Translation script is malformed:\n%s"
               % (options.translationMethod, e))

    l.info("Successfully loaded '%s' translation method ('%s')."
           % (options.translationMethod, os.path.realpath(translations.__file__)))
else:
    import types
    translations = types.ModuleType("translationmodule")
    l.info("Using default translations")

default_translations = [
    ('filterLayer', lambda layer: layer),
    ('filterFeature', lambda feature, fieldNames, reproject: feature),
    ('filterTags', lambda tags: tags),
    ('filterFeaturePost', lambda feature, fieldNames, reproject: feature),
    ('preOutputTransform', lambda geometries, features: None),
    ]

for (k, v) in default_translations:
    if hasattr(translations, k) and getattr(translations, k):
        l.debug("Using user " + k)
    else:
        l.debug("Using default " + k)
        setattr(translations, k, v)

Geometry.elementIdCounter = options.id
if options.idfile:
    with open(options.idfile, 'r') as ff:
        Geometry.elementIdCounter = int(ff.readline(20))
    l.info("Starting counter value '%d' read from file '%s'." \
        % (Geometry.elementIdCounter, options.idfile))

if options.positiveID:
    Geometry.elementIdCounterIncr = 1 # default is -1

def openData(source):
    if re.match('^PG:', source):
        return openDatabaseSource(source)
    else:
        return getFileData(source)

def openDatabaseSource(source):
    dataSource = ogr.Open(source, 0)  # 0 means read-only
    if dataSource is None:
        l.error('OGR failed to open connection to' + source)
        sys.exit(1)
    else:
        return dataSource

def getFileData(filename):
    ogr_accessmethods = [ "/vsicurl/", "/vsicurl_streaming/", "/vsisubfile/",
        "/vsistdin/" ]
    ogr_filemethods = [ "/vsisparse/", "/vsigzip/", "/vsitar/", "/vsizip/" ]
    ogr_unsupported = [ "/vsimem/", "/vsistdout/", ]
    has_unsup = [ m for m in ogr_unsupported if m[1:-1] in filename.split('/') ]
    if has_unsup:
        parser.error("Unsupported OGR access method(s) found: %s."
            % str(has_unsup)[1:-1])
    if not any([ m[1:-1] in filename.split('/') for m in ogr_accessmethods ]):
        # Not using any ogr_accessmethods
        real_filename = filename
        for fm in ogr_filemethods:
            if filename.find(fm) == 0:
                real_filename = filename[len(fm):]
                break
        if not os.path.exists(real_filename):
            parser.error("the file '%s' does not exist" % (real_filename))
        if len(filename) == len(real_filename):
            if filename.endswith('.gz'):
                filename = '/vsigzip/' + filename
            elif filename.endswith('.tar') or filename.endswith('.tgz') or \
              filename.endswith('.tar.gz'):
                filename = '/vsitar/' + filename
            elif filename.endswith('.zip'):
                filename = '/vsizip/' + filename

    fileDataSource = ogr.Open(filename, 0)  # 0 means read-only
    if fileDataSource is None:
        l.error('OGR failed to open ' + filename + ', format may be unsupported')
        sys.exit(1)
    if options.noMemoryCopy:
        return fileDataSource
    else:
        memoryDataSource = ogr.GetDriverByName('Memory').CopyDataSource(fileDataSource,'memoryCopy')
        return memoryDataSource

def parseData(dataSource):
    l.debug("Parsing data")
    global translations
    if options.sqlQuery:
        layer = dataSource.ExecuteSQL(options.sqlQuery)
        layer.ResetReading()
        parseLayer(translations.filterLayer(layer))
    else:
        for i in range(dataSource.GetLayerCount()):
            layer = dataSource.GetLayer(i)
            layer.ResetReading()
            parseLayer(translations.filterLayer(layer))

def getTransform(layer):
    global options
    # First check if the user supplied a projection, then check the layer,
    # then fall back to a default
    spatialRef = None
    if options.sourcePROJ4:
        spatialRef = osr.SpatialReference()
        spatialRef.ImportFromProj4(options.sourcePROJ4)
    elif options.sourceEPSG:
        spatialRef = osr.SpatialReference()
        spatialRef.ImportFromEPSG(options.sourceEPSG)
    else:
        spatialRef = layer.GetSpatialRef()
        if spatialRef != None:
            l.info("Detected projection metadata:\n" + str(spatialRef))
        else:
            l.info("No projection metadata, falling back to EPSG:4326")

    if spatialRef == None:
        # No source proj specified yet? Then default to do no reprojection.
        # Some python magic: skip reprojection altogether by using a dummy
        # lamdba funcion. Otherwise, the lambda will be a call to the OGR
        # reprojection stuff.
        reproject = lambda(geometry): None
    else:
        destSpatialRef = osr.SpatialReference()
        # Destionation projection will *always* be EPSG:4326, WGS84 lat-lon
        destSpatialRef.ImportFromEPSG(4326)
        coordTrans = osr.CoordinateTransformation(spatialRef, destSpatialRef)
        reproject = lambda(geometry): geometry.Transform(coordTrans)

    return reproject

def getLayerFields(layer):
    featureDefinition = layer.GetLayerDefn()
    fieldNames = []
    fieldCount = featureDefinition.GetFieldCount()
    for j in range(fieldCount):
        fieldNames.append(featureDefinition.GetFieldDefn(j).GetNameRef())
    return fieldNames

def getFeatureTags(ogrfeature, fieldNames):
    '''
    This function builds up a dictionary with the source data attributes and passes them to the filterTags function, returning the result.
    '''
    tags = {}
    for i in range(len(fieldNames)):
        # The field needs to be put into the appropriate encoding and leading or trailing spaces stripped
        tags[fieldNames[i].decode(options.encoding)] = ogrfeature.GetFieldAsString(i).decode(options.encoding).strip()
    return translations.filterTags(tags)

def parseLayer(layer):
    if layer is None:
        return
    fieldNames = getLayerFields(layer)
    reproject = getTransform(layer)
    
    for j in range(layer.GetFeatureCount()):
        ogrfeature = layer.GetNextFeature()
        parseFeature(translations.filterFeature(ogrfeature, fieldNames, reproject), fieldNames, reproject)

def parseFeature(ogrfeature, fieldNames, reproject):
    if ogrfeature is None:
        return

    ogrgeometry = ogrfeature.GetGeometryRef()
    if ogrgeometry is None:
        return
    reproject(ogrgeometry)
    geometries = parseGeometry([ogrgeometry])
    
    for geometry in geometries:
        if geometry is None:
            return

        feature = Feature()
        feature.tags = getFeatureTags(ogrfeature, fieldNames)
        feature.geometry = geometry
        geometry.addparent(feature)
        
        translations.filterFeaturePost(feature, ogrfeature, ogrgeometry)
    

def parseGeometry(ogrgeometries):
    returngeometries = []
    for ogrgeometry in ogrgeometries:
        geometryType = ogrgeometry.GetGeometryType()

        if (geometryType == ogr.wkbPoint or
            geometryType == ogr.wkbPoint25D):
            returngeometries.append(parsePoint(ogrgeometry))
        elif (geometryType == ogr.wkbLineString or
              geometryType == ogr.wkbLinearRing or
              geometryType == ogr.wkbLineString25D):
#             geometryType == ogr.wkbLinearRing25D does not exist
            returngeometries.append(parseLineString(ogrgeometry))
        elif (geometryType == ogr.wkbPolygon or
              geometryType == ogr.wkbPolygon25D):
            returngeometries.append(parsePolygon(ogrgeometry))
        elif (geometryType == ogr.wkbMultiPoint or
              geometryType == ogr.wkbMultiLineString or
              geometryType == ogr.wkbMultiPolygon or
              geometryType == ogr.wkbGeometryCollection or
              geometryType == ogr.wkbMultiPoint25D or
              geometryType == ogr.wkbMultiLineString25D or
              geometryType == ogr.wkbMultiPolygon25D or
              geometryType == ogr.wkbGeometryCollection25D):
            returngeometries.extend(parseCollection(ogrgeometry))
        else:
            l.warning("unhandled geometry, type: " + str(geometryType))
            returngeometries.append(None)
            
    return returngeometries
            
def parsePoint(ogrgeometry):
    x = int(round(ogrgeometry.GetX() * 10**options.significantDigits))
    y = int(round(ogrgeometry.GetY() * 10**options.significantDigits))
    geometry = Point(x, y)
    return geometry

linestring_points = {}
def parseLineString(ogrgeometry):
    geometry = Way()
    # LineString.GetPoint() returns a tuple, so we can't call parsePoint on it
    # and instead have to create the point ourself
    global linestring_points
    for i in range(ogrgeometry.GetPointCount()):
        (x, y, unused) = ogrgeometry.GetPoint(i)
        (rx, ry) = (int(round(x*10**options.roundingDigits)), int(round(y*10**options.roundingDigits)))
        (x, y) = (int(round(x*10**options.significantDigits)), int(round(y*10**options.significantDigits)))
        if (rx,ry) in linestring_points:
            mypoint = linestring_points[(rx,ry)]
        else:
            mypoint = Point(x, y)
            linestring_points[(rx,ry)] = mypoint
        geometry.points.append(mypoint)
        mypoint.addparent(geometry)
    return geometry

def parsePolygon(ogrgeometry):
    # Special case polygons with only one ring. This does not (or at least
    # should not) change behavior when simplify relations is turned on.
    if ogrgeometry.GetGeometryCount() == 0:
        l.warning("Polygon with no rings?")
    elif ogrgeometry.GetGeometryCount() == 1:
        return parseLineString(ogrgeometry.GetGeometryRef(0))
    else:
        geometry = Relation()
        try:
            exterior = parseLineString(ogrgeometry.GetGeometryRef(0))
            exterior.addparent(geometry)
        except:
            l.warning("Polygon with no exterior ring?")
            return None
        geometry.members.append((exterior, "outer"))
        for i in range(1, ogrgeometry.GetGeometryCount()):
            interior = parseLineString(ogrgeometry.GetGeometryRef(i))
            interior.addparent(geometry)
            geometry.members.append((interior, "inner"))
        return geometry

def parseCollection(ogrgeometry):
    # OGR MultiPolygon maps easily to osm multipolygon, so special case it
    # TODO: Does anything else need special casing?
    geometryType = ogrgeometry.GetGeometryType()
    if (geometryType == ogr.wkbMultiPolygon or
        geometryType == ogr.wkbMultiPolygon25D):
        if ogrgeometry.GetGeometryCount() > 1:
            geometry = Relation()
            for polygon in range(ogrgeometry.GetGeometryCount()):
                exterior = parseLineString(ogrgeometry.GetGeometryRef(polygon).GetGeometryRef(0))
                exterior.addparent(geometry)
                geometry.members.append((exterior, "outer"))
                for i in range(1, ogrgeometry.GetGeometryRef(polygon).GetGeometryCount()):
                    interior = parseLineString(ogrgeometry.GetGeometryRef(polygon).GetGeometryRef(i))
                    interior.addparent(geometry)
                    geometry.members.append((interior, "inner"))
            return [geometry]
        else:
           return [parsePolygon(ogrgeometry.GetGeometryRef(0))]
    elif (geometryType == ogr.wkbMultiLineString or
          geometryType == ogr.wkbMultiLineString25D):
        geometries = []
        for linestring in range(ogrgeometry.GetGeometryCount()):
            geometries.append(parseLineString(ogrgeometry.GetGeometryRef(linestring)))
        return geometries
    else:
        geometry = Relation()
        for i in range(ogrgeometry.GetGeometryCount()):
            member = parseGeometry(ogrgeometry.GetGeometryRef(i))
            member.addparent(geometry)
            geometry.members.append((member, "member"))
        return [geometry]

def mergePoints():
    l.debug("Merging points")
    points = [geom for geom in Geometry.geometries if type(geom) == Point]

    # Make list of Points at each location
    l.debug("Making list")
    pointcoords = {}
    for i in points:
        rx = int(round(i.x * 10**(options.significantDigits-options.roundingDigits)))
        ry = int(round(i.y * 10**(options.significantDigits-options.roundingDigits)))
        if (rx, ry) in pointcoords:
            pointcoords[(rx, ry)].append(i)
        else:
            pointcoords[(rx, ry)] = [i]

    # Use list to get rid of extras
    l.debug("Checking list")
    for (location, pointsatloc) in pointcoords.items():
        if len(pointsatloc) > 1:
            for point in pointsatloc[1:]:
                for parent in set(point.parents):
                    parent.replacejwithi(pointsatloc[0], point)

def mergeWayPoints():
    l.debug("Merging duplicate points in ways")
    ways = [geom for geom in Geometry.geometries if type(geom) == Way]

    # Remove duplicate points from ways,
    # a duplicate has the same id as its predecessor
    for way in ways:
        previous = options.id
        merged_points = []

        for node in way.points:
            if previous == options.id or previous != node.id:
                merged_points.append(node)
                previous = node.id
           
        if len(merged_points) > 0:
            way.points = merged_points

def output():
    l.debug("Outputting XML")
    # First, set up a few data structures for optimization purposes
    nodes = [geom for geom in Geometry.geometries if type(geom) == Point]
    ways = [geom for geom in Geometry.geometries if type(geom) == Way]
    relations = [geom for geom in Geometry.geometries if type(geom) == Relation]
    featuresmap = {feature.geometry : feature for feature in Feature.features}

    # Open up the output file with the system default buffering
    with open(options.outputFile, 'w', -1) as f:
        
        if options.noUploadFalse:
            f.write('<?xml version="1.0"?>\n<osm version="0.6" generator="uvmogr2osm">\n')
        else:
            f.write('<?xml version="1.0"?>\n<osm version="0.6" upload="false" generator="uvmogr2osm">\n')

        # Build up a dict for optional settings
        attributes = {}
        if options.addVersion:
            attributes.update({'version':'1'})
            
        if options.addTimestamp:
            attributes.update({'timestamp':datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')})
            
        for node in nodes:
            xmlattrs = {'visible':'true','id':str(node.id), 'lat':str(node.y*10**-options.significantDigits), 'lon':str(node.x*10**-options.significantDigits)}
            xmlattrs.update(attributes)
            
            xmlobject = etree.Element('node', xmlattrs)
            
            if node in featuresmap:
                for (key, value) in featuresmap[node].tags.items():
                    tag = etree.Element('tag', {'k':key, 'v':value})
                    xmlobject.append(tag)
                    
            f.write(etree.tostring(xmlobject))
            f.write('\n')
            
        for way in ways:
            xmlattrs = {'visible':'true', 'id':str(way.id)}
            xmlattrs.update(attributes)
            
            xmlobject = etree.Element('way', xmlattrs)
            
            for node in way.points:
                nd = etree.Element('nd',{'ref':str(node.id)})
                xmlobject.append(nd)
            if way in featuresmap:
                for (key, value) in featuresmap[way].tags.items():
                    tag = etree.Element('tag', {'k':key, 'v':value})
                    xmlobject.append(tag)
                    
            f.write(etree.tostring(xmlobject))
            f.write('\n')
            
        for relation in relations:
            xmlattrs = {'visible':'true', 'id':str(relation.id)}
            xmlattrs.update(attributes)
            
            xmlobject = etree.Element('relation', xmlattrs)
            
            for (member, role) in relation.members:
                member = etree.Element('member', {'type':'way', 'ref':str(member.id), 'role':role})
                xmlobject.append(member)
            
            tag = etree.Element('tag', {'k':'type', 'v':'multipolygon'})
            xmlobject.append(tag)
            if relation in featuresmap:
                for (key, value) in featuresmap[relation].tags.items():
                    tag = etree.Element('tag', {'k':key, 'v':value})
                    xmlobject.append(tag)
                    
            f.write(etree.tostring(xmlobject))
            f.write('\n')
            
        f.write('</osm>')


# Main flow
data = openData(source)
parseData(data)
mergePoints()
mergeWayPoints()
translations.preOutputTransform(Geometry.geometries, Feature.features)
output()
if options.saveid:
    with open(options.saveid, 'w') as ff:
        ff.write(str(Geometry.elementIdCounter))
    l.info("Wrote elementIdCounter '%d' to file '%s'"
        % (Geometry.elementIdCounter, options.saveid))
