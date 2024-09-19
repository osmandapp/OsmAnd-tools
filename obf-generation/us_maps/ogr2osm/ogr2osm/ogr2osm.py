# -*- coding: utf-8 -*-

''' ogr2osm

This program takes any vector data understandable by OGR and outputs an OSM or
PBF file with that data.

By default tags will be naively copied from the input data. Hooks are provided
so that, with a little python programming, you can translate the tags however
you like. More hooks are provided so you can filter or even modify the features
themselves.

To use the hooks, create a file called myfile.py and run ogr2osm.py -t myfile.
This file should define a class derived from TranslationBase where the hooks
you want to use are overridden.

The program will use projection metadata from the source, if it has any. If
there is no projection information, or if you want to override it, you can use
-e or -p to specify an EPSG code or Proj.4 string, respectively. If there is no
projection metadata and you do not specify one, EPSG:4326 will be used (WGS84
latitude-longitude)

For additional usage information, run ogr2osm --help

Copyright (c) 2012-2021 Roel Derickx, Paul Norman <penorman@mac.com>,
Sebastiaan Couwenberg <sebastic@xs4all.nl>, The University of Vermont
<andrew.guertin@uvm.edu>, github contributors

Released under the MIT license, as given in the file LICENSE, which must
accompany any distribution of this code.

ogr2osm is based very heavily on code released under the following terms:
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
import argparse
import logging
import inspect

from .version import __program__, __version__
from .translation_base_class import TranslationBase
from .osm_geometries import OsmId
from .ogr_datasource import OgrDatasource
from .osm_data import OsmData
from .osm_datawriter import OsmDataWriter
from .pbf_datawriter import is_protobuf_installed, PbfDataWriter

def parse_commandline(logger):
    parser = argparse.ArgumentParser(prog=__program__)

    #parser.add_argument("-v", "--verbose", dest="verbose", action="store_true")
    #parser.add_argument("-d", "--debug-tags", dest="debugTags", action="store_true",
    #                    help="Output the tags for every feature parsed.")
    parser.add_argument('--version', action='version', version="%s %s" % (__program__, __version__))
    parser.add_argument("-t", "--translation", dest="translationmodule",
                        metavar="TRANSLATION",
                        help="Select the attribute-tags translation method. See " +
                             "the translations/ directory for valid values.")
    # datasource options
    parser.add_argument("--encoding", dest="encoding",
                        help="Encoding of the source file. If specified, overrides " +
                             "the default of %(default)s", default="utf-8")
    parser.add_argument("--sql", dest="sqlQuery", type=str, default=None,
                        help="SQL query to execute on a PostgreSQL source")
    parser.add_argument("--no-memory-copy", dest="noMemoryCopy", action="store_true",
                        help="Do not make an in-memory working copy")
    # input projection parameters
    parser.add_argument("-e", "--epsg", dest="sourceEPSG", type=int, metavar="EPSG_CODE",
                        help="EPSG code of source file. Do not include the " +
                             "'EPSG:' prefix. If specified, overrides projection " +
                             "from source metadata if it exists.")
    parser.add_argument("-p", "--proj4", dest="sourcePROJ4", type=str, metavar="PROJ4_STRING",
                        help="PROJ.4 string. If specified, overrides projection " +
                             "from source metadata if it exists.")
    parser.add_argument("--gis-order", dest="gisorder", action="store_true",
                        help="Consider the source coordinates to be in traditional GIS order")
    # precision options
    parser.add_argument("--rounding-digits", dest="roundingDigits", type=int,
                        help="Number of decimal places for rounding when snapping " +
                             "nodes together (default: %(default)s)",
                        default=7)
    parser.add_argument("--significant-digits", dest="significantDigits", type=int,
                        help="Number of decimal places for coordinates to output " +
                             "(default: %(default)s)",
                        default=9)
    # transformation options
    parser.add_argument("--split-ways", dest="maxNodesPerWay", type=int, default=1800,
                        help="Split ways with more than the specified number of nodes. " +
                             "Defaults to %(default)s. Any value below 2 - do not split.")
    # ID generation options
    parser.add_argument("--id", dest="id", type=int, default=0,
                        help="ID to start counting from for the output file. " +
                             "Defaults to %(default)s.")
    parser.add_argument("--idfile", dest="idfile", type=str, default=None,
                        help="Read ID to start counting from from a file.")
    parser.add_argument("--saveid", dest="saveid", type=str, default=None,
                        help="Save last ID after execution to a file.")
    parser.add_argument("--positive-id", dest="positiveId", action="store_true",
                        help=argparse.SUPPRESS) # can cause problems when used inappropriately
    # output file options
    parser.add_argument("-o", "--output", dest="outputFile", metavar="OUTPUT",
                        help="Set destination .osm file name and location.")
    parser.add_argument("-f", "--force", dest="forceOverwrite", action="store_true",
                        help="Force overwrite of output file.")
    parser.add_argument("--pbf", dest="pbf", action="store_true",
                        help="Write the output as a PBF file in stead of an OSM file")
    parser.add_argument("--no-upload-false", dest="noUploadFalse", action="store_true",
                        help="Omit upload=false from the completed file to suppress " +
                             "JOSM warnings when uploading.")
    parser.add_argument("--never-download", dest="neverDownload", action="store_true",
                        help="Prevent JOSM from downloading more data to this file.")
    parser.add_argument("--never-upload", dest="neverUpload", action="store_true",
                        help="Completely disables all upload commands for this file " +
                             "in JOSM, rather than merely showing a warning before " +
                             "uploading.")
    parser.add_argument("--locked", dest="locked", action="store_true",
                        help="Prevent any changes to this file in JOSM, " +
                             "such as editing or downloading, and also prevents uploads. " +
                             "Implies upload=\"never\" and download=\"never\".")
    parser.add_argument("--add-bounds", dest="addBounds", action="store_true",
                        help="Add boundaries to output file")
    parser.add_argument("--suppress-empty-tags", dest="suppressEmptyTags", action="store_true",
                        help="Suppress empty tags")
    parser.add_argument("--max-tag-length", dest="maxTagLength", type=int, default=255,
                        help="Set max character length of tag values. Exceeding values will be " +
                             f"truncated and end with '{OsmDataWriter.TAG_OVERFLOW}'. Defaults " +
                             "to %(default)s. Values smaller than " +
                             f"{len(OsmDataWriter.TAG_OVERFLOW)} disable the limit.")
    parser.add_argument("--add-z-value-tag", dest="zValueTagName", type=str, metavar="TAGNAME",
                        help="The tagname in which the z-value will be saved.")
    parser.add_argument("--add-version", dest="addVersion", action="store_true",
                        help=argparse.SUPPRESS) # can cause problems when used inappropriately
    parser.add_argument("--add-timestamp", dest="addTimestamp", action="store_true",
                        help=argparse.SUPPRESS) # can cause problems when used inappropriately
    # required source file
    parser.add_argument("source", metavar="DATASOURCE",
                        help="DATASOURCE can be a file path or a org PostgreSQL connection " +
                             "string such as: \"PG:dbname=pdx_bldgs user=emma host=localhost\" " +
                             "(including the quotes)")
    params = parser.parse_args()

    if not is_protobuf_installed:
        params.pbf = False

    if params.outputFile:
        params.outputFile = os.path.realpath(params.outputFile)

    # check consistency of parameters
    if params.source.startswith('PG:'):
        if not params.outputFile:
            parser.error("ERROR: An output file must be explicitly specified " +
                         "when using a database source")
        if not params.sqlQuery:
            parser.error("ERROR: You must specify a query with --sql when using a database source")
    else:
        if not params.outputFile:
            (base, ext) = os.path.splitext(os.path.basename(params.source))
            output_ext = ".osm"
            if params.pbf:
                output_ext = ".osm.pbf"
            params.outputFile = os.path.join(os.getcwd(), base + output_ext)
        else:
            (base, ext) = os.path.splitext(params.outputFile)
            if params.pbf and ext.lower() == '.osm':
                logger.warning("WARNING: You specified PBF output with --pbf "
                               "but the outputfile has extension .osm, "
                               "ignoring --pbf parameter")
                params.pbf = False
            elif is_protobuf_installed and not params.pbf and ext.lower() == '.pbf':
                logger.warning("WARNING: You didn't specify PBF output with --pbf "
                               "but the outputfile has extension .pbf, "
                               "automatically setting --pbf parameter")
                params.pbf = True
            elif not is_protobuf_installed and not params.pbf and ext.lower() == '.pbf':
                logger.warning("WARNING: PBF output is not supported on this "
                               "system but the outputfile has extension .pbf, "
                               "automatically removing .pbf extension")
                (base_osm, ext_osm) = os.path.splitext(base)
                if ext_osm == '.osm':
                    params.outputFile = base
                else:
                    params.outputFile = base + '.osm'
        if params.sqlQuery:
            logger.warning("WARNING: You specified a query with --sql "
                           "but you are not using a database source")

    if not params.forceOverwrite and os.path.exists(params.outputFile):
        parser.error("ERROR: output file '%s' exists" % params.outputFile)

    if params.maxTagLength < len(OsmDataWriter.TAG_OVERFLOW):
        params.maxTagValueLength = sys.maxsize

    return params


def load_translation_object(logger, translation_module):
    translation_object = None

    if translation_module:
        # add dirs to path if necessary
        (root, ext) = os.path.splitext(translation_module)
        if os.path.exists(translation_module) and ext == '.py':
            # user supplied translation file directly
            sys.path.insert(0, os.path.dirname(root))
        else:
            # first check translations in the subdir translations of cwd
            sys.path.insert(0, os.path.join(os.getcwd(), "translations"))
            # (the cwd will also be checked implicitly)

        # strip .py if present, as import wants just the module name
        if ext == '.py':
            translation_module = os.path.basename(root)

        imported_module = None
        try:
            imported_module = __import__(translation_module)
        except ImportError as e:
            logger.error("Could not load translation method '%s'. Translation "
                         "script must be in your current directory, or in the "
                         "translations/ subdirectory of your current directory. "
                         "The following directories have been considered: %s",
                         translation_module, str(sys.path))
        except SyntaxError as e:
            logger.error("Syntax error in '%s'. Translation script is malformed:\n%s",
                         translation_module, e)

        for class_name in [ d for d in dir(imported_module) \
                                    if d != 'TranslationBase' and not d.startswith('__') ]:
            translation_class = getattr(imported_module, class_name)

            if inspect.isclass(translation_class) and \
               issubclass(translation_class, TranslationBase):
                logger.info('Found valid translation class %s', class_name)
                setattr(sys.modules[__name__], class_name, translation_class)
                translation_object = translation_class()
                break

    if not translation_object:
        if translation_module:
            logger.warning('WARNING: translation file does not contain a valid translation class')
            logger.warning('Falling back to DEFAULT translations')
        else:
            logger.info('Using default translations')
        translation_object = TranslationBase()

    return translation_object


def main():
    logger = logging.getLogger(__program__)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(logging.StreamHandler())

    params = parse_commandline(logger)

    translation_object = load_translation_object(logger, params.translationmodule)

    logger.info("Preparing to convert '%s' to '%s'.", params.source, params.outputFile)

    osmdata = OsmData(translation_object, params.roundingDigits, params.significantDigits, \
                      params.maxNodesPerWay, params.addBounds, params.id, params.positiveId, \
                      params.zValueTagName)

    osmdata.load_start_id_from_file(params.idfile)

    # create datasource and process data
    datasource = OgrDatasource(translation_object, \
                               params.sourcePROJ4, params.sourceEPSG, params.gisorder, \
                               params.encoding)
    datasource.open_datasource(params.source, not params.noMemoryCopy)
    datasource.set_query(params.sqlQuery)
    osmdata.process(datasource)
    #create datawriter and write OSM data
    datawriter = None
    if params.pbf:
        datawriter = PbfDataWriter(params.outputFile, params.addVersion, params.addTimestamp, \
                                   params.suppressEmptyTags, params.maxTagLength)
    else:
        datawriter = OsmDataWriter(params.outputFile, params.neverUpload, params.noUploadFalse, \
                                   params.neverDownload, params.locked, params.addVersion, \
                                   params.addTimestamp, params.significantDigits, \
                                   params.suppressEmptyTags, params.maxTagLength)
    osmdata.output(datawriter)

    osmdata.save_current_id_to_file(params.saveid)
