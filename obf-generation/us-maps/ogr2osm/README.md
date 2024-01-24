# ogr2osm

![license](https://img.shields.io/github/license/roelderickx/ogr2osm) [![test](https://github.com/roelderickx/ogr2osm/actions/workflows/test.yml/badge.svg)](https://github.com/roelderickx/ogr2osm/actions/workflows/test.yml) [![docker](https://github.com/roelderickx/ogr2osm/actions/workflows/deploy.yml/badge.svg)](https://github.com/roelderickx/ogr2osm/actions/workflows/deploy.yml)

A tool for converting ogr-readable files like shapefiles into .pbf or .osm data

## Installation

Ogr2osm requires python 3, gdal with python bindings, lxml and optionally protobuf if you want to generate pbf files. Depending on the file formats you want to read you may have to compile gdal yourself but there should be no issues with shapefiles. You can also use docker to run ogr2osm.

### Via Linux package manager

#### Arch Linux

Install via

```console
paru -Syu ogr2osm-git
```

### Using pip

```console
pip install --upgrade ogr2osm
```

### From source

```console
git clone https://github.com/roelderickx/ogr2osm.git
cd ogr2osm
python setup.py install
```

### Running from source without installation

If you do not have the required permissions to install ogr2osm, you can run the package as a module directly from the cloned source.
```console
git clone https://github.com/roelderickx/ogr2osm.git
cd ogr2osm
python -m ogr2osm
```

### Running with Docker
If you do not want to install ogr2osm on your system, you can run it with Docker. This is especially useful if you are on Windows or Mac and do not want to install Python and GDAL yourself.

The usage of ogr2osm with Docker is the same as the usage as a standalone application. You can run ogr2osm with the following command:
```bash
# If you have test.json in the current directory
# you can run the following command to convert it to an OSM file
# and save it in the current directory
docker run -ti --rm -v $(pwd):/app roelderickx/ogr2osm /app/test.json -o /app/test.osm
```

## Upgrading

If you are upgrading from pnorman's version and you use a translation file for your data, be sure to read about [the modifications you will need to do](https://github.com/roelderickx/ogr2osm-translations).

## About

This program is based on [pnorman's version of ogr2osm](https://github.com/pnorman/ogr2osm), but is rewritten to make it useable as a general purpose library.

Ogr2osm will read any data source that ogr can read and handle reprojection for you. It takes a python file to translate external data source tags into OSM tags, allowing you to use complicated logic. If no translation is specified it will use an identity translation, carrying all tags from the source to the .pbf or .osm output.

## Import Cautions

Anyone planning an import into OpenStreetMap should read and review the import guidelines located [on the wiki](http://wiki.openstreetmap.org/wiki/Import/Guidelines). When writing your translation file you should look at other examples and carefully consider each external data source tag to see if it should be converted to an OSM tag.

## Usage

Ogr2osm can be used as a standalone application, but you can also use its classes in your own python project.

### Standalone

```console
usage: ogr2osm [-h] [--version] [-t TRANSLATION] [--encoding ENCODING]
               [--sql SQLQUERY] [--no-memory-copy] [-e EPSG_CODE]
               [-p PROJ4_STRING] [--gis-order]
               [--rounding-digits ROUNDINGDIGITS]
               [--significant-digits SIGNIFICANTDIGITS]
               [--split-ways MAXNODESPERWAY] [--id ID] [--idfile IDFILE]
               [--saveid SAVEID] [-o OUTPUT] [-f] [--pbf] [--no-upload-false]
               [--never-download] [--never-upload] [--locked] [--add-bounds]
               [--suppress-empty-tags] [--max-tag-length MAXTAGLENGTH]
               [--add-z-value-tag TAGNAME]
               DATASOURCE

positional arguments:
  DATASOURCE            DATASOURCE can be a file path or a org PostgreSQL
                        connection string such as: "PG:dbname=pdx_bldgs
                        user=emma host=localhost" (including the quotes)

options:
  -h, --help            show this help message and exit
  --version             show program's version number and exit
  -t TRANSLATION, --translation TRANSLATION
                        Select the attribute-tags translation method. See the
                        translations/ directory for valid values.
  --encoding ENCODING   Encoding of the source file. If specified, overrides
                        the default of utf-8
  --sql SQLQUERY        SQL query to execute on a PostgreSQL source
  --no-memory-copy      Do not make an in-memory working copy
  -e EPSG_CODE, --epsg EPSG_CODE
                        EPSG code of source file. Do not include the 'EPSG:'
                        prefix. If specified, overrides projection from source
                        metadata if it exists.
  -p PROJ4_STRING, --proj4 PROJ4_STRING
                        PROJ.4 string. If specified, overrides projection from
                        source metadata if it exists.
  --gis-order           Consider the source coordinates to be in traditional
                        GIS order
  --rounding-digits ROUNDINGDIGITS
                        Number of decimal places for rounding when snapping
                        nodes together (default: 7)
  --significant-digits SIGNIFICANTDIGITS
                        Number of decimal places for coordinates to output
                        (default: 9)
  --split-ways MAXNODESPERWAY
                        Split ways with more than the specified number of
                        nodes. Defaults to 1800. Any value below 2 - do not
                        split.
  --id ID               ID to start counting from for the output file.
                        Defaults to 0.
  --idfile IDFILE       Read ID to start counting from from a file.
  --saveid SAVEID       Save last ID after execution to a file.
  -o OUTPUT, --output OUTPUT
                        Set destination .osm file name and location.
  -f, --force           Force overwrite of output file.
  --pbf                 Write the output as a PBF file in stead of an OSM file
  --no-upload-false     Omit upload=false from the completed file to suppress
                        JOSM warnings when uploading.
  --never-download      Prevent JOSM from downloading more data to this file.
  --never-upload        Completely disables all upload commands for this file
                        in JOSM, rather than merely showing a warning before
                        uploading.
  --locked              Prevent any changes to this file in JOSM, such as
                        editing or downloading, and also prevents uploads.
                        Implies upload="never" and download="never".
  --add-bounds          Add boundaries to output file
  --suppress-empty-tags
                        Suppress empty tags
  --max-tag-length MAXTAGLENGTH
                        Set max character length of tag values. Exceeding
                        values will be truncated and end with '...'. Defaults
                        to 255. Values smaller than 3 disable the limit.
  --add-z-value-tag TAGNAME
                        The tagname in which the z-value will be saved.
```

### As a library

Example code:
```python
import logging
import ogr2osm

# 1. Set the logging level of the logger object named 'ogr2osm' to the desired output level

ogr2osmlogger = logging.getLogger('ogr2osm')
ogr2osmlogger.setLevel(logging.ERROR)
ogr2osmlogger.addHandler(logging.StreamHandler())

# 2. Required parameters for this example:

# - datasource_parameter is a variable holding the input filename, or a
#   database connection such as "PG:dbname=pdx_bldgs user=emma host=localhost"
datasource_parameter = ...

# - in case your datasource is a database, you will need a query
query = ...

# - the output file to write
output_file = ...

# 3. Create the translation object. If no translation is required you
#    can use the base class from ogr2osm, otherwise you need to instantiate
#    a subclass of ogr2osm.TranslationBase
translation_object = ogr2osm.TranslationBase()

# 4. Create the ogr datasource. You can specify a source projection but
#    EPSG:4326 will be assumed if none is given and if the projection of the
#    datasource is unknown.
datasource = ogr2osm.OgrDatasource(translation_object)
# Optional constructor parameters:
# - source_proj4: --proj4 parameter
# - source_epsg: --epsg parameter
# - gisorder: --gis-order parameter
# - source_encoding: --encoding parameter
datasource.open_datasource(datasource_parameter)
# Optional open_datasource parameters:
# - prefer_mem_copy: --no-memory-copy parameter

# 5. If the datasource is a database then you must set the query to use.
#    Setting the query for any other datasource is useless but not an error.
datasource.set_query(query)

# 6. Instantiate the ogr to osm converter class ogr2osm.OsmData and start the
#    conversion process
osmdata = ogr2osm.OsmData(translation_object)
# Optional constructor parameters:
# - rounding_digits: --rounding-digits parameter
# - significant_digits: --significant-digits parameter
# - max_points_in_way: --split-ways parameter
# - add_bounds: --add-bounds parameter
# - start_id: --id parameter
# - z_value_tagname: --add-z-value-tag
osmdata.process(datasource)

# 7. Instantiate either ogr2osm.OsmDataWriter or ogr2osm.PbfDataWriter and
#    invoke output() to write the output file. If required you can write a
#    custom datawriter class by subclassing ogr2osm.DataWriterBase.
datawriter = ogr2osm.OsmDataWriter(output_file)
# Optional constructor parameters:
# - never_upload: --never-upload parameter
# - no_upload_false: --no-upload-false parameter
# - never_download: --never-download parameter
# - locked: --locked parameter
# - significant_digits: --significant-digits parameter
# - suppress_empty_tags: --suppress-empty-tags parameter
# - max_tag_length: --max-tag-length parameter
osmdata.output(datawriter)
```

Refer to [contour-osm](https://github.com/roelderickx/contour-osm) for a complete example with a custom translation class and coordinate reprojection.

## Translations

ogr2osm supports custom translations for your data. To do this you need to subclass ogr2osm.TranslationBase and override the methods in which you want to run custom code.

```python
class TranslationBase:
    def filter_layer(self, layer):
        '''
        Override this method if you want to modify the given layer,
        or return None if you want to suppress the layer
        '''
        return layer


    def filter_feature(self, ogrfeature, layer_fields, reproject):
        '''
        Override this method if you want to modify the given feature,
        or return None if you want to suppress the feature
        Note 1: layer_fields contains a tuple (index, field_name, field_type)
        Note 2: reproject is a function to convert the feature to 4326 projection
        with coordinates in traditional gis order. However, do not return the
        reprojected feature since it will be done again in ogr2osm.
        '''
        return ogrfeature


    def filter_tags(self, tags):
        '''
        Override this method if you want to modify or add tags to the xml output
        '''
        return tags


    def merge_tags(self, geometry_type, tags_existing_geometry, tags_new_geometry):
        '''
        This method is called when two geometries are found to be duplicates.
        Override this method if you want to customize how the tags of both
        geometries should be merged. The parameter geometry_type is a string
        containing either 'node', 'way', 'reverse_way' or 'relation', depending
        on which type of geometry the tags belong to. Type 'reverse_way' is a
        special case of 'way', it indicates both ways are duplicates when one
        of them is reversed. The parameter tags_existing_geometry is a
        dictionary containing a list of values for each key, the list will be
        concatenated to a comma-separated string when writing the output file.
        The parameter tags_new_geometry is a dictionary containing a string
        value for each key.
        Return None if the tags cannot be merged. As a result both geometries
        will be included in the output file, each with their respective tags.
        Warning: not merging geometries will lead to invalid osm files and
        has an impact on the detection of duplicates among their parents.
        '''
        tags = {}
        # ...
        # Default behaviour: add all tags from both geometries
        # If both contain the same key with a different value, then relate
        # the key to a comma separated list of both values
        # ...
        return tags


    def process_feature_post(self, osmgeometry, ogrfeature, ogrgeometry):
        '''
        This method is called after the creation of an OsmGeometry object. The
        ogr feature and ogr geometry used to create the object are passed as
        well. Note that any return values will be discarded by ogr2osm.
        '''
        pass


    def process_output(self, osmnodes, osmways, osmrelations):
        '''
        Override this method if you want to modify the list of nodes, ways or
        relations, or take any additional actions right before writing the
        objects to the OSM file. Note that any return values will be discarded
        by ogr2osm.
        '''
        pass
```

