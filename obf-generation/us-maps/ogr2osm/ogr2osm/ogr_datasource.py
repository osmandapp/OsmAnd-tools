# -*- coding: utf-8 -*-

'''
Copyright (c) 2012-2021 Roel Derickx, Paul Norman <penorman@mac.com>,
Sebastiaan Couwenberg <sebastic@xs4all.nl>, The University of Vermont
<andrew.guertin@uvm.edu>, github contributors

Released under the MIT license, as given in the file LICENSE, which must
accompany any distribution of this code.
'''

import os
import logging
from osgeo import gdalconst
from osgeo import ogr
from osgeo import osr

from .version import __program__

ogr.UseExceptions()

class OgrDatasource:
    def __init__(self, translation, source_proj4=None, source_epsg=None, gisorder=False, \
                       source_encoding='utf-8'):
        self.logger = logging.getLogger(__program__)
        self.datasource = None
        self.is_database_source = False
        self.query = None
        self.translation = translation
        self.source_proj4 = source_proj4
        self.source_epsg = source_epsg
        self.gisorder = gisorder
        self.source_encoding = source_encoding


    def open_datasource(self, ogrpath, prefer_mem_copy=True):
        full_ogrpath = None

        # database source ?
        if ogrpath.startswith('PG:'):
            self.is_database_source = True
            full_ogrpath = ogrpath
        else:
            # unsupported access method ?
            ogr_unsupported = [ "vsimem", "vsistdout" ]
            has_unsup = [ m for m in ogr_unsupported if m in ogrpath.split('/') ]
            if has_unsup:
                self.logger.error("Unsupported OGR access method(s) found: %s.", \
                                  ', '.join(has_unsup))

            # using ogr access methods ?
            ogr_accessmethods = [ "vsicurl", "vsicurl_streaming", "vsisubfile", "vsistdin" ]
            if any([ m in ogrpath.split('/') for m in ogr_accessmethods ]):
                full_ogrpath = ogrpath
            else:
                filename = ogrpath

                # filter out file access method if present
                ogr_filemethods = [ "/vsisparse/", "/vsigzip/", "/vsitar/", "/vsizip/" ]
                for fm in ogr_filemethods:
                    if ogrpath.find(fm) == 0:
                        filename = ogrpath[len(fm):]
                        break

                if not os.path.exists(filename):
                    self.logger.error("The file '%s' does not exist.", filename)
                elif ogrpath == filename:
                    if filename.endswith('.tar') or \
                       filename.endswith('.tgz') or \
                       filename.endswith('.tar.gz'):
                        full_ogrpath = '/vsitar/' + filename
                    elif filename.endswith('.gz'):
                        full_ogrpath = '/vsigzip/' + filename
                    elif filename.endswith('.zip'):
                        full_ogrpath = '/vsizip/' + filename
                    else:
                        full_ogrpath = filename
                else:
                    full_ogrpath = filename

        if full_ogrpath:
            file_datasource = None
            if not self.is_database_source and prefer_mem_copy:
                file_datasource = ogr.Open(full_ogrpath, gdalconst.GA_ReadOnly)
            else:
                self.datasource = ogr.Open(full_ogrpath, gdalconst.GA_ReadOnly)

            if self.is_database_source and not self.datasource:
                self.logger.error("OGR failed to open connection to %s.", full_ogrpath)
            elif not self.is_database_source and not self.datasource and not file_datasource:
                self.logger.error("OGR failed to open '%s', format may be unsupported.", \
                                  full_ogrpath)
            elif not self.is_database_source and file_datasource and prefer_mem_copy:
                mem_driver = ogr.GetDriverByName('Memory')
                self.datasource = mem_driver.CopyDataSource(file_datasource, 'memoryCopy')


    def set_query(self, query):
        self.query = query


    def __get_source_reprojection_func(self, layer):
        layer_spatial_ref = layer.GetSpatialRef() if layer else None

        spatial_ref = None
        if self.source_proj4:
            spatial_ref = osr.SpatialReference()
            if self.gisorder:
                spatial_ref.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
            spatial_ref.ImportFromProj4(self.source_proj4)
        elif self.source_epsg:
            spatial_ref = osr.SpatialReference()
            if self.gisorder:
                spatial_ref.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
            spatial_ref.ImportFromEPSG(self.source_epsg)
        elif not layer:
            self.logger.info("Skipping filtered out layer")
        elif layer_spatial_ref:
            spatial_ref = layer_spatial_ref
            self.logger.info("Detected projection metadata:\n%s", str(layer_spatial_ref))
        else:
            self.logger.info("Layer has no projection metadata, falling back to EPSG:4326")
            if not self.gisorder:
                spatial_ref = osr.SpatialReference()
                spatial_ref.ImportFromEPSG(4326)

        # No source proj specified yet? Then default to do no reprojection.
        # Some python magic: skip reprojection altogether by using a dummy
        # lamdba funcion. Otherwise, the lambda will be a call to the OGR
        # reprojection stuff.
        reproject = lambda geometry: None

        if spatial_ref:
            # Destionation projection will *always* be EPSG:4326, WGS84 lat-lon
            dest_spatial_ref = osr.SpatialReference()
            dest_spatial_ref.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
            dest_spatial_ref.ImportFromEPSG(4326)
            coord_trans = osr.CoordinateTransformation(spatial_ref, dest_spatial_ref)
            reproject = lambda geometry: geometry.Transform(coord_trans)

        return reproject


    def get_layer_count(self):
        if self.is_database_source:
            return 1
        else:
            return self.datasource.GetLayerCount()


    def get_layer(self, index):
        layer = None
        if self.is_database_source:
            if self.query:
                # TODO: avoid executing the query more than once
                layer = self.datasource.ExecuteSQL(self.query)
                layer.ResetReading()
            else:
                self.logger.error("Query must be set first when the datasource is a database.")
        else:
            layer = self.datasource.GetLayer(index)
            layer.ResetReading()

        filteredlayer = self.translation.filter_layer(layer)
        return (filteredlayer, self.__get_source_reprojection_func(filteredlayer))
