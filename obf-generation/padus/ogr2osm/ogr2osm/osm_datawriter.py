# -*- coding: utf-8 -*-

'''
Copyright (c) 2012-2021 Roel Derickx, Paul Norman <penorman@mac.com>,
Sebastiaan Couwenberg <sebastic@xs4all.nl>, The University of Vermont
<andrew.guertin@uvm.edu>, github contributors

Released under the MIT license, as given in the file LICENSE, which must
accompany any distribution of this code.
'''

import logging
import time

from .version import __program__
from .datawriter_base_class import DataWriterBase

class OsmDataWriter(DataWriterBase):
    def __init__(self, filename, never_upload=False, no_upload_false=False, never_download=False, \
                 locked=False, add_version=False, add_timestamp=False, significant_digits=9, \
                 suppress_empty_tags=False, max_tag_length=255):
        self.logger = logging.getLogger(__program__)

        self.filename = filename
        self.never_upload = never_upload
        self.no_upload_false = no_upload_false
        self.never_download = never_download
        self.locked = locked
        self.significant_digits = significant_digits
        self.suppress_empty_tags = suppress_empty_tags
        self.max_tag_length = max_tag_length
        #self.gzip_compression_level = gzip_compression_level
        self.f = None

        # Build up a dict for optional settings
        self.attributes = {}
        if add_version:
            self.attributes.update({'version':'1'})
        if add_timestamp:
            self.attributes.update({'timestamp':time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())})


    def open(self):
        #if 0 < self.gzip_compression_level < 10:
        #    import gzip
        #    self.f = gzip.open(self.filename, "wb", self.gzip_compression_level)
        #else:
        #    self.f = open(self.filename, "w", buffering = -1)
        self.f = open(self.filename, 'w', buffering=-1, encoding='utf-8')


    def write_header(self, bounds):
        self.logger.debug("Writing file header")
        
        self.f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
        self.f.write('<osm version="0.6" generator="ogr2osm %s"' % self.get_version())
        if self.never_upload:
            self.f.write(' upload="never"')
        elif not self.no_upload_false:
            self.f.write(' upload="false"')
        if self.never_download:
            self.f.write(' download="never"')
        if self.locked:
            self.f.write(' locked="true"')
        self.f.write('>\n')

        if bounds and bounds.is_valid:
            self.f.write(bounds.to_xml(self.significant_digits))
            self.f.write('\n')


    def __write_geometries(self, geoms):
        for osm_geom in geoms:
            self.f.write(osm_geom.to_xml(self.attributes, \
                                         self.significant_digits, \
                                         self.suppress_empty_tags, \
                                         self.max_tag_length, \
                                         DataWriterBase.TAG_OVERFLOW))
            self.f.write('\n')


    def write_nodes(self, nodes):
        self.logger.debug("Writing nodes")
        self.__write_geometries(nodes)


    def write_ways(self, ways):
        self.logger.debug("Writing ways")
        self.__write_geometries(ways)


    def write_relations(self, relations):
        self.logger.debug("Writing relations")
        self.__write_geometries(relations)


    def write_footer(self):
        self.logger.debug("Writing file footer")
        self.f.write('</osm>')


    def close(self):
        if self.f:
            self.f.close()
            self.f = None
