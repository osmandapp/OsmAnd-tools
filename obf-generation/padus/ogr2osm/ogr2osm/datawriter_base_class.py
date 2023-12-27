# -*- coding: utf-8 -*-

'''
Copyright (c) 2012-2021 Roel Derickx, Paul Norman <penorman@mac.com>,
Sebastiaan Couwenberg <sebastic@xs4all.nl>, The University of Vermont
<andrew.guertin@uvm.edu>, github contributors

Released under the MIT license, as given in the file LICENSE, which must
accompany any distribution of this code.
'''

from .version import __version__

class DataWriterBase:
    '''
    Base class for all datawriters. Ogr2osm will do the following,
    given an instance dw of class DataWriterBase:

    dw.open()
    try:
        dw.write_header()
        dw.write_nodes(node_list)
        dw.write_ways(way_list)
        dw.write_relations(relation_list)
        dw.write_footer()
    finally:
        dw.close()
    '''

    TAG_OVERFLOW = '...'

    def __init__(self):
        pass


    def get_version(self):
        '''
        This method returns the ogr2osm version number which can be used
        as part of the generator identifier string in the output file
        '''
        return __version__


    def open(self):
        pass


    def write_header(self, bounds):
        pass


    def write_nodes(self, nodes):
        pass


    def write_ways(self, ways):
        pass


    def write_relations(self, relations):
        pass


    def write_footer(self):
        pass


    def close(self):
        pass
