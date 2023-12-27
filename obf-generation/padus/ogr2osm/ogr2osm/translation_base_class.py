# -*- coding: utf-8 -*-

'''
Copyright (c) 2012-2021 Roel Derickx, Paul Norman <penorman@mac.com>,
Sebastiaan Couwenberg <sebastic@xs4all.nl>, The University of Vermont
<andrew.guertin@uvm.edu>, github contributors

Released under the MIT license, as given in the file LICENSE, which must
accompany any distribution of this code.
'''

class TranslationBase:
    '''
    Base class for translations. Override the methods in this class to remove
    layers, filter features and tags, merge tags or otherwise modify the output
    to suit your needs.
    '''

    def __init__(self):
        pass


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
        for (key, value_list) in tags_existing_geometry.items():
            if key in tags_new_geometry.keys() and tags_new_geometry[key] not in value_list:
                value_list.append(tags_new_geometry[key])
                tags.update({ key: value_list })
            else:
                tags.update({ key: value_list })
        for (key, value) in tags_new_geometry.items():
            if key not in tags.keys():
                tags.update({ key: [ value ] })
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
