# -*- coding: utf-8 -*-

'''
Copyright (c) 2012-2021 Roel Derickx, Paul Norman <penorman@mac.com>,
Sebastiaan Couwenberg <sebastic@xs4all.nl>, The University of Vermont
<andrew.guertin@uvm.edu>, github contributors

Released under the MIT license, as given in the file LICENSE, which must
accompany any distribution of this code.
'''

import logging
from osgeo import ogr
from osgeo import osr

from .version import __program__
from .osm_geometries import OsmId, OsmBoundary, OsmNode, OsmWay, OsmRelation

class OsmData:
    def __init__(self, translation, rounding_digits=7, significant_digits=9, \
                 max_points_in_way=1800, add_bounds=False, start_id=0, is_positive=False, \
                 z_value_tagname=None):
        self.logger = logging.getLogger(__program__)

        # options
        self.translation = translation
        self.rounding_digits = rounding_digits
        self.significant_digits = significant_digits
        self.max_points_in_way = max_points_in_way
        self.add_bounds = add_bounds
        self.z_value_tagname = z_value_tagname

        self.__bounds = OsmBoundary()
        self.__nodes = []
        self.__unique_node_index = {}
        self.__ways = []
        self.__relations = []

        OsmId.set_id(start_id, is_positive)


    def load_start_id_from_file(self, filename):
        if filename:
            if len(self.__nodes) == 0 and len(self.__ways) == 0 and len(self.__relations) == 0:
                OsmId.load_id(filename)
            else:
                self.logger.error("Id can only be set before processing, no file has been loaded")


    def save_current_id_to_file(self, filename):
        if filename:
            OsmId.save_id(filename)


    def __get_layer_fields(self, layer):
        layer_fields = []
        layer_def = layer.GetLayerDefn()
        for i in range(layer_def.GetFieldCount()):
            field_def = layer_def.GetFieldDefn(i)
            layer_fields.append((i, field_def.GetNameRef(), field_def.GetType()))
        return layer_fields


    # This function builds up a dictionary with the source data attributes
    # and passes them to the filter_tags function, returning the result.
    def __get_feature_tags(self, ogrfeature, layer_fields, source_encoding):
        tags = {}
        for (index, field_name, field_type) in layer_fields:
            field_value = ''
            if field_type == ogr.OFTString:
                field_value = ogrfeature.GetFieldAsBinary(index).decode(source_encoding)
            else:
                field_value = ogrfeature.GetFieldAsString(index)

            tags[field_name] = field_value.strip()

        return self.translation.filter_tags(tags)


    def __calc_bounds(self, ogrgeometry):
        (minx, maxx, miny, maxy) = ogrgeometry.GetEnvelope()
        self.__bounds.add_envelope(minx, maxx, miny, maxy)


    def __round_number(self, n):
        return int(round(n * 10**self.rounding_digits))


    def __add_node(self, x, y, z, tags, is_way_member):
        rx = self.__round_number(x)
        ry = self.__round_number(y)

        unique_node_id = (rx, ry)

        if unique_node_id in self.__unique_node_index:
            for index in self.__unique_node_index[unique_node_id]:
                duplicate_node = self.__nodes[index]
                merged_tags = self.translation.merge_tags('node', duplicate_node.tags, tags)
                if self.z_value_tagname:
                    strz = (('%%.%df' % self.significant_digits) % z).rstrip('0').rstrip('.')
                    new_tags = { self.z_value_tagname: strz }
                    merged_tags = self.translation.merge_tags('node', merged_tags, new_tags)
                if merged_tags is not None:
                    duplicate_node.tags = merged_tags
                    return duplicate_node

            node = OsmNode(x, y, tags)
            self.__unique_node_index[unique_node_id].append(len(self.__nodes))
            self.__nodes.append(node)
            return node
        else:
            merged_tags = tags
            if self.z_value_tagname:
                strz = (('%%.%df' % self.significant_digits) % z).rstrip('0').rstrip('.')
                new_tags = { self.z_value_tagname: [ strz ] }
                merged_tags = self.translation.merge_tags('node', new_tags, tags)
            node = OsmNode(x, y, merged_tags)
            self.__unique_node_index[unique_node_id] = [ len(self.__nodes) ]
            self.__nodes.append(node)
            return node


    def __add_way(self, tags):
        way = OsmWay(tags)
        self.__ways.append(way)
        return way


    def __add_relation(self, tags):
        relation = OsmRelation(tags)
        self.__relations.append(relation)
        return relation


    def __parse_point(self, ogrgeometry, tags):
        return self.__add_node(ogrgeometry.GetX(), ogrgeometry.GetY(), ogrgeometry.GetZ(), \
                               tags, False)


    def __parse_multi_point(self, ogrgeometry, tags):
        nodes = []
        for point in range(ogrgeometry.GetGeometryCount()):
            nodes.append(self.__parse_point(ogrgeometry.GetGeometryRef(point), tags))
        return nodes


    def __get_ordered_nodes(self, nodes):
        is_closed = len(nodes) > 2 and nodes[0].id == nodes[-1].id
        if is_closed:
            lowest_index = 0
            lowest_node_id = nodes[0].id
            for i in range(1, len(nodes)):
                if nodes[i].id < lowest_node_id:
                    lowest_node_id = nodes[i].id
                    lowest_index = i

            return nodes[lowest_index:-1] + nodes[:lowest_index+1]
        else:
            return nodes


    def __verify_duplicate_ways(self, potential_duplicate_ways, nodes, tags):
        duplicate_ways = []
        ordered_nodes = self.__get_ordered_nodes(nodes)
        for dupway in potential_duplicate_ways:
            if len(dupway.nodes) == len(nodes):
                dupnodes = self.__get_ordered_nodes(dupway.nodes)
                merged_tags = None
                if dupnodes == ordered_nodes:
                    #duplicate_ways.append((dupway, 'way'))
                    merged_tags = self.translation.merge_tags('way', dupway.tags, tags)
                elif dupnodes == list(reversed(ordered_nodes)):
                    #duplicate_ways.append((dupway, 'reverse_way'))
                    merged_tags = self.translation.merge_tags('reverse_way', dupway.tags, tags)
                if merged_tags is not None:
                    dupway.tags = merged_tags
                    return dupway

        way = self.__add_way(tags)
        way.nodes = nodes
        for node in nodes:
            node.addparent(way)
        return way


    def __parse_linestring(self, ogrgeometry, tags):
        previous_node_id = None
        nodes = []
        potential_duplicate_ways = []
        for i in range(ogrgeometry.GetPointCount()):
            (x, y, z) = ogrgeometry.GetPoint(i)
            node = self.__add_node(x, y, z, {}, True)
            if previous_node_id is None or previous_node_id != node.id:
                if previous_node_id is None:
                    # first node: add all parent ways as potential duplicates
                    potential_duplicate_ways = [ p for p in node.get_parents() if type(p) == OsmWay ]
                elif not any(node.get_parents()) and any(potential_duplicate_ways):
                    # next nodes: if node doesn't belong to another way then this way is unique
                    potential_duplicate_ways.clear()
                nodes.append(node)
                previous_node_id = node.id

        return self.__verify_duplicate_ways(potential_duplicate_ways, nodes, tags)


    def __parse_multi_linestring(self, ogrgeometry, tags):
        ways = []
        for linestring in range(ogrgeometry.GetGeometryCount()):
            ways.append(self.__parse_linestring(ogrgeometry.GetGeometryRef(linestring), tags))
        return ways


    def __parse_polygon_members(self, ogrgeometry, potential_duplicate_relations, first = True):
        members = []
        
        # exterior ring
        exterior_geom_type = ogrgeometry.GetGeometryRef(0).GetGeometryType()
        if exterior_geom_type in [ ogr.wkbLineString, ogr.wkbLinearRing, ogr.wkbLineString25D ]:
            exterior = self.__parse_linestring(ogrgeometry.GetGeometryRef(0), {})
            members.append((exterior, 'outer'))
            if first:
                # first member: add all parent relations as potential duplicates
                potential_duplicate_relations.extend(
                    [ p for p in exterior.get_parents() \
                        if type(p) == OsmRelation and p.get_member_role(exterior) == 'outer' ])
            elif not any(exterior.get_parents()) and any(potential_duplicate_relations):
                # next members: if interior doesn't belong to another relation then this
                #               relation is unique
                potential_duplicate_relations.clear()
        else:
            self.logger.warning("Polygon with no exterior ring?")
            return None

        # interior rings
        for i in range(1, ogrgeometry.GetGeometryCount()):
            interior = self.__parse_linestring(ogrgeometry.GetGeometryRef(i), {})
            members.append((interior, "inner"))
            if not any(interior.get_parents()) and any(potential_duplicate_relations):
                # next members: if interior doesn't belong to another relation then this
                #               relation is unique
                potential_duplicate_relations.clear()

        return members


    def __verify_duplicate_relations(self, potential_duplicate_relations, members, tags):
        if members is None:
            return None

        duplicate_relations = []
        for duprelation in potential_duplicate_relations:
            if duprelation.members == members:
                merged_tags = self.translation.merge_tags('relation', duprelation.tags, tags)
                if merged_tags is not None:
                    duprelation.tags = merged_tags
                    return duprelation

        relation = self.__add_relation(tags)
        for m in members:
            m[0].addparent(relation)
        relation.members = members
        return relation


    def __parse_polygon(self, ogrgeometry, tags):
        # Special case polygons with only one ring. This does not (or at least
        # should not) change behavior when simplify relations is turned on.
        if ogrgeometry.GetGeometryCount() == 0:
            self.logger.warning("Polygon with no rings?")
            return None
        elif ogrgeometry.GetGeometryCount() == 1 and \
             ogrgeometry.GetGeometryRef(0).GetPointCount() <= self.max_points_in_way:
            # only 1 linestring which is not too long: no relation required
            result = self.__parse_linestring(ogrgeometry.GetGeometryRef(0), tags)
            return result
        else:
            potential_duplicate_relations = []
            members = self.__parse_polygon_members(ogrgeometry, potential_duplicate_relations)
            return self.__verify_duplicate_relations(potential_duplicate_relations, members, tags)


    def __parse_multi_polygon(self, ogrgeometry, tags):
        if ogrgeometry.GetGeometryCount() > 1:
            members = []
            potential_duplicate_relations = []
            for polygon in range(ogrgeometry.GetGeometryCount()):
                members.extend(self.__parse_polygon_members(ogrgeometry.GetGeometryRef(polygon), \
                                                            potential_duplicate_relations, \
                                                            polygon == 0))

            return self.__verify_duplicate_relations(potential_duplicate_relations, members, tags)
        else:
            return self.__parse_polygon(ogrgeometry.GetGeometryRef(0), tags)


    def __parse_collection(self, ogrgeometry, tags):
        osmgeometries = []
        members = []
        potential_duplicate_relations = []
        for geom in range(ogrgeometry.GetGeometryCount()):
            collection_geom = ogrgeometry.GetGeometryRef(geom)
            collection_geom_type = collection_geom.GetGeometryType()

            if collection_geom_type in [ ogr.wkbPoint, ogr.wkbPoint25D, \
                                         ogr.wkbMultiPoint, ogr.wkbMultiPoint25D, \
                                         ogr.wkbLineString, ogr.wkbLinearRing, ogr.wkbLineString25D, \
                                         ogr.wkbMultiLineString, ogr.wkbMultiLineString25D ]:
                osmgeometries.extend(self.__parse_geometry(collection_geom, tags))
            elif collection_geom_type in [ ogr.wkbPolygon, ogr.wkbPolygon25D, \
                                           ogr.wkbMultiPolygon, ogr.wkbMultiPolygon25D ]:
                members.extend(self.__parse_polygon_members(collection_geom, \
                                                            potential_duplicate_relations, \
                                                            not any(members)))
            else:
                # no support for nested collections or other unsupported types
                self.logger.warning("Unhandled geometry in collection, type %d", geometry_type)

        if len(members) == 1 and len(members[0].nodes) <= self.max_points_in_way:
            # only 1 polygon with 1 outer ring
            member[0].tags.update(tags)
            osmgeometries.append(member[0])
        elif len(members) > 1:
            osmgeometries.append(\
                self.__verify_duplicate_relations(potential_duplicate_relations, members, tags))

        return osmgeometries


    def __parse_geometry(self, ogrgeometry, tags):
        osmgeometries = []

        geometry_type = ogrgeometry.GetGeometryType()

        if geometry_type in [ ogr.wkbPoint, ogr.wkbPoint25D ]:
            osmgeometries.append(self.__parse_point(ogrgeometry, tags))
        elif geometry_type in [ ogr.wkbMultiPoint, ogr.wkbMultiPoint25D ]:
            osmgeometries.extend(self.__parse_multi_point(ogrgeometry, tags))
        elif geometry_type in [ ogr.wkbLineString, ogr.wkbLinearRing, ogr.wkbLineString25D ]:
            # ogr.wkbLinearRing25D does not exist
            osmgeometries.append(self.__parse_linestring(ogrgeometry, tags))
        elif geometry_type in [ ogr.wkbMultiLineString, ogr.wkbMultiLineString25D ]:
            osmgeometries.extend(self.__parse_multi_linestring(ogrgeometry, tags))
        elif geometry_type in [ ogr.wkbPolygon, ogr.wkbPolygon25D ]:
            osmgeometries.append(self.__parse_polygon(ogrgeometry, tags))
        elif geometry_type in [ ogr.wkbMultiPolygon, ogr.wkbMultiPolygon25D ]:
            # OGR MultiPolygon maps easily to osm multipolygon, so special case it
            # TODO: Does anything else need special casing?
            osmgeometries.append(self.__parse_multi_polygon(ogrgeometry, tags))
        elif geometry_type in [ ogr.wkbGeometryCollection, ogr.wkbGeometryCollection25D ]:
            osmgeometries.append(self.__parse_collection(ogrgeometry, tags))
        else:
            self.logger.warning("Unhandled geometry, type %d", geometry_type)

        return osmgeometries


    def add_feature(self, ogrfeature, layer_fields, source_encoding, reproject = lambda geometry: None):
        ogrfilteredfeature = self.translation.filter_feature(ogrfeature, layer_fields, reproject)
        if ogrfilteredfeature is None:
            return

        ogrgeometry = ogrfilteredfeature.GetGeometryRef()
        if ogrgeometry is None:
            return

        feature_tags = self.__get_feature_tags(ogrfilteredfeature, layer_fields, source_encoding)
        if feature_tags is None:
            return

        reproject(ogrgeometry)

        if self.add_bounds:
            self.__calc_bounds(ogrgeometry)

        osmgeometries = self.__parse_geometry(ogrgeometry, feature_tags)

        # TODO performance: run in __parse_geometry to avoid second loop
        for osmgeometry in [ geom for geom in osmgeometries if geom ]:
            self.translation.process_feature_post(osmgeometry, ogrfilteredfeature, ogrgeometry)


    def __split_way(self, way):
        new_nodes = [ way.nodes[i:i + self.max_points_in_way] \
                               for i in range(0, len(way.nodes), self.max_points_in_way - 1) ]
        new_ways = [ way ] + [ OsmWay(way.tags) for i in range(len(new_nodes) - 1) ]

        for new_way, nodes in zip(new_ways, new_nodes):
            new_way.nodes = nodes
            if new_way.id != way.id:
                self.__ways.append(new_way)
                for node in nodes:
                    node.removeparent(way)
                    node.addparent(new_way)

        return new_ways


    def __split_way_in_relation(self, rel, way_parts):
        way_role = rel.get_member_role(way_parts[0])
        for way in way_parts[1:]:
            way.addparent(rel)
            rel.members.append((way, way_role))


    def split_long_ways(self):
        if self.max_points_in_way < 2:
            # pointless :-)
            return

        self.logger.debug("Splitting long ways")

        for way in self.__ways:
            if len(way.nodes) > self.max_points_in_way:
                way_parts = self.__split_way(way)
                for rel in way.get_parents():
                    self.__split_way_in_relation(rel, way_parts)


    def process(self, datasource):
        for i in range(datasource.get_layer_count()):
            (layer, reproject) = datasource.get_layer(i)

            if layer:
                layer_fields = self.__get_layer_fields(layer)
                for j in range(layer.GetFeatureCount()):
                    ogrfeature = layer.GetNextFeature()
                    self.add_feature(ogrfeature, layer_fields, datasource.source_encoding, reproject)

        self.split_long_ways()


    class DataWriterContextManager:
        def __init__(self, datawriter):
            self.datawriter = datawriter

        def __enter__(self):
            self.datawriter.open()
            return self.datawriter

        def __exit__(self, exception_type, value, traceback):
            self.datawriter.close()


    def output(self, datawriter):
        self.translation.process_output(self.__nodes, self.__ways, self.__relations)

        with self.DataWriterContextManager(datawriter) as dw:
            dw.write_header(self.__bounds)
            dw.write_nodes(self.__nodes)
            dw.write_ways(self.__ways)
            dw.write_relations(self.__relations)
            dw.write_footer()
