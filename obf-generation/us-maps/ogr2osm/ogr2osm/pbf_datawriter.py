# -*- coding: utf-8 -*-

'''
Copyright (c) 2012-2021 Roel Derickx, Paul Norman <penorman@mac.com>,
Sebastiaan Couwenberg <sebastic@xs4all.nl>, The University of Vermont
<andrew.guertin@uvm.edu>, github contributors

Released under the MIT license, as given in the file LICENSE, which must
accompany any distribution of this code.
'''

import logging
import sys
import time
import zlib

from .version import __program__
from .osm_geometries import OsmId, OsmNode, OsmWay, OsmRelation
from .datawriter_base_class import DataWriterBase

is_protobuf_installed = False

class PbfDataWriter(DataWriterBase):
    def __init__(self, filename, add_version=False, add_timestamp=False, \
                 suppress_empty_tags=False):
        pass

try:
    import ogr2osm.fileformat_pb2 as fileprotobuf
    import ogr2osm.osmformat_pb2 as osmprotobuf

    is_protobuf_installed = True

    # https://wiki.openstreetmap.org/wiki/PBF_Format

    class PbfPrimitiveGroup:
        def __init__(self, add_version, add_timestamp, suppress_empty_tags, max_tag_length):
            self.stringtable = {}
            self._add_string("")

            self._add_version = add_version
            self._version = -1
            if self._add_version:
                self._version = 1
            self._add_timestamp = add_timestamp
            self._timestamp = -1
            if self._add_timestamp:
                self._timestamp = time.time()
            self.suppress_empty_tags = suppress_empty_tags
            self.max_tag_length = max_tag_length

            self.granularity = 100
            self.lat_offset = 0
            self.lon_offset = 0
            self.date_granularity = 1000

            self.primitive_group = osmprotobuf.PrimitiveGroup()


        def _add_string(self, s):
            '''
            Add string s to the stringtable if not yet present and returns index
            '''
            if not s in self.stringtable:
                index = len(self.stringtable)
                self.stringtable[s] = index
                return index
            else:
                return self.stringtable[s]


        def _get_tag_iterator(self, tags):
            '''
            Returns an iterator over all (key, value) pairs of tags which should be included
            based on suppress_empty_tags.
            '''
            for (key, value_list) in tags.items():
                value = ';'.join([ v for v in value_list if v ])
                if len(value) > self.max_tag_length:
                    value = value[:(self.max_tag_length - len(DataWriterBase.TAG_OVERFLOW))] \
                            + DataWriterBase.TAG_OVERFLOW
                if value or not self.suppress_empty_tags:
                    yield (self._add_string(key), self._add_string(value))


        def _lat_to_pbf(self, lat):
            '''
            Convert given latitude to value used in pbf
            '''
            return int((lat * 1e9 - self.lat_offset) / self.granularity)


        def _lon_to_pbf(self, lon):
            '''
            Convert given longitude to value used in pbf
            '''
            return int((lon * 1e9 - self.lon_offset) / self.granularity)


        def _timestamp_to_pbf(self, timestamp):
            '''
            Convert seconds since the unix epoch to value used in pbf
            '''
            return int(timestamp * 1000 / self.date_granularity)



    class PbfPrimitiveGroupDenseNodes(PbfPrimitiveGroup):
        def __init__(self, add_version, add_timestamp, suppress_empty_tags, max_tag_length):
            super().__init__(add_version, add_timestamp, suppress_empty_tags, max_tag_length)

            self.__last_id = 0
            self.__last_timestamp = 0
            self.__last_changeset = 0
            self.__last_lat = 0
            self.__last_lon = 0


        def add_node(self, osmnode):
            pbftimestamp = self._timestamp_to_pbf(self._timestamp)
            pbfchangeset = 1
            pbflat = self._lat_to_pbf(osmnode.y)
            pbflon = self._lon_to_pbf(osmnode.x)

            self.primitive_group.dense.id.append(osmnode.id - self.__last_id)

            # osmosis always requires the whole denseinfo block
            if self._add_version or self._add_timestamp:
                self.primitive_group.dense.denseinfo.version.append(self._version)
                self.primitive_group.dense.denseinfo.timestamp.append( \
                                                            pbftimestamp - self.__last_timestamp)
                self.primitive_group.dense.denseinfo.changeset.append( \
                                                            pbfchangeset - self.__last_changeset)
                self.primitive_group.dense.denseinfo.uid.append(0)
                self.primitive_group.dense.denseinfo.user_sid.append(0)

            self.primitive_group.dense.lat.append(pbflat - self.__last_lat)
            self.primitive_group.dense.lon.append(pbflon - self.__last_lon)

            self.__last_id = osmnode.id
            self.__last_timestamp = pbftimestamp
            self.__last_changeset = pbfchangeset
            self.__last_lat = pbflat
            self.__last_lon = pbflon

            for (key, value) in self._get_tag_iterator(osmnode.tags):
                self.primitive_group.dense.keys_vals.append(key)
                self.primitive_group.dense.keys_vals.append(value)
            self.primitive_group.dense.keys_vals.append(0)



    class PbfPrimitiveGroupWays(PbfPrimitiveGroup):
        def __init__(self, add_version, add_timestamp, suppress_empty_tags, max_tag_length):
            super().__init__(add_version, add_timestamp, suppress_empty_tags, max_tag_length)


        def add_way(self, osmway):
            way = osmprotobuf.Way()
            way.id = osmway.id

            for (key, value) in self._get_tag_iterator(osmway.tags):
                way.keys.append(key)
                way.vals.append(value)

            # osmosis always requires the whole info block
            if self._add_version or self._add_timestamp:
                way.info.version = self._version
                way.info.timestamp = self._timestamp_to_pbf(self._timestamp)
                way.info.changeset = 1
                way.info.uid = 0
                way.info.user_sid = 0

            prev_node_id = 0
            for node in osmway.nodes:
                way.refs.append(node.id - prev_node_id)
                prev_node_id = node.id

            self.primitive_group.ways.append(way)



    class PbfPrimitiveGroupRelations(PbfPrimitiveGroup):
        def __init__(self, add_version, add_timestamp, suppress_empty_tags, max_tag_length):
            super().__init__(add_version, add_timestamp, suppress_empty_tags, max_tag_length)


        def add_relation(self, osmrelation):
            relation = osmprotobuf.Relation()
            relation.id = osmrelation.id

            for (key, value) in self._get_tag_iterator(osmrelation.tags):
                relation.keys.append(key)
                relation.vals.append(value)

            # osmosis always requires the whole info block
            if self._add_version or self._add_timestamp:
                relation.info.version = self._version
                relation.info.timestamp = self._timestamp_to_pbf(self._timestamp)
                relation.info.changeset = 1
                relation.info.uid = 0
                relation.info.user_sid = 0

            prev_member_id = 0
            for (member, role) in osmrelation.members:
                relation.roles_sid.append(self._add_string(role))

                relation.memids.append(member.id - prev_member_id)
                prev_member_id = member.id

                relation_type = osmprotobuf.Relation.MemberType.NODE
                if type(member) == OsmWay:
                    relation_type = osmprotobuf.Relation.MemberType.WAY
                elif type(member) == OsmRelation:
                    relation_type = osmprotobuf.Relation.MemberType.RELATION
                relation.types.append(relation_type)

            self.primitive_group.relations.append(relation)



    class PbfDataWriter(DataWriterBase):
        def __init__(self, filename, add_version=False, add_timestamp=False, \
                     suppress_empty_tags=False, max_tag_length=255):
            self.logger = logging.getLogger(__program__)

            self.filename = filename
            self.add_version = add_version
            self.add_timestamp = add_timestamp
            self.suppress_empty_tags = suppress_empty_tags
            self.max_tag_length = max_tag_length

            self.__max_nodes_per_node_block = 8000
            self.__max_node_refs_per_way_block = 32000
            self.__max_member_refs_per_relation_block = 32000


        def open(self):
            self.f = open(self.filename, 'wb', buffering = -1)


        def __write_blob(self, data, block_type):
            #self.logger.debug("Writing blob, type = %s" % block_type)

            blob = fileprotobuf.Blob()
            blob.raw_size = len(data)
            blob.zlib_data = zlib.compress(data)

            blobheader = fileprotobuf.BlobHeader()
            blobheader.type = block_type
            blobheader.datasize = blob.ByteSize()

            blobheaderlen = blobheader.ByteSize().to_bytes(4, byteorder='big')
            self.f.write(blobheaderlen)
            self.f.write(blobheader.SerializeToString())
            self.f.write(blob.SerializeToString())


        def write_header(self, bounds):
            self.logger.debug("Writing file header")

            header_block = osmprotobuf.HeaderBlock()
            if bounds and bounds.is_valid:
                header_block.bbox.left = int(bounds.minlon * 1e9)
                header_block.bbox.right = int(bounds.maxlon * 1e9)
                header_block.bbox.top = int(bounds.maxlat * 1e9)
                header_block.bbox.bottom = int(bounds.minlat * 1e9)
            header_block.required_features.append("OsmSchema-V0.6")
            header_block.required_features.append("DenseNodes")
            header_block.writingprogram = "ogr2osm %s" % self.get_version()
            self.__write_blob(header_block.SerializeToString(), "OSMHeader")


        def __write_primitive_block(self, pbf_primitive_group):
            #self.logger.debug("Primitive block generation")

            primitive_block = osmprotobuf.PrimitiveBlock()
            # add stringtable
            for (string, index) in sorted(pbf_primitive_group.stringtable.items(), \
                                          key=lambda kv: kv[1]):
                primitive_block.stringtable.s.append(string.encode('utf-8'))
            # add geometries
            primitive_block.primitivegroup.append(pbf_primitive_group.primitive_group)
            # set parameters
            primitive_block.granularity = pbf_primitive_group.granularity
            primitive_block.lat_offset = pbf_primitive_group.lat_offset
            primitive_block.lon_offset = pbf_primitive_group.lon_offset
            primitive_block.date_granularity = pbf_primitive_group.date_granularity
            # write primitive block
            self.__write_blob(primitive_block.SerializeToString(), "OSMData")


        def write_nodes(self, nodes):
            self.logger.debug("Writing nodes")
            for i in range(0, len(nodes), self.__max_nodes_per_node_block):
                primitive_group = PbfPrimitiveGroupDenseNodes(self.add_version, \
                                                              self.add_timestamp, \
                                                              self.suppress_empty_tags, \
                                                              self.max_tag_length)
                for node in nodes[i:i+self.__max_nodes_per_node_block]:
                    primitive_group.add_node(node)
                self.__write_primitive_block(primitive_group)


        def write_ways(self, ways):
            self.logger.debug("Writing ways")
            amount_node_refs = 0
            primitive_group = None
            for way in ways:
                if amount_node_refs == 0:
                    primitive_group = PbfPrimitiveGroupWays(self.add_version, \
                                                            self.add_timestamp, \
                                                            self.suppress_empty_tags, \
                                                            self.max_tag_length)
                primitive_group.add_way(way)
                amount_node_refs += len(way.nodes)
                if amount_node_refs > self.__max_node_refs_per_way_block:
                    self.__write_primitive_block(primitive_group)
                    amount_node_refs = 0
            else:
                if amount_node_refs > 0:
                    self.__write_primitive_block(primitive_group)


        def write_relations(self, relations):
            self.logger.debug("Writing relations")
            amount_member_refs = 0
            primitive_group = None
            for relation in relations:
                if amount_member_refs == 0:
                    primitive_group = PbfPrimitiveGroupRelations(self.add_version, \
                                                                 self.add_timestamp, \
                                                                 self.suppress_empty_tags, \
                                                                 self.max_tag_length)
                primitive_group.add_relation(relation)
                amount_member_refs += len(relation.members)
                if amount_member_refs > self.__max_member_refs_per_relation_block:
                    self.__write_primitive_block(primitive_group)
                    amount_member_refs = 0
            else:
                if amount_member_refs > 0:
                    self.__write_primitive_block(primitive_group)


        def close(self):
            if self.f:
                self.f.close()
                self.f = None
except:
    pass
