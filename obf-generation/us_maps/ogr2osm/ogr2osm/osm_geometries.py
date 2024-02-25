# -*- coding: utf-8 -*-

'''
Copyright (c) 2012-2021 Roel Derickx, Paul Norman <penorman@mac.com>,
Sebastiaan Couwenberg <sebastic@xs4all.nl>, The University of Vermont
<andrew.guertin@uvm.edu>, github contributors

Released under the MIT license, as given in the file LICENSE, which must
accompany any distribution of this code.
'''

import logging
from lxml import etree

from .version import __program__

class OsmId:
    element_id_counter = 0
    element_id_counter_incr = -1

    @staticmethod
    def set_id(start_id, is_positive = False):
        OsmId.element_id_counter = start_id
        if is_positive:
            OsmId.element_id_counter_incr = 1


    @staticmethod
    def load_id(filename):
        logger = logging.getLogger(__program__)
        with open(filename, 'r') as ff:
            OsmId.element_id_counter = int(ff.readline(20))
        logger.info("Starting counter value '%d' read from file '%s'.", \
                    OsmId.element_id_counter, filename)


    @staticmethod
    def save_id(filename):
        logger = logging.getLogger(__program__)
        with open(filename, 'w') as ff:
            ff.write(str(OsmId.element_id_counter))
        logger.info("Wrote elementIdCounter '%d' to file '%s'", \
                    OsmId.element_id_counter, filename)



class OsmBoundary:
    def __init__(self):
        self.is_valid = False
        self.minlon = 0.0
        self.maxlon = 0.0
        self.minlat = 0.0
        self.maxlat = 0.0


    def add_envelope(self, minx, maxx, miny, maxy):
        if self.is_valid:
            self.minlon = min(self.minlon, minx)
            self.maxlon = max(self.maxlon, maxx)
            self.minlat = min(self.minlat, miny)
            self.maxlat = max(self.maxlat, maxy)
        else:
            self.is_valid = True
            self.minlon = minx
            self.maxlon = maxx
            self.minlat = miny
            self.maxlat = maxy


    def to_xml(self, significant_digits):
        formatting = ('%%.%df' % significant_digits)
        xmlattrs = { 'minlon':(formatting % self.minlon).rstrip('0').rstrip('.'), \
                     'minlat':(formatting % self.minlat).rstrip('0').rstrip('.'), \
                     'maxlon':(formatting % self.maxlon).rstrip('0').rstrip('.'), \
                     'maxlat':(formatting % self.maxlat).rstrip('0').rstrip('.') }
        xmlobject = etree.Element('bounds', xmlattrs)

        return etree.tostring(xmlobject, encoding='unicode')



class OsmGeometry:
    def __init__(self):
        self.id = self.__get_new_id()
        self.tags = {}
        self.__parents = set()


    def __get_new_id(self):
        OsmId.element_id_counter += OsmId.element_id_counter_incr
        return OsmId.element_id_counter


    def addparent(self, parent):
        self.__parents.add(parent)


    def removeparent(self, parent):
        self.__parents.discard(parent)


    def get_parents(self):
        return self.__parents


    def _add_tags_to_xml(self, xmlobject, suppress_empty_tags, max_tag_length, tag_overflow):
        for (key, value_list) in self.tags.items():
            value = ';'.join([ v for v in value_list if v ])
            if len(value) > max_tag_length:
                value = value[:(max_tag_length - len(tag_overflow))] + tag_overflow
            if value or not suppress_empty_tags:
                tag = etree.Element('tag', { 'k':key, 'v':value })
                xmlobject.append(tag)


    def to_xml(self, attributes, significant_digits, \
                     suppress_empty_tags, max_tag_length, tag_overflow):
        pass



class OsmNode(OsmGeometry):
    def __init__(self, x, y, tags):
        super().__init__()
        self.x = x
        self.y = y
        self.tags.update({ k: (v if type(v) == list else [ v ]) for (k, v) in tags.items() })


    def to_xml(self, attributes, significant_digits, \
                     suppress_empty_tags, max_tag_length, tag_overflow):
        formatting = ('%%.%df' % significant_digits)
        xmlattrs = { 'visible':'true', \
                     'id':('%d' % self.id), \
                     'lat':(formatting % self.y).rstrip('0').rstrip('.'), \
                     'lon':(formatting % self.x).rstrip('0').rstrip('.') }
        xmlattrs.update(attributes)

        xmlobject = etree.Element('node', xmlattrs)

        self._add_tags_to_xml(xmlobject, suppress_empty_tags, max_tag_length, tag_overflow)

        return etree.tostring(xmlobject, encoding='unicode')



class OsmWay(OsmGeometry):
    def __init__(self, tags):
        super().__init__()
        self.nodes = []
        self.tags.update({ k: (v if type(v) == list else [ v ]) for (k, v) in tags.items() })


    def to_xml(self, attributes, significant_digits, \
                     suppress_empty_tags, max_tag_length, tag_overflow):
        xmlattrs = { 'visible':'true', 'id':('%d' % self.id) }
        xmlattrs.update(attributes)

        xmlobject = etree.Element('way', xmlattrs)

        for node in self.nodes:
            nd = etree.Element('nd', { 'ref':('%d' % node.id) })
            xmlobject.append(nd)

        self._add_tags_to_xml(xmlobject, suppress_empty_tags, max_tag_length, tag_overflow)

        return etree.tostring(xmlobject, encoding='unicode')



class OsmRelation(OsmGeometry):
    def __init__(self, tags):
        super().__init__()
        self.members = []
        self.tags['type'] = [ 'multipolygon' ]
        self.tags.update({ k: (v if type(v) == list else [ v ]) for (k, v) in tags.items() })


    def get_member_role(self, member):
        member_roles = [ m[1] for m in self.members if m[0] == member ]
        member_role = member_roles[0] if any(member_roles) else ""
        return member_role


    def to_xml(self, attributes, significant_digits, \
                     suppress_empty_tags, max_tag_length, tag_overflow):
        xmlattrs = { 'visible':'true', 'id':('%d' % self.id) }
        xmlattrs.update(attributes)

        xmlobject = etree.Element('relation', xmlattrs)

        for (member, role) in self.members:
            member_type = None
            if type(member) == OsmNode:
                member_type = 'node'
            elif type(member) == OsmWay:
                member_type = 'way'
            elif type(member) == OsmRelation:
                member_type = 'relation'
            xmlmember = etree.Element('member', { 'type':member_type, \
                                                  'ref':('%d' % member.id), 'role':role })
            xmlobject.append(xmlmember)

        self._add_tags_to_xml(xmlobject, suppress_empty_tags, max_tag_length, tag_overflow)

        return etree.tostring(xmlobject, encoding='unicode')
