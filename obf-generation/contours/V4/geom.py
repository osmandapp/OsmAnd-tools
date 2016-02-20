# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2013 Paul Norman
# <penorman@mac.com>
# Released under the MIT license: http://opensource.org/licenses/mit-license.php

# Classes
class Geometry(object):
    elementIdCounter = 0
    elementIdCounterIncr = -1
    geometries = []
    def __init__(self):
        self.id = getNewID()
        self.parents = set()
        Geometry.geometries.append(self)
    def replacejwithi(self, i, j):
        pass
    def addparent(self, parent):
        self.parents.add(parent)
    def removeparent(self, parent, shoulddestroy=True):
        self.parents.discard(parent)
        if shoulddestroy and len(self.parents) == 0:
            Geometry.geometries.remove(self)

# Helper function to get a new ID
def getNewID():
    Geometry.elementIdCounter += Geometry.elementIdCounterIncr
    return Geometry.elementIdCounter

class Point(Geometry):
    def __init__(self, x, y):
        Geometry.__init__(self)
        self.x = x
        self.y = y
    def replacejwithi(self, i, j):
        pass

class Way(Geometry):
    def __init__(self):
        Geometry.__init__(self)
        self.points = []
    def replacejwithi(self, i, j):
        self.points = [i if x == j else x for x in self.points]
        j.removeparent(self)
        i.addparent(self)

class Relation(Geometry):
    def __init__(self):
        Geometry.__init__(self)
        self.members = []
    def replacejwithi(self, i, j):
        self.members = [(i, x[1]) if x[0] == j else x for x in self.members]
        j.removeparent(self)
        i.addparent(self)

class Feature(object):
    features = []
    def __init__(self):
        self.geometry = None
        self.tags = {}
        Feature.features.append(self)
    def replacejwithi(self, i, j):
        if self.geometry == j:
            self.geometry = i
        j.removeparent(self)
        i.addparent(self)
