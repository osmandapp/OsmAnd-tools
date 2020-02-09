#! /usr/bin/env python3

# (c) 2020 by ppenguin // hieronymusv@gmail.com
# License: CC BY
# Script to download current map files for OsmAnd based on a "wanted"-list,
# and unzip/rename them in the CWD,for use with "air-gapped" devices like in-car head units
#
# This is a quick and dirty hack, probably huge potential for optimisations...
# Only downloading by country name (in the wanted list) has been completely implemented,
# but extension for smaller entities is trivial

# base, list, file URL parts
BURL = 'https://download.osmand.net'
LURL = '/list.php'
FURL = '/download?standard=yes&file='
EXT = '2.obf.zip'
EXTT = '.obf'
TMP = '/tmp'
WANTFILE = 'wanted.txt'

from html.parser import HTMLParser
import urllib.request, urllib.parse
import subprocess as sp
import os.path
from anytree import AnyNode, RenderTree
import anytree

class OSMap:

    def __init__(self, fname):
        self.dfname = fname                                 # download file name of map
        self.dffullpath = os.path.join(TMP, self.dfname)    # full path of downloaded map file
        self.tfname = fname.replace('_' + EXT, EXTT)        # target file name of (unzipped) map
        self.cname = ''  # country name
        self.sname = ''  # state or province name
        self.aname = ''  # area name (e.g. Europe or Central-America)
        self.saname = '' # sub area name or region/county/...
        sn = fname.split('_')
        self.ext = sn.pop(sn.index(EXT))     # now sn contains only name state area (state optional)
        self.downloaded = False
        self.noderef = None
        
        if len(sn) == 1:        # this is a single area, i.e. continent
            self.aname = sn[0] 
        elif len(sn) == 2:      # country and area given
            # an exception is for World_xxx where area should be World and another designation, we use cname, is e.g. basemap or seamarks
            if sn[0] == 'World':
                 self.aname = sn[0]
                 self.cname = sn[1]
            else:    
                self.cname = sn[0]
                self.aname = sn[1]
        elif len(sn) == 3:      # country, state/province, area
            self.cname = sn[0]
            self.sname = sn[1]
            self.aname = sn[2]
        elif len(sn) == 4:      # country, state/province, sub-area(region), area
            self.cname = sn[0]
            self.sname = sn[1]
            self.aname = sn[3]
            self.saname = sn[2]
        else:
            print("Problem? File name \"%s\" doesn\'t follow standard?"%fname) 

        # print("OSMap dfname=%(dfname)s cname=%(cname)s sname=%(sname)s aname=%(aname)s saname=%(saname)s"%vars(self))      

    def isCountry(self):
        return (self.cname != '' and not self.isState())

    def isState(self):
        return (self.sname != '' and not self.isArea())   

    def isArea(self):
        return (self.aname != '' and not self.isSubArea() and not self.isCountry())

    def isSubArea(self):
        return (self.saname != '')

    def download(self):
        if not self.downloaded:
            # here we download and unpack the file
            print("Downloading %s from %s to %s"%(self.dfname, urllib.parse.urljoin(BURL, FURL + self.dfname), self.dffullpath))
            sp.check_call(("wget --quiet %s -O %s"%(urllib.parse.urljoin(BURL, FURL + self.dfname), self.dffullpath)).split())
            sp.check_call(("unzip %s"%(self.dffullpath)).split())
            sp.check_call(("mv %s %s"%(self.dfname.replace('.zip', ''), self.tfname)).split())
            sp.check_call(("rm -f %s"%(self.dffullpath)).split())
            self.downloaded = True


class OSMapsList(HTMLParser):
    
    def __init__(self):
        super().__init__()
        self.afiles = []    # list of available files
        self.amaps = []     # list of available maps, now just a placeholder for the AnyNode objects
        self.wanted = []    # list of country or state names
        self.world = AnyNode(id="world")   # root node of tree of geographical entities (tree with first level world)

    def hasNode(self, nname):
        c = anytree.search.findall(self.world, filter_=lambda node: node.id == nname)
        # print(len(c), nname, c)
        return (len(c) > 0)
    
    def getNodeById(self, idname):
        r = anytree.search.findall(self.world, filter_=lambda node: node.id == idname)
        if len(r) > 0:
            return r[0]
        else:
            return r

    def isCountry(self, cname):
        c = anytree.search.findall(self.world, filter_=lambda node: node.id == cname and node.etype == 'country')
        # print(len(c), nname, c)
        return (len(c) > 0)  

    def getCountry(self, cname):
        r = anytree.search.findall(self.world, filter_=lambda node: node.id == cname and node.etype == 'country')
        if len(r) > 0:
            return r[0]
        else:
            return r             

    def getStates(self, cname):
        cn = self.getNodeById(cname)
        r = anytree.search.findall(cn, filter_=lambda node: node.etype == 'state' and node.osmap is not None)
        return r 

    def hasMap(self, fname):
        for m in self.amaps:
            if m.dfname == fname:
                return True
        return False

    def numStates(self, cname):
        i = 0
        for m in self.amaps:
            if m.cname == cname and m.isState():
                i += 1
        return i

    def addMap(self, fname):
        if not self.hasMap(fname):
            m = OSMap(fname)
            # now check the level and add to the tree
            # check from large to small, i.e. area, country, state/province, sub-area(region/county)
            if m.aname != '' and not self.hasNode(m.aname):
                m.noderef = AnyNode(id=m.aname, parent=self.world, etype='area', osmap=m)
            if m.cname != '':
                an = self.getNodeById(m.aname)    # this one exists because it was just created if not existed before
                if m.sname == '':  # in this case this is a map file of a whole country
                    if self.hasNode(m.cname): # we already have a node, which probably has no map, so overwrite its osmap property
                        cn = self.getNodeById(m.cname)  
                        cn.osmap = m
                        m.noderef = cn
                    else:
                        m.noderef = AnyNode(id=m.cname, parent=an, etype='country', osmap=m) 
                else:   # the current map is a state and the country does not exist yet
                    if not self.hasNode(m.cname):
                        m.noderef = AnyNode(id=m.cname, parent=an, etype='country', osmap=None)                 
            if m.sname != '' and not self.hasNode(m.sname):
                n = self.getNodeById(m.cname)    # this one exists because it was just created if not existed before
                if m.saname == '':  # in this case this is a map file of a whole state
                    m.noderef = AnyNode(id=m.sname, parent=n, etype='state', osmap=m)
                else:
                    m.noderef = AnyNode(id=m.sname, parent=n, etype='state', osmap=None)                           
            if m.saname != '' and not self.hasNode(m.saname):
                n = self.getNodeById(m.sname)    # this one exists because it was just created if not existed before
                m.noderef = AnyNode(id=m.saname, parent=n, etype='region', osmap=m)

            self.amaps.append(m)
        else:
            print("Skipping %(fname)s because already in list"%vars())


    def read_wanted(self, fname):
        with open(fname) as f:
            self.wanted = [line.strip() for line in f]

    def download(self):
        # print(RenderTree(self.world))
        # we have now a tree and a list of wanted entities.
        # Let's say wen want either countries or provinces, but no smaller entities.
        # If a country has one file covering all, we take it, otherwise by separate state/province
        for w in self.wanted:
            c = self.getCountry(w)
            if type(c) == type(AnyNode()):  # country node is valid data type
                if c.osmap is not None:     # node has a map object
                    print("Downloading country as single file: %s"%c.id)
                    c.osmap.download()
                else: # there is no single map of the country, so we download the states
                    ss = self.getStates(w)
                    for s in ss:
                        print("Downloading state %s of country %s, because there is no single country file."%(s.id, c.id))
                        s.osmap.download()
            else:
                print("Problem with country node (%s)? Ignoring..."%w)
                

    def handle_starttag(self, tag, attrs):
        #print("Encountered a start tag:", tag)
        # if the tag is a <a> with href starts with "/download?standard=yes&file='
        # we want to parse it to the available map files
        if tag == 'a':
            for attr in attrs:
                (k, v) = attr
                if k == 'href':
                    if FURL in v and EXT in v:  # we check for ext because the list also contains non-map files
                        self.addMap(v.split('=')[-1])
                        # print("Adding %s"%v)
                    ## do we want to handle other files such as voice data?


if __name__ == '__main__':
    
    maplist = urllib.request.urlopen(BURL + LURL).read()
    ml = OSMapsList()
    ml.feed(maplist.decode())
    ml.read_wanted(WANTFILE)
    ml.download()



