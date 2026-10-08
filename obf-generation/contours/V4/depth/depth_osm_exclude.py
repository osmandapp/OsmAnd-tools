#!/usr/bin/env python3
"""Leave the coverage of detailed depth maps out of a wider depth map's OSM (contours, soundings).

    depth_osm_exclude.py INPUT.osm.gz OUTPUT.osm.gz --exclude COVERAGE.gpkg [--exclude ...] [--cell 0.1]

COVERAGE files come from depth_coverage.py (any polygon layer works). Tagged nodes (soundings) inside a coverage are
dropped; ways (contours) are cut at the coverage edge and only their parts outside are kept, as new ways with the
tags of the original. Everything else is copied. The coverage is split into CELL degree squares, so a way is tested
only against the polygons near it. Relations are not supported: the input must have none.
"""
import argparse
import gzip
import math
import xml.etree.ElementTree as ET
from xml.sax.saxutils import quoteattr

from osgeo import ogr

ogr.UseExceptions()


def box(w, s, e, n):
    return ogr.CreateGeometryFromWkt('POLYGON((%r %r,%r %r,%r %r,%r %r,%r %r))' % (w, s, e, s, e, n, w, n, w, s))


class Coverage:
    def __init__(self, paths, cell):
        self.cell = cell
        multi = ogr.Geometry(ogr.wkbMultiPolygon)
        for path in paths:
            ds = ogr.Open(path)
            for layer in ds:
                for f in layer:
                    g = f.GetGeometryRef()
                    if g is None:
                        continue
                    g = g.GetLinearGeometry()
                    for i in range(g.GetGeometryCount()) if ogr.GT_Flatten(g.GetGeometryType()) != ogr.wkbPolygon \
                            else [None]:
                        p = g if i is None else g.GetGeometryRef(i)
                        if ogr.GT_Flatten(p.GetGeometryType()) == ogr.wkbPolygon:
                            multi.AddGeometry(p)
        self.area = multi.UnionCascaded() if multi.GetGeometryCount() else None
        self.cache = {}

    def near(self, w, s, e, n):
        """The coverage around a box (the union of its cells), None where there is none"""
        if self.area is None:
            return None
        c = self.cell
        key = (math.floor(w / c), math.floor(s / c), math.floor(e / c), math.floor(n / c))
        if key not in self.cache:
            if len(self.cache) > 10000:
                self.cache.clear()
            clip = box(key[0] * c, key[1] * c, (key[2] + 1) * c, (key[3] + 1) * c)
            part = self.area.Intersection(clip) if self.area.Intersects(clip) else None
            self.cache[key] = part if part is not None and not part.IsEmpty() else None
        return self.cache[key]


def lines(g):
    t = ogr.GT_Flatten(g.GetGeometryType())
    if t == ogr.wkbLineString:
        return [g]
    if t in (ogr.wkbMultiLineString, ogr.wkbGeometryCollection):
        out = []
        for i in range(g.GetGeometryCount()):
            out += lines(g.GetGeometryRef(i))
        return out
    return []


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('input')
    parser.add_argument('output')
    parser.add_argument('--exclude', action='append', required=True, help='coverage polygons, repeatable')
    parser.add_argument('--cell', type=float, default=0.1, help='degrees')
    args = parser.parse_args()
    coverage = Coverage(args.exclude, args.cell)

    nodes = {}
    min_id = 0
    kept = cut = dropped = points_kept = points_dropped = 0
    with gzip.open(args.input, 'rb') as src, gzip.open(args.output, 'wt', compresslevel=5) as out:
        out.write("<?xml version='1.0' encoding='UTF-8'?>\n<osm version='0.6' upload='false' generator='depth_osm_exclude'>\n")
        new_id = None
        for event, el in ET.iterparse(src, events=('end',)):
            if el.tag == 'node':
                nid, lat, lon = int(el.get('id')), float(el.get('lat')), float(el.get('lon'))
                min_id = min(min_id, nid)
                tags = el.findall('tag')
                if tags:
                    # a sounding: kept unless inside the coverage
                    near = coverage.near(lon, lat, lon, lat)
                    p = ogr.Geometry(ogr.wkbPoint)
                    p.AddPoint_2D(lon, lat)
                    if near is not None and near.Contains(p):
                        points_dropped += 1
                    else:
                        out.write(ET.tostring(el, encoding='unicode').strip() + '\n')
                        points_kept += 1
                else:
                    nodes[nid] = (lon, lat)
                el.clear()
            elif el.tag == 'way':
                if new_id is None:
                    new_id = min_id - 1
                refs = [int(nd.get('ref')) for nd in el.findall('nd')]
                tags = ''.join('<tag k=%s v=%s/>' % (quoteattr(t.get('k')), quoteattr(t.get('v'))) for t in el.findall('tag'))
                pts = [nodes[r] for r in refs if r in nodes]
                if len(pts) < 2:
                    el.clear()
                    continue
                xs, ys = [p[0] for p in pts], [p[1] for p in pts]
                near = coverage.near(min(xs), min(ys), max(xs), max(ys))
                parts = [pts]
                if near is not None:
                    line = ogr.Geometry(ogr.wkbLineString)
                    for x, y in pts:
                        line.AddPoint_2D(x, y)
                    if line.Intersects(near):
                        rest = line.Difference(near)
                        parts = [[l.GetPoint_2D(i) for i in range(l.GetPointCount())] for l in lines(rest)]
                        cut += 1
                        if not parts:
                            dropped += 1
                for part in parts:
                    if len(part) < 2:
                        continue
                    ids = []
                    for x, y in part:
                        out.write('<node id="%d" visible="true" lat="%.7f" lon="%.7f"/>\n' % (new_id, y, x))
                        ids.append(new_id)
                        new_id -= 1
                    out.write('<way id="%d" visible="true">%s%s</way>\n' % (
                        new_id, ''.join('<nd ref="%d"/>' % i for i in ids), tags))
                    new_id -= 1
                    kept += 1
                el.clear()
            elif el.tag == 'relation':
                raise SystemExit('relations are not supported: %s' % args.input)
        out.write('</osm>\n')
    print('exclude %s: %d ways written, %d ways cut (%d left out whole), %d soundings kept, %d left out' % (
        args.input, kept, cut, dropped, points_kept, points_dropped))


if __name__ == '__main__':
    main()
