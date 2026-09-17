#!/usr/bin/env python3
"""Depth areas (fill) as OSM multipolygons from charted depth area polygons, cut out of OSM land.

    depth_areas_osm.py SOURCE OUTPUT.osm.gz --layer LAYER --field MIN_DEPTH_FIELD --land LAND
                       [--nested] [--bbox W S E N] [--cell 0.25] [--simplify 0.00002] [--first-id N]

Every polygon is classed by its shallowest depth like the style colours it: areatype=-1 dries (below 0 m), 0 for
0-2 m, 2 for 2-5 m, 5 for 5-10 m; deeper ones are left out, the sea colour shows there. Polygons of one class are
merged per CELL degree square (fewer vertices than the charted bands, which the classes join), OSM land is cut out
(a dry band must not lie on the beach) and the result is written as closed ways or multipolygon relations tagged
contourarea=depth, areatype=...

--nested: every polygon is "shallower than" its class limit, as polygons of a grid above a level are, so a class
contains the shallower ones. Classes are cut one by one from the shallowest, each minus all shallower classes.
"""
import argparse
import gzip
import math

from osgeo import gdal, ogr

ogr.UseExceptions()
# IsValid() reports every self-intersection of the gdal_contour bands as a warning; MakeValid() fixes them
gdal.PushErrorHandler('CPLQuietErrorHandler')


def areatype(depth):
    if depth < 0:
        return '-1'
    if depth < 2:
        return '0'
    if depth < 5:
        return '2'
    if depth < 10:
        return '5'
    return None


def box(w, s, e, n):
    return ogr.CreateGeometryFromWkt('POLYGON((%r %r,%r %r,%r %r,%r %r,%r %r))' % (w, s, e, s, e, n, w, n, w, s))


def polygons(g):
    """The polygon parts as clones: a part got by GetGeometryRef dies with its parent, and the parent is often a
    temporary (MakeValid, Intersection) - using the part after that crashes GDAL."""
    if g is None or g.IsEmpty():
        return []
    t = ogr.GT_Flatten(g.GetGeometryType())
    if t == ogr.wkbPolygon:
        return [g.Clone()]
    if t in (ogr.wkbMultiPolygon, ogr.wkbGeometryCollection):
        out = []
        for i in range(g.GetGeometryCount()):
            out += polygons(g.GetGeometryRef(i))
        return out
    return []


def multipolygon(parts):
    mp = ogr.Geometry(ogr.wkbMultiPolygon)
    for p in parts:
        mp.AddGeometry(p)
    return mp


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('source')
    parser.add_argument('output')
    parser.add_argument('--layer', required=True)
    parser.add_argument('--field', required=True, help='shallowest depth of the area, metres, negative dries')
    parser.add_argument('--land', required=True)
    parser.add_argument('--bbox', nargs=4, type=float, metavar=('W', 'S', 'E', 'N'))
    parser.add_argument('--cell', type=float, default=0.25)
    parser.add_argument('--simplify', type=float, default=0.00002, help='degrees, about 2 m')
    parser.add_argument('--first-id', type=int, default=700000000)
    parser.add_argument('--nested', action='store_true', help='a class holds the shallower ones: cut them out')
    args = parser.parse_args()

    src = ogr.Open(args.source)
    layer = src.GetLayer(args.layer)
    land_src = ogr.Open(args.land)
    land = land_src.GetLayer(0)
    w, s, e, n = args.bbox if args.bbox else layer.GetExtent()[0:1] + layer.GetExtent()[2:3] + \
        layer.GetExtent()[1:2] + layer.GetExtent()[3:4]
    c = args.cell
    nid = args.first_id
    written = 0
    with gzip.open(args.output, 'wt', compresslevel=5) as out:
        out.write("<?xml version='1.0' encoding='UTF-8'?>\n<osm version='0.6' upload='false' generator='depth_areas_osm'>\n")

        def ring(r):
            nonlocal nid
            refs = []
            for i in range(r.GetPointCount() - 1):
                nid += 1
                out.write('<node id="-%d" visible="true" lat="%.7f" lon="%.7f"/>\n' % (nid, r.GetY(i), r.GetX(i)))
                refs.append(nid)
            nid += 1
            out.write('<way id="-%d" visible="true">%s' % (nid, ''.join('<nd ref="-%d"/>' % x for x in refs + refs[:1])))
            return nid

        for i in range(math.floor(w / c), math.ceil(e / c)):
            for j in range(math.floor(s / c), math.ceil(n / c)):
                cell = box(max(w, i * c), max(s, j * c), min(e, (i + 1) * c), min(n, (j + 1) * c))
                x0, x1, y0, y1 = cell.GetEnvelope()
                layer.SetSpatialFilterRect(x0, y0, x1, y1)
                classes = {}
                for f in layer:
                    depth, g = f.GetField(args.field), f.GetGeometryRef()
                    # a layer made from an empty query may have its depth as text
                    at = areatype(float(depth)) if depth not in (None, '') else None
                    if at is None or g is None:
                        continue
                    classes.setdefault(at, []).append(g.Clone())
                if not classes:
                    continue
                land.SetSpatialFilterRect(x0, y0, x1, y1)
                # clipped to the cell first: a complete land polygon is a continent, its union alone takes seconds
                dry = multipolygon(p for f in land for p in polygons(f.GetGeometryRef().Intersection(cell)))
                dry = dry.UnionCascaded() if dry.GetGeometryCount() else None
                shallower = None
                for at, geoms in sorted(classes.items(), key=lambda c: int(c[0])):
                    # every part is made valid on its own: MakeValid() of the whole multipolygon treats the parts
                    # that touch or overlap (the bands of a chart do) as an error and cuts holes where they meet
                    mp = multipolygon(p for g in geoms for p in polygons(g if g.IsValid() else g.MakeValid()))
                    area = mp.UnionCascaded()
                    if area is None or area.IsEmpty() or not area.IsValid():
                        area = mp.Buffer(0)  # the same union, the slow way, for a GEOS that gave up
                    area = area.Intersection(cell)
                    if args.nested:
                        own = area
                        if shallower is not None:
                            area = area.Difference(shallower)
                        shallower = own if shallower is None else shallower.Union(own)
                    if dry is not None and area.Intersects(dry):
                        area = area.Difference(dry)
                    area = area.SimplifyPreserveTopology(args.simplify)
                    for p in polygons(area):
                        outer = p.GetGeometryRef(0)
                        if outer.GetPointCount() < 4:
                            continue
                        tags = '<tag k="contourarea" v="depth"/><tag k="areatype" v="%s"/>' % at
                        inners = [p.GetGeometryRef(k) for k in range(1, p.GetGeometryCount())
                                  if p.GetGeometryRef(k).GetPointCount() >= 4]
                        ow = ring(outer)
                        if not inners:
                            out.write(tags + '</way>\n')
                        else:
                            out.write('</way>\n')
                            iw = []
                            for r in inners:
                                iw.append(ring(r))
                                out.write('</way>\n')
                            nid += 1
                            out.write('<relation id="-%d" visible="true"><member type="way" ref="-%d" role="outer"/>%s'
                                      '<tag k="type" v="multipolygon"/>%s</relation>\n' % (
                                          nid, ow, ''.join('<member type="way" ref="-%d" role="inner"/>' % x for x in iw), tags))
                        written += 1
        out.write('</osm>\n')
    print('depth areas written: %d' % written)


if __name__ == '__main__':
    main()
