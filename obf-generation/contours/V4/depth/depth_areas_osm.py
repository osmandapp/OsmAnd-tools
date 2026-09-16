#!/usr/bin/env python3
"""Depth areas (fill) as OSM multipolygons from charted depth area polygons, cut out of OSM land.

    depth_areas_osm.py SOURCE OUTPUT.osm.gz --layer LAYER --field MIN_DEPTH_FIELD --land LAND
                       [--bbox W S E N] [--cell 0.25] [--simplify 0.00002] [--first-id N]

Every polygon is classed by its shallowest depth like the style colours it: areatype=-1 dries (below 0 m), 0 for
0-2 m, 2 for 2-5 m, 5 for 5-10 m; deeper ones are left out, the sea colour shows there. Polygons of one class are
merged per CELL degree square (fewer vertices than the charted bands, which the classes join), OSM land is cut out
(a dry band must not lie on the beach) and the result is written as closed ways or multipolygon relations tagged
contourarea=depth, areatype=...
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
                    at = areatype(depth) if depth is not None else None
                    if at is None or g is None:
                        continue
                    classes.setdefault(at, []).append(g.Clone())
                if not classes:
                    continue
                land.SetSpatialFilterRect(x0, y0, x1, y1)
                dry = ogr.Geometry(ogr.wkbMultiPolygon)
                for f in land:
                    for p in polygons(f.GetGeometryRef()):
                        dry.AddGeometry(p)
                dry = dry.UnionCascaded().Intersection(cell) if dry.GetGeometryCount() else None
                for at, geoms in classes.items():
                    mp = ogr.Geometry(ogr.wkbMultiPolygon)
                    for g in geoms:
                        for p in polygons(g.MakeValid() if not g.IsValid() else g):
                            mp.AddGeometry(p)
                    # gdal_contour bands can self-intersect; a union of invalid rings may crash an older GEOS
                    area = (mp if mp.IsValid() else mp.MakeValid()).UnionCascaded().Intersection(cell)
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
