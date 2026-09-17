#!/usr/bin/env python3
"""Check a cut made by depth_osm_exclude.py: nothing is left inside the coverage, nothing is lost outside it.

    depth_exclude_check.py BEFORE_DIR AFTER_DIR --exclude COVERAGE.gpkg [--exclude ...] [--tolerance 0.0001]

Every .osm.gz of BEFORE_DIR is compared with the file of the same name in AFTER_DIR:
- contours: length in km inside the coverage (should be 0) and outside it (should be the same before and after);
- soundings: count inside (0 after) and outside (the same).
TOLERANCE (degrees) shrinks the coverage for the "inside" test and grows it for the "outside" one, so points on the
cut itself count for neither. Exits with 1 when a check fails.
"""
import argparse
import glob
import gzip
import math
import os
import xml.etree.ElementTree as ET

from osgeo import ogr

ogr.UseExceptions()


def length_km(g):
    total = 0.0
    t = ogr.GT_Flatten(g.GetGeometryType())
    if t == ogr.wkbLineString:
        for i in range(1, g.GetPointCount()):
            (x1, y1), (x2, y2) = g.GetPoint_2D(i - 1), g.GetPoint_2D(i)
            total += math.hypot((x2 - x1) * math.cos(math.radians((y1 + y2) / 2)), y2 - y1) * 111.32
    else:
        for i in range(g.GetGeometryCount()):
            total += length_km(g.GetGeometryRef(i))
    return total


def measure(path, inner, outer):
    nodes, lin = {}, [0.0, 0.0]
    pts = [0, 0]
    for _, el in ET.iterparse(gzip.open(path, 'rb'), events=('end',)):
        if el.tag == 'node':
            x, y = float(el.get('lon')), float(el.get('lat'))
            if el.find('tag') is not None:
                p = ogr.Geometry(ogr.wkbPoint)
                p.AddPoint_2D(x, y)
                if inner.Contains(p):
                    pts[0] += 1
                elif not outer.Contains(p):
                    pts[1] += 1
            else:
                nodes[el.get('id')] = (x, y)
            el.clear()
        elif el.tag == 'way':
            line = ogr.Geometry(ogr.wkbLineString)
            for nd in el.findall('nd'):
                if nd.get('ref') in nodes:
                    line.AddPoint_2D(*nodes[nd.get('ref')])
            if line.GetPointCount() > 1:
                lin[0] += length_km(line.Intersection(inner))
                lin[1] += length_km(line.Difference(outer))
            el.clear()
    return lin, pts


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('before')
    parser.add_argument('after')
    parser.add_argument('--exclude', action='append', required=True)
    parser.add_argument('--tolerance', type=float, default=0.0001)
    args = parser.parse_args()
    multi = ogr.Geometry(ogr.wkbMultiPolygon)
    for path in args.exclude:
        ds = ogr.Open(path)
        for layer in ds:
            for f in layer:
                g = f.GetGeometryRef()
                for i in range(g.GetGeometryCount()):
                    multi.AddGeometry(g.GetGeometryRef(i))
    area = multi.UnionCascaded()
    inner, outer = area.Buffer(-args.tolerance), area.Buffer(args.tolerance)

    total = {'before': [[0.0, 0.0], [0, 0]], 'after': [[0.0, 0.0], [0, 0]]}
    for before in sorted(glob.glob(os.path.join(args.before, '*.osm.gz'))):
        after = os.path.join(args.after, os.path.basename(before))
        for key, path in (('before', before), ('after', after)):
            lin, pts = measure(path, inner, outer)
            for i in range(2):
                total[key][0][i] += lin[i]
                total[key][1][i] += pts[i]
    (bl, bp), (al, ap) = total['before'], total['after']
    ok = al[0] < 0.01 and ap[0] == 0 and abs(al[1] - bl[1]) <= max(0.001 * bl[1], 0.01) and ap[1] == bp[1]
    print('contours km inside: %.2f -> %.2f; outside: %.2f -> %.2f' % (bl[0], al[0], bl[1], al[1]))
    print('soundings inside: %d -> %d; outside: %d -> %d' % (bp[0], ap[0], bp[1], ap[1]))
    print('OK' if ok else 'FAILED')
    raise SystemExit(0 if ok else 1)


if __name__ == '__main__':
    main()
