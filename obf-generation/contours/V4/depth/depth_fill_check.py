#!/usr/bin/env python3
"""Check the fill of every band polygon at many points of its area.

    depth_fill_check.py AREAS.gpkg RASTER [--layer areas] [--field mindepth] [--lake NAME]
                        [--to-depth LEVEL] [--samples 12] [--margin 1.5]
                        [--min-area-km2 0.01] [--min-bad-fraction 0.02]
                        [--bbox W S E N] [--max-report 20] [--quiet]

AREAS is the band polygons of depth_bands_from_contours.py (one class per polygon: 0 = 0-2 m,
2 = 2-5 m, 5 = 5-10 m). RASTER is the depth raster the bands were traced from: negative in the
water, land is NoData or positive. With --to-depth the raster holds the bed height above sea
level and the depth is value - LEVEL.

A polygon is sampled on a grid inside its minimum rotated rectangle, so a long thin band is hit
all along its length, not only near its centre. A sample is skipped near a class boundary
(2/5/10 m within --margin) and on land. The class of every other sample must be the class of the
polygon, otherwise the polygon paints water of another depth: a shallow rectangle on deep water
or a hole in the shallow band.

The check is meant for the build: a significant defect exits with 1.
"""
import argparse
import math
import sys

import numpy as np
import rasterio
from osgeo import ogr
from shapely import wkb
from shapely.geometry import Point

ogr.UseExceptions()

LIMITS = (0, 2, 5)
MAX_DEPTH = {0: 2.0, 2: 5.0, 5: 10.0}


def classify(depth):
    if depth > -2.0:
        return 0
    if depth > -5.0:
        return 2
    if depth > -10.0:
        return 5
    return None


def grid_points(poly, n):
    """Points spread over the polygon: a grid in its minimum rotated rectangle plus its centre."""
    rect = poly.minimum_rotated_rectangle
    if rect.is_empty or rect.geom_type != 'Polygon':
        return [poly.representative_point()]
    c = list(rect.exterior.coords)
    if len(c) < 4:
        return [poly.representative_point()]
    x0, y0 = c[0]
    ux, uy = c[1][0] - x0, c[1][1] - y0
    vx, vy = c[3][0] - x0, c[3][1] - y0
    if (ux == 0 and uy == 0) or (vx == 0 and vy == 0):
        return [poly.representative_point()]
    pts = []
    for i in range(1, n + 1):
        a = float(i) / (n + 1)
        for j in range(1, n + 1):
            b = float(j) / (n + 1)
            p = Point(x0 + ux * a + vx * b, y0 + uy * a + vy * b)
            if poly.contains(p):
                pts.append(p)
    rp = poly.representative_point()
    if poly.contains(rp):
        pts.append(rp)
    if not pts:
        pts.append(rp)
    return pts


def read_areas(path, layer_name, field, lake):
    ds = ogr.Open(path)
    layer = ds.GetLayerByName(layer_name) if layer_name else ds.GetLayer(0)
    if layer is None:
        raise SystemExit('%s: no layer %s' % (path, layer_name))
    has_lake = layer.GetLayerDefn().GetFieldIndex('lake') >= 0
    out = []
    for f in layer:
        if lake is not None and has_lake and f.GetField('lake') != lake:
            continue
        g = f.GetGeometryRef()
        if g is None:
            continue
        poly = wkb.loads(bytes(g.ExportToWkb()))
        if poly.is_empty or poly.area <= 0:
            continue
        out.append((int(round(float(f.GetField(field)))), poly))
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('areas')
    ap.add_argument('raster')
    ap.add_argument('--layer', default='areas')
    ap.add_argument('--field', default='mindepth')
    ap.add_argument('--lake')
    ap.add_argument('--to-depth', type=float, default=0.0,
                    help='the raster holds the bed height above this level instead of the depth')
    ap.add_argument('--samples', type=int, default=12,
                    help='grid divisions of the minimum rotated rectangle, default 12')
    ap.add_argument('--margin', type=float, default=1.5,
                    help='ignore samples within this distance of a class boundary, metres')
    ap.add_argument('--min-area-km2', type=float, default=0.01,
                    help='report polygons whose bad area is at least this, default 0.01 km2')
    ap.add_argument('--min-bad-fraction', type=float, default=0.02,
                    help='report polygons whose bad samples are at least this fraction, default 0.02')
    ap.add_argument('--bbox', type=float, nargs=4, metavar=('W', 'S', 'E', 'N'))
    ap.add_argument('--max-report', type=int, default=20)
    ap.add_argument('--coverage', type=int, default=0, metavar='N',
                    help='sample a grid of N points of the raster water, report those no band covers')
    ap.add_argument('--require', action='append', nargs=3, default=[], metavar=('LON', 'LAT', 'CLASS'),
                    help='a control point that must be covered by the band CLASS; repeatable')
    ap.add_argument('--quiet', action='store_true')
    args = ap.parse_args()

    boxes = None
    if args.bbox:
        w, s, e, n = args.bbox
        boxes = (w, s, e, n)
    areas = read_areas(args.areas, args.layer, args.field, args.lake)
    if boxes:
        w, s, e, n = boxes
        areas = [(at, g) for at, g in areas
                 if g.bounds[0] <= e and g.bounds[2] >= w and g.bounds[1] <= n and g.bounds[3] >= s]
    if not areas:
        print('no band polygons to check')
        return 0

    ds = rasterio.open(args.raster)
    # a spatial index over the band polygons for the coverage check: a water sample must be
    # covered by the band of its class, otherwise the fill has a hole (a shoal or a shore pocket
    # lost where a contour ring was cut into pieces and the face was dropped)
    from shapely import STRtree
    polys = [g for _, g in areas]
    classes = [at for at, _ in areas]
    tree = STRtree(polys)
    checked = 0
    flagged = []
    total_bad_area = 0.0
    for at, poly in areas:
        if at not in MAX_DEPTH:
            continue
        pts = grid_points(poly, args.samples)
        if not pts:
            continue
        vals = list(ds.sample([(p.x, p.y) for p in pts]))
        good = bad = 0
        bad_depth = []
        for p, v in zip(pts, vals):
            z = float(v[0])
            if not np.isfinite(z) or z <= -9000.0:
                continue
            depth = z - args.to_depth
            if depth >= 0.0:
                continue
            if any(abs(depth + b) <= args.margin for b in MAX_DEPTH.values()):
                continue
            good += 1
            if classify(depth) != at:
                bad += 1
                bad_depth.append(depth)
        if good == 0:
            continue
        checked += 1
        frac = float(bad) / float(good + bad)
        area = poly.area * 8.4e9 / 1e6  # rough km2 at this latitude range
        bad_area = area * frac
        if bad >= 2 and frac >= args.min_bad_fraction and bad_area >= args.min_area_km2:
            total_bad_area += bad_area
            b = poly.bounds
            flagged.append((bad_area, frac, at, area, b, bad, good,
                            float(np.median(bad_depth)) if bad_depth else 0.0))

    flagged.sort(reverse=True)
    print('checked %d band polygons, flagged %d, bad area about %.2f km2'
          % (checked, len(flagged), total_bad_area))
    for bad_area, frac, at, area, b, bad, good, med in flagged[:args.max_report]:
        print('  type %d area %.2f km2 bad %.1f%% (%.2f km2) median bad depth %.1f m '
              'bounds %.5f..%.5f %.5f..%.5f samples %d/%d'
              % (at, area, 100.0 * frac, bad_area, med, b[0], b[2], b[1], b[3], bad, good + bad))

    holes = []
    if args.coverage > 0:
        holes += coverage_check(ds, polys, classes, tree, args)
    for lon, lat, cls in args.require:
        holes += require_point(ds, polys, classes, tree, args, float(lon), float(lat), int(cls))
    if holes:
        print('coverage holes: %d' % len(holes))
    return 1 if flagged or holes else 0


def _covering_classes(polys, classes, tree, lon, lat):
    p = Point(lon, lat)
    return sorted(set(classes[i] for i in tree.query(p) if polys[i].contains(p)))


def expected_classes(depth, margin):
    """The classes a sample may legitimately fall into, given the boundary tolerance.

    Near an isobath the raster and the polygon edge disagree by a few metres, so either the
    shallow or the deep side is acceptable; away from a boundary only the class of the depth.
    Deeper than the last isobath there is no band at all, so a sample just past it may stay
    uncovered: it returns None.
    """
    allowed = set()
    exp = classify(depth)
    if exp is not None:
        allowed.add(exp)
    for b, shallow, deep in ((2.0, 0, 2), (5.0, 2, 5), (10.0, 5, None)):
        if abs(depth + b) <= margin:
            allowed.add(shallow)
            if deep is not None:
                allowed.add(deep)
            else:
                return None  # deeper than the last band: no coverage is correct too
    return allowed


def coverage_check(ds, polys, classes, tree, args):
    """Sample a grid of the raster water and report those points no band polygon covers.

    A hole in the fill (a shoal or a shore pocket lost with a dropped face) is invisible to the
    per-polygon check above: every written polygon is consistent, the missing one is simply not
    there. Sampling the water finds the gap.
    """
    w = ds.bounds.left
    s = ds.bounds.bottom
    e = ds.bounds.right
    n = ds.bounds.top
    side = int(math.ceil(math.sqrt(args.coverage)))
    lons = np.linspace(w, e, side * 4)
    lats = np.linspace(s, n, side * 4)
    samples = []
    for lat in lats:
        for lon in lons:
            z = next(ds.sample([(float(lon), float(lat))]))[0]
            if not np.isfinite(z) or z <= -9000.0:
                continue
            depth = float(z) - args.to_depth
            if depth >= 0.0:
                continue
            exp = expected_classes(depth, args.margin)
            if not exp:
                continue
            samples.append((lon, lat, depth, exp))
    holes = []
    for lon, lat, depth, exp in samples:
        covering = _covering_classes(polys, classes, tree, lon, lat)
        if not (exp & set(covering)):
            holes.append((lon, lat, depth, sorted(exp), covering))
    print('coverage: sampled %d water points, uncovered %d' % (len(samples), len(holes)))
    for lon, lat, depth, exp, covering in holes[:args.max_report]:
        print('  no %s m band at %.5f/%.5f depth %.1f covering %s'
              % ('/'.join(str(x) for x in exp), lon, lat, depth, covering))
    return holes


def require_point(ds, polys, classes, tree, args, lon, lat, cls):
    """A control point that must be covered by the band of the class its depth gives."""
    z = next(ds.sample([(lon, lat)]))[0]
    if not np.isfinite(z) or z <= -9000.0:
        return []  # outside this lake's water: another lake's control point, not this check's scope
    depth = float(z) - args.to_depth
    covering = _covering_classes(polys, classes, tree, lon, lat)
    if cls in covering:
        return []
    print('  required point %.5f/%.5f: depth %.1f m expects band %d, covering %s'
          % (lon, lat, depth, cls, covering))
    return [(lon, lat, depth, cls, covering)]


if __name__ == '__main__':
    sys.exit(main())
