#!/usr/bin/env python3
"""Build the depth bands from the contour lines of the map, not from the raster cells.

    depth_bands_from_contours.py CONTOURS.gpkg WATER.gpkg --lake KEY --raster DEPTH.vrt OUTPUT.gpkg
                                 [--cell 0.05] [--levels 2 5 10] [--append]

`gdal_contour -p` traces a band along the raster cell edges, so its boundary is a staircase while
the isobath is an interpolated line: at a high zoom the line visibly crosses the band. Here the
band boundaries are the contour lines themselves: the water outline and the 2/5/10 m lines are
polygonized into faces, each face gets its band from the depth raster, and the faces of one band
are merged. The lines are the same features that go to the map, so a band edge and its isobath
share every vertex.

The faces are sampled from the filled raster (the same one the lines came from). A face with no
finite sample is skipped. Cells keep the polygonization small; the water polygon is clipped to a
cell first, so a lake with tens of thousands of islands does not go through GEOS in one piece.
"""
import argparse
import math
import os
import sys

import numpy as np
import rasterio
from osgeo import ogr, osr
from rasterio.features import shapes as raster_shapes
from rasterio.windows import Window, from_bounds
from shapely import wkb
from shapely.geometry import LineString, MultiLineString, Point, Polygon, shape as geojson_shape
from shapely.ops import nearest_points, polygonize, unary_union
from shapely.validation import make_valid

ogr.UseExceptions()


def polygons(g):
    if g is None or g.is_empty:
        return []
    if g.geom_type == 'Polygon':
        return [g]
    if g.geom_type == 'MultiPolygon':
        return list(g.geoms)
    if g.geom_type == 'GeometryCollection':
        return [p for q in g.geoms for p in polygons(q)]
    return []


def read_water(layer, key, cell):
    layer.SetAttributeFilter("lake = '%s'" % key.replace("'", "''"))
    layer.SetSpatialFilterRect(*(float(v) for v in cell.bounds))
    parts = []
    for f in layer:
        g = wkb.loads(bytes(f.GetGeometryRef().ExportToWkb())).intersection(cell)
        parts += polygons(g)
    layer.SetAttributeFilter(None)
    layer.SetSpatialFilter(None)
    return unary_union(parts) if parts else None


def read_lines(layer, cell, levels):
    # keep the datasource that owns the layer referenced by the caller: a layer of a collected
    # datasource is a dangling SWIG proxy and every method raises a type error
    layer.SetSpatialFilterRect(*(float(v) for v in cell.bounds))
    lines = []
    for f in layer:
        d = f.GetField('depth')
        if d in levels:
            g = wkb.loads(bytes(f.GetGeometryRef().ExportToWkb()))
            if not g.is_empty:
                lines.append((d, g))
    layer.SetSpatialFilter(None)
    return lines


def line_parts(g):
    if g is None or g.is_empty:
        return []
    if g.geom_type == 'LineString':
        return [g]
    if g.geom_type in ('MultiLineString', 'GeometryCollection'):
        return [p for q in g.geoms for p in line_parts(q)]
    return []


def extend_line_ends(lines, eps=1e-7):
    """Stretch every open line end a centimetre past its last vertex, along the end tangent.

    intersection() clips a contour at the water outline with floating-point noise at the 1e-16
    deg level, so an end that looks on the outline is not covered by the exact predicates: the
    polygonizer sees a tangent touch it cannot node and demotes the whole shore-to-shore chain to
    a cut edge, the face is not split and a whole band is lost. A line that crosses the outline
    transversally is noded reliably.
    """
    out = []
    for d, g in lines:
        parts = []
        for q in line_parts(g):
            cs = [list(c) for c in q.coords]
            if len(cs) >= 2 and not q.is_closed:
                for a, b, i in ((cs[1], cs[0], 0), (cs[-2], cs[-1], len(cs) - 1)):
                    dx, dy = b[0] - a[0], b[1] - a[1]
                    L = math.hypot(dx, dy)
                    if L > 0:
                        cs[i][0] += dx / L * eps
                        cs[i][1] += dy / L * eps
            parts.append(LineString(cs))
        out.append((d, parts[0] if len(parts) == 1 else MultiLineString(parts)))
    return out


def data_coverage(ds, cell):
    """The water the raster holds a value for, as one geometry per cell.

    A band face can span a survey gap (a bay the swissBATHY3D tiles do not cover, a hole between
    tiles): no contour line crosses the gap, so the face merges shallow and deep water and one
    median classifies it all. Cutting the faces by the coverage first classifies each surveyed
    piece on its own and drops the gaps, so no band colour is painted over water that has no data.
    """
    win = from_bounds(*cell.bounds, transform=ds.transform)
    band = ds.read(1, window=win, boundless=True, fill_value=np.nan)
    m = np.isfinite(band) & (band < 0.0) & (band > -9000.0)
    if not m.any():
        return None
    tr = ds.window_transform(win)
    parts = [geojson_shape(g) for g, v in raster_shapes(m.astype(np.uint8), mask=m, transform=tr)
             if v == 1]
    if not parts:
        return None
    return unary_union(parts).intersection(cell)


def close_to_water(lines, boundary, snap_deg):
    """Connect a contour that stops a cell short of the shore to the water outline.

    gdal_contour ends a line at the last raster cell with data, and the water polygon is a little
    further: the line is then a dangle, polygonize ignores it, and two bands merge across it.
    """
    extra = []
    for d, g in lines:
        for q in line_parts(g):
            if len(q.coords) < 2:
                continue
            for p in (Point(q.coords[0]), Point(q.coords[-1])):
                if boundary.distance(p) <= snap_deg:
                    bp = nearest_points(boundary, p)[0]
                    if p.distance(bp) > 0:
                        extra.append(LineString([p, bp]))
    return extra


def join_ends(lines, join_deg):
    """Join the pieces of one contour that the filter split: a ring becomes several open ways
    a few metres apart, and a gap of that size merges the bands on both sides of the line."""
    ends = []
    for d, g in lines:
        for q in line_parts(g):
            if len(q.coords) >= 2:
                ends.append((d, Point(q.coords[0]), Point(q.coords[-1])))
    extra = []
    for idx, (d, p0, p1) in enumerate(ends):
        for p in (p0, p1):
            best = None
            for idx2, (d2, q0, q1) in enumerate(ends):
                if idx2 == idx or d2 != d:
                    continue
                for q in (q0, q1):
                    dd = p.distance(q)
                    if 0 < dd <= join_deg and (best is None or dd < best[0]):
                        best = (dd, q)
            if best is not None:
                extra.append(LineString([p, best[1]]))
    return extra


def sample_depth(ds, p):
    row, col = ds.index(p.x, p.y)
    if 0 <= row < ds.height and 0 <= col < ds.width:
        v = ds.read(1, window=Window(col, row, 1, 1))[0, 0]
        if not np.isfinite(v) or v <= -9000.0 or v >= 1e30:
            return None  # nodata: NaN, the -9999 grid nodata, or the float-max sentinel
        return float(v)
    return None


def face_samples(ds, face):
    """Points and depths spread over the face.

    The grid is built in the minimum rotated rectangle: a long thin band along the shore fills a
    small part of its bounding box, and a bbox grid would sample the deep water next to it.
    """
    out = []
    rp = face.representative_point()
    v = sample_depth(ds, rp)
    if v is not None:
        out.append((rp, v))
    rect = face.minimum_rotated_rectangle
    if rect.geom_type == 'Polygon' and not rect.is_empty:
        c = list(rect.exterior.coords)
        if len(c) >= 4:
            x0, y0 = c[0]
            ux, uy = c[1][0] - x0, c[1][1] - y0
            vx, vy = c[3][0] - x0, c[3][1] - y0
            for i in range(1, 9):
                a = float(i) / 9.0
                for j in range(1, 9):
                    b = float(j) / 9.0
                    p = Point(x0 + ux * a + vx * b, y0 + uy * a + vy * b)
                    if face.contains(p):
                        v = sample_depth(ds, p)
                        if v is not None:
                            out.append((p, v))
    return out


def face_depth(ds, face):
    vals = [v for _, v in face_samples(ds, face)]
    if not vals:
        return None
    return float(np.median(vals))


def split_face(face, lines, ds, depth=0):
    """Split a face that spans several band classes along the contour lines inside it.

    A contour ring cut into pieces by the node limit can leave dangling ends inside a face: the
    face goes round the gap and covers two bands (a 4 m pocket joined to >10 m water, or a shore
    strip joined to 5-10 m). The median of the whole face then picks the wrong class and one band
    is lost. Every open line end that lies inside the face is connected to the nearest boundary
    point, so the missing cut is closed and each part is classified on its own.
    """
    samples = face_samples(ds, face)
    classes = set()
    for _, d in samples:
        if any(abs(d + b) <= 1.0 for b in (2.0, 5.0, 10.0)):
            continue
        c = classify(d)
        if c is not None:
            classes.add(c)
    at = face_class(face, lines, ds)
    if os.environ.get('BANDS_DEBUG'):
        rp = face.representative_point()
        print('split_face: face at %.4f,%.4f area %.2f km2 classes %s at %s depth %.1f'
              % (rp.x, rp.y, face.area * 8.4e9 / 1e6, sorted(classes), at,
                 face_depth(ds, face) or 0), file=sys.stderr)
    if len(classes) <= 1 or depth > 1:
        return [(face, at)] if at is not None else []
    clipped = []
    for _, g in lines:
        q = g.intersection(face)
        if not q.is_empty:
            clipped.append(q)
    extra = []
    for q in clipped:
        for part in line_parts(q):
            if len(part.coords) < 2:
                continue
            for p in (Point(part.coords[0]), Point(part.coords[-1])):
                if face.boundary.distance(p) <= 1e-9:
                    continue
                bp = nearest_points(face.boundary, p)[0]
                seg = LineString([p, bp])
                if face.covers(seg) and seg.length > 1e-12:
                    extra.append(seg)
    if not extra:
        return [(face, at)] if at is not None else []
    parts = [q for q in polygonize(unary_union([face.boundary] + clipped + extra))
             if face.covers(q.representative_point()) and q.area > 0]
    out = []
    for q in parts:
        for piece, a in split_face(q, lines, ds, depth + 1):
            if a is not None:
                out.append((piece, a))
    if out:
        return out
    return [(face, at)] if at is not None else []


def classify(depth):
    if depth > -2:
        return 0
    if depth > -5:
        return 2
    if depth > -10:
        return 5
    return None


def boundary_levels(face, lines):
    """The contour depths on the face boundary: the band follows from them, not from one sample.

    A large face can include a shallow bank whose 2 m ring was dropped, and a sample at its centre
    then puts the whole 2-5 m face into the 0-2 m band.
    """
    levels = set()
    fb = face.boundary
    for d, g in lines:
        if fb.intersects(g):
            levels.add(d)
    return levels


def face_class(face, lines, ds):
    """The band of a face, checked against the raster.

    A large deep face borders the outer ring of every shoal and island in its cell: a 5 m ring of
    one shoal and a 10 m ring of another touch the same face, and the levels alone then say 5-10 m
    while the face is the deep water between them. A sample of the face tells the two apart: the
    boundaries of a real band face lie within its depth range, a deep face reaches much deeper.
    """
    levels = boundary_levels(face, lines)
    depth = face_depth(ds, face)
    if len(levels) == 1 and depth is not None:
        # a closed ring inside one band: the level alone does not say which side of it the face is
        # on - inside the 2 m ring of a depression the water is deeper than 2 m, inside the 2 m
        # ring of a shoal it is shallower - so the sampled depth decides. The margin below cannot
        # apply either: a 2.2 m pocket is a whole band above its 2 m ring, not a 0-2 m face.
        return classify(depth)
    if 2 in levels and 5 in levels:
        at = 2
    elif 5 in levels and 10 in levels:
        at = 5
    elif 2 in levels:
        at = 0
    elif 5 in levels:
        at = 2
    else:
        at = None
    if depth is None:
        return at
    if at is None:
        return classify(depth)
    # both bounds of the class: the face must stay between them, a margin absorbs the interpolation
    # next to the contour itself. A face that only touches a deeper contour at one corner but is
    # shallow in its area is reclassified by the depth too.
    lower = {0: -2.0, 2: -5.0, 5: -10.0}[at]
    upper = {0: 0.0, 2: -2.0, 5: -5.0}[at]
    if depth < lower - 1.0 or depth > upper + 1.0:
        return classify(depth)
    return at


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('contours')
    parser.add_argument('water')
    parser.add_argument('output')
    parser.add_argument('--lake', required=True, help='value of the lake field in the water layer')
    parser.add_argument('--raster', required=True, help='the filled depth raster the contour lines came from')
    parser.add_argument('--cell', type=float, default=0.05)
    parser.add_argument('--snap', type=float, default=10.0,
                        help='connect a contour line ending this many metres from the water outline, metres')
    parser.add_argument('--split', action='store_true',
                        help='split a face that spans several band classes along the contour lines inside it')
    parser.add_argument('--join', type=float, default=10.0,
                        help='join the pieces of one contour cut by the filter, metres; 0 keeps them apart')
    parser.add_argument('--levels', type=int, nargs='+', default=(2, 5, 10))
    parser.add_argument('--layer', default='water')
    parser.add_argument('--append', action='store_true')
    args = parser.parse_args()

    contours_src = ogr.Open(args.contours)
    if contours_src is None:
        sys.exit('cannot open %s' % args.contours)
    contours = contours_src.GetLayer(0)
    water_src = ogr.Open(args.water)
    if water_src is None:
        sys.exit('cannot open %s' % args.water)
    water = water_src.GetLayerByName(args.layer) or water_src.GetLayer(0)
    levels = set(args.levels)

    ds = rasterio.open(args.raster)
    w, s, e, n = ds.bounds.left, ds.bounds.bottom, ds.bounds.right, ds.bounds.top
    c = args.cell
    cells = [Polygon([(max(w, i * c), max(s, j * c)), (min(e, (i + 1) * c), max(s, j * c)),
                      (min(e, (i + 1) * c), min(n, (j + 1) * c)), (max(w, i * c), min(n, (j + 1) * c))])
             for i in range(math.floor(w / c), math.ceil(e / c))
             for j in range(math.floor(s / c), math.ceil(n / c))]

    driver = ogr.GetDriverByName('GPKG')
    if args.append:
        out = driver.Open(args.output, 1)
        if out is None:
            sys.exit('cannot open %s for append' % args.output)
    else:
        import os
        if os.path.exists(args.output):
            driver.DeleteDataSource(args.output)
        out = driver.CreateDataSource(args.output)
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(4326)
    srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
    out_layer = out.GetLayerByName('areas')
    if out_layer is None:
        out_layer = out.CreateLayer('areas', srs, ogr.wkbUnknown)
        out_layer.CreateField(ogr.FieldDefn('mindepth', ogr.OFTReal))
    if out_layer.GetLayerDefn().GetFieldIndex('lake') < 0:
        out_layer.CreateField(ogr.FieldDefn('lake', ogr.OFTString))

    written = 0
    for cell in cells:
        W = read_water(water, args.lake, cell)
        if W is None or W.is_empty:
            continue
        coverage = data_coverage(ds, cell)
        if coverage is None or coverage.is_empty:
            continue
        lines = []
        for d, g in read_lines(contours, cell, levels):
            q = g.intersection(W)
            if not q.is_empty:
                lines.append((d, q))
        lines = extend_line_ends(lines)
        snap_deg = args.snap / 111320.0
        extra = close_to_water(lines, W.boundary, snap_deg)
        if args.join > 0:
            extra += join_ends(lines, args.join / 111320.0)
        network = unary_union([W.boundary] + [g for d, g in lines] + extra)
        faces = list(polygonize(network))
        bands = {}
        for face in faces:
            if not W.contains(face.representative_point()):
                continue
            for piece in polygons(face.intersection(coverage)):
                if piece.area <= 0 or not W.contains(piece.representative_point()):
                    continue
                if args.split:
                    pieces = split_face(piece, lines, ds)
                else:
                    at = face_class(piece, lines, ds)
                    pieces = [(piece, at)] if at is not None else []
                for p2, at in pieces:
                    if at is None:
                        continue
                    bands.setdefault(at, []).append(p2)
        for at, geoms in bands.items():
            area = unary_union(geoms).intersection(W)
            for p in polygons(area):
                if p.area <= 0:
                    continue
                f = ogr.Feature(out_layer.GetLayerDefn())
                f.SetField('mindepth', float(at))
                if out_layer.GetLayerDefn().GetFieldIndex('lake') >= 0:
                    f.SetField('lake', args.lake)
                f.SetGeometry(ogr.CreateGeometryFromWkb(p.wkb))
                out_layer.CreateFeature(f)
                written += 1
    out = None
    print('%s: %d band polygons' % (args.output, written))


if __name__ == '__main__':
    main()
