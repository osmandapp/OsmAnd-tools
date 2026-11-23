#!/usr/bin/env python3
"""Chart vector data of several scale bands merged into one set: the largest scale band wins wherever it has depth areas.

    depth_bands_merge.py SOURCE OUTPUT.gpkg --bbox W S E N [--bands 5] [--cell 1]

SOURCE has, for every band b from 1 (largest scale, harbour charts) to BANDS (smallest), the layers contour_b (field
valdco), sounding_b (depth) and area_b (drval1): LINZ hydrographic data as download_all.sh writes it. The depth areas
of a band are its coverage. Per CELL degree square, band by band from the largest scale, a band keeps what lies
outside the coverage of the larger scale bands; its own coverage is then added. Curves are linearized.

OUTPUT gets the layers and fields of Kartverket's "Sjøkart - Dybdedata", so build_depth_charts.sh builds it:
dybdekurve (dybde), dybdepunkt (dybde), dybdeareal (minimumsdybde), geometry column SHAPE, EPSG:4326.
"""
import argparse
import math

from osgeo import gdal, ogr, osr

ogr.UseExceptions()
gdal.PushErrorHandler('CPLQuietErrorHandler')


def box(w, s, e, n):
    return ogr.CreateGeometryFromWkt('POLYGON((%r %r,%r %r,%r %r,%r %r,%r %r))' % (w, s, e, s, e, n, w, n, w, s))


def parts(g, flat_type):
    """Clones of the parts of g of one flat type (wkbPoint, wkbLineString, wkbPolygon)"""
    if g is None or g.IsEmpty():
        return []
    t = ogr.GT_Flatten(g.GetGeometryType())
    if t == flat_type:
        return [g.Clone()]
    if t in (ogr.wkbMultiPoint, ogr.wkbMultiLineString, ogr.wkbMultiPolygon, ogr.wkbGeometryCollection):
        out = []
        for i in range(g.GetGeometryCount()):
            out += parts(g.GetGeometryRef(i), flat_type)
        return out
    return []


def multi(geoms, multi_type):
    m = ogr.Geometry(multi_type)
    for g in geoms:
        m.AddGeometry(g)
    return m


def shift_points(g, dx):
    """The points of the geometry moved along the longitudes."""
    t = ogr.GT_Flatten(g.GetGeometryType())
    if t == ogr.wkbPoint:
        g.SetPoint_2D(0, g.GetX(0) + dx, g.GetY(0))
    elif t == ogr.wkbLineString:
        for i in range(g.GetPointCount()):
            g.SetPoint_2D(i, g.GetX(i) + dx, g.GetY(i))
    elif t == ogr.wkbPolygon:
        for r in range(g.GetGeometryCount()):
            ring = g.GetGeometryRef(r)
            for i in range(ring.GetPointCount()):
                ring.SetPoint_2D(i, ring.GetX(i) + dx, ring.GetY(i))
    elif t in (ogr.wkbMultiPoint, ogr.wkbMultiLineString, ogr.wkbMultiPolygon, ogr.wkbGeometryCollection):
        for i in range(g.GetGeometryCount()):
            shift_points(g.GetGeometryRef(i), dx)


def cell_features(layer, rects, y0, y1):
    """The features of a layer inside a cell, in the frame of the cell.

    LINZ keeps the frame of every chart: the longitudes west of the antimeridian come as 180+, so a cell of the
    western half is looked up in both frames, and the geometry of the second frame is moved west.
    """
    for (rx0, rx1) in rects:
        layer.SetSpatialFilterRect(rx0, y0, rx1, y1)
        for f in layer:
            g = f.GetGeometryRef()
            if g is None:
                continue
            if rx0 > 180:
                g = g.Clone()
                shift_points(g, -360)
            yield f, g


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('source')
    parser.add_argument('output')
    parser.add_argument('--bbox', nargs=4, type=float, required=True, metavar=('W', 'S', 'E', 'N'))
    parser.add_argument('--bands', type=int, default=5)
    parser.add_argument('--cell', type=float, default=1)
    parser.add_argument('--max-extent', type=float, default=20,
                        help='a feature bigger than this many degrees is broken and skipped')
    args = parser.parse_args()

    src = ogr.Open(args.source)
    dst = ogr.GetDriverByName('GPKG').CreateDataSource(args.output)
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(4326)
    srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
    out = {}
    for name, geom_type, field, field_max in (('dybdekurve', ogr.wkbMultiLineString, 'dybde', None),
                                              ('dybdepunkt', ogr.wkbPoint, 'dybde', None),
                                              ('dybdeareal', ogr.wkbMultiPolygon, 'minimumsdybde', 'maksimumsdybde')):
        layer = dst.CreateLayer(name, srs, geom_type, options=['GEOMETRY_NAME=SHAPE', 'SPATIAL_INDEX=YES'])
        layer.CreateField(ogr.FieldDefn(field, ogr.OFTReal))
        if field_max:
            layer.CreateField(ogr.FieldDefn(field_max, ogr.OFTReal))
        out[name] = (layer, field, field_max)
    counts = {name: 0 for name in out}

    def write(name, geom, value, value_max=None):
        layer, field, field_max = out[name]
        f = ogr.Feature(layer.GetLayerDefn())
        f.SetField(field, value)
        if field_max and value_max is not None:
            f.SetField(field_max, value_max)
        f.SetGeometry(geom)
        layer.CreateFeature(f)
        counts[name] += 1

    w, s, e, n = args.bbox
    c = args.cell
    max_extent = args.max_extent
    # a box that crosses the antimeridian is two ranges of longitudes
    ranges = [(w, e)] if w <= e else [(w, 180.0), (-180.0, e)]
    dst.StartTransaction()
    for (w0, w1) in ranges:
        for i in range(math.floor(w0 / c), math.ceil(w1 / c)):
            for j in range(math.floor(s / c), math.ceil(n / c)):
                x0, y0, x1, y1 = max(w0, i * c), max(s, j * c), min(w1, (i + 1) * c), min(n, (j + 1) * c)
                cell = box(x0, y0, x1, y1)
                # the data west of the antimeridian is stored as 180+: a cell of the western half is looked up
                # in both frames
                rects = [(x0, x1)] + ([(x0 + 360.0, x1 + 360.0)] if x0 < 0 else [])
                taken = None  # coverage of the larger scale bands inside the cell
                for b in range(1, args.bands + 1):
                    areas = src.GetLayer('area_%d' % b)
                    own = []
                    for f, g in cell_features(areas, rects, y0, y1):
                        xmin, xmax, ymin, ymax = g.GetEnvelope()
                        if max_extent and (xmax - xmin > max_extent or ymax - ymin > max_extent):
                            continue    # a broken feature: the LINZ depth area layer has one around the globe
                        g = g.GetLinearGeometry()
                        if not g.IsValid():
                            g = g.MakeValid()
                        for p in parts(g.Intersection(cell), ogr.wkbPolygon):
                            rest = p if taken is None else p.Difference(taken)
                            for q in parts(rest, ogr.wkbPolygon):
                                write('dybdeareal', multi([q], ogr.wkbMultiPolygon), f.GetField('drval1'),
                                      f.GetField('drval2'))
                            own.append(p)
                    coverage = multi(own, ogr.wkbMultiPolygon).UnionCascaded() if own else None

                    contours = src.GetLayer('contour_%d' % b)
                    for f, g in cell_features(contours, rects, y0, y1):
                        if f.GetField('valdco') is None:
                            continue
                        g = g.GetLinearGeometry().Intersection(cell)
                        if taken is not None and not g.IsEmpty():
                            g = g.Difference(taken)
                        lines = parts(g, ogr.wkbLineString)
                        if lines:
                            write('dybdekurve', multi(lines, ogr.wkbMultiLineString), f.GetField('valdco'))

                    soundings = src.GetLayer('sounding_%d' % b)
                    for f, g in cell_features(soundings, rects, y0, y1):
                        if f.GetField('depth') is None:
                            continue
                        p = ogr.Geometry(ogr.wkbPoint)
                        p.AddPoint_2D(g.GetX(), g.GetY())
                        # half-open cell, so a sounding on a cell edge is written once
                        if not (x0 <= p.GetX() < x1 and y0 <= p.GetY() < y1):
                            continue
                        if taken is not None and taken.Contains(p):
                            continue
                        write('dybdepunkt', p, f.GetField('depth'))

                    if coverage is not None:
                        taken = coverage if taken is None else taken.Union(coverage)
    dst.CommitTransaction()
    print('merged bands: %d contours, %d soundings, %d depth areas' % (
        counts['dybdekurve'], counts['dybdepunkt'], counts['dybdeareal']))


if __name__ == '__main__':
    main()
