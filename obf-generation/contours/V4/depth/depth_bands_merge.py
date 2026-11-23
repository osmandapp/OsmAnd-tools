#!/usr/bin/env python3
"""Chart vector data of several scale bands merged into one set: the largest scale band wins wherever it has depth areas.

    depth_bands_merge.py SOURCE OUTPUT.gpkg --bbox W S E N [--bands 5] [--cell 1]

SOURCE has, for every band b from 1 (largest scale, harbour charts) to BANDS (smallest), the layers contour_b (field
valdco), sounding_b (depth) and area_b (drval1): LINZ hydrographic data as download_all.sh writes it. The depth areas
of a band are its coverage. Per CELL degree square, band by band from the largest scale, a band keeps what lies
outside the coverage of the larger scale bands; its own coverage is then added. Curves are linearized.

OUTPUT gets the layers and fields of Kartverket's "Sjøkart - Dybdedata", so build_depth_kartverket.sh builds it:
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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('source')
    parser.add_argument('output')
    parser.add_argument('--bbox', nargs=4, type=float, required=True, metavar=('W', 'S', 'E', 'N'))
    parser.add_argument('--bands', type=int, default=5)
    parser.add_argument('--cell', type=float, default=1)
    args = parser.parse_args()

    src = ogr.Open(args.source)
    dst = ogr.GetDriverByName('GPKG').CreateDataSource(args.output)
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(4326)
    srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
    out = {}
    for name, geom_type, field in (('dybdekurve', ogr.wkbMultiLineString, 'dybde'),
                                   ('dybdepunkt', ogr.wkbPoint, 'dybde'),
                                   ('dybdeareal', ogr.wkbMultiPolygon, 'minimumsdybde')):
        layer = dst.CreateLayer(name, srs, geom_type, options=['GEOMETRY_NAME=SHAPE', 'SPATIAL_INDEX=YES'])
        layer.CreateField(ogr.FieldDefn(field, ogr.OFTReal))
        out[name] = (layer, field)
    counts = {name: 0 for name in out}

    def write(name, geom, value):
        layer, field = out[name]
        f = ogr.Feature(layer.GetLayerDefn())
        f.SetField(field, value)
        f.SetGeometry(geom)
        layer.CreateFeature(f)
        counts[name] += 1

    w, s, e, n = args.bbox
    c = args.cell
    dst.StartTransaction()
    for i in range(math.floor(w / c), math.ceil(e / c)):
        for j in range(math.floor(s / c), math.ceil(n / c)):
            x0, y0, x1, y1 = max(w, i * c), max(s, j * c), min(e, (i + 1) * c), min(n, (j + 1) * c)
            cell = box(x0, y0, x1, y1)
            taken = None  # coverage of the larger scale bands inside the cell
            for b in range(1, args.bands + 1):
                areas = src.GetLayer('area_%d' % b)
                areas.SetSpatialFilterRect(x0, y0, x1, y1)
                own = []
                for f in areas:
                    g = f.GetGeometryRef()
                    if g is None:
                        continue
                    g = g.GetLinearGeometry()
                    if not g.IsValid():
                        g = g.MakeValid()
                    for p in parts(g.Intersection(cell), ogr.wkbPolygon):
                        rest = p if taken is None else p.Difference(taken)
                        for q in parts(rest, ogr.wkbPolygon):
                            write('dybdeareal', multi([q], ogr.wkbMultiPolygon), f.GetField('drval1'))
                        own.append(p)
                coverage = multi(own, ogr.wkbMultiPolygon).UnionCascaded() if own else None

                contours = src.GetLayer('contour_%d' % b)
                contours.SetSpatialFilterRect(x0, y0, x1, y1)
                for f in contours:
                    g = f.GetGeometryRef()
                    if g is None or f.GetField('valdco') is None:
                        continue
                    g = g.GetLinearGeometry().Intersection(cell)
                    if taken is not None and not g.IsEmpty():
                        g = g.Difference(taken)
                    lines = parts(g, ogr.wkbLineString)
                    if lines:
                        write('dybdekurve', multi(lines, ogr.wkbMultiLineString), f.GetField('valdco'))

                soundings = src.GetLayer('sounding_%d' % b)
                soundings.SetSpatialFilterRect(x0, y0, x1, y1)
                for f in soundings:
                    g = f.GetGeometryRef()
                    if g is None or f.GetField('depth') is None:
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
