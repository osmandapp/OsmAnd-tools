#!/usr/bin/env python3
"""The land mask of a charted region without its lakes: the charted depth areas are water.

The OSM land polygons cover the inland lakes, so the soundings of a lake are dropped as if they were on land
and the depth areas of a lake are cut away. This mask is the land with the charted depth areas taken out, so
that a lake with charted depths keeps its soundings and its fill. Only the areas inside the land are taken
out: the areas of the sea lie outside the land already.

    depth_water_mask.py LAND OUT.gpkg --area SOURCE.gpkg [--layer NAME ...] [--bbox W S E N] [--max-extent DEG]

LAND is the OSM land polygons (the first layer), SOURCE the chart data with the depth areas (the layers of
--layer, all of them by default). A feature bigger than --max-extent degrees is skipped: the LINZ depth area
layer has one polygon around the globe, which is broken.
"""
import argparse

from osgeo import ogr, osr

ogr.UseExceptions()


def polygons(g):
    """The polygons of a geometry, whatever its type"""
    if g is None or g.IsEmpty():
        return []
    try:
        # the rings of the land mask and of the chart areas are curves
        g = g.GetLinearGeometry()
    except (AttributeError, RuntimeError):
        pass
    if g.GetGeometryType() == ogr.wkbPolygon:
        return [g.Clone()]
    out = []
    for i in range(g.GetGeometryCount()):
        out += polygons(g.GetGeometryRef(i))
    return out


def shift_points(g, dx):
    """The points of the geometry moved along the longitudes."""
    t = ogr.GT_Flatten(g.GetGeometryType())
    if t == ogr.wkbPolygon:
        for r in range(g.GetGeometryCount()):
            ring = g.GetGeometryRef(r)
            for i in range(ring.GetPointCount()):
                ring.SetPoint_2D(i, ring.GetX(i) + dx, ring.GetY(i))
    elif t in (ogr.wkbMultiPolygon, ogr.wkbGeometryCollection):
        for i in range(g.GetGeometryCount()):
            shift_points(g.GetGeometryRef(i), dx)


def merge(parts):
    """One geometry out of many, valid"""
    flat = []
    for p in parts:
        flat += p
    if not flat:
        return None
    m = ogr.Geometry(ogr.wkbMultiPolygon)
    for p in flat:
        if p.GetGeometryType() != ogr.wkbPolygon:
            p = ogr.ForceToPolygon(p)
            if p is None or p.IsEmpty():
                continue
        m.AddGeometry(p)
    u = m.UnionCascaded()
    try:
        if u.IsValid():
            return u
    except RuntimeError:
        pass
    try:
        v = u.MakeValid()
        return v if v is not None and not v.IsEmpty() else u
    except (AttributeError, RuntimeError):
        return u


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('land', help='the OSM land polygons')
    ap.add_argument('output', help='the GeoPackage to write')
    ap.add_argument('--area', required=True, help='the chart data with the depth areas')
    ap.add_argument('--layer', action='append', help='the layer of the depth areas, repeatable')
    ap.add_argument('--bbox', nargs=4, type=float, metavar=('W', 'S', 'E', 'N'))
    ap.add_argument('--max-extent', type=float, default=20,
                    help='a feature bigger than this many degrees is broken and skipped')
    args = ap.parse_args()

    land_src = ogr.Open(args.land)
    land = land_src.GetLayer(0)
    area_src = ogr.Open(args.area)
    layers = [area_src.GetLayerByName(n) for n in args.layer] if args.layer \
        else [area_src.GetLayer(i) for i in range(area_src.GetLayerCount())]

    srs = osr.SpatialReference()
    srs.ImportFromEPSG(4326)
    srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
    dst = ogr.GetDriverByName('GPKG').CreateDataSource(args.output)
    out = dst.CreateLayer('land_polygons', srs, ogr.wkbPolygon, options=['SPATIAL_INDEX=YES'])

    if args.bbox:
        w, s, e, n = args.bbox
    else:
        xmin, xmax, ymin, ymax = land.GetExtent()
        w, s, e, n = xmin, ymin, xmax, ymax

    # a degree cell: the land of the cell is cut out of the land polygons, the lakes of the cell are the
    # depth areas inside it. A box that crosses the antimeridian is two ranges of longitudes.
    ranges = [(w, e)] if w <= e else [(w, 180.0), (-180.0, e)]
    lakes = 0
    for (w0, w1) in ranges:
        for i in range(int(w0), int(w1) + 1):
            for j in range(int(s), int(n) + 1):
                cell = ogr.CreateGeometryFromWkt(
                    'POLYGON((%d %d,%d %d,%d %d,%d %d,%d %d))' % (i, j, i + 1, j, i + 1, j + 1, i, j + 1, i, j))
                land.SetSpatialFilterRect(i, j, i + 1, j + 1)
                cell_land = merge(polygons(f.GetGeometryRef().Intersection(cell)) for f in land)
                if cell_land is None or cell_land.IsEmpty():
                    continue
                parts = []
                for l in layers:
                    # the data west of the antimeridian is stored as 180+: the cell is looked up in both frames
                    for (rx0, rx1) in ([(i, i + 1), (i + 360.0, i + 361.0)] if i < 0 else [(i, i + 1)]):
                        l.SetSpatialFilterRect(rx0, j, rx1, j + 1)
                        for f in l:
                            g = f.GetGeometryRef()
                            if g is None:
                                continue
                            b = g.GetEnvelope()
                            if b[1] - b[0] > args.max_extent or b[3] - b[2] > args.max_extent:
                                continue    # a broken feature
                            if rx0 > 180:
                                g = g.Clone()
                                shift_points(g, -360)
                            # the chart areas are curve polygons: the arcs are straightened
                            inter = g.GetLinearGeometry().Intersection(cell_land)
                            if inter is None or inter.IsEmpty():
                                continue
                            parts += polygons(inter.GetLinearGeometry())
                cell_area = merge(parts)
                if cell_area is None or cell_area.IsEmpty():
                    result = cell_land
                else:
                    try:
                        result = cell_land.Difference(cell_area)
                    except RuntimeError:
                        result = cell_land
                    lakes += 1
                for p in polygons(result):
                    f = ogr.Feature(out.GetLayerDefn())
                    f.SetGeometry(p)
                    out.CreateFeature(f)
    dst = None
    print('water mask: %d cells with lakes' % lakes)


if __name__ == '__main__':
    main()
