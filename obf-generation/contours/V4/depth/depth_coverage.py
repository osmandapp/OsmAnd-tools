#!/usr/bin/env python3
"""Coverage polygons of a detailed depth source: where it has data, so that a wider map can leave that out.

    depth_coverage.py SOURCE [SOURCE ...] OUTPUT.gpkg [--bbox W S E N] [--cell 0.001] [--shrink CELLS] [--below VALUE]
                           [--layer LAYER ...] [--radius DEG] [--max-extent DEG]

SOURCEs are rasters (GeoTIFF, VRT) or, with --layer, vector sources whose LAYERs are taken (Kartverket datakvalitet,
LINZ area_1 ... area_5). A raster is reduced to CELL degree squares that hold any data and polygonized; all polygons
are merged. With --below, only the cells below VALUE count as data: a bathymetric grid such as GEBCO also holds land
heights. The result is shrunk by SHRINK cells (default 1), so that the wider map still meets the detailed one at the edge
instead of leaving a gap, then simplified by half a cell. OUTPUT has one layer "coverage", EPSG:4326, no attributes.

The source keeps the frame of every chart: the longitudes west of the antimeridian come as 180+, so the geometries are
moved into -180..180 before the bbox is applied. With --radius the lines and the points of a vector layer reach that
far into the coverage, so that a wider map leaves them out as well.
"""
import argparse
import math
import os

from osgeo import gdal, ogr, osr

gdal.UseExceptions()
ogr.UseExceptions()
gdal.PushErrorHandler('CPLQuietErrorHandler')


def box(w, s, e, n):
    return ogr.CreateGeometryFromWkt('POLYGON((%r %r,%r %r,%r %r,%r %r,%r %r))' % (w, s, e, s, e, n, w, n, w, s))


def bbox_geom(bbox):
    """The box of a region; a box that crosses the antimeridian is two boxes."""
    w, s, e, n = bbox
    if w <= e:
        return box(w, s, e, n)
    return box(w, s, 180, n).Union(box(-180, s, e, n))


def hull_components(area, cell, bbox):
    """The convex hull of every connected component of the area on a grid of CELL degrees: the gaps inside a
    continuous area are closed, remote islands do not drag the emptiness between them. This is how the base of the
    detailed maps is cut (depth_emodnet_base.py), and how the chart data of New Zealand becomes its coverage.

    The parts west of the antimeridian are moved east first, so that the data of the Pacific is one area, and the
    hulls are moved back and cut at the antimeridian afterwards."""
    from scipy import ndimage
    import numpy as np
    import rasterio.features
    import rasterio.transform
    import shapely
    from shapely.affinity import translate
    from shapely.geometry import shape

    g = shapely.from_wkb(bytes(area.ExportToWkb()))
    crossing = bool(bbox) and bbox[0] > bbox[2]
    if crossing:
        e_west = bbox[2]
        parts = list(g.geoms) if g.geom_type == 'MultiPolygon' else [g]
        g = shapely.union_all([translate(p, 360) if p.bounds[2] <= e_west + 1e-9 else p for p in parts])
    minx, miny, maxx, maxy = g.bounds
    w, h = max(1, int(math.ceil((maxx - minx) / cell))), max(1, int(math.ceil((maxy - miny) / cell)))
    transform = rasterio.transform.from_origin(minx, miny + h * cell, cell, cell)
    mask = rasterio.features.rasterize([(g, 1)], out_shape=(h, w), transform=transform, fill=0, dtype='uint8')
    labels, count = ndimage.label(mask, structure=np.ones((3, 3)))
    hulls = [shape(geom).convex_hull
             for geom, value in rasterio.features.shapes(labels.astype('int32'), mask=mask,
                                                         transform=transform, connectivity=4)]
    hull = shapely.union_all(hulls)
    if not crossing:
        return ogr.CreateGeometryFromWkb(hull.wkb)
    parts = list(hull.geoms) if hull.geom_type == 'MultiPolygon' else [hull]
    back = []
    for p in parts:
        x0, y0, x1, y1 = p.bounds
        if x0 >= 180 - 1e-9:
            back.append(translate(p, -360))
        elif x1 > 180 + 1e-9:
            # a hull that spans the antimeridian: it is cut there
            east = p.intersection(shapely.box(180, y0, x1, y1))
            west = p.intersection(shapely.box(-180, y0, 180, y1))
            if not east.is_empty:
                back.append(translate(east, -360))
            if not west.is_empty:
                back.append(west)
        else:
            back.append(p)
    return ogr.CreateGeometryFromWkb(shapely.union_all(back).wkb)


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


def shift(g, dx):
    """A copy of the geometry with its longitudes moved by DX."""
    g = g.Clone()
    shift_points(g, dx)
    return g


def wrap(g):
    """The geometry with its longitudes in -180..180: the source keeps the frame of every chart, so the data west
    of the antimeridian comes as 180+."""
    xmin, xmax, ymin, ymax = g.GetEnvelope()
    if xmax <= 180:
        return g
    if xmin >= 180:
        return shift(g, -360)
    # the geometry crosses the antimeridian: the part east of it moves west
    east = box(180, ymin - 1, 360, ymax + 1)
    return g.Difference(east).Union(shift(g.Intersection(east), -360))


def raster_polygons(path, bbox, cell, below=None):
    warp = dict(format='VRT', dstSRS='EPSG:4326', xRes=cell, yRes=cell, resampleAlg='average', dstNodata=float('nan'),
                outputType=gdal.GDT_Float32)
    if bbox:
        warp['outputBounds'] = bbox
    coarse = gdal.Warp('/vsimem/coverage_coarse.tif', path, **dict(warp, format='GTiff'))
    if below is None:
        # every value with data becomes 1, nodata stays nodata (0 in Byte): no numpy needed
        mask = gdal.Translate('/vsimem/coverage_mask.tif', coarse, outputType=gdal.GDT_Byte, noData=0,
                              scaleParams=[[-1e6, 1e6, 1, 1]])
    else:
        # a bathymetric grid also holds land heights: only the cells below the level are water
        import numpy as np
        arr = coarse.GetRasterBand(1).ReadAsArray()
        water = np.where(np.isfinite(arr) & (arr < below), 1, 0).astype('uint8')
        mask = gdal.GetDriverByName('MEM').Create('', coarse.RasterXSize, coarse.RasterYSize, 1,
                                                  gdal.GDT_Byte)
        mask.SetGeoTransform(coarse.GetGeoTransform())
        mask.SetProjection(coarse.GetProjection())
        band = mask.GetRasterBand(1)
        band.WriteArray(water)
        band.SetNoDataValue(0)
    band = mask.GetRasterBand(1)
    mem = ogr.GetDriverByName('Memory').CreateDataSource('') if int(gdal.VersionInfo()) < 3110000 \
        else ogr.GetDriverByName('MEM').CreateDataSource('')
    layer = mem.CreateLayer('p', mask.GetSpatialRef(), ogr.wkbPolygon)
    layer.CreateField(ogr.FieldDefn('v', ogr.OFTInteger))
    gdal.Polygonize(band, band.GetMaskBand(), layer, 0)
    geoms = [f.GetGeometryRef().Clone() for f in layer]
    coarse = mask = None
    for p in ('/vsimem/coverage_coarse.tif', '/vsimem/coverage_mask.tif'):
        try:
            gdal.Unlink(p)
        except RuntimeError:
            pass
    return geoms


def vector_geoms(path, layer_name, bbox, max_extent=0):
    """The geometries of a vector layer in -180..180: the polygons as they are, the lines and the points as they are."""
    ds = ogr.Open(path)
    layer = ds.GetLayer(layer_name)
    geoms = []
    for f in layer:
        g = f.GetGeometryRef()
        if g is None:
            continue
        xmin, xmax, ymin, ymax = g.GetEnvelope()
        if max_extent and (xmax - xmin > max_extent or ymax - ymin > max_extent):
            continue    # a broken feature: the LINZ depth area layer has one around the globe
        g = g.GetLinearGeometry()
        if not g.IsValid():
            g = g.MakeValid()
        g = wrap(g)
        if bbox:
            # the bbox is applied after the wrap: the frames of the source and the bbox are different
            w, s, e, n = bbox
            bxmin, bxmax, bymin, bymax = g.GetEnvelope()
            if bymax < s or bymin > n:
                continue
            if w <= e:
                if bxmax < w or bxmin > e:
                    continue
            elif bxmin > e and bxmax < w:
                continue    # a box that crosses the antimeridian: the data is east of w or west of e
        geoms.append(g)
    return geoms


def polygon_parts(g):
    """The parts of a geometry that are polygons."""
    t = ogr.GT_Flatten(g.GetGeometryType())
    if t == ogr.wkbPolygon:
        return [g.Clone()]
    if t in (ogr.wkbMultiPolygon, ogr.wkbGeometryCollection):
        out = []
        for i in range(g.GetGeometryCount()):
            out += polygon_parts(g.GetGeometryRef(i))
        return out
    return []


def union(geoms):
    """The union of the polygons."""
    multi = ogr.Geometry(ogr.wkbMultiPolygon)
    for g in geoms:
        for p in polygon_parts(g):
            multi.AddGeometry(p)
    return multi.UnionCascaded() if multi.GetGeometryCount() else ogr.Geometry(ogr.wkbMultiPolygon)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('paths', nargs='+', metavar='SOURCE ... OUTPUT')
    parser.add_argument('--bbox', nargs=4, type=float, metavar=('W', 'S', 'E', 'N'))
    parser.add_argument('--cell', type=float, default=0.001, help='degrees')
    parser.add_argument('--shrink', type=float, default=1,
                        help='cells; 0 for a coverage that must meet its neighbour exactly and for --complement')
    parser.add_argument('--below', type=float,
                        help='only the cells below this value are data (for bathymetric grids '
                             'that also hold land heights)')
    parser.add_argument('--layer', action='append', help='vector layer, repeatable; without it the sources are rasters')
    parser.add_argument('--radius', type=float, default=0,
                        help='how far the lines and the points of a vector layer reach into the coverage')
    parser.add_argument('--max-extent', type=float, default=0,
                        help='a vector feature bigger than this many degrees is broken and skipped '
                             '(the LINZ depth area layer has one around the globe)')
    parser.add_argument('--hull', type=float, metavar='CELL',
                        help='the convex hull of every connected component of the data on a grid of CELL degrees, '
                             'as the base of the detailed maps is cut')
    parser.add_argument('--complement', action='store_true',
                        help='the rest of the bbox instead: where the source has no data (needs --bbox)')
    parser.add_argument('--exclude', action='append', metavar='FILE',
                        help='a vector file whose areas are cut out of the coverage (the inland water: '
                             'the lakes and the depressions below the sea level), repeatable')
    args = parser.parse_args()
    if len(args.paths) < 2:
        parser.error('a source and the output are needed')
    sources, output = args.paths[:-1], args.paths[-1]

    polys, lines, points = [], [], []
    for source in sources:
        if args.layer:
            for layer in args.layer:
                for g in vector_geoms(source, layer, args.bbox, args.max_extent):
                    t = ogr.GT_Flatten(g.GetGeometryType())
                    if t in (ogr.wkbPolygon, ogr.wkbMultiPolygon):
                        polys += polygon_parts(g)
                    elif t in (ogr.wkbLineString, ogr.wkbMultiLineString):
                        lines.append(g)
                    elif t == ogr.wkbPoint:
                        points.append(g)
        elif source.lower().endswith('.gpkg'):
            # a coverage of another map: taken as it is
            for g in vector_geoms(source, 0, args.bbox, args.max_extent):
                polys += polygon_parts(g)
        else:
            polys += raster_polygons(source, args.bbox, args.cell, args.below)
    area = union(polys)
    if args.radius and (lines or points):
        extra = []
        for g in lines:
            extra.append(g.Buffer(args.radius))
        for g in points:
            extra.append(box(g.GetX() - args.radius, g.GetY() - args.radius,
                             g.GetX() + args.radius, g.GetY() + args.radius))
        area = area.Union(union(extra))
    if args.bbox:
        area = area.Intersection(bbox_geom(args.bbox))
    if args.hull:
        area = hull_components(area, args.hull, args.bbox)
    if args.complement:
        if not args.bbox:
            parser.error('--complement needs --bbox')
        area = bbox_geom(args.bbox).Difference(area)
    for f in args.exclude or []:
        for g in vector_geoms(f, 0, args.bbox, args.max_extent):
            area = area.Difference(g)
    if args.shrink:
        area = area.Buffer(-args.shrink * args.cell)
    area = area.SimplifyPreserveTopology(args.cell / 2)

    srs = osr.SpatialReference()
    srs.ImportFromEPSG(4326)
    srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
    if os.path.exists(output):
        os.remove(output)
    out = ogr.GetDriverByName('GPKG').CreateDataSource(output)
    layer = out.CreateLayer('coverage', srs, ogr.wkbMultiPolygon)
    f = ogr.Feature(layer.GetLayerDefn())
    f.SetGeometry(ogr.ForceToMultiPolygon(area))
    layer.CreateFeature(f)
    print('coverage: %d polygons, %.4f square degrees' % (area.GetGeometryCount(), area.GetArea()))


if __name__ == '__main__':
    main()
