#!/usr/bin/env python3
"""Coverage polygons of a detailed depth source: where it has data, so that a wider map can leave that out.

    depth_coverage.py SOURCE [SOURCE ...] OUTPUT.gpkg [--bbox W S E N] [--cell 0.001] [--shrink CELLS] [--layer LAYER ...]

SOURCEs are rasters (GeoTIFF, VRT) or, with --layer, vector sources whose LAYERs are taken (Kartverket datakvalitet,
LINZ area_1 ... area_5). A raster is reduced to CELL degree squares that hold any data and polygonized; all polygons
are merged. The result is shrunk by SHRINK cells (default 1), so that the wider map still meets the detailed one at the edge
instead of leaving a gap, then simplified by half a cell. OUTPUT has one layer "coverage", EPSG:4326, no attributes.
"""
import argparse
import os

from osgeo import gdal, ogr, osr

gdal.UseExceptions()
ogr.UseExceptions()
gdal.PushErrorHandler('CPLQuietErrorHandler')


def raster_polygons(path, bbox, cell):
    warp = dict(format='VRT', dstSRS='EPSG:4326', xRes=cell, yRes=cell, resampleAlg='average', dstNodata=float('nan'),
                outputType=gdal.GDT_Float32)
    if bbox:
        warp['outputBounds'] = bbox
    coarse = gdal.Warp('/vsimem/coverage_coarse.tif', path, **dict(warp, format='GTiff'))
    # every value with data becomes 1, nodata stays nodata (0 in Byte): no numpy needed
    mask = gdal.Translate('/vsimem/coverage_mask.tif', coarse, outputType=gdal.GDT_Byte, noData=0,
                          scaleParams=[[-1e6, 1e6, 1, 1]])
    band = mask.GetRasterBand(1)
    mem = ogr.GetDriverByName('Memory').CreateDataSource('') if int(gdal.VersionInfo()) < 3110000 \
        else ogr.GetDriverByName('MEM').CreateDataSource('')
    layer = mem.CreateLayer('p', mask.GetSpatialRef(), ogr.wkbPolygon)
    layer.CreateField(ogr.FieldDefn('v', ogr.OFTInteger))
    gdal.Polygonize(band, band.GetMaskBand(), layer, 0)
    geoms = [f.GetGeometryRef().Clone() for f in layer]
    coarse = mask = None
    gdal.Unlink('/vsimem/coverage_coarse.tif')
    gdal.Unlink('/vsimem/coverage_mask.tif')
    return geoms


def vector_polygons(path, layer_name, bbox):
    ds = ogr.Open(path)
    layer = ds.GetLayer(layer_name)
    if bbox:
        layer.SetSpatialFilterRect(*bbox)
    geoms = []
    for f in layer:
        g = f.GetGeometryRef()
        if g is not None:
            g = g.GetLinearGeometry()
            geoms.append(g if g.IsValid() else g.MakeValid())
    return geoms


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('paths', nargs='+', metavar='SOURCE ... OUTPUT')
    parser.add_argument('--bbox', nargs=4, type=float, metavar=('W', 'S', 'E', 'N'))
    parser.add_argument('--cell', type=float, default=0.001, help='degrees')
    parser.add_argument('--shrink', type=float, default=1, help='cells')
    parser.add_argument('--layer', action='append', help='vector layer, repeatable; without it the sources are rasters')
    args = parser.parse_args()
    if len(args.paths) < 2:
        parser.error('a source and the output are needed')
    sources, output = args.paths[:-1], args.paths[-1]

    geoms = []
    for source in sources:
        if args.layer:
            for layer in args.layer:
                geoms += vector_polygons(source, layer, args.bbox)
        else:
            geoms += raster_polygons(source, args.bbox, args.cell)
    multi = ogr.Geometry(ogr.wkbMultiPolygon)
    for g in geoms:
        t = ogr.GT_Flatten(g.GetGeometryType())
        parts = [g] if t == ogr.wkbPolygon else [g.GetGeometryRef(i) for i in range(g.GetGeometryCount())]
        for p in parts:
            if ogr.GT_Flatten(p.GetGeometryType()) == ogr.wkbPolygon:
                multi.AddGeometry(p)
    area = multi.UnionCascaded() if multi.GetGeometryCount() else ogr.Geometry(ogr.wkbMultiPolygon)
    if args.bbox:
        w, s, e, n = args.bbox
        area = area.Intersection(ogr.CreateGeometryFromWkt(
            'POLYGON((%r %r,%r %r,%r %r,%r %r,%r %r))' % (w, s, e, s, e, n, w, n, w, s)))
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
