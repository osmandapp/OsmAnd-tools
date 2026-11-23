#!/usr/bin/env python3
"""The EMODnet base of a detailed map: only inside the contour of the detailed data.

The general EMODnet covers the whole sea of the region, far beyond the detailed surveys. This
script limits it to the contour of the detailed data:

  - the detailed sources are reduced to a coarse grid (CELL degrees) and split into connected
    components; every component is a continuous area of the data;
  - the convex hull of every component is taken and all the hulls are united into one contour:
    the gaps inside a continuous area get the base, the sea far away from the data does not;
  - the contour is cut by the allowed area (the midpoint to the neighbouring land for the UK);
  - the EMODnet is cut by the contour, the result is the base of the map and its VRT.

The contour is written to a GPKG (layer "region"), so that it can be used for the coverage of
the neighbouring maps and for the preview.

    depth_emodnet_base.py --name NAME --detail SRC ... --emodnet EMODNET_VRT --bbox W S E N
                          --out BASE.tif --region REGION.gpkg
                          [--cell 0.005] [--allowed GPKG] [--preview PNG]
"""
import argparse
import math
import os

import numpy as np
import rasterio
import rasterio.features
import rasterio.transform
from osgeo import gdal, ogr, osr
from scipy import ndimage
from shapely import wkb
from shapely.geometry import shape
from shapely.ops import unary_union

from uk_blend import atomic_vrt

gdal.UseExceptions()
ogr.UseExceptions()
gdal.PushErrorHandler('CPLQuietErrorHandler')


def detail_mask(sources, bbox, cell):
    """The cells of the detailed data on the coarse grid."""
    left, bottom, right, top = bbox
    width = math.ceil((right - left) / cell)
    height = math.ceil((top - bottom) / cell)
    bounds = (left, top - height * cell, left + width * cell, top)
    ds = gdal.Warp('/vsimem/depth_emodnet_base_mask.tif', sources, format='MEM', dstSRS='EPSG:4326',
                   outputBounds=bounds, xRes=cell, yRes=cell, resampleAlg='near',
                   dstNodata=float('nan'), outputType=gdal.GDT_Float32)
    mask = np.isfinite(ds.GetRasterBand(1).ReadAsArray())
    ds = None
    return mask, bounds


def components(mask, bounds, cell):
    """The connected areas of the detailed data and the hull of every one of them."""
    labels, count = ndimage.label(mask, structure=np.ones((3, 3)))
    transform = rasterio.transform.from_origin(bounds[0], bounds[3], cell, cell)
    geoms = {}
    for geom, value in rasterio.features.shapes(labels.astype(np.int32), mask=mask,
                                                transform=transform, connectivity=4):
        g = shape(geom)
        geoms[value] = g if value not in geoms else unary_union([geoms[value], g])
    hulls = [g.convex_hull for g in geoms.values()]
    return count, unary_union(hulls)


def polygonal(geom):
    """Only the polygonal parts of a geometry: the cutline of GDAL must be a polygon."""
    if geom.geom_type in ('Polygon', 'MultiPolygon'):
        return geom
    if geom.geom_type == 'GeometryCollection':
        parts = [g for g in geom.geoms if g.geom_type in ('Polygon', 'MultiPolygon')]
        return unary_union(parts) if parts else None
    return None


def load_gpkg(path):
    ds = ogr.Open(path)
    geoms = [wkb.loads(bytes(f.GetGeometryRef().ExportToWkb()))
             for f in ds.GetLayer(0) if f.GetGeometryRef() is not None and not f.GetGeometryRef().IsEmpty()]
    layer = ds.GetLayer(0).GetName()
    ds = None
    return unary_union(geoms), layer


def write_region(path, geom):
    if os.path.exists(path):
        os.remove(path)
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(4326)
    ds = ogr.GetDriverByName('GPKG').CreateDataSource(path)
    layer = ds.CreateLayer('region', srs, ogr.wkbMultiPolygon)
    feature = ogr.Feature(layer.GetLayerDefn())
    feature.SetGeometry(ogr.CreateGeometryFromWkb(geom.wkb))
    layer.CreateFeature(feature)
    ds = None


def cut_emodnet(emodnet, region_path, bbox, out_path):
    """The EMODnet inside the contour: the base of the map."""
    with rasterio.open(emodnet) as source:
        cell = source.res[0]
    left, bottom, right, top = bbox
    tmp = out_path + '.tmp.tif'
    if os.path.exists(tmp):
        os.remove(tmp)
    try:
        gdal.Warp(tmp, os.path.abspath(emodnet), format='GTiff', dstSRS='EPSG:4326',
                  outputBounds=(left, bottom, right, top), xRes=cell, yRes=cell,
                  targetAlignedPixels=True, resampleAlg='near', cutlineDSName=region_path,
                  cutlineLayer='region', cropToCutline=False, dstNodata=float('nan'),
                  outputType=gdal.GDT_Float32, multithread=True,
                  warpOptions=['NUM_THREADS=16'],
                  creationOptions=['COMPRESS=DEFLATE', 'PREDICTOR=3', 'TILED=YES',
                                   'SPARSE_OK=TRUE', 'BIGTIFF=IF_SAFER'])
        os.replace(tmp, out_path)
    finally:
        if os.path.exists(tmp):
            os.remove(tmp)
    return cell


def save_preview(path, mask, bounds, region, cell):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    left, bottom, right, top = bounds
    fig, ax = plt.subplots(figsize=(14, 14 * mask.shape[0] / mask.shape[1]), dpi=110)
    ax.set_facecolor('#dceaf5')
    ax.imshow(mask, extent=(left, right, bottom, top), cmap='Greys', interpolation='nearest')
    from matplotlib.path import Path
    from matplotlib.patches import PathPatch
    polys = region.geoms if region.geom_type == 'MultiPolygon' else [region]
    for poly in polys:
        verts, codes = [], []
        for ring in [poly.exterior] + list(poly.interiors):
            pts = list(ring.coords)
            verts.extend(pts)
            codes.append(Path.MOVETO)
            codes.extend([Path.LINETO] * (len(pts) - 2))
            codes.append(Path.CLOSEPOLY)
        ax.add_patch(PathPatch(Path(verts, codes), fill=False, edgecolor='#d62728', linewidth=2))
    ax.set_xlim(left, right)
    ax.set_ylim(bottom, top)
    ax.set_aspect(1 / math.cos(math.radians((bottom + top) / 2)))
    ax.set_xticks([])
    ax.set_yticks([])
    ax.set_title('the detailed data and the contour of the base', fontsize=14, fontweight='bold')
    fig.tight_layout()
    fig.savefig(path)
    plt.close(fig)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--name', required=True, help='the name of the map')
    parser.add_argument('--detail', nargs='+', required=True, metavar='SRC',
                        help='the detailed sources: the contour of the base is computed from them')
    parser.add_argument('--emodnet', required=True, help='the general EMODnet')
    parser.add_argument('--bbox', nargs=4, type=float, required=True, metavar=('W', 'S', 'E', 'N'))
    parser.add_argument('--out', required=True, help='the base of the map (GeoTIFF)')
    parser.add_argument('--region', required=True, help='the contour of the base (GPKG)')
    parser.add_argument('--cell', type=float, default=0.005, help='the grid of the contour in degrees')
    parser.add_argument('--allowed', help='the area the base may not leave (the midpoint to the neighbours)')
    parser.add_argument('--preview', help='a PNG with the detailed data and the contour')
    args = parser.parse_args()

    for source in args.detail:
        if not os.path.exists(source):
            raise SystemExit('%s: no %s' % (args.name, source))
    if not os.path.exists(args.emodnet):
        raise SystemExit('%s: no %s' % (args.name, args.emodnet))

    mask, bounds = detail_mask(args.detail, args.bbox, args.cell)
    count, region = components(mask, bounds, args.cell)
    if args.allowed:
        allowed, _ = load_gpkg(args.allowed)
        region = region.intersection(allowed)
    region = polygonal(region)
    if region is None or region.is_empty:
        raise SystemExit('%s: the contour of the base is empty' % args.name)
    print('%s: %d continuous areas, the contour is %.2f square degrees'
          % (args.name, count, region.area), flush=True)

    os.makedirs(os.path.dirname(os.path.abspath(args.region)), exist_ok=True)
    write_region(args.region, region)
    cell = cut_emodnet(args.emodnet, args.region, args.bbox, args.out)
    atomic_vrt([args.out], args.out.replace('.tif', '_source.txt'), args.out.replace('.tif', '.vrt'), cell)
    print('%s: the base is written to %s' % (args.name, args.out), flush=True)

    if args.preview:
        save_preview(args.preview, mask, bounds, region, args.cell)
        print('%s: the preview is written to %s' % (args.name, args.preview), flush=True)


if __name__ == '__main__':
    main()
