#!/usr/bin/env python3
"""The inland water of a depth grid: the water that is not connected with the ocean.

GEBCO keeps the bottom of the lakes and the depressions of the dry land below the sea level. A depth map
built from it draws them as if they were the sea: the Caspian Sea, Lake Baikal, the parts of the Great
Lakes, the Turpan Depression, the Danakil. Only the water that is connected with the ocean is the sea.

    depth_inland_water.py GRID OUT.gpkg [--cell 0.01] [--margin 0.4] [--min-area 0.002] [--keep KEEP.gpkg]

GRID is the raster of the depths (the GEBCO VRT), the negative values are the water. The components of the
water are counted on a coarse grid (--cell): the ones that touch the border of the world are the ocean.
Every other component is checked again on the fine grid of the source: the water that reaches the border of
its own box is connected with the sea through a strait (the Danish belts), the water that does not reach it
is inland.

The result is one polygon per inland water body. It is used as land, so that the cells of the lakes and of
the depressions do not get into the map.
"""
import argparse
import json
import os
import sys
import tempfile

import numpy as np
import shapely.geometry
import shapely.ops
from osgeo import gdal, ogr, osr
from scipy import ndimage

gdal.UseExceptions()
ogr.UseExceptions()


def coarse_grid(grid, cell, path, nodata):
    """The grid of the water on a coarse cell: the mean of the cell, so that a narrow strait with land in it
    stays closed. The whole world, the ocean of the Arctic has to be in it."""
    gdal.Warp(path, grid, format="GTiff", outputBounds=(-180, -90, 180, 90), xRes=cell, yRes=cell,
              resampleAlg="average", srcNodata=nodata, dstSRS="EPSG:4326", outputType=gdal.GDT_Float32,
              dstNodata=float("nan"), creationOptions=["COMPRESS=DEFLATE", "TILED=YES"])


def components(arr):
    """The labels of the components of the water and their sizes"""
    labels, _ = ndimage.label(arr)
    sizes = np.bincount(labels.ravel())
    return labels, sizes


def border_labels(labels):
    """The labels of the components that touch the border of the array"""
    out = set()
    for line in (labels[0, :], labels[-1, :], labels[:, 0], labels[:, -1]):
        out.update(int(v) for v in np.unique(line) if v)
    return out


def enclosed_mask(fine):
    """The water of the block that does not reach the border of the block: the inland water of the block"""
    labels, sizes = components(fine)
    outside = border_labels(labels)
    keep = np.zeros(len(sizes), dtype=bool)
    for lab in range(1, len(sizes)):
        if lab not in outside:
            keep[lab] = True
    return keep[labels], labels, sizes


def polygonize(mask, gt):
    """The polygons of the mask in degrees"""
    mem = gdal.GetDriverByName("MEM").Create("", mask.shape[1], mask.shape[0], 1, gdal.GDT_Byte)
    mem.SetGeoTransform((gt[0], gt[1], 0, gt[3], 0, gt[5]))
    mem.GetRasterBand(1).WriteArray(mask.astype(np.uint8))
    src_ds = ogr.GetDriverByName("Memory").CreateDataSource("polygons")
    out = src_ds.CreateLayer("polygons", None, ogr.wkbPolygon)
    out.CreateField(ogr.FieldDefn("DN", ogr.OFTInteger))
    gdal.Polygonize(mem.GetRasterBand(1), None, out, 0)
    geoms = []
    for feat in out:
        if feat.GetField("DN") != 1:
            continue
        g = feat.GetGeometryRef()
        if g is not None and not g.IsEmpty() and g.GetGeometryType() in (ogr.wkbPolygon, ogr.wkbMultiPolygon):
            geoms.append(shapely.geometry.shape(json.loads(g.ExportToJson())))
    if not geoms:
        return None
    return shapely.ops.unary_union(geoms)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("grid", help="the raster of the depths (the negative values are the water)")
    ap.add_argument("out", help="the GPKG of the inland water")
    ap.add_argument("--cell", type=float, default=0.01, help="the cell of the coarse grid of the components")
    ap.add_argument("--margin", type=float, default=0.4, help="the margin of the box of a component, degrees")
    ap.add_argument("--min-area", type=float, default=0.002, help="the smallest component to keep, square degrees")
    ap.add_argument("--keep", help="a GPKG of the water to keep (the inland seas, the Black Sea)")
    args = ap.parse_args()

    src = gdal.Open(args.grid)
    if src is None:
        raise SystemExit(f"{args.grid} cannot be opened")
    gt = src.GetGeoTransform()
    nodata = src.GetRasterBand(1).GetNoDataValue()

    tmp = tempfile.mkdtemp(prefix="inland_water_")
    coarse_tif = os.path.join(tmp, "coarse.tif")
    coarse_grid(args.grid, args.cell, coarse_tif, nodata)

    carr = gdal.Open(coarse_tif).ReadAsArray()
    water = np.isfinite(carr) & (carr < 0)
    labels, sizes = components(water)
    del carr, water
    ocean = border_labels(labels)
    objects = ndimage.find_objects(labels)
    min_cells = args.min_area / (args.cell * args.cell)

    keep_geoms = []
    if args.keep and os.path.exists(args.keep):
        ds = ogr.Open(args.keep)
        lyr = ds.GetLayer(0)
        for feat in lyr:
            g = feat.GetGeometryRef()
            if g is not None and not g.IsEmpty():
                keep_geoms.append(shapely.geometry.shape(json.loads(g.ExportToJson())))

    out = gdal.GetDriverByName("GPKG").Create(args.out, 0, 0, 0, gdal.GDT_Unknown)
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(4326)
    srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
    lyr = out.CreateLayer("inland_water", srs, ogr.wkbMultiPolygon)
    lyr.CreateField(ogr.FieldDefn("area", ogr.OFTReal))
    defn = lyr.GetLayerDefn()

    kept = 0
    total = 0
    for lab in range(1, len(sizes)):
        if lab in ocean or sizes[lab] < min_cells:
            continue
        sl = objects[lab - 1]
        west = -180 + sl[1].start * args.cell
        east = -180 + sl[1].stop * args.cell
        south = 90 - sl[0].stop * args.cell
        north = 90 - sl[0].start * args.cell
        west -= args.margin
        east += args.margin
        south -= args.margin
        north += args.margin
        west = max(west, -180.0)
        east = min(east, 180.0)
        south = max(south, -90.0)
        north = min(north, 90.0)
        x0 = int((west - gt[0]) / gt[1])
        x1 = int((east - gt[0]) / gt[1])
        y0 = int((north - gt[3]) / gt[5])
        y1 = int((south - gt[3]) / gt[5])
        x0 = max(x0, 0)
        y0 = max(y0, 0)
        x1 = min(x1, src.RasterXSize)
        y1 = min(y1, src.RasterYSize)
        if x1 <= x0 or y1 <= y0:
            continue
        block = src.ReadAsArray(x0, y0, x1 - x0, y1 - y0)
        fine = np.isfinite(block) & (block < 0)
        if nodata is not None:
            fine &= block != nodata
        del block
        mask, flab, fsizes = enclosed_mask(fine)
        del fine
        if not mask.any():
            # the water of the box reaches its border: it is connected with the sea through a strait
            print(f"connected with the sea: {west:.2f} {south:.2f} {east:.2f} {north:.2f}", flush=True)
            continue
        bgt = (gt[0] + x0 * gt[1], gt[1], 0, gt[3] + y0 * gt[5], 0, gt[5])
        geom = polygonize(mask, bgt)
        del mask
        if geom is None or geom.is_empty:
            continue
        if keep_geoms and any(geom.intersects(k) for k in keep_geoms):
            kept += 1
            print(f"kept: {west:.2f} {south:.2f} {east:.2f} {north:.2f}", flush=True)
            continue
        total += 1
        feat = ogr.Feature(defn)
        feat.SetGeometry(ogr.CreateGeometryFromWkt(geom.wkt))
        feat.SetField("area", geom.area)
        lyr.CreateFeature(feat)
        print(f"inland water: {west:.2f} {south:.2f} {east:.2f} {north:.2f} {geom.area:.3f} sq deg", flush=True)
    out = None
    print(f"{total} inland water bodies, {kept} kept", flush=True)


if __name__ == "__main__":
    main()
