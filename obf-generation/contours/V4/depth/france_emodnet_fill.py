#!/usr/bin/env python3
"""Add EMODnet to the SHOM mosaic only in coastal and enclosed data gaps.

The SHOM zones are averaged to the 0.0002 degree grid used by France_contours. On a
coarser selection grid, EMODnet is admitted only into the corridor between the coast
and nearby SHOM/EMODnet data and into empty components enclosed by detailed data and
land. SHOM always has priority. The resulting france_with_emodnet.vrt is the input of
the coastal Laplace fill in uk_coastal_fill.py; open water remains empty.

    france_emodnet_fill.py FRANCE.vrt EMODNET.vrt OUT_DIR --land LAND_GPKG
                           [--foreign NEIGHBOUR.poly ...] [--force]
"""

import argparse
import math
import os
import subprocess

import numpy as np
import rasterio
from affine import Affine
from osgeo import gdal, ogr
from rasterio.features import rasterize
from scipy import ndimage

from uk_emodnet_fill import mask_nonnegative, write_byte_raster, write_coverage
from uk_exclude_ireland import poly_shapes

gdal.UseExceptions()
ogr.UseExceptions()

CREATION = ["COMPRESS=DEFLATE", "PREDICTOR=3", "TILED=YES", "SPARSE_OK=TRUE",
            "BIGTIFF=IF_SAFER"]


def aligned_grid(bounds, cell):
    left = math.floor(bounds.left / cell) * cell
    bottom = math.floor(bounds.bottom / cell) * cell
    right = math.ceil(bounds.right / cell) * cell
    top = math.ceil(bounds.top / cell) * cell
    return (left, bottom, right, top), round((right - left) / cell), round((top - bottom) / cell)


def build_base(source, path, bounds, cell, jobs):
    tmp = path + ".tmp.tif"
    if os.path.exists(tmp):
        os.remove(tmp)
    print(f"SHOM working grid: {path}", flush=True)
    gdal.Warp(tmp, source, format="GTiff", dstSRS="EPSG:4326",
              outputBounds=bounds, xRes=cell, yRes=cell, resampleAlg="average",
              dstNodata=float("nan"), outputType=gdal.GDT_Float32,
              multithread=True, warpMemoryLimit=2048,
              warpOptions=[f"NUM_THREADS={jobs}"], creationOptions=CREATION)
    os.replace(tmp, path)


def clip_land(source, path, bounds):
    if os.path.exists(path):
        return
    print(f"Clipping land: {path}", flush=True)
    clip = [repr(v) for v in bounds]
    subprocess.run(["ogr2ogr", "-q", "-f", "GPKG", "-spat", *clip, "-clipsrc", *clip,
                    "-nlt", "MULTIPOLYGON", path, source, "-nln", "land"], check=True)


def coarse_values(source, bounds, width, height):
    path = f"/vsimem/france_emodnet_fill_{os.getpid()}.tif"
    ds = gdal.Warp(path, source, format="GTiff", dstSRS="EPSG:4326",
                   outputBounds=bounds, width=width, height=height,
                   resampleAlg="average", dstNodata=float("nan"),
                   outputType=gdal.GDT_Float32)
    values = ds.ReadAsArray()
    ds = None
    gdal.Unlink(path)
    return values


def coarse_land(source, bounds, width, height):
    path = f"/vsimem/france_emodnet_land_{os.getpid()}.tif"
    ds = gdal.Rasterize(path, source, format="GTiff", layers=["land"], burnValues=[1],
                        initValues=0, outputType=gdal.GDT_Byte, allTouched=True,
                        outputBounds=bounds, width=width, height=height)
    values = ds.ReadAsArray().astype(bool)
    ds = None
    gdal.Unlink(path)
    return values


def build_gap_mask(base, emodnet, land, mask_path, coverage_path, bounds, mask_cell,
                   radius, coast, foreign_paths):
    left, bottom, right, top = bounds
    width = math.ceil((right - left) / mask_cell)
    height = math.ceil((top - bottom) / mask_cell)
    mask_bounds = (left, top - height * mask_cell, left + width * mask_cell, top)
    transform = (left, mask_cell, 0, top, 0, -mask_cell)
    print(f"Gap selection grid: {width}x{height}", flush=True)
    detail = np.isfinite(coarse_values(base, mask_bounds, width, height))
    emodnet_values = coarse_values(emodnet, mask_bounds, width, height)
    emodnet_valid = np.isfinite(emodnet_values) & (emodnet_values < 0)
    del emodnet_values
    land_mask = coarse_land(land, mask_bounds, width, height)
    midlat = (mask_bounds[1] + mask_bounds[3]) / 2
    sampling = (mask_cell, mask_cell * math.cos(math.radians(midlat)))
    radius_deg = radius / 111320.0

    # Close narrow cracks first, then accept only holes with a detailed-data boundary.
    dilated = ndimage.distance_transform_edt(~detail, sampling=sampling) <= radius_deg
    closed = ndimage.distance_transform_edt(dilated, sampling=sampling) > radius_deg
    del dilated
    with_land = closed | land_mask
    holes = ndimage.binary_fill_holes(with_land) & ~with_land & ~land_mask
    hole_labels, hole_count = ndimage.label(holes)
    touching = ndimage.binary_dilation(closed, structure=np.ones((3, 3)))
    keep_holes = np.unique(hole_labels[touching & (hole_labels > 0)])
    enclosed = np.isin(hole_labels, keep_holes)
    del holes, hole_labels, touching, with_land

    # The same corridor as the Laplace fill, but EMODnet itself can be the nearby data:
    # parts of the French coast, including Pleherel-Plage, have no SHOM zone at all.
    known = detail | emodnet_valid
    land_distance = ndimage.distance_transform_edt(~land_mask, sampling=sampling)
    data_distance = ndimage.distance_transform_edt(~known, sampling=sampling)
    coastal = (land_distance + data_distance) <= coast / 111320.0
    envelope = (coastal | enclosed) & ~land_mask
    if foreign_paths:
        shapes = []
        for path in foreign_paths:
            shapes.extend(poly_shapes(path))
        foreign = rasterize(shapes, out_shape=(height, width),
                            transform=Affine.from_gdal(*transform), fill=0,
                            all_touched=True, dtype="uint8").astype(bool)
        envelope &= ~foreign
    write_byte_raster(mask_path, envelope, transform)
    area = write_coverage(coverage_path, envelope, transform)
    print(f"Enclosed holes kept: {len(keep_holes)} of {hole_count}", flush=True)
    print(f"EMODnet candidate cells: {int((envelope & ~detail & emodnet_valid).sum())}; "
          f"envelope {area:.4f} square degrees", flush=True)


def build_underlay(emodnet, land, coverage, path, bounds, cell, jobs):
    tmp_dir = os.path.join(os.path.dirname(path), "tmp")
    os.makedirs(tmp_dir, exist_ok=True)
    native = os.path.join(tmp_dir, "emodnet_marine.tif")
    tmp = path + ".tmp.tif"
    for p in (native, tmp):
        if os.path.exists(p):
            os.remove(p)
    try:
        with rasterio.open(emodnet) as source:
            native_cell = source.res[0]
        print("Warping EMODnet inside the gap envelope", flush=True)
        gdal.Warp(native, os.path.abspath(emodnet), format="GTiff", dstSRS="EPSG:4326",
                  xRes=native_cell, yRes=native_cell, targetAlignedPixels=True,
                  resampleAlg="near", cutlineDSName=coverage, cutlineLayer="coverage",
                  cropToCutline=True, dstNodata=float("nan"), outputType=gdal.GDT_Float32,
                  multithread=True, warpOptions=[f"NUM_THREADS={jobs}"], creationOptions=CREATION)
        mask_nonnegative(native)
        gdal.Warp(tmp, native, format="GTiff", dstSRS="EPSG:4326",
                  outputBounds=bounds, xRes=cell, yRes=cell, resampleAlg="bilinear",
                  cutlineDSName=coverage, cutlineLayer="coverage",
                  dstNodata=float("nan"), outputType=gdal.GDT_Float32,
                  multithread=True, warpMemoryLimit=2048,
                  warpOptions=[f"NUM_THREADS={jobs}"], creationOptions=CREATION)
        mask_nonnegative(tmp)
        subprocess.run(["gdal_rasterize", "-q", "-burn", "nan", "-l", "land", land, tmp], check=True)
        os.replace(tmp, path)
    finally:
        for p in (native, tmp):
            if os.path.exists(p):
                os.remove(p)


def build_composite(base, underlay, path, bounds, cell):
    tmp = path + ".tmp.vrt"
    sources = [base] if underlay is None else [underlay, base]
    cmd = ["gdalbuildvrt", "-q", "-overwrite", "-resolution", "user",
           "-tr", repr(cell), repr(cell), "-te", *(repr(v) for v in bounds),
           "-srcnodata", "nan", "-vrtnodata", "nan", tmp, *sources]
    subprocess.run(cmd, check=True)
    os.replace(tmp, path)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("source", help="france.vrt of the original SHOM zones")
    ap.add_argument("emodnet", help="EMODnet DTM VRT")
    ap.add_argument("out_dir")
    ap.add_argument("--land", required=True, help="land polygons GeoPackage")
    ap.add_argument("--foreign", action="append", default=[],
                    help="neighbour .poly whose land and water are not filled; repeatable")
    ap.add_argument("--cell", type=float, default=0.0002, help="working grid in degrees")
    ap.add_argument("--mask-cell", type=float, default=0.002, help="gap selection grid in degrees")
    ap.add_argument("--radius", type=float, default=2000, help="crack closing radius in metres")
    ap.add_argument("--coast", type=float, default=6000,
                    help="coastal corridor: distance to land + distance to data, in metres")
    ap.add_argument("-j", "--jobs", type=int, default=16)
    ap.add_argument("--force", action="store_true", help="rebuild the intermediate rasters")
    args = ap.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)
    with rasterio.open(args.source) as source:
        if source.crs is None or source.crs.to_epsg() != 4326:
            raise SystemExit(f"{args.source}: expected EPSG:4326, got {source.crs}")
        bounds, width, height = aligned_grid(source.bounds, args.cell)
    print(f"Working grid: {width}x{height}, bounds {bounds}", flush=True)
    land = os.path.join(args.out_dir, "land_clip.gpkg")
    if args.force and os.path.exists(land):
        os.remove(land)
    clip_land(args.land, land, bounds)

    base = os.path.join(args.out_dir, "france_base.tif")
    mask = os.path.join(args.out_dir, "france_emodnet_mask.tif")
    coverage = os.path.join(args.out_dir, "france_emodnet_gaps.gpkg")
    underlay = os.path.join(args.out_dir, "france_emodnet_fill.tif")
    composite = os.path.join(args.out_dir, "france_with_emodnet.vrt")
    if args.force or not os.path.exists(base):
        build_base(args.source, base, bounds, args.cell, args.jobs)
    if args.force or not os.path.exists(mask) or not os.path.exists(coverage):
        build_gap_mask(base, args.emodnet, land, mask, coverage, bounds, args.mask_cell,
                       args.radius, args.coast, args.foreign)
    with rasterio.open(mask) as selected:
        has_envelope = bool(selected.read(1).any())
    if has_envelope and (args.force or not os.path.exists(underlay)):
        build_underlay(args.emodnet, land, coverage, underlay, bounds, args.cell, args.jobs)
    build_composite(base, underlay if has_envelope else None, composite, bounds, args.cell)
    print(composite, flush=True)


if __name__ == "__main__":
    main()
