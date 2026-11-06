#!/usr/bin/env python3
"""Preview the coastal area where EMODnet would fill gaps in detailed UK surveys.

Surveys coarser than 120 m are excluded first. EMODnet replaces only low-resolution components
adjacent to detailed data and fills sea areas enclosed by detailed data and land. The script writes
the envelope, new-value cells and PNG overviews at 0.002 degree. It does not change uk.vrt or
uk_sources.vrt.

Usage:
  uk_emodnet_fill.py SRC_UK_DIR EMODNET_VRT LAND_GPKG IRELAND_VRT IRELAND_POLY --output DIR
                   [--build-vrt] [--vrt-only]
"""

import argparse, math, os, subprocess

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import rasterio
from affine import Affine
from matplotlib.patches import Patch
from osgeo import gdal, ogr, osr
from rasterio.features import rasterize
from scipy import ndimage

from uk_blend import atomic_vrt
from uk_build_vrt import survey_sources
from uk_exclude_ireland import poly_shapes

gdal.UseExceptions()
ogr.UseExceptions()
gdal.PushErrorHandler("CPLQuietErrorHandler")
CELL = 0.002


def warp_array(sources, bounds, width, height, resample="average", output_type=gdal.GDT_Float32):
    path = "/vsimem/uk_emodnet_fill_warp.tif"
    ds = gdal.Warp(path, sources, format="GTiff", dstSRS="EPSG:4326",
                   outputBounds=bounds, width=width, height=height,
                   resampleAlg=resample, outputType=output_type, dstNodata=float("nan"))
    a = ds.ReadAsArray()
    ds = None
    gdal.Unlink(path)
    return a


def rasterize_land(path, bounds, width, height):
    out = "/vsimem/uk_emodnet_fill_land.tif"
    ds = gdal.Rasterize(out, path, format="GTiff", outputBounds=bounds,
                        width=width, height=height, initValues=0, burnValues=1,
                        outputType=gdal.GDT_Byte, allTouched=True)
    a = ds.ReadAsArray()
    ds = None
    gdal.Unlink(out)
    return a.astype(bool)


def write_coverage(path, mask, transform):
    driver = gdal.GetDriverByName("GTiff")
    ds = driver.Create("/vsimem/uk_emodnet_fill_mask.tif", mask.shape[1], mask.shape[0],
                       1, gdal.GDT_Byte, options=["COMPRESS=DEFLATE"])
    ds.SetGeoTransform(transform)
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(4326)
    srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
    ds.SetProjection(srs.ExportToWkt())
    band = ds.GetRasterBand(1)
    band.WriteArray(mask.astype(np.uint8))
    band.SetNoDataValue(0)
    driver_name = "Memory" if int(gdal.VersionInfo()) < 3110000 else "MEM"
    mem = ogr.GetDriverByName(driver_name).CreateDataSource("")
    layer = mem.CreateLayer("coverage", srs, ogr.wkbPolygon)
    layer.CreateField(ogr.FieldDefn("v", ogr.OFTInteger))
    gdal.Polygonize(band, band.GetMaskBand(), layer, 0)
    geoms = [f.GetGeometryRef().Clone() for f in layer]
    multi = ogr.Geometry(ogr.wkbMultiPolygon)
    for g in geoms:
        multi.AddGeometry(g)
    area = multi.UnionCascaded() if geoms else multi
    if os.path.exists(path):
        os.remove(path)
    out = ogr.GetDriverByName("GPKG").CreateDataSource(path)
    out_layer = out.CreateLayer("coverage", srs, ogr.wkbMultiPolygon)
    feature = ogr.Feature(out_layer.GetLayerDefn())
    feature.SetGeometry(ogr.ForceToMultiPolygon(area))
    out_layer.CreateFeature(feature)
    out = None
    ds = None
    gdal.Unlink("/vsimem/uk_emodnet_fill_mask.tif")
    return area.GetArea()


def write_byte_raster(path, mask, transform, nodata=0):
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(4326)
    srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
    driver = gdal.GetDriverByName("GTiff")
    ds = driver.Create(path, mask.shape[1], mask.shape[0], 1, gdal.GDT_Byte,
                       options=["COMPRESS=DEFLATE", "TILED=YES"])
    ds.SetGeoTransform(transform)
    ds.SetProjection(srs.ExportToWkt())
    ds.GetRasterBand(1).WriteArray(mask.astype(np.uint8))
    ds.GetRasterBand(1).SetNoDataValue(nodata)
    ds = None


def smooth_coverage(path, out_path):
    source = ogr.Open(path)
    layer = source.GetLayer(0)
    feature = layer.GetNextFeature()
    original = feature.GetGeometryRef().Clone()
    smoothed = original.Buffer(CELL).Buffer(-CELL).SimplifyPreserveTopology(CELL / 10)
    if smoothed.IsEmpty():
        raise RuntimeError("Smoothed EMODnet coverage is empty")
    srs = layer.GetSpatialRef()
    if os.path.exists(out_path):
        os.remove(out_path)
    out = ogr.GetDriverByName("GPKG").CreateDataSource(out_path)
    out_layer = out.CreateLayer("coverage", srs, ogr.wkbMultiPolygon)
    out_feature = ogr.Feature(out_layer.GetLayerDefn())
    out_feature.SetGeometry(ogr.ForceToMultiPolygon(smoothed))
    out_layer.CreateFeature(out_feature)
    out = None
    source = None
    print(f"Smoothed envelope area: {original.GetArea():.4f} -> {smoothed.GetArea():.4f} "
          "square degrees", flush=True)


def mask_nonnegative(path):
    with rasterio.open(path, "r+") as ds:
        for _, window in ds.block_windows(1):
            a = ds.read(1, window=window)
            a[a >= 0] = np.nan
            ds.write(a, 1, window=window)


def build_underlay(uk, emodnet, land):
    coverage = os.path.join(uk, "emodnet_fill_coverage_smooth.gpkg")
    smooth_coverage(os.path.join(uk, "emodnet_fill_coverage.gpkg"), coverage)
    with rasterio.open(emodnet) as source:
        native_cell = source.res[0]
    tmp_dir = os.path.join(uk, "tmp")
    os.makedirs(tmp_dir, exist_ok=True)
    native = os.path.join(tmp_dir, "emodnet_marine.tmp.tif")
    land_clip = os.path.join(tmp_dir, "emodnet_land.gpkg")
    tif = os.path.join(uk, "emodnet_fill.tif")
    tmp = os.path.join(uk, "emodnet_fill.tmp.tif")
    for path in (native, land_clip, tmp):
        if os.path.exists(path):
            os.remove(path)
    try:
        gdal.Warp(native, os.path.abspath(emodnet), format="GTiff", dstSRS="EPSG:4326",
                  xRes=native_cell, yRes=native_cell, targetAlignedPixels=True,
                  resampleAlg="near", cutlineDSName=coverage, cutlineLayer="coverage",
                  cropToCutline=True, dstNodata=float("nan"), outputType=gdal.GDT_Float32,
                  creationOptions=["COMPRESS=DEFLATE", "PREDICTOR=3", "TILED=YES",
                                   "SPARSE_OK=TRUE", "BIGTIFF=IF_SAFER"])
        mask_nonnegative(native)
        gdal.Warp(tmp, native, format="GTiff", dstSRS="EPSG:4326",
                  xRes=0.0002, yRes=0.0002, resampleAlg="bilinear",
                  dstNodata=float("nan"), outputType=gdal.GDT_Float32, multithread=True,
                  warpOptions=["NUM_THREADS=16"],
                  creationOptions=["COMPRESS=DEFLATE", "PREDICTOR=3", "TILED=YES",
                                   "SPARSE_OK=TRUE", "BIGTIFF=IF_SAFER"])
        with rasterio.open(tmp) as clipped:
            b = clipped.bounds
        clip = [str(b.left), str(b.bottom), str(b.right), str(b.top)]
        subprocess.run(["ogr2ogr", "-q", "-f", "GPKG", "-spat", *clip, "-clipsrc", *clip,
                        "-nlt", "MULTIPOLYGON", land_clip, os.path.abspath(land)], check=True)
        subprocess.run(["gdal_rasterize", "-q", "-burn", "nan", land_clip, tmp], check=True)
        os.replace(tmp, tif)
    finally:
        for path in (native, land_clip, tmp):
            if os.path.exists(path):
                os.remove(path)
    out = os.path.join(uk, "emodnet_fill.vrt")
    atomic_vrt([tif], os.path.join(uk, "emodnet_fill_source.txt"), out, 0.0002)
    print("EMODnet underlay written to", tif, "and", out, flush=True)


def save_preview(path, arrays, bounds, xlim=None, ylim=None, title="EMODnet fill preview"):
    detail, new, replacement, lowres, land = arrays
    rgb = np.full(detail.shape + (3,), 255, dtype=np.uint8)
    rgb[land] = (242, 236, 220)
    rgb[detail] = (128, 128, 128)
    rgb[new] = (44, 162, 95)
    rgb[replacement] = (43, 131, 186)
    rgb[lowres & ~new] = (222, 45, 38)
    fig, ax = plt.subplots(figsize=(12, 10), dpi=150)
    ax.imshow(rgb, origin="upper", extent=(bounds[0], bounds[2], bounds[1], bounds[3]),
              interpolation="nearest")
    if xlim:
        ax.set_xlim(*xlim)
    if ylim:
        ax.set_ylim(*ylim)
    ax.set_title(title)
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    ax.legend(handles=[Patch(facecolor="#808080", label="Accepted detailed surveys"),
                       Patch(facecolor="#2ca25f", label="New coastal and gap values"),
                       Patch(facecolor="#2b83ba", label="EMODnet replacing adjacent low-resolution"),
                       Patch(facecolor="#de2d26", label="Isolated low-resolution, no underlay"),
                       Patch(facecolor="#f2ecdc", label="Land")],
              loc="lower right", fontsize=8)
    fig.tight_layout()
    fig.savefig(path)
    plt.close(fig)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("uk")
    ap.add_argument("emodnet")
    ap.add_argument("land")
    ap.add_argument("ireland")
    ap.add_argument("ireland_poly")
    ap.add_argument("--output", required=True)
    ap.add_argument("--radius", type=float, default=2000, help="closing radius in metres")
    ap.add_argument("--coast", type=float, default=2000, help="coastal selection distance in metres")
    ap.add_argument("--build-vrt", action="store_true", help="also build the EMODnet underlay VRT")
    ap.add_argument("--vrt-only", action="store_true", help="use the existing mask and only build its VRT")
    args = ap.parse_args()
    uk = os.path.abspath(args.uk)
    if args.vrt_only:
        build_underlay(uk, args.emodnet, args.land)
        return
    os.makedirs(args.output, exist_ok=True)
    accepted, excluded = survey_sources(uk)
    detail_path = os.path.join(uk, "uk_detail.vrt")
    atomic_vrt(accepted, os.path.join(uk, "uk_detail_sources.txt"), detail_path, 0.0002)
    with rasterio.open(detail_path) as detail:
        left, top = detail.bounds.left, detail.bounds.top
    width = math.ceil((detail.bounds.right - left) / CELL)
    height = math.ceil((detail.bounds.top - detail.bounds.bottom) / CELL)
    bounds = (left, top - height * CELL, left + width * CELL, top)
    transform = (left, CELL, 0, top, 0, -CELL)
    print(f"Coarse preview grid: {width}x{height}, bounds {bounds}", flush=True)
    detail_a = warp_array(detail_path, bounds, width, height)
    valid = np.isfinite(detail_a)
    emodnet_a = warp_array(os.path.abspath(args.emodnet), bounds, width, height)
    emodnet_valid = np.isfinite(emodnet_a)
    lowres_paths = [p for p, _, _ in excluded]
    lowres_a = warp_array(lowres_paths, bounds, width, height) if lowres_paths else np.zeros_like(detail_a)
    lowres_valid = np.isfinite(lowres_a)
    midlat = (bounds[1] + bounds[3]) / 2
    sampling = (CELL, CELL * math.cos(math.radians(midlat)))
    radius = args.radius / 111320
    dilated = ndimage.distance_transform_edt(~valid, sampling=sampling) <= radius
    closed = ndimage.distance_transform_edt(dilated, sampling=sampling) > radius
    land = rasterize_land(os.path.abspath(args.land), bounds, width, height)
    closed = ndimage.binary_fill_holes(closed | land) & ~land
    land_distance = ndimage.distance_transform_edt(~land, sampling=sampling)
    coast = land_distance <= args.coast / 111320
    labels, count = ndimage.label(closed)
    keep = np.unique(labels[coast & (labels > 0)])
    envelope = np.isin(labels, keep)
    print(f"Connected components: {count}; coastal components kept: {len(keep)}", flush=True)
    low_labels, low_count = ndimage.label(lowres_valid, structure=np.ones((3, 3)))
    touching = ndimage.binary_dilation(valid, structure=np.ones((3, 3)))
    low_keep = np.unique(low_labels[touching & (low_labels > 0)])
    replacement = np.isin(low_labels, low_keep)
    envelope |= replacement
    print(f"Low-resolution components: {low_count}; adjacent to detail: {len(low_keep)}", flush=True)
    poly = rasterize(poly_shapes(args.ireland_poly), out_shape=(height, width),
                     transform=Affine.from_gdal(*transform), fill=0, dtype="uint8").astype(bool)
    ireland_a = warp_array(os.path.abspath(args.ireland), bounds, width, height)
    envelope &= ~poly & ~np.isfinite(ireland_a)
    new = envelope & ~valid & emodnet_valid
    replacement_new = replacement & new
    lowres_only = lowres_valid & ~valid
    coverage_path = os.path.join(uk, "emodnet_fill_coverage.gpkg")
    area = write_coverage(coverage_path, envelope, transform)
    new_path = os.path.join(uk, "emodnet_fill_new.tif")
    write_byte_raster(new_path, new, transform)
    rows = np.arange(height)
    lats = top - (rows + 0.5) * CELL
    cell_km2 = (CELL * 111.32) * (CELL * 111.32 * np.cos(np.radians(lats)))
    new_km2 = float((new.sum(axis=1) * cell_km2).sum())
    replacement_km2 = float((replacement_new.sum(axis=1) * cell_km2).sum())
    unfilled = lowres_only & ~new
    unfilled_km2 = float((unfilled.sum(axis=1) * cell_km2).sum())
    print(f"Coastal envelope: {area:.4f} square degrees")
    print(f"New EMODnet cells: {new.sum()}, approximately {new_km2:.0f} km2")
    print(f"EMODnet replacing adjacent low-resolution: {replacement_new.sum()} cells, "
          f"approximately {replacement_km2:.0f} km2")
    print(f"Isolated low-resolution left without underlay: {unfilled.sum()} cells, "
          f"approximately {unfilled_km2:.0f} km2")
    arrays = (valid, new, replacement_new, lowres_only, land)
    save_preview(os.path.join(args.output, "uk_emodnet_fill_full.png"), arrays, bounds,
                 title="UK: areas affected by EMODnet underlay")
    save_preview(os.path.join(args.output, "uk_emodnet_fill_skegness.png"), arrays, bounds,
                 (0.15, 1.05), (52.90, 53.35), "Skegness: EMODnet underlay")
    save_preview(os.path.join(args.output, "uk_emodnet_fill_south.png"), arrays, bounds,
                 (-8.7, 2.0), (49.8, 53.6), "Southern UK: EMODnet underlay")
    save_preview(os.path.join(args.output, "uk_emodnet_fill_north.png"), arrays, bounds,
                 (-8.7, 2.0), (56.4, 61.0), "Northern UK and offshore surveys")
    print("Preview written to", args.output, flush=True)
    if args.build_vrt:
        build_underlay(uk, args.emodnet, args.land)


if __name__ == "__main__":
    main()
