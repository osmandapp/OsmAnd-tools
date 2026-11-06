#!/usr/bin/env python3
"""Fill the coastal gap of uk.vrt between the coastline (0 m) and the survey data.

Land without data is 0 m; every finite value, including a drying bank above 0 m, is kept
as it is. A sea cell is filled when it lies between the coastline and the data and
distance_to_land + distance_to_data <= LIMIT; the filled values solve laplace(h) = 0 with
Dirichlet boundaries land = 0 and the survey depths. Open sea without data stays NaN. The
mosaic is processed in tiles with an overlap margin; the result is a VRT over per-tile
GeoTIFFs, so the input is not copied as one huge raster.

The grid of the UK reaches into the waters of the neighbouring countries, and their land
lies in it as well. With --foreign the land inside those polygons is not this map's coast:
it stays NaN, it is no 0 m boundary of the fill, and the fill does not reach into their
waters (the corridor is measured from the coast of this map only).

    uk_coastal_fill.py SRC OUT_DIR --land LAND [--exclude IRELAND.poly] [--exclude-raster ireland.vrt]
                       [--land-mask land_mask.tif] [--foreign NEIGHBOUR.poly ...]
                       [--limit 6000] [--tile 4096] [--margin 500] [-j N]
"""

import argparse
import math
import os
import subprocess
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed

import numpy as np
import rasterio
from osgeo import gdal
from rasterio.enums import Resampling
from rasterio.features import rasterize
from rasterio.windows import Window, from_bounds
from scipy import ndimage, sparse
from scipy.sparse.linalg import cg, spsolve

from uk_exclude_ireland import poly_shapes

gdal.UseExceptions()
gdal.PushErrorHandler("CPLQuietErrorHandler")


def rasterize_land(bounds, width, height, land_path, layer):
    """Rasterize land for the window, allTouched like the fill prototype."""
    left, bottom, right, top = bounds
    mem = f"/vsimem/coastal_land_{os.getpid()}.tif"
    ds = gdal.Rasterize(mem, land_path, format="GTiff", layers=[layer], burnValues=[1],
                        initValues=0, outputType=gdal.GDT_Byte, allTouched=True,
                        outputBounds=(left, bottom, right, top), width=width, height=height)
    arr = ds.ReadAsArray().astype(bool)
    ds = None
    gdal.Unlink(mem)
    return arr


def create_land_mask(path, land_path, layer, bounds, width, height):
    """Rasterize the whole source grid once; reading it per tile is much faster."""
    print(f"creating land mask {path} ...", flush=True)
    gdal.Rasterize(path, land_path, format="GTiff", layers=[layer], burnValues=[1], initValues=0,
                   outputType=gdal.GDT_Byte, allTouched=True,
                   outputBounds=(bounds.left, bounds.bottom, bounds.right, bounds.top),
                   width=width, height=height,
                   creationOptions=["COMPRESS=DEFLATE", "TILED=YES", "BIGTIFF=IF_SAFER"])


def solve_component(unknown, known, values):
    n = int(unknown.sum())
    if n == 0:
        return None
    idx = np.full(unknown.shape, -1, dtype=np.int64)
    idx[unknown] = np.arange(n)
    coords = np.argwhere(unknown)
    diag = np.zeros(n)
    rhs = np.zeros(n)
    rows, cols, vals = [], [], []
    h, w = unknown.shape
    for dy, dx in ((1, 0), (-1, 0), (0, 1), (0, -1)):
        yy = coords[:, 0] + dy
        xx = coords[:, 1] + dx
        inside = (yy >= 0) & (yy < h) & (xx >= 0) & (xx < w)
        yy, xx = yy[inside], xx[inside]
        rr = idx[coords[inside, 0], coords[inside, 1]]
        un = unknown[yy, xx]
        # the diagonal counts every neighbour in the domain; a known neighbour moves to
        # the right-hand side, an unknown one to the off-diagonal
        diag[rr[known[yy, xx] | un]] += 1.0
        rows.append(rr[un])
        cols.append(idx[yy[un], xx[un]])
        vals.append(np.full(int(un.sum()), -1.0))
        kn = ~un & known[yy, xx]
        rhs[rr[kn]] += values[yy[kn], xx[kn]]

    matrix = sparse.coo_matrix(
        (np.concatenate(vals), (np.concatenate(rows), np.concatenate(cols))), shape=(n, n)
    ).tocsr()
    matrix = matrix + sparse.diags(diag)
    if n <= 1_000_000:
        return spsolve(matrix, rhs)
    solution, info = cg(matrix, rhs, rtol=1e-6, maxiter=5000)
    if info != 0:
        print(f"  cg did not converge ({info}), using a direct solve for {n} cells", flush=True)
        solution = spsolve(matrix, rhs)
    return solution


def laplace_fill(land, data, fill):
    """Solve laplace(h)=0 on fill cells; land without data is 0, finite data is kept.

    Every connected component is solved on its own bounding box, so the matrix stays small.
    Components without a land/data neighbour have no boundary condition: they get the
    nearest known value instead, otherwise the system is singular.
    """
    known = land | np.isfinite(data)
    values = np.where(np.isfinite(data), data, np.where(land, 0.0, np.nan)).astype(np.float64)
    labels, ncomp = ndimage.label(fill)
    if ncomp == 0:
        return values
    touching = fill & ndimage.binary_dilation(known)
    good = np.unique(labels[touching])
    bad = fill & ~np.isin(labels, good)
    if bad.any():
        _, ind = ndimage.distance_transform_edt(~known, return_indices=True)
        values[bad] = values[ind[0][bad], ind[1][bad]]
        known |= bad
        fill = fill & ~bad
        labels, ncomp = ndimage.label(fill)

    h, w = fill.shape
    for i, sl in enumerate(ndimage.find_objects(labels), start=1):
        if sl is None:
            continue
        y0, y1 = max(0, sl[0].start - 1), min(h, sl[0].stop + 1)
        x0, x1 = max(0, sl[1].start - 1), min(w, sl[1].stop + 1)
        unknown = labels[y0:y1, x0:x1] == i
        if not unknown.any():
            continue
        solution = solve_component(unknown, known[y0:y1, x0:x1], values[y0:y1, x0:x1])
        if solution is not None:
            values[y0:y1, x0:x1][unknown] = solution
    return values


def fill_window(depth, land, transform, limit, exclude=None, foreign=None):
    finite = np.isfinite(depth)
    data = finite & ~land
    # the land of a neighbouring country is not this map's coast: it is no 0 m boundary
    # and it stays NaN in the output
    uk_land = land if foreign is None else (land & ~foreign)
    # a finite value, including a drying bank above 0 m, is kept as is; the land of this
    # map without data is the only source of the 0 m boundary
    values = np.where(finite, depth, np.nan).astype(np.float32)
    values[uk_land] = 0.0
    gap = ~land & ~data
    # no land or no data means no fill corridor at all; no gap means nothing to solve
    if not data.any() or not uk_land.any() or not gap.any():
        out = values.astype(np.float32)
        out[land & ~uk_land] = np.nan
        if exclude is not None:
            out[exclude] = np.nan
        return out, 0, np.zeros(depth.shape, dtype=bool)
    lat = transform.f + transform.e * depth.shape[0] / 2
    sy = abs(transform.e) * 111320.0
    sx = sy * math.cos(math.radians(lat))
    # the corridor is measured from the coast of this map, so the fill does not creep
    # into the waters of a neighbouring country
    dist_land = ndimage.distance_transform_edt(~uk_land, sampling=(sy, sx))
    dist_data = ndimage.distance_transform_edt(~data, sampling=(sy, sx))
    fill = gap & ((dist_land + dist_data) <= limit)
    filled = laplace_fill(uk_land, values, fill)
    out = filled.astype(np.float32)
    out[~land & ~data & ~fill] = np.nan
    out[land & ~uk_land] = np.nan
    if exclude is not None:
        out[exclude] = np.nan
    return out, int(fill.sum()), fill


def tile_path(tile_dir, col, row):
    return os.path.join(tile_dir, f"tile_{col:04d}_{row:04d}.tif")


def process_tiles(src, tile_dir, land, land_layer, width, height,
                  limit, tile_size, margin, tiles, force, exclude_shapes, exclude_raster,
                  land_mask_path, foreign_shapes=None):
    written = 0
    filled = 0
    with rasterio.open(src) as ds:
        for col, row in tiles:
            path = tile_path(tile_dir, col, row)
            if os.path.exists(path) and not force:
                continue
            x0, y0 = col * tile_size, row * tile_size
            x1, y1 = min(x0 + tile_size, width), min(y0 + tile_size, height)
            ex0, ey0 = max(0, x0 - margin), max(0, y0 - margin)
            ex1, ey1 = min(width, x1 + margin), min(height, y1 + margin)
            window = Window(ex0, ey0, ex1 - ex0, ey1 - ey0)
            depth = ds.read(1, window=window, boundless=True, fill_value=np.nan).astype(np.float32)
            window_transform = ds.window_transform(window)
            bounds = rasterio.windows.bounds(window, ds.transform)
            if land_mask_path:
                with rasterio.open(land_mask_path) as lm:
                    mask = lm.read(1, window=window, boundless=True, fill_value=0).astype(bool)
            else:
                mask = rasterize_land(bounds, depth.shape[1], depth.shape[0], land, land_layer)
            exclude = np.zeros(depth.shape, dtype=bool)
            if exclude_shapes:
                exclude |= rasterize(exclude_shapes, out_shape=depth.shape, transform=window_transform,
                                     fill=0, all_touched=False, dtype="uint8").astype(bool)
            if exclude_raster:
                with rasterio.open(exclude_raster) as other:
                    if not (other.bounds.right < bounds[0] or other.bounds.left > bounds[2]
                            or other.bounds.top < bounds[1] or other.bounds.bottom > bounds[3]):
                        win = from_bounds(*bounds, transform=other.transform)
                        arr = other.read(1, window=win, out_shape=depth.shape, boundless=True,
                                         fill_value=np.nan, resampling=Resampling.nearest)
                        exclude |= np.isfinite(arr)
            foreign = None
            if foreign_shapes:
                foreign = rasterize(foreign_shapes, out_shape=depth.shape, transform=window_transform,
                                    fill=0, dtype="uint8", all_touched=True).astype(bool)
                if exclude is not None and exclude.any():
                    # the excluded waters (Ireland) are not this map's coast either
                    foreign |= exclude
            out, count, fill_mask = fill_window(depth, mask, window_transform, limit,
                                                exclude if exclude.any() else None, foreign)
            central = out[y0 - ey0:y1 - ey0, x0 - ex0:x1 - ex0]
            central_filled = fill_mask[y0 - ey0:y1 - ey0, x0 - ex0:x1 - ex0]
            central_transform = ds.window_transform(Window(x0, y0, x1 - x0, y1 - y0))
            profile = {
                "driver": "GTiff", "width": x1 - x0, "height": y1 - y0, "count": 2,
                "dtype": "float32", "crs": ds.crs, "transform": central_transform,
                "nodata": float("nan"), "compress": "deflate", "predictor": 3,
                "tiled": True, "bigtiff": "IF_SAFER",
            }
            with rasterio.open(path + ".tmp", "w", **profile) as dst:
                dst.write(central, 1)
                # the second band marks the cells whose value is interpolated
                dst.write(central_filled.astype(np.uint8), 2)
            os.replace(path + ".tmp", path)
            written += 1
            filled += count
    return written, filled


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("src", help="blended depth mosaic, e.g. src/uk/uk.vrt")
    parser.add_argument("out_dir", help="directory for fill_tiles/ and the output VRT")
    parser.add_argument("--land", required=True, help="land polygons (GeoPackage or shapefile)")
    parser.add_argument("--land-layer", help="layer of the land file, default: first layer")
    parser.add_argument("--land-mask", help="cached land mask GeoTIFF on the source grid (created if missing)")
    parser.add_argument("--land-clip", help="clipped land layer, default OUT_DIR/land_clip.gpkg")
    parser.add_argument("--limit", type=float, default=6000.0,
                        help="fill where distance_to_land + distance_to_data <= LIMIT metres")
    parser.add_argument("--tile", type=int, default=4096, help="tile size in cells")
    parser.add_argument("--margin", type=int, default=500, help="overlap margin in cells")
    parser.add_argument("-j", "--jobs", type=int, default=4, help="tiles processed in parallel")
    parser.add_argument("--name", default="uk_filled", help="name of the output VRT")
    parser.add_argument("--exclude", help="OsmAnd .poly to mask out (Ireland)")
    parser.add_argument("--exclude-raster", help="raster whose valid cells are masked out (INFOMAR)")
    parser.add_argument("--foreign", action="append",
                        help="OsmAnd .poly of a neighbouring country: the land inside it is not "
                             "this map's coast, it stays NaN and the fill does not reach into its "
                             "waters, can be given several times")
    parser.add_argument("--force", action="store_true", help="reprocess existing tiles")
    args = parser.parse_args()

    with rasterio.open(args.src) as ds:
        width, height = ds.width, ds.height
        res, crs, bounds = ds.res, ds.crs, ds.bounds
    if crs is None or crs.to_epsg() != 4326:
        sys.exit(f"{args.src}: expected EPSG:4326, got {crs}")

    os.makedirs(args.out_dir, exist_ok=True)
    # the worldwide land polygons are huge; clip them once, the per-tile rasterization
    # of the clipped layer takes milliseconds instead of minutes
    land_path = args.land_clip or os.path.join(args.out_dir, "land_clip.gpkg")
    land_layer = "land"
    if not os.path.exists(land_path):
        print(f"clipping land to the source grid: {land_path} ...", flush=True)
        clip_cmd = [
            "ogr2ogr", "-q", "-f", "GPKG",
            "-spat", repr(bounds.left), repr(bounds.bottom), repr(bounds.right), repr(bounds.top),
            "-clipsrc", repr(bounds.left), repr(bounds.bottom), repr(bounds.right), repr(bounds.top),
            "-nlt", "MULTIPOLYGON", land_path, args.land, "-nln", "land",
        ]
        if args.land_layer:
            clip_cmd.append(args.land_layer)
        subprocess.run(clip_cmd, check=True)
    if args.land_mask and not os.path.exists(args.land_mask):
        create_land_mask(args.land_mask, land_path, land_layer, bounds, width, height)
    exclude_shapes = poly_shapes(args.exclude) if args.exclude else None
    foreign_shapes = []
    for path in args.foreign or []:
        foreign_shapes.extend(poly_shapes(path))
    if foreign_shapes:
        print(f"foreign land: {len(args.foreign)} polygons", flush=True)
    tile_dir = os.path.join(args.out_dir, "fill_tiles")
    os.makedirs(tile_dir, exist_ok=True)
    ncols = math.ceil(width / args.tile)
    nrows = math.ceil(height / args.tile)
    tiles = [(col, row) for row in range(nrows) for col in range(ncols)]
    # long rows first, so the workers finish at about the same time
    chunks = [tiles[i::args.jobs] for i in range(args.jobs)]

    print(f"{args.src}: {width}x{height}, {len(tiles)} tiles of {args.tile}, "
          f"margin {args.margin}, limit {args.limit:.0f} m, {args.jobs} jobs", flush=True)
    written = filled = 0
    with ProcessPoolExecutor(max_workers=args.jobs) as pool:
        futures = [
            pool.submit(process_tiles, args.src, tile_dir, land_path, land_layer, width, height,
                        args.limit, args.tile, args.margin, chunk, args.force,
                        exclude_shapes, args.exclude_raster, args.land_mask, foreign_shapes)
            for chunk in chunks if chunk
        ]
        for future in as_completed(futures):
            w, f = future.result()
            written += w
            filled += f
            print(f"  {written}/{len(tiles)} tiles, {filled} filled cells", flush=True)

    tile_files = [tile_path(tile_dir, col, row) for col, row in tiles]
    missing = [p for p in tile_files if not os.path.exists(p)]
    if missing:
        sys.exit(f"{len(missing)} tiles are missing, first: {missing[0]}")

    vrt = os.path.join(args.out_dir, f"{args.name}.vrt")
    if os.path.exists(vrt):
        os.chmod(vrt, 0o644)
    cmd = [
        "gdalbuildvrt", "-q", "-overwrite", "-srcnodata", "nan", "-vrtnodata", "nan",
        "-b", "1",
        "-te", repr(bounds.left), repr(bounds.bottom), repr(bounds.right), repr(bounds.top),
        "-tr", repr(res[0]), repr(res[1]), vrt,
    ] + tile_files
    subprocess.run(cmd, check=True)
    os.chmod(vrt, 0o444)
    # the second band of the tiles marks the interpolated cells: no depth point is placed on them
    interp = os.path.join(args.out_dir, f"{args.name}_interp.vrt")
    if os.path.exists(interp):
        os.chmod(interp, 0o644)
    cmd = [
        "gdalbuildvrt", "-q", "-overwrite", "-srcnodata", "nan", "-vrtnodata", "nan",
        "-b", "2",
        "-te", repr(bounds.left), repr(bounds.bottom), repr(bounds.right), repr(bounds.top),
        "-tr", repr(res[0]), repr(res[1]), interp,
    ] + tile_files
    subprocess.run(cmd, check=True)
    os.chmod(interp, 0o444)
    print(f"{vrt}: {written} tiles written, {filled} filled cells", flush=True)


if __name__ == "__main__":
    main()
