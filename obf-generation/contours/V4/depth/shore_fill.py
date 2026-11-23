#!/usr/bin/env python3
"""Fill the gap between the shoreline of a lake and its survey data.

The land is the complement of the lake polygon inside the raster; the islands are holes
of the polygon, so they are land as well. The survey depths (finite negative values) are
kept even where the cell centre falls on a small island: the grid is coarser than the
island, so the cell still holds water, and the products are cut by the water outline
anyway. The land is the 0 m boundary, and a water cell without data is filled when it
lies in the corridor between the land and the data:

    distance_to_land + distance_to_data <= LIMIT
    distance_to_data <= DATA_LIMIT
    water at least OPEN_WIDTH metres from land lies within OPEN_DISTANCE,
    or the cell is within EDGE metres of land

Open water is the part of the lake at least OPEN_WIDTH metres from the land. The third
condition keeps the fill out of narrow river beds and deltas: a channel between two
banks has no open water of its own, so it is filled only near its mouth, where the lake
is still within OPEN_DISTANCE. EDGE fills the shore ring even in narrow water, so the
bands reach the islands; with EDGE=0 a narrow channel keeps a cell-wide gap at the
banks. Open water without data farther from the shore than LIMIT stays NaN.

The filled values solve laplace(h) = 0 with the land at 0 m and the survey depths as
Dirichlet boundaries. The raster is processed in tiles with an overlap margin, so the
matrices stay small; the result is a VRT over per-tile GeoTIFFs. The second band of the
tiles marks the interpolated cells, so no sounding is placed on them.

    shore_fill.py DEPTH WATER.gpkg --lake KEY OUT_DIR [--name NAME]
                  [--layer water] [--limit 2000] [--data-limit 1000]
                  [--open-width 50] [--open-distance 100] [--edge 0]
                  [--tile 2048] [--margin 512] [-j N] [--force]
"""

import argparse
import hashlib
import math
import os
import subprocess
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed

import numpy as np
import rasterio
from osgeo import gdal
from rasterio.windows import Window
from scipy import ndimage

from uk_coastal_fill import laplace_fill, solve_component

gdal.UseExceptions()
gdal.PushErrorHandler('CPLQuietErrorHandler')


def rasterize_water(bounds, width, height, water, layer, lake):
    """Water cells of the lake for a window, by the centre of the cell."""
    left, bottom, right, top = bounds
    mem = '/vsimem/shore_water_%d.tif' % os.getpid()
    where = "lake = '%s'" % lake.replace("'", "''")
    ds = gdal.Rasterize(mem, water, format='GTiff', layers=[layer], burnValues=[1], initValues=0,
                        outputType=gdal.GDT_Byte, allTouched=False,
                        outputBounds=(left, bottom, right, top), width=width, height=height,
                        where=where)
    arr = ds.ReadAsArray().astype(bool)
    ds = None
    gdal.Unlink(mem)
    return arr


def laplace_fill_fast(land, values, fill):
    """Solve laplace(h)=0 on the fill cells.

    A lake shore gives many tiny disconnected gaps (a data cell touching the land cuts the
    strip), and a direct solve per component is dominated by the call overhead. When the
    whole fill mask is small, it is solved as one system instead; a large mask keeps the
    per-component path of uk_coastal_fill.
    """
    n = int(fill.sum())
    if n == 0:
        return values
    if n > 200000:
        return laplace_fill(land, values, fill)
    known = land | np.isfinite(values)
    out = np.where(np.isfinite(values), values, np.where(land, 0.0, np.nan)).astype(np.float64)
    labels, ncomp = ndimage.label(fill)
    if ncomp == 0:
        return out
    touching = fill & ndimage.binary_dilation(known)
    good = np.unique(labels[touching])
    bad = fill & ~np.isin(labels, good)
    if bad.any():
        _, ind = ndimage.distance_transform_edt(~known, return_indices=True)
        out[bad] = out[ind[0][bad], ind[1][bad]]
        known |= bad
        fill = fill & ~bad
    if fill.any():
        solution = solve_component(fill, known, out)
        if solution is not None:
            out[fill] = solution
    return out


def fill_window(depth, water, transform, limit, data_limit, open_width, open_distance, edge):
    """Fill one window; returns the depth (land and unfilled water as NaN) and the fill mask."""
    land = ~water
    finite = np.isfinite(depth)
    # The survey depths are negative; a positive value in the water polygon is the
    # grid's land (the Great Lakes keep the shore as a positive height), not a depth.
    # It is not land for the fill: a 3 arc-second cell of the grid's land can cover
    # water the map draws, and treating it as land would leave a cell-wide gap around
    # every island. The water outline, not the coarse grid, decides what is land.
    data = finite & (depth < 0) & water
    gap = water & ~data
    values = np.where(data, depth, np.nan).astype(np.float32)
    values[land] = 0.0
    out = np.full(depth.shape, np.nan, dtype=np.float32)
    # A cell whose centre falls on a small island is land for the outline, but its survey
    # depth is real water: the grid is coarser than the island. Keep it in the output; the
    # bands, contours and soundings are cut by the water outline anyway, and without it the
    # cell becomes a NaN hole the size of a cell, a diamond gap at every small island.
    survey = finite & (depth < 0)
    out[survey] = depth[survey]
    fill = np.zeros(depth.shape, dtype=bool)
    if not data.any() or not land.any() or not gap.any():
        out[data] = depth[data]
        return out, fill

    lat = transform.f + transform.e * depth.shape[0] / 2
    sy = abs(transform.e) * 111320.0
    sx = sy * math.cos(math.radians(lat))
    dist_land = ndimage.distance_transform_edt(~land, sampling=(sy, sx))
    dist_data = ndimage.distance_transform_edt(~data, sampling=(sy, sx))
    fill = gap & ((dist_land + dist_data) <= limit) & (dist_data <= data_limit)

    # Keep the fill out of narrow channels: within OPEN_DISTANCE of the cell there must be
    # water at least OPEN_WIDTH metres from the land, that is the open lake. A river bed
    # between two banks has no such water nearby and is left without interpolation. The
    # shore ring within EDGE metres of land is filled anyway: in a channel just wide
    # enough to fail the guard, the guard would otherwise leave a cell-wide gap at the
    # bank, and the shallow bands would stop short of the island.
    if open_width > 0 and open_distance > 0 and fill.any():
        radius = max(1, int(round(open_distance / max(1.0, min(sy, sx)))))
        local_open = ndimage.maximum_filter(dist_land, size=2 * radius + 1) > open_width
        if edge > 0:
            local_open |= dist_land <= edge
        fill &= local_open

    if fill.any():
        values = laplace_fill_fast(land, values, fill)
        out[data] = depth[data]
        out[fill] = values[fill]
    else:
        out[data] = depth[data]
    return out, fill


def tile_path(tile_dir, col, row):
    return os.path.join(tile_dir, 'tile_%04d_%04d.tif' % (col, row))


def process_tiles(src, tile_dir, water, layer, lake, width, height, limit, data_limit,
                  open_width, open_distance, edge, tile_size, margin, tiles, force):
    written = filled = 0
    with rasterio.open(src) as ds:
        nodata = ds.nodata
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
            if nodata is not None and not math.isnan(nodata):
                depth[depth == nodata] = np.nan
            bounds = rasterio.windows.bounds(window, ds.transform)
            wet = rasterize_water(bounds, depth.shape[1], depth.shape[0], water, layer, lake)
            transform = ds.window_transform(window)
            out, fill_mask = fill_window(depth, wet, transform, limit, data_limit,
                                         open_width, open_distance, edge)
            central = out[y0 - ey0:y1 - ey0, x0 - ex0:x1 - ex0]
            central_fill = fill_mask[y0 - ey0:y1 - ey0, x0 - ex0:x1 - ex0]
            central_transform = ds.window_transform(Window(x0, y0, x1 - x0, y1 - y0))
            profile = {
                'driver': 'GTiff', 'width': x1 - x0, 'height': y1 - y0, 'count': 2,
                'dtype': 'float32', 'crs': ds.crs, 'transform': central_transform,
                'nodata': float('nan'), 'compress': 'deflate', 'predictor': 3,
                'tiled': True, 'bigtiff': 'IF_SAFER',
            }
            with rasterio.open(path + '.tmp', 'w', **profile) as dst:
                dst.write(central, 1)
                dst.write(central_fill.astype(np.float32), 2)
            os.replace(path + '.tmp', path)
            written += 1
            filled += int(central_fill.sum())
    return written, filled


def signature(src, water, lake, limit, data_limit, open_width, open_distance, edge, tile, margin, name):
    parts = [name, lake, str(limit), str(data_limit), str(open_width), str(open_distance),
             str(edge), str(tile), str(margin)]
    for path in (src, water):
        try:
            st = os.stat(path)
            parts.append('%s:%d:%d' % (path, st.st_size, int(st.st_mtime)))
        except OSError:
            parts.append('%s:missing' % path)
    return hashlib.md5('|'.join(parts).encode()).hexdigest()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('src', help='depth raster of the lake, negative depths')
    parser.add_argument('water', help='water polygons of the lakes (GeoPackage)')
    parser.add_argument('out_dir', help='directory for fill_tiles/ and the output VRTs')
    parser.add_argument('--lake', required=True, help='value of the lake field in the water layer')
    parser.add_argument('--layer', default='water', help='water layer, default water')
    parser.add_argument('--name', default='filled', help='name of the output VRTs, default filled')
    parser.add_argument('--limit', type=float, default=2000.0,
                        help='fill where distance_to_land + distance_to_data <= LIMIT metres')
    parser.add_argument('--data-limit', type=float, default=1000.0,
                        help='fill no farther than DATA_LIMIT metres from real data')
    parser.add_argument('--open-width', type=float, default=50.0,
                        help='water at least OPEN_WIDTH metres from land is open water; '
                             '0 disables the channel guard')
    parser.add_argument('--open-distance', type=float, default=100.0,
                        help='fill only within OPEN_DISTANCE metres of water that is at least '
                             'OPEN_WIDTH metres from land; 0 disables the channel guard')
    parser.add_argument('--edge', type=float, default=0.0,
                        help='fill within EDGE metres of land even where the channel guard '
                             'fails, so the bands reach the banks of a narrow channel; '
                             '0 keeps the cell-wide gap at the banks')
    parser.add_argument('--tile', type=int, default=2048, help='tile size in cells')
    parser.add_argument('--margin', type=int, default=512, help='overlap margin in cells')
    parser.add_argument('-j', '--jobs', type=int, default=4, help='tiles processed in parallel')
    parser.add_argument('--force', action='store_true', help='reprocess existing tiles')
    args = parser.parse_args()

    with rasterio.open(args.src) as ds:
        width, height = ds.width, ds.height
        res, crs, bounds = ds.res, ds.crs, ds.bounds
    # The NOAA grids are NAD83 (EPSG:4269); the datum shift to WGS84 is below a metre, far
    # below the 3 arc-second cell, and the rest of the Great Lakes build already treats them
    # as the same degrees as the OSM outline.
    if crs is None or crs.to_epsg() not in (4326, 4269):
        sys.exit('%s: expected EPSG:4326 or 4269, got %s' % (args.src, crs))

    os.makedirs(args.out_dir, exist_ok=True)
    tile_dir = os.path.join(args.out_dir, 'fill_tiles')
    os.makedirs(tile_dir, exist_ok=True)
    ncols = math.ceil(width / args.tile)
    nrows = math.ceil(height / args.tile)
    tiles = [(col, row) for row in range(nrows) for col in range(ncols)]
    chunks = [tiles[i::args.jobs] for i in range(args.jobs)]

    sig_path = os.path.join(args.out_dir, '%s_fill_signature.txt' % args.name)
    current = signature(args.src, args.water, args.lake, args.limit, args.data_limit,
                        args.open_width, args.open_distance, args.edge, args.tile, args.margin,
                        args.name)
    previous = ''
    if os.path.exists(sig_path):
        with open(sig_path) as f:
            previous = f.read().strip()
    force = args.force or previous != current
    if previous != current:
        print('the inputs of the fill changed (%s -> %s), reprocessing the tiles'
              % (previous[:12] or 'none', current[:12]), flush=True)
    with open(sig_path, 'w') as f:
        f.write(current + '\n')

    print('%s: %dx%d, %d tiles of %d, margin %d, limit %.0f m, data limit %.0f m, '
          'open water > %.0f m within %.0f m, shore edge %.0f m, %d jobs'
          % (args.src, width, height, len(tiles), args.tile, args.margin, args.limit,
             args.data_limit, args.open_width, args.open_distance, args.edge, args.jobs), flush=True)
    written = filled = 0
    with ProcessPoolExecutor(max_workers=args.jobs) as pool:
        futures = [
            pool.submit(process_tiles, args.src, tile_dir, args.water, args.layer, args.lake,
                        width, height, args.limit, args.data_limit, args.open_width,
                        args.open_distance, args.edge, args.tile, args.margin, chunk, force)
            for chunk in chunks if chunk
        ]
        for future in as_completed(futures):
            w, f = future.result()
            written += w
            filled += f
            print('  %d/%d tiles, %d filled cells' % (written, len(tiles), filled), flush=True)

    missing = [tile_path(tile_dir, c, r) for c, r in tiles
               if not os.path.exists(tile_path(tile_dir, c, r))]
    if missing:
        sys.exit('%d tiles are missing, first: %s' % (len(missing), missing[0]))

    tile_files = [tile_path(tile_dir, c, r) for c, r in tiles]
    vrt = os.path.join(args.out_dir, '%s.vrt' % args.name)
    cmd = ['gdalbuildvrt', '-q', '-overwrite', '-srcnodata', 'nan', '-vrtnodata', 'nan',
           '-b', '1', '-te', repr(bounds.left), repr(bounds.bottom), repr(bounds.right),
           repr(bounds.top), '-tr', repr(res[0]), repr(res[1]), vrt] + tile_files
    subprocess.run(cmd, check=True)
    interp = os.path.join(args.out_dir, '%s_interp.vrt' % args.name)
    cmd = ['gdalbuildvrt', '-q', '-overwrite', '-srcnodata', 'nan', '-vrtnodata', 'nan',
           '-b', '2', '-te', repr(bounds.left), repr(bounds.bottom), repr(bounds.right),
           repr(bounds.top), '-tr', repr(res[0]), repr(res[1]), interp] + tile_files
    subprocess.run(cmd, check=True)
    print('%s: %d tiles written, %d filled cells' % (vrt, written, filled), flush=True)


if __name__ == '__main__':
    main()
